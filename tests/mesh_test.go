package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	httpapi "github.com/Swassyman/chatter/api/http"
	"github.com/Swassyman/chatter/node"
)

func startMeshNode(t *testing.T, httpPort, p2pPort int, handle string) *node.NodeService {
	t.Helper()
	svc, err := node.Start(node.Config{
		DataDir: t.TempDir(), HTTPPort: httpPort, P2PPort: p2pPort, DisableMDNS: true,
	})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { svc.Stop() })
	if err := svc.SetHandle(handle); err != nil {
		t.Fatal(err)
	}
	return svc
}

// shuttleBluetooth carries frames between two nodes the way the Android app
// does over a Bluetooth link: drain one node's outbox into the other's inbox.
func shuttleBluetooth(ctx context.Context, from, to *node.NodeService) {
	for ctx.Err() == nil {
		for _, f := range from.BluetoothOutbox(ctx) {
			if f.To == to.ID() {
				_ = to.BluetoothInbox(from.ID(), f.Data)
			}
		}
	}
}

func waitForMessage(t *testing.T, svc *node.NodeService, msgID string) httpapi.ReceivedMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range svc.Store().Since(-1) {
			if m.MsgID == msgID {
				return m
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s never received message %s", svc.ID(), msgID)
	return httpapi.ReceivedMessage{}
}

func waitForReceipt(t *testing.T, svc *node.NodeService, msgID, status string) httpapi.ReceivedReceipt {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range svc.Store().ReceiptsSince(-1) {
			if r.Status == status && len(r.IDs) == 1 && r.IDs[0] == msgID {
				return r
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s never got a %s receipt for %s", svc.ID(), status, msgID)
	return httpapi.ReceivedReceipt{}
}

// A ~~Bluetooth~~ B ---Wi-Fi--- C. A and C have no link of their own, so B
// relays between its Bluetooth and Wi-Fi neighbours, in both directions, and
// receipts find their way back the same way.
func TestRelayAcrossBluetoothAndWiFi(t *testing.T) {
	a := startMeshNode(t, 9111, 5111, "alice")
	b := startMeshNode(t, 9112, 5112, "bob")
	c := startMeshNode(t, 9113, 5113, "carol")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.SetBluetoothPeers([]string{b.ID()})
	b.SetBluetoothPeers([]string{a.ID()})
	go shuttleBluetooth(ctx, a, b)
	go shuttleBluetooth(ctx, b, a)

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()
	if err := b.ConnectPeer(dialCtx, c.Addrs()[0]); err != nil {
		t.Fatalf("B could not reach C over Wi-Fi: %v", err)
	}
	for _, p := range a.Peers() {
		if p.ID == c.ID() {
			t.Fatal("A must not have its own link to C for this test")
		}
	}

	// C -> A: Wi-Fi to B, then Bluetooth to A
	if _, err := c.SendChat(a.ID(), "m-1", "hello alice, via bob"); err != nil {
		t.Fatalf("C send: %v", err)
	}
	m := waitForMessage(t, a, "m-1")
	if m.From != c.ID() || m.Via != b.ID() || m.Handle != "carol" || m.Content != "hello alice, via bob" {
		t.Fatalf("unexpected message at A: %+v", m)
	}
	if r := waitForReceipt(t, c, "m-1", "delivered"); r.From != a.ID() {
		t.Fatalf("delivery receipt from %s, want A", r.From)
	}

	// A -> C: Bluetooth to B, then Wi-Fi to C; then A reads C's message
	if _, err := a.SendChat(c.ID(), "m-2", "hi carol"); err != nil {
		t.Fatalf("A send: %v", err)
	}
	if m := waitForMessage(t, c, "m-2"); m.From != a.ID() || m.Via != b.ID() || m.Handle != "alice" {
		t.Fatalf("unexpected message at C: %+v", m)
	}
	waitForReceipt(t, a, "m-2", "delivered")

	if err := a.SendReceipt(c.ID(), []string{"m-1"}, "read"); err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	waitForReceipt(t, c, "m-1", "read")

	// B only relayed; none of it was addressed to B
	if got := b.Store().Since(-1); len(got) != 0 {
		t.Fatalf("relay B should deliver nothing, got %+v", got)
	}
}

func postJSON(t *testing.T, url string, body any, out any) {
	t.Helper()
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: %d %s", url, resp.StatusCode, msg)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode %s: %v", url, err)
		}
	}
}

func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

// The app drives the Bluetooth bridge over HTTP exactly like this: report
// neighbours, long-poll the outbox, pass each frame's data (base64 in JSON)
// to the other phone, which posts it to its inbox.
func TestBluetoothBridgeOverHTTP(t *testing.T) {
	a := startMeshNode(t, 9121, 5121, "alice")
	b := startMeshNode(t, 9122, 5122, "bob")
	apiA, apiB := "http://127.0.0.1:9121", "http://127.0.0.1:9122"

	postJSON(t, apiA+"/bt/peers", map[string]any{"peers": []string{b.ID()}}, nil)
	postJSON(t, apiB+"/bt/peers", map[string]any{"peers": []string{a.ID()}}, nil)

	var sent struct{ ID string }
	postJSON(t, apiA+"/message", map[string]string{"peer_id": b.ID(), "content": "over the air", "id": "app-id-1"}, &sent)
	if sent.ID != "app-id-1" {
		t.Fatalf("send returned id %q, want the app's", sent.ID)
	}

	type outbox struct {
		Frames []struct {
			To   string `json:"to"`
			Data string `json:"data"`
		} `json:"frames"`
	}
	carry := func(fromURL, toURL, fromID string) {
		var ob outbox
		getJSON(t, fromURL+"/bt/outbox?wait=5", &ob)
		if len(ob.Frames) == 0 {
			t.Fatal("expected a frame in the outbox")
		}
		for _, f := range ob.Frames {
			postJSON(t, toURL+"/bt/inbox", map[string]string{"from": fromID, "data": f.Data}, nil)
		}
	}
	carry(apiA, apiB, a.ID()) // the message
	var msgs struct {
		Messages []httpapi.ReceivedMessage `json:"messages"`
	}
	// Frames are processed asynchronously after /bt/inbox accepts them
	eventually(t, func() bool {
		getJSON(t, apiB+"/messages?after=-1", &msgs)
		return len(msgs.Messages) > 0
	})
	if len(msgs.Messages) != 1 || msgs.Messages[0].MsgID != "app-id-1" || msgs.Messages[0].Handle != "alice" {
		t.Fatalf("unexpected messages at B: %+v", msgs.Messages)
	}

	carry(apiB, apiA, b.ID()) // B's automatic delivery receipt
	postJSON(t, apiB+"/receipts", map[string]any{"peer_id": a.ID(), "ids": []string{"app-id-1"}, "status": "read"}, nil)
	carry(apiB, apiA, b.ID())

	var rs struct {
		Receipts []httpapi.ReceivedReceipt `json:"receipts"`
	}
	statuses := map[string]bool{}
	eventually(t, func() bool {
		getJSON(t, apiA+"/receipts?after=-1", &rs)
		for _, r := range rs.Receipts {
			if r.From == b.ID() && len(r.IDs) == 1 && r.IDs[0] == "app-id-1" {
				statuses[r.Status] = true
			}
		}
		return statuses["delivered"] && statuses["read"]
	})
	if !statuses["delivered"] || !statuses["read"] {
		t.Fatalf("want delivered and read receipts at A, got %+v", rs.Receipts)
	}

	// An empty outbox long-poll returns an empty list, not null
	var empty outbox
	getJSON(t, apiA+"/bt/outbox?wait=0", &empty)
	if empty.Frames == nil || len(empty.Frames) != 0 {
		t.Fatalf("empty outbox should be [], got %+v", empty.Frames)
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return // the caller's assertions report what's missing
		}
		time.Sleep(50 * time.Millisecond)
	}
}
