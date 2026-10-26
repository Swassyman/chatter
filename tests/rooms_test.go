package tests

import (
	"context"
	"testing"
	"time"

	httpapi "github.com/Swassyman/chatter/api/http"
	"github.com/Swassyman/chatter/node"
)

// A ~~Bluetooth~~ B ---Wi-Fi--- C, over the HTTP API the app uses. A room
// post reaches everyone across both kinds of link, relays included; a private
// room's post reaches only those who joined with the passphrase, while B still
// relays it.
func TestRoomsAcrossBluetoothAndWiFi(t *testing.T) {
	a := startMeshNode(t, 9151, 5151, "alice")
	b := startMeshNode(t, 9152, 5152, "bob")
	c := startMeshNode(t, 9153, 5153, "carol")

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

	// Open room: C posts, A (two hops away) and B (the relay) both read it
	var sent struct{ Status, ID string }
	postJSON(t, "http://127.0.0.1:9153/message",
		map[string]string{"room": "general", "content": "water at the gym", "id": "r-1"}, &sent)
	if sent.Status != "sent" || sent.ID != "r-1" {
		t.Fatalf("unexpected send response %+v", sent)
	}
	for name, svc := range map[string]*node.NodeService{"A": a, "B": b} {
		m := waitForMessage(t, svc, "r-1")
		if m.Room != "general" || m.From != c.ID() || m.Handle != "carol" || m.Content != "water at the gym" {
			t.Fatalf("%s got %+v", name, m)
		}
	}

	// Private room: A and C join with the passphrase, B doesn't
	var joined struct{ Room string }
	postJSON(t, "http://127.0.0.1:9151/rooms", map[string]string{"name": "legal-aid", "passphrase": "blue heron"}, &joined)
	tag := joined.Room
	postJSON(t, "http://127.0.0.1:9153/rooms", map[string]string{"name": "legal-aid", "passphrase": "blue heron"}, &joined)
	if joined.Room != tag || tag == "legal-aid" {
		t.Fatalf("private room tags: A %q, C %q", tag, joined.Room)
	}

	postJSON(t, "http://127.0.0.1:9151/message", map[string]string{"room": tag, "content": "lawyer at tent 7", "id": "r-2"}, nil)
	if m := waitForMessage(t, c, "r-2"); m.Room != tag || m.Content != "lawyer at tent 7" || m.Via != b.ID() {
		t.Fatalf("C got %+v", m)
	}
	time.Sleep(300 * time.Millisecond)
	for _, m := range b.Store().Since(-1) {
		if m.MsgID == "r-2" {
			t.Fatalf("B read a private room it never joined: %+v", m)
		}
	}

	// The app reads the room off GET /messages
	var got struct{ Messages []httpapi.ReceivedMessage }
	getJSON(t, "http://127.0.0.1:9151/messages", &got)
	found := false
	for _, m := range got.Messages {
		found = found || (m.MsgID == "r-1" && m.Room == "general")
	}
	if !found {
		t.Fatalf("GET /messages on A lacks the room post: %+v", got.Messages)
	}
}
