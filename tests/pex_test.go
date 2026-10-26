package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	httpapi "github.com/Swassyman/chatter/api/http"
	"github.com/Swassyman/chatter/node"
)

func startPexNode(t *testing.T, httpPort, p2pPort int) *node.NodeService {
	t.Helper()
	svc, err := node.Start(node.Config{
		DataDir: t.TempDir(), HTTPPort: httpPort, P2PPort: p2pPort, DisableMDNS: true, PexLoopback: true,
	})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { svc.Stop() })
	return svc
}

func connected(svc *node.NodeService, id string) bool {
	for _, p := range svc.Peers() {
		if p.ID == id {
			return true
		}
	}
	return false
}

// A ---Wi-Fi--- B ---Wi-Fi--- C, with no discovery of their own: B tells each
// about the other, they connect directly, and the app can list who it heard of.
func TestPeerExchangeLinksNeighboursOfNeighbours(t *testing.T) {
	a := startPexNode(t, 9131, 5131)
	b := startPexNode(t, 9132, 5132)
	c := startPexNode(t, 9133, 5133)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.ConnectPeer(ctx, b.Addrs()[0]); err != nil {
		t.Fatalf("A to B: %v", err)
	}
	if err := c.ConnectPeer(ctx, b.Addrs()[0]); err != nil {
		t.Fatalf("C to B: %v", err)
	}

	// Announce delay, plus the higher peer ID's wait if the lower can't dial
	deadline := time.Now().Add(15 * time.Second)
	for !(connected(a, c.ID()) && connected(c, a.ID())) {
		if time.Now().After(deadline) {
			t.Fatalf("A and C never linked; A has %+v, C has %+v", a.Peers(), c.Peers())
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The app sees who told whom, with addresses it can dial later
	resp, err := http.Get("http://127.0.0.1:9131/peers/discovered")
	if err != nil {
		t.Fatalf("GET /peers/discovered: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Peers []httpapi.DiscoveredPeer `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found *httpapi.DiscoveredPeer
	for i, p := range body.Peers {
		if p.ID == a.ID() {
			t.Fatal("A was told about itself")
		}
		if p.ID == c.ID() {
			found = &body.Peers[i]
		}
	}
	if found == nil {
		t.Fatalf("A's discovered list lacks C: %+v", body.Peers)
	}
	if found.Via != b.ID() || len(found.Addrs) == 0 {
		t.Fatalf("C should come via B with addresses, got %+v", *found)
	}
}
