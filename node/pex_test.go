package node

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func randomPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPexUsableAddresses(t *testing.T) {
	x := newPexState(false)
	for addr, want := range map[string]bool{
		"/ip4/192.168.43.12/tcp/4001": true,
		"/ip4/10.0.0.7/tcp/4001":      true,
		"/ip4/127.0.0.1/tcp/4001":     false,
		"/ip4/0.0.0.0/tcp/4001":       false,
		"/ip4/169.254.10.1/tcp/4001":  false,
		"/ip4/224.0.0.251/tcp/4001":   false,
		"/ip6/fe80::1/tcp/4001":       false,
		"/ip6/::1/tcp/4001":           false,
		"/ip4/192.168.43.12/udp/4001": false,
	} {
		if got := x.usable(multiaddr.StringCast(addr)); got != want {
			t.Errorf("usable(%s) = %v, want %v", addr, got, want)
		}
	}
	if !newPexState(true).usable(multiaddr.StringCast("/ip4/127.0.0.1/tcp/4001")) {
		t.Error("loopback should be usable when tests allow it")
	}
}

// A neighbour's list is trimmed and cleaned before anything is kept: never
// ourselves, the sender, addresses for another peer, or unreachable ones.
func TestPexIngestIsBoundedAndSanitised(t *testing.T) {
	svc, err := Start(Config{DataDir: t.TempDir(), HTTPPort: 9141, P2PPort: 5141, DisableMDNS: true})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Stop()

	from := randomPeer(t)
	stranger := randomPeer(t)
	good := randomPeer(t)
	list := []pexEntry{
		{ID: svc.ID(), Addrs: []string{"/ip4/10.1.2.3/tcp/4001"}},
		{ID: from.String(), Addrs: []string{"/ip4/10.1.2.4/tcp/4001"}},
		{ID: randomPeer(t).String(), Addrs: []string{"/ip4/127.0.0.1/tcp/4001", "/ip6/fe80::1/tcp/4001"}},
		{ID: randomPeer(t).String(), Addrs: []string{"/ip4/10.1.2.5/tcp/4001/p2p/" + stranger.String()}},
		{ID: good.String(), Addrs: []string{
			"/ip4/127.0.0.1/tcp/4001", "/ip4/10.1.2.6/tcp/4001/p2p/" + good.String(),
			"/ip4/10.1.2.6/tcp/4002", "/ip4/10.1.2.6/tcp/4003", "/ip4/10.1.2.6/tcp/4004", "/ip4/10.1.2.6/tcp/4005",
		}},
	}
	for i := 0; i < 3*pexMaxPeers; i++ {
		list = append(list, pexEntry{ID: randomPeer(t).String(), Addrs: []string{fmt.Sprintf("/ip4/10.9.0.%d/tcp/4001", i)}})
	}

	// A different subnet from ours, so nothing is dialled
	svc.ingestPex(from, multiaddr.StringCast("/ip4/192.168.1.2/tcp/5141"), list)

	got := map[string][]string{}
	for _, d := range svc.Discovered() {
		got[d.ID] = d.Addrs
		if d.Via != from.String() {
			t.Errorf("%s via %s, want %s", d.ID, d.Via, from)
		}
	}
	// The five entries above, then only enough padding to reach the batch limit
	if want := 1 + pexMaxPeers - 5; len(got) != want {
		t.Fatalf("kept %d peers, want %d: %v", len(got), want, got)
	}
	if _, ok := got[svc.ID()]; ok {
		t.Error("kept ourselves")
	}
	if _, ok := got[from.String()]; ok {
		t.Error("kept the sender")
	}
	if addrs := got[good.String()]; len(addrs) != pexMaxAddrs || addrs[0] != "/ip4/10.1.2.6/tcp/4001/p2p/"+good.String() {
		t.Errorf("good peer's addresses = %v", addrs)
	}
}
