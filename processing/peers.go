package processing

import (
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerInfo is one row of the peer table (peerId, lastSeen).
type PeerInfo struct {
	ID       peer.ID
	LastSeen time.Time
}

type peerTable struct {
	mu sync.RWMutex
	m  map[peer.ID]PeerInfo
}

func newPeerTable() *peerTable { return &peerTable{m: make(map[peer.ID]PeerInfo)} }

func (t *peerTable) Touch(id peer.ID) {
	t.mu.Lock()
	t.m[id] = PeerInfo{ID: id, LastSeen: time.Now()}
	t.mu.Unlock()
}

func (t *peerTable) List() []PeerInfo {
	t.mu.RLock()
	out := make([]PeerInfo, 0, len(t.m))
	for _, p := range t.m {
		out = append(out, p)
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
