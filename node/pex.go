package node

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	api "github.com/Swassyman/chatter/api/http"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// Peer exchange: neighbours tell each other who else they hold a Wi-Fi link
// to, so with A--B--C, A and C learn each other's addresses (and connect, if
// they share B's network) without anyone scanning a code.
const (
	pexProtocol = "/chatter/pex/1.0.0"

	pexInterval  = 30 * time.Second // every neighbour hears our list this often
	pexMinGap    = 5 * time.Second  // at most one list to, or from, a peer this often
	pexMaxPeers  = 16               // peers per list, sent or taken
	pexMaxAddrs  = 4                // addresses per peer
	pexMaxStored = 256              // learned peers kept for the app
	pexRemember  = 30 * time.Minute // how long a learned peer is kept

	pexRedialAfter = time.Minute // gap between opportunistic dials to one peer
	pexDialTimeout = 8 * time.Second
	pexYieldFor    = 8 * time.Second // the higher peer ID waits this long for the lower to dial
	pexMaxDialing  = 4               // opportunistic dials at once

	// Identify has to finish before a new neighbour's listen port is known
	pexAnnounceDelay = 2 * time.Second
)

type pexEntry struct {
	ID    string   `json:"id"`
	Addrs []string `json:"addrs"`
}

type learnedPeer struct {
	addrs []multiaddr.Multiaddr
	via   peer.ID
	seen  time.Time
}

type pexState struct {
	mu        sync.Mutex
	learned   map[peer.ID]learnedPeer
	lastSent  map[peer.ID]time.Time
	lastHeard map[peer.ID]time.Time
	lastDial  map[peer.ID]time.Time
	dialSlots chan struct{}
	loopback  bool // share and dial 127.0.0.1 (tests run every node on one machine)
}

func newPexState(loopback bool) *pexState {
	return &pexState{
		learned:   make(map[peer.ID]learnedPeer),
		lastSent:  make(map[peer.ID]time.Time),
		lastHeard: make(map[peer.ID]time.Time),
		lastDial:  make(map[peer.ID]time.Time),
		dialSlots: make(chan struct{}, pexMaxDialing),
		loopback:  loopback,
	}
}

// usable reports whether addr is worth sharing or dialling: IPv4 over TCP,
// and not loopback, link-local, multicast or unspecified.
func (x *pexState) usable(addr multiaddr.Multiaddr) bool {
	ipStr, err := addr.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		return false // IPv6 or no IP at all
	}
	if _, err := addr.ValueForProtocol(multiaddr.P_TCP); err != nil {
		return false
	}
	ip := net.ParseIP(ipStr)
	switch {
	case ip == nil, ip.IsUnspecified(), ip.IsMulticast(), ip.IsLinkLocalUnicast():
		return false
	case ip.IsLoopback():
		return x.loopback
	}
	return true
}

// pexAddrs are the addresses others can reach p on: the IP its connection to
// us comes from, with the port it listens on. On Android 11+ a node only
// knows its loopback address, so what it reports about itself is no use, but
// its port still is.
func (s *NodeService) pexAddrs(p peer.ID) []multiaddr.Multiaddr {
	known := s.host.Peerstore().Addrs(p)
	ports := map[string]bool{}
	for _, a := range known {
		if port, err := a.ValueForProtocol(multiaddr.P_TCP); err == nil {
			ports[port] = true
		}
	}
	if len(ports) == 0 {
		ports[strconv.Itoa(s.p2pPort)] = true // every phone listens on the same port by default
	}

	seen := map[string]bool{}
	var out []multiaddr.Multiaddr
	add := func(a multiaddr.Multiaddr) {
		if len(out) >= pexMaxAddrs || seen[a.String()] || !s.pex.usable(a) {
			return
		}
		seen[a.String()] = true
		out = append(out, a)
	}
	for _, c := range s.host.Network().ConnsToPeer(p) {
		remote := c.RemoteMultiaddr()
		if c.Stat().Direction == network.DirOutbound {
			add(remote) // we dialled it there, so it listens there
		}
		ip, err := remote.ValueForProtocol(multiaddr.P_IP4)
		if err != nil {
			continue
		}
		for port := range ports {
			if a, err := multiaddr.NewMultiaddr("/ip4/" + ip + "/tcp/" + port); err == nil {
				add(a)
			}
		}
	}
	for _, a := range known {
		add(a)
	}
	return out
}

// pexList is who we can tell [to] about: our other Wi-Fi neighbours, at most
// pexMaxPeers of them, picked at random so that over a few rounds everyone in
// a crowded network gets passed on.
func (s *NodeService) pexList(to peer.ID) []pexEntry {
	peers := s.host.Network().Peers()
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	var list []pexEntry
	for _, p := range peers {
		if len(list) >= pexMaxPeers {
			break
		}
		if p == to || p == s.host.ID() {
			continue
		}
		addrs := s.pexAddrs(p)
		if len(addrs) == 0 {
			continue
		}
		entry := pexEntry{ID: p.String()}
		for _, a := range addrs {
			entry.Addrs = append(entry.Addrs, a.String())
		}
		list = append(list, entry)
	}
	return list
}

func (s *NodeService) sendPex(p peer.ID) {
	s.pex.mu.Lock()
	if time.Since(s.pex.lastSent[p]) < pexMinGap {
		s.pex.mu.Unlock()
		return
	}
	s.pex.lastSent[p] = time.Now()
	s.pex.mu.Unlock()

	list := s.pexList(p)
	if len(list) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := s.host.NewStream(ctx, p, pexProtocol)
	if err != nil {
		return // an older build without peer exchange, or the link just dropped
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(stream).Encode(list); err != nil {
		log.Printf("Peer exchange to %s failed: %v", p, err)
	}
}

// announcePex tells every neighbour who we're connected to now, e.g. after
// someone new joined.
func (s *NodeService) announcePex() {
	for _, p := range s.host.Network().Peers() {
		go s.sendPex(p)
	}
}

func (s *NodeService) pexLoop(ctx context.Context) {
	t := time.NewTicker(pexInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.announcePex()
		}
	}
}

func (s *NodeService) handlePexStream(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
	from := stream.Conn().RemotePeer()

	s.pex.mu.Lock()
	tooSoon := time.Since(s.pex.lastHeard[from]) < pexMinGap
	if !tooSoon {
		s.pex.lastHeard[from] = time.Now()
	}
	s.pex.mu.Unlock()
	if tooSoon {
		return
	}

	var list []pexEntry
	if err := json.NewDecoder(io.LimitReader(stream, 64<<10)).Decode(&list); err != nil {
		log.Printf("Bad peer exchange from %s: %v", from, err)
		return
	}
	s.ingestPex(from, stream.Conn().LocalMultiaddr(), list)
}

// ingestPex records the peers [from] told us about, and dials the ones on the
// network we share with it ([local] is our end of that link).
func (s *NodeService) ingestPex(from peer.ID, local multiaddr.Multiaddr, list []pexEntry) {
	if len(list) > pexMaxPeers {
		list = list[:pexMaxPeers]
	}
	now := time.Now()
	for _, e := range list {
		id, err := peer.Decode(e.ID)
		if err != nil || id == s.host.ID() || id == from {
			continue
		}
		var addrs []multiaddr.Multiaddr
		for _, str := range e.Addrs {
			if len(addrs) >= pexMaxAddrs {
				break
			}
			a, err := multiaddr.NewMultiaddr(str)
			if err != nil {
				continue
			}
			// Addresses arrive with or without /p2p/<id>; keep the transport part
			if transportPart, last := multiaddr.SplitLast(a); last != nil && last.Protocol().Code == multiaddr.P_P2P {
				if last.Value() != id.String() {
					continue // someone else's address
				}
				a = transportPart
			}
			if s.pex.usable(a) {
				addrs = append(addrs, a)
			}
		}
		if len(addrs) == 0 {
			continue
		}

		s.pex.mu.Lock()
		s.pex.learned[id] = learnedPeer{addrs: addrs, via: from, seen: now}
		s.pex.mu.Unlock()
		s.host.Peerstore().AddAddrs(id, addrs, pexRemember)

		if s.host.Network().Connectedness(id) != network.Connected && sameSubnet(local, addrs) {
			go s.pexDial(id, addrs)
		}
	}
	s.prunePex(now)
}

func (s *NodeService) prunePex(now time.Time) {
	s.pex.mu.Lock()
	defer s.pex.mu.Unlock()
	for id, l := range s.pex.learned {
		if now.Sub(l.seen) > pexRemember {
			delete(s.pex.learned, id)
		}
	}
	if len(s.pex.learned) <= pexMaxStored {
		return
	}
	ids := make([]peer.ID, 0, len(s.pex.learned))
	for id := range s.pex.learned {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return s.pex.learned[ids[i]].seen.Before(s.pex.learned[ids[j]].seen) })
	for _, id := range ids[:len(ids)-pexMaxStored] {
		delete(s.pex.learned, id)
	}
}

// pexDial connects to a peer we heard about. Both sides usually hear about
// each other at once, so the lower peer ID dials and the higher only steps in
// if no link has appeared after a while.
func (s *NodeService) pexDial(id peer.ID, addrs []multiaddr.Multiaddr) {
	s.pex.mu.Lock()
	if time.Since(s.pex.lastDial[id]) < pexRedialAfter {
		s.pex.mu.Unlock()
		return
	}
	s.pex.lastDial[id] = time.Now()
	s.pex.mu.Unlock()

	if s.host.ID() > id {
		select {
		case <-time.After(pexYieldFor):
		case <-s.done:
			return
		}
		if s.host.Network().Connectedness(id) == network.Connected {
			return
		}
	}

	select {
	case s.pex.dialSlots <- struct{}{}:
		defer func() { <-s.pex.dialSlots }()
	case <-s.done:
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), pexDialTimeout)
	defer cancel()
	if err := s.host.Connect(ctx, peer.AddrInfo{ID: id, Addrs: addrs}); err != nil {
		log.Printf("Peer exchange: could not reach %s: %v", id, err)
		return
	}
	log.Printf("Peer exchange: connected to %s", id)
}

// sameSubnet reports whether any of addrs is on the /24 of local, i.e. on the
// Wi-Fi network or hotspot we share with the peer that told us about it.
func sameSubnet(local multiaddr.Multiaddr, addrs []multiaddr.Multiaddr) bool {
	mine, err := local.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		return false
	}
	myNet := net.ParseIP(mine).Mask(net.CIDRMask(24, 32))
	for _, a := range addrs {
		ip, err := a.ValueForProtocol(multiaddr.P_IP4)
		if err == nil && net.ParseIP(ip).Mask(net.CIDRMask(24, 32)).Equal(myNet) {
			return true
		}
	}
	return false
}

// Discovered lists the peers neighbours have told us about recently, newest
// first, with the addresses they gave and who passed them on.
func (s *NodeService) Discovered() []api.DiscoveredPeer {
	s.pex.mu.Lock()
	defer s.pex.mu.Unlock()
	out := make([]api.DiscoveredPeer, 0, len(s.pex.learned))
	for id, l := range s.pex.learned {
		if time.Since(l.seen) > pexRemember {
			continue
		}
		d := api.DiscoveredPeer{ID: id.String(), Via: l.via.String(), Seen: l.seen}
		for _, a := range l.addrs {
			d.Addrs = append(d.Addrs, a.String()+"/p2p/"+id.String())
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seen.After(out[j].Seen) })
	return out
}
