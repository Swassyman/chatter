package transport

import (
	"context"
	"errors"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ErrNotNeighbor is returned when sending over Bluetooth to a peer the app
// has no Bluetooth link to.
var ErrNotNeighbor = errors.New("not a Bluetooth neighbour")

// Frame is an outgoing message the app must carry over a Bluetooth link.
type Frame struct {
	To   peer.ID
	Data []byte
}

// BluetoothBridge exposes Bluetooth links that live outside Go (in the
// Android app) to the processing layer. The app reports its current
// Bluetooth neighbours, collects frames to send, and hands in frames it
// received.
type BluetoothBridge struct {
	mu        sync.RWMutex
	neighbors map[peer.ID]bool
	outbox    chan Frame
	inbox     chan Message
}

func NewBluetoothBridge() *BluetoothBridge {
	return &BluetoothBridge{
		neighbors: make(map[peer.ID]bool),
		outbox:    make(chan Frame, 256),
		inbox:     make(chan Message, 64),
	}
}

// SetNeighbors replaces the set of peers the app has Bluetooth links to.
func (b *BluetoothBridge) SetNeighbors(ids []peer.ID) {
	n := make(map[peer.ID]bool, len(ids))
	for _, id := range ids {
		n[id] = true
	}
	b.mu.Lock()
	b.neighbors = n
	b.mu.Unlock()
}

func (b *BluetoothBridge) Neighbors() []peer.ID {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]peer.ID, 0, len(b.neighbors))
	for id := range b.neighbors {
		out = append(out, id)
	}
	return out
}

func (b *BluetoothBridge) IsNeighbor(id peer.ID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.neighbors[id]
}

// Send queues data for the app to deliver to Bluetooth neighbour `to`.
func (b *BluetoothBridge) Send(to peer.ID, data []byte) error {
	if !b.IsNeighbor(to) {
		return ErrNotNeighbor
	}
	select {
	case b.outbox <- Frame{To: to, Data: append([]byte(nil), data...)}:
		return nil
	default:
		return errors.New("bluetooth outbox full")
	}
}

// Outgoing returns every queued frame, waiting until there is at least one
// or ctx ends (in which case it returns nil).
func (b *BluetoothBridge) Outgoing(ctx context.Context) []Frame {
	var frames []Frame
	select {
	case f := <-b.outbox:
		frames = append(frames, f)
	case <-ctx.Done():
		return nil
	}
	for {
		select {
		case f := <-b.outbox:
			frames = append(frames, f)
		default:
			return frames
		}
	}
}

// Deliver hands in data the app received from Bluetooth neighbour `from`.
func (b *BluetoothBridge) Deliver(from peer.ID, data []byte) error {
	select {
	case b.inbox <- Message{From: from, Data: data}:
		return nil
	default:
		return errors.New("bluetooth inbox full")
	}
}

func (b *BluetoothBridge) Messages() <-chan Message { return b.inbox }

// MeshNetwork joins the libp2p transport (Wi-Fi) and the Bluetooth bridge
// into the one network the processing layer routes over. A peer reachable
// over either counts as a neighbour; sends prefer Wi-Fi and fall back to
// Bluetooth when Wi-Fi fails.
type MeshNetwork struct {
	wifi      *Libp2pTransport
	bt        *BluetoothBridge
	messages  chan Message
	done      chan struct{}
	closeOnce sync.Once
}

func NewMeshNetwork(wifi *Libp2pTransport, bt *BluetoothBridge) *MeshNetwork {
	m := &MeshNetwork{
		wifi:     wifi,
		bt:       bt,
		messages: make(chan Message, 64),
		done:     make(chan struct{}),
	}
	go m.pump(wifi.Messages())
	go m.pump(bt.Messages())
	return m
}

func (m *MeshNetwork) pump(src <-chan Message) {
	for {
		select {
		case msg, ok := <-src:
			if !ok {
				return
			}
			select {
			case m.messages <- msg:
			case <-m.done:
				return
			}
		case <-m.done:
			return
		}
	}
}

func (m *MeshNetwork) PeerID() peer.ID { return m.wifi.PeerID() }

func (m *MeshNetwork) Messages() <-chan Message { return m.messages }

func (m *MeshNetwork) Connect(ctx context.Context, info peer.AddrInfo) error {
	return m.wifi.Connect(ctx, info)
}

// Peers returns every neighbour over Wi-Fi or Bluetooth, each once.
func (m *MeshNetwork) Peers() []peer.ID {
	seen := make(map[peer.ID]bool)
	var out []peer.ID
	for _, p := range append(m.wifi.Peers(), m.bt.Neighbors()...) {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func (m *MeshNetwork) Send(to peer.ID, data []byte) error {
	var wifiErr error
	if m.wifi.IsConnected(to) {
		if wifiErr = m.wifi.Send(to, data); wifiErr == nil {
			return nil
		}
		// A connected peer that didn't acknowledge is most likely gone (e.g.
		// its Wi-Fi dropped). Close the link so it stops counting as a
		// neighbour, then try Bluetooth.
		_ = m.wifi.Disconnect(to)
	}
	if m.bt.IsNeighbor(to) {
		return m.bt.Send(to, data)
	}
	if wifiErr != nil {
		return wifiErr
	}
	// Not linked at all: let libp2p dial any address it knows for the peer
	return m.wifi.Send(to, data)
}

func (m *MeshNetwork) Close() error {
	m.closeOnce.Do(func() { close(m.done) })
	return nil
}
