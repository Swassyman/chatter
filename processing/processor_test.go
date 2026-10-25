package processing

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Swassyman/chatter/transport"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ---- in-memory fake network: lets us test multi-hop without real sockets ----

type hub struct {
	mu    sync.Mutex
	nodes map[peer.ID]*fakeNet
	links map[peer.ID]map[peer.ID]bool
}

type fakeNet struct {
	id   peer.ID
	hub  *hub
	msgs chan transport.Message
}

func newHub() *hub {
	return &hub{nodes: map[peer.ID]*fakeNet{}, links: map[peer.ID]map[peer.ID]bool{}}
}

func (h *hub) add(id peer.ID) *fakeNet {
	f := &fakeNet{id: id, hub: h, msgs: make(chan transport.Message, 32)}
	h.nodes[id] = f
	h.links[id] = map[peer.ID]bool{}
	return f
}

func (h *hub) link(a, b peer.ID) { h.links[a][b] = true; h.links[b][a] = true }

func (f *fakeNet) PeerID() peer.ID                              { return f.id }
func (f *fakeNet) Messages() <-chan transport.Message           { return f.msgs }
func (f *fakeNet) Close() error                                 { return nil }
func (f *fakeNet) Connect(context.Context, peer.AddrInfo) error { return nil }
func (f *fakeNet) Peers() []peer.ID {
	f.hub.mu.Lock()
	defer f.hub.mu.Unlock()
	var out []peer.ID
	for id := range f.hub.links[f.id] {
		out = append(out, id)
	}
	return out
}
func (f *fakeNet) Send(to peer.ID, data []byte) error {
	f.hub.mu.Lock()
	ok := f.hub.links[f.id][to]
	dst := f.hub.nodes[to]
	f.hub.mu.Unlock()
	if !ok {
		return errors.New("not connected")
	}
	dst.msgs <- transport.Message{From: f.id, Data: append([]byte(nil), data...)}
	return nil
}

func newNode(t *testing.T, h *hub, c Cipher) (*Processor, *fakeNet) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	fn := h.add(id)
	p, err := New(Config{Net: fn, Key: priv, Cipher: c})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	t.Cleanup(p.Stop)
	return p, fn
}

func expect(t *testing.T, p *Processor) ChatMessage {
	t.Helper()
	select {
	case m := <-p.Incoming():
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return ChatMessage{}
	}
}

func expectNothing(t *testing.T, p *Processor) {
	t.Helper()
	select {
	case m := <-p.Incoming():
		t.Fatalf("unexpected message: %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

// A -- B -- C : A is not connected to C, so B must relay.
func TestMultiHopDirectMessage(t *testing.T) {
	h := newHub()
	key, _ := NewAESGCM(KeyFromPassphrase("test"))
	a, fa := newNode(t, h, key)
	b, fb := newNode(t, h, key)
	c, fc := newNode(t, h, key)
	h.link(fa.id, fb.id)
	h.link(fb.id, fc.id)

	if err := a.SendTo(c.ID(), "hello C"); err != nil {
		t.Fatal(err)
	}
	m := expect(t, c)
	if m.Content != "hello C" || m.From != a.ID() {
		t.Fatalf("bad message: %+v", m)
	}
	expectNothing(t, b) // B relayed but the message was not for B
}

func TestBroadcastDeliveredOnceEvenWithLoop(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	h.link(fb.id, fc.id)
	h.link(fc.id, fa.id) // triangle => flooding loops without dedup

	if err := a.SendRoom("general", "hi all"); err != nil {
		t.Fatal(err)
	}
	expect(t, b)
	expect(t, c)
	expectNothing(t, b)
	expectNothing(t, c)
	expectNothing(t, a)
}

func TestTamperedMessageRejected(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	h.link(fa.id, fb.id)

	env := &Envelope{ID: "x1", Type: TypeChat, From: a.ID(), To: b.ID(), TTL: 3, Payload: []byte("legit")}
	sb, _ := env.signingBytes()
	env.Sig, _ = a.key.Sign(sb)
	env.Payload = []byte("tampered")
	data, _ := Encode(env)
	fb.msgs <- transport.Message{From: fa.id, Data: data}
	expectNothing(t, b)
}

func TestNoPeers(t *testing.T) {
	h := newHub()
	a, _ := newNode(t, h, nil)
	if err := a.SendRoom("r", "x"); !errors.Is(err, ErrNoPeers) {
		t.Fatalf("want ErrNoPeers, got %v", err)
	}
}
