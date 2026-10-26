package processing

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Swassyman/chatter/transport"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ---- in-memory fake network: lets us test multi-hop without real sockets ----

type hub struct {
	mu     sync.Mutex
	nodes  map[peer.ID]*fakeNet
	links  map[peer.ID]map[peer.ID]bool
	broken map[[2]peer.ID]bool // listed as neighbours, but sends fail
}

type fakeNet struct {
	id   peer.ID
	hub  *hub
	msgs chan transport.Message
}

func newHub() *hub {
	return &hub{nodes: map[peer.ID]*fakeNet{}, links: map[peer.ID]map[peer.ID]bool{}, broken: map[[2]peer.ID]bool{}}
}

func (h *hub) add(id peer.ID) *fakeNet {
	f := &fakeNet{id: id, hub: h, msgs: make(chan transport.Message, 32)}
	h.nodes[id] = f
	h.links[id] = map[peer.ID]bool{}
	return f
}

// link locks: the carry loop reads links (via Peers) while tests change them.
func (h *hub) link(a, b peer.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.links[a][b] = true
	h.links[b][a] = true
}

// brokenLink makes a and b look connected while every send between them fails,
// like a Wi-Fi link that died without closing.
func (h *hub) brokenLink(a, b peer.ID) {
	h.link(a, b)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broken[[2]peer.ID{a, b}] = true
	h.broken[[2]peer.ID{b, a}] = true
}

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
	ok := f.hub.links[f.id][to] && !f.hub.broken[[2]peer.ID{f.id, to}]
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

// Alone, a room message is kept rather than lost, and goes out to whoever
// turns up first; they pass it on like any room message.
func TestRoomMessageCarriedToLateArrivals(t *testing.T) {
	fastCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)

	if err := a.SendRoom("general", "water at the gym"); !errors.Is(err, ErrQueued) {
		t.Fatalf("want ErrQueued, got %v", err)
	}
	h.link(fb.id, fc.id)
	h.link(fa.id, fb.id)
	for _, p := range []*Processor{b, c} {
		if m := expect(t, p); m.Room != "general" || m.Content != "water at the gym" {
			t.Fatalf("bad room message: %+v", m)
		}
	}
	expectNothing(t, b)
}

// A ~ X ~ B ~ C ~ D. A and D share a private room; X only listens on the
// wire, B relays without the key, C has the name but the wrong passphrase.
func TestPrivateRoomOnlyReadableWithPassphrase(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)
	d, fd := newNode(t, h, nil)
	priv, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	xid, _ := peer.IDFromPrivateKey(priv)
	fx := h.add(xid)
	h.link(fa.id, fx.id)
	h.link(fa.id, fb.id)
	h.link(fb.id, fc.id)
	h.link(fc.id, fd.id)

	tag, err := a.JoinPrivateRoom("legal-aid", "blue heron 42")
	if err != nil {
		t.Fatal(err)
	}
	if dTag, _ := d.JoinPrivateRoom("legal-aid", "blue heron 42"); dTag != tag {
		t.Fatalf("same name and passphrase gave different tags: %s, %s", tag, dTag)
	}
	if cTag, _ := c.JoinPrivateRoom("legal-aid", "wrong guess"); cTag == tag {
		t.Fatal("a different passphrase gave the same room")
	}

	if _, err := a.SendRoomWithID(tag, "", "lawyer at tent 7 until 6pm"); err != nil {
		t.Fatal(err)
	}
	if m := expect(t, d); m.Content != "lawyer at tent 7 until 6pm" || m.Room != tag {
		t.Fatalf("bad private room message: %+v", m)
	}
	expectNothing(t, b)
	expectNothing(t, c)

	// On the wire: neither the room's name nor the message
	select {
	case raw := <-fx.msgs:
		if bytes.Contains(raw.Data, []byte("legal-aid")) || bytes.Contains(raw.Data, []byte("tent 7")) {
			t.Fatalf("private room leaked on the wire: %s", raw.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("observer saw nothing")
	}
}

func TestRoomNamesAndMembershipChecked(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	_, fb := newNode(t, h, nil)
	h.link(fa.id, fb.id)

	for _, bad := range []string{"", "General", "two words", "-x", strings.Repeat("a", 33)} {
		if _, err := a.SendRoomWithID(bad, "", "hi"); err == nil {
			t.Errorf("room %q was accepted", bad)
		}
	}
	_, tag, err := PrivateRoomKey("food-shelter", "pass")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.SendRoomWithID(tag, "", "hi"); !errors.Is(err, ErrUnknownRoom) {
		t.Fatalf("posting to a private room without its key: %v", err)
	}
	if _, _, err := PrivateRoomKey("food-shelter", ""); err == nil {
		t.Error("a private room without a passphrase was allowed")
	}
}

func expectReceipt(t *testing.T, p *Processor) Receipt {
	t.Helper()
	select {
	case r := <-p.Receipts():
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for receipt")
		return Receipt{}
	}
}

// A -- B -- C: C confirms delivery automatically, the receipt is relayed
// back to A, and A can tell which message it was about.
func TestDeliveryAndReadReceiptsAcrossRelay(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	_, fb := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	h.link(fb.id, fc.id)

	id, err := a.SendDirectWithID(fc.id, "msg-1", "hi C")
	if err != nil || id != "msg-1" {
		t.Fatalf("send: id=%q err=%v", id, err)
	}
	expect(t, c)

	r := expectReceipt(t, a)
	if r.From != c.ID() || r.Status != ReceiptDelivered || len(r.IDs) != 1 || r.IDs[0] != "msg-1" {
		t.Fatalf("bad delivery receipt: %+v", r)
	}
	if r.Via != fb.id.String() {
		t.Fatalf("receipt should arrive via B, got via %s", r.Via)
	}

	if err := c.SendReceipt(a.ID(), []string{"msg-1"}, ReceiptRead); err != nil {
		t.Fatal(err)
	}
	if r := expectReceipt(t, a); r.Status != ReceiptRead || r.IDs[0] != "msg-1" {
		t.Fatalf("bad read receipt: %+v", r)
	}
}

func TestMessageCarriesSignedHandleAndHop(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	_, fb := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	h.link(fb.id, fc.id)

	a.SetHandle("alice")
	if err := a.SendTo(c.ID(), "hello"); err != nil {
		t.Fatal(err)
	}
	m := expect(t, c)
	if m.Handle != "alice" {
		t.Fatalf("handle = %q, want alice", m.Handle)
	}
	if m.From != a.ID() || m.Via != fb.id.String() {
		t.Fatalf("from=%s via=%s, want from A via B", m.From, m.Via)
	}
}

// A's direct link to C looks up but is dead: the message must still get to
// C through B rather than fail.
func TestDeadDirectLinkFallsBackToRelay(t *testing.T) {
	h := newHub()
	a, fa := newNode(t, h, nil)
	_, fb := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	h.link(fb.id, fc.id)
	h.brokenLink(fa.id, fc.id)

	if err := a.SendTo(c.ID(), "still arrives"); err != nil {
		t.Fatal(err)
	}
	if m := expect(t, c); m.Content != "still arrives" || m.Via != fb.id.String() {
		t.Fatalf("bad message: %+v", m)
	}
}

func (h *hub) unlink(a, b peer.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.links[a], b)
	delete(h.links[b], a)
}

func fastCarry(t *testing.T) {
	t.Helper()
	old := carryCheckEvery
	carryCheckEvery = 20 * time.Millisecond
	t.Cleanup(func() { carryCheckEvery = old })
}

// A is alone, so the message waits on A and goes out once C turns up.
func TestQueuedUntilRecipientInRange(t *testing.T) {
	fastCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	c, fc := newNode(t, h, nil)

	err := a.SendTo(c.ID(), "you there?")
	var q interface{ Queued() bool }
	if !errors.As(err, &q) || !q.Queued() {
		t.Fatalf("want a queued error, got %v", err)
	}

	h.link(fa.id, fc.id)
	if m := expect(t, c); m.Content != "you there?" || m.From != a.ID() {
		t.Fatalf("bad message: %+v", m)
	}
	expectNothing(t, c) // handed over once, not on every check
}

// A gives the message to B and leaves; B meets C later and delivers it.
// The sender doesn't need to be around: the mesh carries the message.
func TestRelayCarriesMessageAfterSenderLeaves(t *testing.T) {
	fastCarry(t)
	h := newHub()
	key, _ := NewAESGCM(KeyFromPassphrase("test"))
	a, fa := newNode(t, h, key)
	b, fb := newNode(t, h, key)
	c, fc := newNode(t, h, key)
	h.link(fa.id, fb.id)

	if err := a.SendTo(c.ID(), "meet at the gate"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // B receives and keeps it
	h.unlink(fa.id, fb.id)

	h.link(fb.id, fc.id)
	if m := expect(t, c); m.Content != "meet at the gate" || m.From != a.ID() {
		t.Fatalf("bad message: %+v", m)
	}
	expectNothing(t, b) // B only carried it

	// C reconnecting to another carrier later doesn't deliver it again
	h.link(fa.id, fc.id)
	expectNothing(t, c)
}
