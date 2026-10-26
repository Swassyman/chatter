package processing

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// slowCarry keeps the carry loop out of the way, so whatever a joiner gets
// came from a history answer.
func slowCarry(t *testing.T) {
	t.Helper()
	old := carryCheckEvery
	carryCheckEvery = time.Hour
	t.Cleanup(func() { carryCheckEvery = old })
}

// idleNode is a node whose inbox the test reads itself.
func idleNode(t *testing.T, h *hub) (*Processor, *fakeNet) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := peer.IDFromPrivateKey(priv)
	fn := h.add(id)
	p, err := New(Config{Net: fn, Key: priv})
	if err != nil {
		t.Fatal(err)
	}
	return p, fn
}

// postsBy has a send n posts to room, which b (linked to a) receives.
func postsBy(t *testing.T, a, b *Processor, room string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if err := a.SendRoom(room, fmt.Sprintf("update %d", i)); err != nil {
			t.Fatal(err)
		}
		expect(t, b)
	}
}

// A posts to #triage while B listens. D turns up later, joins, and gets
// what A said from B, in order, still A's.
func TestJoinerCatchesUpOnRoomHistory(t *testing.T) {
	slowCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	d, fd := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	postsBy(t, a, b, "triage", 3)
	if err := a.SendRoom("other", "not asked for"); err != nil {
		t.Fatal(err)
	}
	expect(t, b)

	h.link(fd.id, fb.id)
	d.SubscribeRoom("triage")
	for i := 1; i <= 3; i++ {
		m := expect(t, d)
		if !m.History || m.From != a.ID() || m.Room != "triage" || m.Content != fmt.Sprintf("update %d", i) {
			t.Fatalf("post %d: bad history message %+v", i, m)
		}
	}
	expectNothing(t, d)
}

// A busy room hands over its last 30 posts, no more.
func TestHistoryAnswerIsCapped(t *testing.T) {
	slowCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	d, fd := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	postsBy(t, a, b, "triage", historyLimit+10)

	h.link(fd.id, fb.id)
	d.SubscribeRoom("triage")
	for i := 11; i <= historyLimit+10; i++ {
		if m := expect(t, d); m.Content != fmt.Sprintf("update %d", i) {
			t.Fatalf("want update %d, got %+v", i, m)
		}
	}
	expectNothing(t, d)
}

// B answers with one post it altered and one it made up in A's name. Only
// the genuine post gets through: B can't change what A said.
func TestForgedHistoryIsRejected(t *testing.T) {
	slowCarry(t)
	h := newHub()
	a, _ := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	d, fd := newNode(t, h, nil)
	h.link(fd.id, fb.id)
	d.SubscribeRoom("triage") // asks B, which has nothing yet

	signed := func(id, content string) *Envelope {
		env := &Envelope{ID: id, Type: TypeChat, From: a.ID(), Room: "triage", Timestamp: time.Now().UnixMilli(), TTL: 8, Payload: []byte(content)}
		sb, _ := env.signingBytes()
		env.Sig, _ = a.key.Sign(sb)
		return env
	}
	genuine := signed("h-ok", "shelter open")
	altered := signed("h-alt", "shelter open")
	altered.Payload = []byte("shelter closed, go home")
	madeUp := &Envelope{ID: "h-fake", Type: TypeChat, From: a.ID(), Room: "triage", Timestamp: time.Now().UnixMilli(), TTL: 8, Payload: []byte("trust b")}
	madeUp.Sig, _ = b.key.Sign([]byte("anything"))

	var raws []json.RawMessage
	for _, e := range []*Envelope{altered, madeUp, genuine} {
		data, _ := Encode(e)
		raws = append(raws, data)
	}
	body, _ := json.Marshal(historyResponse{Room: "triage", Envelopes: raws})
	if err := b.sendToNeighbour(fd.id, TypeHistoryResponse, body); err != nil {
		t.Fatal(err)
	}
	if m := expect(t, d); m.ID != "h-ok" || m.Content != "shelter open" {
		t.Fatalf("want only the genuine post, got %+v", m)
	}
	expectNothing(t, d)
}

// History nobody asked for, or for a room we're not in, is ignored.
func TestUnaskedHistoryIsIgnored(t *testing.T) {
	slowCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	d, fd := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	h.link(fd.id, fb.id)
	postsBy(t, a, b, "triage", 1)
	expect(t, d) // the live post, relayed by B

	raws := []json.RawMessage{}
	for _, e := range b.history.recent("triage", 0, historyLimit) {
		data, _ := Encode(e)
		raws = append(raws, data)
	}
	body, _ := json.Marshal(historyResponse{Room: "triage", Envelopes: raws})
	if err := b.sendToNeighbour(fd.id, TypeHistoryResponse, body); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, d)
}

// A neighbour is answered at most once a minute per room, and asks no more
// often than that either.
func TestHistoryRequestsAreRateLimited(t *testing.T) {
	slowCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	d, fd := idleNode(t, h) // reads its own inbox
	h.link(fa.id, fb.id)
	h.link(fd.id, fb.id)
	postsBy(t, a, b, "triage", 2)

	ask, _ := json.Marshal(historyRequest{Room: "triage", Limit: historyLimit})
	for i := 0; i < 2; i++ {
		if err := d.sendToNeighbour(fb.id, TypeHistoryRequest, ask); err != nil {
			t.Fatal(err)
		}
	}
	answers := 0
	deadline := time.After(500 * time.Millisecond)
	for done := false; !done; {
		select {
		case m := <-fd.msgs:
			if env, err := Decode(m.Data); err == nil && env.Type == TypeHistoryResponse {
				answers++
			}
		case <-deadline:
			done = true
		}
	}
	if answers != 1 {
		t.Fatalf("want 1 answer to 2 quick requests, got %d", answers)
	}

	// Asking: once, then not again within the minute
	d.requestHistory(fb.id, "triage")
	if d.history.due(d.history.asked, fb.id.String()+"|triage") {
		t.Fatal("would ask the same neighbour again within a minute")
	}
}

// A private room's posts are kept sealed by a relay without the key, and
// opened by a member who joins later.
func TestPrivateRoomHistoryThroughRelayWithoutKey(t *testing.T) {
	slowCarry(t)
	h := newHub()
	a, fa := newNode(t, h, nil)
	b, fb := newNode(t, h, nil)
	d, fd := newNode(t, h, nil)
	h.link(fa.id, fb.id)
	tag, err := a.JoinPrivateRoom("medics", "correct horse")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SendRoom(tag, "insulin in fridge 2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // B relays and keeps it, unread
	expectNothing(t, b)

	h.link(fd.id, fb.id)
	if _, err := d.JoinPrivateRoom("medics", "correct horse"); err != nil {
		t.Fatal(err)
	}
	d.SubscribeRoom(tag)
	if m := expect(t, d); m.Content != "insulin in fridge 2" || !m.History {
		t.Fatalf("bad history message %+v", m)
	}
}

// History requests are for one neighbour: a relay never passes them on.
func TestHistoryRequestsAreNotRelayed(t *testing.T) {
	slowCarry(t)
	h := newHub()
	_, fb := newNode(t, h, nil)
	c, fc := idleNode(t, h)
	d, fd := idleNode(t, h)
	h.link(fd.id, fb.id)
	h.link(fb.id, fc.id)

	ask, _ := json.Marshal(historyRequest{Room: "triage", Limit: historyLimit})
	env, _ := d.newEnvelope("", TypeHistoryRequest, c.ID(), "", ask)
	env.TTL = 5
	data, _ := Encode(env)
	if err := fd.Send(fb.id, data); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-fc.msgs:
		t.Fatalf("relayed to C: %d bytes", len(m.Data))
	case <-time.After(300 * time.Millisecond):
	}
}
