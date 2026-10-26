package processing

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Room history catch-up. Someone who joins a channel, or walks into range of
// a neighbour while in one, asks each neighbour for the room's recent posts:
//
//	D --history_req{room, since, limit}--> B
//	D <--history_res{room, [envelopes]}--- B
//
// B answers with the original envelopes exactly as their authors signed
// them, and D checks every signature before taking any, so B can't forge or
// alter what anyone said. Requests and responses go to direct neighbours
// only (never relayed or carried), at most once a minute per neighbour and
// room, so a phone joining can't flood the radio.
const (
	TypeHistoryRequest  MsgType = "history_req"
	TypeHistoryResponse MsgType = "history_res"

	historyLimit  = 30              // posts per room kept, and per answer
	historyWindow = 12 * time.Hour  // nothing older is kept or taken
	maxHistRooms  = 32              // rooms kept; the quietest makes way
	maxHistChunk  = 128 * 1024      // bytes of envelopes per response
	historyFuture = 5 * time.Minute // clock skew allowed on a post's time
	answerWithin  = 2 * time.Minute // a response must follow our request
)

// How often each neighbour may be asked (or is answered) per room.
var historyEvery = 60 * time.Second

type historyRequest struct {
	Room  string `json:"room"`
	Since int64  `json:"since"` // unix millis
	Limit int    `json:"limit"`
}

type historyResponse struct {
	Room      string            `json:"room"`
	Envelopes []json.RawMessage `json:"envelopes"`
}

type roomHistory struct {
	mu     sync.Mutex
	posts  map[string][]*Envelope // per room, oldest first
	subs   map[string]bool        // rooms the app is in
	asked  map[string]time.Time   // neighbour|room -> when we asked
	served map[string]time.Time   // neighbour|room -> when we answered
	handed *seenCache             // posts already handed over from history
}

func newRoomHistory() *roomHistory {
	return &roomHistory{
		posts:  map[string][]*Envelope{},
		subs:   map[string]bool{},
		asked:  map[string]time.Time{},
		served: map[string]time.Time{},
		handed: newSeenCache(historyWindow, 5000),
	}
}

// record keeps a room post for answering others: every one seen, read or
// only relayed (a private room's stays sealed; only its members can open it).
func (h *roomHistory) record(env *Envelope) {
	if env.Type != TypeChat || env.Room == "" || env.To != "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	list := h.posts[env.Room]
	for _, e := range list {
		if e.ID == env.ID {
			return
		}
	}
	if list == nil && len(h.posts) >= maxHistRooms {
		h.dropQuietestRoom()
	}
	list = append(list, env)
	// Carried posts arrive late, so keep the list in the order they were written
	sort.SliceStable(list, func(i, j int) bool { return list[i].Timestamp < list[j].Timestamp })
	if len(list) > historyLimit {
		list = list[len(list)-historyLimit:]
	}
	h.posts[env.Room] = list
}

// dropQuietestRoom forgets the room whose latest post is oldest. Caller holds mu.
func (h *roomHistory) dropQuietestRoom() {
	quietest, at := "", int64(0)
	for room, list := range h.posts {
		last := list[len(list)-1].Timestamp
		if quietest == "" || last < at {
			quietest, at = room, last
		}
	}
	delete(h.posts, quietest)
}

// recent returns up to limit of room's posts written since since and within
// historyWindow, oldest first.
func (h *roomHistory) recent(room string, since int64, limit int) []*Envelope {
	if floor := time.Now().Add(-historyWindow).UnixMilli(); since < floor {
		since = floor
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*Envelope
	for _, e := range h.posts[room] {
		if e.Timestamp >= since {
			out = append(out, e)
		}
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// due reports whether key's last time in times is at least historyEvery ago,
// and if so stamps it now.
func (h *roomHistory) due(times map[string]time.Time, key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	if last, ok := times[key]; ok && now.Sub(last) < historyEvery {
		return false
	}
	times[key] = now
	return true
}

func (h *roomHistory) subscribed() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.subs))
	for room := range h.subs {
		out = append(out, room)
	}
	return out
}

func (h *roomHistory) isSubscribed(room string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.subs[room]
}

// askedRecently reports whether we asked neighbour about room within answerWithin.
func (h *roomHistory) askedRecently(neighbour peer.ID, room string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	at, ok := h.asked[neighbour.String()+"|"+room]
	return ok && time.Since(at) < answerWithin
}

// ---------- Processor API ----------

// SubscribeRoom tells the processor the app is in room (an open room's name
// or a joined private room's tag), and asks every neighbour for its recent
// posts. Neighbours met later are asked too.
func (p *Processor) SubscribeRoom(room string) {
	if !validRoomField(room) {
		return
	}
	p.history.mu.Lock()
	p.history.subs[room] = true
	p.history.mu.Unlock()
	for _, n := range p.net.Peers() {
		go p.requestHistory(n, room)
	}
}

// UnsubscribeRoom stops catching up on room. Its posts are still relayed.
func (p *Processor) UnsubscribeRoom(room string) {
	p.history.mu.Lock()
	delete(p.history.subs, room)
	p.history.mu.Unlock()
}

// requestHistoryFrom asks a newly met neighbour about every room we're in.
func (p *Processor) requestHistoryFrom(n peer.ID) {
	for _, room := range p.history.subscribed() {
		p.requestHistory(n, room)
	}
}

func (p *Processor) requestHistory(n peer.ID, room string) {
	if !p.history.due(p.history.asked, n.String()+"|"+room) {
		return
	}
	body, err := json.Marshal(historyRequest{
		Room:  room,
		Since: time.Now().Add(-historyWindow).UnixMilli(),
		Limit: historyLimit,
	})
	if err != nil {
		return
	}
	if err := p.sendToNeighbour(n, TypeHistoryRequest, body); err != nil {
		log.Printf("processing: history request to %s: %v", n, err)
	}
}

// sendToNeighbour sends a one-hop envelope: never relayed, never carried.
func (p *Processor) sendToNeighbour(n peer.ID, typ MsgType, body []byte) error {
	env, err := p.newEnvelope("", typ, n.String(), "", body)
	if err != nil {
		return err
	}
	env.TTL = 1
	data, err := Encode(env)
	if err != nil {
		return err
	}
	return p.net.Send(n, data)
}

// handleHistory takes a request or response sent to us. Only from the
// neighbour that wrote it: these never travel further than one hop.
func (p *Processor) handleHistory(env *Envelope, payload []byte, via peer.ID) {
	if env.To != p.self.String() || env.From != via.String() {
		return
	}
	switch env.Type {
	case TypeHistoryRequest:
		var req historyRequest
		if err := json.Unmarshal(payload, &req); err != nil || !validRoomField(req.Room) {
			return
		}
		p.answerHistory(via, req)
	case TypeHistoryResponse:
		var res historyResponse
		if err := json.Unmarshal(payload, &res); err != nil {
			return
		}
		p.takeHistory(via, res)
	}
}

func (p *Processor) answerHistory(to peer.ID, req historyRequest) {
	if !p.history.due(p.history.served, to.String()+"|"+req.Room) {
		return
	}
	limit := req.Limit
	if limit <= 0 || limit > historyLimit {
		limit = historyLimit
	}
	posts := p.history.recent(req.Room, req.Since, limit)
	var chunk []json.RawMessage
	size := 0
	flush := func() {
		if len(chunk) == 0 {
			return
		}
		body, err := json.Marshal(historyResponse{Room: req.Room, Envelopes: chunk})
		if err == nil {
			err = p.sendToNeighbour(to, TypeHistoryResponse, body)
		}
		if err != nil {
			log.Printf("processing: history for %s: %v", to, err)
		}
		chunk, size = nil, 0
	}
	for _, e := range posts {
		if e.From == to.String() {
			continue // they have their own posts
		}
		data, err := Encode(e)
		if err != nil {
			continue
		}
		if size+len(data) > maxHistChunk {
			flush()
		}
		chunk = append(chunk, data)
		size += len(data)
	}
	flush()
}

// takeHistory checks every post in a response we asked for and hands the
// genuine, new ones to the app as if they had just arrived.
func (p *Processor) takeHistory(from peer.ID, res historyResponse) {
	if !p.history.isSubscribed(res.Room) || !p.history.askedRecently(from, res.Room) {
		return // unasked-for history isn't taken
	}
	if len(res.Envelopes) > historyLimit {
		res.Envelopes = res.Envelopes[len(res.Envelopes)-historyLimit:]
	}
	now := time.Now()
	oldest, newest := now.Add(-historyWindow).UnixMilli(), now.Add(historyFuture).UnixMilli()
	for _, raw := range res.Envelopes {
		env, err := Decode(raw)
		if err != nil {
			continue
		}
		// The author's own signature, so the neighbour can't have written or changed it
		if err := verify(env); err != nil {
			log.Printf("processing: drop history %s from %s: %v", env.ID, from, err)
			continue
		}
		if env.Type != TypeChat || env.To != "" || env.Room != res.Room ||
			env.Timestamp < oldest || env.Timestamp > newest {
			continue
		}
		p.history.record(env)
		p.seen.Seen(env.ID) // a live copy arriving later is a repeat
		if env.From == p.self.String() || p.history.handed.Seen(env.ID) {
			continue
		}
		p.deliverWith(env, from, true)
	}
}
