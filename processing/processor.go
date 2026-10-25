// Package processing is the "Processing layer" from the architecture:
// message handling, peer management, gossip/multi-hop relay.
//
//	Application layer  <-->  Processor  <-->  transport.Transport (libp2p / BLE)
package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Swassyman/chatter"
	"github.com/Swassyman/chatter/transport"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

var ErrNoPeers = errors.New("no connected peers to send to")

// ErrQueued means no one is in range, so the message was kept and will go out
// when someone is. HTTP callers recognise it by its Queued method (they can't
// import this package without a cycle).
var ErrQueued error = queuedError{}

type queuedError struct{}

func (queuedError) Error() string { return "no one in range; the message will be sent when someone is" }
func (queuedError) Queued() bool  { return true }

// Store, carry, forward: an addressed message that can't be handed straight to
// its recipient is kept and offered to every neighbour that turns up later,
// so it hops on until it meets them. Every phone does this, sender and relays
// alike; there is no server to hold mail.
const (
	carryFor   = 24 * time.Hour
	maxCarried = 500
)

// How often to look for neighbours that arrived since the last look.
var carryCheckEvery = 3 * time.Second

type carried struct {
	env   *Envelope
	until time.Time
}

// Network is what the processor needs from the transport layer.
// *transport.Libp2pTransport already satisfies it; a future BLE transport
// only has to implement the same methods.
type Network interface {
	PeerID() peer.ID
	Send(to peer.ID, data []byte) error
	Messages() <-chan transport.Message
	Connect(ctx context.Context, info peer.AddrInfo) error
	Peers() []peer.ID
	Close() error
}

var _ Network = (*transport.Libp2pTransport)(nil)

type Config struct {
	Net    Network        // required
	Key    crypto.PrivKey // required: this node's libp2p private key (signs messages)
	Cipher Cipher         // optional: end-to-end payload encryption
	Store  MessageStore   // optional: defaults to in-memory
	TTL    int            // optional: hop limit, default DefaultTTL
}

type Processor struct {
	net    Network
	key    crypto.PrivKey
	cipher Cipher
	store  MessageStore
	ttl    int
	self   peer.ID

	seen  *seenCache
	peers *peerTable

	carryMu sync.Mutex
	carry   map[string]carried
	known   map[peer.ID]bool // neighbours at the last carry check; carry loop only

	handleMu sync.RWMutex
	handle   string

	incoming  chan ChatMessage
	receipts  chan Receipt
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func New(cfg Config) (*Processor, error) {
	if cfg.Net == nil {
		return nil, errors.New("processing: Net is required")
	}
	if cfg.Key == nil {
		return nil, errors.New("processing: Key is required")
	}
	self := cfg.Net.PeerID()
	keyID, err := peer.IDFromPrivateKey(cfg.Key)
	if err != nil {
		return nil, err
	}
	if keyID != self {
		return nil, errors.New("processing: Key does not belong to the transport's peer ID")
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.TTL <= 0 || cfg.TTL > MaxTTL {
		cfg.TTL = DefaultTTL
	}
	return &Processor{
		net:    cfg.Net,
		key:    cfg.Key,
		cipher: cfg.Cipher,
		store:  cfg.Store,
		ttl:    cfg.TTL,
		self:   self,
		// Remembered as long as messages are carried, so a copy that comes back
		// round later is still recognised and not delivered twice
		seen:     newSeenCache(carryFor, 10000),
		carry:    map[string]carried{},
		known:    map[peer.ID]bool{},
		peers:    newPeerTable(),
		incoming: make(chan ChatMessage, 64),
		receipts: make(chan Receipt, 64),
		done:     make(chan struct{}),
	}, nil
}

// ---------- lifecycle ----------

// Start begins reading from the transport. Call once.
func (p *Processor) Start() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.done:
				return
			case m, ok := <-p.net.Messages():
				if !ok {
					return
				}
				p.handleRaw(m)
			}
		}
	}()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(carryCheckEvery)
		defer t.Stop()
		for {
			select {
			case <-p.done:
				return
			case <-t.C:
				p.offerCarried()
			}
		}
	}()
}

// Stop halts the processor. It does not close the transport (the caller owns it).
func (p *Processor) Stop() {
	p.closeOnce.Do(func() { close(p.done) })
	p.wg.Wait()
}

// ---------- API used by the application layer ----------

func (p *Processor) ID() string { return p.self.String() }

// Incoming delivers messages addressed to this node (or broadcast).
func (p *Processor) Incoming() <-chan ChatMessage { return p.incoming }

// Receipts delivers delivery/read confirmations for messages we sent.
func (p *Processor) Receipts() <-chan Receipt { return p.receipts }

// SetHandle sets the display name carried (and signed) in what we send.
func (p *Processor) SetHandle(handle string) {
	p.handleMu.Lock()
	p.handle = handle
	p.handleMu.Unlock()
}

func (p *Processor) currentHandle() string {
	p.handleMu.RLock()
	defer p.handleMu.RUnlock()
	return p.handle
}

// History returns the last `limit` stored messages (0 = all).
func (p *Processor) History(limit int) ([]ChatMessage, error) { return p.store.List(limit) }

// Peers returns the peer table, refreshed with currently connected neighbours.
func (p *Processor) Peers() []PeerInfo {
	for _, id := range p.net.Peers() {
		p.peers.Touch(id)
	}
	return p.peers.List()
}

// SendTo is SendDirect with a string peer ID (what the HTTP API has).
func (p *Processor) SendTo(peerID, content string) error {
	id, err := peer.Decode(peerID)
	if err != nil {
		return fmt.Errorf("invalid peer id: %w", err)
	}
	return p.SendDirect(id, content)
}

// SendDirect sends to one peer; if not a direct neighbour it is relayed via others.
func (p *Processor) SendDirect(to peer.ID, content string) error {
	_, err := p.SendDirectWithID(to, "", content)
	return err
}

// SendDirectWithID is SendDirect with a caller-chosen message ID (generated
// when empty), so the caller can match receipts to its own copy. It returns
// the ID used.
func (p *Processor) SendDirectWithID(to peer.ID, id, content string) (string, error) {
	if to == p.self {
		return "", errors.New("cannot send a message to yourself")
	}
	return p.publish(id, to.String(), "", content)
}

// SendRoom broadcasts to everyone reachable (gossip flood, TTL-limited).
func (p *Processor) SendRoom(room, content string) error {
	_, err := p.publish("", "", room, content)
	return err
}

// SendReceipt tells peer `to` that we received or read the given messages.
// Like any message it is relayed when `to` is not a direct neighbour.
func (p *Processor) SendReceipt(to string, ids []string, status string) error {
	target, err := peer.Decode(to)
	if err != nil {
		return fmt.Errorf("invalid peer id: %w", err)
	}
	if target == p.self {
		return errors.New("cannot send a receipt to yourself")
	}
	if len(ids) == 0 {
		return errors.New("no message ids")
	}
	if status != ReceiptDelivered && status != ReceiptRead {
		return fmt.Errorf("unknown receipt status %q", status)
	}
	body, err := json.Marshal(receiptPayload{IDs: ids, Status: status})
	if err != nil {
		return err
	}
	env, err := p.newEnvelope("", TypeReceipt, to, "", body)
	if err != nil {
		return err
	}
	return p.route(env, "")
}

// ConnectPeer dials a full multiaddr such as /ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...
func (p *Processor) ConnectPeer(ctx context.Context, address string) error {
	info, err := peer.AddrInfoFromString(address)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}
	if info.ID == p.self {
		return errors.New("cannot connect to yourself")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.net.Connect(ctx, *info); err != nil {
		return err
	}
	p.peers.Touch(info.ID)
	return nil
}

// ---------- outbound ----------

func (p *Processor) publish(id, to, room, content string) (string, error) {
	if content == "" {
		return "", errors.New("empty message")
	}
	env, err := p.newEnvelope(id, TypeChat, to, room, []byte(content))
	if err != nil {
		return "", err
	}
	_ = p.store.Save(ChatMessage{
		ID: env.ID, From: env.From, To: to, Room: room, Content: content,
		Timestamp: time.UnixMilli(env.Timestamp), Outgoing: true, Handle: env.Handle,
	})
	return env.ID, p.route(env, "")
}

// newEnvelope builds and signs an envelope authored by this node.
func (p *Processor) newEnvelope(id string, typ MsgType, to, room string, payload []byte) (*Envelope, error) {
	if p.cipher != nil {
		var err error
		if payload, err = p.cipher.Encrypt(payload); err != nil {
			return nil, fmt.Errorf("encrypt: %w", err)
		}
	}
	if id == "" {
		var err error
		if id, err = newID(); err != nil {
			return nil, err
		}
	} else if len(id) > 64 {
		return nil, errors.New("message id longer than 64 characters")
	}
	env := &Envelope{
		ID:        id,
		Type:      typ,
		From:      p.self.String(),
		To:        to,
		Room:      room,
		Handle:    p.currentHandle(),
		Timestamp: time.Now().UnixMilli(),
		TTL:       p.ttl,
		Payload:   payload,
	}
	sb, err := env.signingBytes()
	if err != nil {
		return nil, err
	}
	if env.Sig, err = p.key.Sign(sb); err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	p.seen.Seen(env.ID) // so our own message is ignored if it echoes back
	return env, nil
}

// route sends env to its target if that is a direct neighbour, otherwise floods
// it to every neighbour except `exclude` and the original author. If the direct
// hop fails, it falls back to flooding so other paths can still carry it.
func (p *Processor) route(env *Envelope, exclude peer.ID) error {
	data, err := Encode(env)
	if err != nil {
		return err
	}
	if len(data) > chatter.MAX_MESSAGE_SIZE {
		return errors.New("message too large")
	}
	neighbours := p.net.Peers()

	var failedDirect peer.ID
	if env.To != "" {
		if target, err := peer.Decode(env.To); err == nil {
			for _, n := range neighbours {
				if n == target {
					err := p.net.Send(target, data) // direct hop
					if err == nil {
						p.forget(env.ID)
						return nil
					}
					log.Printf("processing: direct send to %s failed (%v); relaying instead", target, err)
					failedDirect = target
					break
				}
			}
		}
	}

	if env.To != "" {
		p.keep(env)
	}

	sent := 0
	for _, n := range neighbours {
		if n == p.self || n == exclude || n == failedDirect || n.String() == env.From {
			continue
		}
		sent++
		go func(n peer.ID) {
			if err := p.net.Send(n, data); err != nil {
				log.Printf("processing: send to %s failed: %v", n, err)
			}
		}(n)
	}
	if sent == 0 {
		if env.To != "" {
			return ErrQueued
		}
		return ErrNoPeers
	}
	return nil
}

func (p *Processor) keep(env *Envelope) {
	p.carryMu.Lock()
	defer p.carryMu.Unlock()
	if _, ok := p.carry[env.ID]; ok {
		return
	}
	if len(p.carry) >= maxCarried { // make room by dropping the one closest to expiring
		var oldest string
		for id, c := range p.carry {
			if oldest == "" || c.until.Before(p.carry[oldest].until) {
				oldest = id
			}
		}
		delete(p.carry, oldest)
	}
	p.carry[env.ID] = carried{env: env, until: time.Now().Add(carryFor)}
}

func (p *Processor) forget(id string) {
	p.carryMu.Lock()
	delete(p.carry, id)
	p.carryMu.Unlock()
}

// offerCarried hands every kept message to neighbours that weren't connected
// at the last check. A message handed to its own recipient is done.
// shortcut: carriers other than the one that met the recipient keep their copy
// until it expires (the recipient drops repeats); add delivery gossip if
// carried traffic ever matters.
func (p *Processor) offerCarried() {
	current := map[peer.ID]bool{}
	var fresh []peer.ID
	for _, n := range p.net.Peers() {
		current[n] = true
		if !p.known[n] {
			fresh = append(fresh, n)
		}
	}
	p.known = current
	if len(fresh) == 0 {
		return
	}

	now := time.Now()
	var envs []*Envelope
	p.carryMu.Lock()
	for id, c := range p.carry {
		if now.After(c.until) {
			delete(p.carry, id)
			continue
		}
		envs = append(envs, c.env)
	}
	p.carryMu.Unlock()

	for _, env := range envs {
		data, err := Encode(env)
		if err != nil {
			continue
		}
		for _, n := range fresh {
			if n.String() == env.From {
				continue
			}
			if p.net.Send(n, data) == nil && n.String() == env.To {
				p.forget(env.ID)
			}
		}
	}
}

// ---------- inbound ----------

func (p *Processor) handleRaw(m transport.Message) {
	env, err := Decode(m.Data)
	if err != nil {
		log.Printf("processing: drop from %s: %v", m.From, err)
		return
	}
	if err := verify(env); err != nil {
		log.Printf("processing: drop %s from %s: %v", env.ID, m.From, err)
		return
	}
	if p.seen.Seen(env.ID) {
		return // duplicate
	}
	p.peers.Touch(m.From)

	self := p.self.String()
	if env.To == "" || env.To == self {
		p.deliver(env, m.From)
	}
	if env.To != self && env.TTL > 1 {
		fwd := *env
		fwd.TTL--
		go func() {
			if err := p.route(&fwd, m.From); err != nil && !errors.Is(err, ErrNoPeers) && !errors.Is(err, ErrQueued) {
				log.Printf("processing: relay %s: %v", fwd.ID, err)
			}
		}()
	}
}

func verify(env *Envelope) error {
	pid, err := peer.Decode(env.From)
	if err != nil {
		return fmt.Errorf("bad sender id: %w", err)
	}
	pub, err := pid.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("cannot get sender key: %w", err)
	}
	sb, err := env.signingBytes()
	if err != nil {
		return err
	}
	ok, err := pub.Verify(sb, env.Sig)
	if err != nil || !ok {
		return errors.New("invalid signature")
	}
	return nil
}

func (p *Processor) deliver(env *Envelope, via peer.ID) {
	payload := env.Payload
	if p.cipher != nil {
		pt, err := p.cipher.Decrypt(payload)
		if err != nil {
			log.Printf("processing: cannot decrypt %s: %v", env.ID, err)
			return
		}
		payload = pt
	}

	if env.Type == TypeReceipt {
		var r receiptPayload
		if err := json.Unmarshal(payload, &r); err != nil || len(r.IDs) == 0 {
			log.Printf("processing: bad receipt %s from %s: %v", env.ID, env.From, err)
			return
		}
		select {
		case p.receipts <- Receipt{From: env.From, IDs: r.IDs, Status: r.Status, Via: via.String()}:
		default:
			log.Printf("processing: receipt buffer full, dropped %s", env.ID)
		}
		return
	}

	msg := ChatMessage{
		ID: env.ID, From: env.From, To: env.To, Room: env.Room,
		Content: string(payload), Timestamp: time.UnixMilli(env.Timestamp),
		Handle: env.Handle, Via: via.String(),
	}
	_ = p.store.Save(msg)
	select {
	case p.incoming <- msg:
	default:
		log.Printf("processing: incoming buffer full, dropped %s", env.ID)
	}

	// Tell the author it arrived. Only for messages addressed to us; nobody
	// expects receipts for room broadcasts.
	if env.To == p.self.String() {
		go func() {
			if err := p.SendReceipt(env.From, []string{env.ID}, ReceiptDelivered); err != nil && !errors.Is(err, ErrNoPeers) {
				log.Printf("processing: delivery receipt for %s: %v", env.ID, err)
			}
		}()
	}
}
