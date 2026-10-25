// Package processing is the "Processing layer" from the architecture:
// message handling, peer management, gossip/multi-hop relay.
//
//	Application layer  <-->  Processor  <-->  transport.Transport (libp2p / BLE)
package processing

import (
	"context"
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

	incoming  chan ChatMessage
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
		net:      cfg.Net,
		key:      cfg.Key,
		cipher:   cfg.Cipher,
		store:    cfg.Store,
		ttl:      cfg.TTL,
		self:     self,
		seen:     newSeenCache(10*time.Minute, 10000),
		peers:    newPeerTable(),
		incoming: make(chan ChatMessage, 64),
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
	if to == p.self {
		return errors.New("cannot send a message to yourself")
	}
	return p.publish(to.String(), "", content)
}

// SendRoom broadcasts to everyone reachable (gossip flood, TTL-limited).
func (p *Processor) SendRoom(room, content string) error {
	return p.publish("", room, content)
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

func (p *Processor) publish(to, room, content string) error {
	if content == "" {
		return errors.New("empty message")
	}
	payload := []byte(content)
	if p.cipher != nil {
		var err error
		if payload, err = p.cipher.Encrypt(payload); err != nil {
			return fmt.Errorf("encrypt: %w", err)
		}
	}
	id, err := newID()
	if err != nil {
		return err
	}
	env := &Envelope{
		ID:        id,
		Type:      TypeChat,
		From:      p.self.String(),
		To:        to,
		Room:      room,
		Timestamp: time.Now().UnixMilli(),
		TTL:       p.ttl,
		Payload:   payload,
	}
	sb, err := env.signingBytes()
	if err != nil {
		return err
	}
	if env.Sig, err = p.key.Sign(sb); err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	p.seen.Seen(env.ID) // so our own message is ignored if it echoes back
	_ = p.store.Save(ChatMessage{
		ID: env.ID, From: env.From, To: to, Room: room, Content: content,
		Timestamp: time.UnixMilli(env.Timestamp), Outgoing: true,
	})
	return p.route(env, "")
}

// route sends env to its target if that is a direct neighbour, otherwise floods
// it to every neighbour except `exclude` and the original author.
func (p *Processor) route(env *Envelope, exclude peer.ID) error {
	data, err := Encode(env)
	if err != nil {
		return err
	}
	if len(data) > chatter.MAX_MESSAGE_SIZE {
		return errors.New("message too large")
	}
	neighbours := p.net.Peers()

	if env.To != "" {
		if target, err := peer.Decode(env.To); err == nil {
			for _, n := range neighbours {
				if n == target {
					return p.net.Send(target, data) // direct hop
				}
			}
		}
	}

	sent := 0
	for _, n := range neighbours {
		if n == p.self || n == exclude || n.String() == env.From {
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
		return ErrNoPeers
	}
	return nil
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
		p.deliver(env)
	}
	if env.To != self && env.TTL > 1 {
		fwd := *env
		fwd.TTL--
		go func() {
			if err := p.route(&fwd, m.From); err != nil && !errors.Is(err, ErrNoPeers) {
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

func (p *Processor) deliver(env *Envelope) {
	payload := env.Payload
	if p.cipher != nil {
		pt, err := p.cipher.Decrypt(payload)
		if err != nil {
			log.Printf("processing: cannot decrypt %s: %v", env.ID, err)
			return
		}
		payload = pt
	}
	msg := ChatMessage{
		ID: env.ID, From: env.From, To: env.To, Room: env.Room,
		Content: string(payload), Timestamp: time.UnixMilli(env.Timestamp),
	}
	_ = p.store.Save(msg)
	select {
	case p.incoming <- msg:
	default:
		log.Printf("processing: incoming buffer full, dropped %s", env.ID)
	}
}
