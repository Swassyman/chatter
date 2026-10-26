package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Swassyman/chatter"
	api "github.com/Swassyman/chatter/api/http"
	"github.com/Swassyman/chatter/processing"
	"github.com/Swassyman/chatter/transport"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/security/insecure"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
)

var macRegex = regexp.MustCompile(`^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$`)

const (
	// profileProtocol carries a peer's handle, pushed when a connection comes up
	profileProtocol = "/chatter/profile/1.0.0"
	maxHandleLen    = 32

	// Liveness: connected peers are pinged every peerCheckInterval, and a peer
	// that misses peerMaxPingFailures pings in a row is disconnected
	peerCheckInterval   = 3 * time.Second
	peerPingTimeout     = 2500 * time.Millisecond
	peerMaxPingFailures = 2
)

type profile struct {
	Handle string `json:"handle"`
}

// Config defines the startup configuration for a Chatter node.
type Config struct {
	DataDir  string // Directory where identity.key and node state are stored
	HTTPPort int    // Local HTTP API port (default: 8080)
	P2PPort  int    // libp2p listen TCP port (default: 4001)

	// DisableMDNS turns off LAN auto-discovery, e.g. so tests control topology
	DisableMDNS bool

	// PexLoopback lets peer exchange share and dial 127.0.0.1 addresses, for
	// tests that run every node on one machine
	PexLoopback bool
}

// NodeService encapsulates the libp2p host, transport, message store, HTTP server, and mDNS discovery.
type NodeService struct {
	host       host.Host
	transport  *transport.Libp2pTransport
	bridge     *transport.BluetoothBridge
	mesh       *transport.MeshNetwork
	proc       *processing.Processor
	store      *api.MessageStore
	server     *api.Server
	httpServer *http.Server
	mdnsSvc    mdns.Service
	p2pPort    int
	dialLocks  sync.Map
	cancel     context.CancelFunc
	done       chan struct{}
	closeOnce  sync.Once

	profileMu   sync.RWMutex
	handle      string
	peerHandles map[peer.ID]string

	pex *pexState
}

type mdnsNotifee struct {
	h   host.Host
	ctx context.Context
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.h.ID() {
		return
	}
	if n.h.Network().Connectedness(pi.ID) == network.Connected {
		return
	}
	var routable []multiaddr.Multiaddr
	for _, a := range pi.Addrs {
		str := a.String()
		if !strings.Contains(str, "/127.0.0.1/") && !strings.Contains(str, "/::1/") && !strings.Contains(str, "/ip6/") {
			routable = append(routable, a)
		}
	}
	if len(routable) == 0 {
		return
	}
	pi.Addrs = routable
	log.Printf("mDNS discovered local peer: %s with addrs: %v", pi.ID, pi.Addrs)
	if err := n.h.Connect(n.ctx, pi); err != nil {
		log.Printf("mDNS auto-connect note (%s): %v", pi.ID, err)
	} else {
		log.Printf("mDNS auto-connected successfully to peer: %s", pi.ID)
	}
}

func (s *NodeService) ID() string {
	return s.transport.PeerID().String()
}

// Addrs returns clean IPv4 multiaddr strings with the peer ID appended.
// Prioritizes routable LAN/Hotspot addresses first, completely excludes
// link-local IPv6 addresses to avoid unreachable dial attempts, and dynamically
// queries system network interfaces so Wi-Fi/Hotspot changes are immediately reflected.
func (s *NodeService) Addrs() []string {
	hostID := s.transport.PeerID()
	existing := make(map[string]bool)

	var lanAddrs []string
	var loopbackAddrs []string
	var otherAddrs []string

	addAddr := func(a multiaddr.Multiaddr) {
		str := a.String()
		// Strictly filter out IPv6 addresses to prevent unroutable link-local dials
		if strings.Contains(str, "/ip6/") {
			return
		}

		full := fmt.Sprintf("%s/p2p/%s", a, hostID)
		if existing[full] {
			return
		}
		existing[full] = true

		if strings.Contains(str, "/127.0.0.1/") {
			loopbackAddrs = append(loopbackAddrs, full)
		} else if strings.Contains(str, "/192.168.") || strings.Contains(str, "/10.") || strings.Contains(str, "/172.") {
			lanAddrs = append(lanAddrs, full)
		} else {
			otherAddrs = append(otherAddrs, full)
		}
	}

	// 1. Host known listen addresses (TCP)
	for _, a := range s.host.Addrs() {
		addAddr(a)
	}

	// 2. Dynamically scan active network interfaces (e.g. Wi-Fi / Hotspot 192.168.43.1)
	if ifaces, err := net.Interfaces(); err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if !ok || ipnet.IP.To4() == nil {
					continue
				}
				ipStr := ipnet.IP.String()
				if ma, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ipStr, s.p2pPort)); err == nil {
					addAddr(ma)
				}
			}
		}
	}

	result := append(lanAddrs, otherAddrs...)
	if len(result) == 0 {
		return loopbackAddrs
	}
	return append(result, loopbackAddrs...)
}

func (s *NodeService) Peers() []api.Peer {
	peers := s.transport.Peers()
	result := make([]api.Peer, 0, len(peers))
	s.profileMu.RLock()
	defer s.profileMu.RUnlock()
	for _, p := range peers {
		result = append(result, api.Peer{
			ID:     p.String(),
			Handle: s.peerHandles[p],
		})
	}
	return result
}

func (s *NodeService) SendMessage(peerID string, content string) error {
	_, err := s.SendChat(peerID, "", content)
	return err
}

// SendChat sends content to peerID under msgID (generated when empty) and
// returns the ID used. Without a direct Wi-Fi or Bluetooth link, the message
// is relayed through whichever peers are connected.
func (s *NodeService) SendChat(peerID, msgID, content string) (string, error) {
	to, err := peer.Decode(peerID)
	if err != nil {
		return "", err
	}
	if len(content) > chatter.MAX_MESSAGE_SIZE {
		return "", fmt.Errorf("message too large")
	}
	return s.proc.SendDirectWithID(to, msgID, content)
}

// SendRoom posts content to a room (an open room's name, or the tag JoinRoom
// returned for a private one) under msgID (generated when empty), relayed to
// everyone in range and carried for those who come later.
func (s *NodeService) SendRoom(room, msgID, content string) (string, error) {
	if len(content) > chatter.MAX_MESSAGE_SIZE {
		return "", fmt.Errorf("message too large")
	}
	return s.proc.SendRoomWithID(room, msgID, content)
}

// JoinRoom returns the ID a room goes by: an open room's own name, or for a
// private one (passphrase set) the tag derived from its key, which this
// node can then read and post in. Either way neighbours are asked for the
// room's recent posts, and so is everyone met while the app stays in it.
func (s *NodeService) JoinRoom(name, passphrase string) (string, error) {
	room := name
	if passphrase == "" {
		if !processing.ValidRoomName(name) {
			return "", fmt.Errorf("room names are lowercase letters, digits and dashes, up to 32 characters")
		}
	} else {
		var err error
		if room, err = s.proc.JoinPrivateRoom(name, passphrase); err != nil {
			return "", err
		}
	}
	s.proc.SubscribeRoom(room)
	return room, nil
}

// LeaveRoom stops catching up on a room, and stops reading it if private.
// Its posts are still relayed for others, as every room's are.
func (s *NodeService) LeaveRoom(room string) {
	s.proc.UnsubscribeRoom(room)
	if processing.IsPrivateRoom(room) {
		s.proc.LeavePrivateRoom(room)
	}
}

// SendReceipt tells peerID that we received or read the given messages.
func (s *NodeService) SendReceipt(peerID string, msgIDs []string, status string) error {
	return s.proc.SendReceipt(peerID, msgIDs, status)
}

// SetBluetoothPeers records which peers the app currently has Bluetooth links to.
func (s *NodeService) SetBluetoothPeers(ids []string) {
	peers := make([]peer.ID, 0, len(ids))
	for _, id := range ids {
		if pid, err := peer.Decode(id); err == nil {
			peers = append(peers, pid)
		}
	}
	s.bridge.SetNeighbors(peers)
}

// BluetoothOutbox waits (until ctx ends) for frames the app should carry
// over its Bluetooth links.
func (s *NodeService) BluetoothOutbox(ctx context.Context) []api.BridgeFrame {
	frames := s.bridge.Outgoing(ctx)
	out := make([]api.BridgeFrame, 0, len(frames))
	for _, f := range frames {
		out = append(out, api.BridgeFrame{To: f.To.String(), Data: f.Data})
	}
	return out
}

// BluetoothInbox hands the mesh a frame the app received over Bluetooth.
func (s *NodeService) BluetoothInbox(from string, data []byte) error {
	pid, err := peer.Decode(from)
	if err != nil {
		return fmt.Errorf("invalid peer id: %w", err)
	}
	return s.bridge.Deliver(pid, data)
}

// Handle returns this node's user-chosen display name.
func (s *NodeService) Handle() string {
	s.profileMu.RLock()
	defer s.profileMu.RUnlock()
	return s.handle
}

// SetHandle sets this node's display name and pushes it to connected peers.
func (s *NodeService) SetHandle(handle string) error {
	handle = strings.TrimSpace(handle)
	if utf8.RuneCountInString(handle) > maxHandleLen {
		return fmt.Errorf("handle is longer than %d characters", maxHandleLen)
	}
	s.profileMu.Lock()
	s.handle = handle
	s.profileMu.Unlock()
	s.proc.SetHandle(handle) // carried, signed, in every message we send

	for _, p := range s.host.Network().Peers() {
		go s.sendProfile(p)
	}
	return nil
}

func (s *NodeService) sendProfile(p peer.ID) {
	handle := s.Handle()
	if handle == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := s.host.NewStream(ctx, p, profileProtocol)
	if err != nil {
		log.Printf("Profile push to %s failed: %v", p, err)
		return
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(stream).Encode(profile{Handle: handle}); err != nil {
		log.Printf("Profile push to %s failed: %v", p, err)
	}
}

func (s *NodeService) handleProfileStream(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))

	var p profile
	if err := json.NewDecoder(io.LimitReader(stream, 4096)).Decode(&p); err != nil {
		log.Printf("Bad profile from %s: %v", stream.Conn().RemotePeer(), err)
		return
	}
	handle := strings.TrimSpace(p.Handle)
	if handle == "" || utf8.RuneCountInString(handle) > maxHandleLen {
		return
	}
	s.profileMu.Lock()
	s.peerHandles[stream.Conn().RemotePeer()] = handle
	s.profileMu.Unlock()
}

// monitorPeers pings connected peers and closes links that stop answering, so
// a peer whose Wi-Fi went away drops out of /peers within seconds rather than
// lingering until TCP times out.
func (s *NodeService) monitorPeers(ctx context.Context) {
	failures := make(map[peer.ID]int)
	ticker := time.NewTicker(peerCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		peers := s.host.Network().Peers()
		alive := make(map[peer.ID]bool, len(peers))
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, p := range peers {
			wg.Add(1)
			go func(p peer.ID) {
				defer wg.Done()
				pctx, cancel := context.WithTimeout(ctx, peerPingTimeout)
				defer cancel()
				// The channel closes without a result if the ping is cut off by the timeout
				res, ok := <-ping.Ping(pctx, s.host, p)
				mu.Lock()
				alive[p] = ok && res.Error == nil
				mu.Unlock()
			}(p)
		}
		wg.Wait()

		for p := range failures {
			if _, connected := alive[p]; !connected {
				delete(failures, p)
			}
		}
		for p, ok := range alive {
			if ok {
				delete(failures, p)
				continue
			}
			failures[p]++
			if failures[p] >= peerMaxPingFailures {
				log.Printf("Peer %s stopped answering pings; closing connection", p)
				_ = s.host.Network().ClosePeer(p)
				delete(failures, p)
			}
		}
	}
}

// ConnectPeer parses incoming address representations (raw multiaddr, comma-delimited list,
// or JSON payload with {"id": "...", "addrs": [...]}), sanitizes inputs, resets any dial backoff,
// registers addresses in the peerstore, and dials the peer.
func (s *NodeService) ConnectPeer(ctx context.Context, addressInput string) error {
	trimmed := strings.TrimSpace(addressInput)
	trimmed = strings.Trim(trimmed, "\r\n\"'")
	if trimmed == "" {
		return fmt.Errorf("empty peer address")
	}

	// Guard against raw Bluetooth MAC addresses being mistakenly passed as multiaddrs
	if macRegex.MatchString(trimmed) {
		return fmt.Errorf("cannot dial Bluetooth MAC address (%s) over TCP/IP: a multiaddress (e.g. /ip4/.../tcp/4001/p2p/...) is required", trimmed)
	}

	// Strip optional chatter:// URI scheme
	if strings.HasPrefix(trimmed, "chatter://") {
		trimmed = strings.TrimPrefix(trimmed, "chatter://")
	}

	var targetInfo *peer.AddrInfo

	// Check if input is a JSON payload: {"id": "...", "addrs": [...]}
	if strings.HasPrefix(trimmed, "{") {
		var payload struct {
			ID    string   `json:"id"`
			Addrs []string `json:"addrs"`
		}
		if err := json.Unmarshal([]byte(trimmed), &payload); err == nil && payload.ID != "" {
			pid, err := peer.Decode(payload.ID)
			if err == nil {
				info := peer.AddrInfo{ID: pid}
				for _, aStr := range payload.Addrs {
					aStr = strings.TrimSpace(aStr)
					// Exclude any IPv6 addresses from dialing
					if strings.Contains(aStr, "/ip6/") {
						continue
					}
					if ma, err := multiaddr.NewMultiaddr(aStr); err == nil {
						info.Addrs = append(info.Addrs, ma)
					}
				}
				targetInfo = &info
			}
		}
	}

	// Fallback to parsing single or comma-separated multiaddrs
	if targetInfo == nil {
		addrParts := strings.Split(trimmed, ",")
		for _, part := range addrParts {
			part = strings.TrimSpace(part)
			if part == "" || strings.Contains(part, "/ip6/") {
				continue
			}
			ma, err := multiaddr.NewMultiaddr(part)
			if err != nil {
				continue
			}
			info, err := peer.AddrInfoFromP2pAddr(ma)
			if err != nil {
				continue
			}
			if targetInfo == nil {
				targetInfo = info
			} else if targetInfo.ID == info.ID {
				targetInfo.Addrs = append(targetInfo.Addrs, info.Addrs...)
			}
		}
	}

	if targetInfo == nil || len(targetInfo.ID) == 0 {
		return fmt.Errorf("unable to parse valid peer multiaddress from %q", addressInput)
	}

	// 1. Guard against dialing self
	if targetInfo.ID == s.transport.PeerID() {
		return fmt.Errorf("cannot connect to self: scanned address belongs to this node")
	}

	// 2. Filter out loopback addresses if routable LAN/Wi-Fi addresses exist
	var routableAddrs []multiaddr.Multiaddr
	for _, a := range targetInfo.Addrs {
		sStr := a.String()
		if !strings.Contains(sStr, "/127.0.0.1/") && !strings.Contains(sStr, "/::1/") && !strings.Contains(sStr, "/ip6/") {
			routableAddrs = append(routableAddrs, a)
		}
	}
	if len(routableAddrs) > 0 {
		targetInfo.Addrs = routableAddrs
	} else {
		return fmt.Errorf("cannot connect to peer over loopback (127.0.0.1): both devices must be on the same Wi-Fi or Hotspot")
	}

	// 3. Fast-path: Check if already connected
	if s.host.Network().Connectedness(targetInfo.ID) == network.Connected {
		log.Printf("Already connected to peer %s", targetInfo.ID)
		return nil
	}

	// 4. Per-peer lock to prevent simultaneous dial storms from camera frames or mDNS
	lockVal, _ := s.dialLocks.LoadOrStore(targetInfo.ID, &sync.Mutex{})
	peerMu := lockVal.(*sync.Mutex)
	peerMu.Lock()
	defer peerMu.Unlock()

	// Re-check connectedness after acquiring lock
	if s.host.Network().Connectedness(targetInfo.ID) == network.Connected {
		log.Printf("Already connected to peer %s (after lock)", targetInfo.ID)
		return nil
	}

	// Register addresses in host Peerstore with permanent TTL
	s.host.Peerstore().AddAddrs(targetInfo.ID, targetInfo.Addrs, peerstore.PermanentAddrTTL)

	// 5. Dial with automatic retry and jitter for symmetrical Noise handshake resolution
	var lastDialErr error
	for attempt := 1; attempt <= 3; attempt++ {
		// Clear swarm backoff before each attempt
		if sw, ok := s.host.Network().(*swarm.Swarm); ok {
			sw.Backoff().Clear(targetInfo.ID)
		}

		log.Printf("Dialing peer %s (attempt %d/3) via addresses: %v", targetInfo.ID, attempt, targetInfo.Addrs)
		dialCtx, dialCancel := context.WithTimeout(ctx, 8*time.Second)
		dialErr := s.transport.Connect(dialCtx, *targetInfo)
		dialCancel()

		if dialErr == nil {
			log.Printf("Successfully connected to peer %s", targetInfo.ID)
			return nil
		}

		lastDialErr = dialErr

		// Check if the other peer's simultaneous dial succeeded in connecting us
		if s.host.Network().Connectedness(targetInfo.ID) == network.Connected {
			log.Printf("Peer %s is now connected despite dial error note: %v", targetInfo.ID, dialErr)
			return nil
		}

		log.Printf("Dial attempt %d failed for peer %s: %v", attempt, targetInfo.ID, dialErr)

		if attempt < 3 {
			// Asymmetric tie-breaking delay: lower Peer ID dials earlier, higher Peer ID waits longer
			baseDelay := 100 * attempt
			if s.transport.PeerID() > targetInfo.ID {
				baseDelay += 350
			}
			// Add random jitter between 50ms and 150ms
			jitter := rand.Intn(100) + 50
			waitDuration := time.Duration(baseDelay+jitter) * time.Millisecond

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitDuration):
			}

			// Check again after jitter - if inbound dial succeeded, we are already connected!
			if s.host.Network().Connectedness(targetInfo.ID) == network.Connected {
				log.Printf("Peer %s connected during backoff window", targetInfo.ID)
				return nil
			}
		}
	}

	return fmt.Errorf("dial failed: %w", lastDialErr)
}

func (s *NodeService) AddIncomingMessage(from, content string) {
	s.store.Add(from, content)
}

// Store returns the internal MessageStore
func (s *NodeService) Store() *api.MessageStore {
	return s.store
}

// loadOrGenerateKey loads an existing Ed25519 private key from dataDir/identity.key
// or generates and saves a new one. This ensures PeerID never changes across restarts.
func loadOrGenerateKey(dataDir string) (crypto.PrivKey, error) {
	if dataDir == "" {
		dataDir = "."
	}
	keyPath := filepath.Join(dataDir, "identity.key")
	if data, err := os.ReadFile(keyPath); err == nil && len(data) > 0 {
		privKey, err := crypto.UnmarshalPrivateKey(data)
		if err == nil {
			log.Printf("Loaded persistent node identity from %s", keyPath)
			return privKey, nil
		}
		log.Printf("Corrupted identity key in %s: %v, regenerating", keyPath, err)
	}

	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	data, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	if err := os.WriteFile(keyPath, data, 0600); err != nil {
		return nil, fmt.Errorf("failed to write identity key: %w", err)
	}

	log.Printf("Generated and persisted new identity to %s", keyPath)
	return privKey, nil
}

// Start boots the libp2p host with persistent identity, pure TCP transport,
// symmetric peer-to-peer security (Noise & Plaintext without TLS), mDNS auto-discovery,
// and begins serving the local HTTP API.
func Start(cfg Config) (*NodeService, error) {
	if cfg.HTTPPort <= 0 {
		cfg.HTTPPort = 8080
	}
	if cfg.P2PPort <= 0 {
		cfg.P2PPort = 4001
	}

	privKey, err := loadOrGenerateKey(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("identity key error: %w", err)
	}

	tcpListen := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.P2PPort)

	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(tcpListen),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		// Symmetrical P2P security protocols: Noise (Noise_XX_25519_ChaChaPoly_SHA256) and Plaintext fallback.
		// Absolutely NO TLS is registered. This eliminates asymmetric TLS client/server handshake issues.
		libp2p.Security(noise.ID, noise.New),
		libp2p.Security(insecure.ID, insecure.NewWithIdentity),
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		libp2p.EnableRelayService(),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		// Fallback to dynamic port if 4001 is already bound
		log.Printf("Notice: failed to bind %s (%v), attempting dynamic port", tcpListen, err)
		fallbackOpts := []libp2p.Option{
			libp2p.Identity(privKey),
			libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
			libp2p.Transport(tcp.NewTCPTransport),
			libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
			libp2p.Security(noise.ID, noise.New),
			libp2p.Security(insecure.ID, insecure.NewWithIdentity),
		}
		h, err = libp2p.New(fallbackOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create libp2p host: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	nodeTransport := transport.NewLibp2pTransport(h)
	store := api.NewMessageStore()

	// Route over Wi-Fi and the app's Bluetooth links alike, relaying through
	// other peers when there is no direct link
	bridge := transport.NewBluetoothBridge()
	mesh := transport.NewMeshNetwork(nodeTransport, bridge)
	proc, err := processing.New(processing.Config{Net: mesh, Key: privKey})
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create processor: %w", err)
	}
	proc.Start()

	svc := &NodeService{
		host:        h,
		transport:   nodeTransport,
		bridge:      bridge,
		mesh:        mesh,
		proc:        proc,
		store:       store,
		p2pPort:     cfg.P2PPort,
		cancel:      cancel,
		done:        make(chan struct{}),
		peerHandles: make(map[peer.ID]string),
		pex:         newPexState(cfg.PexLoopback),
	}

	// Exchange handles with every peer as soon as a connection comes up, and
	// tell neighbours about the newcomer (and the newcomer about them)
	h.SetStreamHandler(profileProtocol, svc.handleProfileStream)
	h.SetStreamHandler(pexProtocol, svc.handlePexStream)
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			go svc.sendProfile(c.RemotePeer())
			time.AfterFunc(pexAnnounceDelay, svc.announcePex)
		},
	})
	go svc.monitorPeers(ctx)
	go svc.pexLoop(ctx)

	// Initialize and start mDNS peer auto-discovery for local Wi-Fi / Hotspot
	if !cfg.DisableMDNS {
		notifee := &mdnsNotifee{h: h, ctx: ctx}
		svc.mdnsSvc = mdns.NewMdnsService(h, "chatter-mesh", notifee)
		if err := svc.mdnsSvc.Start(); err != nil {
			log.Printf("mDNS start notice (non-fatal): %v", err)
		} else {
			log.Printf("mDNS auto-discovery active on local network (service: chatter-mesh)")
		}
	}

	// The processing layer is the only reader of the network: it verifies,
	// dedups and relays envelopes, handing over what is addressed to us
	go func() {
		for {
			select {
			case m := <-svc.proc.Incoming():
				log.Printf("Received message %s from %s via %s", m.ID, m.From, m.Via)
				received := api.ReceivedMessage{
					From: m.From, Content: m.Content, MsgID: m.ID, Handle: m.Handle, Via: m.Via, Room: m.Room,
				}
				if m.History {
					// Caught up from a neighbour: when it was written, not now
					received.History = true
					received.Timestamp = m.Timestamp.UTC().Format(time.RFC3339)
				}
				store.AddMessage(received)
				if pid, err := peer.Decode(m.From); err == nil && m.Handle != "" {
					svc.profileMu.Lock()
					svc.peerHandles[pid] = m.Handle
					svc.profileMu.Unlock()
				}
			case r := <-svc.proc.Receipts():
				store.AddReceipt(r.From, r.Via, r.IDs, r.Status)
			case <-ctx.Done():
				return
			}
		}
	}()

	svc.server = api.NewServer(svc, store)
	httpAddr := "127.0.0.1:" + strconv.Itoa(cfg.HTTPPort)
	svc.httpServer = &http.Server{
		Addr:    httpAddr,
		Handler: api.CORSMiddleware(svc.server.Mux()),
	}

	ln, err := net.Listen("tcp", httpAddr)
	if err != nil {
		if svc.mdnsSvc != nil {
			svc.mdnsSvc.Close()
		}
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to listen on HTTP port %d: %w", cfg.HTTPPort, err)
	}

	go func() {
		log.Printf("HTTP API listening on http://%s", httpAddr)
		if err := svc.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Chatter node started. Peer ID: %s", h.ID())
	for _, a := range svc.Addrs() {
		log.Printf("  Multiaddr: %s", a)
	}

	return svc, nil
}

// Stop shuts down mDNS, the HTTP server, and libp2p host cleanly.
func (s *NodeService) Stop() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.mdnsSvc != nil {
			s.mdnsSvc.Close()
		}
		if s.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			s.httpServer.Shutdown(ctx)
		}
		if s.proc != nil {
			s.proc.Stop()
		}
		if s.mesh != nil {
			s.mesh.Close()
		}
		if s.transport != nil {
			s.transport.Close()
		}
		if s.host != nil {
			s.host.Close()
		}
		close(s.done)
	})
	return nil
}
