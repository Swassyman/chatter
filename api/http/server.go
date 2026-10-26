package httpapi

import (
	"context"
	"net/http"
	"time"
)

type Peer struct {
	ID     string `json:"id"`
	Handle string `json:"handle,omitempty"`
}

// ProfileNode is implemented by nodes that carry a user-chosen handle and
// share it with their peers.
type ProfileNode interface {
	Handle() string
	SetHandle(handle string) error
}

// MeshNode is implemented by nodes that route chat through the mesh (relayed
// over other peers when there's no direct link) and exchange receipts.
type MeshNode interface {
	// SendChat sends under msgID (generated when empty) and returns the ID used.
	SendChat(peerID, msgID, content string) (string, error)
	SendReceipt(peerID string, msgIDs []string, status string) error
}

// RoomNode is implemented by nodes with mesh channels: broadcast rooms that
// every node relays, some of them private (passphrase-keyed).
type RoomNode interface {
	// SendRoom posts under msgID (generated when empty) and returns the ID used.
	SendRoom(room, msgID, content string) (string, error)
	// JoinRoom returns the room's ID: its name if open, or a private room's tag.
	JoinRoom(name, passphrase string) (string, error)
	LeaveRoom(room string)
}

// BridgeFrame is a mesh frame travelling over a Bluetooth link the app owns.
type BridgeFrame struct {
	To   string `json:"to"`
	Data []byte `json:"data"` // base64 in JSON
}

// BridgeNode is implemented by nodes that route over Bluetooth links living
// in the app: the app reports its Bluetooth neighbours, carries outgoing
// frames, and hands in frames it receives.
type BridgeNode interface {
	SetBluetoothPeers(ids []string)
	BluetoothOutbox(ctx context.Context) []BridgeFrame
	BluetoothInbox(from string, data []byte) error
}

// DiscoveredPeer is a peer a neighbour told us about (peer exchange): not
// necessarily connected, but reachable at Addrs on that neighbour's network.
type DiscoveredPeer struct {
	ID    string    `json:"id"`
	Addrs []string  `json:"addrs"`
	Via   string    `json:"via"`
	Seen  time.Time `json:"seen"`
}

// PexNode is implemented by nodes that learn about peers from their neighbours.
type PexNode interface {
	Discovered() []DiscoveredPeer
}

// Node is the interface the HTTP server uses to interact with the p2p node.
type Node interface {
	ID() string
	Addrs() []string
	Peers() []Peer
	SendMessage(peerID string, content string) error
	ConnectPeer(ctx context.Context, address string) error
	AddIncomingMessage(from, content string)
}

type Server struct {
	node  Node
	mux   *http.ServeMux
	store *MessageStore
}

func NewServer(node Node, store *MessageStore) *Server {
	mux := http.NewServeMux()
	server := &Server{
		node:  node,
		mux:   mux,
		store: store,
	}

	server.registerRoutes()

	return server
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/node", s.handleNode)
	s.mux.HandleFunc("/peers", s.handlePeers)
	s.mux.HandleFunc("/message", s.handleMessage)
	s.mux.HandleFunc("/peers/connect", s.handleConnectPeer)
	s.mux.HandleFunc("/peers/discovered", s.handleDiscoveredPeers)
	s.mux.HandleFunc("/messages", s.handleMessages)
	s.mux.HandleFunc("/profile", s.handleProfile)
	s.mux.HandleFunc("/receipts", s.handleReceipts)
	s.mux.HandleFunc("/rooms", s.handleRooms)
	s.mux.HandleFunc("/bt/peers", s.handleBluetoothPeers)
	s.mux.HandleFunc("/bt/outbox", s.handleBluetoothOutbox)
	s.mux.HandleFunc("/bt/inbox", s.handleBluetoothInbox)
}

func (s *Server) Start(addr string) error {
	return http.ListenAndServe(addr, CORSMiddleware(s.mux))
}

// Mux returns the underlying ServeMux. Useful for testing with httptest.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}
