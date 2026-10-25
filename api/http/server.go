package httpapi

import (
	"context"
	"net/http"
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
	s.mux.HandleFunc("/messages", s.handleMessages)
	s.mux.HandleFunc("/profile", s.handleProfile)
	s.mux.HandleFunc("/receipts", s.handleReceipts)
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
