package httpapi

import (
	"context"
	"net/http"
)

type Peer struct {
	ID string `json:"id"`
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
}

func (s *Server) Start(addr string) error {
	return http.ListenAndServe(addr, CORSMiddleware(s.mux))
}

// Mux returns the underlying ServeMux. Useful for testing with httptest.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}
