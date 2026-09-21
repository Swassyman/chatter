package httpapi

import (
	"context"
	"net/http"
)

type Peer struct {
	ID string `json:"id"`
}
type Node interface {
	ID() string
	Peers() []Peer
	SendMessage(peerID string, content string) error
	ConnectPeer(ctx context.Context, address string) error
}

type Server struct {
	node Node
	mux  *http.ServeMux
}

func NewServer(node Node) *Server {
	mux := http.NewServeMux()
	server := &Server{
		node: node,
		mux:  mux,
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
}

func (s *Server) Start(addr string) error {
	return http.ListenAndServe(addr, s.mux)
}
