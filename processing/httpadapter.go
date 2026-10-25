package processing

import (
	"context"

	httpapi "github.com/Swassyman/chatter/api/http"
)

// HTTPNode adapts a Processor to the httpapi.Node interface.
type HTTPNode struct{ P *Processor }

var _ httpapi.Node = HTTPNode{}

func (n HTTPNode) ID() string { return n.P.ID() }

// Addrs returns the transport's listen multiaddrs if it exposes them.
func (n HTTPNode) Addrs() []string {
	if a, ok := n.P.net.(interface{ Addrs() []string }); ok {
		return a.Addrs()
	}
	return nil
}

// AddIncomingMessage is a no-op: the processor stores delivered messages itself.
func (n HTTPNode) AddIncomingMessage(from, content string) {}

func (n HTTPNode) Peers() []httpapi.Peer {
	list := n.P.Peers()
	out := make([]httpapi.Peer, 0, len(list))
	for _, p := range list {
		out = append(out, httpapi.Peer{ID: p.ID.String()})
	}
	return out
}

func (n HTTPNode) SendMessage(peerID, content string) error {
	return n.P.SendTo(peerID, content)
}

func (n HTTPNode) ConnectPeer(ctx context.Context, address string) error {
	return n.P.ConnectPeer(ctx, address)
}
