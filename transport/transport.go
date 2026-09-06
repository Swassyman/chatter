package transport

import "github.com/libp2p/go-libp2p/core/peer"

type Message struct {
	From peer.ID
	Data []byte
}

type Transport interface {
	PeerID() peer.ID
	Send(to peer.ID, data []byte) error
	Messages() <-chan Message
	Close() error
}
