package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/Swassyman/chatter"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Libp2pTransport struct {
	host      host.Host
	messages  chan Message
	done      chan struct{}
	closeOnce sync.Once
}

func NewLibp2pTransport(h host.Host) *Libp2pTransport {
	t := &Libp2pTransport{
		host:     h,
		messages: make(chan Message, 32),
		done:     make(chan struct{}),
	}

	h.SetStreamHandler(chatter.CHAT_PROTOCOL, t.handleStream)

	return t
}

func (t *Libp2pTransport) Messages() <-chan Message {
	return t.messages
}

func (t *Libp2pTransport) PeerID() peer.ID {
	return t.host.ID()
}

func (t *Libp2pTransport) handleStream(stream network.Stream) {
	defer stream.Close()

	lengthBytes := make([]byte, 4)

	if _, err := io.ReadFull(stream, lengthBytes); err != nil {
		log.Println("Error reading message length:", err)
		return
	}

	length := binary.BigEndian.Uint32(lengthBytes)

	if length > chatter.MAX_MESSAGE_SIZE {
		log.Println("Message too large")
		return
	}

	data := make([]byte, length)

	if _, err := io.ReadFull(stream, data); err != nil {
		log.Println("Error reading message:", err)
		return
	}

	select {
	case t.messages <- Message{
		From: stream.Conn().RemotePeer(),
		Data: data,
	}:
	case <-t.done:
		return
	}
}

func (t *Libp2pTransport) Send(to peer.ID, data []byte) error {
	select {
	case <-t.done:
		return fmt.Errorf("Transport is closed")
	default:
	}

	if len(data) > chatter.MAX_MESSAGE_SIZE {
		return fmt.Errorf("message too large")
	}

	stream, err := t.host.NewStream(
		context.Background(),
		to,
		chatter.CHAT_PROTOCOL,
	)
	if err != nil {
		return err
	}
	defer stream.Close()

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))

	if _, err := stream.Write(length); err != nil {
		return err
	}

	if _, err := stream.Write(data); err != nil {
		return err
	}

	return nil
}

func (t *Libp2pTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.done)
	})

	return nil
}
