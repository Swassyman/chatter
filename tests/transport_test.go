package tests

import (
	"context"
	"testing"
	"time"

	"github.com/Swassyman/chatter/transport"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestLibp2pTransportSendReceive(t *testing.T) {
	// Create Node A
	hostA, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()

	// Create Node B
	hostB, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	// Create transports
	transportA := transport.NewLibp2pTransport(hostA)
	transportB := transport.NewLibp2pTransport(hostB)

	// Connect A to B
	err = hostA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Send message from A to B
	message := []byte("Hello from Node A")

	err = transportA.Send(hostB.ID(), message)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for B to receive the message
	select {
	case received := <-transportB.Messages():
		if received.From != hostA.ID() {
			t.Fatalf(
				"expected message from %s, got %s",
				hostA.ID(),
				received.From,
			)
		}

		if string(received.Data) != string(message) {
			t.Fatalf(
				"expected %q, got %q",
				string(message),
				string(received.Data),
			)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLibp2pTransportSendReceiveReverse(t *testing.T) {
	hostA, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()

	hostB, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	transportA := transport.NewLibp2pTransport(hostA)
	transportB := transport.NewLibp2pTransport(hostB)

	err = hostA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("Hello from Node B")

	err = transportB.Send(hostA.ID(), message)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-transportA.Messages():
		if received.From != hostB.ID() {
			t.Fatalf(
				"expected message from %s, got %s",
				hostB.ID(),
				received.From,
			)
		}

		if string(received.Data) != string(message) {
			t.Fatalf(
				"expected %q, got %q",
				string(message),
				string(received.Data),
			)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLibp2pTransportPeerID(t *testing.T) {
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	transport := transport.NewLibp2pTransport(host)

	if transport.PeerID() != host.ID() {
		t.Fatalf(
			"expected peer ID %s, got %s",
			host.ID(),
			transport.PeerID(),
		)
	}
}

func TestLibp2pTransportMultipleMessages(t *testing.T) {
	hostA, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()

	hostB, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	transportA := transport.NewLibp2pTransport(hostA)
	transportB := transport.NewLibp2pTransport(hostB)

	err = hostA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	messages := [][]byte{
		[]byte("message one"),
		[]byte("message two"),
		[]byte("message three"),
	}

	for _, message := range messages {
		if err := transportA.Send(hostB.ID(), message); err != nil {
			t.Fatal(err)
		}
	}

	for _, expected := range messages {
		select {
		case received := <-transportB.Messages():
			if string(received.Data) != string(expected) {
				t.Fatalf(
					"expected %q, got %q",
					string(expected),
					string(received.Data),
				)
			}

			if received.From != hostA.ID() {
				t.Fatalf(
					"expected sender %s, got %s",
					hostA.ID(),
					received.From,
				)
			}

		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for message")
		}
	}
}

func TestLibp2pTransportLargeMessage(t *testing.T) {
	hostA, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()

	hostB, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	transportA := transport.NewLibp2pTransport(hostA)
	transportB := transport.NewLibp2pTransport(hostB)

	err = hostA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	message := make([]byte, 5000)

	for i := range message {
		message[i] = byte(i % 256)
	}

	err = transportA.Send(hostB.ID(), message)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-transportB.Messages():
		if string(received.Data) != string(message) {
			t.Fatal("received message does not match sent message")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLibp2pTransportSendAfterClose(t *testing.T) {
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	transport := transport.NewLibp2pTransport(host)

	err = transport.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = transport.Send(
		host.ID(),
		[]byte("should fail"),
	)

	if err == nil {
		t.Fatal("expected Send to fail after transport was closed")
	}
}

func TestLibp2pTransportCloseTwice(t *testing.T) {
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	transport := transport.NewLibp2pTransport(host)

	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}

	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLibp2pTransportEmptyMessage(t *testing.T) {
	hostA, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()

	hostB, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	transportA := transport.NewLibp2pTransport(hostA)
	transportB := transport.NewLibp2pTransport(hostB)

	err = hostA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	err = transportA.Send(hostB.ID(), []byte{})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-transportB.Messages():
		if len(received.Data) != 0 {
			t.Fatalf(
				"expected empty message, got %d bytes",
				len(received.Data),
			)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}
