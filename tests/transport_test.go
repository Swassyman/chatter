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

	// Connection is explicit.
	err = transportA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("Hello from Node A")

	if err := transportA.Send(hostB.ID(), message); err != nil {
		t.Fatal(err)
	}

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

	err = transportA.Connect(
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

	// B can now send to A because the connection exists.
	if err := transportB.Send(hostA.ID(), message); err != nil {
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

	nodeTransport := transport.NewLibp2pTransport(host)

	if nodeTransport.PeerID() != host.ID() {
		t.Fatalf(
			"expected peer ID %s, got %s",
			host.ID(),
			nodeTransport.PeerID(),
		)
	}
}

func TestLibp2pTransportPeers(t *testing.T) {
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

	// Initially there should be no connected peers.
	if peers := transportA.Peers(); len(peers) != 0 {
		t.Fatalf(
			"expected 0 peers before connection, got %d",
			len(peers),
		)
	}

	err = transportA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	peers := transportA.Peers()

	if len(peers) != 1 {
		t.Fatalf(
			"expected 1 peer after connection, got %d",
			len(peers),
		)
	}

	if peers[0] != hostB.ID() {
		t.Fatalf(
			"expected peer %s, got %s",
			hostB.ID(),
			peers[0],
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

	err = transportA.Connect(
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
			if received.From != hostA.ID() {
				t.Fatalf(
					"expected sender %s, got %s",
					hostA.ID(),
					received.From,
				)
			}

			if string(received.Data) != string(expected) {
				t.Fatalf(
					"expected %q, got %q",
					string(expected),
					string(received.Data),
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

	err = transportA.Connect(
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

	if err := transportA.Send(hostB.ID(), message); err != nil {
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

	nodeTransport := transport.NewLibp2pTransport(host)

	if err := nodeTransport.Close(); err != nil {
		t.Fatal(err)
	}

	err = nodeTransport.Send(
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

	nodeTransport := transport.NewLibp2pTransport(host)

	if err := nodeTransport.Close(); err != nil {
		t.Fatal(err)
	}

	// Close should be safe to call multiple times.
	if err := nodeTransport.Close(); err != nil {
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

	err = transportA.Connect(
		context.Background(),
		peer.AddrInfo{
			ID:    hostB.ID(),
			Addrs: hostB.Addrs(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := transportA.Send(hostB.ID(), []byte{}); err != nil {
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
