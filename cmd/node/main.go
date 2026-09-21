package main

import (
	"context"
	"flag"
	"log"
	"strconv"

	api "github.com/Swassyman/chatter/api/http"
	"github.com/Swassyman/chatter/transport"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type node struct {
	transport *transport.Libp2pTransport
}

func (n *node) ID() string {
	return n.transport.PeerID().String()
}

func (n *node) Peers() []api.Peer {
	peers := n.transport.Peers()

	result := make([]api.Peer, 0, len(peers))

	for _, p := range peers {
		result = append(result, api.Peer{
			ID: p.String(),
		})
	}

	return result
}

func (n *node) SendMessage(peerID string, content string) error {
	to, err := peer.Decode(peerID)
	if err != nil {
		return err
	}

	return n.transport.Send(to, []byte(content))
}

func (n *node) ConnectPeer(ctx context.Context, address string) error {
	addr, err := multiaddr.NewMultiaddr(address)
	if err != nil {
		return err
	}

	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return err
	}

	return n.transport.Connect(ctx, *info)
}

func main() {
	port := flag.Int("port", 8080, "HTTP API port")
	flag.Parse()

	ctx := context.Background()

	host, err := libp2p.New()
	if err != nil {
		log.Fatal("failed to create libp2p host:", err)
	}
	defer host.Close()

	log.Println("libp2p node started")
	log.Println("Peer ID:", host.ID())

	for _, addr := range host.Addrs() {
		log.Printf("Listening on: %s/p2p/%s", addr, host.ID())
	}

	nodeTransport := transport.NewLibp2pTransport(host)

	n := &node{
		transport: nodeTransport,
	}

	go func() {
		for {
			select {
			case msg, ok := <-nodeTransport.Messages():
				if !ok {
					return
				}

				log.Printf(
					"received message from %s: %s",
					msg.From,
					string(msg.Data),
				)

			case <-ctx.Done():
				return
			}
		}
	}()
	server := api.NewServer(n)

	log.Printf("HTTP API listening on :%d", *port)

	if err := server.Start(":" + strconv.Itoa(*port)); err != nil {
		log.Fatal(err)
	}
}
