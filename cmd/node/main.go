package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Swassyman/chatter/transport"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func main() {
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	t := transport.NewLibp2pTransport(host)
	defer t.Close()

	go func() {
		for msg := range t.Messages() {
			fmt.Println("Received communication from:", msg.From)
			fmt.Println("Received:", string(msg.Data))
		}
	}()

	fmt.Println("Node started")
	fmt.Println("Peer ID:", t.PeerID())

	for _, addr := range host.Addrs() {
		fmt.Printf("Listening: %s/p2p/%s\n", addr, t.PeerID())
	}

	if len(os.Args) > 1 {
		targetAddr, err := multiaddr.NewMultiaddr(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}

		info, err := peer.AddrInfoFromP2pAddr(targetAddr)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Connecting to:", info.ID)

		if err := host.Connect(context.Background(), *info); err != nil {
			log.Fatal(err)
		}

		fmt.Println("Connected!")

		err = t.Send(info.ID, []byte("Hello from Node A"))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Message sent!")
	}

	select {}
}
