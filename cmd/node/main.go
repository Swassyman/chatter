package main

import (
	"context"
	"fmt"
	"log"
	"os"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const chatProtocol = "/chatter/1.0.0"

func main() {
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	host.SetStreamHandler(chatProtocol, func(stream network.Stream) {
		defer stream.Close()

		fmt.Println("Recieved communication from: ", stream.Conn().RemotePeer())

		buf := make([]byte, 1024)

		n, err := stream.Read(buf)
		if err != nil {
			log.Println("Error reading: ", err)
			return
		}

		fmt.Println("Received: ", string(buf[:n]))
	})

	fmt.Println("Node started")
	fmt.Println("Peer ID: ", host.ID())

	for _, addr := range host.Addrs() {
		fmt.Printf("Listening: %s/p2p/%s\n ", addr, host.ID())
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

		fmt.Println("Connecting to : ", info.ID)
		if err := host.Connect(context.Background(), *info); err != nil {
			log.Fatal(err)
		}

		fmt.Println("Connected!")

		stream, err := host.NewStream(
			context.Background(),
			info.ID,
			chatProtocol,
		)
		if err != nil {
			log.Fatal(err)
		}
		defer stream.Close()

		_, err = stream.Write([]byte("Hello from Node A"))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Message sent!")
	}

	select {}
}
