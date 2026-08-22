package main

import (
	"context"
	"fmt"
	"log"

	libp2p "github.com/libp2p/go-libp2p"
)

func main() {
	ctx := context.Background()

	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	fmt.Println("Node started")
	fmt.Println("Peer ID: ", host.ID())

	for _, addr := range host.Addrs() {
		fmt.Println("Listening: ", addr)
	}

	<-ctx.Done()
}
