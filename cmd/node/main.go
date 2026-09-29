package main

import (
	"flag"
	"log"
	"strconv"

	api "github.com/Swassyman/chatter/api/http"
	"github.com/Swassyman/chatter/processing"
	"github.com/Swassyman/chatter/transport"

	"github.com/libp2p/go-libp2p"
)

func main() {
	port := flag.Int("port", 8080, "HTTP API port")
	secret := flag.String("secret", "", "shared passphrase for end-to-end payload encryption (all nodes must use the same; empty = plaintext payloads)")
	flag.Parse()

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

	// Transport layer
	nodeTransport := transport.NewLibp2pTransport(host)
	defer nodeTransport.Close()

	// Processing layer
	cfg := processing.Config{
		Net: nodeTransport,
		Key: host.Peerstore().PrivKey(host.ID()),
	}
	if *secret != "" {
		c, err := processing.NewAESGCM(processing.KeyFromPassphrase(*secret))
		if err != nil {
			log.Fatal("cipher:", err)
		}
		cfg.Cipher = c
	}
	proc, err := processing.New(cfg)
	if err != nil {
		log.Fatal("failed to create processor:", err)
	}
	proc.Start()
	defer proc.Stop()

	// The processor is now the ONLY reader of the transport's Messages()
	// channel. Application-level messages come out of proc.Incoming().
	go func() {
		for m := range proc.Incoming() {
			log.Printf("received message from %s (to=%q room=%q): %s",
				m.From, m.To, m.Room, m.Content)
		}
	}()

	// Application layer (HTTP API)
	server := api.NewServer(processing.HTTPNode{P: proc})

	log.Printf("HTTP API listening on :%d", *port)
	if err := server.Start(":" + strconv.Itoa(*port)); err != nil {
		log.Fatal(err)
	}
}
