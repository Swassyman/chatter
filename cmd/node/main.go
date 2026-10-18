package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Swassyman/chatter/node"
)

func main() {
	port := flag.Int("port", 8080, "HTTP API port")
	p2pPort := flag.Int("p2p-port", 4001, "libp2p listen TCP port")
	dataDir := flag.String("data-dir", "./data", "Directory to persist node identity key")
	flag.Parse()

	svc, err := node.Start(node.Config{
		DataDir:  *dataDir,
		HTTPPort: *port,
		P2PPort:  *p2pPort,
	})
	if err != nil {
		log.Fatal("Failed to start node:", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down Chatter node...")
	svc.Stop()
}
