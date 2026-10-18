package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"log"
	"sync"

	"github.com/Swassyman/chatter/node"
)

var (
	mu         sync.Mutex
	activeNode *node.NodeService
)

//export StartNode
func StartNode(cDataDir *C.char, httpPort C.int, p2pPort C.int) C.int {
	mu.Lock()
	defer mu.Unlock()

	if activeNode != nil {
		return 0 // Already running
	}

	dataDir := ""
	if cDataDir != nil {
		dataDir = C.GoString(cDataDir)
	}

	cfg := node.Config{
		DataDir:  dataDir,
		HTTPPort: int(httpPort),
		P2PPort:  int(p2pPort),
	}

	svc, err := node.Start(cfg)
	if err != nil {
		log.Printf("StartNode error: %v", err)
		return -1
	}

	activeNode = svc
	return 0
}

//export StopNode
func StopNode() C.int {
	mu.Lock()
	defer mu.Unlock()

	if activeNode != nil {
		activeNode.Stop()
		activeNode = nil
	}
	return 0
}

//export IsNodeRunning
func IsNodeRunning() C.int {
	mu.Lock()
	defer mu.Unlock()

	if activeNode != nil {
		return 1
	}
	return 0
}

func main() {}
