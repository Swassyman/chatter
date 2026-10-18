package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Swassyman/chatter/node"
)

func TestNodeIdentityPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "chatter_node_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 1. Start node first time
	svc1, err := node.Start(node.Config{
		DataDir:  tempDir,
		HTTPPort: 9081,
		P2PPort:  5001,
	})
	if err != nil {
		t.Fatalf("failed to start node 1: %v", err)
	}

	peerID1 := svc1.ID()
	if peerID1 == "" {
		t.Fatal("expected non-empty peer ID")
	}

	// Verify key file was created
	keyPath := filepath.Join(tempDir, "identity.key")
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		t.Fatalf("expected identity.key to exist at %s", keyPath)
	}

	// Verify HTTP server works
	time.Sleep(100 * time.Millisecond)
	resp, err := http.Get("http://127.0.0.1:9081/health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Verify /node endpoint returns the same ID
	resp, err = http.Get("http://127.0.0.1:9081/node")
	if err != nil {
		t.Fatalf("failed to get /node: %v", err)
	}
	var nodeData struct {
		ID    string   `json:"id"`
		Addrs []string `json:"addrs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodeData); err != nil {
		t.Fatalf("failed to decode node response: %v", err)
	}
	resp.Body.Close()

	if nodeData.ID != peerID1 {
		t.Fatalf("expected node API ID %s to match service ID %s", nodeData.ID, peerID1)
	}

	// Stop first instance
	if err := svc1.Stop(); err != nil {
		t.Fatalf("failed to stop node 1: %v", err)
	}

	// Give port time to release
	time.Sleep(200 * time.Millisecond)

	// 2. Start node second time using the exact same data directory
	svc2, err := node.Start(node.Config{
		DataDir:  tempDir,
		HTTPPort: 9081,
		P2PPort:  5001,
	})
	if err != nil {
		t.Fatalf("failed to start node 2: %v", err)
	}
	defer svc2.Stop()

	peerID2 := svc2.ID()

	// Verify PeerID did NOT change across restarts!
	if peerID1 != peerID2 {
		t.Fatalf("PeerID changed across restarts! First: %s, Second: %s", peerID1, peerID2)
	}
}

func TestDirectNodeP2PConnectionWithoutTLS(t *testing.T) {
	tempDir1, err := os.MkdirTemp("", "chatter_peer1_*")
	if err != nil {
		t.Fatalf("failed to create temp dir 1: %v", err)
	}
	defer os.RemoveAll(tempDir1)

	tempDir2, err := os.MkdirTemp("", "chatter_peer2_*")
	if err != nil {
		t.Fatalf("failed to create temp dir 2: %v", err)
	}
	defer os.RemoveAll(tempDir2)

	// Node 1
	svc1, err := node.Start(node.Config{
		DataDir:  tempDir1,
		HTTPPort: 9082,
		P2PPort:  5002,
	})
	if err != nil {
		t.Fatalf("failed to start node 1: %v", err)
	}
	defer svc1.Stop()

	// Node 2
	svc2, err := node.Start(node.Config{
		DataDir:  tempDir2,
		HTTPPort: 9083,
		P2PPort:  5003,
	})
	if err != nil {
		t.Fatalf("failed to start node 2: %v", err)
	}
	defer svc2.Stop()

	addrs1 := svc1.Addrs()
	if len(addrs1) == 0 {
		t.Fatal("node 1 has no listen addrs")
	}

	// Ensure no QUIC/TLS addresses are being generated
	for _, a := range addrs1 {
		if filepath.Ext(a) == "quic" || filepath.Ext(a) == "quic-v1" {
			t.Fatalf("unexpected QUIC address found in addrs: %s", a)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect Node 2 to Node 1 via TCP using Noise protocol
	if err := svc2.ConnectPeer(ctx, addrs1[0]); err != nil {
		t.Fatalf("failed to connect node 2 to node 1 without TLS: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Send message from Node 2 to Node 1
	testMessage := "hello peer from pure TCP Noise protocol"
	if err := svc2.SendMessage(svc1.ID(), testMessage); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Verify Node 1 received the message in its store
	var received bool
	for i := 0; i < 20; i++ {
		msgs := svc1.Store().Since(-1)
		for _, m := range msgs {
			if m.Content == testMessage && m.From == svc2.ID() {
				received = true
				break
			}
		}
		if received {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !received {
		t.Fatal("node 1 did not receive message from node 2")
	}

	// Test simultaneous dial between both nodes (symmetric P2P dial)
	var wg sync.WaitGroup
	wg.Add(2)
	var err1, err2 error

	go func() {
		defer wg.Done()
		err1 = svc1.ConnectPeer(ctx, svc2.Addrs()[0])
	}()
	go func() {
		defer wg.Done()
		err2 = svc2.ConnectPeer(ctx, svc1.Addrs()[0])
	}()

	wg.Wait()
	if err1 != nil {
		t.Fatalf("simultaneous dial node 1 failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("simultaneous dial node 2 failed: %v", err2)
	}
}

func TestFreshSimultaneousConnect(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		tempDir1, _ := os.MkdirTemp("", "chatter_sim1_*")
		tempDir2, _ := os.MkdirTemp("", "chatter_sim2_*")

		svc1, err := node.Start(node.Config{
			DataDir:  tempDir1,
			HTTPPort: 9085 + attempt*2,
			P2PPort:  5010 + attempt*2,
		})
		if err != nil {
			t.Fatalf("failed to start node 1: %v", err)
		}

		svc2, err := node.Start(node.Config{
			DataDir:  tempDir2,
			HTTPPort: 9086 + attempt*2,
			P2PPort:  5011 + attempt*2,
		})
		if err != nil {
			svc1.Stop()
			t.Fatalf("failed to start node 2: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var err1, err2 error

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		go func() {
			defer wg.Done()
			err1 = svc1.ConnectPeer(ctx, svc2.Addrs()[0])
		}()
		go func() {
			defer wg.Done()
			err2 = svc2.ConnectPeer(ctx, svc1.Addrs()[0])
		}()

		wg.Wait()
		cancel()

		t.Logf("Attempt %d - Dial Node 1 -> Node 2: %v, Dial Node 2 -> Node 1: %v", attempt, err1, err2)

		svc1.Stop()
		svc2.Stop()
		os.RemoveAll(tempDir1)
		os.RemoveAll(tempDir2)
		time.Sleep(100 * time.Millisecond)
	}
}
