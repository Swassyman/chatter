package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpapi "github.com/Swassyman/chatter/api/http"
)

// mockNode is a test implementation of the httpapi.Node interface.
type mockNode struct {
	store *httpapi.MessageStore
}

func (m *mockNode) ID() string { return "test-peer-id" }

func (m *mockNode) Addrs() []string {
	return []string{"/ip4/127.0.0.1/tcp/4001/p2p/test-peer-id"}
}

func (m *mockNode) Peers() []httpapi.Peer {
	return []httpapi.Peer{{ID: "peer-a"}, {ID: "peer-b"}}
}

func (m *mockNode) SendMessage(peerID string, content string) error {
	return nil
}

func (m *mockNode) ConnectPeer(ctx context.Context, address string) error {
	return nil
}

func (m *mockNode) AddIncomingMessage(from, content string) {
	if m.store != nil {
		m.store.Add(from, content)
	}
}

// newTestServer creates a Server backed by a mockNode and a fresh MessageStore,
// returning both so tests can manipulate the store directly.
func newTestServer() (*httpapi.Server, *httpapi.MessageStore) {
	store := httpapi.NewMessageStore()
	mn := &mockNode{store: store}
	return httpapi.NewServer(mn, store), store
}

// makeRequest creates an httptest.Server wrapping the CORS middleware and the
// server's mux, performs a single HTTP request, and returns the response.
func makeRequest(t *testing.T, srv *httpapi.Server, method, path string, body []byte) *http.Response {
	t.Helper()

	ts := httptest.NewServer(httpapi.CORSMiddleware(srv.Mux()))
	t.Cleanup(ts.Close)

	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, ts.URL+path, bytes.NewReader(body))
	} else {
		req, err = http.NewRequest(method, ts.URL+path, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// --- tests -------------------------------------------------------------------

func TestHealthEndpoint(t *testing.T) {
	srv, _ := newTestServer()
	resp := makeRequest(t, srv, http.MethodGet, "/health", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

func TestNodeEndpoint(t *testing.T) {
	srv, _ := newTestServer()
	resp := makeRequest(t, srv, http.MethodGet, "/node", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["id"]; !ok {
		t.Fatal("expected id field in response")
	}
	if _, ok := body["addrs"]; !ok {
		t.Fatal("expected addrs field in response")
	}
}

func TestPeersEndpoint(t *testing.T) {
	srv, _ := newTestServer()
	resp := makeRequest(t, srv, http.MethodGet, "/peers", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["peers"]; !ok {
		t.Fatal("expected peers field in response")
	}
}

func TestMessageEndpoint_InvalidMethod(t *testing.T) {
	srv, _ := newTestServer()
	resp := makeRequest(t, srv, http.MethodGet, "/message", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestMessageEndpoint_MissingPeerID(t *testing.T) {
	srv, _ := newTestServer()
	payload, _ := json.Marshal(map[string]string{"content": "hello"})
	resp := makeRequest(t, srv, http.MethodPost, "/message", payload)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestMessagesEndpoint(t *testing.T) {
	srv, store := newTestServer()

	// Initially empty.
	resp := makeRequest(t, srv, http.MethodGet, "/messages", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	msgs, ok := body["messages"]
	if !ok {
		t.Fatal("expected messages field")
	}
	if len(msgs.([]interface{})) != 0 {
		t.Fatal("expected empty messages initially")
	}

	// Add a message directly to the store.
	store.Add("peer-x", "hello world")

	resp2 := makeRequest(t, srv, http.MethodGet, "/messages", nil)
	defer resp2.Body.Close()

	var body2 map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&body2); err != nil {
		t.Fatal(err)
	}
	msgs2 := body2["messages"].([]interface{})
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs2))
	}
}

func TestCORSHeaders(t *testing.T) {
	srv, _ := newTestServer()
	resp := makeRequest(t, srv, http.MethodGet, "/health", nil)
	defer resp.Body.Close()

	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin=*, got %q", got)
	}
}

func TestCORSPreflight(t *testing.T) {
	srv, _ := newTestServer()
	resp := makeRequest(t, srv, http.MethodOptions, "/health", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin=*, got %q", got)
	}
}
