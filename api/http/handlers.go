package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := map[string]interface{}{
		"id":    s.node.ID(),
		"addrs": s.node.Addrs(),
	}
	if pn, ok := s.node.(ProfileNode); ok {
		resp["handle"] = pn.Handle()
	}
	writeJSON(w, http.StatusOK, resp)
}

type ProfileRequest struct {
	Handle string `json:"handle"`
}

// handleProfile reads (GET) or sets (POST) this node's handle.
func (s *Server) handleProfile(w http.ResponseWriter, r *http.Request) {
	pn, ok := s.node.(ProfileNode)
	if !ok {
		http.Error(w, "profiles not supported", http.StatusNotImplemented)
		return
	}

	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		var req ProfileRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err := pn.SetHandle(req.Handle); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"id":     s.node.ID(),
		"handle": pn.Handle(),
	})
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"peers": s.node.Peers(),
	})
}

type SendMessageRequest struct {
	PeerID  string `json:"peer_id"`
	Content string `json:"content"`
	ID      string `json:"id,omitempty"` // optional message ID chosen by the app
}

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.PeerID == "" {
		http.Error(w, "peer_id is required", http.StatusBadRequest)
		return
	}

	if req.Content == "" {
		http.Error(w, "content is required", http.StatusBadRequest)
		return
	}

	id := req.ID
	var err error
	if mn, ok := s.node.(MeshNode); ok {
		id, err = mn.SendChat(req.PeerID, req.ID, req.Content)
	} else {
		err = s.node.SendMessage(req.PeerID, req.Content)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "sent",
		"peer_id": req.PeerID,
		"content": req.Content,
		"id":      id,
	})
}

type ReceiptRequest struct {
	PeerID string   `json:"peer_id"`
	IDs    []string `json:"ids"`
	Status string   `json:"status"` // "delivered" or "read"
}

// handleReceipts lists receipts we received (GET ?after=N) or sends one (POST).
func (s *Server) handleReceipts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		afterID := -1
		if raw := r.URL.Query().Get("after"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				http.Error(w, "invalid after param", http.StatusBadRequest)
				return
			}
			afterID = parsed
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"receipts": s.store.ReceiptsSince(afterID),
		})

	case http.MethodPost:
		mn, ok := s.node.(MeshNode)
		if !ok {
			http.Error(w, "receipts not supported", http.StatusNotImplemented)
			return
		}
		var req ReceiptRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if req.PeerID == "" || len(req.IDs) == 0 {
			http.Error(w, "peer_id and ids are required", http.StatusBadRequest)
			return
		}
		if err := mn.SendReceipt(req.PeerID, req.IDs, req.Status); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "sent"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) bridgeNode(w http.ResponseWriter) (BridgeNode, bool) {
	bn, ok := s.node.(BridgeNode)
	if !ok {
		http.Error(w, "bluetooth bridge not supported", http.StatusNotImplemented)
	}
	return bn, ok
}

// handleBluetoothPeers sets the peers the app has Bluetooth links to.
func (s *Server) handleBluetoothPeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bn, ok := s.bridgeNode(w)
	if !ok {
		return
	}
	var req struct {
		Peers []string `json:"peers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	bn.SetBluetoothPeers(req.Peers)
	writeJSON(w, http.StatusOK, map[string]int{"peers": len(req.Peers)})
}

// handleBluetoothOutbox long-polls (up to ?wait=seconds, max 25) for frames
// the app should send over its Bluetooth links.
func (s *Server) handleBluetoothOutbox(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bn, ok := s.bridgeNode(w)
	if !ok {
		return
	}
	wait := 20
	if raw := r.URL.Query().Get("wait"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			http.Error(w, "invalid wait param", http.StatusBadRequest)
			return
		}
		wait = min(parsed, 25)
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(wait)*time.Second)
	defer cancel()
	frames := bn.BluetoothOutbox(ctx)
	if frames == nil {
		frames = []BridgeFrame{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"frames": frames})
}

// handleBluetoothInbox takes a frame the app received over Bluetooth.
func (s *Server) handleBluetoothInbox(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bn, ok := s.bridgeNode(w)
	if !ok {
		return
	}
	var req struct {
		From string `json:"from"`
		Data []byte `json:"data"` // base64 in JSON
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if err := bn.BluetoothInbox(req.From, req.Data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted"})
}

type ConnectPeerRequest struct {
	Address string `json:"address"`
}

func (s *Server) handleConnectPeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ConnectPeerRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	if err := s.node.ConnectPeer(r.Context(), req.Address); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "connected",
		"address": req.Address,
	})
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	afterID := -1
	if raw := r.URL.Query().Get("after"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "invalid after param", http.StatusBadRequest)
			return
		}
		afterID = parsed
	}

	messages := s.store.Since(afterID)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"messages": messages,
	})
}
