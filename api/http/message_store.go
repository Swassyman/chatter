package httpapi

import (
	"sync"
	"time"
)

// ReceivedMessage holds a single incoming chat message with metadata.
type ReceivedMessage struct {
	ID        int    `json:"id"`
	From      string `json:"from"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"` // RFC3339 format

	MsgID  string `json:"msg_id,omitempty"` // mesh message ID, referenced by receipts
	Handle string `json:"handle,omitempty"` // sender's handle, signed by the sender
	Via    string `json:"via,omitempty"`    // neighbour it arrived from; differs from From when relayed
	Room   string `json:"room,omitempty"`   // set for room posts: the room's name, or a private room's tag
}

// ReceivedReceipt reports that peer From received or read our messages.
type ReceivedReceipt struct {
	ID        int      `json:"id"`
	From      string   `json:"from"`
	IDs       []string `json:"ids"`
	Status    string   `json:"status"` // "delivered" or "read"
	Timestamp string   `json:"timestamp"`
	Via       string   `json:"via,omitempty"` // neighbour it arrived from; differs from From when relayed
}

// MessageStore is a thread-safe in-memory store for received messages and
// receipts.
type MessageStore struct {
	mu            sync.RWMutex
	messages      []ReceivedMessage
	nextID        int
	receipts      []ReceivedReceipt
	nextReceiptID int
}

// NewMessageStore creates and returns an empty MessageStore.
func NewMessageStore() *MessageStore {
	return &MessageStore{}
}

// Add records a new message from the given peer with the given content,
// assigning a monotonically increasing ID and an RFC3339 timestamp.
func (ms *MessageStore) Add(from, content string) {
	ms.AddMessage(ReceivedMessage{From: from, Content: content})
}

// AddMessage records m, assigning its ID and timestamp.
func (ms *MessageStore) AddMessage(m ReceivedMessage) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	m.ID = ms.nextID
	m.Timestamp = time.Now().UTC().Format(time.RFC3339)
	ms.messages = append(ms.messages, m)
	ms.nextID++
}

// Since returns all messages with ID strictly greater than afterID.
// Pass -1 to retrieve all messages.
func (ms *MessageStore) Since(afterID int) []ReceivedMessage {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]ReceivedMessage, 0)
	for _, m := range ms.messages {
		if m.ID > afterID {
			result = append(result, m)
		}
	}
	return result
}

// AddReceipt records a delivery or read receipt from peer `from` that
// arrived through neighbour `via`.
func (ms *MessageStore) AddReceipt(from, via string, ids []string, status string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.receipts = append(ms.receipts, ReceivedReceipt{
		ID:        ms.nextReceiptID,
		From:      from,
		IDs:       ids,
		Status:    status,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Via:       via,
	})
	ms.nextReceiptID++
}

// ReceiptsSince returns all receipts with ID strictly greater than afterID.
func (ms *MessageStore) ReceiptsSince(afterID int) []ReceivedReceipt {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]ReceivedReceipt, 0)
	for _, r := range ms.receipts {
		if r.ID > afterID {
			result = append(result, r)
		}
	}
	return result
}
