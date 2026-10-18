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
}

// MessageStore is a thread-safe in-memory store for received messages.
type MessageStore struct {
	mu       sync.RWMutex
	messages []ReceivedMessage
	nextID   int
}

// NewMessageStore creates and returns an empty MessageStore.
func NewMessageStore() *MessageStore {
	return &MessageStore{}
}

// Add records a new message from the given peer with the given content,
// assigning a monotonically increasing ID and an RFC3339 timestamp.
func (ms *MessageStore) Add(from, content string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.messages = append(ms.messages, ReceivedMessage{
		ID:        ms.nextID,
		From:      from,
		Content:   content,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
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
