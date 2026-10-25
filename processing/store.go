package processing

import (
	"sync"
	"time"
)

// ChatMessage is what the application layer sees.
type ChatMessage struct {
	ID        string
	From      string
	To        string // empty for broadcast
	Room      string
	Content   string
	Timestamp time.Time
	Outgoing  bool
}

// MessageStore is the "store / retrieve from local storage" box.
// Swap MemoryStore for SQLite/BoltDB later without touching the processor.
type MessageStore interface {
	Save(ChatMessage) error
	List(limit int) ([]ChatMessage, error)
}

type MemoryStore struct {
	mu   sync.RWMutex
	msgs []ChatMessage
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Save(m ChatMessage) error {
	s.mu.Lock()
	s.msgs = append(s.msgs, m)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) List(limit int) ([]ChatMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := 0
	if limit > 0 && len(s.msgs) > limit {
		start = len(s.msgs) - limit
	}
	out := make([]ChatMessage, len(s.msgs)-start)
	copy(out, s.msgs[start:])
	return out, nil
}
