package processing

import (
	"sync"
	"time"
)

// seenCache remembers message IDs so gossip/flooding never loops or
// delivers a message twice.
type seenCache struct {
	mu    sync.Mutex
	items map[string]time.Time
	ttl   time.Duration
	max   int
}

func newSeenCache(ttl time.Duration, max int) *seenCache {
	return &seenCache{items: make(map[string]time.Time), ttl: ttl, max: max}
}

// Seen returns true if id was already recorded; otherwise records it and returns false.
func (c *seenCache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if t, ok := c.items[id]; ok && now.Sub(t) < c.ttl {
		return true
	}
	if len(c.items) >= c.max {
		for k, t := range c.items {
			if now.Sub(t) >= c.ttl {
				delete(c.items, k)
			}
		}
	}
	c.items[id] = now
	return false
}
