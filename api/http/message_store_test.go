package httpapi

import (
	"testing"
	"time"
)

// Two messages received within the same second keep their order by time.
func TestMessageTimestampsHaveMilliseconds(t *testing.T) {
	ms := NewMessageStore()
	ms.AddMessage(ReceivedMessage{From: "a", Content: "first"})
	time.Sleep(5 * time.Millisecond)
	ms.AddMessage(ReceivedMessage{From: "a", Content: "second"})

	got := ms.Since(-1)
	first, err := time.Parse(time.RFC3339Nano, got[0].Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	second, err := time.Parse(time.RFC3339Nano, got[1].Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	if !second.After(first) {
		t.Fatalf("second message (%s) not after first (%s)", got[1].Timestamp, got[0].Timestamp)
	}

	// A history post keeps the time it was given
	ms.AddMessage(ReceivedMessage{From: "b", Content: "old", Timestamp: "2026-10-26T08:00:00.250Z", History: true})
	if ts := ms.Since(-1)[2].Timestamp; ts != "2026-10-26T08:00:00.250Z" {
		t.Fatalf("history timestamp rewritten to %s", ts)
	}
}
