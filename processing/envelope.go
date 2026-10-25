package processing

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// MsgType identifies what an Envelope carries.
type MsgType string

const (
	TypeChat MsgType = "chat"
)

const (
	DefaultTTL = 8  // hops a message may travel by default
	MaxTTL     = 16 // anything above this is rejected
)

// Envelope is the wire format the processing layer hands to the transport
// layer (as JSON bytes) and receives back from it.
//
// TTL is the only field relays modify, so it is NOT covered by the signature.
type Envelope struct {
	ID        string  `json:"id"`             // unique message ID (dedup key)
	Type      MsgType `json:"type"`           // "chat"
	From      string  `json:"from"`           // ORIGINAL author's peer ID
	To        string  `json:"to,omitempty"`   // target peer ID; empty = broadcast
	Room      string  `json:"room,omitempty"` // chat room name (broadcast)
	Timestamp int64   `json:"ts"`             // unix millis
	TTL       int     `json:"ttl"`            // remaining hops
	Payload   []byte  `json:"payload"`        // content (AES-GCM ciphertext if a Cipher is set)
	Sig       []byte  `json:"sig"`            // signature by From's key
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// signingBytes is the canonical byte string that gets signed/verified.
// TTL and Sig are excluded on purpose.
func (e *Envelope) signingBytes() ([]byte, error) {
	return json.Marshal(struct {
		ID        string
		Type      MsgType
		From      string
		To        string
		Room      string
		Timestamp int64
		Payload   []byte
	}{e.ID, e.Type, e.From, e.To, e.Room, e.Timestamp, e.Payload})
}

func Encode(e *Envelope) ([]byte, error) { return json.Marshal(e) }

// Decode parses and sanity-checks an envelope. It does NOT verify the signature.
func Decode(b []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	switch {
	case e.ID == "" || len(e.ID) > 64:
		return nil, errors.New("invalid envelope id")
	case e.From == "":
		return nil, errors.New("envelope has no sender")
	case e.Type != TypeChat:
		return nil, fmt.Errorf("unknown message type %q", e.Type)
	case e.TTL < 0 || e.TTL > MaxTTL:
		return nil, fmt.Errorf("invalid ttl %d", e.TTL)
	case len(e.Sig) == 0:
		return nil, errors.New("envelope is unsigned")
	}
	return &e, nil
}
