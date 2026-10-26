package processing

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"sync"
)

// Rooms (mesh channels) are broadcast envelopes: To is empty and Room names
// the channel. Every node relays them whether or not it reads that room.
//
// An open room is named in the clear ("general"). A private room is a name
// plus a passphrase: its key is derived from both, its payloads are sealed
// with AES-256-GCM, and on the wire it goes by a tag derived from the key
// ("~" + hex), so relays learn neither the name nor the content, and the same
// name with a different passphrase is a different room.

// roomNamePattern is what a room is called: lowercase words joined by dashes.
var roomNamePattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

const (
	maxRoomNameLen = 32
	privateRoomTag = "~"

	// PBKDF2-HMAC-SHA256 work factor (OWASP's figure for SHA-256). Paid once
	// when a room is joined, never per message.
	roomKeyIterations = 600_000
)

var ErrUnknownRoom = errors.New("not a member of that private room")

// ValidRoomName reports whether name can name a room.
func ValidRoomName(name string) bool {
	return len(name) <= maxRoomNameLen && roomNamePattern.MatchString(name)
}

// IsPrivateRoom reports whether room is a private room's wire tag.
func IsPrivateRoom(room string) bool { return strings.HasPrefix(room, privateRoomTag) }

// validRoomField reports whether an envelope's Room field could be one we
// produce: an open room's name or a private room's tag.
func validRoomField(room string) bool {
	if IsPrivateRoom(room) {
		tag := strings.TrimPrefix(room, privateRoomTag)
		_, err := hex.DecodeString(tag)
		return err == nil && len(tag) == 32
	}
	return ValidRoomName(room)
}

// PrivateRoomKey derives a private room's key and wire tag from its name and
// passphrase. Everyone who knows both gets the same pair.
func PrivateRoomKey(name, passphrase string) (key []byte, tag string, err error) {
	if !ValidRoomName(name) {
		return nil, "", errors.New("room names are lowercase letters, digits and dashes, up to 32 characters")
	}
	if passphrase == "" {
		return nil, "", errors.New("a private room needs a passphrase")
	}
	key, err = pbkdf2.Key(sha256.New, passphrase, []byte("chatter/room/v1/"+name), roomKeyIterations, 32)
	if err != nil {
		return nil, "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("chatter/room-tag/v1"))
	return key, privateRoomTag + hex.EncodeToString(mac.Sum(nil)[:16]), nil
}

type roomKeys struct {
	mu   sync.RWMutex
	byID map[string]*AESGCM
}

func (r *roomKeys) get(tag string) *AESGCM {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[tag]
}

// JoinPrivateRoom lets this node read and post in the private room name,
// keyed by passphrase, and returns the tag it goes by on the wire.
func (p *Processor) JoinPrivateRoom(name, passphrase string) (string, error) {
	key, tag, err := PrivateRoomKey(name, passphrase)
	if err != nil {
		return "", err
	}
	c, err := NewAESGCM(key)
	if err != nil {
		return "", err
	}
	p.rooms.mu.Lock()
	p.rooms.byID[tag] = c
	p.rooms.mu.Unlock()
	return tag, nil
}

// LeavePrivateRoom forgets a private room's key; its messages are still
// relayed, but no longer read.
func (p *Processor) LeavePrivateRoom(tag string) {
	p.rooms.mu.Lock()
	delete(p.rooms.byID, tag)
	p.rooms.mu.Unlock()
}
