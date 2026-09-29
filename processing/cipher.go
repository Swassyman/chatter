package processing

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
)

// Cipher encrypts/decrypts message payloads end-to-end (relays only see ciphertext).
type Cipher interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// AESGCM is a Cipher using a shared 16/24/32-byte key (e.g. a room key).
type AESGCM struct{ aead cipher.AEAD }

func NewAESGCM(key []byte) (*AESGCM, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCM{aead: aead}, nil
}

// KeyFromPassphrase derives a 32-byte key. Fine for a demo; use a real KDF
// (argon2/scrypt) or proper key exchange for production.
func KeyFromPassphrase(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

func (a *AESGCM) Encrypt(pt []byte) ([]byte, error) {
	nonce := make([]byte, a.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return a.aead.Seal(nonce, nonce, pt, nil), nil // nonce || ciphertext
}

func (a *AESGCM) Decrypt(ct []byte) ([]byte, error) {
	ns := a.aead.NonceSize()
	if len(ct) < ns {
		return nil, errors.New("ciphertext too short")
	}
	return a.aead.Open(nil, ct[:ns], ct[ns:], nil)
}
