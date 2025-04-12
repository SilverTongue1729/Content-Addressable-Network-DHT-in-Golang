package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// KeyManager manages encryption and integrity keys
type KeyManager struct {
	// EncryptionKey is used for AES-GCM encryption
	EncryptionKey []byte

	// HMACKey is used for HMAC-SHA256 integrity verification
	HMACKey []byte
}

// NewKeyManager creates a new key manager with randomly generated keys
func NewKeyManager() (*KeyManager, error) {
	// Generate a random 32-byte key for AES-256
	encKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, encKey); err != nil {
		return nil, fmt.Errorf("failed to generate encryption key: %w", err)
	}

	// Generate a random 32-byte key for HMAC-SHA256
	hmacKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, hmacKey); err != nil {
		return nil, fmt.Errorf("failed to generate HMAC key: %w", err)
	}

	return &KeyManager{
		EncryptionKey: encKey,
		HMACKey:       hmacKey,
	}, nil
}

// NewKeyManagerFromKeys creates a key manager from existing keys
func NewKeyManagerFromKeys(encryptionKey, hmacKey []byte) (*KeyManager, error) {
	if len(encryptionKey) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes")
	}
	if len(hmacKey) != 32 {
		return nil, fmt.Errorf("HMAC key must be 32 bytes")
	}

	return &KeyManager{
		EncryptionKey: encryptionKey,
		HMACKey:       hmacKey,
	}, nil
}

// EncryptWithAESGCM encrypts data using AES-GCM
func (km *KeyManager) EncryptWithAESGCM(plaintext []byte) ([]byte, error) {
	// Create a new AES cipher block
	block, err := aes.NewCipher(km.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	// Create a new GCM cipher
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate a random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt and authenticate the plaintext
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)

	return ciphertext, nil
}

// DecryptWithAESGCM decrypts data encrypted with AES-GCM
func (km *KeyManager) DecryptWithAESGCM(ciphertext []byte) ([]byte, error) {
	// Create a new AES cipher block
	block, err := aes.NewCipher(km.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	// Create a new GCM cipher
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Ensure the ciphertext is large enough to contain the nonce
	if len(ciphertext) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// Extract the nonce from the ciphertext
	nonce, ciphertextWithoutNonce := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]

	// Decrypt and verify the ciphertext
	plaintext, err := gcm.Open(nil, nonce, ciphertextWithoutNonce, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// GenerateHMAC generates an HMAC-SHA256 hash for data
func (km *KeyManager) GenerateHMAC(data []byte) []byte {
	h := hmac.New(sha256.New, km.HMACKey)
	h.Write(data)
	return h.Sum(nil)
}

// VerifyHMAC verifies an HMAC-SHA256 hash
func (km *KeyManager) VerifyHMAC(data, providedHMAC []byte) bool {
	expectedHMAC := km.GenerateHMAC(data)
	return hmac.Equal(expectedHMAC, providedHMAC)
}

// SecureData represents encrypted data with integrity protection
type SecureData struct {
	// Ciphertext is the encrypted data
	Ciphertext []byte

	// HMAC is the integrity hash
	HMAC []byte
}

// EncryptAndAuthenticate encrypts and authenticates data
func (km *KeyManager) EncryptAndAuthenticate(plaintext []byte) (*SecureData, error) {
	// Encrypt the plaintext
	ciphertext, err := km.EncryptWithAESGCM(plaintext)
	if err != nil {
		return nil, fmt.Errorf("encryption failed: %w", err)
	}

	// Generate HMAC for the ciphertext
	hmacValue := km.GenerateHMAC(ciphertext)

	return &SecureData{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
	}, nil
}

// DecryptAndVerify decrypts and verifies data
func (km *KeyManager) DecryptAndVerify(secureData *SecureData) ([]byte, error) {
	// Verify the HMAC
	if !km.VerifyHMAC(secureData.Ciphertext, secureData.HMAC) {
		return nil, fmt.Errorf("HMAC verification failed")
	}

	// Decrypt the ciphertext
	plaintext, err := km.DecryptWithAESGCM(secureData.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return plaintext, nil
}

// SerializeSecureData serializes secure data to a string
func SerializeSecureData(secureData *SecureData) string {
	ciphertextHex := hex.EncodeToString(secureData.Ciphertext)
	hmacHex := hex.EncodeToString(secureData.HMAC)
	// Use a separator that won't appear in hex-encoded data
	return fmt.Sprintf("%s|%s", ciphertextHex, hmacHex)
}

// DeserializeSecureData deserializes secure data from a string
func DeserializeSecureData(serialized string) (*SecureData, error) {
	parts := strings.Split(serialized, "|")
	if len(parts) != 2 {
		return nil, fmt.Errorf("failed to parse serialized secure data: invalid format")
	}

	ciphertextHex := parts[0]
	hmacHex := parts[1]

	ciphertext, err := hex.DecodeString(ciphertextHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ciphertext: %w", err)
	}

	hmacValue, err := hex.DecodeString(hmacHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode HMAC: %w", err)
	}

	return &SecureData{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
	}, nil
}
