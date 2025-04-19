package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// KeyManager manages encryption and integrity keys
type KeyManager struct {
	// EncryptionKey is used for AES-GCM encryption
	EncryptionKey []byte

	// HMACKey is used for HMAC-SHA256 integrity verification
	HMACKey []byte
	
	// PreviousKeys stores recently rotated keys for decryption of older data
	PreviousKeys []RotatedKey
	
	// KeyRotationInterval defines how often keys should be rotated
	KeyRotationInterval time.Duration
	
	// MaxPreviousKeys is the maximum number of previous keys to keep
	MaxPreviousKeys int
	
	// LastRotation tracks when keys were last rotated
	LastRotation time.Time
	
	// RotationMutex protects key rotation operations
	RotationMutex sync.RWMutex
}

// RotatedKey represents a previously used encryption/HMAC key pair
type RotatedKey struct {
	// EncryptionKey is the old encryption key
	EncryptionKey []byte
	
	// HMACKey is the old HMAC key
	HMACKey []byte
	
	// RotatedAt is when this key was rotated out
	RotatedAt time.Time
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
		EncryptionKey:       encKey,
		HMACKey:             hmacKey,
		PreviousKeys:        make([]RotatedKey, 0),
		KeyRotationInterval: 24 * time.Hour, // Default to daily rotation
		MaxPreviousKeys:     5,              // Keep 5 previous keys by default
		LastRotation:        time.Now(),
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
		EncryptionKey:       encryptionKey,
		HMACKey:             hmacKey,
		PreviousKeys:        make([]RotatedKey, 0),
		KeyRotationInterval: 24 * time.Hour, // Default to daily rotation
		MaxPreviousKeys:     5,              // Keep 5 previous keys by default
		LastRotation:        time.Now(),
	}, nil
}

// RotateKeys generates new encryption and HMAC keys, storing the current ones as previous keys
func (km *KeyManager) RotateKeys() error {
	km.RotationMutex.Lock()
	defer km.RotationMutex.Unlock()
	
	// Save current keys as previous
	previousKey := RotatedKey{
		EncryptionKey: km.EncryptionKey,
		HMACKey:       km.HMACKey,
		RotatedAt:     time.Now(),
	}
	
	// Add to previous keys
	km.PreviousKeys = append(km.PreviousKeys, previousKey)
	
	// Trim the list if it exceeds MaxPreviousKeys
	if len(km.PreviousKeys) > km.MaxPreviousKeys {
		km.PreviousKeys = km.PreviousKeys[len(km.PreviousKeys)-km.MaxPreviousKeys:]
	}
	
	// Generate new keys
	encKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, encKey); err != nil {
		return fmt.Errorf("failed to generate new encryption key: %w", err)
	}
	
	hmacKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, hmacKey); err != nil {
		return fmt.Errorf("failed to generate new HMAC key: %w", err)
	}
	
	// Update current keys
	km.EncryptionKey = encKey
	km.HMACKey = hmacKey
	km.LastRotation = time.Now()
	
	return nil
}

// CheckAndRotateKeys checks if keys need rotation based on the rotation interval
func (km *KeyManager) CheckAndRotateKeys() error {
	km.RotationMutex.RLock()
	needsRotation := time.Since(km.LastRotation) >= km.KeyRotationInterval
	km.RotationMutex.RUnlock()
	
	if needsRotation {
		return km.RotateKeys()
	}
	
	return nil
}

// EncryptWithAESGCM encrypts data using AES-GCM
func (km *KeyManager) EncryptWithAESGCM(plaintext []byte) ([]byte, error) {
	km.RotationMutex.RLock()
	encKey := km.EncryptionKey
	km.RotationMutex.RUnlock()
	
	// Create a new AES cipher block
	block, err := aes.NewCipher(encKey)
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
	// Try with current key first
	km.RotationMutex.RLock()
	currentKey := km.EncryptionKey
	previousKeys := km.PreviousKeys
	km.RotationMutex.RUnlock()
	
	plaintext, err := km.decryptWithKey(ciphertext, currentKey)
	if err == nil {
		return plaintext, nil
	}
	
	// Try with previous keys if current key fails
	for _, prevKey := range previousKeys {
		plaintext, err = km.decryptWithKey(ciphertext, prevKey.EncryptionKey)
		if err == nil {
			// Successfully decrypted with a previous key
			// Consider re-encrypting with current key in production systems
			return plaintext, nil
		}
	}
	
	// If we get here, decryption failed with all keys
	return nil, fmt.Errorf("failed to decrypt: data may be corrupted or encrypted with unknown key")
}

// decryptWithKey tries to decrypt using a specific key
func (km *KeyManager) decryptWithKey(ciphertext, key []byte) ([]byte, error) {
	// Create a new AES cipher block
	block, err := aes.NewCipher(key)
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
	km.RotationMutex.RLock()
	hmacKey := km.HMACKey
	km.RotationMutex.RUnlock()
	
	h := hmac.New(sha256.New, hmacKey)
	h.Write(data)
	return h.Sum(nil)
}

// VerifyHMAC verifies an HMAC-SHA256 hash
func (km *KeyManager) VerifyHMAC(data, providedHMAC []byte) bool {
	// Try with current key first
	km.RotationMutex.RLock()
	currentKey := km.HMACKey
	previousKeys := km.PreviousKeys
	km.RotationMutex.RUnlock()
	
	// Check with current key
	h := hmac.New(sha256.New, currentKey)
	h.Write(data)
	expectedHMAC := h.Sum(nil)
	
	if hmac.Equal(expectedHMAC, providedHMAC) {
		return true
	}
	
	// Try with previous keys
	for _, prevKey := range previousKeys {
		h = hmac.New(sha256.New, prevKey.HMACKey)
		h.Write(data)
		expectedHMAC = h.Sum(nil)
		
		if hmac.Equal(expectedHMAC, providedHMAC) {
			return true
		}
	}
	
	return false
}

// GenerateEnhancedHMAC generates an HMAC that includes metadata for extra security
func (km *KeyManager) GenerateEnhancedHMAC(data []byte, metadata string) []byte {
	km.RotationMutex.RLock()
	hmacKey := km.HMACKey
	km.RotationMutex.RUnlock()
	
	// Combine data and metadata
	combinedData := append(data, []byte(metadata)...)
	
	h := hmac.New(sha256.New, hmacKey)
	h.Write(combinedData)
	return h.Sum(nil)
}

// VerifyEnhancedHMAC verifies an HMAC with metadata
func (km *KeyManager) VerifyEnhancedHMAC(data []byte, metadata string, providedHMAC []byte) bool {
	// Try with current key first
	km.RotationMutex.RLock()
	currentKey := km.HMACKey
	previousKeys := km.PreviousKeys
	km.RotationMutex.RUnlock()
	
	// Combine data and metadata
	combinedData := append(data, []byte(metadata)...)
	
	// Check with current key
	h := hmac.New(sha256.New, currentKey)
	h.Write(combinedData)
	expectedHMAC := h.Sum(nil)
	
	if hmac.Equal(expectedHMAC, providedHMAC) {
		return true
	}
	
	// Try with previous keys
	for _, prevKey := range previousKeys {
		h = hmac.New(sha256.New, prevKey.HMACKey)
		h.Write(combinedData)
		expectedHMAC = h.Sum(nil)
		
		if hmac.Equal(expectedHMAC, providedHMAC) {
			return true
		}
	}
	
	return false
}

// SecureData represents encrypted data with integrity protection
type SecureData struct {
	// Ciphertext is the encrypted data
	Ciphertext []byte

	// HMAC is the integrity hash
	HMAC []byte
	
	// Version indicates which version of the encryption was used
	Version byte
	
	// CreatedAt is when this secure data was created
	CreatedAt time.Time
	
	// Metadata is optional metadata about the secured data
	Metadata string
}

// EncryptAndAuthenticate encrypts and authenticates data
func (km *KeyManager) EncryptAndAuthenticate(plaintext []byte) (*SecureData, error) {
	// Check if keys need rotation before encryption
	km.CheckAndRotateKeys()
	
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
		Version:    1, // Current version
		CreatedAt:  time.Now(),
	}, nil
}

// EncryptAndAuthenticateWithMetadata encrypts and authenticates data with metadata
func (km *KeyManager) EncryptAndAuthenticateWithMetadata(plaintext []byte, metadata string) (*SecureData, error) {
	// Check if keys need rotation before encryption
	km.CheckAndRotateKeys()
	
	// Encrypt the plaintext
	ciphertext, err := km.EncryptWithAESGCM(plaintext)
	if err != nil {
		return nil, fmt.Errorf("encryption failed: %w", err)
	}

	// Generate enhanced HMAC using metadata
	hmacValue := km.GenerateEnhancedHMAC(ciphertext, metadata)

	return &SecureData{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
		Version:    2, // Enhanced version with metadata
		CreatedAt:  time.Now(),
		Metadata:   metadata,
	}, nil
}

// DecryptAndVerify decrypts and verifies data
func (km *KeyManager) DecryptAndVerify(secureData *SecureData) ([]byte, error) {
	// Verify the HMAC
	var hmacValid bool
	
	if secureData.Version >= 2 && secureData.Metadata != "" {
		// Enhanced HMAC with metadata
		hmacValid = km.VerifyEnhancedHMAC(secureData.Ciphertext, secureData.Metadata, secureData.HMAC)
	} else {
		// Standard HMAC
		hmacValid = km.VerifyHMAC(secureData.Ciphertext, secureData.HMAC)
	}
	
	if !hmacValid {
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
	versionStr := fmt.Sprintf("%d", secureData.Version)
	timestampStr := fmt.Sprintf("%d", secureData.CreatedAt.UnixNano())
	
	// Format with separators that won't appear in hex-encoded data
	parts := []string{versionStr, ciphertextHex, hmacHex, timestampStr}
	
	// Add metadata if present
	if secureData.Version >= 2 && secureData.Metadata != "" {
		parts = append(parts, secureData.Metadata)
	}
	
	return strings.Join(parts, "|")
}

// DeserializeSecureData deserializes secure data from a string
func DeserializeSecureData(serialized string) (*SecureData, error) {
	parts := strings.Split(serialized, "|")
	if len(parts) < 4 {
		return nil, fmt.Errorf("failed to parse serialized secure data: invalid format")
	}

	versionStr := parts[0]
	ciphertextHex := parts[1]
	hmacHex := parts[2]
	timestampStr := parts[3]
	
	version, err := parseVersion(versionStr)
	if err != nil {
		return nil, err
	}
	
	ciphertext, err := hex.DecodeString(ciphertextHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ciphertext: %w", err)
	}

	hmacValue, err := hex.DecodeString(hmacHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode HMAC: %w", err)
	}
	
	timestamp, err := parseTimestamp(timestampStr)
	if err != nil {
		return nil, err
	}
	
	secureData := &SecureData{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
		Version:    version,
		CreatedAt:  timestamp,
	}
	
	// Extract metadata if present for version 2+
	if version >= 2 && len(parts) >= 5 {
		secureData.Metadata = parts[4]
	}

	return secureData, nil
}

// parseVersion parses the version string
func parseVersion(versionStr string) (byte, error) {
	var version int
	_, err := fmt.Sscanf(versionStr, "%d", &version)
	if err != nil {
		return 0, fmt.Errorf("failed to parse version: %w", err)
	}
	return byte(version), nil
}

// parseTimestamp parses the timestamp string
func parseTimestamp(timestampStr string) (time.Time, error) {
	var timestampNano int64
	_, err := fmt.Sscanf(timestampStr, "%d", &timestampNano)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	return time.Unix(0, timestampNano), nil
}

// EncodeBase64 encodes binary data as base64 string
func EncodeBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// DecodeBase64 decodes base64 string to binary data
func DecodeBase64(str string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(str)
}

// EncryptAndEncode encrypts and then base64-encodes data
func (km *KeyManager) EncryptAndEncode(plaintext []byte) (string, error) {
	ciphertext, err := km.EncryptWithAESGCM(plaintext)
	if err != nil {
		return "", err
	}
	return EncodeBase64(ciphertext), nil
}

// DecodeAndDecrypt decodes base64 and then decrypts data
func (km *KeyManager) DecodeAndDecrypt(encoded string) ([]byte, error) {
	ciphertext, err := DecodeBase64(encoded)
	if err != nil {
		return nil, err
	}
	return km.DecryptWithAESGCM(ciphertext)
}

// GenerateHMACBase64 generates and base64-encodes an HMAC for the data
func (km *KeyManager) GenerateHMACBase64(data []byte) string {
	hmac := km.GenerateHMAC(data)
	return EncodeBase64(hmac)
}

// VerifyHMACBase64 verifies a base64-encoded HMAC against the data
func (km *KeyManager) VerifyHMACBase64(data []byte, encodedMAC string) bool {
	mac, err := DecodeBase64(encodedMAC)
	if err != nil {
		return false
	}
	return km.VerifyHMAC(data, mac)
}
