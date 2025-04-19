package crypto

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// KeyVersion represents a version of the encryption keys
type KeyVersion struct {
	// Version is the unique identifier for this key version
	Version int `json:"version"`
	
	// EncryptionKey is the AES-GCM encryption key
	EncryptionKey string `json:"encryption_key"`
	
	// HMACKey is the HMAC-SHA256 integrity key
	HMACKey string `json:"hmac_key"`
	
	// CreatedAt is when this key version was created
	CreatedAt time.Time `json:"created_at"`
}

// KeyStorage manages persistent storage of encryption keys
type KeyStorage struct {
	// CurrentVersion is the active key version
	CurrentVersion int `json:"current_version"`
	
	// KeyVersions is a map of all key versions
	KeyVersions map[int]KeyVersion `json:"key_versions"`
	
	// FilePath is where the keys are stored
	FilePath string `json:"-"`
	
	// mu is a mutex for thread safety
	mu sync.RWMutex `json:"-"`
}

// NewKeyStorage creates a new key storage
func NewKeyStorage(filePath string) (*KeyStorage, error) {
	ks := &KeyStorage{
		CurrentVersion: 1,
		KeyVersions:    make(map[int]KeyVersion),
		FilePath:       filePath,
	}
	
	// Check if the key file exists
	if _, err := os.Stat(filePath); err == nil {
		// File exists, load it
		if err := ks.Load(); err != nil {
			return nil, fmt.Errorf("failed to load key storage: %w", err)
		}
	} else if os.IsNotExist(err) {
		// File doesn't exist, generate initial keys
		if err := ks.generateInitialKeys(); err != nil {
			return nil, fmt.Errorf("failed to generate initial keys: %w", err)
		}
		
		// Save the new keys
		if err := ks.Save(); err != nil {
			return nil, fmt.Errorf("failed to save initial keys: %w", err)
		}
	} else {
		// Some other error
		return nil, fmt.Errorf("failed to check key file: %w", err)
	}
	
	return ks, nil
}

// generateInitialKeys creates the first key version
func (ks *KeyStorage) generateInitialKeys() error {
	// Generate encryption key
	encKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, encKey); err != nil {
		return fmt.Errorf("failed to generate encryption key: %w", err)
	}
	
	// Generate HMAC key
	hmacKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, hmacKey); err != nil {
		return fmt.Errorf("failed to generate HMAC key: %w", err)
	}
	
	// Create key version
	keyVersion := KeyVersion{
		Version:       1,
		EncryptionKey: hex.EncodeToString(encKey),
		HMACKey:       hex.EncodeToString(hmacKey),
		CreatedAt:     time.Now(),
	}
	
	// Add to key versions map
	ks.KeyVersions[1] = keyVersion
	ks.CurrentVersion = 1
	
	return nil
}

// Load loads key storage from file
func (ks *KeyStorage) Load() error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	
	// Read file
	data, err := os.ReadFile(ks.FilePath)
	if err != nil {
		return fmt.Errorf("failed to read key file: %w", err)
	}
	
	// Unmarshal JSON
	if err := json.Unmarshal(data, ks); err != nil {
		return fmt.Errorf("failed to parse key file: %w", err)
	}
	
	return nil
}

// Save saves key storage to file
func (ks *KeyStorage) Save() error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	
	// Create directory if it doesn't exist
	dir := filepath.Dir(ks.FilePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory for key file: %w", err)
	}
	
	// Marshal to JSON
	data, err := json.MarshalIndent(ks, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal key storage: %w", err)
	}
	
	// Write to temporary file
	tempFile := ks.FilePath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write temporary key file: %w", err)
	}
	
	// Rename temporary file to actual file (atomic operation)
	if err := os.Rename(tempFile, ks.FilePath); err != nil {
		return fmt.Errorf("failed to rename temporary key file: %w", err)
	}
	
	return nil
}

// GetCurrentKeyVersion gets the current key version
func (ks *KeyStorage) GetCurrentKeyVersion() (*KeyVersion, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	
	keyVersion, exists := ks.KeyVersions[ks.CurrentVersion]
	if !exists {
		return nil, fmt.Errorf("current key version %d not found", ks.CurrentVersion)
	}
	
	return &keyVersion, nil
}

// GetKeyVersion gets a specific key version
func (ks *KeyStorage) GetKeyVersion(version int) (*KeyVersion, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	
	keyVersion, exists := ks.KeyVersions[version]
	if !exists {
		return nil, fmt.Errorf("key version %d not found", version)
	}
	
	return &keyVersion, nil
}

// RotateKeys generates a new key version and makes it the current version
func (ks *KeyStorage) RotateKeys() error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	
	// New version is current version + 1
	newVersion := ks.CurrentVersion + 1
	
	// Generate encryption key
	encKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, encKey); err != nil {
		return fmt.Errorf("failed to generate encryption key: %w", err)
	}
	
	// Generate HMAC key
	hmacKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, hmacKey); err != nil {
		return fmt.Errorf("failed to generate HMAC key: %w", err)
	}
	
	// Create key version
	keyVersion := KeyVersion{
		Version:       newVersion,
		EncryptionKey: hex.EncodeToString(encKey),
		HMACKey:       hex.EncodeToString(hmacKey),
		CreatedAt:     time.Now(),
	}
	
	// Add to key versions map
	ks.KeyVersions[newVersion] = keyVersion
	ks.CurrentVersion = newVersion
	
	// Save changes
	return ks.Save()
}

// Common errors
var (
	ErrKeyCorruption = errors.New("key corruption detected")
	ErrKeyNotFound   = errors.New("key not found")
)

// PersistentKeyManager is a key manager with persistent storage and versioning
type PersistentKeyManager struct {
	// KeyStorage is the persistent storage for keys
	KeyStorage *KeyStorage
	
	// EncryptionKey is the current encryption key
	EncryptionKey []byte
	
	// HMACKey is the current HMAC key
	HMACKey []byte
	
	// CurrentVersion is the version of the current keys
	CurrentVersion int
	
	// KeyRotationInterval is how often to rotate keys (0 means no automatic rotation)
	KeyRotationInterval time.Duration
	
	// lastRotation is when keys were last rotated
	lastRotation time.Time
	
	// mu is a mutex for thread safety
	mu sync.RWMutex
}

// NewPersistentKeyManager creates a new persistent key manager
func NewPersistentKeyManager(keyFilePath string, rotationInterval time.Duration) (*PersistentKeyManager, error) {
	// Create or load key storage
	keyStorage, err := NewKeyStorage(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize key storage: %w", err)
	}
	
	// Get current key version
	currentKeyVersion, err := keyStorage.GetCurrentKeyVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get current key version: %w", err)
	}
	
	// Decode keys
	encryptionKey, err := hex.DecodeString(currentKeyVersion.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encryption key: %w", err)
	}
	
	hmacKey, err := hex.DecodeString(currentKeyVersion.HMACKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode HMAC key: %w", err)
	}
	
	km := &PersistentKeyManager{
		KeyStorage:         keyStorage,
		EncryptionKey:      encryptionKey,
		HMACKey:            hmacKey,
		CurrentVersion:     currentKeyVersion.Version,
		KeyRotationInterval: rotationInterval,
		lastRotation:       currentKeyVersion.CreatedAt,
	}
	
	// Start rotation scheduler if needed
	if rotationInterval > 0 {
		go km.rotationScheduler()
	}
	
	return km, nil
}

// rotationScheduler periodically rotates keys
func (km *PersistentKeyManager) rotationScheduler() {
	ticker := time.NewTicker(1 * time.Hour) // Check every hour
	defer ticker.Stop()
	
	for range ticker.C {
		km.mu.RLock()
		interval := km.KeyRotationInterval
		lastRotation := km.lastRotation
		km.mu.RUnlock()
		
		if interval > 0 && time.Since(lastRotation) >= interval {
			if err := km.RotateKeys(); err != nil {
				// Log error but continue
				fmt.Printf("Failed to rotate keys: %v\n", err)
			}
		}
	}
}

// RotateKeys generates new keys and updates current keys
func (km *PersistentKeyManager) RotateKeys() error {
	km.mu.Lock()
	defer km.mu.Unlock()
	
	// Rotate keys in storage
	if err := km.KeyStorage.RotateKeys(); err != nil {
		return fmt.Errorf("failed to rotate keys in storage: %w", err)
	}
	
	// Get new current key version
	currentKeyVersion, err := km.KeyStorage.GetCurrentKeyVersion()
	if err != nil {
		return fmt.Errorf("failed to get current key version after rotation: %w", err)
	}
	
	// Decode keys
	encryptionKey, err := hex.DecodeString(currentKeyVersion.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decode encryption key: %w", err)
	}
	
	hmacKey, err := hex.DecodeString(currentKeyVersion.HMACKey)
	if err != nil {
		return fmt.Errorf("failed to decode HMAC key: %w", err)
	}
	
	// Update current keys
	km.EncryptionKey = encryptionKey
	km.HMACKey = hmacKey
	km.CurrentVersion = currentKeyVersion.Version
	km.lastRotation = currentKeyVersion.CreatedAt
	
	return nil
}

// ResetToDefaultKey resets to the most recent key in case of corruption
func (km *PersistentKeyManager) ResetToDefaultKey() error {
	km.mu.Lock()
	defer km.mu.Unlock()
	
	// Reload from storage
	if err := km.KeyStorage.Load(); err != nil {
		return fmt.Errorf("failed to reload key storage: %w", err)
	}
	
	// Get current key version
	currentKeyVersion, err := km.KeyStorage.GetCurrentKeyVersion()
	if err != nil {
		return fmt.Errorf("failed to get current key version: %w", err)
	}
	
	// Decode keys
	encryptionKey, err := hex.DecodeString(currentKeyVersion.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decode encryption key: %w", err)
	}
	
	hmacKey, err := hex.DecodeString(currentKeyVersion.HMACKey)
	if err != nil {
		return fmt.Errorf("failed to decode HMAC key: %w", err)
	}
	
	// Update current keys
	km.EncryptionKey = encryptionKey
	km.HMACKey = hmacKey
	km.CurrentVersion = currentKeyVersion.Version
	
	return nil
}

// SaveKeysToFile saves the current keys to a file
func (km *PersistentKeyManager) SaveKeysToFile(filePath string) error {
	km.mu.RLock()
	defer km.mu.RUnlock()
	
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Create key export data
	exportData := struct {
		EncryptionKey string `json:"encryption_key"`
		HMACKey       string `json:"hmac_key"`
		Version       int    `json:"version"`
		ExportedAt    string `json:"exported_at"`
	}{
		EncryptionKey: hex.EncodeToString(km.EncryptionKey),
		HMACKey:       hex.EncodeToString(km.HMACKey),
		Version:       km.CurrentVersion,
		ExportedAt:    time.Now().Format(time.RFC3339),
	}
	
	// Marshal to JSON
	data, err := json.MarshalIndent(exportData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal key export: %w", err)
	}
	
	// Write to file
	if err := os.WriteFile(filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write key export file: %w", err)
	}
	
	return nil
}

// LoadKeysFromFile loads keys from a file
func (km *PersistentKeyManager) LoadKeysFromFile(filePath string) error {
	km.mu.Lock()
	defer km.mu.Unlock()
	
	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read key file: %w", err)
	}
	
	// Parse JSON
	var importData struct {
		EncryptionKey string `json:"encryption_key"`
		HMACKey       string `json:"hmac_key"`
		Version       int    `json:"version"`
	}
	
	if err := json.Unmarshal(data, &importData); err != nil {
		return fmt.Errorf("failed to parse key file: %w", err)
	}
	
	// Decode keys
	encryptionKey, err := hex.DecodeString(importData.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decode encryption key: %w", err)
	}
	
	hmacKey, err := hex.DecodeString(importData.HMACKey)
	if err != nil {
		return fmt.Errorf("failed to decode HMAC key: %w", err)
	}
	
	// Update current keys
	km.EncryptionKey = encryptionKey
	km.HMACKey = hmacKey
	
	return nil
}

// GetCurrentKeyVersion returns the current key version
func (km *PersistentKeyManager) GetCurrentKeyVersion() int {
	km.mu.RLock()
	defer km.mu.RUnlock()
	return km.CurrentVersion
}

// EncryptAndAuthenticateWithVersion encrypts and authenticates data with version
func (km *PersistentKeyManager) EncryptAndAuthenticateWithVersion(plaintext []byte) (*SecureDataWithVersion, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()
	
	// Create a temporary KeyManager with current keys
	tempKM := &KeyManager{
		EncryptionKey: km.EncryptionKey,
		HMACKey:       km.HMACKey,
	}
	
	// Use the standard encryption method
	secureData, err := tempKM.EncryptAndAuthenticate(plaintext)
	if err != nil {
		return nil, err
	}
	
	// Add version information
	return &SecureDataWithVersion{
		Ciphertext: secureData.Ciphertext,
		HMAC:       secureData.HMAC,
		Version:    km.CurrentVersion,
	}, nil
}

// DecryptAndVerifyWithVersion decrypts and verifies data with version
func (km *PersistentKeyManager) DecryptAndVerifyWithVersion(secureData *SecureDataWithVersion) ([]byte, error) {
	// Get key version needed for this data
	keyVersion, err := km.KeyStorage.GetKeyVersion(secureData.Version)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyNotFound, err)
	}
	
	// Decode keys for this version
	encryptionKey, err := hex.DecodeString(keyVersion.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encryption key: %w", err)
	}
	
	hmacKey, err := hex.DecodeString(keyVersion.HMACKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode HMAC key: %w", err)
	}
	
	// Create a temporary KeyManager with version-specific keys
	tempKM := &KeyManager{
		EncryptionKey: encryptionKey,
		HMACKey:       hmacKey,
	}
	
	// Use the standard decryption method
	plaintext, err := tempKM.DecryptAndVerify(&SecureData{
		Ciphertext: secureData.Ciphertext,
		HMAC:       secureData.HMAC,
	})
	
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyCorruption, err)
	}
	
	return plaintext, nil
}

// SecureDataWithVersion includes version information with secure data
type SecureDataWithVersion struct {
	// Ciphertext is the encrypted data
	Ciphertext []byte
	
	// HMAC is the integrity hash
	HMAC []byte
	
	// Version is the key version used for encryption
	Version int
}

// SerializeSecureDataWithVersion serializes secure data with version
func SerializeSecureDataWithVersion(secureData *SecureDataWithVersion) string {
	ciphertextHex := hex.EncodeToString(secureData.Ciphertext)
	hmacHex := hex.EncodeToString(secureData.HMAC)
	// Use separators that won't appear in hex-encoded data
	return fmt.Sprintf("%s|%s|%d", ciphertextHex, hmacHex, secureData.Version)
}

// DeserializeSecureDataWithVersion deserializes secure data with version
func DeserializeSecureDataWithVersion(serialized string) (*SecureDataWithVersion, error) {
	parts := strings.Split(serialized, "|")
	if len(parts) != 3 {
		return nil, fmt.Errorf("failed to parse serialized secure data: invalid format")
	}
	
	ciphertextHex := parts[0]
	hmacHex := parts[1]
	versionStr := parts[2]
	
	ciphertext, err := hex.DecodeString(ciphertextHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ciphertext: %w", err)
	}
	
	hmacValue, err := hex.DecodeString(hmacHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode HMAC: %w", err)
	}
	
	version := 0
	if _, err := fmt.Sscanf(versionStr, "%d", &version); err != nil {
		return nil, fmt.Errorf("failed to parse version: %w", err)
	}
	
	return &SecureDataWithVersion{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
		Version:    version,
	}, nil
} 