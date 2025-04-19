package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

// EnhancedKeyManager extends the standard KeyManager with additional encryption algorithms
// and enhanced security features
type EnhancedKeyManager struct {
	*KeyManager
	
	// ChaCha20Poly1305 key for alternative encryption
	ChaChaKey []byte
	
	// Argon2 key derivation parameters
	keyDerivationSalt   []byte
	keyDerivationMemory uint32
	keyDerivationTime   uint32
	keyDerivationThreads uint8
	
	// Data keys for metadata encryption
	metadataKey []byte
	
	// Mu for thread safety
	mu sync.RWMutex
}

// EnhancedKeyManagerOptions contains options for creating an EnhancedKeyManager
type EnhancedKeyManagerOptions struct {
	BaseKeyManager    *KeyManager
	KeyDerivationSalt []byte
	ChaChaKey         []byte
	MetadataKey       []byte
}

// DefaultEnhancedKeyManagerOptions returns default options
func DefaultEnhancedKeyManagerOptions() *EnhancedKeyManagerOptions {
	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		panic(fmt.Sprintf("failed to generate salt: %v", err))
	}
	
	return &EnhancedKeyManagerOptions{
		KeyDerivationSalt: salt,
	}
}

// NewEnhancedKeyManager creates a new enhanced key manager
func NewEnhancedKeyManager(opts *EnhancedKeyManagerOptions) (*EnhancedKeyManager, error) {
	if opts == nil {
		opts = DefaultEnhancedKeyManagerOptions()
	}
	
	var baseKM *KeyManager
	var err error
	
	if opts.BaseKeyManager == nil {
		// Create a new base key manager
		baseKM, err = NewKeyManager()
		if err != nil {
			return nil, fmt.Errorf("failed to create base key manager: %w", err)
		}
	} else {
		baseKM = opts.BaseKeyManager
	}
	
	// Generate ChaCha20Poly1305 key if not provided
	chachaKey := opts.ChaChaKey
	if chachaKey == nil {
		chachaKey = make([]byte, chacha20poly1305.KeySize)
		if _, err := io.ReadFull(rand.Reader, chachaKey); err != nil {
			return nil, fmt.Errorf("failed to generate ChaCha20Poly1305 key: %w", err)
		}
	}
	
	// Generate metadata key if not provided
	metadataKey := opts.MetadataKey
	if metadataKey == nil {
		metadataKey = make([]byte, 32)
		if _, err := io.ReadFull(rand.Reader, metadataKey); err != nil {
			return nil, fmt.Errorf("failed to generate metadata key: %w", err)
		}
	}
	
	return &EnhancedKeyManager{
		KeyManager:           baseKM,
		ChaChaKey:            chachaKey,
		keyDerivationSalt:    opts.KeyDerivationSalt,
		keyDerivationMemory:  64 * 1024, // 64MB
		keyDerivationTime:    3,         // 3 iterations
		keyDerivationThreads: 4,         // 4 threads
		metadataKey:          metadataKey,
	}, nil
}

// DeriveKey derives a key from a password using Argon2id
func (ekm *EnhancedKeyManager) DeriveKey(password []byte, keyLen uint32) []byte {
	return argon2.IDKey(
		password,
		ekm.keyDerivationSalt,
		ekm.keyDerivationTime,
		ekm.keyDerivationMemory,
		ekm.keyDerivationThreads,
		keyLen,
	)
}

// EncryptWithChaCha20Poly1305 encrypts data using ChaCha20-Poly1305
func (ekm *EnhancedKeyManager) EncryptWithChaCha20Poly1305(plaintext []byte) ([]byte, error) {
	ekm.mu.RLock()
	defer ekm.mu.RUnlock()
	
	// Create a new ChaCha20-Poly1305 cipher
	aead, err := chacha20poly1305.New(ekm.ChaChaKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create ChaCha20-Poly1305 cipher: %w", err)
	}
	
	// Generate a random nonce
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	
	// Encrypt and authenticate the plaintext
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	
	return ciphertext, nil
}

// DecryptWithChaCha20Poly1305 decrypts data encrypted with ChaCha20-Poly1305
func (ekm *EnhancedKeyManager) DecryptWithChaCha20Poly1305(ciphertext []byte) ([]byte, error) {
	ekm.mu.RLock()
	defer ekm.mu.RUnlock()
	
	// Create a new ChaCha20-Poly1305 cipher
	aead, err := chacha20poly1305.New(ekm.ChaChaKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create ChaCha20-Poly1305 cipher: %w", err)
	}
	
	// Ensure the ciphertext is large enough to contain the nonce
	if len(ciphertext) < aead.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}
	
	// Extract the nonce from the ciphertext
	nonce, ciphertextWithoutNonce := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]
	
	// Decrypt and verify the ciphertext
	plaintext, err := aead.Open(nil, nonce, ciphertextWithoutNonce, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}
	
	return plaintext, nil
}

// EnhancedSecureData represents encrypted data with additional security features
type EnhancedSecureData struct {
	// Ciphertext is the encrypted data
	Ciphertext []byte
	
	// HMAC is the integrity hash
	HMAC []byte
	
	// Version is the format version
	Version int
	
	// Algorithm is the encryption algorithm used
	Algorithm string
	
	// Metadata is additional encrypted metadata
	Metadata []byte
	
	// Timestamp is when the data was encrypted
	Timestamp int64
	
	// Hash is a SHA-512 hash of the original data
	Hash []byte
}

// EncryptAndAuthenticateEnhanced encrypts and authenticates data with enhanced security
func (ekm *EnhancedKeyManager) EncryptAndAuthenticateEnhanced(plaintext []byte, algorithm string) (*EnhancedSecureData, error) {
	var ciphertext []byte
	var err error
	
	// Hash the original data for integrity verification
	hash := sha512.Sum512(plaintext)
	
	// Encrypt using the specified algorithm
	switch algorithm {
	case "AES-GCM":
		ciphertext, err = ekm.EncryptWithAESGCM(plaintext)
	case "ChaCha20-Poly1305":
		ciphertext, err = ekm.EncryptWithChaCha20Poly1305(plaintext)
	default:
		return nil, fmt.Errorf("unsupported encryption algorithm: %s", algorithm)
	}
	
	if err != nil {
		return nil, fmt.Errorf("encryption failed: %w", err)
	}
	
	// Generate HMAC for the ciphertext
	hmacValue := ekm.GenerateHMAC(ciphertext)
	
	// Create encrypted metadata
	metadata, err := ekm.encryptMetadata(map[string]interface{}{
		"size":      len(plaintext),
		"original_hash": base64.StdEncoding.EncodeToString(hash[:]),
		"timestamp": time.Now().UnixNano(),
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt metadata: %w", err)
	}
	
	return &EnhancedSecureData{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
		Version:    1,
		Algorithm:  algorithm,
		Metadata:   metadata,
		Timestamp:  time.Now().Unix(),
		Hash:       hash[:],
	}, nil
}

// DecryptAndVerifyEnhanced decrypts and verifies data with enhanced security
func (ekm *EnhancedKeyManager) DecryptAndVerifyEnhanced(secureData *EnhancedSecureData) ([]byte, error) {
	// Verify the HMAC
	if !ekm.VerifyHMAC(secureData.Ciphertext, secureData.HMAC) {
		return nil, fmt.Errorf("HMAC verification failed")
	}
	
	var plaintext []byte
	var err error
	
	// Decrypt using the specified algorithm
	switch secureData.Algorithm {
	case "AES-GCM":
		plaintext, err = ekm.DecryptWithAESGCM(secureData.Ciphertext)
	case "ChaCha20-Poly1305":
		plaintext, err = ekm.DecryptWithChaCha20Poly1305(secureData.Ciphertext)
	default:
		return nil, fmt.Errorf("unsupported encryption algorithm: %s", secureData.Algorithm)
	}
	
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}
	
	// Verify the hash
	hash := sha512.Sum512(plaintext)
	if !hmac.Equal(hash[:], secureData.Hash) {
		return nil, fmt.Errorf("data integrity check failed: hash mismatch")
	}
	
	return plaintext, nil
}

// encryptMetadata encrypts metadata using the metadata key
func (ekm *EnhancedKeyManager) encryptMetadata(metadata map[string]interface{}) ([]byte, error) {
	// Convert metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	
	// Create a new AES cipher block
	block, err := aes.NewCipher(ekm.metadataKey)
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
	
	// Encrypt and authenticate the metadata
	encryptedMetadata := gcm.Seal(nonce, nonce, metadataJSON, nil)
	
	return encryptedMetadata, nil
}

// decryptMetadata decrypts metadata
func (ekm *EnhancedKeyManager) decryptMetadata(encryptedMetadata []byte) (map[string]interface{}, error) {
	// Create a new AES cipher block
	block, err := aes.NewCipher(ekm.metadataKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	
	// Create a new GCM cipher
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	
	// Ensure the ciphertext is large enough to contain the nonce
	if len(encryptedMetadata) < gcm.NonceSize() {
		return nil, fmt.Errorf("encrypted metadata too short")
	}
	
	// Extract the nonce from the ciphertext
	nonce, ciphertextWithoutNonce := encryptedMetadata[:gcm.NonceSize()], encryptedMetadata[gcm.NonceSize():]
	
	// Decrypt and verify the ciphertext
	metadataJSON, err := gcm.Open(nil, nonce, ciphertextWithoutNonce, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt metadata: %w", err)
	}
	
	// Parse the metadata JSON
	var metadata map[string]interface{}
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	
	return metadata, nil
}

// SerializeEnhancedSecureData serializes enhanced secure data
func SerializeEnhancedSecureData(secureData *EnhancedSecureData) (string, error) {
	data, err := json.Marshal(struct {
		Ciphertext string `json:"c"`
		HMAC       string `json:"h"`
		Version    int    `json:"v"`
		Algorithm  string `json:"a"`
		Metadata   string `json:"m"`
		Timestamp  int64  `json:"t"`
		Hash       string `json:"s"`
	}{
		Ciphertext: base64.StdEncoding.EncodeToString(secureData.Ciphertext),
		HMAC:       base64.StdEncoding.EncodeToString(secureData.HMAC),
		Version:    secureData.Version,
		Algorithm:  secureData.Algorithm,
		Metadata:   base64.StdEncoding.EncodeToString(secureData.Metadata),
		Timestamp:  secureData.Timestamp,
		Hash:       base64.StdEncoding.EncodeToString(secureData.Hash),
	})
	
	if err != nil {
		return "", fmt.Errorf("failed to serialize secure data: %w", err)
	}
	
	return base64.StdEncoding.EncodeToString(data), nil
}

// DeserializeEnhancedSecureData deserializes enhanced secure data
func DeserializeEnhancedSecureData(serialized string) (*EnhancedSecureData, error) {
	data, err := base64.StdEncoding.DecodeString(serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to decode serialized data: %w", err)
	}
	
	var sd struct {
		Ciphertext string `json:"c"`
		HMAC       string `json:"h"`
		Version    int    `json:"v"`
		Algorithm  string `json:"a"`
		Metadata   string `json:"m"`
		Timestamp  int64  `json:"t"`
		Hash       string `json:"s"`
	}
	
	if err := json.Unmarshal(data, &sd); err != nil {
		return nil, fmt.Errorf("failed to unmarshal secure data: %w", err)
	}
	
	ciphertext, err := base64.StdEncoding.DecodeString(sd.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ciphertext: %w", err)
	}
	
	hmacValue, err := base64.StdEncoding.DecodeString(sd.HMAC)
	if err != nil {
		return nil, fmt.Errorf("failed to decode HMAC: %w", err)
	}
	
	metadata, err := base64.StdEncoding.DecodeString(sd.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	
	hash, err := base64.StdEncoding.DecodeString(sd.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hash: %w", err)
	}
	
	return &EnhancedSecureData{
		Ciphertext: ciphertext,
		HMAC:       hmacValue,
		Version:    sd.Version,
		Algorithm:  sd.Algorithm,
		Metadata:   metadata,
		Timestamp:  sd.Timestamp,
		Hash:       hash,
	}, nil
}

// SaveEnhancedSecureFile writes enhanced secure data to a file with robust error handling
func SaveEnhancedSecureFile(path string, secureData *EnhancedSecureData, options *SecureFileOptions) error {
	if options == nil {
		options = DefaultSecureFileOptions()
	}
	
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, options.DirPerms); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Serialize the secure data
	serializedData, err := SerializeEnhancedSecureData(secureData)
	if err != nil {
		return fmt.Errorf("failed to serialize secure data: %w", err)
	}
	
	// Create a temporary file in the same directory
	tempFile := fmt.Sprintf("%s.%d.tmp", path, time.Now().UnixNano())
	if err := os.WriteFile(tempFile, []byte(serializedData), options.FilePerms); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}
	
	// Create a backup if the original file exists
	if _, err := os.Stat(path); err == nil {
		backupPath, err := CreateSecureBackup(path, options)
		if err != nil {
			// Try to remove the temporary file
			os.Remove(tempFile)
			return fmt.Errorf("failed to create backup: %w", err)
		}
		
		// Log backup creation
		fmt.Printf("Created backup at %s\n", backupPath)
	}
	
	// Rename temporary file to the target file (atomic operation)
	if err := os.Rename(tempFile, path); err != nil {
		// Try to remove the temporary file
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}
	
	return nil
}

// LoadEnhancedSecureFile reads enhanced secure data from a file
func LoadEnhancedSecureFile(path string) (*EnhancedSecureData, error) {
	// Read the file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	
	// Deserialize the secure data
	secureData, err := DeserializeEnhancedSecureData(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize secure data: %w", err)
	}
	
	return secureData, nil
}

// EncryptAndSaveFile encrypts a file and saves it with enhanced security
func (ekm *EnhancedKeyManager) EncryptAndSaveFile(sourcePath, destPath string, algorithm string, options *SecureFileOptions) error {
	// Read the source file
	plaintext, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}
	
	// Encrypt the data
	secureData, err := ekm.EncryptAndAuthenticateEnhanced(plaintext, algorithm)
	if err != nil {
		return fmt.Errorf("failed to encrypt data: %w", err)
	}
	
	// Save the encrypted data
	if err := SaveEnhancedSecureFile(destPath, secureData, options); err != nil {
		return fmt.Errorf("failed to save encrypted file: %w", err)
	}
	
	return nil
}

// DecryptAndLoadFile decrypts a file
func (ekm *EnhancedKeyManager) DecryptAndLoadFile(path string) ([]byte, error) {
	// Load the encrypted data
	secureData, err := LoadEnhancedSecureFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load encrypted file: %w", err)
	}
	
	// Decrypt the data
	plaintext, err := ekm.DecryptAndVerifyEnhanced(secureData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}
	
	return plaintext, nil
}

// VerifyFileIntegrity checks if a file's integrity is intact
func (ekm *EnhancedKeyManager) VerifyFileIntegrity(path string) (bool, error) {
	// Load the encrypted data
	secureData, err := LoadEnhancedSecureFile(path)
	if err != nil {
		return false, fmt.Errorf("failed to load encrypted file: %w", err)
	}
	
	// Verify the HMAC
	if !ekm.VerifyHMAC(secureData.Ciphertext, secureData.HMAC) {
		return false, nil
	}
	
	return true, nil
}

// GetFileMetadata retrieves metadata from an encrypted file
func (ekm *EnhancedKeyManager) GetFileMetadata(path string) (map[string]interface{}, error) {
	// Load the encrypted data
	secureData, err := LoadEnhancedSecureFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load encrypted file: %w", err)
	}
	
	// Decrypt the metadata
	metadata, err := ekm.decryptMetadata(secureData.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt metadata: %w", err)
	}
	
	// Add file-specific information
	metadata["algorithm"] = secureData.Algorithm
	metadata["version"] = secureData.Version
	metadata["encrypted_at"] = time.Unix(secureData.Timestamp, 0).Format(time.RFC3339)
	
	return metadata, nil
} 