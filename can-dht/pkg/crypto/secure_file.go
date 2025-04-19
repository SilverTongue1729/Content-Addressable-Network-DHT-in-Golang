package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/pbkdf2"
)

// SecureFileOptions contains options for secure file operations
type SecureFileOptions struct {
	// FilePerms is the permission bits for files
	FilePerms os.FileMode

	// DirPerms is the permission bits for directories
	DirPerms os.FileMode

	// BackupDir is the directory where backups are stored
	BackupDir string

	// BackupCount is the maximum number of backups to keep
	BackupCount int

	// TempFileTemplate is the template for temporary files
	TempFileTemplate string
	
	// UseEnhancedSecurity enables the use of enhanced encryption features
	UseEnhancedSecurity bool
	
	// PreferredAlgorithm specifies which encryption algorithm to use
	PreferredAlgorithm string
	
	// EnableVersioning enables key versioning for encrypted files
	EnableVersioning bool
}

// DefaultSecureFileOptions returns default SecureFileOptions
func DefaultSecureFileOptions() *SecureFileOptions {
	return &SecureFileOptions{
		FilePerms:           0600,
		DirPerms:            0700,
		BackupDir:           "",
		BackupCount:         5,
		TempFileTemplate:    ".tmp",
		UseEnhancedSecurity: true,
		PreferredAlgorithm:  "AES-GCM",
		EnableVersioning:    true,
	}
}

// SecureWriteFile writes data to a file securely with atomic operations
func SecureWriteFile(path string, data []byte, opts *SecureFileOptions) error {
	if opts == nil {
		opts = DefaultSecureFileOptions()
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, opts.DirPerms); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// If enabled and the file exists, create a backup
	if _, err := os.Stat(path); err == nil {
		if err := CreateSecureBackup(path, opts); err != nil {
			return fmt.Errorf("failed to create backup: %w", err)
		}
	}

	// Create a temporary file
	tempFilePath := path + fmt.Sprintf(".%d%s", time.Now().UnixNano(), opts.TempFileTemplate)
	tempFile, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, opts.FilePerms)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tempFilePath) // Clean up in case of errors

	// Write data to temporary file
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to write to temporary file: %w", err)
	}

	// Sync and close temporary file
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Rename temporary file to the target file (atomic operation)
	if err := os.Rename(tempFilePath, path); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// CreateSecureBackup creates a secure backup of a file
func CreateSecureBackup(path string, opts *SecureFileOptions) (string, error) {
	if opts == nil {
		opts = DefaultSecureFileOptions()
	}

	// Get file info
	fileInfo, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("failed to get file info: %w", err)
	}

	// Determine backup directory
	backupDir := opts.BackupDir
	if backupDir == "" {
		backupDir = filepath.Join(filepath.Dir(path), ".backups")
	}

	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(backupDir, opts.DirPerms); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup filename
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Base(path)
	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.%s.bak", filename, timestamp))

	// Copy file to backup location
	err = func() error {
		src, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open source file: %w", err)
		}
		defer src.Close()

		dst, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileInfo.Mode())
		if err != nil {
			return fmt.Errorf("failed to create backup file: %w", err)
		}
		defer dst.Close()

		if _, err := io.Copy(dst, src); err != nil {
			return fmt.Errorf("failed to copy data to backup: %w", err)
		}

		return nil
	}()

	if err != nil {
		return "", err
	}

	// Prune old backups if needed
	if opts.BackupCount > 0 {
		if err := pruneOldBackups(backupDir, filename, opts.BackupCount); err != nil {
			// Log the error but continue with the backup
			fmt.Printf("Failed to prune old backups: %v\n", err)
		}
	}

	return backupPath, nil
}

// pruneOldBackups removes old backups, keeping only the most recent ones
func pruneOldBackups(backupDir, filename string, keepCount int) error {
	// Find all backups for this file
	pattern := fmt.Sprintf("%s.*.bak", filename)
	matches, err := filepath.Glob(filepath.Join(backupDir, pattern))
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	// If we don't have too many backups, do nothing
	if len(matches) <= keepCount {
		return nil
	}

	// Get file information for sorting by modification time
	type backupFile struct {
		path    string
		modTime time.Time
	}

	backups := make([]backupFile, 0, len(matches))
	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			// Skip files we can't stat
			continue
		}
		backups = append(backups, backupFile{path: path, modTime: info.ModTime()})
	}

	// Sort by modification time (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].modTime.After(backups[j].modTime)
	})

	// Remove older backups
	for i := keepCount; i < len(backups); i++ {
		if err := os.Remove(backups[i].path); err != nil {
			// Log the error but continue with the pruning
			fmt.Printf("Failed to remove old backup %s: %v\n", backups[i].path, err)
		}
	}

	return nil
}

// EncryptFileContents encrypts data using AES-GCM and a password-derived key
func EncryptFileContents(data []byte, password []byte) ([]byte, error) {
	// Use the enhanced encryption if available
	if DefaultSecureFileOptions().UseEnhancedSecurity {
		// Create ephemeral key manager with key derived from password
		salt := make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			return nil, fmt.Errorf("failed to generate salt: %w", err)
		}
		
		// Create options with the salt
		opts := DefaultEnhancedKeyManagerOptions()
		opts.KeyDerivationSalt = salt
		
		// Create the enhanced key manager
		ekm, err := NewEnhancedKeyManager(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create enhanced key manager: %w", err)
		}
		
		// Derive encryption key and HMAC key from password
		derivedEncKey := ekm.DeriveKey(password, 32)
		derivedHmacKey := ekm.DeriveKey(append(password, []byte("hmac")...), 32)
		
		// Create key manager with derived keys
		km, err := NewKeyManagerFromKeys(derivedEncKey, derivedHmacKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create key manager with derived keys: %w", err)
		}
		
		// Create enhanced key manager with derived keys
		opts.BaseKeyManager = km
		ekm, err = NewEnhancedKeyManager(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create enhanced key manager: %w", err)
		}
		
		// Encrypt with enhanced security
		secureData, err := ekm.EncryptAndAuthenticateEnhanced(data, DefaultSecureFileOptions().PreferredAlgorithm)
		if err != nil {
			return nil, fmt.Errorf("enhanced encryption failed: %w", err)
		}
		
		// Serialize the secure data
		serialized, err := SerializeEnhancedSecureData(secureData)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize secure data: %w", err)
		}
		
		// Create wrapper with salt and format information
		wrapper := struct {
			Salt      string `json:"salt"`
			Data      string `json:"data"`
			Enhanced  bool   `json:"enhanced"`
			Version   int    `json:"version"`
			Algorithm string `json:"algorithm"`
		}{
			Salt:      fmt.Sprintf("%x", salt),
			Data:      serialized,
			Enhanced:  true,
			Version:   secureData.Version,
			Algorithm: secureData.Algorithm,
		}
		
		// Serialize the wrapper
		return json.Marshal(wrapper)
	}
	
	// Legacy encryption method
	// Derive key from password
	key := deriveKey(password, nil, 32)

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

	// Generate a random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt and authenticate the data
	ciphertext := gcm.Seal(nonce, nonce, data, nil)

	return ciphertext, nil
}

// DecryptFileContents decrypts data that was encrypted with a password
func DecryptFileContents(encryptedData []byte, password []byte) ([]byte, error) {
	// Check if this is enhanced encrypted data
	var wrapper struct {
		Salt      string `json:"salt"`
		Data      string `json:"data"`
		Enhanced  bool   `json:"enhanced"`
		Version   int    `json:"version"`
		Algorithm string `json:"algorithm"`
	}
	
	// Try to unmarshal as enhanced data
	isEnhanced := json.Unmarshal(encryptedData, &wrapper) == nil && wrapper.Enhanced
	
	if isEnhanced {
		// This is enhanced encrypted data
		// Decode the salt
		salt, err := hex.DecodeString(wrapper.Salt)
		if err != nil {
			return nil, fmt.Errorf("failed to decode salt: %w", err)
		}
		
		// Create options with the salt
		opts := DefaultEnhancedKeyManagerOptions()
		opts.KeyDerivationSalt = salt
		
		// Create the enhanced key manager
		ekm, err := NewEnhancedKeyManager(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create enhanced key manager: %w", err)
		}
		
		// Derive encryption key and HMAC key from password
		derivedEncKey := ekm.DeriveKey(password, 32)
		derivedHmacKey := ekm.DeriveKey(append(password, []byte("hmac")...), 32)
		
		// Create key manager with derived keys
		km, err := NewKeyManagerFromKeys(derivedEncKey, derivedHmacKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create key manager with derived keys: %w", err)
		}
		
		// Create enhanced key manager with derived keys
		opts.BaseKeyManager = km
		ekm, err = NewEnhancedKeyManager(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create enhanced key manager: %w", err)
		}
		
		// Deserialize the secure data
		secureData, err := DeserializeEnhancedSecureData(wrapper.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize secure data: %w", err)
		}
		
		// Decrypt with enhanced security
		return ekm.DecryptAndVerifyEnhanced(secureData)
	}
	
	// Legacy decryption method
	// Derive key from password
	key := deriveKey(password, nil, 32)

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
	if len(encryptedData) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// Extract the nonce from the ciphertext
	nonce, ciphertext := encryptedData[:gcm.NonceSize()], encryptedData[gcm.NonceSize():]

	// Decrypt and verify the ciphertext
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// Simple key derivation function (not using Argon2 to avoid dependencies)
func deriveKey(password, salt []byte, keyLen int) []byte {
	if salt == nil {
		// Use a fixed salt if none is provided
		salt = []byte("CAN-DHT-Static-Salt")
	}
	
	// This is a simple PBKDF implementation
	// In production, you'd use a proper KDF like Argon2
	key := make([]byte, keyLen)
	
	// Stretch the password
	stretched := append(password, salt...)
	for i := 0; i < 10000; i++ {
		h := sha256.New()
		h.Write(stretched)
		stretched = h.Sum(nil)
	}
	
	// Copy the bytes needed
	copy(key, stretched[:keyLen])
	
	return key
} 