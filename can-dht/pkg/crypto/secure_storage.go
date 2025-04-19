package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// SecureFileOptions defines options for secure file operations
type SecureFileOptions struct {
	// FilePerms specifies the file permissions for created files
	FilePerms os.FileMode
	// DirPerms specifies the directory permissions for created directories
	DirPerms os.FileMode
	// TempDir specifies where temporary files should be created
	TempDir string
	// AtomicWrites determines if atomic write operations should be used
	AtomicWrites bool
}

// DefaultSecureFileOptions returns the recommended secure file options
func DefaultSecureFileOptions() *SecureFileOptions {
	return &SecureFileOptions{
		FilePerms:    0600, // rw for owner only
		DirPerms:     0700, // rwx for owner only
		TempDir:      "",   // use system default
		AtomicWrites: true,
	}
}

// SecureWriteFile writes data to a file with secure permissions
// If atomicWrite is true, it writes to a temp file and renames to ensure atomicity
func SecureWriteFile(filename string, data []byte, opts *SecureFileOptions) error {
	if opts == nil {
		opts = DefaultSecureFileOptions()
	}

	// Ensure parent directory exists with proper permissions
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, opts.DirPerms); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	if opts.AtomicWrites {
		return atomicWriteFile(filename, data, opts)
	}

	// Direct write
	if err := os.WriteFile(filename, data, opts.FilePerms); err != nil {
		return fmt.Errorf("failed to write file %s: %w", filename, err)
	}

	return nil
}

// atomicWriteFile writes data to a temporary file then renames it to the target filename
func atomicWriteFile(filename string, data []byte, opts *SecureFileOptions) error {
	// Create temp file in the same directory as the target file for atomic rename
	dir := filepath.Dir(filename)
	tempFile, err := os.CreateTemp(dir, filepath.Base(filename)+".tmp.*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()

	// Clean up the temp file if anything goes wrong
	success := false
	defer func() {
		if !success {
			os.Remove(tempPath)
		}
	}()

	// Set proper permissions before writing any data
	if err := tempFile.Chmod(opts.FilePerms); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to set temp file permissions: %w", err)
	}

	// Write data
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	// Sync to ensure data is written to disk
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, filename); err != nil {
		return fmt.Errorf("failed to rename temp file to %s: %w", filename, err)
	}

	success = true
	return nil
}

// EncryptFileContents encrypts the given data using the provided encryption key
func EncryptFileContents(data []byte, encryptionKey []byte) ([]byte, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create the GCM instance
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Create a random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt and seal the data
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// DecryptFileContents decrypts the given data using the provided encryption key
func DecryptFileContents(encryptedData []byte, encryptionKey []byte) ([]byte, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create the GCM instance
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Extract the nonce
	if len(encryptedData) < gcm.NonceSize() {
		return nil, errors.New("encrypted data too short")
	}
	nonce, ciphertext := encryptedData[:gcm.NonceSize()], encryptedData[gcm.NonceSize():]

	// Decrypt the data
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}

	return plaintext, nil
}

// CreateSecureBackup creates a backup of the specified file with a timestamp
func CreateSecureBackup(srcPath string, opts *SecureFileOptions) (string, error) {
	if opts == nil {
		opts = DefaultSecureFileOptions()
	}

	// Check if source file exists
	_, err := os.Stat(srcPath)
	if err != nil {
		return "", fmt.Errorf("source file does not exist: %w", err)
	}

	// Read the file contents
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to read source file: %w", err)
	}

	// Generate backup filename with timestamp
	timestamp := time.Now().UTC().Format("20060102-150405")
	backupPath := fmt.Sprintf("%s.backup.%s", srcPath, timestamp)

	// Write backup file
	if err := SecureWriteFile(backupPath, data, opts); err != nil {
		return "", fmt.Errorf("failed to write backup file: %w", err)
	}

	return backupPath, nil
} 