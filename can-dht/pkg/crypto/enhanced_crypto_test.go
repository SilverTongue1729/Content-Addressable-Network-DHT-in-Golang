package crypto

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnhancedKeyManager(t *testing.T) {
	// Create a new enhanced key manager
	ekm, err := NewEnhancedKeyManager(nil)
	if err != nil {
		t.Fatalf("Failed to create enhanced key manager: %v", err)
	}

	// Test ChaCha20Poly1305 encryption
	plaintext := []byte("This is a test message for ChaCha20Poly1305")
	ciphertext, err := ekm.EncryptWithChaCha20Poly1305(plaintext)
	if err != nil {
		t.Fatalf("Failed to encrypt with ChaCha20Poly1305: %v", err)
	}

	// Test ChaCha20Poly1305 decryption
	decrypted, err := ekm.DecryptWithChaCha20Poly1305(ciphertext)
	if err != nil {
		t.Fatalf("Failed to decrypt with ChaCha20Poly1305: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("Decrypted data doesn't match original: got %s, want %s", decrypted, plaintext)
	}

	// Test key derivation
	password := []byte("test-password")
	key1 := ekm.DeriveKey(password, 32)
	key2 := ekm.DeriveKey(password, 32)

	// Same password with same salt should produce same key
	if !bytes.Equal(key1, key2) {
		t.Errorf("Key derivation is not deterministic")
	}

	// Different passwords should produce different keys
	key3 := ekm.DeriveKey([]byte("different-password"), 32)
	if bytes.Equal(key1, key3) {
		t.Errorf("Key derivation produced same key for different passwords")
	}
}

func TestEnhancedSecureData(t *testing.T) {
	// Create a new enhanced key manager
	ekm, err := NewEnhancedKeyManager(nil)
	if err != nil {
		t.Fatalf("Failed to create enhanced key manager: %v", err)
	}

	// Test AES-GCM encryption
	plaintext := []byte("This is a test message for AES-GCM")
	secureData, err := ekm.EncryptAndAuthenticateEnhanced(plaintext, "AES-GCM")
	if err != nil {
		t.Fatalf("Failed to encrypt with AES-GCM: %v", err)
	}

	// Verify the secure data
	if secureData.Algorithm != "AES-GCM" {
		t.Errorf("Algorithm mismatch: got %s, want %s", secureData.Algorithm, "AES-GCM")
	}

	if len(secureData.HMAC) == 0 {
		t.Errorf("HMAC is empty")
	}

	if len(secureData.Hash) == 0 {
		t.Errorf("Hash is empty")
	}

	// Test decryption
	decrypted, err := ekm.DecryptAndVerifyEnhanced(secureData)
	if err != nil {
		t.Fatalf("Failed to decrypt: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("Decrypted data doesn't match original: got %s, want %s", decrypted, plaintext)
	}

	// Test ChaCha20Poly1305 encryption
	secureData2, err := ekm.EncryptAndAuthenticateEnhanced(plaintext, "ChaCha20-Poly1305")
	if err != nil {
		t.Fatalf("Failed to encrypt with ChaCha20Poly1305: %v", err)
	}

	// Test decryption
	decrypted2, err := ekm.DecryptAndVerifyEnhanced(secureData2)
	if err != nil {
		t.Fatalf("Failed to decrypt: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted2) {
		t.Errorf("Decrypted data doesn't match original: got %s, want %s", decrypted2, plaintext)
	}

	// Test serialization
	serialized, err := SerializeEnhancedSecureData(secureData)
	if err != nil {
		t.Fatalf("Failed to serialize secure data: %v", err)
	}

	deserialized, err := DeserializeEnhancedSecureData(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize secure data: %v", err)
	}

	if !bytes.Equal(secureData.Ciphertext, deserialized.Ciphertext) {
		t.Errorf("Deserialized ciphertext doesn't match original")
	}

	if !bytes.Equal(secureData.HMAC, deserialized.HMAC) {
		t.Errorf("Deserialized HMAC doesn't match original")
	}

	if secureData.Algorithm != deserialized.Algorithm {
		t.Errorf("Deserialized algorithm doesn't match original")
	}
}

func TestFileOperations(t *testing.T) {
	// Create a temp directory for file tests
	tempDir, err := os.MkdirTemp("", "can-dht-crypto-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new enhanced key manager
	ekm, err := NewEnhancedKeyManager(nil)
	if err != nil {
		t.Fatalf("Failed to create enhanced key manager: %v", err)
	}

	// Test file encryption and decryption
	testFilePath := filepath.Join(tempDir, "test.txt")
	encryptedFilePath := filepath.Join(tempDir, "encrypted.dat")
	testData := []byte("This is test data for file encryption and decryption")

	// Write test data to file
	if err := os.WriteFile(testFilePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Encrypt file
	err = ekm.EncryptAndSaveFile(testFilePath, encryptedFilePath, "AES-GCM", nil)
	if err != nil {
		t.Fatalf("Failed to encrypt file: %v", err)
	}

	// Verify the encrypted file exists
	if _, err := os.Stat(encryptedFilePath); os.IsNotExist(err) {
		t.Errorf("Encrypted file wasn't created")
	}

	// Decrypt file
	decrypted, err := ekm.DecryptAndLoadFile(encryptedFilePath)
	if err != nil {
		t.Fatalf("Failed to decrypt file: %v", err)
	}

	if !bytes.Equal(testData, decrypted) {
		t.Errorf("Decrypted file doesn't match original data")
	}

	// Test file integrity verification
	isValid, err := ekm.VerifyFileIntegrity(encryptedFilePath)
	if err != nil {
		t.Fatalf("Failed to verify file integrity: %v", err)
	}

	if !isValid {
		t.Errorf("File integrity verification failed for valid file")
	}

	// Test metadata retrieval
	metadata, err := ekm.GetFileMetadata(encryptedFilePath)
	if err != nil {
		t.Fatalf("Failed to get file metadata: %v", err)
	}

	if metadata["size"] != len(testData) {
		t.Errorf("Metadata size doesn't match: got %v, want %d", metadata["size"], len(testData))
	}

	if metadata["algorithm"] != "AES-GCM" {
		t.Errorf("Metadata algorithm doesn't match: got %v, want AES-GCM", metadata["algorithm"])
	}
}

func TestEnhancedSecureFileOperations(t *testing.T) {
	// Create a temp directory for file tests
	tempDir, err := os.MkdirTemp("", "can-dht-secure-file-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create file paths
	testFilePath := filepath.Join(tempDir, "secure_data.txt")
	backupDir := filepath.Join(tempDir, "backups")

	// Create options
	opts := DefaultSecureFileOptions()
	opts.BackupDir = backupDir
	opts.BackupCount = 3

	// Test data
	testData := []byte("This is test data for secure file operations")

	// Write secure file
	err = SecureWriteFile(testFilePath, testData, opts)
	if err != nil {
		t.Fatalf("Failed to write secure file: %v", err)
	}

	// Verify the file was created
	if _, err := os.Stat(testFilePath); os.IsNotExist(err) {
		t.Errorf("Secure file wasn't created")
	}

	// Create multiple backups
	for i := 0; i < 5; i++ {
		// Modify data slightly each time
		modifiedData := append([]byte(nil), testData...)
		modifiedData = append(modifiedData, byte(i+1))

		// Create backup of the previous version
		backupPath, err := CreateSecureBackup(testFilePath, opts)
		if err != nil {
			t.Fatalf("Failed to create backup: %v", err)
		}

		// Verify backup file exists
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			t.Errorf("Backup file %s wasn't created", backupPath)
		}

		// Write modified data
		err = SecureWriteFile(testFilePath, modifiedData, opts)
		if err != nil {
			t.Fatalf("Failed to write modified data: %v", err)
		}

		// Wait a bit to ensure different timestamps
		time.Sleep(100 * time.Millisecond)
	}

	// Check backups were pruned
	backupFiles, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatalf("Failed to read backup directory: %v", err)
	}

	// Should have at most BackupCount backups
	if len(backupFiles) > opts.BackupCount {
		t.Errorf("Too many backups: got %d, want <= %d", len(backupFiles), opts.BackupCount)
	}

	// Test encryption with password
	password := []byte("test-password")
	encryptedData, err := EncryptFileContents(testData, password)
	if err != nil {
		t.Fatalf("Failed to encrypt file contents: %v", err)
	}

	// Test decryption with password
	decryptedData, err := DecryptFileContents(encryptedData, password)
	if err != nil {
		t.Fatalf("Failed to decrypt file contents: %v", err)
	}

	if !bytes.Equal(testData, decryptedData) {
		t.Errorf("Decrypted data doesn't match original")
	}

	// Test decryption with wrong password
	_, err = DecryptFileContents(encryptedData, []byte("wrong-password"))
	if err == nil {
		t.Errorf("Decryption with wrong password should fail")
	}
} 