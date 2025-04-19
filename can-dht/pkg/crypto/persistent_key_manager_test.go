package crypto

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistentKeyManager(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "pkm-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	keyFilePath := filepath.Join(tempDir, "keys.json")
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Test 1: Create a new PersistentKeyManager
	t.Run("NewPersistentKeyManager", func(t *testing.T) {
		pkm, err := NewPersistentKeyManager(keyFilePath, 24*time.Hour, logger)
		require.NoError(t, err)
		assert.NotNil(t, pkm)
		assert.NotNil(t, pkm.KeyManager)
		assert.NotNil(t, pkm.encryptionKey)
		assert.NotNil(t, pkm.hmacKey)

		// Verify that the key file was created
		_, err = os.Stat(keyFilePath)
		assert.NoError(t, err)
	})

	// Test 2: Load existing keys
	t.Run("LoadExistingKeys", func(t *testing.T) {
		// Create the first manager
		pkm1, err := NewPersistentKeyManager(keyFilePath, 24*time.Hour, logger)
		require.NoError(t, err)

		// Save the original keys
		origEncKey := make([]byte, len(pkm1.encryptionKey))
		origHMACKey := make([]byte, len(pkm1.hmacKey))
		copy(origEncKey, pkm1.encryptionKey)
		copy(origHMACKey, pkm1.hmacKey)

		// Create a second manager that should load the same keys
		pkm2, err := NewPersistentKeyManager(keyFilePath, 24*time.Hour, logger)
		require.NoError(t, err)

		// Verify that the keys match
		assert.Equal(t, origEncKey, pkm2.encryptionKey)
		assert.Equal(t, origHMACKey, pkm2.hmacKey)
	})

	// Test 3: Backup and restore
	t.Run("BackupAndRestore", func(t *testing.T) {
		// Create a manager
		pkm, err := NewPersistentKeyManager(keyFilePath, 24*time.Hour, logger)
		require.NoError(t, err)

		// Create a backup
		err = pkm.BackupKeys()
		require.NoError(t, err)

		// Find the backup file
		backupPattern := keyFilePath + ".backup.*"
		matches, err := filepath.Glob(backupPattern)
		require.NoError(t, err)
		require.Len(t, matches, 1)
		backupPath := matches[0]

		// Save original keys
		origEncKey := make([]byte, len(pkm.encryptionKey))
		origHMACKey := make([]byte, len(pkm.hmacKey))
		copy(origEncKey, pkm.encryptionKey)
		copy(origHMACKey, pkm.hmacKey)

		// Create new keys to simulate rotation
		newKeyManager, err := NewKeyManager()
		require.NoError(t, err)
		
		// Replace the keys
		pkm.encryptionKey = newKeyManager.encryptionKey
		pkm.hmacKey = newKeyManager.hmacKey
		
		// Verify keys are different now
		assert.NotEqual(t, origEncKey, pkm.encryptionKey)
		assert.NotEqual(t, origHMACKey, pkm.hmacKey)

		// Restore from backup
		err = pkm.RestoreFromBackup(backupPath)
		require.NoError(t, err)

		// Verify keys are restored
		assert.Equal(t, origEncKey, pkm.encryptionKey)
		assert.Equal(t, origHMACKey, pkm.hmacKey)
	})

	// Test 4: Basic crypto operations
	t.Run("CryptoOperations", func(t *testing.T) {
		pkm, err := NewPersistentKeyManager(keyFilePath, 24*time.Hour, logger)
		require.NoError(t, err)

		// Test encryption and decryption
		plaintext := []byte("this is a test message")
		ciphertext, err := pkm.EncryptWithAESGCM(plaintext)
		require.NoError(t, err)
		assert.NotEqual(t, plaintext, ciphertext)

		decrypted, err := pkm.DecryptWithAESGCM(ciphertext)
		require.NoError(t, err)
		assert.Equal(t, plaintext, decrypted)

		// Test HMAC operations
		data := []byte("data to authenticate")
		hmac, err := pkm.GenerateHMAC(data)
		require.NoError(t, err)

		valid, err := pkm.VerifyHMAC(data, hmac)
		require.NoError(t, err)
		assert.True(t, valid)

		// Test combined operations
		encAndAuth, err := pkm.EncryptAndAuthenticate(plaintext)
		require.NoError(t, err)

		decAndVerified, err := pkm.DecryptAndVerify(encAndAuth)
		require.NoError(t, err)
		assert.Equal(t, plaintext, decAndVerified)
	})
} 