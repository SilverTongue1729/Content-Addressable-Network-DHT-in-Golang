package crypto

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// KeyFile represents the structure of the key file
type KeyFile struct {
	EncryptionKey []byte `json:"encryption_key"`
	HMACKey       []byte `json:"hmac_key"`
	CreatedAt     int64  `json:"created_at"`
	Version       int    `json:"version"` // Added version field for schema management
}

// PersistentKeyManager extends KeyManager with persistence capabilities
type PersistentKeyManager struct {
	*KeyManager
	keyFilePath        string
	rotationInterval   time.Duration
	lastRotation       time.Time
	rotationInProgress bool
	masterPassword     []byte              // Added for file encryption
	secureOptions      *SecureFileOptions  // Added secure file options
	mu                 sync.RWMutex
	logger             *logrus.Logger
}

// PersistentKeyManagerOptions contains options for creating a PersistentKeyManager
type PersistentKeyManagerOptions struct {
	KeyFilePath      string
	RotationInterval time.Duration
	MasterPassword   []byte           // Optional master password for file encryption
	SecureOptions    *SecureFileOptions  // Optional secure file options
	Logger           *logrus.Logger
}

// NewPersistentKeyManager creates a new persistent key manager
func NewPersistentKeyManager(opts PersistentKeyManagerOptions) (*PersistentKeyManager, error) {
	if opts.Logger == nil {
		opts.Logger = logrus.New()
		opts.Logger.SetLevel(logrus.InfoLevel)
	}

	if opts.SecureOptions == nil {
		opts.SecureOptions = DefaultSecureFileOptions()
	}

	// Ensure the directory exists with secure permissions
	dir := filepath.Dir(opts.KeyFilePath)
	if err := os.MkdirAll(dir, opts.SecureOptions.DirPerms); err != nil {
		return nil, fmt.Errorf("failed to create directory for key file: %w", err)
	}

	var km *KeyManager
	var lastRotation time.Time

	// Check if key file exists
	if _, err := os.Stat(opts.KeyFilePath); err == nil {
		// Load existing keys
		keyData, err := loadKeyFile(opts.KeyFilePath, opts.MasterPassword)
		if err != nil {
			return nil, fmt.Errorf("failed to load existing key file: %w", err)
		}

		// Create KeyManager with loaded keys
		km = &KeyManager{
			encryptionKey: keyData.EncryptionKey,
			hmacKey:       keyData.HMACKey,
		}
		lastRotation = time.Unix(keyData.CreatedAt, 0)
		opts.Logger.Info("Loaded existing encryption keys")
	} else if os.IsNotExist(err) {
		// Generate new keys
		km, err = NewKeyManager()
		if err != nil {
			return nil, fmt.Errorf("failed to create new key manager: %w", err)
		}
		lastRotation = time.Now()

		// Save new keys
		err = saveKeyFile(opts.KeyFilePath, km.encryptionKey, km.hmacKey, lastRotation, opts.MasterPassword, opts.SecureOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to save new key file: %w", err)
		}
		opts.Logger.Info("Generated and saved new encryption keys")
	} else {
		return nil, fmt.Errorf("error checking key file: %w", err)
	}

	pkm := &PersistentKeyManager{
		KeyManager:       km,
		keyFilePath:      opts.KeyFilePath,
		rotationInterval: opts.RotationInterval,
		lastRotation:     lastRotation,
		masterPassword:   opts.MasterPassword,
		secureOptions:    opts.SecureOptions,
		logger:           opts.Logger,
	}

	// Start key rotation if interval is set
	if opts.RotationInterval > 0 {
		go pkm.startKeyRotation()
	}

	return pkm, nil
}

// startKeyRotation periodically rotates the encryption keys
func (pkm *PersistentKeyManager) startKeyRotation() {
	ticker := time.NewTicker(1 * time.Hour) // Check hourly
	defer ticker.Stop()

	for range ticker.C {
		pkm.checkAndRotateKeys()
	}
}

// checkAndRotateKeys checks if key rotation is needed and performs it
func (pkm *PersistentKeyManager) checkAndRotateKeys() {
	pkm.mu.RLock()
	timeSinceRotation := time.Since(pkm.lastRotation)
	inProgress := pkm.rotationInProgress
	pkm.mu.RUnlock()

	if inProgress || timeSinceRotation < pkm.rotationInterval {
		return
	}

	// Set rotation flag
	pkm.mu.Lock()
	if pkm.rotationInProgress {
		pkm.mu.Unlock()
		return
	}
	pkm.rotationInProgress = true
	pkm.mu.Unlock()

	defer func() {
		pkm.mu.Lock()
		pkm.rotationInProgress = false
		pkm.mu.Unlock()
	}()

	// Before rotation, create a backup of current keys
	if err := pkm.BackupKeys(); err != nil {
		pkm.logger.Warnf("Failed to create backup before key rotation: %v", err)
		// Continue with rotation even if backup fails
	}

	// Generate new keys
	newKM, err := NewKeyManager()
	if err != nil {
		pkm.logger.Errorf("Failed to generate new keys during rotation: %v", err)
		return
	}

	// Update keys
	pkm.mu.Lock()
	pkm.encryptionKey = newKM.encryptionKey
	pkm.hmacKey = newKM.hmacKey
	now := time.Now()
	pkm.lastRotation = now
	pkm.mu.Unlock()

	// Save new keys
	err = saveKeyFile(pkm.keyFilePath, newKM.encryptionKey, newKM.hmacKey, now, pkm.masterPassword, pkm.secureOptions)
	if err != nil {
		pkm.logger.Errorf("Failed to save new keys during rotation: %v", err)
		return
	}

	pkm.logger.Info("Successfully rotated encryption keys")
}

// BackupKeys creates a backup of the current keys
func (pkm *PersistentKeyManager) BackupKeys() error {
	// Use CreateSecureBackup instead of manual backup
	backupPath, err := CreateSecureBackup(pkm.keyFilePath, pkm.secureOptions)
	if err != nil {
		return fmt.Errorf("failed to create key backup: %w", err)
	}
	
	pkm.logger.Infof("Created encryption key backup at %s", backupPath)
	return nil
}

// RestoreFromBackup restores keys from a backup file
func (pkm *PersistentKeyManager) RestoreFromBackup(backupPath string) error {
	keyData, err := loadKeyFile(backupPath, pkm.masterPassword)
	if err != nil {
		return fmt.Errorf("failed to load backup key file: %w", err)
	}

	pkm.mu.Lock()
	defer pkm.mu.Unlock()

	pkm.encryptionKey = keyData.EncryptionKey
	pkm.hmacKey = keyData.HMACKey
	pkm.lastRotation = time.Unix(keyData.CreatedAt, 0)

	// Save restored keys to the original location
	err = saveKeyFile(pkm.keyFilePath, pkm.encryptionKey, pkm.hmacKey, pkm.lastRotation, pkm.masterPassword, pkm.secureOptions)
	if err != nil {
		return fmt.Errorf("failed to save restored keys: %w", err)
	}

	pkm.logger.Infof("Successfully restored encryption keys from backup %s", backupPath)
	return nil
}

// SetMasterPassword updates the master password used for encrypting the key file
func (pkm *PersistentKeyManager) SetMasterPassword(newPassword []byte) error {
	if len(newPassword) == 0 {
		return fmt.Errorf("master password cannot be empty")
	}

	// Lock to prevent concurrent key operations
	pkm.mu.Lock()
	defer pkm.mu.Unlock()

	// Update the master password
	oldPassword := pkm.masterPassword
	pkm.masterPassword = newPassword

	// Re-encrypt and save the key file with the new password
	err := saveKeyFile(pkm.keyFilePath, pkm.encryptionKey, pkm.hmacKey, pkm.lastRotation, newPassword, pkm.secureOptions)
	if err != nil {
		// Revert to old password if saving fails
		pkm.masterPassword = oldPassword
		return fmt.Errorf("failed to save key file with new master password: %w", err)
	}

	pkm.logger.Info("Successfully updated master password")
	return nil
}

// Helper functions for key file operations

func loadKeyFile(path string, masterPassword []byte) (*KeyFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Decrypt if master password is provided
	if len(masterPassword) > 0 {
		data, err = DecryptFileContents(data, masterPassword)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt key file: %w", err)
		}
	}

	var keyFile KeyFile
	if err := json.Unmarshal(data, &keyFile); err != nil {
		return nil, err
	}

	return &keyFile, nil
}

func saveKeyFile(path string, encKey, hmacKey []byte, creationTime time.Time, masterPassword []byte, opts *SecureFileOptions) error {
	keyFile := KeyFile{
		EncryptionKey: encKey,
		HMACKey:       hmacKey,
		CreatedAt:     creationTime.Unix(),
		Version:       1, // Current schema version
	}

	data, err := json.Marshal(keyFile)
	if err != nil {
		return err
	}

	// Encrypt if master password is provided
	if len(masterPassword) > 0 {
		data, err = EncryptFileContents(data, masterPassword)
		if err != nil {
			return fmt.Errorf("failed to encrypt key file: %w", err)
		}
	}

	// Use secure file writing
	return SecureWriteFile(path, data, opts)
} 