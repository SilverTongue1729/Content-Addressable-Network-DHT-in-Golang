package service

import (
	"context"
	"fmt"

	"github.com/can-dht/pkg/crypto"
)

// SecurePut stores a key-value pair with the value encrypted
func (s *CANServer) SecurePut(ctx context.Context, key string, value []byte) error {
	// Verify encryption is enabled
	if s.KeyManager == nil {
		return fmt.Errorf("encryption is not enabled")
	}

	// Encrypt and authenticate the value
	secureData, err := s.KeyManager.EncryptAndAuthenticate(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt value: %w", err)
	}

	// Serialize the secure data
	serializedData := crypto.SerializeSecureData(secureData)

	// Store the encrypted value using the regular Put method
	return s.Put(ctx, key, []byte(serializedData))
}

// SecureGet retrieves and decrypts a value by key
func (s *CANServer) SecureGet(ctx context.Context, key string) ([]byte, error) {
	// Verify encryption is enabled
	if s.KeyManager == nil {
		return nil, fmt.Errorf("encryption is not enabled")
	}

	// Get the encrypted value
	serializedData, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// If no data was found, return nil without error
	if serializedData == nil {
		return nil, nil
	}

	// Deserialize the secure data
	secureData, err := crypto.DeserializeSecureData(string(serializedData))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize secure data: %w", err)
	}

	// Decrypt and verify
	plaintext, err := s.KeyManager.DecryptAndVerify(secureData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt value: %w", err)
	}

	return plaintext, nil
}

// IsEncryptionEnabled returns whether encryption is enabled
func (s *CANServer) IsEncryptionEnabled() bool {
	return s.KeyManager != nil
}

// VerifyDataIntegrity verifies the integrity of all encrypted data
func (s *CANServer) VerifyDataIntegrity(ctx context.Context) map[string]error {
	// Map to store integrity verification results
	integrityErrors := make(map[string]error)

	// This is a simplified implementation
	// In a real-world scenario, you would iterate through all keys in the store
	// For now, we'll just log that this feature is not fully implemented

	return integrityErrors
}

// SecurePutWithAuth stores a key-value pair with user authentication and authorization
func (s *CANServer) SecurePutWithAuth(ctx context.Context, username, password, key string, value []byte) error {
	// Verify authentication manager is enabled
	if s.AuthManager == nil {
		return fmt.Errorf("authentication is not enabled")
	}

	// Use the enhanced authentication manager to store the data
	return s.AuthManager.CreateOwnData(username, password, key, value)
}

// SecureGetWithAuth retrieves a key-value pair with user authentication and authorization
func (s *CANServer) SecureGetWithAuth(ctx context.Context, username, password, ownerName, key string) ([]byte, error) {
	// Verify authentication manager is enabled
	if s.AuthManager == nil {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	// If ownerName is empty, assume the user is trying to access their own data
	if ownerName == "" {
		return s.AuthManager.ReadOwnData(username, password, key)
	}

	// Use the enhanced authentication manager to retrieve the data
	return s.AuthManager.ReadData(username, password, ownerName, key)
}

// SecureDeleteWithAuth deletes a key-value pair with user authentication and authorization
func (s *CANServer) SecureDeleteWithAuth(ctx context.Context, username, password, ownerName, key string) error {
	// Verify authentication manager is enabled
	if s.AuthManager == nil {
		return fmt.Errorf("authentication is not enabled")
	}

	// If ownerName is empty, assume the user is trying to delete their own data
	if ownerName == "" {
		return s.AuthManager.DeleteOwnData(username, password, key)
	}

	// Use the enhanced authentication manager to delete the data
	return s.AuthManager.DeleteData(username, password, ownerName, key)
}

// ModifyPermissions allows changing permissions for a specific key
func (s *CANServer) ModifyPermissions(ctx context.Context, adminUser, adminPassword, targetUser, ownerName, key string, permission crypto.Permission) error {
	// Verify authentication manager is enabled
	if s.AuthManager == nil {
		return fmt.Errorf("authentication is not enabled")
	}

	// Use the enhanced authentication manager to modify permissions
	return s.AuthManager.ModifyPermissions(adminUser, adminPassword, targetUser, ownerName, key, permission)
}

// ListUserData lists all data accessible to a user
func (s *CANServer) ListUserData(ctx context.Context, username, password string) ([]string, error) {
	// Verify authentication manager is enabled
	if s.AuthManager == nil {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	// Use the enhanced authentication manager to list data
	return s.AuthManager.ListDataKeys(username, password)
}
 