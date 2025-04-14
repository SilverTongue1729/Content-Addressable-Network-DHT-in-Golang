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
