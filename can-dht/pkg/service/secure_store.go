package service

import (
	"fmt"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/node"
)

// SecureStore provides secure key-value operations with encryption and authentication
type SecureStore struct {
	// The node that holds the data
	node *node.Node

	// Key manager for encryption and authentication
	keyManager *crypto.KeyManager
}

// NewSecureStore creates a new secure store with the given node and key manager
func NewSecureStore(node *node.Node, keyManager *crypto.KeyManager) *SecureStore {
	return &SecureStore{
		node:       node,
		keyManager: keyManager,
	}
}

// Put stores a key-value pair with the value encrypted
func (s *SecureStore) Put(key, value string) error {
	// Encrypt and authenticate the value
	secureData, err := s.keyManager.EncryptAndAuthenticate([]byte(value))
	if err != nil {
		return fmt.Errorf("failed to encrypt value: %w", err)
	}

	// Serialize the secure data
	serialized := crypto.SerializeSecureData(secureData)

	// Store the encrypted value
	s.node.Put(key, serialized)

	return nil
}

// Get retrieves and decrypts a value by key
func (s *SecureStore) Get(key string) (string, bool, error) {
	// Get the encrypted value
	serialized, exists := s.node.Get(key)
	if !exists {
		return "", false, nil
	}

	// Deserialize the secure data
	secureData, err := crypto.DeserializeSecureData(serialized)
	if err != nil {
		return "", true, fmt.Errorf("failed to deserialize secure data: %w", err)
	}

	// Decrypt and verify
	plaintext, err := s.keyManager.DecryptAndVerify(secureData)
	if err != nil {
		return "", true, fmt.Errorf("failed to decrypt value: %w", err)
	}

	return string(plaintext), true, nil
}

// Delete removes a key-value pair
func (s *SecureStore) Delete(key string) bool {
	return s.node.Delete(key)
}

// VerifyIntegrity checks the integrity of all stored data
func (s *SecureStore) VerifyIntegrity() map[string]error {
	// Map to store integrity verification results
	integrityErrors := make(map[string]error)

	// Get all keys (this would need to be added to the node implementation)
	// For this example, we'll assume we have direct access to the data map
	for key, serialized := range s.node.Data {
		// Deserialize the secure data
		secureData, err := crypto.DeserializeSecureData(serialized)
		if err != nil {
			integrityErrors[key] = fmt.Errorf("failed to deserialize: %w", err)
			continue
		}

		// Verify HMAC
		if !s.keyManager.VerifyHMAC(secureData.Ciphertext, secureData.HMAC) {
			integrityErrors[key] = fmt.Errorf("HMAC verification failed")
		}
	}

	return integrityErrors
}
