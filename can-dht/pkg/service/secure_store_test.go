package service

import (
	"testing"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/node"
)

func TestSecureStoreOperations(t *testing.T) {
	// Create a test node
	zone, err := node.NewZone(node.Point{0, 0}, node.Point{1, 1})
	if err != nil {
		t.Fatalf("Failed to create zone: %v", err)
	}

	testNode := node.NewNode("test-node", "localhost:8080", zone, 2)

	// Create a key manager
	keyManager, err := crypto.NewKeyManager()
	if err != nil {
		t.Fatalf("Failed to create key manager: %v", err)
	}

	// Create the secure store
	secureStore := NewSecureStore(testNode, keyManager)

	// Test storing and retrieving encrypted data
	testKey := "user123"
	testValue := "sensitive_data"

	// Store the data
	err = secureStore.Put(testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to store data: %v", err)
	}

	// Verify the value is encrypted in the node's storage
	rawValue, exists := testNode.Get(testKey)
	if !exists {
		t.Fatal("Data not stored in node")
	}

	// The raw value should be different from the plain text
	if rawValue == testValue {
		t.Error("Value is not encrypted")
	}

	// Retrieve and decrypt the data
	retrievedValue, exists, err := secureStore.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve data: %v", err)
	}
	if !exists {
		t.Fatal("Data not found")
	}

	// Verify the decrypted value matches the original
	if retrievedValue != testValue {
		t.Errorf("Expected %q, got %q", testValue, retrievedValue)
	}

	// Test integrity verification
	integrityErrors := secureStore.VerifyIntegrity()
	if len(integrityErrors) > 0 {
		t.Errorf("Integrity errors found: %v", integrityErrors)
	}

	// Test with tampered data (direct manipulation of node's data)
	testNode.Put(testKey, "tampered_data")

	// Attempt to retrieve the tampered data
	_, _, err = secureStore.Get(testKey)
	if err == nil {
		t.Error("Expected error when retrieving tampered data")
	}

	// Test deletion
	deleted := secureStore.Delete(testKey)
	if !deleted {
		t.Error("Failed to delete data")
	}

	// Verify data is gone
	_, exists, _ = secureStore.Get(testKey)
	if exists {
		t.Error("Data still exists after deletion")
	}
}
