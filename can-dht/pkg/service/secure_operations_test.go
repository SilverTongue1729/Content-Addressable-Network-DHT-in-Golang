package service

import (
	"context"
	"testing"
	"time"
)

func TestSecureOperations(t *testing.T) {
	// Create a test config with encryption enabled
	config := &CANConfig{
		Dimensions:        2,
		DataDir:           "test-data",
		EnableEncryption:  true,
		ReplicationFactor: 1,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  3 * time.Second,
	}

	// Create a server with encryption
	server, err := NewCANServer("test-node", "localhost:8080", config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Verify encryption is enabled
	if !server.IsEncryptionEnabled() {
		t.Fatalf("Expected encryption to be enabled")
	}

	// Setup test data
	ctx := context.Background()
	testKey := "user123"
	testValue := []byte("sensitive_data")

	// Test SecurePut
	err = server.SecurePut(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to store encrypted data: %v", err)
	}

	// Test SecureGet
	retrievedValue, err := server.SecureGet(ctx, testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve encrypted data: %v", err)
	}

	// Verify decrypted value matches original
	if string(retrievedValue) != string(testValue) {
		t.Errorf("Expected %q, got %q", string(testValue), string(retrievedValue))
	}

	// Test with disabled encryption
	configNoEncryption := &CANConfig{
		Dimensions:        2,
		DataDir:           "test-data-no-enc",
		EnableEncryption:  false,
		ReplicationFactor: 1,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  3 * time.Second,
	}

	serverNoEncryption, err := NewCANServer("test-node-no-enc", "localhost:8081", configNoEncryption)
	if err != nil {
		t.Fatalf("Failed to create server without encryption: %v", err)
	}
	defer serverNoEncryption.Stop()

	// Verify encryption is disabled
	if serverNoEncryption.IsEncryptionEnabled() {
		t.Fatalf("Expected encryption to be disabled")
	}

	// Test SecurePut with encryption disabled (should fail)
	err = serverNoEncryption.SecurePut(ctx, testKey, testValue)
	if err == nil {
		t.Errorf("Expected error when using SecurePut with encryption disabled")
	}

	// Test SecureGet with encryption disabled (should fail)
	_, err = serverNoEncryption.SecureGet(ctx, testKey)
	if err == nil {
		t.Errorf("Expected error when using SecureGet with encryption disabled")
	}
}

// TestDataIntegrity tests the data integrity verification
func TestDataIntegrity(t *testing.T) {
	// Create a test config with encryption enabled
	config := &CANConfig{
		Dimensions:        2,
		DataDir:           "test-data-integrity",
		EnableEncryption:  true,
		ReplicationFactor: 1,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  3 * time.Second,
	}

	// Create a server with encryption
	server, err := NewCANServer("test-node-integrity", "localhost:8082", config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Setup test data
	ctx := context.Background()
	testKey := "integrity_test"
	testValue := []byte("data_to_verify")

	// Store data securely
	err = server.SecurePut(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to store encrypted data: %v", err)
	}

	// Verify data integrity
	integrityErrors := server.VerifyDataIntegrity(ctx)
	if len(integrityErrors) > 0 {
		t.Errorf("Unexpected integrity errors: %v", integrityErrors)
	}

	// Simulating a tampered value would require direct manipulation of the storage
	// which is beyond the scope of this test
}
 