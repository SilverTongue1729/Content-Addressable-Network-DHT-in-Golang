package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/service"
	"github.com/google/uuid"
)

var (
	dataDir = flag.String("data-dir", "test-data", "Directory for test data storage")
)

func main() {
	flag.Parse()

	log.Println("CAN-DHT Test Client")
	log.Println("==================")

	// Create a unique test ID
	testID := uuid.New().String()[:8]
	log.Printf("Running test with ID: %s", testID)

	// Create a data directory
	testDataDir := fmt.Sprintf("%s/%s", *dataDir, testID)
	if err := os.MkdirAll(testDataDir, 0755); err != nil {
		log.Fatalf("Failed to create test data directory: %v", err)
	}
	defer os.RemoveAll(testDataDir) // Clean up after test

	// Create a test server
	nodeID := node.NodeID(fmt.Sprintf("test-node-%s", testID))
	address := fmt.Sprintf("localhost:0") // Use port 0 to avoid conflicts

	config := service.DefaultCANConfig()
	config.DataDir = testDataDir
	config.Dimensions = 2

	log.Printf("Initializing test node with ID %s...", nodeID)
	server, err := service.NewCANServer(nodeID, address, config)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	// Start the server
	server.Start()
	log.Printf("Test node started")

	// Run basic operations test
	if err := runBasicTest(server); err != nil {
		log.Fatalf("Test failed: %v", err)
	}

	// Shut down
	log.Println("Test completed successfully")
	if err := server.Stop(); err != nil {
		log.Printf("Warning: error stopping server: %v", err)
	}
}

func runBasicTest(server *service.CANServer) error {
	ctx := context.Background()

	// Test data
	testKey := "test-key"
	testValue := []byte("test-value")

	// Test PUT operation
	log.Printf("Testing PUT operation...")
	if err := server.Put(ctx, testKey, testValue); err != nil {
		return fmt.Errorf("PUT failed: %w", err)
	}
	log.Printf("PUT successful")

	// Test GET operation
	log.Printf("Testing GET operation...")
	retrievedValue, err := server.Get(ctx, testKey)
	if err != nil {
		return fmt.Errorf("GET failed: %w", err)
	}

	if string(retrievedValue) != string(testValue) {
		return fmt.Errorf("GET returned wrong value: expected %q, got %q", testValue, retrievedValue)
	}
	log.Printf("GET successful: retrieved %q", retrievedValue)

	// Test DELETE operation
	log.Printf("Testing DELETE operation...")
	if err := server.Delete(ctx, testKey); err != nil {
		return fmt.Errorf("DELETE failed: %w", err)
	}
	log.Printf("DELETE successful")

	// Verify the key is gone
	log.Printf("Verifying key was deleted...")
	_, err = server.Get(ctx, testKey)
	if err == nil {
		return fmt.Errorf("key still exists after DELETE")
	}
	log.Printf("Key properly deleted")

	// Test storing and retrieving multiple values
	log.Printf("Testing multiple values...")
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := []byte(fmt.Sprintf("value-%d", i))

		if err := server.Put(ctx, k, v); err != nil {
			return fmt.Errorf("failed to PUT key %s: %w", k, err)
		}
	}
	log.Printf("Stored 10 values successfully")

	// Retrieve them all
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		v, err := server.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("failed to GET key %s: %w", k, err)
		}

		if string(v) != expectedValue {
			return fmt.Errorf("wrong value for key %s: expected %q, got %q", k, expectedValue, v)
		}
	}
	log.Printf("Retrieved 10 values successfully")

	// Test encryption
	if server.Config.EnableEncryption {
		log.Printf("Testing encryption...")

		// Store a value with encryption
		secureKey := "secure-data"
		secureValue := []byte("this is sensitive information")

		if err := server.Put(ctx, secureKey, secureValue); err != nil {
			return fmt.Errorf("failed to PUT encrypted value: %w", err)
		}

		// Check if we can retrieve and decrypt it
		decrypted, err := server.Get(ctx, secureKey)
		if err != nil {
			return fmt.Errorf("failed to GET encrypted value: %w", err)
		}

		if string(decrypted) != string(secureValue) {
			return fmt.Errorf("encryption/decryption failed: expected %q, got %q", secureValue, decrypted)
		}

		log.Printf("Encryption test passed")
	}

	// Test concurrent operations
	log.Printf("Testing concurrent operations...")
	errorCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			k := fmt.Sprintf("concurrent-key-%d", id)
			v := []byte(fmt.Sprintf("concurrent-value-%d", id))

			// Put
			if err := server.Put(ctx, k, v); err != nil {
				errorCh <- fmt.Errorf("concurrent PUT failed: %w", err)
				return
			}

			// Small delay to test for race conditions
			time.Sleep(10 * time.Millisecond)

			// Get
			retrieved, err := server.Get(ctx, k)
			if err != nil {
				errorCh <- fmt.Errorf("concurrent GET failed: %w", err)
				return
			}

			if string(retrieved) != string(v) {
				errorCh <- fmt.Errorf("concurrent operation returned wrong value: expected %q, got %q", v, retrieved)
				return
			}

			errorCh <- nil
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < 10; i++ {
		if err := <-errorCh; err != nil {
			return fmt.Errorf("concurrent test failed: %w", err)
		}
	}
	log.Printf("Concurrent operations test passed")

	return nil
}
