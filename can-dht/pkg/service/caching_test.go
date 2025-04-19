package service

import (
	"context"
	"testing"
	"time"

	"github.com/can-dht/pkg/node"
)

func TestCaching(t *testing.T) {
	// Create a test node
	nodeID := node.NodeID("test-node")
	address := "localhost:8080"
	
	// Create config with caching enabled
	config := DefaultCANConfig()
	config.EnableCache = true
	config.CacheSize = 100
	config.CacheTTL = 1 * time.Hour
	
	// Create a server
	server, err := NewCANServer(nodeID, address, &config)
	if err != nil {
		t.Fatalf("Failed to create CAN server: %v", err)
	}
	
	// Check that cache was initialized
	if server.Cache == nil {
		t.Fatalf("Cache was not initialized")
	}
	
	// Create a test key and value
	key := "test-key"
	value := []byte("test-value")
	
	// Mock a put operation by directly putting to the store
	// In a real scenario, this would be done through the Put method
	err = server.Store.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value to store: %v", err)
	}
	
	// Override normal zone checks for this test
	originalZoneContains := server.Node.Zone.Contains
	server.Node.Zone.Contains = func(p node.Point) bool {
		return true // Always return true for this test
	}
	defer func() {
		// Restore original behavior
		server.Node.Zone.Contains = originalZoneContains
	}()
	
	// Access the key multiple times to verify caching behavior
	for i := 0; i < 5; i++ {
		retrievedValue, err := server.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		
		if string(retrievedValue) != string(value) {
			t.Errorf("Retrieved value doesn't match expected: got %s, want %s", 
				string(retrievedValue), string(value))
		}
	}
	
	// Check cache metrics
	metrics := server.Cache.GetMetrics()
	if metrics.Hits < 3 {
		t.Errorf("Expected at least 3 cache hits, got %d", metrics.Hits)
	}
	
	// Test cache invalidation on delete
	err = server.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
	
	// Make sure the value is no longer in the cache
	cachedValue, found := server.Cache.Get(key)
	if found {
		t.Errorf("Key should have been removed from cache but found value: %s", string(cachedValue))
	}
	
	// Test cache invalidation on put
	newValue := []byte("new-value")
	err = server.Put(context.Background(), key, newValue)
	if err != nil {
		t.Fatalf("Failed to put new value: %v", err)
	}
	
	// Get the value again, it should populate the cache
	retrievedValue, err := server.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get value after put: %v", err)
	}
	
	if string(retrievedValue) != string(newValue) {
		t.Errorf("Retrieved value doesn't match expected after put: got %s, want %s", 
			string(retrievedValue), string(newValue))
	}
	
	// Final cache check
	cachedValue, found = server.Cache.Get(key)
	if !found {
		t.Errorf("Key should be in cache after get but not found")
	} else if string(cachedValue) != string(newValue) {
		t.Errorf("Cached value doesn't match expected: got %s, want %s", 
			string(cachedValue), string(newValue))
	}
} 