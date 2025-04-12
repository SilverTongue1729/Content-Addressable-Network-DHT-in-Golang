package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/can-dht/pkg/node"
)

// LoadStats tracks load statistics for a node
type LoadStats struct {
	// RequestCount is the total number of requests processed
	RequestCount int64

	// HotKeys tracks frequently accessed keys
	HotKeys map[string]int64

	// LastRebalanced is when load balancing was last performed
	LastRebalanced time.Time

	// mu protects access to LoadStats
	mu sync.RWMutex
}

// NewLoadStats creates a new LoadStats instance
func NewLoadStats() *LoadStats {
	return &LoadStats{
		RequestCount:   0,
		HotKeys:        make(map[string]int64),
		LastRebalanced: time.Now(),
	}
}

// RecordRequest records a request for load balancing purposes
func (s *CANServer) RecordRequest(key string, isWrite bool) {
	// Initialize load stats if not already initialized
	if s.LoadStats == nil {
		s.LoadStats = NewLoadStats()
	}

	s.LoadStats.mu.Lock()
	defer s.LoadStats.mu.Unlock()

	// Increment total request count
	s.LoadStats.RequestCount++

	// Record hot key access
	if _, exists := s.LoadStats.HotKeys[key]; !exists {
		s.LoadStats.HotKeys[key] = 0
	}
	s.LoadStats.HotKeys[key]++

	// Check if we need to rebalance
	if s.LoadStats.RequestCount%100 == 0 {
		// Only check every 100 requests
		if time.Since(s.LoadStats.LastRebalanced) > 5*time.Minute {
			// Rebalance at most every 5 minutes
			go s.CheckLoadBalance(context.Background())
		}
	}
}

// CheckLoadBalance checks if load balancing is needed
func (s *CANServer) CheckLoadBalance(ctx context.Context) {
	s.LoadStats.mu.Lock()
	s.LoadStats.LastRebalanced = time.Now()
	s.LoadStats.mu.Unlock()

	// Find hot keys
	hotKeys := s.identifyHotKeys()
	if len(hotKeys) == 0 {
		return
	}

	// Perform load balancing actions
	s.rebalanceLoad(ctx, hotKeys)
}

// identifyHotKeys identifies hot keys based on access patterns
func (s *CANServer) identifyHotKeys() []string {
	s.LoadStats.mu.RLock()
	defer s.LoadStats.mu.RUnlock()

	// This is a simplified algorithm - in a real system, we'd use a more
	// sophisticated approach that considers access frequency, recency, etc.
	hotKeys := make([]string, 0)
	threshold := int64(s.LoadStats.RequestCount / 10) // Keys accessed more than 10% of total requests

	if threshold < 10 {
		threshold = 10 // Minimum threshold
	}

	for key, count := range s.LoadStats.HotKeys {
		if count > threshold {
			hotKeys = append(hotKeys, key)
		}
	}

	return hotKeys
}

// rebalanceLoad rebalances load by adjusting zones or replicating hot keys
func (s *CANServer) rebalanceLoad(ctx context.Context, hotKeys []string) {
	log.Printf("Performing load balancing for %d hot keys", len(hotKeys))

	// In a more sophisticated implementation, we could:
	// 1. Split zones more finely around hot keys
	// 2. Create more replicas for hot keys
	// 3. Adjust routing to distribute requests more evenly

	// For now, we'll just ensure hot keys are replicated to all neighbors
	s.replicateHotKeys(ctx, hotKeys)
}

// replicateHotKeys replicates hot keys to neighbors for load distribution
func (s *CANServer) replicateHotKeys(ctx context.Context, hotKeys []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, key := range hotKeys {
		// Get the value from our local storage
		value, exists, err := s.Store.Get(key)
		if err != nil || !exists {
			log.Printf("Failed to get hot key %s for replication: %v", key, err)
			continue
		}

		// Replicate to all neighbors, regardless of the replication factor
		// This is specifically for load balancing
		if err := s.ReplicateData(ctx, key, value); err != nil {
			log.Printf("Failed to replicate hot key %s: %v", key, err)
		}
	}
}

// AdjustZone adjusts the zone of this node to better balance load
func (s *CANServer) AdjustZone(ctx context.Context, neighborID node.NodeID) error {
	// This is a more advanced operation that would:
	// 1. Identify a neighbor with lower load
	// 2. Negotiate a zone adjustment with that neighbor
	// 3. Transfer keys that would move to the neighbor
	// 4. Update both nodes' zones
	// 5. Notify other neighbors of the change

	// This would be quite complex and context-dependent, so for now
	// we're just providing a placeholder implementation
	log.Printf("Zone adjustment with neighbor %s not implemented yet", neighborID)
	return nil
}
