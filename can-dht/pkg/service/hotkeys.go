package service

import (
	"context"
	"math"
	"sync"
	"time"
	"log"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/internal/proto"
)

// KeyAccessStats tracks access statistics for a specific key
type KeyAccessStats struct {
	// LastAccessed is the timestamp of the most recent access
	LastAccessed time.Time
	
	// AccessCount is the total number of accesses
	AccessCount uint32
	
	// DecayedCount is a time-weighted access count
	DecayedCount float64
	
	// IsHot indicates if this key is currently classified as "hot"
	IsHot bool
}

// HotKeyTracker monitors key access patterns and identifies "hot" keys
type HotKeyTracker struct {
	// keyStats maps keys to their access statistics
	keyStats map[string]*KeyAccessStats
	
	// hotKeys contains the set of currently hot keys
	hotKeys map[string]struct{}
	
	// hotThreshold defines when a key is considered "hot"
	hotThreshold float64
	
	// decayFactor determines how quickly older accesses lose weight
	// Lower values mean faster decay (more emphasis on recent accesses)
	decayFactor float64
	
	// decayInterval is how often to apply decay to access counts
	decayInterval time.Duration
	
	// maxTrackedKeys limits the number of keys we track to prevent memory bloat
	maxTrackedKeys int
	
	// mu protects concurrent access to maps
	mu sync.RWMutex
}

// NewHotKeyTracker creates a new hot key tracker
func NewHotKeyTracker(hotThreshold float64, decayFactor float64, decayInterval time.Duration, maxKeys int) *HotKeyTracker {
	return &HotKeyTracker{
		keyStats:       make(map[string]*KeyAccessStats),
		hotKeys:        make(map[string]struct{}),
		hotThreshold:   hotThreshold,
		decayFactor:    decayFactor,
		decayInterval:  decayInterval,
		maxTrackedKeys: maxKeys,
	}
}

// RecordAccess records a key access and updates its statistics
func (h *HotKeyTracker) RecordAccess(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Get existing stats or create new ones
	stats, exists := h.keyStats[key]
	if !exists {
		// If we're at capacity, consider pruning least accessed keys
		if len(h.keyStats) >= h.maxTrackedKeys {
			h.pruneLeastAccessed()
		}
		
		stats = &KeyAccessStats{
			AccessCount:  0,
			DecayedCount: 0,
		}
		h.keyStats[key] = stats
	}
	
	now := time.Now()
	
	// Calculate time-weighted decay based on time since last access
	if !stats.LastAccessed.IsZero() {
		elapsedSeconds := now.Sub(stats.LastAccessed).Seconds()
		// Apply decay factor based on elapsed time
		stats.DecayedCount *= math.Pow(h.decayFactor, elapsedSeconds/h.decayInterval.Seconds())
	}
	
	// Update stats
	stats.AccessCount++
	stats.DecayedCount++
	stats.LastAccessed = now
	
	// Check if key has become hot
	if stats.DecayedCount >= h.hotThreshold && !stats.IsHot {
		stats.IsHot = true
		h.hotKeys[key] = struct{}{}
	}
}

// IsHotKey checks if a key is currently considered "hot"
func (h *HotKeyTracker) IsHotKey(key string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	stats, exists := h.keyStats[key]
	if !exists {
		return false
	}
	return stats.IsHot
}

// GetHotKeys returns the current set of hot keys
func (h *HotKeyTracker) GetHotKeys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	keys := make([]string, 0, len(h.hotKeys))
	for key := range h.hotKeys {
		keys = append(keys, key)
	}
	return keys
}

// ApplyDecay applies time decay to all tracked keys
// Should be called periodically
func (h *HotKeyTracker) ApplyDecay() {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	now := time.Now()
	
	// Apply decay to all keys and update hot status
	for key, stats := range h.keyStats {
		if stats.LastAccessed.IsZero() {
			continue
		}
		
		elapsedSeconds := now.Sub(stats.LastAccessed).Seconds()
		// Apply decay factor based on elapsed time
		stats.DecayedCount *= math.Pow(h.decayFactor, elapsedSeconds/h.decayInterval.Seconds())
		
		// Update hot status
		wasHot := stats.IsHot
		stats.IsHot = stats.DecayedCount >= h.hotThreshold
		
		// Update hot keys map
		if wasHot && !stats.IsHot {
			delete(h.hotKeys, key)
		} else if !wasHot && stats.IsHot {
			h.hotKeys[key] = struct{}{}
		}
	}
}

// pruneLeastAccessed removes the least accessed keys to stay under maxTrackedKeys
func (h *HotKeyTracker) pruneLeastAccessed() {
	// Don't prune hot keys
	// Find the coldest key to remove
	var coldestKey string
	lowestScore := math.MaxFloat64
	
	for key, stats := range h.keyStats {
		if !stats.IsHot && stats.DecayedCount < lowestScore {
			coldestKey = key
			lowestScore = stats.DecayedCount
		}
	}
	
	// Remove the coldest key if found
	if coldestKey != "" {
		delete(h.keyStats, coldestKey)
	}
}

// GetKeyStats returns access statistics for a specific key
func (h *HotKeyTracker) GetKeyStats(key string) (KeyAccessStats, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	stats, exists := h.keyStats[key]
	if !exists {
		return KeyAccessStats{}, false
	}
	
	// Return a copy to avoid concurrent modification issues
	return KeyAccessStats{
		LastAccessed: stats.LastAccessed,
		AccessCount:  stats.AccessCount,
		DecayedCount: stats.DecayedCount,
		IsHot:        stats.IsHot,
	}, true
}

// GetHotKeyCount returns the current number of hot keys
func (h *HotKeyTracker) GetHotKeyCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return len(h.hotKeys)
}

// ReplicateHotKeys analyzes current hot keys and replicates them as needed
func (s *CANServer) ReplicateHotKeys(ctx context.Context) error {
	if s.HotKeyTracker == nil || !s.Config.EnableFrequencyBasedReplication {
		return nil
	}

	// Get the current list of hot keys
	hotKeys := s.HotKeyTracker.GetHotKeys()
	
	if len(hotKeys) == 0 {
		return nil
	}
	
	log.Printf("Analyzing %d hot keys for potential replication", len(hotKeys))
	
	// For each hot key, check if we need to replicate it
	for _, key := range hotKeys {
		// Check if this node is responsible for the key
		point := s.Router.HashToPoint(key)
		
		// Skip if we're not responsible for this key's data
		if !s.Node.Zone.Contains(point) {
			continue
		}
		
		// Get current replicas for this key
		replicas := s.ReplicaTracker.GetReplicas(key)
		
		// Calculate how many additional replicas we need
		totalReplicasNeeded := s.Config.HotKeyReplicationFactor
		frequencyReplicas := 0
		
		// Count existing frequency-based replicas
		for _, info := range replicas {
			if info.IsFrequencyBased {
				frequencyReplicas++
			}
		}
		
		additionalReplicas := totalReplicasNeeded - frequencyReplicas
		
		if additionalReplicas <= 0 {
			continue
		}
		
		log.Printf("Hot key %s needs %d additional frequency-based replicas", key, additionalReplicas)
		
		// Get the value to replicate
		value, exists, err := s.Store.Get(key)
		if err != nil || !exists {
			log.Printf("Failed to retrieve hot key %s for replication: %v", key, err)
			continue
		}
		
		// Create replicas for this hot key
		if err := s.replicateHotKeyData(ctx, key, value, additionalReplicas); err != nil {
			log.Printf("Failed to replicate hot key %s: %v", key, err)
		}
	}
	
	return nil
}

// replicateHotKeyData creates frequency-based replicas for a hot key
func (s *CANServer) replicateHotKeyData(ctx context.Context, key string, value []byte, count int) error {
	// Get neighbors for potential replication
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()
	
	if len(neighbors) == 0 {
		return nil // No neighbors to replicate to
	}
	
	// Calculate expiration time for replicas
	expiresAt := time.Now().Add(s.Config.HotKeyTTL)
	
	// Track successful replications
	successCount := 0
	
	// Try to replicate to each neighbor
	for _, neighbor := range neighbors {
		// Stop if we've created enough replicas
		if successCount >= count {
			break
		}
		
		// Skip if this neighbor already has a replica of this key
		replicas := s.ReplicaTracker.GetReplicas(key)
		if _, exists := replicas[neighbor.ID]; exists {
			continue
		}
		
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for hot key replication: %v", neighbor.ID, err)
			continue
		}
		
		// Create a special put request for hot key replication
		req := &pb.PutRequest{
			Key:          key,
			Value:        value,
			Forward:      true,
			IsHotKey:     true,
			HotKeyExpiry: expiresAt.UnixNano(),
		}
		
		// Send the replication request
		resp, err := client.Put(ctx, req)
		conn.Close()
		
		if err != nil || !resp.Success {
			log.Printf("Failed to replicate hot key to neighbor %s: %v", neighbor.ID, err)
			continue
		}
		
		// Record successful hot key replication
		s.ReplicaTracker.AddReplicaWithOptions(key, neighbor.ID, []node.NodeID{neighbor.ID}, expiresAt, true)
		successCount++
		log.Printf("Successfully created frequency-based replica of hot key %s on node %s", key, neighbor.ID)
	}
	
	return nil
}

// StartHotKeyAnalysis starts the background process for hot key analysis and replication
func (s *CANServer) StartHotKeyAnalysis() {
	if !s.Config.EnableFrequencyBasedReplication || s.HotKeyTracker == nil {
		return
	}

	// Start decay process
	go func() {
		decayTicker := time.NewTicker(1 * time.Minute) // Apply decay every minute
		defer decayTicker.Stop()

		for {
			select {
			case <-decayTicker.C:
				s.HotKeyTracker.ApplyDecay()
			case <-s.ctx.Done():
				log.Printf("Hot key decay process stopped")
				return
			}
		}
	}()

	// Start replication process
	go func() {
		replicationTicker := time.NewTicker(s.Config.HotKeyAnalysisInterval)
		defer replicationTicker.Stop()

		for {
			select {
			case <-replicationTicker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := s.ReplicateHotKeys(ctx)
				cancel()
				if err != nil {
					log.Printf("Error in hot key replication: %v", err)
				}
			case <-s.ctx.Done():
				log.Printf("Hot key replication process stopped")
				return
			}
		}
	}()

	// Start cleanup process for expired frequency-based replicas
	go func() {
		cleanupTicker := time.NewTicker(1 * time.Hour) // Check for expired replicas hourly
		defer cleanupTicker.Stop()

		for {
			select {
			case <-cleanupTicker.C:
				s.CleanupExpiredHotKeyReplicas()
			case <-s.ctx.Done():
				log.Printf("Hot key replica cleanup process stopped")
				return
			}
		}
	}()

	log.Printf("Started hot key analysis and replication processes")
}

// CleanupExpiredHotKeyReplicas removes expired frequency-based replicas
func (s *CANServer) CleanupExpiredHotKeyReplicas() {
	if s.ReplicaTracker == nil {
		return
	}

	s.ReplicaTracker.mu.Lock()
	defer s.ReplicaTracker.mu.Unlock()

	now := time.Now()
	removedCount := 0

	// Check all hot key replicas
	for key, replicas := range s.ReplicaTracker.HotKeyReplicas {
		for nodeID, info := range replicas {
			// Skip if expiration time not set or not expired
			if info.ExpiresAt.IsZero() || now.Before(info.ExpiresAt) {
				continue
			}

			// Remove the replica
			delete(replicas, nodeID)
			removedCount++

			// Try to delete from the local store if it's our replica
			if nodeID == s.Node.ID {
				if err := s.Store.Delete(key); err != nil {
					log.Printf("Failed to delete expired hot key replica %s: %v", key, err)
				}
			}
		}

		// Clean up empty maps
		if len(replicas) == 0 {
			delete(s.ReplicaTracker.HotKeyReplicas, key)
		}
	}

	if removedCount > 0 {
		log.Printf("Cleaned up %d expired hot key replicas", removedCount)
	}
} 