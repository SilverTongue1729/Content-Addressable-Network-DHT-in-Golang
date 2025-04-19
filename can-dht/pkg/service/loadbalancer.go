package service

import (
	"context"
	"log"
	"sync"
	"time"
	"container/ring"
	"math"

	"github.com/can-dht/pkg/node"
)

// TimeWindow represents a sliding window of time for tracking requests
type TimeWindow struct {
	// Buckets for the sliding window (each represents a time slot)
	Buckets *ring.Ring
	// Duration of each bucket
	BucketDuration time.Duration
	// Number of buckets in the window
	NumBuckets int
	// Current bucket index
	CurrentBucket int
	// Last update time
	LastUpdate time.Time
}

// NewTimeWindow creates a new time window for tracking requests
func NewTimeWindow(windowDuration time.Duration, numBuckets int) *TimeWindow {
	bucketDuration := windowDuration / time.Duration(numBuckets)
	r := ring.New(numBuckets)
	
	// Initialize all buckets with zeroes
	for i := 0; i < numBuckets; i++ {
		r.Value = int64(0)
		r = r.Next()
	}
	
	return &TimeWindow{
		Buckets:        r,
		BucketDuration: bucketDuration,
		NumBuckets:     numBuckets,
		CurrentBucket:  0,
		LastUpdate:     time.Now(),
	}
}

// Record adds a count to the current time bucket
func (tw *TimeWindow) Record(count int64) {
	now := time.Now()
	elapsed := now.Sub(tw.LastUpdate)
	
	// If we've moved to a new bucket, advance the ring
	if elapsed >= tw.BucketDuration {
		// Calculate how many buckets to advance
		advance := int(elapsed / tw.BucketDuration)
		if advance >= tw.NumBuckets {
			// If we've advanced beyond the window, reset all buckets
			for i := 0; i < tw.NumBuckets; i++ {
				tw.Buckets.Value = int64(0)
				tw.Buckets = tw.Buckets.Next()
			}
		} else {
			// Advance and reset the appropriate number of buckets
			for i := 0; i < advance; i++ {
				tw.Buckets = tw.Buckets.Next()
				tw.Buckets.Value = int64(0)
			}
		}
		tw.LastUpdate = now
	}
	
	// Add to current bucket
	tw.Buckets.Value = tw.Buckets.Value.(int64) + count
}

// GetTotal returns the total count across all buckets
func (tw *TimeWindow) GetTotal() int64 {
	var total int64
	r := tw.Buckets
	for i := 0; i < tw.NumBuckets; i++ {
		if r.Value != nil {
			total += r.Value.(int64)
		}
		r = r.Next()
	}
	return total
}

// GetRate returns the rate of requests per second
func (tw *TimeWindow) GetRate() float64 {
	total := tw.GetTotal()
	duration := time.Duration(tw.NumBuckets) * tw.BucketDuration
	return float64(total) / duration.Seconds()
}

// KeyStats tracks statistics for a specific key
type KeyStats struct {
	// Total access count
	AccessCount int64
	
	// Recent access tracking with time window
	RecentAccesses *TimeWindow
	
	// Last access time
	LastAccess time.Time
	
	// Is this a write-heavy key?
	WriteRatio float64
}

// NewKeyStats creates a new KeyStats
func NewKeyStats() *KeyStats {
	return &KeyStats{
		AccessCount:    0,
		RecentAccesses: NewTimeWindow(5*time.Minute, 10), // 5 minute window with 10 buckets
		LastAccess:     time.Now(),
		WriteRatio:     0.0,
	}
}

// Record records an access to the key
func (ks *KeyStats) Record(isWrite bool) {
	ks.AccessCount++
	ks.RecentAccesses.Record(1)
	ks.LastAccess = time.Now()
	
	// Update write ratio with exponential decay
	alpha := 0.1 // Weight for new observations
	if isWrite {
		ks.WriteRatio = (1-alpha)*ks.WriteRatio + alpha*1.0
	} else {
		ks.WriteRatio = (1-alpha)*ks.WriteRatio
	}
}

// IsHot determines if a key is hot based on its access pattern
func (ks *KeyStats) IsHot(threshold float64) bool {
	// Check if recent access rate exceeds threshold
	return ks.RecentAccesses.GetRate() > threshold
}

// LoadStats tracks load statistics for a node
type LoadStats struct {
	// RequestCount is the total number of requests processed
	RequestCount int64
	
	// RecentRequestWindow tracks recent requests in a sliding window
	RecentRequestWindow *TimeWindow
	
	// KeyStats tracks statistics for individual keys
	KeyStats map[string]*KeyStats
	
	// ZoneLoadFactor is the ratio of this node's zone size to average zone size
	ZoneLoadFactor float64
	
	// RequestRate is the current rate of requests per second
	RequestRate float64
	
	// HotKeyThreshold is the dynamic threshold for hot key detection
	HotKeyThreshold float64
	
	// LastRebalanced is when load balancing was last performed
	LastRebalanced time.Time
	
	// LoadTrend tracks if load is increasing, decreasing, or stable
	LoadTrend string // "increasing", "decreasing", "stable"
	
	// mu protects access to LoadStats
	mu sync.RWMutex
}

// NewLoadStats creates a new LoadStats instance
func NewLoadStats() *LoadStats {
	return &LoadStats{
		RequestCount:       0,
		RecentRequestWindow: NewTimeWindow(1*time.Minute, 6), // 1 minute window with 6 buckets (10 second each)
		KeyStats:           make(map[string]*KeyStats),
		ZoneLoadFactor:     1.0, // Default to neutral
		RequestRate:        0.0,
		HotKeyThreshold:    10.0, // Default threshold (requests/second)
		LastRebalanced:     time.Now(),
		LoadTrend:          "stable",
		mu:                 sync.RWMutex{},
	}
}

// UpdateZoneLoadFactor updates the zone load factor based on current zone size
func (s *CANServer) UpdateZoneLoadFactor() {
	if s.LoadStats == nil {
		s.LoadStats = NewLoadStats()
	}

	s.LoadStats.mu.Lock()
	defer s.LoadStats.mu.Unlock()
	
	myZoneVolume := calculateZoneVolume(s.Node.Zone)
	avgZoneVolume := s.estimateAverageZoneVolume()
	
	if avgZoneVolume > 0 {
		s.LoadStats.ZoneLoadFactor = myZoneVolume / avgZoneVolume
	} else {
		s.LoadStats.ZoneLoadFactor = 1.0
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
	s.LoadStats.RecentRequestWindow.Record(1)
	
	// Update request rate
	s.LoadStats.RequestRate = s.LoadStats.RecentRequestWindow.GetRate()
	
	// Record key access
	if _, exists := s.LoadStats.KeyStats[key]; !exists {
		s.LoadStats.KeyStats[key] = NewKeyStats()
	}
	s.LoadStats.KeyStats[key].Record(isWrite)
	
	// Update hot key threshold dynamically (5% of overall request rate, minimum 1.0)
	s.LoadStats.HotKeyThreshold = math.Max(s.LoadStats.RequestRate * 0.05, 1.0)
	
	// Check load trend
	previousRate := s.LoadStats.RequestRate
	currentRate := s.LoadStats.RecentRequestWindow.GetRate()
	
	if currentRate > previousRate*1.2 {
		s.LoadStats.LoadTrend = "increasing"
	} else if currentRate < previousRate*0.8 {
		s.LoadStats.LoadTrend = "decreasing"
	} else {
		s.LoadStats.LoadTrend = "stable"
	}

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
	
	// Update zone load factor
	s.UpdateZoneLoadFactor()

	// Find hot keys
	hotKeys := s.identifyHotKeys()
	if len(hotKeys) == 0 && s.LoadStats.ZoneLoadFactor < 2.0 {
		// No hot keys and zone size is reasonable
		return
	}

	// If our zone is too large compared to average, consider adjusting zone
	if s.LoadStats.ZoneLoadFactor > 2.0 {
		log.Printf("Zone is %.1fx larger than average, considering zone adjustment", 
			s.LoadStats.ZoneLoadFactor)
		go s.considerZoneAdjustment(ctx)
	}

	// Perform load balancing actions for hot keys
	s.rebalanceLoad(ctx, hotKeys)
}

// identifyHotKeys identifies hot keys based on access patterns
func (s *CANServer) identifyHotKeys() []string {
	s.LoadStats.mu.RLock()
	defer s.LoadStats.mu.RUnlock()

	hotKeys := make([]string, 0)
	threshold := s.LoadStats.HotKeyThreshold
	
	for key, stats := range s.LoadStats.KeyStats {
		if stats.IsHot(threshold) {
			hotKeys = append(hotKeys, key)
		}
	}

	return hotKeys
}

// considerZoneAdjustment determines if we should adjust our zone boundaries
func (s *CANServer) considerZoneAdjustment(ctx context.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Find neighbors with less load
	var bestNeighbor node.NodeID
	bestLoad := math.MaxFloat64
	
	for id := range s.Node.Neighbors {
		load := s.getNodeLoadScore(id)
		if load < bestLoad && load < s.getNodeLoadScore(s.Node.ID) {
			bestLoad = load
			bestNeighbor = id
		}
	}
	
	if bestNeighbor != "" {
		log.Printf("Considering zone adjustment with neighbor %s", bestNeighbor)
		// This would call AdjustZone when implemented
	}
}

// rebalanceLoad rebalances load by adjusting zones or replicating hot keys
func (s *CANServer) rebalanceLoad(ctx context.Context, hotKeys []string) {
	log.Printf("Performing load balancing for %d hot keys", len(hotKeys))

	// First ensure hot keys are replicated for load distribution
	s.replicateHotKeys(ctx, hotKeys)
	
	// Next, check if any keys need special routing treatment
	s.updateRoutingForHotKeys(hotKeys)
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

// updateRoutingForHotKeys updates routing information for hot keys
func (s *CANServer) updateRoutingForHotKeys(hotKeys []string) {
	// This will be used when we implement probabilistic routing
	// For now, we just log the keys
	if len(hotKeys) > 0 {
		log.Printf("Identified %d hot keys that should use probabilistic routing", len(hotKeys))
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
