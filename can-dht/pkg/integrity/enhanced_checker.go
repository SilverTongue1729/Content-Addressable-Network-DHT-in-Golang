package integrity

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/storage"
)

// EnhancedChecker is an extended integrity checker with additional features
type EnhancedChecker struct {
	// Base is the standard periodic checker
	*PeriodicChecker
	
	// IntegrityKey is the key used for HMAC calculation
	IntegrityKey []byte
	
	// Options configures the enhanced integrity checking
	Options IntegrityOptions
	
	// CorruptionRegistry keeps track of corrupted keys
	CorruptionRegistry *CorruptionRegistry
	
	// ReplicaManager manages data replica for recovery
	ReplicaManager *ReplicaManager
	
	// OnConsistencyFailure is called when replica consistency fails
	OnConsistencyFailure func(key string, results []ConsistencyResult)
	
	// OnIntegrityRepaired is called when data integrity is repaired
	OnIntegrityRepaired func(key string, source string)
	
	// LastKeyRotation is when the integrity key was last rotated
	LastKeyRotation time.Time
	
	// batchCheckCh is used to trigger batch integrity checks
	batchCheckCh chan string
	
	mu sync.RWMutex
}

// ReplicaManager handles replica management for integrity verification
type ReplicaManager struct {
	// Replicas maps node IDs to their stores
	Replicas map[string]*storage.Store
	
	// ReplicaStats tracks statistics about replicas
	ReplicaStats map[string]*ReplicaStats
	
	mu sync.RWMutex
}

// ReplicaStats tracks statistics about a replica
type ReplicaStats struct {
	// SuccessfulChecks is the number of successful integrity checks
	SuccessfulChecks int
	
	// FailedChecks is the number of failed integrity checks
	FailedChecks int
	
	// LastCheck is when the replica was last checked
	LastCheck time.Time
	
	// ReliabilityScore is a 0-100 score of replica reliability
	ReliabilityScore int
}

// CorruptionRegistry keeps track of corrupted data
type CorruptionRegistry struct {
	// CorruptedKeys maps keys to their corruption details
	CorruptedKeys map[string]*CorruptionDetails
	
	// History tracks the history of corruptions
	History []*CorruptionEvent
	
	mu sync.RWMutex
}

// CorruptionDetails contains details about a corruption
type CorruptionDetails struct {
	// Key is the corrupted key
	Key string
	
	// FirstDetected is when the corruption was first detected
	FirstDetected time.Time
	
	// LastChecked is when the corruption was last checked
	LastChecked time.Time
	
	// RepairAttempts is the number of repair attempts
	RepairAttempts int
	
	// Repaired indicates if the corruption has been repaired
	Repaired bool
	
	// RepairedAt is when the corruption was repaired
	RepairedAt time.Time
	
	// Severity is the corruption severity
	Severity CorruptionSeverity
}

// CorruptionEvent represents a corruption event
type CorruptionEvent struct {
	// Key is the corrupted key
	Key string
	
	// Timestamp is when the event occurred
	Timestamp time.Time
	
	// EventType is the type of corruption event
	EventType string
	
	// Details contains additional details about the event
	Details string
}

// NewEnhancedChecker creates a new enhanced integrity checker
func NewEnhancedChecker(store *storage.Store, keyManager *crypto.KeyManager, options IntegrityOptions) *EnhancedChecker {
	// Create base checker
	baseChecker := NewPeriodicChecker(store, keyManager, options.CheckInterval)
	
	// Generate initial integrity key
	integrityKey := make([]byte, 32)
	if _, err := rand.Read(integrityKey); err != nil {
		log.Printf("Failed to generate integrity key: %v", err)
	}
	
	// Create enhanced checker
	checker := &EnhancedChecker{
		PeriodicChecker: baseChecker,
		IntegrityKey:    integrityKey,
		Options:         options,
		LastKeyRotation: time.Now(),
		CorruptionRegistry: &CorruptionRegistry{
			CorruptedKeys: make(map[string]*CorruptionDetails),
			History:       make([]*CorruptionEvent, 0),
		},
		ReplicaManager: &ReplicaManager{
			Replicas:     make(map[string]*storage.Store),
			ReplicaStats: make(map[string]*ReplicaStats),
		},
		batchCheckCh: make(chan string, 100),
	}
	
	// Override the corruption handler
	baseChecker.OnCorruptionFound = func(key string, result *CheckResult) {
		checker.handleCorruption(key, result)
	}
	
	// Set default consistency failure handler
	checker.OnConsistencyFailure = func(key string, results []ConsistencyResult) {
		log.Printf("Consistency failure for key %s across %d replicas", key, len(results))
	}
	
	// Set default repair handler
	checker.OnIntegrityRepaired = func(key string, source string) {
		log.Printf("Integrity repaired for key %s from %s", key, source)
	}
	
	// Start batch processor
	go checker.processBatchChecks()
	
	return checker
}

// AddReplica adds a replica to the enhanced checker
func (e *EnhancedChecker) AddReplica(nodeID string, store *storage.Store) {
	e.ReplicaManager.mu.Lock()
	defer e.ReplicaManager.mu.Unlock()
	
	e.ReplicaManager.Replicas[nodeID] = store
	
	// Initialize stats
	e.ReplicaManager.ReplicaStats[nodeID] = &ReplicaStats{
		LastCheck:        time.Now(),
		ReliabilityScore: 100, // Start with perfect score
	}
	
	// Add to base checker as well
	e.PeriodicChecker.AddReplica(store)
}

// RotateIntegrityKey rotates the integrity key
func (e *EnhancedChecker) RotateIntegrityKey() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// Generate new key
	newKey := make([]byte, 32)
	if _, err := rand.Read(newKey); err != nil {
		return fmt.Errorf("failed to generate new integrity key: %w", err)
	}
	
	// Update key and rotation time
	e.IntegrityKey = newKey
	e.LastKeyRotation = time.Now()
	
	log.Printf("Rotated integrity key")
	return nil
}

// Start starts the enhanced checker
func (e *EnhancedChecker) Start() {
	e.PeriodicChecker.Start()
	
	// Start key rotation timer if needed
	if e.Options.KeyRotationInterval > 0 {
		go e.runKeyRotation()
	}
}

// runKeyRotation handles periodic key rotation
func (e *EnhancedChecker) runKeyRotation() {
	ticker := time.NewTicker(1 * time.Hour) // Check hourly
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			e.mu.RLock()
			interval := e.Options.KeyRotationInterval
			lastRotation := e.LastKeyRotation
			e.mu.RUnlock()
			
			if time.Since(lastRotation) >= interval {
				if err := e.RotateIntegrityKey(); err != nil {
					log.Printf("Failed to rotate integrity key: %v", err)
				}
			}
		case <-e.PeriodicChecker.stopChan:
			return
		}
	}
}

// handleCorruption processes a detected corruption
func (e *EnhancedChecker) handleCorruption(key string, result *CheckResult) {
	e.CorruptionRegistry.mu.Lock()
	defer e.CorruptionRegistry.mu.Unlock()
	
	// Log the corruption event
	event := &CorruptionEvent{
		Key:       key,
		Timestamp: time.Now(),
		EventType: "detection",
		Details:   fmt.Sprintf("Status: %v, Error: %v", result.Status, result.Error),
	}
	e.CorruptionRegistry.History = append(e.CorruptionRegistry.History, event)
	
	// Check if already registered
	details, exists := e.CorruptionRegistry.CorruptedKeys[key]
	if !exists {
		// Register new corruption
		details = &CorruptionDetails{
			Key:           key,
			FirstDetected: time.Now(),
			LastChecked:   time.Now(),
			Severity:      determineSeverity(key, e.Options.CriticalKeys),
		}
		e.CorruptionRegistry.CorruptedKeys[key] = details
	} else {
		// Update existing corruption
		details.LastChecked = time.Now()
		details.RepairAttempts++
	}
	
	// Attempt repair based on severity
	go func() {
		if details.Severity >= SeverityHigh {
			// High severity needs immediate repair
			e.repairCorruption(key, result)
		} else {
			// Lower severity can go through batch repair
			e.batchCheckCh <- key
		}
		
		// Notify on corruption
		if e.PeriodicChecker.OnCorruptionFound != nil {
			e.PeriodicChecker.OnCorruptionFound(key, result)
		}
	}()
}

// processBatchChecks handles batch processing of integrity checks
func (e *EnhancedChecker) processBatchChecks() {
	batchTicker := time.NewTicker(1 * time.Minute)
	defer batchTicker.Stop()
	
	batch := make(map[string]struct{})
	
	for {
		select {
		case key := <-e.batchCheckCh:
			batch[key] = struct{}{}
			
			// Process batch when it reaches the size limit
			if len(batch) >= e.Options.BatchSize {
				e.processBatch(batch)
				batch = make(map[string]struct{})
			}
		case <-batchTicker.C:
			// Process batch periodically
			if len(batch) > 0 {
				e.processBatch(batch)
				batch = make(map[string]struct{})
			}
		case <-e.PeriodicChecker.stopChan:
			return
		}
	}
}

// processBatch processes a batch of integrity checks
func (e *EnhancedChecker) processBatch(batch map[string]struct{}) {
	for key := range batch {
		result := e.PeriodicChecker.CheckDataIntegrity(key)
		if result.Status == StatusCorrupted || result.Status == StatusVerificationFailed {
			e.repairCorruption(key, result)
		}
	}
}

// repairCorruption attempts to repair corrupted data
func (e *EnhancedChecker) repairCorruption(key string, result *CheckResult) {
	// Try to repair from replicas first
	repaired := e.PeriodicChecker.tryRepairFromReplicas(key)
	
	if repaired {
		// Update corruption registry
		e.CorruptionRegistry.mu.Lock()
		if details, exists := e.CorruptionRegistry.CorruptedKeys[key]; exists {
			details.Repaired = true
			details.RepairedAt = time.Now()
		}
		
		// Log the repair event
		event := &CorruptionEvent{
			Key:       key,
			Timestamp: time.Now(),
			EventType: "repair",
			Details:   "Repaired from replica",
		}
		e.CorruptionRegistry.History = append(e.CorruptionRegistry.History, event)
		e.CorruptionRegistry.mu.Unlock()
		
		// Notify repair
		if e.OnIntegrityRepaired != nil {
			e.OnIntegrityRepaired(key, "replica")
		}
		
		return
	}
	
	// If replica repair failed, try consistency check
	results, err := e.runConsistencyCheck(key)
	if err != nil {
		log.Printf("Consistency check failed for key %s: %v", key, err)
		return
	}
	
	// Try to resolve inconsistency
	if len(results) > 0 {
		err = e.resolveInconsistency(key, results)
		if err != nil {
			log.Printf("Failed to resolve inconsistency for key %s: %v", key, err)
		}
	}
}

// runConsistencyCheck verifies data across replicas
func (e *EnhancedChecker) runConsistencyCheck(key string) ([]ConsistencyResult, error) {
	e.ReplicaManager.mu.RLock()
	defer e.ReplicaManager.mu.RUnlock()
	
	results := make([]ConsistencyResult, 0)
	
	// Check original data
	origData, exists, err := e.Store.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve original data: %w", err)
	}
	
	if !exists {
		return nil, fmt.Errorf("key does not exist in primary store")
	}
	
	// Parse integrity data from original
	origDataBytes, origIntegrityData, err := DeserializeDataWithIntegrity(origData)
	if err != nil {
		// Data might be in old format
		origDataBytes = origData
		origIntegrityData = IntegrityData{
			Hash:      "", // Will be recomputed
			Timestamp: 0,
			Version:   0,
		}
	}
	
	// If hash is empty, compute it
	if origIntegrityData.Hash == "" {
		hasher := sha256.New()
		hasher.Write(origDataBytes)
		origIntegrityData.Hash = hex.EncodeToString(hasher.Sum(nil))
	}
	
	// Check each replica
	for nodeID, replica := range e.ReplicaManager.Replicas {
		result := ConsistencyResult{
			NodeID:    nodeID,
			Timestamp: time.Now(),
			IsValid:   false,
		}
		
		// Get replica data
		replicaData, exists, err := replica.Get(key)
		if err != nil || !exists {
			result.Error = fmt.Errorf("failed to retrieve from replica: %v", err)
			results = append(results, result)
			continue
		}
		
		// Parse integrity data from replica
		replicaDataBytes, replicaIntegrityData, err := DeserializeDataWithIntegrity(replicaData)
		if err != nil {
			// Data might be in old format
			replicaDataBytes = replicaData
			replicaIntegrityData = IntegrityData{
				Hash:      "", // Will be recomputed
				Timestamp: 0,
				Version:   0,
			}
		}
		
		// If hash is empty, compute it
		if replicaIntegrityData.Hash == "" {
			hasher := sha256.New()
			hasher.Write(replicaDataBytes)
			replicaIntegrityData.Hash = hex.EncodeToString(hasher.Sum(nil))
		}
		
		result.IntegrityData = replicaIntegrityData
		
		// Check if integrity data matches
		if replicaIntegrityData.Hash == origIntegrityData.Hash {
			result.IsValid = true
			
			// Update replica stats
			if stats, exists := e.ReplicaManager.ReplicaStats[nodeID]; exists {
				stats.SuccessfulChecks++
				stats.LastCheck = time.Now()
				stats.ReliabilityScore = calculateReliabilityScore(stats)
			}
		} else {
			result.Error = fmt.Errorf("hash mismatch")
			
			// Update replica stats
			if stats, exists := e.ReplicaManager.ReplicaStats[nodeID]; exists {
				stats.FailedChecks++
				stats.LastCheck = time.Now()
				stats.ReliabilityScore = calculateReliabilityScore(stats)
			}
		}
		
		results = append(results, result)
	}
	
	return results, nil
}

// resolveInconsistency handles inconsistent replicas
func (e *EnhancedChecker) resolveInconsistency(key string, results []ConsistencyResult) error {
	if len(results) == 0 {
		return fmt.Errorf("no results to resolve")
	}
	
	// Track vote counts for different hashes
	hashVotes := make(map[string]int)
	hashData := make(map[string]string) // Maps hash to node ID
	
	// Count votes for each hash
	for _, result := range results {
		if !result.IsValid {
			continue
		}
		
		hash := result.IntegrityData.Hash
		hashVotes[hash]++
		hashData[hash] = result.NodeID
	}
	
	// Find the hash with the most votes
	var majorityHash string
	var maxVotes int
	for hash, votes := range hashVotes {
		if votes > maxVotes {
			majorityHash = hash
			maxVotes = votes
		}
	}
	
	// If no clear majority, use highest reliability replica
	if maxVotes <= len(results)/2 {
		nodeID := findMostReliableReplica(e.ReplicaManager.ReplicaStats, results)
		if nodeID == "" {
			return fmt.Errorf("no reliable replica found")
		}
		
		// Find the hash for this node
		for _, result := range results {
			if result.NodeID == nodeID {
				majorityHash = result.IntegrityData.Hash
				break
			}
		}
	}
	
	if majorityHash == "" {
		return fmt.Errorf("couldn't determine correct data")
	}
	
	// Find a replica with the majority hash
	sourceNodeID := hashData[majorityHash]
	if sourceNodeID == "" {
		return fmt.Errorf("couldn't find replica with majority hash")
	}
	
	e.ReplicaManager.mu.RLock()
	sourceReplica := e.ReplicaManager.Replicas[sourceNodeID]
	e.ReplicaManager.mu.RUnlock()
	
	if sourceReplica == nil {
		return fmt.Errorf("source replica not found")
	}
	
	// Get data from source replica
	data, exists, err := sourceReplica.Get(key)
	if err != nil || !exists {
		return fmt.Errorf("failed to get data from source replica: %v", err)
	}
	
	// Update primary with correct data
	err = e.Store.Put(key, data)
	if err != nil {
		return fmt.Errorf("failed to update primary data: %v", err)
	}
	
	// Update corruption registry
	e.CorruptionRegistry.mu.Lock()
	if details, exists := e.CorruptionRegistry.CorruptedKeys[key]; exists {
		details.Repaired = true
		details.RepairedAt = time.Now()
	}
	
	// Log the repair event
	event := &CorruptionEvent{
		Key:       key,
		Timestamp: time.Now(),
		EventType: "repair",
		Details:   fmt.Sprintf("Repaired from replica %s", sourceNodeID),
	}
	e.CorruptionRegistry.History = append(e.CorruptionRegistry.History, event)
	e.CorruptionRegistry.mu.Unlock()
	
	// Notify repair
	if e.OnIntegrityRepaired != nil {
		e.OnIntegrityRepaired(key, fmt.Sprintf("replica %s", sourceNodeID))
	}
	
	return nil
}

// GetCorruptionHistory returns the corruption event history
func (e *EnhancedChecker) GetCorruptionHistory() []*CorruptionEvent {
	e.CorruptionRegistry.mu.RLock()
	defer e.CorruptionRegistry.mu.RUnlock()
	
	// Make a copy to avoid race conditions
	history := make([]*CorruptionEvent, len(e.CorruptionRegistry.History))
	copy(history, e.CorruptionRegistry.History)
	
	return history
}

// GetReplicaStats returns statistics for all replicas
func (e *EnhancedChecker) GetReplicaStats() map[string]*ReplicaStats {
	e.ReplicaManager.mu.RLock()
	defer e.ReplicaManager.mu.RUnlock()
	
	// Make a copy to avoid race conditions
	stats := make(map[string]*ReplicaStats)
	for nodeID, stat := range e.ReplicaManager.ReplicaStats {
		statsCopy := *stat // Make a copy
		stats[nodeID] = &statsCopy
	}
	
	return stats
}

// RunIntegrityCheck performs an integrity check on a specific key
func (e *EnhancedChecker) RunIntegrityCheck(ctx context.Context, key string) (*CheckResult, error) {
	// Run the check
	result := e.PeriodicChecker.CheckDataIntegrity(key)
	
	// If corrupted, try to repair
	if result.Status == StatusCorrupted || result.Status == StatusVerificationFailed {
		e.repairCorruption(key, result)
	}
	
	return result, nil
}

// VerifyAllKeys verifies all keys in the store with progress reporting
func (e *EnhancedChecker) VerifyAllKeys(ctx context.Context, progressCh chan<- float64) (*IntegrityStats, error) {
	// Get all keys
	keys, err := e.Store.GetAllKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to get all keys: %v", err)
	}
	
	totalKeys := len(keys)
	if totalKeys == 0 {
		return &e.Stats, nil
	}
	
	// Reset stats
	e.Stats.ResetStats()
	e.Stats.LastCheckTime = time.Now()
	
	// Process keys
	for i, key := range keys {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return &e.Stats, ctx.Err()
		default:
			// Continue processing
		}
		
		// Run integrity check
		result := e.PeriodicChecker.CheckDataIntegrity(key)
		
		// Update stats
		e.mu.Lock()
		e.Stats.TotalChecks++
		if result.Status == StatusCorrupted || result.Status == StatusVerificationFailed {
			e.Stats.CorruptedData++
			if result.RepairedOK {
				e.Stats.RepairedData++
			} else {
				e.Stats.UnrepairedData++
			}
			
			// Try to repair in the background
			go e.repairCorruption(key, result)
		}
		e.mu.Unlock()
		
		// Report progress
		if progressCh != nil {
			progress := float64(i+1) / float64(totalKeys)
			select {
			case progressCh <- progress:
				// Progress reported
			default:
				// Channel full, skip this update
			}
		}
	}
	
	// Mark as completed
	e.mu.Lock()
	e.Stats.ChecksCompleted = true
	e.mu.Unlock()
	
	// Call completion callback
	if e.OnCheckCompleted != nil {
		stats := e.Stats // Make a copy
		e.OnCheckCompleted(&stats)
	}
	
	return &e.Stats, nil
}

// Helper functions

// determineSeverity determines the severity of corruption for a key
func determineSeverity(key string, criticalKeys []string) CorruptionSeverity {
	// Check if key is in critical keys list
	for _, criticalKey := range criticalKeys {
		if key == criticalKey {
			return SeverityCritical
		}
	}
	
	// Default to medium severity
	return SeverityMedium
}

// calculateReliabilityScore calculates a reliability score for a replica
func calculateReliabilityScore(stats *ReplicaStats) int {
	total := stats.SuccessfulChecks + stats.FailedChecks
	if total == 0 {
		return 100 // No data yet
	}
	
	// Calculate percentage of successful checks
	score := (stats.SuccessfulChecks * 100) / total
	
	// Ensure score is in range 0-100
	if score < 0 {
		score = 0
	} else if score > 100 {
		score = 100
	}
	
	return score
}

// findMostReliableReplica finds the most reliable replica from results
func findMostReliableReplica(stats map[string]*ReplicaStats, results []ConsistencyResult) string {
	var bestNodeID string
	var bestScore int
	
	for _, result := range results {
		if !result.IsValid {
			continue
		}
		
		if stats, exists := stats[result.NodeID]; exists {
			if stats.ReliabilityScore > bestScore {
				bestScore = stats.ReliabilityScore
				bestNodeID = result.NodeID
			}
		}
	}
	
	return bestNodeID
} 