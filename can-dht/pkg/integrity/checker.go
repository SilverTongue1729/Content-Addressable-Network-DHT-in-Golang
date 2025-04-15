package integrity

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/storage"
)

// CheckStatus represents the result of an integrity check
type CheckStatus int

const (
	StatusOK CheckStatus = iota
	StatusCorrupted
	StatusMissing
	StatusVerificationFailed
)

// CheckResult stores the result of an integrity check for a specific key
type CheckResult struct {
	Key        string
	Status     CheckStatus
	Error      error
	RepairedOK bool
	CheckTime  time.Time
}

// IntegrityStats tracks statistics about integrity checks
type IntegrityStats struct {
	TotalChecks     int
	CorruptedData   int
	RepairedData    int
	UnrepairedData  int
	LastCheckTime   time.Time
	ChecksCompleted bool
	mu              sync.RWMutex
}

// ResetStats resets the integrity check statistics
func (s *IntegrityStats) ResetStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalChecks = 0
	s.CorruptedData = 0
	s.RepairedData = 0
	s.UnrepairedData = 0
	s.ChecksCompleted = false
}

// PeriodicChecker is responsible for running periodic integrity checks
type PeriodicChecker struct {
	// Store is the data store to check
	Store *storage.Store

	// KeyManager is used for integrity verification
	KeyManager *crypto.KeyManager

	// Replicas are used to repair corrupted data
	Replicas []*storage.Store

	// CheckInterval is the time between integrity checks
	CheckInterval time.Duration

	// OnCorruptionFound is called when corrupted data is found
	OnCorruptionFound func(key string, result *CheckResult)

	// OnCheckCompleted is called when a full check cycle completes
	OnCheckCompleted func(stats *IntegrityStats)

	// Stats collects statistics about the integrity checks
	Stats IntegrityStats

	// Running indicates if the checker is currently running
	Running bool

	// stopChan is used to signal the checker to stop
	stopChan chan struct{}

	// lastResults caches the results of the most recent checks
	lastResults map[string]*CheckResult

	mu sync.RWMutex
}

// NewPeriodicChecker creates a new periodic integrity checker
func NewPeriodicChecker(store *storage.Store, keyManager *crypto.KeyManager, interval time.Duration) *PeriodicChecker {
	return &PeriodicChecker{
		Store:         store,
		KeyManager:    keyManager,
		CheckInterval: interval,
		lastResults:   make(map[string]*CheckResult),
		stopChan:      make(chan struct{}),
		OnCorruptionFound: func(key string, result *CheckResult) {
			log.Printf("Corruption found for key %s: %v", key, result.Error)
		},
		OnCheckCompleted: func(stats *IntegrityStats) {
			log.Printf("Integrity check completed: %d total, %d corrupted, %d repaired, %d unrepaired",
				stats.TotalChecks, stats.CorruptedData, stats.RepairedData, stats.UnrepairedData)
		},
	}
}

// AddReplica adds a replica store that can be used to repair corrupted data
func (p *PeriodicChecker) AddReplica(replica *storage.Store) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Replicas = append(p.Replicas, replica)
}

// Start begins periodic integrity checking
func (p *PeriodicChecker) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Running {
		return // Already running
	}

	p.Running = true
	go p.runChecks()
}

// Stop halts periodic integrity checking
func (p *PeriodicChecker) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.Running {
		return // Not running
	}

	close(p.stopChan)
	p.Running = false

	// Create a new stop channel for future use
	p.stopChan = make(chan struct{})
}

// runChecks is the main loop that periodically runs integrity checks
func (p *PeriodicChecker) runChecks() {
	ticker := time.NewTicker(p.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.CheckAllData()
		case <-p.stopChan:
			return
		}
	}
}

// CheckAllData performs an integrity check on all data in the store
func (p *PeriodicChecker) CheckAllData() {
	p.mu.Lock()
	p.Stats.ResetStats()
	p.Stats.LastCheckTime = time.Now()
	p.Stats.ChecksCompleted = false
	p.mu.Unlock()

	// Get all keys from the store
	keys, err := p.Store.GetAllKeys()
	if err != nil {
		log.Printf("Failed to get keys for integrity check: %v", err)
		return
	}

	// Process each key
	for _, key := range keys {
		result := p.CheckDataIntegrity(key)

		p.mu.Lock()
		// Update stats
		p.Stats.TotalChecks++

		// Handle corruption
		if result.Status == StatusCorrupted || result.Status == StatusVerificationFailed {
			p.Stats.CorruptedData++

			// Try to repair the data
			if result.RepairedOK {
				p.Stats.RepairedData++
			} else {
				p.Stats.UnrepairedData++
			}

			// Call the corruption callback
			if p.OnCorruptionFound != nil {
				go p.OnCorruptionFound(key, result)
			}
		}

		// Cache the result
		p.lastResults[key] = result
		p.mu.Unlock()
	}

	// Mark check as completed
	p.mu.Lock()
	p.Stats.ChecksCompleted = true

	// Call completion callback
	if p.OnCheckCompleted != nil {
		stats := p.Stats // Make a copy
		go p.OnCheckCompleted(&stats)
	}
	p.mu.Unlock()
}

// CheckDataIntegrity checks the integrity of a specific key
func (p *PeriodicChecker) CheckDataIntegrity(key string) *CheckResult {
	result := &CheckResult{
		Key:       key,
		CheckTime: time.Now(),
	}

	// Get the data from the store
	data, exists, err := p.Store.Get(key)
	if err != nil {
		result.Status = StatusVerificationFailed
		result.Error = fmt.Errorf("failed to retrieve data: %w", err)
		return result
	}

	if !exists {
		result.Status = StatusMissing
		result.Error = fmt.Errorf("data not found")
		return result
	}

	// For unencrypted data, we can't verify integrity
	if p.KeyManager == nil {
		result.Status = StatusOK
		return result
	}

	// Parse the serialized secure data
	secureData, err := crypto.DeserializeSecureData(string(data))
	if err != nil {
		result.Status = StatusCorrupted
		result.Error = fmt.Errorf("failed to deserialize secure data: %w", err)

		// Try to repair from replicas
		if repaired := p.tryRepairFromReplicas(key); repaired {
			result.RepairedOK = true
		}

		return result
	}

	// Verify integrity with HMAC
	_, err = p.KeyManager.DecryptAndVerify(secureData)
	if err != nil {
		result.Status = StatusCorrupted
		result.Error = fmt.Errorf("integrity verification failed: %w", err)

		// Try to repair from replicas
		if repaired := p.tryRepairFromReplicas(key); repaired {
			result.RepairedOK = true
		}

		return result
	}

	// Data is OK
	result.Status = StatusOK
	return result
}

// tryRepairFromReplicas attempts to repair corrupted data from replicas
func (p *PeriodicChecker) tryRepairFromReplicas(key string) bool {
	if len(p.Replicas) == 0 {
		return false
	}

	for _, replica := range p.Replicas {
		// Get data from the replica
		data, exists, err := replica.Get(key)
		if err != nil || !exists {
			continue
		}

		// Verify the replica data integrity
		if p.KeyManager != nil {
			secureData, err := crypto.DeserializeSecureData(string(data))
			if err != nil {
				continue
			}

			_, err = p.KeyManager.DecryptAndVerify(secureData)
			if err != nil {
				continue
			}
		}

		// If we get here, the replica data is good, so repair the primary
		err = p.Store.Put(key, data)
		if err != nil {
			log.Printf("Failed to repair data for key %s: %v", key, err)
			continue
		}

		log.Printf("Successfully repaired data for key %s from replica", key)
		return true
	}

	return false
}

// GetLastCheckResult returns the most recent integrity check result for a key
func (p *PeriodicChecker) GetLastCheckResult(key string) (*CheckResult, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result, exists := p.lastResults[key]
	return result, exists
}

// GetStats returns a copy of the current integrity statistics
func (p *PeriodicChecker) GetStats() IntegrityStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.Stats
}

// RunManualCheck immediately performs an integrity check on all data
func (p *PeriodicChecker) RunManualCheck() {
	go p.CheckAllData()
}
