package integrity

import (
	"context"
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
	StatusReplicated       // Key exists on replica but not primary
	StatusConflict         // Conflict between replicas detected
	StatusRepairedFromSelf // Repaired from backup in same node
	StatusRepairedFromPeer // Repaired from a peer's replica
)

// CheckResult stores the result of an integrity check for a specific key
type CheckResult struct {
	Key           string
	Status        CheckStatus
	Error         error
	RepairedOK    bool
	CheckTime     time.Time
	RepairSource  string // Which replica was used for repair
	VersionBefore int    // Version before repair
	VersionAfter  int    // Version after repair
	HashBefore    string // Hash before repair
	HashAfter     string // Hash after repair
}

// IntegrityStats tracks statistics about integrity checks
type IntegrityStats struct {
	TotalChecks           int
	CorruptedData         int
	RepairedData          int
	UnrepairedData        int
	Conflicts             int
	ConflictsResolved     int
	LastCheckTime         time.Time
	ChecksCompleted       bool
	ConsistencyPercentage float64 // Percentage of consistent data
	mu                    sync.RWMutex
}

// ResetStats resets the integrity check statistics
func (s *IntegrityStats) ResetStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalChecks = 0
	s.CorruptedData = 0
	s.RepairedData = 0
	s.UnrepairedData = 0
	s.Conflicts = 0
	s.ConflictsResolved = 0
	s.ChecksCompleted = false
	s.ConsistencyPercentage = 100.0
}

// DataConflict represents a conflict between different replicas of the same data
type DataConflict struct {
	Key           string
	LocalData     []byte
	ReplicaData   []map[string][]byte // Map of nodeID -> data from different replicas
	Timestamps    []map[string]time.Time
	ResolutionStrategy ConflictResolutionStrategy
	Resolved      bool
	ChosenData    []byte
}

// ConflictResolutionStrategy defines how conflicts are resolved
type ConflictResolutionStrategy int

const (
	// PreferLocal prefers the local copy
	PreferLocal ConflictResolutionStrategy = iota
	
	// PreferMajority chooses the value that appears most frequently
	PreferMajority
	
	// PreferNewest chooses the value with the most recent timestamp
	PreferNewest
	
	// MergeValues attempts to merge values (if applicable)
	MergeValues
)

// PeriodicChecker is responsible for running periodic integrity checks
type PeriodicChecker struct {
	// Store is the data store to check
	Store *storage.Store

	// KeyManager is used for integrity verification
	KeyManager *crypto.KeyManager

	// Replicas are used to repair corrupted data
	Replicas []*storage.Store
	
	// RepairClient is used to fetch data from other nodes
	RepairClient RepairClient

	// CheckInterval is the time between integrity checks
	CheckInterval time.Duration
	
	// RepairInterval is the time between automatic repair attempts
	RepairInterval time.Duration
	
	// BackupInterval is the time between local backups
	BackupInterval time.Duration

	// OnCorruptionFound is called when corrupted data is found
	OnCorruptionFound func(key string, result *CheckResult)

	// OnCheckCompleted is called when a full check cycle completes
	OnCheckCompleted func(stats *IntegrityStats)
	
	// OnConflictDetected is called when a data conflict is detected
	OnConflictDetected func(conflict *DataConflict)
	
	// ConflictResolutionStrategy is the default strategy for resolving conflicts
	ConflictResolutionStrategy ConflictResolutionStrategy
	
	// ReplicaNodes contains the IDs of known replica nodes
	ReplicaNodes []string

	// Stats collects statistics about the integrity checks
	Stats IntegrityStats

	// Running indicates if the checker is currently running
	Running bool

	// stopChan is used to signal the checker to stop
	stopChan chan struct{}

	// lastResults caches the results of the most recent checks
	lastResults map[string]*CheckResult
	
	// backupStore is used to maintain local backup copies
	backupStore *storage.Store
	
	// pendingRepairs tracks keys waiting for repair
	pendingRepairs sync.Map

	mu sync.RWMutex
}

// RepairClient defines the interface for fetching data from other nodes
type RepairClient interface {
	// FetchDataFromPeer gets data from a peer node
	FetchDataFromPeer(ctx context.Context, nodeID string, key string) ([]byte, error)
	
	// ListPeersWithData lists peers that might have a copy of the data
	ListPeersWithData(ctx context.Context, key string) ([]string, error)
}

// NewPeriodicChecker creates a new periodic integrity checker
func NewPeriodicChecker(store *storage.Store, keyManager *crypto.KeyManager, interval time.Duration) *PeriodicChecker {
	// Create a backup store in the same directory as the main store
	backupPath := store.Path + "_backup"
	backupStore, err := storage.NewStore(backupPath)
	if err != nil {
		log.Printf("Warning: Failed to create backup store at %s: %v", backupPath, err)
	}

	return &PeriodicChecker{
		Store:                    store,
		KeyManager:               keyManager,
		CheckInterval:            interval,
		RepairInterval:           time.Hour,
		BackupInterval:           time.Hour * 6,
		lastResults:              make(map[string]*CheckResult),
		stopChan:                 make(chan struct{}),
		backupStore:              backupStore,
		ConflictResolutionStrategy: PreferMajority,
		OnCorruptionFound: func(key string, result *CheckResult) {
			log.Printf("Corruption found for key %s: %v", key, result.Error)
		},
		OnCheckCompleted: func(stats *IntegrityStats) {
			log.Printf("Integrity check completed: %d total, %d corrupted, %d repaired, %d unrepaired, %.2f%% consistency",
				stats.TotalChecks, stats.CorruptedData, stats.RepairedData, stats.UnrepairedData, stats.ConsistencyPercentage)
		},
		OnConflictDetected: func(conflict *DataConflict) {
			log.Printf("Data conflict detected for key %s", conflict.Key)
		},
	}
}

// SetRepairClient sets the client used for repairing data from peers
func (p *PeriodicChecker) SetRepairClient(client RepairClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.RepairClient = client
}

// AddReplica adds a replica store that can be used to repair corrupted data
func (p *PeriodicChecker) AddReplica(replica *storage.Store) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Replicas = append(p.Replicas, replica)
}

// AddReplicaNode adds a node ID that is known to host replicas
func (p *PeriodicChecker) AddReplicaNode(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Check if node is already in the list
	for _, existing := range p.ReplicaNodes {
		if existing == nodeID {
			return
		}
	}
	
	p.ReplicaNodes = append(p.ReplicaNodes, nodeID)
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
	go p.runBackups()
	go p.runPendingRepairs()
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

// runBackups periodically backs up data locally
func (p *PeriodicChecker) runBackups() {
	if p.backupStore == nil {
		return
	}
	
	ticker := time.NewTicker(p.BackupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.BackupAllData()
		case <-p.stopChan:
			return
		}
	}
}

// runPendingRepairs periodically attempts to repair data that couldn't be fixed
func (p *PeriodicChecker) runPendingRepairs() {
	ticker := time.NewTicker(p.RepairInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.AttemptPendingRepairs(context.Background())
		case <-p.stopChan:
			return
		}
	}
}

// BackupAllData creates a backup of all data in the store
func (p *PeriodicChecker) BackupAllData() error {
	if p.backupStore == nil {
		return fmt.Errorf("backup store not available")
	}
	
	// Get all keys from the store
	keys, err := p.Store.GetAllKeys()
	if err != nil {
		log.Printf("Failed to get keys for backup: %v", err)
		return err
	}
	
	// Backup each key
	for _, key := range keys {
		data, err := p.Store.Get(key)
		if err != nil {
			log.Printf("Failed to get data for key %s during backup: %v", key, err)
			continue
		}
		
		// Store in backup
		if err := p.backupStore.Put(key, data); err != nil {
			log.Printf("Failed to backup data for key %s: %v", key, err)
			continue
		}
	}
	
	return nil
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
				// Add to pending repairs if not repaired
				p.pendingRepairs.Store(key, time.Now())
			}

			// Call the corruption callback
			if p.OnCorruptionFound != nil {
				go p.OnCorruptionFound(key, result)
			}
		} else if result.Status == StatusConflict {
			p.Stats.Conflicts++
			// Conflicts are handled separately by the conflict resolution process
		}

		// Cache the result
		p.lastResults[key] = result
		p.mu.Unlock()
	}

	// Calculate consistency percentage
	p.mu.Lock()
	if p.Stats.TotalChecks > 0 {
		consistentCount := p.Stats.TotalChecks - p.Stats.CorruptedData
		p.Stats.ConsistencyPercentage = float64(consistentCount) / float64(p.Stats.TotalChecks) * 100.0
	} else {
		p.Stats.ConsistencyPercentage = 100.0 // No keys means 100% consistent
	}
	p.Stats.ChecksCompleted = true

	// Notify that check is complete
	if p.OnCheckCompleted != nil {
		stats := p.Stats // Create a copy to avoid race conditions
		go p.OnCheckCompleted(&stats)
	}
	p.mu.Unlock()
}

// AttemptPendingRepairs tries to repair data that previously failed repair
func (p *PeriodicChecker) AttemptPendingRepairs(ctx context.Context) {
	// Collect keys that need repair
	var keysToRepair []string
	
	p.pendingRepairs.Range(func(key, value interface{}) bool {
		keysToRepair = append(keysToRepair, key.(string))
		return true
	})
	
	// Attempt repair for each key
	for _, key := range keysToRepair {
		repaired := false
		
		// Try from peers first
		if p.RepairClient != nil {
			// Get list of peers that might have this data
			peers, err := p.RepairClient.ListPeersWithData(ctx, key)
			if err == nil && len(peers) > 0 {
				// Try each peer
				for _, peerID := range peers {
					data, err := p.RepairClient.FetchDataFromPeer(ctx, peerID, key)
					if err == nil && data != nil {
						// Verify the data before using it
						if p.verifyDataFromPeer(key, data) {
							// Use the peer data to repair
							if err := p.Store.Put(key, data); err == nil {
								log.Printf("Successfully repaired key %s from peer %s", key, peerID)
								repaired = true
								break
							}
						}
					}
				}
			}
		}
		
		// If not repaired from peers, try local backup
		if !repaired && p.backupStore != nil {
			data, err := p.backupStore.Get(key)
			if err == nil && data != nil {
				if err := p.Store.Put(key, data); err == nil {
					log.Printf("Successfully repaired key %s from local backup", key)
					repaired = true
				}
			}
		}
		
		// If repaired, remove from pending repairs
		if repaired {
			p.pendingRepairs.Delete(key)
			
			// Update stats
			p.mu.Lock()
			p.Stats.RepairedData++
			p.Stats.UnrepairedData--
			p.mu.Unlock()
		}
	}
}

// verifyDataFromPeer verifies that data from a peer is valid before using it
func (p *PeriodicChecker) verifyDataFromPeer(key string, data []byte) bool {
	// Check if data appears to be in the secure data format
	if len(data) < 32 { // Minimum size for a meaningful secure packet
		return false
	}
	
	// Try to deserialize the data if it's in SecureData format
	secureData, err := crypto.DeserializeSecureData(string(data))
	if err == nil {
		// Verify HMAC
		_, err = p.KeyManager.DecryptAndVerify(secureData)
		return err == nil
	}
	
	// If not secure data, we should implement some basic validation
	// For now, we'll just accept non-secure data
	return true
}

// CheckDataIntegrity checks the integrity of a specific key
func (p *PeriodicChecker) CheckDataIntegrity(key string) *CheckResult {
	result := &CheckResult{
		Key:       key,
		CheckTime: time.Now(),
	}

	// Get the data from the store
	data, err := p.Store.Get(key)
	if err != nil {
		result.Status = StatusMissing
		result.Error = fmt.Errorf("failed to get data: %w", err)
		return result
	}

	// Check if the data is in the secure data format
	secureData, err := crypto.DeserializeSecureData(string(data))
	if err != nil {
		// The data is not in the expected format
		result.Status = StatusCorrupted
		result.Error = fmt.Errorf("failed to deserialize secure data: %w", err)
		
		// Try to repair
		result.RepairedOK = p.tryRepairCorruptedData(key, data, secureData, result)
		return result
	}

	// Verify the HMAC
	_, err = p.KeyManager.DecryptAndVerify(secureData)
	if err != nil {
		result.Status = StatusVerificationFailed
		result.Error = fmt.Errorf("failed to verify data integrity: %w", err)
		
		// Try to repair
		result.RepairedOK = p.tryRepairCorruptedData(key, data, secureData, result)
		return result
	}

	// Everything looks good
	result.Status = StatusOK
	return result
}

// tryRepairCorruptedData attempts to repair corrupted data
func (p *PeriodicChecker) tryRepairCorruptedData(key string, data []byte, secureData *crypto.SecureData, result *CheckResult) bool {
	// First try from local replicas
	if len(p.Replicas) > 0 {
		if repaired := p.tryRepairFromReplicas(key); repaired {
			result.RepairSource = "replica"
			return true
		}
	}
	
	// If there's a backup store, try that
	if p.backupStore != nil {
		backupData, err := p.backupStore.Get(key)
		if err == nil && backupData != nil {
			// Verify the backup data
			backupSecureData, err := crypto.DeserializeSecureData(string(backupData))
			if err == nil {
				_, err = p.KeyManager.DecryptAndVerify(backupSecureData)
				if err == nil {
					// Backup data is valid, use it
					if err := p.Store.Put(key, backupData); err == nil {
						result.RepairSource = "backup"
						result.Status = StatusRepairedFromSelf
						return true
					}
				}
			}
		}
	}
	
	// As a last resort, try from peer nodes if the repair client is available
	if p.RepairClient != nil {
		ctx := context.Background()
		peers, err := p.RepairClient.ListPeersWithData(ctx, key)
		if err == nil && len(peers) > 0 {
			// Collect data from multiple peers for conflict resolution
			peerData := make(map[string][]byte)
			
			for _, peerID := range peers {
				data, err := p.RepairClient.FetchDataFromPeer(ctx, peerID, key)
				if err == nil && data != nil {
					peerData[peerID] = data
				}
			}
			
			if len(peerData) > 0 {
				// If only one peer has the data, use it
				if len(peerData) == 1 {
					for peerID, data := range peerData {
						if p.verifyDataFromPeer(key, data) {
							if err := p.Store.Put(key, data); err == nil {
								result.RepairSource = "peer:" + peerID
								result.Status = StatusRepairedFromPeer
								return true
							}
						}
					}
				} else {
					// Multiple peers have data, need to resolve conflicts
					resolvedData := p.resolveDataConflict(key, nil, peerData)
					if resolvedData != nil {
						if err := p.Store.Put(key, resolvedData); err == nil {
							result.RepairSource = "conflict_resolution"
							result.Status = StatusRepairedFromPeer
							return true
						}
					}
				}
			}
		}
	}
	
	// No repair options worked
	return false
}

// tryRepairFromReplicas tries to repair data from local replicas
func (p *PeriodicChecker) tryRepairFromReplicas(key string) bool {
	for _, replica := range p.Replicas {
		// Try to get the key from the replica
		data, err := replica.Get(key)
		if err != nil || data == nil {
			continue
		}

		// Try to deserialize the data to verify its format
		secureData, err := crypto.DeserializeSecureData(string(data))
		if err != nil {
			continue
		}

		// Verify the integrity of the replica data
		_, err = p.KeyManager.DecryptAndVerify(secureData)
		if err != nil {
			continue
		}

		// The replica data looks good, use it to repair the main store
		err = p.Store.Put(key, data)
		if err != nil {
			log.Printf("Failed to repair key %s from replica: %v", key, err)
			continue
		}

		log.Printf("Successfully repaired key %s from replica", key)
		return true
	}

	return false
}

// resolveDataConflict resolves conflicts between different copies of the same data
func (p *PeriodicChecker) resolveDataConflict(key string, localData []byte, peerData map[string][]byte) []byte {
	// Create a conflict object
	conflict := &DataConflict{
		Key:         key,
		LocalData:   localData,
		ReplicaData: []map[string][]byte{peerData},
		ResolutionStrategy: p.ConflictResolutionStrategy,
	}
	
	// Call the conflict detected handler
	if p.OnConflictDetected != nil {
		p.OnConflictDetected(conflict)
	}
	
	// If we only have peer data (no local data)
	if localData == nil {
		// Handle based on resolution strategy
		switch p.ConflictResolutionStrategy {
		case PreferMajority:
			// Count occurrences of each value
			valueCounts := make(map[string]int)
			valueMap := make(map[string][]byte)
			
			for peerID, data := range peerData {
				// Use hash as key for counting
				dataHash := crypto.HashData(data)
				valueCounts[dataHash]++
				valueMap[dataHash] = data
			}
			
			// Find the most common value
			maxCount := 0
			var mostCommonHash string
			
			for hash, count := range valueCounts {
				if count > maxCount {
					maxCount = count
					mostCommonHash = hash
				}
			}
			
			if mostCommonHash != "" {
				conflict.Resolved = true
				conflict.ChosenData = valueMap[mostCommonHash]
				return valueMap[mostCommonHash]
			}
		
		case PreferNewest:
			// Try to extract timestamps from the data
			var newestData []byte
			var newestTime time.Time
			
			for _, data := range peerData {
				secureData, err := crypto.DeserializeSecureData(string(data))
				if err != nil {
					continue
				}
				
				if secureData.CreatedAt.After(newestTime) {
					newestTime = secureData.CreatedAt
					newestData = data
				}
			}
			
			if newestData != nil {
				conflict.Resolved = true
				conflict.ChosenData = newestData
				return newestData
			}
		
		default:
			// For other strategies, just pick the first available value
			for _, data := range peerData {
				conflict.Resolved = true
				conflict.ChosenData = data
				return data
			}
		}
	}
	
	// If we have local data, prefer it by default
	if localData != nil {
		conflict.Resolved = true
		conflict.ChosenData = localData
		return localData
	}
	
	// If we couldn't resolve, return nil
	return nil
}

// GetLastCheckResult retrieves the result of the most recent integrity check for a key
func (p *PeriodicChecker) GetLastCheckResult(key string) (*CheckResult, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result, exists := p.lastResults[key]
	return result, exists
}

// GetStats retrieves the current integrity check statistics
func (p *PeriodicChecker) GetStats() IntegrityStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.Stats
}

// RunManualCheck initiates a manual integrity check
func (p *PeriodicChecker) RunManualCheck() {
	go p.CheckAllData()
}

// RunManualBackup initiates a manual backup
func (p *PeriodicChecker) RunManualBackup() {
	go p.BackupAllData()
}

// HashData is a helper function to create a hash of data for comparison
func HashData(data []byte) string {
	return crypto.HashData(data)
}
