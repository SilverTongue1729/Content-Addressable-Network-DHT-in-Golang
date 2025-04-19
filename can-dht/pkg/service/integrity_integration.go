package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/integrity"
	"github.com/can-dht/pkg/storage"
)

// IntegrityManager manages data integrity for the CAN service
type IntegrityManager struct {
	// Server is the CAN server this manager is attached to
	Server *CANServer
	
	// IntegrityStore is the store wrapper that handles integrity
	IntegrityStore *integrity.IntegrityStore
	
	// Options configures integrity checking
	Options integrity.IntegrityOptions
	
	// Checker is the enhanced integrity checker
	Checker *integrity.EnhancedChecker
	
	// ReplicaManagers keeps track of replica nodes
	ReplicaManagers map[string]*ReplicaManager
	
	// NotificationCallbacks is a list of callbacks when data integrity issues occur
	NotificationCallbacks []IntegrityNotificationCallback
}

// ReplicaManager manages a replica node
type ReplicaManager struct {
	// NodeID is the ID of the replica node
	NodeID string
	
	// Store is the store instance for this replica
	Store *storage.Store
	
	// ReliabilityScore is a score of replica reliability (0-100)
	ReliabilityScore int
	
	// LastChecked is when this replica was last checked
	LastChecked time.Time
}

// IntegrityNotificationCallback is called when integrity issues are detected
type IntegrityNotificationCallback func(key string, severity integrity.CorruptionSeverity, details string)

// InitializeIntegrityManager creates and initializes the IntegrityManager
func (s *CANServer) InitializeIntegrityManager() error {
	if s.Store == nil || s.KeyManager == nil {
		return fmt.Errorf("store or key manager not initialized")
	}
	
	// Create integrity options
	options := integrity.DefaultIntegrityOptions()
	if s.Config.IntegrityCheckInterval > 0 {
		options.CheckInterval = s.Config.IntegrityCheckInterval
	}
	
	// Set critical keys
	options.CriticalKeys = []string{
		"topology", 
		"neighbors", 
		"settings",
		fmt.Sprintf("node:%s", s.NodeID),
	}
	
	// Create integrity key
	integrityKey := make([]byte, 32)
	if _, err := crypto.RandomBytes(integrityKey); err != nil {
		return fmt.Errorf("failed to generate integrity key: %w", err)
	}
	
	// Create enhanced checker
	checker := integrity.NewEnhancedChecker(
		s.Store,
		s.KeyManager,
		options,
	)
	
	// Create integrity store
	integrityStoreOptions := integrity.IntegrityStoreOptions{
		Store:            s.Store,
		KeyManager:       s.KeyManager,
		IntegrityKey:     integrityKey,
		Level:            integrity.LevelStrong,
		AutoRepair:       true,
		IntegrityChecker: checker,
		CacheSize:        1000,
	}
	
	integrityStore := integrity.NewIntegrityStore(integrityStoreOptions)
	
	// Create integrity manager
	s.IntegrityManager = &IntegrityManager{
		Server:        s,
		IntegrityStore: integrityStore,
		Options:       options,
		Checker:       checker,
		ReplicaManagers: make(map[string]*ReplicaManager),
		NotificationCallbacks: make([]IntegrityNotificationCallback, 0),
	}
	
	// Set up callbacks
	checker.OnCorruptionFound = s.handleCorruptionFound
	checker.OnIntegrityRepaired = s.handleIntegrityRepaired
	checker.OnConsistencyFailure = s.handleConsistencyFailure
	
	// Start background checker
	checker.Start()
	
	// Start data migration in the background
	go s.migrateToIntegrityStorage()
	
	return nil
}

// migrateToIntegrityStorage migrates existing data to include integrity data
func (s *CANServer) migrateToIntegrityStorage() {
	log.Printf("Starting migration to integrity storage...")
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	
	progressCh := make(chan float64, 10)
	
	// Start progress reporter
	go func() {
		for progress := range progressCh {
			if int(progress*100)%10 == 0 {
				log.Printf("Migration progress: %.1f%%", progress*100)
			}
		}
	}()
	
	// Run migration
	err := s.IntegrityManager.IntegrityStore.MigrateToIntegrityStorage(ctx, progressCh)
	close(progressCh)
	
	if err != nil {
		log.Printf("Migration error: %v", err)
	} else {
		log.Printf("Migration to integrity storage completed successfully")
	}
}

// handleCorruptionFound is called when corruption is detected
func (s *CANServer) handleCorruptionFound(key string, result *integrity.CheckResult) {
	// Log the corruption
	log.Printf("Corruption detected for key %s: %v", key, result.Error)
	
	// Determine severity
	severity := integrity.SeverityMedium
	for _, criticalKey := range s.IntegrityManager.Options.CriticalKeys {
		if key == criticalKey {
			severity = integrity.SeverityCritical
			break
		}
	}
	
	// Notify callbacks
	details := fmt.Sprintf("Status: %v, Error: %v", result.Status, result.Error)
	for _, callback := range s.IntegrityManager.NotificationCallbacks {
		go callback(key, severity, details)
	}
	
	// For critical keys, try immediate repair
	if severity >= integrity.SeverityHigh {
		repaired, err := s.IntegrityManager.IntegrityStore.RepairKey(key)
		if err != nil || !repaired {
			log.Printf("Failed to repair critical key %s: %v", key, err)
			
			// Try to fetch from neighbors
			s.fetchKeyFromNeighbors(key)
		}
	}
}

// handleIntegrityRepaired is called when data integrity is repaired
func (s *CANServer) handleIntegrityRepaired(key string, source string) {
	log.Printf("Data integrity repaired for key %s from %s", key, source)
	
	// Update replica stats if applicable
	if replicaNodeID, isReplica := isReplicaSource(source); isReplica {
		if replicaManager, exists := s.IntegrityManager.ReplicaManagers[replicaNodeID]; exists {
			replicaManager.ReliabilityScore += 5
			if replicaManager.ReliabilityScore > 100 {
				replicaManager.ReliabilityScore = 100
			}
			replicaManager.LastChecked = time.Now()
		}
	}
	
	// Notify callbacks
	for _, callback := range s.IntegrityManager.NotificationCallbacks {
		go callback(key, integrity.SeverityLow, fmt.Sprintf("Repaired from %s", source))
	}
}

// handleConsistencyFailure is called when replica consistency fails
func (s *CANServer) handleConsistencyFailure(key string, results []integrity.ConsistencyResult) {
	log.Printf("Consistency failure for key %s across %d replicas", key, len(results))
	
	// Count valid vs invalid replicas
	valid := 0
	invalid := 0
	for _, result := range results {
		if result.IsValid {
			valid++
		} else {
			invalid++
		}
	}
	
	// Determine severity based on ratio
	severity := integrity.SeverityMedium
	if invalid > valid {
		severity = integrity.SeverityHigh
	}
	
	// Notify callbacks
	details := fmt.Sprintf("Consistency failure: %d valid, %d invalid replicas", valid, invalid)
	for _, callback := range s.IntegrityManager.NotificationCallbacks {
		go callback(key, severity, details)
	}
}

// RegisterIntegrityNotificationCallback registers a callback for integrity notifications
func (s *CANServer) RegisterIntegrityNotificationCallback(callback IntegrityNotificationCallback) {
	s.IntegrityManager.NotificationCallbacks = append(
		s.IntegrityManager.NotificationCallbacks, 
		callback,
	)
}

// AddReplicaNode adds a replica node to the integrity system
func (s *CANServer) AddReplicaNode(nodeID string, store *storage.Store) {
	if s.IntegrityManager == nil {
		return
	}
	
	// Add to replica manager
	s.IntegrityManager.ReplicaManagers[nodeID] = &ReplicaManager{
		NodeID:          nodeID,
		Store:           store,
		ReliabilityScore: 100, // Start with perfect score
		LastChecked:     time.Now(),
	}
	
	// Add to enhanced checker
	s.IntegrityManager.Checker.AddReplica(nodeID, store)
}

// RemoveReplicaNode removes a replica node from the integrity system
func (s *CANServer) RemoveReplicaNode(nodeID string) {
	if s.IntegrityManager == nil {
		return
	}
	
	delete(s.IntegrityManager.ReplicaManagers, nodeID)
}

// RunManualIntegrityCheck initiates a manual integrity check
func (s *CANServer) RunManualIntegrityCheck(ctx context.Context) (*integrity.IntegrityStats, error) {
	if s.IntegrityManager == nil {
		return nil, fmt.Errorf("integrity manager not initialized")
	}
	
	progressCh := make(chan float64, 10)
	
	// Start progress reporter
	go func() {
		for progress := range progressCh {
			if int(progress*100)%10 == 0 {
				log.Printf("Integrity check progress: %.1f%%", progress*100)
			}
		}
	}()
	
	// Run verification
	stats, err := s.IntegrityManager.Checker.VerifyAllKeys(ctx, progressCh)
	close(progressCh)
	
	return stats, err
}

// fetchKeyFromNeighbors tries to fetch a key from neighboring nodes
func (s *CANServer) fetchKeyFromNeighbors(key string) {
	// Get neighbors
	neighbors := s.getNeighbors()
	if len(neighbors) == 0 {
		log.Printf("No neighbors available to fetch key %s", key)
		return
	}
	
	// Try each neighbor
	for _, neighbor := range neighbors {
		// Skip offline neighbors
		if !neighbor.IsOnline {
			continue
		}
		
		// Try to fetch data from neighbor
		log.Printf("Fetching key %s from neighbor %s", key, neighbor.NodeID)
		
		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		
		// Make the RPC call
		resp, err := s.GetFromReplica(ctx, neighbor.Address, key)
		cancel()
		
		if err != nil {
			log.Printf("Failed to fetch key %s from neighbor %s: %v", key, neighbor.NodeID, err)
			continue
		}
		
		if !resp.Exists {
			log.Printf("Key %s not found on neighbor %s", key, neighbor.NodeID)
			continue
		}
		
		// Verify integrity of received data
		integrityData, err := integrity.ComputeIntegrityData(
			resp.Data, 
			s.IntegrityManager.Options.Level,
			s.IntegrityManager.IntegrityStore.IntegrityKey,
		)
		if err != nil {
			log.Printf("Failed to compute integrity for fetched data: %v", err)
			continue
		}
		
		// Store with integrity data
		combined, err := integrity.SerializeDataWithIntegrity(resp.Data, integrityData)
		if err != nil {
			log.Printf("Failed to serialize data with integrity: %v", err)
			continue
		}
		
		// Update local store
		err = s.Store.Put(key, combined)
		if err != nil {
			log.Printf("Failed to store fetched data: %v", err)
			continue
		}
		
		log.Printf("Successfully fetched and restored key %s from neighbor %s", key, neighbor.NodeID)
		return
	}
	
	log.Printf("Failed to fetch key %s from any neighbor", key)
}

// Helper Functions

// isReplicaSource checks if a source string represents a replica
func isReplicaSource(source string) (string, bool) {
	// Check if source starts with "replica "
	if len(source) > 8 && source[:8] == "replica " {
		return source[8:], true
	}
	return "", false
}

// GetFromReplica fetches data from a replica node
func (s *CANServer) GetFromReplica(ctx context.Context, address string, key string) (*GetResponse, error) {
	// This would make an RPC call to another node
	// For now, just return a stub
	return &GetResponse{
		Exists: false,
		Data:   nil,
	}, nil
}

// GetResponse represents a response from a replica
type GetResponse struct {
	Exists bool
	Data   []byte
} 