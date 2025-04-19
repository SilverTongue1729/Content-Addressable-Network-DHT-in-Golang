package integrity

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
)

// StorageInterface defines the interface for storage operations
type StorageInterface interface {
	// GetAllKeys returns all keys in the storage
	GetAllKeys() ([]string, error)
	
	// Get retrieves a value by key
	Get(key string) ([]byte, error)
	
	// Put stores a value with a key
	Put(key string, value []byte) error
}

// IntegrityData represents data with integrity information
type IntegrityData struct {
	// Value is the actual data
	Value []byte
	
	// HMAC is the integrity hash
	HMAC []byte
	
	// LastVerified is when this data was last verified
	LastVerified time.Time
}

// PeriodicChecker performs periodic integrity checks
type PeriodicChecker struct {
	// Storage interface for accessing data
	Storage StorageInterface
	
	// KeyManager for integrity operations
	KeyManager *crypto.KeyManager
	
	// CheckInterval is how often to perform integrity checks
	CheckInterval time.Duration
	
	// BatchSize is how many items to check in one batch
	BatchSize int
	
	// RecoveryHandler is called when corrupted data is detected
	RecoveryHandler func(ctx context.Context, key string) error
	
	// Stats tracks integrity check statistics
	Stats CheckStats
	
	// stopChan is used to signal the checker to stop
	stopChan chan struct{}
	
	// statsMu protects the stats
	statsMu sync.RWMutex
}

// CheckStats tracks statistics for integrity checks
type CheckStats struct {
	// LastCheckTime is when the last check was performed
	LastCheckTime time.Time
	
	// ItemsChecked is the total number of items checked
	ItemsChecked int
	
	// CorruptedItems is the number of corrupted items detected
	CorruptedItems int
	
	// RepairedItems is the number of items successfully repaired
	RepairedItems int
	
	// FailedRepairs is the number of failed repair attempts
	FailedRepairs int
}

// NewPeriodicChecker creates a new periodic integrity checker
func NewPeriodicChecker(storage StorageInterface, keyManager *crypto.KeyManager, interval time.Duration) *PeriodicChecker {
	return &PeriodicChecker{
		Storage:         storage,
		KeyManager:      keyManager,
		CheckInterval:   interval,
		BatchSize:       100, // Process up to 100 items per batch
		RecoveryHandler: nil,
		stopChan:        make(chan struct{}),
		Stats: CheckStats{
			LastCheckTime:  time.Time{},
			ItemsChecked:   0,
			CorruptedItems: 0,
			RepairedItems:  0,
			FailedRepairs:  0,
		},
	}
}

// SetRecoveryHandler sets the handler for corrupted data
func (pc *PeriodicChecker) SetRecoveryHandler(handler func(ctx context.Context, key string) error) {
	pc.RecoveryHandler = handler
}

// Start begins periodic integrity checks
func (pc *PeriodicChecker) Start(ctx context.Context) {
	ticker := time.NewTicker(pc.CheckInterval)
	defer ticker.Stop()

	// Perform an initial check
	go pc.PerformCheck(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.stopChan:
			return
		case <-ticker.C:
			go pc.PerformCheck(ctx)
		}
	}
}

// Stop stops the periodic integrity checks
func (pc *PeriodicChecker) Stop() {
	close(pc.stopChan)
}

// GetStats returns the current integrity check statistics
func (pc *PeriodicChecker) GetStats() CheckStats {
	pc.statsMu.RLock()
	defer pc.statsMu.RUnlock()
	return pc.Stats
}

// PerformCheck performs an integrity check on all stored data
func (pc *PeriodicChecker) PerformCheck(ctx context.Context) {
	log.Printf("Starting integrity check")
	
	// Get all keys
	keys, err := pc.Storage.GetAllKeys()
	if err != nil {
		log.Printf("Error getting keys for integrity check: %v", err)
		return
	}
	
	// Update stats
	pc.statsMu.Lock()
	pc.Stats.LastCheckTime = time.Now()
	localStats := CheckStats{
		LastCheckTime:  pc.Stats.LastCheckTime,
		ItemsChecked:   0,
		CorruptedItems: 0,
		RepairedItems:  0,
		FailedRepairs:  0,
	}
	pc.statsMu.Unlock()
	
	// Process in batches to avoid memory pressure
	for i := 0; i < len(keys); i += pc.BatchSize {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return
		case <-pc.stopChan:
			return
		default:
			// Continue processing
		}
		
		// Determine the end of this batch
		end := i + pc.BatchSize
		if end > len(keys) {
			end = len(keys)
		}
		
		// Process this batch
		batchKeys := keys[i:end]
		for _, key := range batchKeys {
			// Check integrity of this key
			err := pc.CheckIntegrity(ctx, key)
			
			// Update local stats
			localStats.ItemsChecked++
			if err != nil {
				localStats.CorruptedItems++
				log.Printf("Integrity check failed for key %s: %v", key, err)
				
				// Try to recover if a recovery handler is set
				if pc.RecoveryHandler != nil {
					if err := pc.RecoveryHandler(ctx, key); err != nil {
						log.Printf("Failed to recover corrupted data for key %s: %v", key, err)
						localStats.FailedRepairs++
					} else {
						log.Printf("Successfully recovered data for key %s", key)
						localStats.RepairedItems++
					}
				}
			}
		}
		
		// Update global stats periodically
		pc.statsMu.Lock()
		pc.Stats.ItemsChecked += localStats.ItemsChecked
		pc.Stats.CorruptedItems += localStats.CorruptedItems
		pc.Stats.RepairedItems += localStats.RepairedItems
		pc.Stats.FailedRepairs += localStats.FailedRepairs
		pc.statsMu.Unlock()
		
		// Reset local stats for next batch
		localStats.ItemsChecked = 0
		localStats.CorruptedItems = 0
		localStats.RepairedItems = 0
		localStats.FailedRepairs = 0
	}
	
	log.Printf("Completed integrity check: checked %d items, found %d corrupted, repaired %d", 
		pc.Stats.ItemsChecked, pc.Stats.CorruptedItems, pc.Stats.RepairedItems)
}

// CheckIntegrity checks the integrity of a single item
func (pc *PeriodicChecker) CheckIntegrity(ctx context.Context, key string) error {
	// Get the stored value
	data, err := pc.Storage.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}
	
	// Parse the data
	integrityData, err := parseIntegrityData(data)
	if err != nil {
		return fmt.Errorf("failed to parse integrity data: %w", err)
	}
	
	// Verify the HMAC
	if !pc.KeyManager.VerifyHMAC(integrityData.Value, integrityData.HMAC) {
		return fmt.Errorf("HMAC verification failed")
	}
	
	// Update last verified time
	integrityData.LastVerified = time.Now()
	
	// Reserialize and store
	updatedData, err := serializeIntegrityData(integrityData)
	if err != nil {
		return fmt.Errorf("failed to serialize updated data: %w", err)
	}
	
	// Only update if we need to (to avoid unnecessary writes)
	if !bytesEqual(data, updatedData) {
		if err := pc.Storage.Put(key, updatedData); err != nil {
			return fmt.Errorf("failed to update last verified timestamp: %w", err)
		}
	}
	
	return nil
}

// PrepareDataWithIntegrity adds integrity information to data
func (pc *PeriodicChecker) PrepareDataWithIntegrity(value []byte) ([]byte, error) {
	// Generate HMAC
	hmac := pc.KeyManager.GenerateHMAC(value)
	
	// Create integrity data
	integrityData := IntegrityData{
		Value:        value,
		HMAC:         hmac,
		LastVerified: time.Now(),
	}
	
	// Serialize
	return serializeIntegrityData(integrityData)
}

// VerifyDataIntegrity verifies the integrity of data
func (pc *PeriodicChecker) VerifyDataIntegrity(data []byte) ([]byte, error) {
	// Parse the data
	integrityData, err := parseIntegrityData(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse integrity data: %w", err)
	}
	
	// Verify the HMAC
	if !pc.KeyManager.VerifyHMAC(integrityData.Value, integrityData.HMAC) {
		return nil, fmt.Errorf("HMAC verification failed")
	}
	
	// Return the actual value
	return integrityData.Value, nil
}

// parseIntegrityData parses serialized integrity data
func parseIntegrityData(data []byte) (*IntegrityData, error) {
	// For simplicity, we'll just assume the first 32 bytes are the HMAC,
	// the next 8 bytes are the last verified timestamp, and the rest is the value
	if len(data) < 40 {
		return nil, fmt.Errorf("data too short")
	}
	
	hmac := data[0:32]
	lastVerifiedBytes := data[32:40]
	value := data[40:]
	
	// Parse timestamp
	lastVerified := time.Unix(0, bytesToInt64(lastVerifiedBytes))
	
	return &IntegrityData{
		Value:        value,
		HMAC:         hmac,
		LastVerified: lastVerified,
	}, nil
}

// serializeIntegrityData serializes integrity data
func serializeIntegrityData(data *IntegrityData) ([]byte, error) {
	// Calculate total size
	totalSize := len(data.HMAC) + 8 + len(data.Value)
	result := make([]byte, totalSize)
	
	// Copy HMAC
	copy(result[0:], data.HMAC)
	
	// Copy timestamp
	timestampBytes := int64ToBytes(data.LastVerified.UnixNano())
	copy(result[32:], timestampBytes)
	
	// Copy value
	copy(result[40:], data.Value)
	
	return result, nil
}

// int64ToBytes converts an int64 to 8 bytes
func int64ToBytes(val int64) []byte {
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[i] = byte(val >> (i * 8))
	}
	return result
}

// bytesToInt64 converts 8 bytes to an int64
func bytesToInt64(data []byte) int64 {
	var result int64
	for i := 0; i < 8; i++ {
		result |= int64(data[i]) << (i * 8)
	}
	return result
}

// bytesEqual compares two byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
} 