package integrity

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/storage"
)

// IntegrityStore wraps a Store with automatic integrity verification
type IntegrityStore struct {
	// Store is the underlying data store
	Store *storage.Store
	
	// KeyManager is used for integrity operations
	KeyManager *crypto.KeyManager
	
	// IntegrityKey is the key used for HMAC calculation
	IntegrityKey []byte
	
	// Level determines the integrity level to use
	Level IntegrityLevel
	
	// AutoRepair indicates whether to automatically repair corrupted data
	AutoRepair bool
	
	// Checker is the integrity checker used for verification
	Checker *EnhancedChecker
	
	// IntegrityCache caches integrity data for frequently accessed keys
	IntegrityCache map[string]IntegrityData
	
	// mu protects concurrent access
	mu sync.RWMutex
	
	// cacheHits counts cache hits for statistics
	cacheHits int
	
	// cacheMisses counts cache misses for statistics
	cacheMisses int
}

// IntegrityStoreOptions configures the integrity store
type IntegrityStoreOptions struct {
	// Store is the underlying data store
	Store *storage.Store
	
	// KeyManager is used for crypto operations
	KeyManager *crypto.KeyManager
	
	// IntegrityKey is the key used for HMAC
	IntegrityKey []byte
	
	// Level determines the integrity level to use
	Level IntegrityLevel
	
	// AutoRepair indicates whether to automatically repair corrupted data
	AutoRepair bool
	
	// IntegrityChecker is an existing checker to use (optional)
	IntegrityChecker *EnhancedChecker
	
	// CacheSize is the maximum size of the integrity cache
	CacheSize int
}

// NewIntegrityStore creates a new store with integrity verification
func NewIntegrityStore(options IntegrityStoreOptions) *IntegrityStore {
	store := &IntegrityStore{
		Store:          options.Store,
		KeyManager:     options.KeyManager,
		IntegrityKey:   options.IntegrityKey,
		Level:          options.Level,
		AutoRepair:     options.AutoRepair,
		IntegrityCache: make(map[string]IntegrityData),
	}
	
	// If no integrity key was provided, create one
	if store.IntegrityKey == nil {
		store.IntegrityKey = make([]byte, 32)
		_, err := crypto.RandomBytes(store.IntegrityKey)
		if err != nil {
			panic(fmt.Sprintf("failed to generate integrity key: %v", err))
		}
	}
	
	// If no checker was provided, create one
	if options.IntegrityChecker == nil {
		integrityOptions := DefaultIntegrityOptions()
		integrityOptions.Level = options.Level
		
		store.Checker = NewEnhancedChecker(
			options.Store,
			options.KeyManager,
			integrityOptions,
		)
	} else {
		store.Checker = options.IntegrityChecker
	}
	
	return store
}

// Put stores a value with integrity data
func (s *IntegrityStore) Put(key string, value []byte) error {
	// Compute integrity data
	integrityData, err := ComputeIntegrityData(value, s.Level, s.IntegrityKey)
	if err != nil {
		return fmt.Errorf("failed to compute integrity data: %w", err)
	}
	
	// Serialize data with integrity
	combined, err := SerializeDataWithIntegrity(value, integrityData)
	if err != nil {
		return fmt.Errorf("failed to serialize data with integrity: %w", err)
	}
	
	// Update integrity cache
	s.mu.Lock()
	s.IntegrityCache[key] = integrityData
	s.mu.Unlock()
	
	// Store in the underlying store
	return s.Store.Put(key, combined)
}

// Get retrieves a value and verifies its integrity
func (s *IntegrityStore) Get(key string) ([]byte, bool, error) {
	// Get from the underlying store
	combined, exists, err := s.Store.Get(key)
	if err != nil || !exists {
		return nil, exists, err
	}
	
	// Try to deserialize with integrity data
	data, integrityData, err := DeserializeDataWithIntegrity(combined)
	if err != nil {
		// If deserialization fails, this might be data without integrity info
		// Just return it as-is
		return combined, true, nil
	}
	
	// Verify integrity
	valid, err := VerifyIntegrity(data, integrityData, s.IntegrityKey)
	if err != nil || !valid {
		if s.AutoRepair {
			// Try to repair automatically
			ctx := context.Background()
			result, repairErr := s.Checker.RunIntegrityCheck(ctx, key)
			if repairErr != nil || !result.RepairedOK {
				return nil, true, fmt.Errorf("integrity verification failed: %w", err)
			}
			
			// Try again after repair
			return s.Get(key)
		}
		return nil, true, fmt.Errorf("integrity verification failed: %w", err)
	}
	
	// Update cache
	s.mu.Lock()
	s.IntegrityCache[key] = integrityData
	s.mu.Unlock()
	
	return data, true, nil
}

// CheckIntegrity verifies the integrity of a key
func (s *IntegrityStore) CheckIntegrity(key string) (bool, error) {
	// Check cache first
	s.mu.RLock()
	cachedIntegrity, found := s.IntegrityCache[key]
	s.mu.RUnlock()
	
	if found {
		s.mu.Lock()
		s.cacheHits++
		s.mu.Unlock()
		
		// Get data from store
		combined, exists, err := s.Store.Get(key)
		if err != nil || !exists {
			return false, err
		}
		
		// Try to deserialize with integrity data
		data, integrityData, err := DeserializeDataWithIntegrity(combined)
		if err != nil {
			return false, err
		}
		
		// Verify that stored integrity matches cached
		if cachedIntegrity.Hash != integrityData.Hash || 
		   (s.Level >= LevelStrong && cachedIntegrity.HMAC != integrityData.HMAC) {
			return false, fmt.Errorf("integrity mismatch between cache and storage")
		}
		
		// Verify integrity
		return VerifyIntegrity(data, integrityData, s.IntegrityKey)
	}
	
	s.mu.Lock()
	s.cacheMisses++
	s.mu.Unlock()
	
	// Not in cache, get from store
	combined, exists, err := s.Store.Get(key)
	if err != nil || !exists {
		return false, err
	}
	
	// Try to deserialize with integrity data
	data, integrityData, err := DeserializeDataWithIntegrity(combined)
	if err != nil {
		return false, err
	}
	
	// Verify integrity
	valid, err := VerifyIntegrity(data, integrityData, s.IntegrityKey)
	if valid && err == nil {
		// Update cache
		s.mu.Lock()
		s.IntegrityCache[key] = integrityData
		s.mu.Unlock()
	}
	
	return valid, err
}

// Delete removes a key-value pair
func (s *IntegrityStore) Delete(key string) error {
	// Remove from cache
	s.mu.Lock()
	delete(s.IntegrityCache, key)
	s.mu.Unlock()
	
	// Remove from underlying store
	return s.Store.Delete(key)
}

// GetCacheStats returns statistics about the integrity cache
func (s *IntegrityStore) GetCacheStats() (hits, misses int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return s.cacheHits, s.cacheMisses
}

// MigrateToIntegrityStorage migrates existing data to include integrity data
func (s *IntegrityStore) MigrateToIntegrityStorage(ctx context.Context, progressCh chan<- float64) error {
	// Get all keys
	keys, err := s.Store.GetAllKeys()
	if err != nil {
		return fmt.Errorf("failed to get all keys: %w", err)
	}
	
	totalKeys := len(keys)
	if totalKeys == 0 {
		return nil
	}
	
	// Process each key
	for i, key := range keys {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue processing
		}
		
		// Get the data
		data, exists, err := s.Store.Get(key)
		if err != nil || !exists {
			continue
		}
		
		// Check if already has integrity data
		_, _, err = DeserializeDataWithIntegrity(data)
		if err == nil {
			// Already has integrity data, skip
			continue
		}
		
		// Compute integrity data
		integrityData, err := ComputeIntegrityData(data, s.Level, s.IntegrityKey)
		if err != nil {
			continue
		}
		
		// Serialize with integrity
		combined, err := SerializeDataWithIntegrity(data, integrityData)
		if err != nil {
			continue
		}
		
		// Update in store
		err = s.Store.Put(key, combined)
		if err != nil {
			continue
		}
		
		// Update cache
		s.mu.Lock()
		s.IntegrityCache[key] = integrityData
		s.mu.Unlock()
		
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
	
	return nil
}

// VerifyAllKeys checks the integrity of all keys
func (s *IntegrityStore) VerifyAllKeys(ctx context.Context, progressCh chan<- float64) (int, int, error) {
	// Get all keys
	keys, err := s.Store.GetAllKeys()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get all keys: %w", err)
	}
	
	totalKeys := len(keys)
	if totalKeys == 0 {
		return 0, 0, nil
	}
	
	valid := 0
	invalid := 0
	
	// Process each key
	for i, key := range keys {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return valid, invalid, ctx.Err()
		default:
			// Continue processing
		}
		
		// Verify integrity
		isValid, err := s.CheckIntegrity(key)
		if err != nil || !isValid {
			invalid++
		} else {
			valid++
		}
		
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
	
	return valid, invalid, nil
}

// RepairKey attempts to repair a corrupted key
func (s *IntegrityStore) RepairKey(key string) (bool, error) {
	// Run integrity check
	ctx := context.Background()
	result, err := s.Checker.RunIntegrityCheck(ctx, key)
	if err != nil {
		return false, err
	}
	
	return result.RepairedOK, nil
}

// GetIntegrityData returns the integrity data for a key
func (s *IntegrityStore) GetIntegrityData(key string) (IntegrityData, error) {
	// Check cache first
	s.mu.RLock()
	cachedIntegrity, found := s.IntegrityCache[key]
	s.mu.RUnlock()
	
	if found {
		return cachedIntegrity, nil
	}
	
	// Not in cache, get from store
	combined, exists, err := s.Store.Get(key)
	if err != nil {
		return IntegrityData{}, err
	}
	
	if !exists {
		return IntegrityData{}, fmt.Errorf("key not found")
	}
	
	// Try to deserialize with integrity data
	_, integrityData, err := DeserializeDataWithIntegrity(combined)
	if err != nil {
		return IntegrityData{}, err
	}
	
	// Update cache
	s.mu.Lock()
	s.IntegrityCache[key] = integrityData
	s.mu.Unlock()
	
	return integrityData, nil
} 