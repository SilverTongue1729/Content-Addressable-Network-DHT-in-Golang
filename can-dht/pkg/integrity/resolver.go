package integrity

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"
)

// ConflictType represents the type of conflict detected
type ConflictType int

const (
	// NoConflict indicates no conflict was detected
	NoConflict ConflictType = iota
	
	// HashMismatch indicates the hash of the data doesn't match the expected hash
	HashMismatch
	
	// VersionConflict indicates two replicas have different versions of the data
	VersionConflict
	
	// TimestampConflict indicates replicas have the same version but different timestamps
	TimestampConflict
	
	// ContentConflict indicates the content is different but the version and timestamp are the same
	ContentConflict
)

// Resolution represents the resolution strategy for a conflict
type Resolution int

const (
	// KeepExisting keeps the existing data
	KeepExisting Resolution = iota
	
	// UseRemote uses the remote data
	UseRemote
	
	// MergeData attempts to merge the data
	MergeData
	
	// UseNewest uses the data with the newest timestamp
	UseNewest
	
	// UseHighestVersion uses the data with the highest version
	UseHighestVersion
)

// VersionedData represents a versioned piece of data
type VersionedData struct {
	// The key associated with this data
	Key string
	
	// The actual data value
	Value []byte
	
	// Version number, incremented on updates
	Version int
	
	// Last update timestamp
	Timestamp time.Time
	
	// Hash of the data for integrity verification
	Hash []byte
	
	// NodeID that last updated this data
	LastUpdatedBy string
}

// ConflictResolver handles conflict detection and resolution
type ConflictResolver struct {
	// Lock for concurrent access
	mu sync.RWMutex
	
	// Map of conflict resolutions in progress
	inProgress map[string]bool
	
	// Storage adapter for accessing the data
	storage StorageAdapter
	
	// Custom conflict handlers
	handlers map[ConflictType]ResolutionHandler
}

// StorageAdapter is an interface for accessing data storage
type StorageAdapter interface {
	// Get retrieves data for a key
	Get(key string) ([]byte, error)
	
	// Put stores data for a key
	Put(key string, value []byte) error
	
	// GetAllKeys returns all keys in the storage
	GetAllKeys() ([]string, error)
	
	// GetVersionedData retrieves versioned data for a key
	GetVersionedData(key string) (*VersionedData, error)
	
	// PutVersionedData stores versioned data
	PutVersionedData(key string, data *VersionedData) error
	
	// GetReplicaData retrieves data from a replica
	GetReplicaData(key string, nodeID string) (*VersionedData, error)
}

// ResolutionHandler is a function that resolves a specific type of conflict
type ResolutionHandler func(key string, local, remote *VersionedData) (Resolution, *VersionedData, error)

// NewConflictResolver creates a new conflict resolver
func NewConflictResolver(storage StorageAdapter) *ConflictResolver {
	resolver := &ConflictResolver{
		inProgress: make(map[string]bool),
		storage:    storage,
		handlers:   make(map[ConflictType]ResolutionHandler),
	}
	
	// Set up default handlers
	resolver.RegisterHandler(HashMismatch, resolver.handleHashMismatch)
	resolver.RegisterHandler(VersionConflict, resolver.handleVersionConflict)
	resolver.RegisterHandler(TimestampConflict, resolver.handleTimestampConflict)
	resolver.RegisterHandler(ContentConflict, resolver.handleContentConflict)
	
	return resolver
}

// RegisterHandler registers a custom handler for a conflict type
func (r *ConflictResolver) RegisterHandler(conflictType ConflictType, handler ResolutionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.handlers[conflictType] = handler
}

// DetectConflict detects conflicts between local and remote data
func (r *ConflictResolver) DetectConflict(local, remote *VersionedData) ConflictType {
	// Check hash first
	if !bytes.Equal(local.Hash, ComputeHash(local.Value)) {
		return HashMismatch
	}
	
	// Check version
	if local.Version != remote.Version {
		return VersionConflict
	}
	
	// Check timestamp
	if !local.Timestamp.Equal(remote.Timestamp) {
		return TimestampConflict
	}
	
	// Check content
	if !bytes.Equal(local.Value, remote.Value) {
		return ContentConflict
	}
	
	return NoConflict
}

// ResolveConflict resolves a conflict between local and remote data
func (r *ConflictResolver) ResolveConflict(key string, local, remote *VersionedData) (*VersionedData, error) {
	r.mu.Lock()
	if r.inProgress[key] {
		r.mu.Unlock()
		return nil, fmt.Errorf("conflict resolution already in progress for key %s", key)
	}
	
	r.inProgress[key] = true
	r.mu.Unlock()
	
	defer func() {
		r.mu.Lock()
		delete(r.inProgress, key)
		r.mu.Unlock()
	}()
	
	// Detect the conflict type
	conflictType := r.DetectConflict(local, remote)
	if conflictType == NoConflict {
		return local, nil
	}
	
	// Log the conflict
	log.Printf("Detected %s conflict for key %s", conflictTypeToString(conflictType), key)
	
	// Get the appropriate handler
	r.mu.RLock()
	handler, exists := r.handlers[conflictType]
	r.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("no handler for conflict type %d", conflictType)
	}
	
	// Resolve the conflict
	resolution, resolved, err := handler(key, local, remote)
	if err != nil {
		return nil, fmt.Errorf("conflict resolution failed: %w", err)
	}
	
	// Log the resolution
	log.Printf("Resolved conflict for key %s using %s", key, resolutionToString(resolution))
	
	// If the resolution produced a new version, store it
	if resolved != nil && resolution != KeepExisting {
		err = r.storage.PutVersionedData(key, resolved)
		if err != nil {
			return nil, fmt.Errorf("failed to store resolved data: %w", err)
		}
	}
	
	return resolved, nil
}

// Default handlers

// handleHashMismatch handles hash mismatch conflicts
func (r *ConflictResolver) handleHashMismatch(key string, local, remote *VersionedData) (Resolution, *VersionedData, error) {
	// For hash mismatches, prefer remote data as local is corrupted
	resolved := &VersionedData{
		Key:          key,
		Value:        make([]byte, len(remote.Value)),
		Version:      remote.Version,
		Timestamp:    time.Now(),
		LastUpdatedBy: remote.LastUpdatedBy,
	}
	
	copy(resolved.Value, remote.Value)
	resolved.Hash = ComputeHash(resolved.Value)
	
	return UseRemote, resolved, nil
}

// handleVersionConflict handles version conflicts
func (r *ConflictResolver) handleVersionConflict(key string, local, remote *VersionedData) (Resolution, *VersionedData, error) {
	// For version conflicts, use the higher version
	if local.Version > remote.Version {
		return KeepExisting, local, nil
	}
	
	resolved := &VersionedData{
		Key:          key,
		Value:        make([]byte, len(remote.Value)),
		Version:      remote.Version,
		Timestamp:    remote.Timestamp,
		LastUpdatedBy: remote.LastUpdatedBy,
	}
	
	copy(resolved.Value, remote.Value)
	resolved.Hash = ComputeHash(resolved.Value)
	
	return UseHighestVersion, resolved, nil
}

// handleTimestampConflict handles timestamp conflicts
func (r *ConflictResolver) handleTimestampConflict(key string, local, remote *VersionedData) (Resolution, *VersionedData, error) {
	// For timestamp conflicts, use the newer timestamp
	if local.Timestamp.After(remote.Timestamp) {
		return KeepExisting, local, nil
	}
	
	resolved := &VersionedData{
		Key:          key,
		Value:        make([]byte, len(remote.Value)),
		Version:      remote.Version,
		Timestamp:    remote.Timestamp,
		LastUpdatedBy: remote.LastUpdatedBy,
	}
	
	copy(resolved.Value, remote.Value)
	resolved.Hash = ComputeHash(resolved.Value)
	
	return UseNewest, resolved, nil
}

// handleContentConflict handles content conflicts
func (r *ConflictResolver) handleContentConflict(key string, local, remote *VersionedData) (Resolution, *VersionedData, error) {
	// For content conflicts with same version and timestamp, we need a merge strategy
	// Here we'll use a simple "last write wins" based on the node ID
	if local.LastUpdatedBy > remote.LastUpdatedBy {
		// If local was updated by a "higher" node ID, keep local
		return KeepExisting, local, nil
	}
	
	resolved := &VersionedData{
		Key:          key,
		Value:        make([]byte, len(remote.Value)),
		Version:      remote.Version + 1, // Increment version after merge
		Timestamp:    time.Now(),
		LastUpdatedBy: remote.LastUpdatedBy,
	}
	
	copy(resolved.Value, remote.Value)
	resolved.Hash = ComputeHash(resolved.Value)
	
	return MergeData, resolved, nil
}

// Helper functions

// conflictTypeToString converts a conflict type to a string
func conflictTypeToString(ct ConflictType) string {
	switch ct {
	case NoConflict:
		return "NoConflict"
	case HashMismatch:
		return "HashMismatch"
	case VersionConflict:
		return "VersionConflict"
	case TimestampConflict:
		return "TimestampConflict"
	case ContentConflict:
		return "ContentConflict"
	default:
		return fmt.Sprintf("Unknown(%d)", ct)
	}
}

// resolutionToString converts a resolution to a string
func resolutionToString(r Resolution) string {
	switch r {
	case KeepExisting:
		return "KeepExisting"
	case UseRemote:
		return "UseRemote"
	case MergeData:
		return "MergeData"
	case UseNewest:
		return "UseNewest"
	case UseHighestVersion:
		return "UseHighestVersion"
	default:
		return fmt.Sprintf("Unknown(%d)", r)
	}
} 