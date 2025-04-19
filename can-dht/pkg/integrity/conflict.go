package integrity

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"time"
)

// ConflictResolutionStrategy defines how conflicts are resolved
type ConflictResolutionStrategy int

const (
	// StrategyLatestWins uses timestamps to choose the latest version
	StrategyLatestWins ConflictResolutionStrategy = iota
	// StrategyMajorityWins uses the most common version among replicas
	StrategyMajorityWins
	// StrategyMerge attempts to merge conflicting data if possible
	StrategyMerge
)

// VersionedData represents data with version information for conflict resolution
type VersionedData struct {
	// Data is the actual value
	Data []byte
	// Version is a logical timestamp or version number
	Version uint64
	// Timestamp is when this data was last updated
	Timestamp time.Time
	// SourceID identifies the node that created this version
	SourceID string
	// Checksum is a hash of the data for integrity verification
	Checksum []byte
}

// ConflictResolver handles conflict resolution between different versions
type ConflictResolver struct {
	// Strategy defines how conflicts are resolved
	Strategy ConflictResolutionStrategy
	// MaxVersionsToKeep limits how many older versions are retained
	MaxVersionsToKeep int
}

// NewConflictResolver creates a new conflict resolver
func NewConflictResolver(strategy ConflictResolutionStrategy) *ConflictResolver {
	return &ConflictResolver{
		Strategy:         strategy,
		MaxVersionsToKeep: 3, // Default to keeping 3 versions
	}
}

// ResolveConflict resolves conflicts between multiple versions of data
func (cr *ConflictResolver) ResolveConflict(versions []*VersionedData) (*VersionedData, error) {
	if len(versions) == 0 {
		return nil, fmt.Errorf("no versions provided")
	}
	
	if len(versions) == 1 {
		return versions[0], nil
	}
	
	switch cr.Strategy {
	case StrategyLatestWins:
		return cr.resolveByLatestTimestamp(versions)
	case StrategyMajorityWins:
		return cr.resolveByMajority(versions)
	case StrategyMerge:
		return cr.resolveByMerge(versions)
	default:
		return cr.resolveByLatestTimestamp(versions)
	}
}

// resolveByLatestTimestamp selects the version with the latest timestamp
func (cr *ConflictResolver) resolveByLatestTimestamp(versions []*VersionedData) (*VersionedData, error) {
	if len(versions) == 0 {
		return nil, fmt.Errorf("no versions provided")
	}
	
	// Sort by timestamp, descending
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Timestamp.After(versions[j].Timestamp)
	})
	
	return versions[0], nil
}

// resolveByMajority selects the version that appears most frequently
func (cr *ConflictResolver) resolveByMajority(versions []*VersionedData) (*VersionedData, error) {
	if len(versions) == 0 {
		return nil, fmt.Errorf("no versions provided")
	}
	
	// Count occurrences of each version based on checksum
	counts := make(map[string]int)
	dataMap := make(map[string]*VersionedData)
	
	for _, v := range versions {
		key := fmt.Sprintf("%x", v.Checksum)
		counts[key]++
		dataMap[key] = v
	}
	
	// Find the most common version
	var maxKey string
	maxCount := 0
	for key, count := range counts {
		if count > maxCount {
			maxCount = count
			maxKey = key
		}
	}
	
	// In case of a tie, use the latest timestamp
	if maxCount == 1 && len(versions) > 1 {
		return cr.resolveByLatestTimestamp(versions)
	}
	
	return dataMap[maxKey], nil
}

// resolveByMerge attempts to merge data if possible
func (cr *ConflictResolver) resolveByMerge(versions []*VersionedData) (*VersionedData, error) {
	// First check if we can find a majority
	majorityVersion, err := cr.resolveByMajority(versions)
	if err == nil && majorityVersion != nil {
		// If we have a clear majority, use that
		return majorityVersion, nil
	}
	
	// Sort by timestamp, descending
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Timestamp.After(versions[j].Timestamp)
	})
	
	// For this basic implementation, we'll just use the latest version
	// A more advanced implementation could attempt to merge JSON or other
	// structured data formats intelligently
	return versions[0], nil
}

// EnhancedIntegrityManager manages versioned data and conflict resolution
type EnhancedIntegrityManager struct {
	// Store provides access to stored data
	Store DataStoreInterface
	// Resolver handles conflict resolution
	Resolver *ConflictResolver
	// ReplicaStore provides access to replica data
	ReplicaStore ReplicaStoreInterface
}

// NewEnhancedIntegrityManager creates a new integrity manager
func NewEnhancedIntegrityManager(store DataStoreInterface, replicaStore ReplicaStoreInterface) *EnhancedIntegrityManager {
	return &EnhancedIntegrityManager{
		Store:       store,
		Resolver:    NewConflictResolver(StrategyMajorityWins),
		ReplicaStore: replicaStore,
	}
}

// CheckAndResolve checks data integrity and resolves conflicts if needed
func (im *EnhancedIntegrityManager) CheckAndResolve(ctx context.Context, key string) error {
	// Get all available versions from replicas
	versions, err := im.collectVersions(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to collect versions: %w", err)
	}
	
	if len(versions) == 0 {
		return fmt.Errorf("no versions found for key %s", key)
	}
	
	// Check if we have conflicts
	if len(versions) == 1 {
		// No conflict, just verify integrity
		if !im.verifyIntegrity(versions[0]) {
			return fmt.Errorf("integrity check failed for key %s", key)
		}
		return nil
	}
	
	// We have multiple versions, need to resolve conflicts
	resolved, err := im.Resolver.ResolveConflict(versions)
	if err != nil {
		return fmt.Errorf("failed to resolve conflict: %w", err)
	}
	
	// Update with the resolved version
	log.Printf("Resolved conflict for key %s with strategy %d", key, im.Resolver.Strategy)
	return im.updateWithResolved(ctx, key, resolved)
}

// collectVersions gathers all available versions from primary and replicas
func (im *EnhancedIntegrityManager) collectVersions(ctx context.Context, key string) ([]*VersionedData, error) {
	versions := make([]*VersionedData, 0)
	
	// Get the local version
	localData, err := im.Store.Get(key)
	if err == nil {
		// Try to unmarshal as versioned data
		vd, err := unmarshalVersionedData(localData)
		if err == nil {
			versions = append(versions, vd)
		} else {
			// Not versioned, create a version
			vd = &VersionedData{
				Data:      localData,
				Version:   1,
				Timestamp: time.Now(),
				Checksum:  calculateChecksum(localData),
			}
			versions = append(versions, vd)
		}
	}
	
	// If we have a replica store, get versions from replicas
	if im.ReplicaStore != nil {
		replicaNodes, err := im.ReplicaStore.GetReplicaNodes(key)
		if err == nil {
			for _, nodeID := range replicaNodes {
				replicaData, err := im.ReplicaStore.GetFromReplica(ctx, key, nodeID)
				if err == nil {
					vd, err := unmarshalVersionedData(replicaData)
					if err == nil {
						versions = append(versions, vd)
					} else {
						// Not versioned, create a version
						vd = &VersionedData{
							Data:      replicaData,
							Version:   1,
							Timestamp: time.Now(),
							SourceID:  string(nodeID),
							Checksum:  calculateChecksum(replicaData),
						}
						versions = append(versions, vd)
					}
				}
			}
		}
	}
	
	return versions, nil
}

// verifyIntegrity verifies the integrity of a single version
func (im *EnhancedIntegrityManager) verifyIntegrity(vd *VersionedData) bool {
	if vd == nil || vd.Checksum == nil {
		return false
	}
	
	currentChecksum := calculateChecksum(vd.Data)
	return bytes.Equal(currentChecksum, vd.Checksum)
}

// updateWithResolved updates storage with the resolved version
func (im *EnhancedIntegrityManager) updateWithResolved(ctx context.Context, key string, resolved *VersionedData) error {
	// Increment version
	resolved.Version++
	resolved.Timestamp = time.Now()
	resolved.Checksum = calculateChecksum(resolved.Data)
	
	// Marshal the versioned data
	data, err := marshalVersionedData(resolved)
	if err != nil {
		return fmt.Errorf("failed to marshal resolved data: %w", err)
	}
	
	// Update the local store
	err = im.Store.Put(key, data)
	if err != nil {
		return fmt.Errorf("failed to update store with resolved data: %w", err)
	}
	
	// Update replicas if we have a replica store
	if im.ReplicaStore != nil {
		replicaNodes, err := im.ReplicaStore.GetReplicaNodes(key)
		if err == nil {
			for _, nodeID := range replicaNodes {
				err := im.ReplicaStore.UpdateReplica(ctx, key, data, nodeID)
				if err != nil {
					log.Printf("Warning: failed to update replica %s for key %s: %v", nodeID, key, err)
				}
			}
		}
	}
	
	return nil
}

// marshalVersionedData serializes versioned data
func marshalVersionedData(vd *VersionedData) ([]byte, error) {
	// Format:
	// - 8 bytes: Version (uint64)
	// - 8 bytes: Timestamp (int64 unix nano)
	// - 4 bytes: SourceID length (uint32)
	// - X bytes: SourceID
	// - 4 bytes: Checksum length (uint32)
	// - Y bytes: Checksum
	// - 4 bytes: Data length (uint32)
	// - Z bytes: Data
	
	var buf bytes.Buffer
	
	// Write version
	if err := binary.Write(&buf, binary.LittleEndian, vd.Version); err != nil {
		return nil, err
	}
	
	// Write timestamp
	if err := binary.Write(&buf, binary.LittleEndian, vd.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	
	// Write source ID
	sourceIDLen := uint32(len(vd.SourceID))
	if err := binary.Write(&buf, binary.LittleEndian, sourceIDLen); err != nil {
		return nil, err
	}
	if sourceIDLen > 0 {
		if _, err := buf.Write([]byte(vd.SourceID)); err != nil {
			return nil, err
		}
	}
	
	// Write checksum
	checksumLen := uint32(len(vd.Checksum))
	if err := binary.Write(&buf, binary.LittleEndian, checksumLen); err != nil {
		return nil, err
	}
	if checksumLen > 0 {
		if _, err := buf.Write(vd.Checksum); err != nil {
			return nil, err
		}
	}
	
	// Write data
	dataLen := uint32(len(vd.Data))
	if err := binary.Write(&buf, binary.LittleEndian, dataLen); err != nil {
		return nil, err
	}
	if dataLen > 0 {
		if _, err := buf.Write(vd.Data); err != nil {
			return nil, err
		}
	}
	
	return buf.Bytes(), nil
}

// unmarshalVersionedData deserializes versioned data
func unmarshalVersionedData(data []byte) (*VersionedData, error) {
	if len(data) < 24 { // Minimum size for header (8+8+4+4)
		return nil, fmt.Errorf("data too short to be valid versioned data")
	}
	
	buf := bytes.NewReader(data)
	vd := &VersionedData{}
	
	// Read version
	if err := binary.Read(buf, binary.LittleEndian, &vd.Version); err != nil {
		return nil, err
	}
	
	// Read timestamp
	var unixNano int64
	if err := binary.Read(buf, binary.LittleEndian, &unixNano); err != nil {
		return nil, err
	}
	vd.Timestamp = time.Unix(0, unixNano)
	
	// Read source ID
	var sourceIDLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &sourceIDLen); err != nil {
		return nil, err
	}
	if sourceIDLen > 0 {
		sourceIDBytes := make([]byte, sourceIDLen)
		if _, err := buf.Read(sourceIDBytes); err != nil {
			return nil, err
		}
		vd.SourceID = string(sourceIDBytes)
	}
	
	// Read checksum
	var checksumLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &checksumLen); err != nil {
		return nil, err
	}
	if checksumLen > 0 {
		vd.Checksum = make([]byte, checksumLen)
		if _, err := buf.Read(vd.Checksum); err != nil {
			return nil, err
		}
	}
	
	// Read data
	var dataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return nil, err
	}
	if dataLen > 0 {
		vd.Data = make([]byte, dataLen)
		if _, err := buf.Read(vd.Data); err != nil {
			return nil, err
		}
	}
	
	return vd, nil
}

// Interface for replica storage access
type ReplicaStoreInterface interface {
	// GetReplicaNodes returns the node IDs that have a replica of the key
	GetReplicaNodes(key string) ([]string, error)
	// GetFromReplica gets data from a specific replica node
	GetFromReplica(ctx context.Context, key string, nodeID string) ([]byte, error)
	// UpdateReplica updates data on a specific replica node
	UpdateReplica(ctx context.Context, key string, data []byte, nodeID string) error
} 