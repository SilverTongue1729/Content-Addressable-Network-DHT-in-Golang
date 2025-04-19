package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/can-dht/pkg/integrity"
	"github.com/can-dht/pkg/node"
)

// IntegrityManager adapts the CANServer to work with the enhanced integrity system
type IntegrityManager struct {
	// The underlying enhanced integrity manager
	manager *integrity.EnhancedIntegrityManager
	
	// Reference to the CAN server for access to replica data
	server *CANServer
	
	// Regular integrity checker for backward compatibility
	basicChecker *integrity.PeriodicChecker
	
	// Corruption handler function
	onCorruptionDetected func(key string, err error)
	
	// Check completion handler
	onCheckComplete func(success bool, count int)
}

// NewIntegrityManager creates a new IntegrityManager
func NewIntegrityManager(server *CANServer) (*IntegrityManager, error) {
	// Create replica store adapter
	replicaStore := &CANReplicaStoreAdapter{
		server: server,
	}
	
	// Create data store adapter
	dataStore := &CANDataStoreAdapter{
		server: server,
	}
	
	// Create enhanced integrity manager
	enhancedManager := integrity.NewEnhancedIntegrityManager(dataStore, replicaStore)
	
	return &IntegrityManager{
		manager: enhancedManager,
		server:  server,
	}, nil
}

// SetCorruptionHandler sets the handler for corruption detection
func (im *IntegrityManager) SetCorruptionHandler(handler func(key string, err error)) {
	im.onCorruptionDetected = handler
}

// SetCheckCompleteHandler sets the handler for check completion
func (im *IntegrityManager) SetCheckCompleteHandler(handler func(success bool, count int)) {
	im.onCheckComplete = handler
}

// CheckIntegrity performs an integrity check on a specific key
func (im *IntegrityManager) CheckIntegrity(ctx context.Context, key string) error {
	err := im.manager.CheckAndResolve(ctx, key)
	if err != nil {
		if im.onCorruptionDetected != nil {
			im.onCorruptionDetected(key, err)
		}
		return err
	}
	return nil
}

// PerformPeriodicChecks starts periodic integrity checks
func (im *IntegrityManager) PerformPeriodicChecks(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("Starting periodic integrity check")
			im.performFullIntegrityCheck(ctx)
		}
	}
}

// performFullIntegrityCheck checks the integrity of all keys
func (im *IntegrityManager) performFullIntegrityCheck(ctx context.Context) {
	// Get all keys from the server
	im.server.mu.RLock()
	keys := make([]string, 0, len(im.server.Node.Data))
	for key := range im.server.Node.Data {
		keys = append(keys, key)
	}
	im.server.mu.RUnlock()
	
	// Check each key
	corruptedCount := 0
	for _, key := range keys {
		err := im.CheckIntegrity(ctx, key)
		if err != nil {
			log.Printf("Integrity check failed for key %s: %v", key, err)
			corruptedCount++
		}
	}
	
	// Report completion
	if im.onCheckComplete != nil {
		success := corruptedCount == 0
		im.onCheckComplete(success, len(keys))
	}
	
	log.Printf("Integrity check complete: %d/%d keys corrupted", corruptedCount, len(keys))
}

// Use fallback basic checker if needed
func (im *IntegrityManager) UseFallbackChecker(ctx context.Context, interval time.Duration) {
	if im.basicChecker == nil {
		// Create basic checker
		hashVerifier := integrity.NewHashVerifier()
		
		im.basicChecker = integrity.NewPeriodicChecker(
			im.server.Store, 
			hashVerifier, 
			interval,
		)
		
		// Set handler functions
		if im.onCorruptionDetected != nil {
			im.basicChecker.OnCorruptionDetected = im.onCorruptionDetected
		}
		
		if im.onCheckComplete != nil {
			im.basicChecker.OnCheckComplete = im.onCheckComplete
		}
	}
	
	// Start the basic checker
	im.basicChecker.Start()
}

// CANDataStoreAdapter adapts CANServer to the DataStoreInterface
type CANDataStoreAdapter struct {
	server *CANServer
}

func (a *CANDataStoreAdapter) Get(key string) ([]byte, error) {
	a.server.mu.RLock()
	defer a.server.mu.RUnlock()
	
	data, exists := a.server.Node.Data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	
	return []byte(data), nil
}

func (a *CANDataStoreAdapter) Put(key string, value []byte) error {
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	
	a.server.Node.Data[key] = string(value)
	return nil
}

// CANReplicaStoreAdapter adapts CANServer to the ReplicaStoreInterface
type CANReplicaStoreAdapter struct {
	server *CANServer
}

func (a *CANReplicaStoreAdapter) GetReplicaNodes(key string) ([]string, error) {
	if a.server.ReplicaTracker == nil {
		return nil, fmt.Errorf("replica tracker not initialized")
	}
	
	replicas, err := a.server.ReplicaTracker.GetReplicasForKey(key)
	if err != nil {
		return nil, err
	}
	
	// Convert node.NodeID to string
	result := make([]string, 0, len(replicas))
	for _, nodeID := range replicas {
		result = append(result, string(nodeID))
	}
	
	return result, nil
}

func (a *CANReplicaStoreAdapter) GetFromReplica(ctx context.Context, key string, nodeID string) ([]byte, error) {
	// Find the node address
	a.server.mu.RLock()
	neighbor, exists := a.server.Node.Neighbors[node.NodeID(nodeID)]
	a.server.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("neighbor not found: %s", nodeID)
	}
	
	// Connect to the node
	client, conn, err := a.server.ConnectToNode(ctx, neighbor.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to replica: %w", err)
	}
	defer conn.Close()
	
	// Make a get request
	req := &pb.GetRequest{
		Key:     key,
		Forward: false, // Direct access to the replica
	}
	
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get data from replica: %w", err)
	}
	
	if !resp.Success {
		return nil, fmt.Errorf("replica returned failure: %s", resp.Error)
	}
	
	return resp.Value, nil
}

func (a *CANReplicaStoreAdapter) UpdateReplica(ctx context.Context, key string, data []byte, nodeID string) error {
	// Find the node address
	a.server.mu.RLock()
	neighbor, exists := a.server.Node.Neighbors[node.NodeID(nodeID)]
	a.server.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("neighbor not found: %s", nodeID)
	}
	
	// Connect to the node
	client, conn, err := a.server.ConnectToNode(ctx, neighbor.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to replica: %w", err)
	}
	defer conn.Close()
	
	// Make a replication put request
	req := &pb.ReplicatePutRequest{
		Key:   key,
		Value: data,
		// We're replaying a resolved conflict
		IsConflictResolution: true,
	}
	
	resp, err := client.ReplicatePut(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to update replica: %w", err)
	}
	
	if !resp.Success {
		return fmt.Errorf("replica update failed: %s", resp.Error)
	}
	
	return nil
} 