package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/internal/proto"
)

// ReplicaInfo contains information about a stored replica
type ReplicaInfo struct {
	// NodeID is the ID of the node storing the replica
	NodeID node.NodeID

	// LastUpdated is when the replica was last updated
	LastUpdated time.Time

	// Path is the path to reach this replica
	Path []node.NodeID

	// HopCount is how many hops away this replica is
	HopCount int

	// ExpiresAt is when this replica should expire (TTL)
	ExpiresAt time.Time

	// AccessCount tracks how many times this replica has been accessed
	AccessCount uint32

	// IsFrequencyBased indicates if this replica was created due to high access frequency
	IsFrequencyBased bool
}

// ReplicaTracker tracks where keys are replicated
type ReplicaTracker struct {
	// KeyReplicas maps keys to the nodes holding replicas
	KeyReplicas map[string]map[node.NodeID]*ReplicaInfo

	// HotKeyReplicas tracks replicas created specifically for hot keys
	HotKeyReplicas map[string]map[node.NodeID]*ReplicaInfo

	// LastSync is when we last synced replica metadata
	LastSync time.Time

	mu sync.RWMutex
}

// NewReplicaTracker creates a new replica tracker
func NewReplicaTracker() *ReplicaTracker {
	return &ReplicaTracker{
		KeyReplicas:      make(map[string]map[node.NodeID]*ReplicaInfo),
		HotKeyReplicas:   make(map[string]map[node.NodeID]*ReplicaInfo),
		LastSync:         time.Now(),
	}
}

// AddReplica records that a key has been replicated to a node
func (rt *ReplicaTracker) AddReplica(key string, nodeID node.NodeID, path []node.NodeID) {
	rt.AddReplicaWithOptions(key, nodeID, path, time.Time{}, false)
}

// AddReplicaWithOptions adds a replica with additional options like expiration and frequency-based flags
func (rt *ReplicaTracker) AddReplicaWithOptions(key string, nodeID node.NodeID, path []node.NodeID, expiresAt time.Time, isFrequencyBased bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Standard replicas
	if !isFrequencyBased {
		if _, exists := rt.KeyReplicas[key]; !exists {
			rt.KeyReplicas[key] = make(map[node.NodeID]*ReplicaInfo)
		}

		rt.KeyReplicas[key][nodeID] = &ReplicaInfo{
			NodeID:          nodeID,
			LastUpdated:     time.Now(),
			Path:            path,
			HopCount:        len(path),
			ExpiresAt:       expiresAt,
			AccessCount:     0,
			IsFrequencyBased: false,
		}
	} else {
		// Frequency-based hot key replicas
		if _, exists := rt.HotKeyReplicas[key]; !exists {
			rt.HotKeyReplicas[key] = make(map[node.NodeID]*ReplicaInfo)
		}

		rt.HotKeyReplicas[key][nodeID] = &ReplicaInfo{
			NodeID:          nodeID,
			LastUpdated:     time.Now(),
			Path:            path,
			HopCount:        len(path),
			ExpiresAt:       expiresAt,
			AccessCount:     0,
			IsFrequencyBased: true,
		}
	}
}

// RemoveReplica removes a replica record
func (rt *ReplicaTracker) RemoveReplica(key string, nodeID node.NodeID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Check standard replicas
	if replicas, exists := rt.KeyReplicas[key]; exists {
		delete(replicas, nodeID)
		if len(replicas) == 0 {
			delete(rt.KeyReplicas, key)
		}
	}

	// Also check hot key replicas
	if replicas, exists := rt.HotKeyReplicas[key]; exists {
		delete(replicas, nodeID)
		if len(replicas) == 0 {
			delete(rt.HotKeyReplicas, key)
		}
	}
}

// GetReplicas returns all nodes that have a replica of a key
func (rt *ReplicaTracker) GetReplicas(key string) map[node.NodeID]*ReplicaInfo {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	result := make(map[node.NodeID]*ReplicaInfo)

	// Add standard replicas
	if replicas, exists := rt.KeyReplicas[key]; exists {
		for id, info := range replicas {
			// Create a copy of the replica info to avoid concurrent modification issues
			replica := *info
			result[id] = &replica
		}
	}

	// Add hot key replicas
	if replicas, exists := rt.HotKeyReplicas[key]; exists {
		for id, info := range replicas {
			// Skip if already added from standard replicas
			if _, exists := result[id]; exists {
				continue
			}
			// Create a copy of the replica info
			replica := *info
			result[id] = &replica
		}
	}

	return result
}

// GetAllMetadata returns all replica metadata
func (rt *ReplicaTracker) GetAllMetadata() map[string]map[node.NodeID]*ReplicaInfo {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	// Create a deep copy
	result := make(map[string]map[node.NodeID]*ReplicaInfo)
	for key, replicas := range rt.KeyReplicas {
		result[key] = make(map[node.NodeID]*ReplicaInfo, len(replicas))
		for id, info := range replicas {
			replica := *info // Make a copy
			result[key][id] = &replica
		}
	}

	return result
}

// ReplicateData replicates a key-value pair to neighbors with multi-hop support
func (s *CANServer) ReplicateData(ctx context.Context, key string, value []byte) error {
	// If replication is disabled, do nothing
	if s.Config.ReplicationFactor <= 1 {
		return nil
	}

	// Initialize the replica tracker if not already done
	if s.ReplicaTracker == nil {
		s.ReplicaTracker = NewReplicaTracker()
	}

	// Perform multi-hop replication if enabled
	if s.Config.MaxReplicationHops > 1 {
		return s.replicateDataMultiHop(ctx, key, value, 1, s.Config.MaxReplicationHops, []node.NodeID{}, s.Node.ID)
	}

	// Otherwise, fall back to the original neighbor-only replication
	return s.replicateDataToNeighbors(ctx, key, value)
}

// replicateDataToNeighbors handles the original neighbor-only replication
func (s *CANServer) replicateDataToNeighbors(ctx context.Context, key string, value []byte) error {
	// Get the list of neighbors
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	if len(neighbors) == 0 {
		// No neighbors to replicate to
		return nil
	}

	// We want to replicate to (replicationFactor - 1) neighbors
	// because the primary copy is already stored on this node
	replicasNeeded := s.Config.ReplicationFactor - 1

	// If we have fewer neighbors than replicationFactor - 1,
	// we'll replicate to all available neighbors
	replicasNeeded = min(replicasNeeded, len(neighbors))

	// Count successful replications
	successCount := 0

	// Try to replicate to each neighbor
	for _, neighbor := range neighbors {
		// Skip if we already have enough replicas
		if successCount >= replicasNeeded {
			break
		}

		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for replication: %v", neighbor.ID, err)
			continue
		}

		// Create a special put request for replication
		// The 'forward' flag is set to true so the neighbor will store it locally
		// even if it's not responsible for the key's zone
		req := &pb.PutRequest{
			Key:     key,
			Value:   value,
			Forward: true,
		}

		// Send the replication request
		resp, err := client.Put(ctx, req)
		conn.Close()

		if err != nil || !resp.Success {
			log.Printf("Failed to replicate to neighbor %s: %v", neighbor.ID, err)
			continue
		}

		// Record successful replication
		s.ReplicaTracker.AddReplica(key, neighbor.ID, []node.NodeID{neighbor.ID})
		successCount++
	}

	// Log if we couldn't achieve the desired replication factor
	if successCount < replicasNeeded {
		log.Printf("Warning: Could only replicate key %s to %d nodes (wanted %d)", key, successCount, replicasNeeded)
	}

	return nil
}

// replicateDataMultiHop handles multi-hop replication
func (s *CANServer) replicateDataMultiHop(ctx context.Context, key string, value []byte, 
	currentHop int, maxHops int, path []node.NodeID, originNodeID node.NodeID) error {
	
	log.Printf("Performing multi-hop replication for key %s (hop %d/%d)", key, currentHop, maxHops)

	// First, select the best nodes to replicate to at this hop level
	targets := s.selectReplicationTargets(key, s.Config.ReplicationFactor)
	if len(targets) == 0 {
		log.Printf("No suitable replication targets found for key %s at hop %d", key, currentHop)
		return nil
	}

	// Create a path that includes this node
	updatedPath := append(path, s.Node.ID)

	// Track successful replications
	successCount := 0
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Replicate to each target
	for _, target := range targets {
		wg.Add(1)
		go func(targetID node.NodeID, targetInfo struct {
			Address  string
			HopCount int
			Path     []node.NodeID
		}) {
			defer wg.Done()

			// Skip if this node is already in the path (avoid cycles)
			for _, nodeID := range updatedPath {
				if nodeID == targetID {
					return
				}
			}

			// Connect to the target
			client, conn, err := ConnectToNode(ctx, targetInfo.Address)
			if err != nil {
				log.Printf("Failed to connect to replication target %s: %v", targetID, err)
				return
			}
			defer conn.Close()

			// Convert path to strings for the protobuf message
			stringPath := make([]string, len(updatedPath))
			for i, id := range updatedPath {
				stringPath[i] = string(id)
			}

			// Create a replication request
			req := &pb.ReplicatePutRequest{
				Key:                key,
				Value:              value,
				ReplicationHop:     int32(currentHop),
				MaxReplicationHops: int32(maxHops),
				Path:               stringPath,
				OriginNodeId:       string(originNodeID),
			}

			// Send the replication request
			resp, err := client.ReplicatePut(ctx, req)
			if err != nil || !resp.Success {
				log.Printf("Failed to replicate to target %s: %v", targetID, err)
				return
			}

			// Record successful replication
			targetPath := append(updatedPath, targetID)
			s.ReplicaTracker.AddReplica(key, targetID, targetPath)

			// Count successful replications
			mu.Lock()
			successCount++
			mu.Unlock()
		}(target, s.getNodeInfo(target))
	}

	// Wait for all replication attempts to complete
	wg.Wait()

	log.Printf("Successfully replicated key %s to %d/%d targets at hop %d", key, successCount, len(targets), currentHop)
	return nil
}

// getNodeInfo returns info about a node from either neighbors or extended neighbors
func (s *CANServer) getNodeInfo(nodeID node.NodeID) struct {
	Address  string
	HopCount int
	Path     []node.NodeID
} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check immediate neighbors first
	if nbr, exists := s.Node.Neighbors[nodeID]; exists {
		return struct {
			Address  string
			HopCount int
			Path     []node.NodeID
		}{
			Address:  nbr.Address,
			HopCount: 1,
			Path:     []node.NodeID{nodeID},
		}
	}

	// Check extended neighbors
	if extNbr, exists := s.Node.ExtendedNeighbors[nodeID]; exists {
		return struct {
			Address  string
			HopCount int
			Path     []node.NodeID
		}{
			Address:  extNbr.Address,
			HopCount: extNbr.HopCount,
			Path:     extNbr.Path,
		}
	}

	// Default (should not happen if node is in targets)
	return struct {
		Address  string
		HopCount int
		Path     []node.NodeID
	}{
		Address:  "",
		HopCount: 0,
		Path:     []node.NodeID{},
	}
}

// selectReplicationTargets selects the best nodes for replication
func (s *CANServer) selectReplicationTargets(key string, count int) []node.NodeID {
	// Get all available nodes (both neighbors and extended neighbors)
	allNodes := s.GetExtendedNeighbors()
	if len(allNodes) == 0 {
		return []node.NodeID{}
	}

	// Calculate node scores based on criteria like hop distance, coverage, etc.
	nodeScores := make(map[node.NodeID]float64)
	for nodeID, info := range allNodes {
		// Simple scoring - inverse of hop count (closer nodes score higher)
		nodeScores[nodeID] = 1.0 / float64(info.HopCount)
	}

	// Sort nodes by score and select the top ones
	selectedNodes := make([]node.NodeID, 0, count)
	for len(selectedNodes) < count && len(nodeScores) > 0 {
		// Find the node with the highest score
		var bestNode node.NodeID
		bestScore := -1.0

		for nodeID, score := range nodeScores {
			if score > bestScore {
				bestScore = score
				bestNode = nodeID
			}
		}

		if bestScore > 0 {
			selectedNodes = append(selectedNodes, bestNode)
			delete(nodeScores, bestNode)
		} else {
			break
		}
	}

	return selectedNodes
}

// MaintainReplicationFactor ensures each key has the required number of replicas
func (s *CANServer) MaintainReplicationFactor(ctx context.Context) error {
	// Skip if replication is disabled
	if s.Config.ReplicationFactor <= 1 {
		return nil
	}

	// Get all keys we own
	keys, err := s.Store.GetAllKeys()
	if err != nil {
		return err
	}

	log.Printf("Checking replication for %d keys", len(keys))

	// Check each key
	for _, key := range keys {
		// Get current replicas for this key
		replicas := s.ReplicaTracker.GetReplicas(key)
		
		// Check if we need more replicas
		if len(replicas) < s.Config.ReplicationFactor-1 { // -1 because we own one copy
			// Get the value
			value, exists, err := s.Store.Get(key)
			if err != nil || !exists {
				log.Printf("Failed to get key %s for replication maintenance: %v", key, err)
				continue
			}

			// Replicate to more nodes
			if err := s.ReplicateData(ctx, key, value); err != nil {
				log.Printf("Failed to maintain replication factor for key %s: %v", key, err)
			}
		}
	}

	return nil
}

// RecoverData recovers data from replicas when a node fails
func (s *CANServer) RecoverData(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone) error {
	// Get keys that might have been on the failed node
	// For all keys we own, check if the failed node would have been responsible for them
	keysToCheck := make([]string, 0)

	s.mu.RLock()
	for key := range s.Node.Data {
		point := s.Router.HashToPoint(key)
		if failedZone.Contains(point) {
			keysToCheck = append(keysToCheck, key)
		}
	}
	s.mu.RUnlock()

	// Also check for keys that were replicated on the failed node
	if s.ReplicaTracker != nil {
		s.ReplicaTracker.mu.RLock()
		for key, replicas := range s.ReplicaTracker.KeyReplicas {
			if _, wasFailed := replicas[failedNodeID]; wasFailed {
				if !contains(keysToCheck, key) {
					keysToCheck = append(keysToCheck, key)
				}
			}
		}
		s.ReplicaTracker.mu.RUnlock()
	}

	log.Printf("Recovering %d keys from failed node %s", len(keysToCheck), failedNodeID)

	// Check both neighbors and extended neighbors for replicas
	allNodes := s.GetExtendedNeighbors()

	// For each key that the failed node was responsible for
	for _, key := range keysToCheck {
		// Check if any node has a replica
		for nodeID, info := range allNodes {
			// Skip the failed node
			if nodeID == failedNodeID {
				continue
			}

			// Connect to the node
			client, conn, err := ConnectToNode(ctx, info.Address)
			if err != nil {
				log.Printf("Failed to connect to node %s for recovery: %v", nodeID, err)
				continue
			}

			// Try to get the key from the node
			req := &pb.GetRequest{
				Key:     key,
				Forward: true, // Force the node to check locally
			}

			resp, err := client.Get(ctx, req)
			conn.Close()

			if err != nil || !resp.Success || !resp.Exists {
				// This node doesn't have the key
				continue
			}

			// Found a replica, update our copy
			s.mu.Lock()
			s.Node.Data[key] = string(resp.Value)
			s.mu.Unlock()

			log.Printf("Recovered key %s from node %s", key, nodeID)

			// Re-replicate if needed to maintain replication factor
			s.ReplicateData(ctx, key, resp.Value)

			// Break out of the node loop once we've found a replica
			break
		}
	}

	return nil
}

// UpdatePutOperation updates the normal Put operation to include replication
func (s *CANServer) UpdatedPutWithReplication(ctx context.Context, key string, value []byte) error {
	// First store locally
	if err := s.Store.Put(key, value); err != nil {
		return err
	}

	// Then replicate to other nodes
	return s.ReplicateData(ctx, key, value)
}

// UpdateDeleteOperation updates the normal Delete operation to include replication
func (s *CANServer) UpdatedDeleteWithReplication(ctx context.Context, key string) error {
	// Delete locally
	if err := s.Store.Delete(key); err != nil {
		return err
	}

	// Delete from replicas using multi-hop replication if enabled
	if s.Config.MaxReplicationHops > 1 && s.ReplicaTracker != nil {
		// Get all replicas for this key
		replicas := s.ReplicaTracker.GetReplicas(key)
		
		for nodeID, info := range replicas {
			// Connect to the replica node
			nodeInfo := s.getNodeInfo(nodeID)
			if nodeInfo.Address == "" {
				continue
			}

			client, conn, err := ConnectToNode(ctx, nodeInfo.Address)
			if err != nil {
				log.Printf("Failed to connect to replica %s for delete: %v", nodeID, err)
				continue
			}

			// Create string path for the protobuf message
			stringPath := make([]string, len(info.Path))
			for i, id := range info.Path {
				stringPath[i] = string(id)
			}

			// Create a replicate delete request
			req := &pb.ReplicateDeleteRequest{
				Key:                key,
				ReplicationHop:     1,
				MaxReplicationHops: int32(s.Config.MaxReplicationHops),
				Path:               stringPath,
				OriginNodeId:       string(s.Node.ID),
			}

			// Send the request
			_, err = client.ReplicateDelete(ctx, req)
			conn.Close()

			if err != nil {
				log.Printf("Failed to delete replica on node %s: %v", nodeID, err)
			} else {
				// Remove from our replica tracking
				s.ReplicaTracker.RemoveReplica(key, nodeID)
			}
		}
		
		return nil
	}

	// Fall back to the original neighbor-only deletion if multi-hop is disabled
	neighbors := s.Node.GetNeighbors()
	for _, neighbor := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for delete replication: %v", neighbor.ID, err)
			continue
		}

		// Create a delete request
		req := &pb.DeleteRequest{
			Key:     key,
			Forward: true, // Force the neighbor to check locally
		}

		_, err = client.Delete(ctx, req)
		conn.Close()

		if err != nil {
			log.Printf("Failed to delete replica on neighbor %s: %v", neighbor.ID, err)
		}
	}

	return nil
}

// SyncReplicaMetadata synchronizes replica metadata with other nodes
func (s *CANServer) SyncReplicaMetadata(ctx context.Context) error {
	// Skip if replica tracker isn't initialized
	if s.ReplicaTracker == nil {
		return nil
	}

	// Get all our metadata
	metadata := s.ReplicaTracker.GetAllMetadata()
	
	// Convert to protobuf format
	entries := make([]*pb.ReplicaMetadataEntry, 0)
	for key, replicas := range metadata {
		// Convert node IDs to strings
		replicaNodes := make([]string, 0, len(replicas))
		for nodeID := range replicas {
			replicaNodes = append(replicaNodes, string(nodeID))
		}
		
		entries = append(entries, &pb.ReplicaMetadataEntry{
			Key:           key,
			ReplicaNodes:  replicaNodes,
			Version:       0, // Version not currently used
			LastUpdateTime: time.Now().UnixNano(),
		})
	}
	
	// Send metadata to immediate neighbors
	neighbors := s.Node.GetNeighbors()
	for _, neighbor := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for metadata sync: %v", neighbor.ID, err)
			continue
		}
		
		// Create sync request
		req := &pb.SyncReplicaMetadataRequest{
			NodeId:  string(s.Node.ID),
			Entries: entries,
		}
		
		// Send request
		_, err = client.SyncReplicaMetadata(ctx, req)
		conn.Close()
		
		if err != nil {
			log.Printf("Failed to sync replica metadata with neighbor %s: %v", neighbor.ID, err)
		}
	}
	
	s.ReplicaTracker.LastSync = time.Now()
	return nil
}

// startReplicaMaintenance starts periodic replica maintenance
func (s *CANServer) startReplicaMaintenance() {
	// Skip if maintenance is disabled
	if s.Config.ReplicaRepairInterval <= 0 || s.Config.ReplicationFactor <= 1 {
		return
	}

	ticker := time.NewTicker(s.Config.ReplicaRepairInterval)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), s.Config.ReplicaRepairInterval/2)
		
		// Maintain replication factor
		err := s.MaintainReplicationFactor(ctx)
		if err != nil {
			log.Printf("Replica maintenance error: %v", err)
		}
		
		// Sync metadata
		err = s.SyncReplicaMetadata(ctx)
		if err != nil {
			log.Printf("Replica metadata sync error: %v", err)
		}
		
		cancel()
	}
}

// TakeOverZone handles taking over a zone from a failed node
func (s *CANServer) TakeOverZone(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone) error {
	// If this is called directly, we first check if we're the best node to take over
	bestNodeID, found := s.selectTakeoverNeighbor(failedNodeID, failedZone)
	if !found || bestNodeID != s.Node.ID {
		// We're not the best node to take over, so we don't
		log.Printf("Not taking over zone for node %s (best node: %s)", failedNodeID, bestNodeID)
		return nil
	}
	
	log.Printf("Selected as best node to take over zone for failed node %s", failedNodeID)
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Check if the combined zone would be too large
	combinedVolume := calculateZoneVolume(s.Node.Zone) + calculateZoneVolume(failedZone)
	avgZoneVolume := s.estimateAverageZoneVolume()
	
	// If the combined volume would be more than twice the average,
	// consider splitting the zone
	if combinedVolume > avgZoneVolume*2.0 {
		log.Printf("Combined zone would be too large, splitting zone")
		
		// Split the failed zone optimally
		myNewZone, remainingZone, err := node.OptimallySplitMergedZone(s.Node.Zone, failedZone)
		if err != nil {
			log.Printf("Failed to split zone optimally: %v, falling back to simple merge", err)
			// Fall back to simple merge
			myNewZone, err = node.MergeZones(s.Node.Zone, failedZone)
			if err != nil {
				return fmt.Errorf("failed to merge zones: %w", err)
			}
			s.Node.Zone = myNewZone
		} else {
			// Successfully split the zone
			s.Node.Zone = myNewZone
			
			// Find another neighbor to take the remaining zone
			secondNeighborID := s.selectBackupNeighbor(failedNodeID, failedZone, s.Node.ID)
			if secondNeighborID != "" {
				// Notify this neighbor to take over the remaining zone
				go func() {
					err := s.notifyZoneTakeover(context.Background(), secondNeighborID, remainingZone)
					if err != nil {
						log.Printf("Failed to notify neighbor %s to take over zone: %v", secondNeighborID, err)
					}
				}()
			} else {
				// No suitable second neighbor, we take the whole zone
				log.Printf("No suitable second neighbor found, taking the whole zone")
				myNewZone, err = node.MergeZones(s.Node.Zone, failedZone)
				if err != nil {
					return fmt.Errorf("failed to merge zones: %w", err)
				}
				s.Node.Zone = myNewZone
			}
		}
	} else {
		// The combined zone is not too large, use the simple merge
		log.Printf("Using simple merge for zone takeover")
		myNewZone, err := node.MergeZones(s.Node.Zone, failedZone)
		if err != nil {
			return fmt.Errorf("failed to merge zones: %w", err)
		}
		s.Node.Zone = myNewZone
	}
	
	// Update replica tracker to remove failed node
	if s.ReplicaTracker != nil {
		allMetadata := s.ReplicaTracker.GetAllMetadata()
		for key, replicas := range allMetadata {
			if _, exists := replicas[failedNodeID]; exists {
				s.ReplicaTracker.RemoveReplica(key, failedNodeID)
			}
		}
	}

	// Recover data for which the failed node was responsible
	if err := s.RecoverData(ctx, failedNodeID, failedZone); err != nil {
		log.Printf("Warning: Error recovering data from failed node %s: %v", failedNodeID, err)
	}

	// Notify neighbors about our new zone
	s.NotifyNeighborsOfZoneChange(ctx)

	return nil
}

// NotifyNeighborsOfZoneChange notifies neighbors about a zone change
func (s *CANServer) NotifyNeighborsOfZoneChange(ctx context.Context) {
	neighbors := s.Node.GetNeighbors()

	for _, neighbor := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for zone change notification: %v", neighbor.ID, err)
			continue
		}

		// Send update neighbors request with our updated info
		updateReq := &pb.UpdateNeighborsRequest{
			NodeId: string(s.Node.ID),
			Neighbors: []*pb.NodeInfo{
				{
					Id:      string(s.Node.ID),
					Address: s.Node.Address,
					Zone: &pb.Zone{
						MinPoint: &pb.Point{
							Coordinates: s.Node.Zone.MinPoint,
						},
						MaxPoint: &pb.Point{
							Coordinates: s.Node.Zone.MaxPoint,
						},
					},
				},
			},
		}

		_, err = client.UpdateNeighbors(ctx, updateReq)
		conn.Close()

		if err != nil {
			log.Printf("Failed to notify neighbor %s of zone change: %v", neighbor.ID, err)
		}
	}
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func contains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

// GetAllReplicas returns all replica information, both standard and hot key replicas
func (rt *ReplicaTracker) GetAllReplicas() (map[string]map[node.NodeID]*ReplicaInfo, map[string]map[node.NodeID]*ReplicaInfo) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	// Create deep copies of both maps
	standardReplicas := make(map[string]map[node.NodeID]*ReplicaInfo)
	for key, replicas := range rt.KeyReplicas {
		standardReplicas[key] = make(map[node.NodeID]*ReplicaInfo, len(replicas))
		for id, info := range replicas {
			replica := *info // Make a copy
			standardReplicas[key][id] = &replica
		}
	}

	hotKeyReplicas := make(map[string]map[node.NodeID]*ReplicaInfo)
	for key, replicas := range rt.HotKeyReplicas {
		hotKeyReplicas[key] = make(map[node.NodeID]*ReplicaInfo, len(replicas))
		for id, info := range replicas {
			replica := *info // Make a copy
			hotKeyReplicas[key][id] = &replica
		}
	}

	return standardReplicas, hotKeyReplicas
}

// RecordReplicaAccess records an access to a replica
func (rt *ReplicaTracker) RecordReplicaAccess(key string, nodeID node.NodeID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Check standard replicas
	if replicas, exists := rt.KeyReplicas[key]; exists {
		if info, found := replicas[nodeID]; found {
			info.AccessCount++
			return
		}
	}

	// Check hot key replicas
	if replicas, exists := rt.HotKeyReplicas[key]; exists {
		if info, found := replicas[nodeID]; found {
			info.AccessCount++
			return
		}
	}
}
