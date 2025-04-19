package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/proto"
)

// ReplicaInfo contains information about a stored replica
type ReplicaInfo struct {
	// NodeID is the ID of the node storing the replica
	NodeID node.NodeID

	// LastUpdated is when the replica was last updated
	LastUpdated time.Time
}

// ReplicateData replicates a key-value pair to neighbors
func (s *CANServer) ReplicateData(ctx context.Context, key string, value []byte) error {
	// If replication is disabled, do nothing
	if s.Config.ReplicationFactor <= 1 {
		return nil
	}

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

		successCount++
	}

	// Log if we couldn't achieve the desired replication factor
	if successCount < replicasNeeded {
		log.Printf("Warning: Could only replicate key %s to %d nodes (wanted %d)", key, successCount, replicasNeeded)
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

	// Check all neighbors to see if they have replicas of these keys
	neighbors := s.Node.GetNeighbors()

	// For each key that the failed node was responsible for
	for _, key := range keysToCheck {
		// Check if any neighbor has a replica
		for _, neighbor := range neighbors {
			// Skip the failed node
			if neighbor.ID == failedNodeID {
				continue
			}

			// Connect to the neighbor
			client, conn, err := ConnectToNode(ctx, neighbor.Address)
			if err != nil {
				log.Printf("Failed to connect to neighbor %s for recovery: %v", neighbor.ID, err)
				continue
			}

			// Try to get the key from the neighbor
			req := &pb.GetRequest{
				Key:     key,
				Forward: true, // Force the neighbor to check locally
			}

			resp, err := client.Get(ctx, req)
			conn.Close()

			if err != nil || !resp.Success || !resp.Exists {
				// This neighbor doesn't have the key
				continue
			}

			// Found a replica, update our copy
			s.mu.Lock()
			s.Node.Data[key] = string(resp.Value)
			s.mu.Unlock()

			// Break out of the neighbor loop once we've found a replica
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

	// Then replicate to neighbors
	return s.ReplicateData(ctx, key, value)
}

// UpdateDeleteOperation updates the normal Delete operation to include replication
func (s *CANServer) UpdatedDeleteWithReplication(ctx context.Context, key string) error {
	// Delete locally
	if err := s.Store.Delete(key); err != nil {
		return err
	}

	// Delete from replicas
	// This is a simplification - we're just sending a delete to all neighbors
	// and letting them handle it accordingly
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

// TakeOverZone handles taking over a zone from a failed node
func (s *CANServer) TakeOverZone(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone) error {
	// According to the CAN paper, we should merge the zone with the neighbor that has the
	// smallest zone volume, to maintain load balance

	// First, check if we're the appropriate node to take over this zone
	// This avoids multiple nodes trying to take over simultaneously
	s.mu.RLock()

	// Calculate our zone volume
	ourVolume := calculateZoneVolume(s.Node.Zone)

	// Check if we're the smallest neighbor
	isSmallestNeighbor := true
	for nodeID, nbrInfo := range s.Node.GetNeighbors() {
		// Skip the failed node
		if nodeID == failedNodeID {
			continue
		}

		// Skip nodes that aren't neighbors of the failed node
		if !checkZonesAdjacent(nbrInfo.Zone, failedZone, s.Config.Dimensions) {
			continue
		}

		// Calculate neighbor volume
		nbrVolume := calculateZoneVolume(nbrInfo.Zone)

		// If a neighbor has a smaller zone, we shouldn't take over
		if nbrVolume < ourVolume {
			isSmallestNeighbor = false
			break
		}
	}
	s.mu.RUnlock()

	// If we're not the smallest neighbor, don't take over
	if !isSmallestNeighbor {
		log.Printf("Not taking over zone for node %s as we're not the smallest neighbor", failedNodeID)
		return nil
	}

	// Now take the write lock for the actual takeover
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Taking over zone from failed node %s", failedNodeID)

	// 1. Check if it's possible to merge the zones
	// To be mergeable, the combined zone must still be a hyperrectangle
	isMergeable, mergedZone := tryMergeZones(s.Node.Zone, failedZone)

	if !isMergeable {
		log.Printf("Cannot merge with failed node %s zone - not taking over", failedNodeID)
		return fmt.Errorf("zones cannot be merged into a hyperrectangle")
	}

	// 2. Update our zone to the merged zone
	s.Node.Zone = mergedZone

	// 3. Recover data for which the failed node was responsible
	if err := s.RecoverData(ctx, failedNodeID, failedZone); err != nil {
		log.Printf("Warning: Error recovering data from failed node %s: %v", failedNodeID, err)
		// Continue despite errors - better to have an incomplete takeover than none
	}

	// 4. Update our neighbor list
	// Remove the failed node
	s.Node.RemoveNeighbor(failedNodeID)

	// Add the failed node's neighbors as our neighbors (if they aren't already)
	// This requires communication with other nodes to discover the failed node's neighbors
	if err := s.discoverAndAddFailedNodesNeighbors(ctx, failedNodeID, failedZone); err != nil {
		log.Printf("Warning: Error discovering failed node's neighbors: %v", err)
	}

	// 5. Notify our neighbors about the zone change
	if err := s.NotifyNeighborsOfZoneChange(ctx); err != nil {
		log.Printf("Warning: Error notifying neighbors of zone change: %v", err)
	}

	log.Printf("Successfully took over zone for failed node %s", failedNodeID)
	return nil
}

// calculateZoneVolume calculates the volume (hypervolume) of a zone
func calculateZoneVolume(zone *node.Zone) float64 {
	if zone == nil {
		return 0
	}

	volume := 1.0
	for i := range zone.MinPoint {
		dimension := zone.MaxPoint[i] - zone.MinPoint[i]
		volume *= dimension
	}

	return volume
}

// tryMergeZones attempts to merge two zones and returns whether it's possible
// and the resulting merged zone if possible
func tryMergeZones(zone1, zone2 *node.Zone) (bool, *node.Zone) {
	if zone1 == nil || zone2 == nil {
		return false, nil
	}

	// Check if the zones can be merged into a hyperrectangle
	// This is only possible if they share a full (d-1)-dimensional face

	// Count dimensions where the zones are adjacent
	adjacentDimensions := 0

	// Check which dimension they're adjacent along
	adjacentDimension := -1

	// Check dimensions where they have matching coordinates
	matchingDimensions := 0

	for i := range zone1.MinPoint {
		// Check if they're adjacent along this dimension
		if (zone1.MaxPoint[i] == zone2.MinPoint[i]) || (zone1.MinPoint[i] == zone2.MaxPoint[i]) {
			adjacentDimensions++
			adjacentDimension = i
		}

		// Check if they have matching min/max coordinates in this dimension
		if (zone1.MinPoint[i] == zone2.MinPoint[i]) && (zone1.MaxPoint[i] == zone2.MaxPoint[i]) {
			matchingDimensions++
		}
	}

	// For zones to be mergeable into a hyperrectangle:
	// 1. They must be adjacent along exactly one dimension
	// 2. All other dimensions must have matching coordinates
	if adjacentDimensions != 1 || matchingDimensions != len(zone1.MinPoint)-1 {
		return false, nil
	}

	// Create the merged zone
	minPoint := make(node.Point, len(zone1.MinPoint))
	maxPoint := make(node.Point, len(zone1.MaxPoint))

	for i := range zone1.MinPoint {
		if i == adjacentDimension {
			// For the adjacent dimension, take the min of mins and max of maxes
			if zone1.MaxPoint[i] == zone2.MinPoint[i] {
				// zone1 is "before" zone2
				minPoint[i] = zone1.MinPoint[i]
				maxPoint[i] = zone2.MaxPoint[i]
			} else {
				// zone2 is "before" zone1
				minPoint[i] = zone2.MinPoint[i]
				maxPoint[i] = zone1.MaxPoint[i]
			}
		} else {
			// For other dimensions, they should be the same
			minPoint[i] = zone1.MinPoint[i]
			maxPoint[i] = zone1.MaxPoint[i]
		}
	}

	// Create the new merged zone
	mergedZone, err := node.NewZone(minPoint, maxPoint)
	if err != nil {
		return false, nil
	}

	return true, mergedZone
}

// discoverAndAddFailedNodesNeighbors attempts to discover and add the failed node's neighbors
func (s *CANServer) discoverAndAddFailedNodesNeighbors(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone) error {
	// Query all our current neighbors about their neighbors
	// Some of them might have been neighbors with the failed node

	// Track new potential neighbors
	potentialNeighbors := make(map[node.NodeID]*node.NeighborInfo)

	// Get our current neighbors
	ourNeighbors := s.Node.GetNeighbors()

	// Ask each neighbor about their neighbors
	for _, nbrInfo := range ourNeighbors {
		// Skip if this was the failed node
		if nbrInfo.ID == failedNodeID {
			continue
		}

		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, nbrInfo.Address)
		if err != nil {
			log.Printf("Warning: failed to connect to neighbor %s: %v", nbrInfo.ID, err)
			continue
		}

		// Query for their neighbors
		resp, err := client.FindNode(ctx, &pb.FindNodeRequest{
			Target: &pb.FindNodeRequest_Point{
				Point: &pb.Point{
					Coordinates: failedZone.MinPoint,
				},
			},
		})
		conn.Close()

		if err != nil {
			log.Printf("Warning: failed to query neighbor %s: %v", nbrInfo.ID, err)
			continue
		}

		// If this neighbor knows about the responsible node, add it to potential neighbors
		if !resp.IsResponsible && resp.ResponsibleNode != nil {
			nodeID := node.NodeID(resp.ResponsibleNode.Id)

			// Skip ourselves and already known neighbors
			if nodeID == s.Node.ID || ourNeighbors[nodeID] != nil {
				continue
			}

			// Create the zone
			zone, err := node.NewZone(
				node.Point(resp.ResponsibleNode.Zone.MinPoint.Coordinates),
				node.Point(resp.ResponsibleNode.Zone.MaxPoint.Coordinates),
			)
			if err != nil {
				log.Printf("Warning: failed to create zone for potential neighbor: %v", err)
				continue
			}

			// Add to potential neighbors
			potentialNeighbors[nodeID] = &node.NeighborInfo{
				ID:      nodeID,
				Address: resp.ResponsibleNode.Address,
				Zone:    zone,
			}
		}
	}

	// Now add the potential neighbors that are truly neighbors of our new expanded zone
	for _, nbrInfo := range potentialNeighbors {
		if s.Node.IsNeighborZone(nbrInfo.Zone) {
			s.Node.AddNeighbor(nbrInfo.ID, nbrInfo.Address, nbrInfo.Zone)

			// Also notify them about us
			client, conn, err := ConnectToNode(ctx, nbrInfo.Address)
			if err != nil {
				log.Printf("Warning: failed to connect to new neighbor %s: %v", nbrInfo.ID, err)
				continue
			}

			// Send update
			_, err = client.UpdateNeighbors(ctx, &pb.UpdateNeighborsRequest{
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
			})
			conn.Close()

			if err != nil {
				log.Printf("Warning: failed to notify new neighbor %s: %v", nbrInfo.ID, err)
			}
		}
	}

	return nil
}

// NotifyNeighborsOfZoneChange notifies neighbors about a zone change
func (s *CANServer) NotifyNeighborsOfZoneChange(ctx context.Context) error {
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
			return err
		}
	}

	return nil
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

func checkZonesAdjacent(zone1, zone2 *node.Zone, dimensions int) bool {
	for i := 0; i < dimensions; i++ {
		if (zone1.MaxPoint[i] == zone2.MinPoint[i]) || (zone1.MinPoint[i] == zone2.MaxPoint[i]) {
			return true
		}
	}
	return false
}

// EnhancedReplicateData replicates a key-value pair to neighbors with multi-hop support
func (s *CANServer) EnhancedReplicateData(ctx context.Context, key string, value []byte, replicationFactor int) error {
	// If replication is disabled, do nothing
	if s.Config.ReplicationFactor <= 1 {
		return nil
	}

	// Get the list of neighbors
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	if len(neighbors) == 0 {
		// No neighbors to replicate to
		return nil
	}

	// Track successful replications
	successCount := 0
	replicatedTo := make(map[node.NodeID]bool)

	// Try to replicate to immediate neighbors first
	for _, neighbor := range neighbors {
		// Skip if we already have enough replicas
		if successCount >= replicationFactor {
			break
		}

		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for replication: %v", neighbor.ID, err)
			continue
		}

		// Create a special put request for replication
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

		// Mark as replicated
		replicatedTo[neighbor.ID] = true
		successCount++
	}

	// If we haven't reached the desired replication factor, try multi-hop replication
	if successCount < replicationFactor {
		// Get neighbors' neighbors (2-hop)
		twoHopNeighbors, err := s.discoverTwoHopNeighbors(ctx)
		if err != nil {
			log.Printf("Failed to discover two-hop neighbors: %v", err)
		} else {
			// Try to replicate to two-hop neighbors
			for _, twoHopNeighbor := range twoHopNeighbors {
				// Skip if we already replicated to this node or already have enough replicas
				if replicatedTo[twoHopNeighbor.ID] || successCount >= replicationFactor {
					continue
				}

				// Connect to the two-hop neighbor
				client, conn, err := ConnectToNode(ctx, twoHopNeighbor.Address)
				if err != nil {
					continue
				}

				// Send multi-hop replication request
				req := &pb.PutRequest{
					Key:      key,
					Value:    value,
					Forward:  true,
					IsReplica: true,
				}

				resp, err := client.Put(ctx, req)
				conn.Close()

				if err != nil || !resp.Success {
					continue
				}

				// Mark as replicated
				replicatedTo[twoHopNeighbor.ID] = true
				successCount++
			}
		}
	}

	// Log if we couldn't achieve the desired replication factor
	if successCount < replicationFactor {
		log.Printf("Warning: Could only replicate key %s to %d nodes (wanted %d)", key, successCount, replicationFactor)
	}

	return nil
}

// discoverTwoHopNeighbors discovers neighbors that are two hops away
func (s *CANServer) discoverTwoHopNeighbors(ctx context.Context) ([]*node.NeighborInfo, error) {
	// Get direct neighbors
	s.mu.RLock()
	directNeighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	twoHopNeighbors := make([]*node.NeighborInfo, 0)
	visitedNodes := make(map[node.NodeID]bool)

	// Mark our direct neighbors as visited
	for id := range directNeighbors {
		visitedNodes[id] = true
	}
	// Also mark ourselves as visited
	visitedNodes[s.Node.ID] = true

	// For each direct neighbor, get their neighbors
	for _, neighbor := range directNeighbors {
		// Connect to neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for two-hop discovery: %v", neighbor.ID, err)
			continue
		}

		// Ask for their neighbors
		req := &pb.GetNeighborsRequest{}
		resp, err := client.GetNeighbors(ctx, req)
		conn.Close()

		if err != nil {
			log.Printf("Failed to get neighbors from %s: %v", neighbor.ID, err)
			continue
		}

		// Process their neighbors
		for _, nInfo := range resp.Neighbors {
			neighborID := node.NodeID(nInfo.Id)
			
			// Skip if we've already visited this node
			if visitedNodes[neighborID] {
				continue
			}
			
			// Convert proto neighbor to our neighbor info format
			zone, _ := node.NewZone(
				node.Point(nInfo.Zone.MinPoint.Coordinates),
				node.Point(nInfo.Zone.MaxPoint.Coordinates),
			)
			
			twoHopNeighbor := &node.NeighborInfo{
				ID:      neighborID,
				Address: nInfo.Address,
				Zone:    zone,
			}
			
			twoHopNeighbors = append(twoHopNeighbors, twoHopNeighbor)
			visitedNodes[neighborID] = true
		}
	}

	return twoHopNeighbors, nil
}

// GetNeighborsWithinHops gets all neighbors within a specific hop distance
func (s *CANServer) GetNeighborsWithinHops(ctx context.Context, maxHops int) ([]*node.NeighborInfo, error) {
	if maxHops <= 0 {
		return []*node.NeighborInfo{}, nil
	}

	// For hop = 1, just return direct neighbors
	if maxHops == 1 {
		s.mu.RLock()
		directNeighbors := s.Node.GetNeighbors()
		s.mu.RUnlock()
		
		result := make([]*node.NeighborInfo, 0, len(directNeighbors))
		for _, info := range directNeighbors {
			result = append(result, info)
		}
		return result, nil
	}

	// For hop > 1, we need to do BFS
	visitedNodes := make(map[node.NodeID]bool)
	visitedNodes[s.Node.ID] = true
	
	// Get direct neighbors first
	s.mu.RLock()
	directNeighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()
	
	// Initialize result and queue for BFS
	result := make([]*node.NeighborInfo, 0)
	queue := make([]*node.NeighborInfo, 0)
	hopCounts := make(map[node.NodeID]int)
	
	// Add direct neighbors to queue
	for _, info := range directNeighbors {
		queue = append(queue, info)
		visitedNodes[info.ID] = true
		hopCounts[info.ID] = 1
		result = append(result, info)
	}
	
	// BFS to find neighbors within hop limit
	for len(queue) > 0 {
		// Dequeue
		current := queue[0]
		queue = queue[1:]
		
		currentHopCount := hopCounts[current.ID]
		if currentHopCount >= maxHops {
			continue // Don't explore further than maxHops
		}
		
		// Connect to this neighbor
		client, conn, err := ConnectToNode(ctx, current.Address)
		if err != nil {
			continue
		}
		
		// Ask for their neighbors
		req := &pb.GetNeighborsRequest{}
		resp, err := client.GetNeighbors(ctx, req)
		conn.Close()
		
		if err != nil {
			continue
		}
		
		// Process their neighbors
		for _, nInfo := range resp.Neighbors {
			neighborID := node.NodeID(nInfo.Id)
			
			// Skip if we've already visited this node
			if visitedNodes[neighborID] {
				continue
			}
			
			// Convert proto neighbor to our neighbor info format
			zone, _ := node.NewZone(
				node.Point(nInfo.Zone.MinPoint.Coordinates),
				node.Point(nInfo.Zone.MaxPoint.Coordinates),
			)
			
			nextHopNeighbor := &node.NeighborInfo{
				ID:      neighborID,
				Address: nInfo.Address,
				Zone:    zone,
			}
			
			queue = append(queue, nextHopNeighbor)
			result = append(result, nextHopNeighbor)
			visitedNodes[neighborID] = true
			hopCounts[neighborID] = currentHopCount + 1
		}
	}
	
	return result, nil
}

// UpdatedPutWithReplication updates the normal Put operation to include multi-hop replication
func (s *CANServer) UpdatedPutWithMultiHopReplication(ctx context.Context, key string, value []byte) error {
	// First store locally
	if err := s.Store.Put(key, value); err != nil {
		return err
	}

	// Then replicate with the configured hop count
	return s.EnhancedReplicateData(ctx, key, value, s.Config.ReplicationFactor)
}

// ReplicateHotKey replicates frequently accessed keys to more nodes for load balancing
func (s *CANServer) ReplicateHotKey(ctx context.Context, key string, accessCount int) error {
	// Get the value of the hot key
	s.mu.RLock()
	valueStr, exists := s.Node.Data[key]
	s.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("key %s not found", key)
	}
	
	// Calculate a dynamic replication factor based on access frequency
	// More popular keys get more replicas, up to a maximum
	dynamicFactor := min(s.Config.MaxHotKeyReplicas, 
		s.Config.ReplicationFactor + (accessCount / s.Config.HotKeyThreshold))
	
	// If the dynamic factor is higher than our standard replication factor,
	// create additional replicas
	if dynamicFactor > s.Config.ReplicationFactor {
		// Count existing replicas
		replicaCount := 1 // Start with our copy
		replicaNodes := make(map[node.NodeID]bool)
		replicaNodes[s.Node.ID] = true
		
		// Check immediate neighbors
		neighbors := s.Node.GetNeighbors()
		for neighborID, neighbor := range neighbors {
			client, conn, err := ConnectToNode(ctx, neighbor.Address)
			if err != nil {
				continue
			}
			
			// Check if neighbor has the key
			req := &pb.GetRequest{
				Key:     key,
				Forward: true,
			}
			
			resp, err := client.Get(ctx, req)
			conn.Close()
			
			if err == nil && resp.Success && resp.Exists {
				replicaCount++
				replicaNodes[neighborID] = true
			}
		}
		
		// If we need more replicas based on the dynamic factor
		if replicaCount < dynamicFactor {
			log.Printf("Hot key %s detected (%d accesses), increasing replicas from %d to %d", 
				key, accessCount, replicaCount, dynamicFactor)
			
			// Temporarily override the replication factor
			originalFactor := s.Config.ReplicationFactor
			s.Config.ReplicationFactor = dynamicFactor
			
			// Create the additional replicas
			err := s.EnhancedReplicateWithExclusions(ctx, key, []byte(valueStr), replicaNodes)
			
			// Restore the original replication factor
			s.Config.ReplicationFactor = originalFactor
			
			return err
		}
	}
	
	return nil
}

// TrackKeyAccess tracks access to keys to identify hot keys
func (s *CANServer) TrackKeyAccess(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Initialize if needed
	if s.KeyAccessCounts == nil {
		s.KeyAccessCounts = make(map[string]int)
	}
	
	// Increment access count
	s.KeyAccessCounts[key]++
	
	// Check for hot key threshold
	if s.KeyAccessCounts[key] >= s.Config.HotKeyThreshold && 
	   s.KeyAccessCounts[key] % s.Config.HotKeyCheckInterval == 0 {
		
		// Schedule hot key replication asynchronously to avoid blocking
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			
			if err := s.ReplicateHotKey(ctx, key, s.KeyAccessCounts[key]); err != nil {
				log.Printf("Failed to replicate hot key %s: %v", key, err)
			}
		}()
	}
}

// CheckAndRebalanceReplicas periodically checks if replicas meet the replication factor
// and adjusts replication if necessary (e.g., when nodes fail)
func (s *CANServer) CheckAndRebalanceReplicas(ctx context.Context) {
	ticker := time.NewTicker(s.Config.ReplicationCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			// Make a copy of data to avoid holding the lock during network operations
			keysToCheck := make([]string, 0, len(s.Node.Data))
			for key := range s.Node.Data {
				keysToCheck = append(keysToCheck, key)
			}
			s.mu.RUnlock()

			for _, key := range keysToCheck {
				s.balanceReplicasForKey(ctx, key)
			}
			
			// Also check for expired hot key tracking
			s.pruneAccessCounts()
		}
	}
}

// pruneAccessCounts removes old entries from the key access tracking
func (s *CANServer) pruneAccessCounts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.KeyAccessCounts == nil {
		return
	}
	
	// Remove entries that haven't been accessed recently
	// This prevents the map from growing indefinitely
	for key, count := range s.KeyAccessCounts {
		// If count is below threshold, or not accessed recently, remove it
		if count < s.Config.HotKeyThreshold || 
		   (s.lastAccessTime != nil && 
		    s.lastAccessTime[key].Add(s.Config.HotKeyTTL).Before(time.Now())) {
			
			delete(s.KeyAccessCounts, key)
			if s.lastAccessTime != nil {
				delete(s.lastAccessTime, key)
			}
		}
	}
}

// balanceReplicasForKey checks and rebalances replicas for a specific key
func (s *CANServer) balanceReplicasForKey(ctx context.Context, key string) {
	// Get value for the key
	s.mu.RLock()
	valueStr, exists := s.Node.Data[key]
	s.mu.RUnlock()

	if !exists {
		return
	}

	// Count existing replicas by querying known nodes
	replicaCount := 1 // Start at 1 for our own copy
	replicaNodes := make(map[node.NodeID]bool)
	replicaNodes[s.Node.ID] = true

	// First check immediate neighbors
	neighbors := s.Node.GetNeighbors()
	for neighborID, neighbor := range neighbors {
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			continue
		}

		// Check if this neighbor has a replica
		req := &pb.GetRequest{
			Key:     key,
			Forward: true, // Check local store only
		}

		resp, err := client.Get(ctx, req)
		conn.Close()

		if err == nil && resp.Success && resp.Exists {
			replicaCount++
			replicaNodes[neighborID] = true
		}
	}

	// If we don't have enough replicas, use enhanced replication to create more
	if replicaCount < s.Config.ReplicationFactor {
		log.Printf("Rebalancing replicas for key %s (current: %d, target: %d)", 
			key, replicaCount, s.Config.ReplicationFactor)
		
		// Create value from string
		value := []byte(valueStr)
		
		// Use enhanced replication with the existing replica set
		s.EnhancedReplicateWithExclusions(ctx, key, value, replicaNodes)
	}
}

// EnhancedReplicateWithExclusions replicates data to nodes not in the exclusion set
func (s *CANServer) EnhancedReplicateWithExclusions(ctx context.Context, key string, value []byte, exclusions map[node.NodeID]bool) error {
	// Number of additional replicas we need to create
	replicasNeeded := s.Config.ReplicationFactor - len(exclusions)
	if replicasNeeded <= 0 {
		// We already have enough replicas
		return nil
	}

	// Get immediate neighbors
	s.mu.RLock()
	immediateNeighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	// Map to store node ID to address for connection purposes
	nodeAddresses := make(map[node.NodeID]string)
	
	// Add immediate neighbors to the address map
	for id, info := range immediateNeighbors {
		nodeAddresses[id] = info.Address
	}

	// Create a queue for breadth-first traversal of the network
	// Start with immediate neighbors not in the exclusion set
	queue := make([]node.NodeID, 0, len(immediateNeighbors))
	for id := range immediateNeighbors {
		if !exclusions[id] {
			queue = append(queue, id)
		}
	}

	// Track successful replications
	successCount := 0
	
	// Track nodes visited to avoid cycles
	visited := make(map[node.NodeID]bool)
	for id := range exclusions {
		visited[id] = true
	}

	// Use BFS to find enough nodes for replication
	maxHops := s.Config.MaxReplicationHops
	if maxHops <= 0 {
		maxHops = 3 // Default to 3 hops if not specified
	}
	
	currentHop := 0
	nodesInCurrentLayer := len(queue)
	nodesInNextLayer := 0
	
	// For each hop level
	for currentHop < maxHops && len(queue) > 0 && successCount < replicasNeeded {
		// Process all nodes at the current hop level
		for i := 0; i < nodesInCurrentLayer && successCount < replicasNeeded; i++ {
			if len(queue) == 0 {
				break
			}
			
			// Dequeue a node
			currentNodeID := queue[0]
			queue = queue[1:]
			
			// Skip if already visited
			if visited[currentNodeID] {
				continue
			}
			visited[currentNodeID] = true
			
			// Get address of the node
			address, exists := nodeAddresses[currentNodeID]
			if !exists {
				continue
			}
			
			// Connect to the node
			client, conn, err := ConnectToNode(ctx, address)
			if err != nil {
				log.Printf("Failed to connect to node %s for replication: %v", currentNodeID, err)
				continue
			}
			
			// Send replication request
			req := &pb.PutRequest{
				Key:     key,
				Value:   value,
				Forward: true, // Store locally
				Ttl:     int32(s.Config.ReplicaTTL.Seconds()), // Set TTL for replica
			}
			
			resp, err := client.Put(ctx, req)
			if err == nil && resp.Success {
				successCount++
				log.Printf("Successfully replicated key %s to node %s (hop %d)", key, currentNodeID, currentHop)
			}
			
			// Get neighbors for the next hop level
			if currentHop < maxHops-1 {
				// Request neighbor list
				neighReq := &pb.GetNeighborsRequest{}
				neighResp, err := client.GetNeighbors(ctx, neighReq)
				
				if err == nil && neighResp.Success {
					// Add each neighbor to the queue for the next layer
					for _, neighbor := range neighResp.Neighbors {
						neighborID := node.NodeID(neighbor.Id)
						if !visited[neighborID] && !exclusions[neighborID] {
							queue = append(queue, neighborID)
							nodeAddresses[neighborID] = neighbor.Address
							nodesInNextLayer++
						}
					}
				}
			}
			
			// Close connection
			conn.Close()
		}
		
		// Move to the next hop level
		currentHop++
		nodesInCurrentLayer = nodesInNextLayer
		nodesInNextLayer = 0
	}
	
	// Log replication status
	if successCount < replicasNeeded {
		log.Printf("Warning: Could only create %d additional replicas for key %s (wanted %d)", 
			successCount, key, replicasNeeded)
	} else {
		log.Printf("Successfully created %d additional replicas for key %s", successCount, key)
	}
	
	return nil
}
