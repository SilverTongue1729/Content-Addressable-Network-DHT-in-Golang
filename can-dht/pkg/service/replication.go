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
