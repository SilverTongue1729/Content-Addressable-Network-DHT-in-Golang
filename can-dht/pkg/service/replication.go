package service

import (
	"context"
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
	// First, update our zone to include the failed node's zone
	// This is a simplification - in a real implementation, we'd need to
	// calculate a proper merge of the zones

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a new zone that encompasses both our zone and the failed node's zone
	minPoint := make(node.Point, len(s.Node.Zone.MinPoint))
	maxPoint := make(node.Point, len(s.Node.Zone.MaxPoint))

	for i := 0; i < len(minPoint); i++ {
		minPoint[i] = minFloat64(s.Node.Zone.MinPoint[i], failedZone.MinPoint[i])
		maxPoint[i] = maxFloat64(s.Node.Zone.MaxPoint[i], failedZone.MaxPoint[i])
	}

	newZone, err := node.NewZone(minPoint, maxPoint)
	if err != nil {
		return err
	}

	// Update our zone
	s.Node.Zone = newZone

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
