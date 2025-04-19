package service

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/internal/proto"
)

// LoadStats tracks load statistics for a node
type LoadStats struct {
	// RequestCount tracks the number of requests handled
	RequestCount int64
	
	// KeyCount tracks the number of keys stored
	KeyCount int64
	
	// LastSplitTime is when the zone was last split
	LastSplitTime time.Time
	
	// DataSize tracks the total size of stored data in bytes
	DataSize int64
	
	// KeysByZone maps subzones to key counts
	KeysByZone map[string]int
	
	// ZoneUtilization tracks zone usage as a percentage (0-1.0)
	ZoneUtilization float64
	
	mu sync.RWMutex
}

// NewLoadStats creates a new LoadStats instance
func NewLoadStats() *LoadStats {
	return &LoadStats{
		RequestCount:  0,
		KeyCount:      0,
		LastSplitTime: time.Now(),
		DataSize:      0,
		KeysByZone:    make(map[string]int),
		ZoneUtilization: 0.0,
	}
}

// IncrementRequestCount increments the request counter
func (ls *LoadStats) IncrementRequestCount() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.RequestCount++
}

// UpdateKeyCount updates the key count
func (ls *LoadStats) UpdateKeyCount(count int64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.KeyCount = count
}

// UpdateDataSize updates the total data size
func (ls *LoadStats) UpdateDataSize(size int64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.DataSize = size
}

// UpdateZoneUtilization updates the zone utilization metric
func (ls *LoadStats) UpdateZoneUtilization(utilization float64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.ZoneUtilization = utilization
}

// GetZoneUtilization gets the current zone utilization
func (ls *LoadStats) GetZoneUtilization() float64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.ZoneUtilization
}

// ResetStats resets the statistics
func (ls *LoadStats) ResetStats() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.RequestCount = 0
	ls.LastSplitTime = time.Now()
}

// StartLoadMonitoring begins periodic load monitoring
func (s *CANServer) StartLoadMonitoring(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.updateLoadMetrics()
			s.checkForLoadImbalance()
		}
	}
}

// updateLoadMetrics collects and updates load metrics
func (s *CANServer) updateLoadMetrics() {
	// Update key count
	keyCount := int64(len(s.Node.Data))
	s.LoadStats.UpdateKeyCount(keyCount)
	
	// Calculate data size
	var totalSize int64
	for _, data := range s.Node.Data {
		totalSize += int64(len(data))
	}
	s.LoadStats.UpdateDataSize(totalSize)
	
	// Calculate zone utilization based on key distribution
	expectedKeysPerUnit := float64(s.estimateAverageKeyCount()) / calculateZoneVolume(s.Node.Zone)
	actualDensity := float64(keyCount) / calculateZoneVolume(s.Node.Zone)
	utilization := actualDensity / expectedKeysPerUnit
	if utilization > 1.0 {
		utilization = 1.0 // Cap at 100%
	}
	
	s.LoadStats.UpdateZoneUtilization(utilization)
}

// estimateAverageKeyCount estimates the average number of keys across the network
func (s *CANServer) estimateAverageKeyCount() int {
	// This is a simple estimate based on local knowledge
	// A more advanced version would collect stats from other nodes
	totalKeysKnown := len(s.Node.Data)
	totalNodesKnown := 1 + len(s.Node.Neighbors)
	
	// Return a minimum of 1 to avoid division by zero
	if totalNodesKnown == 0 {
		return 1
	}
	
	return totalKeysKnown / totalNodesKnown
}

// checkForLoadImbalance checks if this node is overloaded or underloaded
func (s *CANServer) checkForLoadImbalance() {
	// Skip if we've split or merged recently
	sinceLastSplit := time.Since(s.LoadStats.LastSplitTime)
	if sinceLastSplit < 5*time.Minute {
		return
	}
	
	// Get current utilization
	utilization := s.LoadStats.GetZoneUtilization()
	
	// Check if we're overloaded and need to split
	if utilization >= s.Config.LoadBalancingThreshold {
		log.Printf("Node %s is overloaded (utilization: %.2f). Initiating proactive split.", 
			s.Node.ID, utilization)
		
		s.proactiveSplitZone()
		return
	}
	
	// Check if we're underloaded and should consider merging
	if s.Config.EnableZoneMerging && utilization <= s.Config.ZoneUtilizationThreshold {
		log.Printf("Node %s is underloaded (utilization: %.2f). Considering zone merge.", 
			s.Node.ID, utilization)
		
		s.considerZoneMerge()
	}
}

// proactiveSplitZone performs a proactive split of the node's zone
func (s *CANServer) proactiveSplitZone() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Create a random point that's guaranteed to be in this node's zone
	randomPoint := s.generateRandomPointInZone()
	
	// Create a temp node ID for the virtual node that will take part of the zone
	tempNodeID := node.NodeID(fmt.Sprintf("temp-%s-%d", s.Node.ID, time.Now().UnixNano()))
	
	// Split the zone
	newZone, err := s.Node.ProactiveSplit()
	if err != nil {
		log.Printf("Failed to split zone proactively: %v", err)
		return
	}
	
	log.Printf("Proactively split zone. New zone: %v", newZone)
	
	// Update load stats
	s.LoadStats.LastSplitTime = time.Now()
	s.LoadStats.ResetStats()
	
	// Redistribute data
	s.redistributeData(newZone)
}

// generateRandomPointInZone creates a random point within the node's zone
func (s *CANServer) generateRandomPointInZone() node.Point {
	zone := s.Node.Zone
	point := make(node.Point, len(zone.MinPoint))
	
	for i := 0; i < len(zone.MinPoint); i++ {
		// Generate a random value between min and max for this dimension
		point[i] = zone.MinPoint[i] + rand.Float64()*(zone.MaxPoint[i]-zone.MinPoint[i])
	}
	
	return point
}

// redistributeData redistributes data after a zone split
func (s *CANServer) redistributeData(newZone *node.Zone) {
	// Collect keys that need to be moved
	keysToMove := make(map[string]string)
	
	for key, value := range s.Node.Data {
		// Hash the key to find its point
		keyPoint := s.Router.HashToPoint(key)
		
		// If the key belongs in the new zone, mark it for moving
		if newZone.Contains(keyPoint) {
			keysToMove[key] = value
		}
	}
	
	// Remove moved keys from this node
	for key := range keysToMove {
		delete(s.Node.Data, key)
	}
	
	log.Printf("Redistributed %d keys after proactive split", len(keysToMove))
}

// considerZoneMerge evaluates if this node's zone should be merged with a neighbor
func (s *CANServer) considerZoneMerge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Get all adjacent neighbors
	adjacentNeighbors := s.getAdjacentNeighbors(s.Node.Zone)
	if len(adjacentNeighbors) == 0 {
		return
	}
	
	// Find the best neighbor to merge with
	bestNeighbor := s.selectMergeNeighbor(adjacentNeighbors)
	if bestNeighbor == nil {
		log.Printf("No suitable neighbor found for zone merging")
		return
	}
	
	// Check if merging would exceed the average zone size
	avgZoneSize := s.estimateAverageZoneVolume()
	mergedVolume := calculateZoneVolume(s.Node.Zone) + calculateZoneVolume(bestNeighbor.Zone)
	
	if mergedVolume > avgZoneSize * 1.5 {
		log.Printf("Skipping merge with %s: merged volume (%.2f) would exceed threshold", 
			bestNeighbor.ID, mergedVolume)
		return
	}
	
	// Check if the zones can actually be merged (must share a face)
	canMerge, mergedZone := canZonesMerge(s.Node.Zone, bestNeighbor.Zone)
	if !canMerge {
		log.Printf("Zones cannot be merged with %s: not properly aligned", bestNeighbor.ID)
		return
	}
	
	// Propose merge to the neighbor
	err := s.proposeZoneMerge(context.Background(), bestNeighbor.ID, bestNeighbor.Address)
	if err != nil {
		log.Printf("Failed to propose zone merge to %s: %v", bestNeighbor.ID, err)
		return
	}
	
	log.Printf("Successfully proposed zone merge to %s", bestNeighbor.ID)
}

// selectMergeNeighbor finds the best neighbor to merge with
func (s *CANServer) selectMergeNeighbor(neighbors map[node.NodeID]*node.NeighborInfo) *node.NeighborInfo {
	var bestNeighbor *node.NeighborInfo
	bestScore := -1.0
	
	for _, neighbor := range neighbors {
		// Calculate a score based on load, stability, and zone alignment
		loadScore := s.getNodeLoadScore(neighbor.ID)
		stabilityScore := s.getNodeStabilityScore(neighbor.ID)
		
		// Only consider underloaded neighbors
		if loadScore > s.Config.ZoneUtilizationThreshold {
			continue
		}
		
		// Check if zones can be merged
		canMerge, _ := canZonesMerge(s.Node.Zone, neighbor.Zone)
		if !canMerge {
			continue
		}
		
		// Calculate a combined score - prioritize stability and low load
		score := (stabilityScore * 0.7) + ((1.0 - loadScore) * 0.3)
		
		if score > bestScore {
			bestScore = score
			bestNeighbor = neighbor
		}
	}
	
	return bestNeighbor
}

// canZonesMerge checks if two zones can be merged and returns the merged zone if possible
func canZonesMerge(zone1, zone2 *node.Zone) (bool, *node.Zone) {
	// Zones must share a face to be mergeable
	// For each dimension, check if the zones share a face
	sharedFaceDimension := -1
	
	for dim := 0; dim < len(zone1.MinPoint); dim++ {
		// Check if the zones are adjacent in this dimension
		if zone1.MaxPoint[dim] == zone2.MinPoint[dim] || zone2.MaxPoint[dim] == zone1.MinPoint[dim] {
			// Check if they have the same coordinates in all other dimensions
			sameInOtherDimensions := true
			for otherDim := 0; otherDim < len(zone1.MinPoint); otherDim++ {
				if otherDim == dim {
					continue
				}
				
				if zone1.MinPoint[otherDim] != zone2.MinPoint[otherDim] || 
				   zone1.MaxPoint[otherDim] != zone2.MaxPoint[otherDim] {
					sameInOtherDimensions = false
					break
				}
			}
			
			if sameInOtherDimensions {
				sharedFaceDimension = dim
				break
			}
		}
	}
	
	if sharedFaceDimension == -1 {
		return false, nil
	}
	
	// Create the merged zone
	newMin := make(node.Point, len(zone1.MinPoint))
	newMax := make(node.Point, len(zone1.MaxPoint))
	
	for dim := 0; dim < len(zone1.MinPoint); dim++ {
		if dim == sharedFaceDimension {
			// In the shared dimension, take the min and max from both zones
			newMin[dim] = math.Min(zone1.MinPoint[dim], zone2.MinPoint[dim])
			newMax[dim] = math.Max(zone1.MaxPoint[dim], zone2.MaxPoint[dim])
		} else {
			// In other dimensions, the coordinates are the same
			newMin[dim] = zone1.MinPoint[dim]
			newMax[dim] = zone1.MaxPoint[dim]
		}
	}
	
	mergedZone, err := node.NewZone(newMin, newMax)
	if err != nil {
		return false, nil
	}
	
	return true, mergedZone
}

// proposeZoneMerge proposes a zone merge to another node
func (s *CANServer) proposeZoneMerge(ctx context.Context, nodeID node.NodeID, address string) error {
	// Connect to the node
	client, conn, err := s.ConnectToNode(ctx, address)
	if err != nil {
		return fmt.Errorf("failed to connect to node: %w", err)
	}
	defer conn.Close()
	
	// Create the merge proposal
	req := &pb.ZoneMergeProposalRequest{
		ProposingNodeId: string(s.Node.ID),
		ProposingNodeZone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: s.Node.Zone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: s.Node.Zone.MaxPoint,
			},
		},
		ProposingNodeAddress: s.Node.Address,
		ProposingNodeLoad: s.LoadStats.GetZoneUtilization(),
	}
	
	// Send the proposal
	resp, err := client.ProposeZoneMerge(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send zone merge proposal: %w", err)
	}
	
	if !resp.Accepted {
		return fmt.Errorf("zone merge proposal rejected: %s", resp.Reason)
	}
	
	// If accepted, start the merge process
	go s.performZoneMerge(context.Background(), nodeID, address)
	
	return nil
}

// performZoneMerge executes the zone merge with another node
func (s *CANServer) performZoneMerge(ctx context.Context, nodeID node.NodeID, address string) {
	log.Printf("Starting zone merge with node %s", nodeID)
	
	// Get the neighbor's zone
	s.mu.RLock()
	neighbor, exists := s.Node.Neighbors[nodeID]
	if !exists {
		log.Printf("Neighbor %s no longer exists, aborting merge", nodeID)
		s.mu.RUnlock()
		return
	}
	
	// Check if the zones can be merged
	canMerge, mergedZone := canZonesMerge(s.Node.Zone, neighbor.Zone)
	if !canMerge {
		log.Printf("Zones can no longer be merged with %s, aborting", nodeID)
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	
	// Connect to the node
	client, conn, err := s.ConnectToNode(ctx, address)
	if err != nil {
		log.Printf("Failed to connect to node %s for merge: %v", nodeID, err)
		return
	}
	defer conn.Close()
	
	// Get the node's data
	req := &pb.GetZoneDataRequest{
		RequestingNodeId: string(s.Node.ID),
	}
	
	resp, err := client.GetZoneData(ctx, req)
	if err != nil {
		log.Printf("Failed to get zone data from %s: %v", nodeID, err)
		return
	}
	
	// Take over the neighbor's zone
	s.mu.Lock()
	
	// Update zone
	s.Node.Zone = mergedZone
	
	// Merge data
	for key, value := range resp.Data {
		s.Node.Data[key] = string(value)
	}
	
	// Update neighbors
	for _, neighborInfo := range resp.Neighbors {
		if string(s.Node.ID) == neighborInfo.Id {
			continue // Skip ourselves
		}
		
		zoneMin := make(node.Point, len(neighborInfo.Zone.MinPoint.Coordinates))
		zoneMax := make(node.Point, len(neighborInfo.Zone.MaxPoint.Coordinates))
		copy(zoneMin, neighborInfo.Zone.MinPoint.Coordinates)
		copy(zoneMax, neighborInfo.Zone.MaxPoint.Coordinates)
		
		neighborZone, err := node.NewZone(zoneMin, zoneMax)
		if err != nil {
			continue
		}
		
		s.Node.AddNeighbor(node.NodeID(neighborInfo.Id), neighborInfo.Address, neighborZone)
	}
	
	s.mu.Unlock()
	
	// Reset load stats
	s.LoadStats.LastSplitTime = time.Now()
	s.LoadStats.ResetStats()
	
	// Notify neighbors about the zone change
	go s.notifyNeighborsAboutZoneChange(ctx)
	
	log.Printf("Successfully merged zone with node %s", nodeID)
}

// notifyNeighborsAboutZoneChange notifies all neighbors about a zone change
func (s *CANServer) notifyNeighborsAboutZoneChange(ctx context.Context) {
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()
	
	for _, neighbor := range neighbors {
		// Connect to the neighbor
		client, conn, err := s.ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for zone change notification: %v", neighbor.ID, err)
			continue
		}
		
		// Create the notification
		req := &pb.UpdateNeighborRequest{
			NodeId: string(s.Node.ID),
			Zone: &pb.Zone{
				MinPoint: &pb.Point{
					Coordinates: s.Node.Zone.MinPoint,
				},
				MaxPoint: &pb.Point{
					Coordinates: s.Node.Zone.MaxPoint,
				},
			},
		}
		
		// Send the notification
		_, err = client.UpdateNeighborZone(ctx, req)
		if err != nil {
			log.Printf("Failed to notify neighbor %s about zone change: %v", neighbor.ID, err)
		}
		
		conn.Close()
	}
} 