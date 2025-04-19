package service

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoadBalancingStrategy defines different load balancing strategies
type LoadBalancingStrategy int

const (
	// RoundRobin distributes requests evenly among nodes
	RoundRobin LoadBalancingStrategy = iota
	
	// LeastLoaded sends requests to the node with the lowest load
	LeastLoaded
	
	// Probabilistic sends requests with probability inversely proportional to load
	Probabilistic
	
	// ConsistentHashing uses consistent hashing to map keys to nodes
	ConsistentHashing
)

// LoadStats tracks load statistics for a node
type LoadStats struct {
	// Total requests processed
	TotalRequests int
	
	// Read requests processed
	ReadRequests int
	
	// Write requests processed
	WriteRequests int
	
	// Request rate (requests per second)
	RequestRate float64
	
	// CPU utilization (0-100%)
	CPUUtilization float64
	
	// Memory utilization (0-100%)
	MemoryUtilization float64
	
	// Network utilization (0-100%)
	NetworkUtilization float64
	
	// Storage utilization (0-100%)
	StorageUtilization float64
	
	// Timestamp of the last update
	LastUpdate time.Time
	
	// Key frequency tracking
	KeyFrequency map[string]int
	
	// Hot keys identified
	HotKeys map[string]int
	
	// Zone adjustment history
	ZoneAdjustments []ZoneAdjustment
	
	// Request latency (in milliseconds)
	AverageLatency float64
	
	// Load percentage relative to capacity (0-100%)
	LoadPercentage float64
	
	// Request time history (for tracking load over time)
	requestTimes []time.Time
	
	// Request count window (last n seconds)
	requestCountWindow int
	
	// Overall node load (0-1)
	NodeLoad float64
	
	// Key access counts
	KeyAccessCounts map[string]int
	
	// Access timestamps for recency-based decisions
	KeyLastAccessed map[string]time.Time
	
	// Zone size relative to the entire coordinate space
	ZoneSize float64
	
	// Number of operations per second
	OperationsPerSecond float64
	
	// Last time the node was overloaded
	LastOverloadTime time.Time
	
	// Count of consecutive overload detections
	ConsecutiveOverloads int
	
	// Load history for trend analysis
	LoadHistory []float64
	
	// Currently active zone transfers
	ActiveZoneTransfers int
	
	// Last time zone was adjusted
	LastZoneAdjustment time.Time
	
	mu sync.RWMutex
}

// ZoneAdjustment tracks a zone adjustment event
type ZoneAdjustment struct {
	// Time of adjustment
	Time time.Time
	
	// Original zone
	OriginalZone *node.Zone
	
	// New zone
	NewZone *node.Zone
	
	// Reason for adjustment
	Reason string
	
	// Load before adjustment
	LoadBefore float64
	
	// Load after adjustment
	LoadAfter float64
}

// NewLoadStats creates a new load statistics tracker
func NewLoadStats() *LoadStats {
	return &LoadStats{
		KeyFrequency:       make(map[string]int),
		HotKeys:            make(map[string]int),
		ZoneAdjustments:    make([]ZoneAdjustment, 0),
		requestTimes:       make([]time.Time, 0, 1000),
		requestCountWindow: 60, // Default 60 second window
		LastUpdate:         time.Now(),
		KeyAccessCounts:    make(map[string]int),
		KeyLastAccessed:    make(map[string]time.Time),
		ZoneSize:           1.0, // Assuming default zone size
		OperationsPerSecond: 0.0,
		LoadHistory:        make([]float64, 0, 100), // Keep the last 100 load samples
		LastZoneAdjustment: time.Now().Add(-1 * time.Hour), // Start with ability to adjust immediately
	}
}

// RecordRequest records a request for load statistics
func (ls *LoadStats) RecordRequest(key string, isWrite bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	
	// Update request counts
	ls.TotalRequests++
	if isWrite {
		ls.WriteRequests++
	} else {
		ls.ReadRequests++
	}
	
	// Track key frequency
	ls.KeyFrequency[key]++
	
	// Add to request times
	now := time.Now()
	ls.requestTimes = append(ls.requestTimes, now)
	
	// Trim old request times (older than window)
	cutoff := now.Add(-time.Duration(ls.requestCountWindow) * time.Second)
	newIdx := 0
	for i, t := range ls.requestTimes {
		if t.After(cutoff) {
			newIdx = i
			break
		}
	}
	if newIdx > 0 {
		ls.requestTimes = ls.requestTimes[newIdx:]
	}
	
	// Update request rate
	windowDuration := float64(ls.requestCountWindow)
	if len(ls.requestTimes) > 0 {
		oldest := ls.requestTimes[0]
		actualDuration := now.Sub(oldest).Seconds()
		if actualDuration > 0 {
			ls.RequestRate = float64(len(ls.requestTimes)) / actualDuration
		}
	}
	
	// Check for hot keys
	threshold := 100 // Configurable threshold
	if ls.KeyFrequency[key] > threshold {
		ls.HotKeys[key] = ls.KeyFrequency[key]
	}
	
	ls.LastUpdate = now
}

// UpdateSystemStats updates system utilization stats
func (ls *LoadStats) UpdateSystemStats(cpu, memory, network, storage float64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	
	ls.CPUUtilization = cpu
	ls.MemoryUtilization = memory
	ls.NetworkUtilization = network
	ls.StorageUtilization = storage
	
	// Calculate overall load percentage (weighted average)
	ls.LoadPercentage = 0.4*cpu + 0.3*memory + 0.2*network + 0.1*storage
	
	ls.LastUpdate = time.Now()
}

// RecordZoneAdjustment records a zone adjustment event
func (ls *LoadStats) RecordZoneAdjustment(originalZone, newZone *node.Zone, reason string, loadBefore, loadAfter float64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	
	adjustment := ZoneAdjustment{
		Time:         time.Now(),
		OriginalZone: originalZone,
		NewZone:      newZone,
		Reason:       reason,
		LoadBefore:   loadBefore,
		LoadAfter:    loadAfter,
	}
	
	ls.ZoneAdjustments = append(ls.ZoneAdjustments, adjustment)
}

// GetCurrentLoad returns the current load percentage
func (ls *LoadStats) GetCurrentLoad() float64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.LoadPercentage
}

// GetHotKeys returns a list of hot keys
func (ls *LoadStats) GetHotKeys() map[string]int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	
	// Create a copy to avoid concurrent modification
	hotKeys := make(map[string]int)
	for k, v := range ls.HotKeys {
		hotKeys[k] = v
	}
	
	return hotKeys
}

// IsOverloaded checks if the node is overloaded
func (ls *LoadStats) IsOverloaded() bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	
	// Consider a node overloaded if:
	// 1. Load percentage is high
	// 2. Request rate is high
	// 3. CPU utilization is high
	
	return ls.LoadPercentage > 80 || ls.RequestRate > 1000 || ls.CPUUtilization > 80
}

// HandleHotSpotDetection detects and handles hot spots in the network
func (s *CANServer) HandleHotSpotDetection(ctx context.Context) {
	// Periodically check for hot spots
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if this node is a hot spot
			if s.IsHotSpot() {
				log.Printf("Hot spot detected, attempting to rebalance")
				s.RebalanceLoad(ctx)
			}
		}
	}
}

// IsHotSpot determines if this node is a hot spot
func (s *CANServer) IsHotSpot() bool {
	if s.LoadStats == nil {
		return false
	}
	
	// A node is a hot spot if:
	// 1. It's overloaded
	// 2. It has a significantly higher load than neighbors
	
	if !s.LoadStats.IsOverloaded() {
		return false
	}
	
	// Check neighbor loads
	neighborLoads := s.GetNeighborLoads(context.Background())
	if len(neighborLoads) == 0 {
		// Can't compare, assume not a hot spot
		return false
	}
	
	// Calculate average neighbor load
	var totalLoad float64
	for _, load := range neighborLoads {
		totalLoad += load
	}
	avgNeighborLoad := totalLoad / float64(len(neighborLoads))
	
	// If our load is significantly higher, we're a hot spot
	ourLoad := s.LoadStats.GetCurrentLoad()
	loadRatio := ourLoad / avgNeighborLoad
	
	return loadRatio > 1.5 // 50% higher load than average
}

// GetNeighborLoads retrieves load information from neighbors
func (s *CANServer) GetNeighborLoads(ctx context.Context) map[node.NodeID]float64 {
	neighborLoads := make(map[node.NodeID]float64)
	
	// Get neighbors
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()
	
	// Query each neighbor for its load
	for id, info := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, info.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s: %v", id, err)
			continue
		}
		
		// Create load stats request
		req := &pb.GetLoadStatsRequest{}
		
		// Send the request
		resp, err := client.GetLoadStats(ctx, req)
		conn.Close()
		
		if err != nil {
			log.Printf("Failed to get load stats from neighbor %s: %v", id, err)
			continue
		}
		
		// Record the load
		neighborLoads[id] = float64(resp.LoadPercentage)
	}
	
	return neighborLoads
}

// RebalanceLoad performs load balancing by adjusting zones
func (s *CANServer) RebalanceLoad(ctx context.Context) {
	// Get neighbor loads
	neighborLoads := s.GetNeighborLoads(ctx)
	if len(neighborLoads) == 0 {
		log.Printf("No neighbors to rebalance with")
		return
	}
	
	// Find least loaded neighbor
	var leastLoadedID node.NodeID
	var leastLoad float64 = 100.0
	
	for id, load := range neighborLoads {
		if load < leastLoad {
			leastLoad = load
			leastLoadedID = id
		}
	}
	
	// Get our load
	ourLoad := s.LoadStats.GetCurrentLoad()
	
	// If the least loaded neighbor is significantly less loaded
	if ourLoad - leastLoad > 20 { // At least 20% difference
		// Get the neighbor info
		s.mu.RLock()
		neighborInfo := s.Node.Neighbors[leastLoadedID]
		s.mu.RUnlock()
		
		if neighborInfo == nil {
			log.Printf("Neighbor %s not found", leastLoadedID)
			return
		}
		
		// Try to adjust zones
		if err := s.AdjustZoneWithNeighbor(ctx, leastLoadedID, neighborInfo); err != nil {
			log.Printf("Failed to adjust zone with neighbor %s: %v", leastLoadedID, err)
		}
	} else {
		log.Printf("Load difference not significant enough for rebalancing")
	}
}

// AdjustZoneWithNeighbor attempts to adjust the zone boundary with a neighbor
func (s *CANServer) AdjustZoneWithNeighbor(ctx context.Context, neighborID node.NodeID, neighborInfo *node.NeighborInfo) error {
	// Create a copy of our current zone
	s.mu.RLock()
	originalZone := s.Node.Zone.Copy()
	s.mu.RUnlock()
	
	// Calculate the direction of adjustment
	// Determine which dimension to adjust based on zone properties
	adjustDimension := 0
	maxRange := 0.0
	
	for dim := 0; dim < originalZone.MinPoint.Dimensions(); dim++ {
		// Check if zones are adjacent in this dimension
		if canAdjustInDimension(originalZone, neighborInfo.Zone, dim) {
			// Find the dimension with the largest range to adjust
			range := originalZone.MaxPoint[dim] - originalZone.MinPoint[dim]
			if range > maxRange {
				maxRange = range
				adjustDimension = dim
			}
		}
	}
	
	if maxRange == 0.0 {
		return fmt.Errorf("cannot find suitable dimension for adjustment")
	}
	
	// Calculate adjustment amount (give 20% of our zone to the neighbor)
	adjustmentAmount := maxRange * 0.2
	
	// Create new zones
	ourNewZone := originalZone.Copy()
	neighborNewZone := neighborInfo.Zone.Copy()
	
	// Adjust the zones
	if originalZone.MinPoint[adjustDimension] < neighborInfo.Zone.MinPoint[adjustDimension] {
		// We're on the lower side, give upper part
		ourNewZone.MaxPoint[adjustDimension] -= adjustmentAmount
		neighborNewZone.MinPoint[adjustDimension] -= adjustmentAmount
	} else {
		// We're on the upper side, give lower part
		ourNewZone.MinPoint[adjustDimension] += adjustmentAmount
		neighborNewZone.MaxPoint[adjustDimension] += adjustmentAmount
	}
	
	// Verify adjustment is valid
	if !isValidZone(ourNewZone) || !isValidZone(neighborNewZone) {
		return fmt.Errorf("adjustment would create invalid zones")
	}
	
	// Connect to the neighbor to propose zone adjustment
	client, conn, err := ConnectToNode(ctx, neighborInfo.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to neighbor: %w", err)
	}
	defer conn.Close()
	
	// Create adjustment request
	req := &pb.AdjustZoneRequest{
		RequestingNodeId: string(s.Node.ID),
		ProposedNeighborZone: &pb.Zone{
			MinPoint: &pb.Point{Coordinates: neighborNewZone.MinPoint},
			MaxPoint: &pb.Point{Coordinates: neighborNewZone.MaxPoint},
		},
	}
	
	// Send the request
	resp, err := client.AdjustZone(ctx, req)
	if err != nil {
		return fmt.Errorf("zone adjustment request failed: %w", err)
	}
	
	if !resp.Success {
		return fmt.Errorf("neighbor rejected zone adjustment")
	}
	
	// Neighbor accepted, transfer relevant data
	adjustedKeys, err := s.TransferDataForZoneAdjustment(ctx, originalZone, ourNewZone, neighborInfo.Address)
	if err != nil {
		return fmt.Errorf("data transfer failed: %w", err)
	}
	
	// Update our zone
	s.mu.Lock()
	s.Node.Zone = ourNewZone
	s.mu.Unlock()
	
	// Record the adjustment
	loadBefore := s.LoadStats.GetCurrentLoad()
	
	// Estimate our new load after adjustment
	// Simplified: Assume load is proportional to zone volume
	oldVolume := calculateZoneVolume(originalZone)
	newVolume := calculateZoneVolume(ourNewZone)
	volumeRatio := newVolume / oldVolume
	
	loadAfter := loadBefore * volumeRatio
	
	// Record the adjustment
	s.LoadStats.RecordZoneAdjustment(originalZone, ourNewZone, "Hot spot mitigation", loadBefore, loadAfter)
	
	log.Printf("Successfully adjusted zone with neighbor %s, transferred %d keys", neighborID, adjustedKeys)
	return nil
}

// TransferDataForZoneAdjustment transfers data to a neighbor based on zone adjustment
func (s *CANServer) TransferDataForZoneAdjustment(ctx context.Context, oldZone, newZone *node.Zone, neighborAddress string) (int, error) {
	// Connect to the neighbor
	client, conn, err := ConnectToNode(ctx, neighborAddress)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to neighbor: %w", err)
	}
	defer conn.Close()
	
	// Get all keys in our store
	s.mu.RLock()
	allKeys := make([]string, 0, len(s.Node.Data))
	for key := range s.Node.Data {
		allKeys = append(allKeys, key)
	}
	s.mu.RUnlock()
	
	// Identify keys that need to be transferred
	keysToTransfer := make(map[string][]byte)
	for _, key := range allKeys {
		point := s.Router.HashToPoint(key)
		
		// If key is in old zone but not in new zone, it needs to be transferred
		if oldZone.Contains(point) && !newZone.Contains(point) {
			// Get the value
			value, err := s.Store.Get(key)
			if err != nil {
				log.Printf("Error getting value for key %s: %v", key, err)
				continue
			}
			
			keysToTransfer[key] = value
		}
	}
	
	// If no keys to transfer, we're done
	if len(keysToTransfer) == 0 {
		return 0, nil
	}
	
	// Send the data to the neighbor
	req := &pb.TransferDataRequest{
		Data: keysToTransfer,
	}
	
	_, err = client.TransferData(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to transfer data: %w", err)
	}
	
	// Delete transferred keys from our store
	for key := range keysToTransfer {
		if err := s.Store.Delete(key); err != nil {
			log.Printf("Error deleting key %s after transfer: %v", key, err)
		}
		
		// Also remove from cache
		if s.cache != nil {
			s.cache.Delete(key)
		}
	}
	
	return len(keysToTransfer), nil
}

// SelectRouteByStrategy selects a node based on the specified load balancing strategy
func (s *CANServer) SelectRouteByStrategy(ctx context.Context, candidates []*node.NeighborInfo, strategy LoadBalancingStrategy) (*node.NeighborInfo, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate nodes available")
	}
	
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	
	switch strategy {
	case RoundRobin:
		// Simple round-robin selection
		s.mu.Lock()
		if s.roundRobinCounter >= len(candidates) {
			s.roundRobinCounter = 0
		}
		selected := candidates[s.roundRobinCounter]
		s.roundRobinCounter++
		s.mu.Unlock()
		return selected, nil
		
	case LeastLoaded:
		// Get load info for all candidates
		loads := make(map[node.NodeID]float64)
		for _, candidate := range candidates {
			// Connect to the node
			client, conn, err := ConnectToNode(ctx, candidate.Address)
			if err != nil {
				log.Printf("Failed to connect to node %s: %v", candidate.ID, err)
				continue
			}
			
			// Get load stats
			req := &pb.GetLoadStatsRequest{}
			resp, err := client.GetLoadStats(ctx, req)
			conn.Close()
			
			if err != nil {
				log.Printf("Failed to get load stats from node %s: %v", candidate.ID, err)
				continue
			}
			
			loads[candidate.ID] = float64(resp.LoadPercentage)
		}
		
		// Find the least loaded node
		var leastLoaded *node.NeighborInfo
		minLoad := 100.0
		
		for _, candidate := range candidates {
			if load, exists := loads[candidate.ID]; exists {
				if load < minLoad {
					minLoad = load
					leastLoaded = candidate
				}
			}
		}
		
		if leastLoaded != nil {
			return leastLoaded, nil
		}
		
		// Fallback to random if we couldn't get load info
		return candidates[rand.Intn(len(candidates))], nil
		
	case Probabilistic:
		// Weight inversely proportional to load
		weights := make(map[node.NodeID]float64)
		totalWeight := 0.0
		
		// Get load info for all candidates
		for _, candidate := range candidates {
			// Connect to the node
			client, conn, err := ConnectToNode(ctx, candidate.Address)
			if err != nil {
				// Default weight if we can't get load
				weights[candidate.ID] = 1.0
				totalWeight += 1.0
				continue
			}
			
			// Get load stats
			req := &pb.GetLoadStatsRequest{}
			resp, err := client.GetLoadStats(ctx, req)
			conn.Close()
			
			if err != nil {
				// Default weight if we can't get load
				weights[candidate.ID] = 1.0
				totalWeight += 1.0
				continue
			}
			
			// Inverse of load (higher weight = lower load)
			load := float64(resp.LoadPercentage)
			if load < 1.0 {
				load = 1.0 // Avoid division by zero
			}
			weight := 100.0 / load
			weights[candidate.ID] = weight
			totalWeight += weight
		}
		
		// Select based on weights
		r := rand.Float64() * totalWeight
		currentSum := 0.0
		
		for _, candidate := range candidates {
			currentSum += weights[candidate.ID]
			if r <= currentSum {
				return candidate, nil
			}
		}
		
		// Should not reach here, but just in case
		return candidates[0], nil
		
	default: // ConsistentHashing or unknown strategy
		// For consistent hashing or as a fallback, use the first candidate
		return candidates[0], nil
	}
}

// isValidZone checks if a zone is valid
func isValidZone(zone *node.Zone) bool {
	dimensions := zone.MinPoint.Dimensions()
	
	for dim := 0; dim < dimensions; dim++ {
		// Check that min < max for each dimension
		if zone.MinPoint[dim] >= zone.MaxPoint[dim] {
			return false
		}
		
		// Check that coordinates are within bounds [0,1]
		if zone.MinPoint[dim] < 0 || zone.MinPoint[dim] > 1 ||
		   zone.MaxPoint[dim] < 0 || zone.MaxPoint[dim] > 1 {
			return false
		}
	}
	
	return true
}

// canAdjustInDimension checks if two zones can be adjusted in a specific dimension
func canAdjustInDimension(zone1, zone2 *node.Zone, dimension int) bool {
	dimensions := zone1.MinPoint.Dimensions()
	if dimension >= dimensions {
		return false
	}
	
	// Zones can be adjusted if they share (d-1) dimensional hyperplane
	// This means they're adjacent in the dimension we want to adjust
	
	// Check if they're adjacent in this dimension
	if zone1.MaxPoint[dimension] == zone2.MinPoint[dimension] ||
	   zone2.MaxPoint[dimension] == zone1.MinPoint[dimension] {
		
		// Check if they span the same range in all other dimensions
		for d := 0; d < dimensions; d++ {
			if d == dimension {
				continue
			}
			
			// If range doesn't overlap in other dimensions, they're not adjacent
			if zone1.MaxPoint[d] <= zone2.MinPoint[d] ||
			   zone2.MaxPoint[d] <= zone1.MinPoint[d] {
				return false
			}
		}
		
		return true
	}
	
	return false
}

// calculateZoneVolume calculates the volume of a zone
func calculateZoneVolume(zone *node.Zone) float64 {
	volume := 1.0
	for dim := 0; dim < zone.MinPoint.Dimensions(); dim++ {
		volume *= (zone.MaxPoint[dim] - zone.MinPoint[dim])
	}
	return volume
}

// BalanceZones tries to balance the size of zones in the network
func (s *CANServer) BalanceZones(ctx context.Context) error {
	// Collect information about all nodes in the system
	nodes, err := s.CollectNetworkNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect network nodes: %w", err)
	}
	
	// Calculate zone volumes
	nodeVolumes := make(map[string]float64)
	var totalVolume float64
	
	for id, nodeInfo := range nodes {
		volume := calculateZoneVolume(nodeInfo.Zone)
		nodeVolumes[id] = volume
		totalVolume += volume
	}
	
	// Calculate ideal volume per node
	nodeCount := len(nodes)
	if nodeCount == 0 {
		return fmt.Errorf("no nodes found in network")
	}
	
	idealVolume := totalVolume / float64(nodeCount)
	
	// Sort nodes by volume
	type NodeVolume struct {
		ID     string
		Volume float64
	}
	
	nodesByVolume := make([]NodeVolume, 0, len(nodes))
	for id, volume := range nodeVolumes {
		nodesByVolume = append(nodesByVolume, NodeVolume{ID: id, Volume: volume})
	}
	
	sort.Slice(nodesByVolume, func(i, j int) bool {
		return nodesByVolume[i].Volume > nodesByVolume[j].Volume
	})
	
	// Try to balance zones by transferring space from largest to smallest
	if len(nodesByVolume) < 2 {
		return nil // Nothing to balance
	}
	
	// Focus on the most unbalanced nodes
	largestNode := nodesByVolume[0]
	smallestNode := nodesByVolume[len(nodesByVolume)-1]
	
	// If the ratio between largest and smallest is too high
	if largestNode.Volume / smallestNode.Volume > 1.5 {
		// Try to balance these two nodes
		return s.BalanceNodePair(ctx, nodes[largestNode.ID], nodes[smallestNode.ID])
	}
	
	return nil
}

// BalanceNodePair attempts to balance the zones of two nodes
func (s *CANServer) BalanceNodePair(ctx context.Context, largeNode, smallNode *node.NeighborInfo) error {
	// This is a simplified implementation
	// In a real system, you would:
	// 1. Check if the nodes are adjacent
	// 2. Calculate a new boundary that balances the volumes
	// 3. Coordinate the transfer of responsibility
	// 4. Transfer the affected data
	
	log.Printf("Attempting to balance nodes %s and %s", largeNode.ID, smallNode.ID)
	
	// For now, just log that we tried
	return fmt.Errorf("zone balancing not fully implemented")
}

// CollectNetworkNodes collects information about all nodes in the network
func (s *CANServer) CollectNetworkNodes(ctx context.Context) (map[string]*node.NeighborInfo, error) {
	// Initialize with our own node
	s.mu.RLock()
	allNodes := make(map[string]*node.NeighborInfo)
	allNodes[string(s.Node.ID)] = &node.NeighborInfo{
		ID:      s.Node.ID,
		Address: s.Node.Address,
		Zone:    s.Node.Zone,
	}
	
	// Add direct neighbors
	for id, info := range s.Node.Neighbors {
		allNodes[string(id)] = info
	}
	s.mu.RUnlock()
	
	// Queue for BFS traversal
	visited := make(map[string]bool)
	visited[string(s.Node.ID)] = true
	
	queue := make([]string, 0)
	for id := range allNodes {
		if id != string(s.Node.ID) {
			queue = append(queue, id)
			visited[id] = true
		}
	}
	
	// BFS to discover all nodes
	for len(queue) > 0 {
		// Dequeue
		currentID := queue[0]
		queue = queue[1:]
		
		// Skip if we don't have info for this node
		nodeInfo, exists := allNodes[currentID]
		if !exists {
			continue
		}
		
		// Connect to this node
		client, conn, err := ConnectToNode(ctx, nodeInfo.Address)
		if err != nil {
			log.Printf("Failed to connect to node %s: %v", currentID, err)
			continue
		}
		
		// Get neighbors
		req := &pb.GetNeighborsRequest{}
		resp, err := client.GetNeighbors(ctx, req)
		conn.Close()
		
		if err != nil {
			log.Printf("Failed to get neighbors from node %s: %v", currentID, err)
			continue
		}
		
		// Process neighbors
		for _, nbr := range resp.Neighbors {
			if _, exists := visited[nbr.Id]; !exists {
				// Create zone
				minPoint := make(node.Point, len(nbr.Zone.MinPoint.Coordinates))
				maxPoint := make(node.Point, len(nbr.Zone.MaxPoint.Coordinates))
				
				copy(minPoint, nbr.Zone.MinPoint.Coordinates)
				copy(maxPoint, nbr.Zone.MaxPoint.Coordinates)
				
				zone, err := node.NewZone(minPoint, maxPoint)
				if err != nil {
					log.Printf("Failed to create zone for node %s: %v", nbr.Id, err)
					continue
				}
				
				// Add to all nodes
				allNodes[nbr.Id] = &node.NeighborInfo{
					ID:      node.NodeID(nbr.Id),
					Address: nbr.Address,
					Zone:    zone,
				}
				
				// Add to queue
				queue = append(queue, nbr.Id)
				visited[nbr.Id] = true
			}
		}
	}
	
	return allNodes, nil
}

// LoadBalancer handles load balancing for the CAN DHT
type LoadBalancer struct {
	// Reference to the CAN server
	server *CANServer
	
	// Load statistics
	stats *LoadStats
	
	// Load threshold for considering a node overloaded (0-1)
	overloadThreshold float64
	
	// Minimum time between zone adjustments
	minAdjustmentInterval time.Duration
	
	// Maximum number of consecutive overloads before forcing adjustment
	maxConsecutiveOverloads int
	
	// Hot key threshold
	hotKeyThreshold int
	
	// Rate at which to sample load
	loadSampleRate time.Duration
	
	// Minimum zone size (fraction of coordinate space)
	minZoneSize float64
	
	// Maximum zone size (fraction of coordinate space)
	maxZoneSize float64
	
	// Stop channel for background monitoring
	stop chan struct{}
}

// NewLoadBalancer creates a new load balancer
func NewLoadBalancer(server *CANServer) *LoadBalancer {
	stats := &LoadStats{
		NodeLoad:           0.0,
		KeyAccessCounts:    make(map[string]int),
		KeyLastAccessed:    make(map[string]time.Time),
		ZoneSize:           1.0, // Assuming default zone size
		OperationsPerSecond: 0.0,
		LoadHistory:        make([]float64, 0, 100), // Keep the last 100 load samples
		LastZoneAdjustment: time.Now().Add(-1 * time.Hour), // Start with ability to adjust immediately
	}
	
	lb := &LoadBalancer{
		server:                 server,
		stats:                  stats,
		overloadThreshold:      0.8,                    // 80% load is considered overloaded
		minAdjustmentInterval:  10 * time.Minute,       // Allow adjustment every 10 minutes
		maxConsecutiveOverloads: 5,                     // Allow 5 consecutive overloads before forcing adjustment
		hotKeyThreshold:        server.Config.HotKeyThreshold,
		loadSampleRate:         5 * time.Second,        // Sample load every 5 seconds
		minZoneSize:            0.05,                   // Minimum 5% of coordinate space
		maxZoneSize:            0.5,                    // Maximum 50% of coordinate space
		stop:                   make(chan struct{}),
	}
	
	// Start background monitoring
	go lb.monitorLoad()
	
	return lb
}

// Stop stops the load balancer
func (lb *LoadBalancer) Stop() {
	close(lb.stop)
}

// TrackKeyAccess tracks access to a key for hot key detection
func (lb *LoadBalancer) TrackKeyAccess(key string, isWrite bool) {
	lb.stats.mu.Lock()
	defer lb.stats.mu.Unlock()
	
	// Update access count
	count, exists := lb.stats.KeyAccessCounts[key]
	if !exists {
		count = 0
	}
	
	// Writes count more toward "hotness" than reads
	if isWrite {
		count += 2
	} else {
		count++
	}
	
	lb.stats.KeyAccessCounts[key] = count
	lb.stats.KeyLastAccessed[key] = time.Now()
	
	// Check if this is a hot key and should be replicated
	if count > lb.hotKeyThreshold && count%lb.server.Config.HotKeyCheckInterval == 0 {
		go lb.replicateHotKey(key)
	}
}

// IsOverloaded returns true if the node is currently overloaded
func (lb *LoadBalancer) IsOverloaded() bool {
	lb.stats.mu.RLock()
	defer lb.stats.mu.RUnlock()
	
	return lb.stats.NodeLoad > lb.overloadThreshold
}

// GetCurrentLoad returns the current load of the node
func (lb *LoadBalancer) GetCurrentLoad() float64 {
	lb.stats.mu.RLock()
	defer lb.stats.mu.RUnlock()
	
	return lb.stats.NodeLoad
}

// GetHotKeys returns a list of hot keys on this node
func (lb *LoadBalancer) GetHotKeys() []string {
	lb.stats.mu.RLock()
	defer lb.stats.mu.RUnlock()
	
	hotKeys := make([]string, 0)
	for key, count := range lb.stats.KeyAccessCounts {
		if count > lb.hotKeyThreshold {
			hotKeys = append(hotKeys, key)
		}
	}
	
	return hotKeys
}

// AdjustZoneForLoad performs zone adjustment for load balancing
func (lb *LoadBalancer) AdjustZoneForLoad(ctx context.Context) error {
	lb.stats.mu.Lock()
	
	// Check if zone adjustment is allowed
	if time.Since(lb.stats.LastZoneAdjustment) < lb.minAdjustmentInterval && 
	   lb.stats.ConsecutiveOverloads < lb.maxConsecutiveOverloads {
		lb.stats.mu.Unlock()
		return fmt.Errorf("zone adjustment not allowed: too soon since last adjustment")
	}
	
	// Check if we are already in a zone transfer
	if lb.stats.ActiveZoneTransfers > 0 {
		lb.stats.mu.Unlock()
		return fmt.Errorf("zone adjustment not allowed: already in a zone transfer")
	}
	
	// Mark that we're starting a zone transfer
	lb.stats.ActiveZoneTransfers++
	lb.stats.LastZoneAdjustment = time.Now()
	lb.stats.mu.Unlock()
	
	// Find the least loaded neighbor
	leastLoadedNeighbor, err := lb.findLeastLoadedNeighbor(ctx)
	if err != nil {
		lb.stats.mu.Lock()
		lb.stats.ActiveZoneTransfers--
		lb.stats.mu.Unlock()
		return fmt.Errorf("failed to find least loaded neighbor: %w", err)
	}
	
	// Determine how to split the zone with the neighbor
	newZone, err := lb.calculateZoneSplit(leastLoadedNeighbor)
	if err != nil {
		lb.stats.mu.Lock()
		lb.stats.ActiveZoneTransfers--
		lb.stats.mu.Unlock()
		return fmt.Errorf("failed to calculate zone split: %w", err)
	}
	
	// Execute the zone transfer
	err = lb.executeZoneTransfer(ctx, leastLoadedNeighbor, newZone)
	if err != nil {
		lb.stats.mu.Lock()
		lb.stats.ActiveZoneTransfers--
		lb.stats.mu.Unlock()
		return fmt.Errorf("failed to execute zone transfer: %w", err)
	}
	
	// Update our load stats now that zone is adjusted
	lb.stats.mu.Lock()
	lb.stats.ActiveZoneTransfers--
	lb.stats.ZoneSize = calculateZoneSize(lb.server.Node.Zone, lb.server.Router.Dimensions)
	lb.stats.ConsecutiveOverloads = 0 // Reset overload counter
	lb.stats.mu.Unlock()
	
	return nil
}

// findLeastLoadedNeighbor finds the least loaded neighbor
func (lb *LoadBalancer) findLeastLoadedNeighbor(ctx context.Context) (*node.NeighborInfo, error) {
	lb.server.Node.RLock()
	defer lb.server.Node.RUnlock()
	
	if len(lb.server.Node.Neighbors) == 0 {
		return nil, fmt.Errorf("no neighbors available")
	}
	
	var (
		leastLoaded *node.NeighborInfo
		minLoad     = 1.1 // Start higher than max possible load (1.0)
	)
	
	// Check each neighbor's load
	for _, neighbor := range lb.server.Node.Neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s: %v", neighbor.ID, err)
			continue
		}
		defer conn.Close()
		
		// Request the neighbor's load
		resp, err := client.GetLoadStats(ctx, &pb.GetLoadStatsRequest{})
		if err != nil {
			log.Printf("Failed to get load stats from neighbor %s: %v", neighbor.ID, err)
			continue
		}
		
		// Check if this is the least loaded neighbor
		if resp.NodeLoad < minLoad {
			minLoad = resp.NodeLoad
			leastLoaded = neighbor
		}
	}
	
	if leastLoaded == nil {
		return nil, fmt.Errorf("could not find any responsive neighbors")
	}
	
	return leastLoaded, nil
}

// calculateZoneSplit calculates how to split the current zone with a neighbor
func (lb *LoadBalancer) calculateZoneSplit(neighbor *node.NeighborInfo) (*node.Zone, error) {
	// Determine the dimension with the largest range to split on
	splitDimension := 0
	maxRange := 0.0
	
	for dim := 0; dim < lb.server.Router.Dimensions; dim++ {
		dimRange := lb.server.Node.Zone.MaxPoint[dim] - lb.server.Node.Zone.MinPoint[dim]
		if dimRange > maxRange {
			maxRange = dimRange
			splitDimension = dim
		}
	}
	
	// Calculate the split point - give 30% of our zone to the neighbor
	// This percentage can be adjusted based on how overloaded we are
	overloadFactor := min(1.0, lb.GetCurrentLoad()/lb.overloadThreshold)
	transferPercentage := 0.3 * overloadFactor
	
	splitPoint := lb.server.Node.Zone.MinPoint[splitDimension] + 
		transferPercentage * (lb.server.Node.Zone.MaxPoint[splitDimension] - lb.server.Node.Zone.MinPoint[splitDimension])
	
	// Create the new zone for the neighbor
	minPoint := make(node.Point, lb.server.Router.Dimensions)
	maxPoint := make(node.Point, lb.server.Router.Dimensions)
	
	copy(minPoint, lb.server.Node.Zone.MinPoint)
	copy(maxPoint, lb.server.Node.Zone.MaxPoint)
	
	// Adjust the split dimension
	maxPoint[splitDimension] = splitPoint
	
	// Create the new zone
	newZone, err := node.NewZone(minPoint, maxPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create new zone: %w", err)
	}
	
	return newZone, nil
}

// executeZoneTransfer transfers a portion of our zone to a neighbor
func (lb *LoadBalancer) executeZoneTransfer(ctx context.Context, neighbor *node.NeighborInfo, newZone *node.Zone) error {
	// Connect to the neighbor
	client, conn, err := ConnectToNode(ctx, neighbor.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to neighbor %s: %v", neighbor.ID, err)
	}
	defer conn.Close()
	
	// Find keys in the zone to transfer
	keysToTransfer := make(map[string][]byte)
	lb.server.Node.Lock()
	
	for key, value := range lb.server.Node.Data {
		keyPoint := lb.server.Router.HashToPoint(key)
		if newZone.Contains(keyPoint) {
			keysToTransfer[key] = []byte(value)
		}
	}
	
	// Request the neighbor to accept the zone
	req := &pb.AdjustZoneRequest{
		SourceNodeId: string(lb.server.Node.ID),
		NewZone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: newZone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: newZone.MaxPoint,
			},
		},
		Keys: keysToTransfer,
	}
	
	resp, err := client.AdjustZone(ctx, req)
	if err != nil {
		lb.server.Node.Unlock()
		return fmt.Errorf("neighbor rejected zone transfer: %w", err)
	}
	
	if !resp.Success {
		lb.server.Node.Unlock()
		return fmt.Errorf("neighbor rejected zone transfer: %s", resp.Error)
	}
	
	// Update our zone
	oldMinPoint := make(node.Point, len(lb.server.Node.Zone.MinPoint))
	copy(oldMinPoint, lb.server.Node.Zone.MinPoint)
	
	// Adjust our zone
	lb.server.Node.Zone.MinPoint[newZone.MaxPoint.DimensionWithMaxRange()] = newZone.MaxPoint[newZone.MaxPoint.DimensionWithMaxRange()]
	
	// Remove transferred keys from our data
	for key := range keysToTransfer {
		delete(lb.server.Node.Data, key)
	}
	
	lb.server.Node.Unlock()
	
	// Notify other neighbors about our zone change
	go lb.notifyNeighborsOfZoneChange()
	
	log.Printf("Zone transfer complete: transferred %d keys to node %s", len(keysToTransfer), neighbor.ID)
	
	return nil
}

// replicateHotKey creates replicas of a hot key on neighboring nodes
func (lb *LoadBalancer) replicateHotKey(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	lb.server.Node.RLock()
	value, exists := lb.server.Node.Data[key]
	lb.server.Node.RUnlock()
	
	if !exists {
		log.Printf("Hot key %s no longer exists, skipping replication", key)
		return
	}
	
	// Get a random subset of neighbors to replicate to
	neighbors := lb.server.Node.GetNeighbors()
	if len(neighbors) == 0 {
		log.Printf("No neighbors available for hot key replication")
		return
	}
	
	// Determine how many replicas to create based on access frequency
	lb.stats.mu.RLock()
	accessCount := lb.stats.KeyAccessCounts[key]
	lb.stats.mu.RUnlock()
	
	// Scale replicas with access count, but cap at maxReplicas and number of neighbors
	maxReplicas := lb.server.Config.MaxHotKeyReplicas
	numReplicas := min(maxReplicas, accessCount/lb.hotKeyThreshold)
	numReplicas = min(numReplicas, len(neighbors))
	
	if numReplicas <= 0 {
		numReplicas = 1 // Always create at least one replica
	}
	
	// Shuffle the neighbors to pick random ones
	neighborList := make([]*node.NeighborInfo, 0, len(neighbors))
	for _, n := range neighbors {
		neighborList = append(neighborList, n)
	}
	shuffleNeighbors(neighborList)
	
	// Replicate to the chosen neighbors
	successCount := 0
	for i := 0; i < numReplicas && i < len(neighborList); i++ {
		neighbor := neighborList[i]
		
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for hot key replication: %v", neighbor.ID, err)
			continue
		}
		
		// Send the hot key to the neighbor
		req := &pb.PutRequest{
			Key:      key,
			Value:    []byte(value),
			IsHotKey: true,
		}
		
		resp, err := client.Put(ctx, req)
		conn.Close()
		
		if err != nil {
			log.Printf("Failed to replicate hot key %s to neighbor %s: %v", key, neighbor.ID, err)
			continue
		}
		
		if !resp.Success {
			log.Printf("Neighbor %s rejected hot key replication: %s", neighbor.ID, resp.Error)
			continue
		}
		
		successCount++
	}
	
	log.Printf("Hot key %s replicated to %d/%d neighbors", key, successCount, numReplicas)
}

// monitorLoad periodically samples the node's load
func (lb *LoadBalancer) monitorLoad() {
	ticker := time.NewTicker(lb.loadSampleRate)
	defer ticker.Stop()
	
	// Track operations per second
	var (
		lastSampleTime      = time.Now()
		lastTotalOperations = 0
		windowSamples       = 0 // Number of samples in the current window
	)
	
	for {
		select {
		case <-ticker.C:
			// Calculate the current load
			currentLoad := lb.calculateCurrentLoad()
			
			lb.stats.mu.Lock()
			
			// Add to load history
			lb.stats.LoadHistory = append(lb.stats.LoadHistory, currentLoad)
			if len(lb.stats.LoadHistory) > 100 {
				// Keep only last 100 samples
				lb.stats.LoadHistory = lb.stats.LoadHistory[1:]
			}
			
			// Calculate operations per second
			cacheMetrics := lb.server.cache.GetMetrics()
			totalOps := cacheMetrics.TotalOperations
			timeDiff := time.Since(lastSampleTime).Seconds()
			if timeDiff > 0 {
				opsPerSec := float64(totalOps-lastTotalOperations) / timeDiff
				
				// Use an exponential moving average to smooth out operations per second
				if lb.stats.OperationsPerSecond == 0 {
					lb.stats.OperationsPerSecond = opsPerSec
				} else {
					lb.stats.OperationsPerSecond = 0.9*lb.stats.OperationsPerSecond + 0.1*opsPerSec
				}
			}
			
			// Save for next iteration
			lastTotalOperations = totalOps
			lastSampleTime = time.Now()
			
			// Check if overloaded
			oldLoad := lb.stats.NodeLoad
			lb.stats.NodeLoad = currentLoad
			
			if currentLoad > lb.overloadThreshold {
				if oldLoad > lb.overloadThreshold {
					// Still overloaded
					lb.stats.ConsecutiveOverloads++
				} else {
					// Newly overloaded
					lb.stats.ConsecutiveOverloads = 1
				}
				
				lb.stats.LastOverloadTime = time.Now()
				
				// Check if we need to force a zone adjustment
				if lb.stats.ConsecutiveOverloads >= lb.maxConsecutiveOverloads {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						
						err := lb.AdjustZoneForLoad(ctx)
						if err != nil {
							log.Printf("Failed to adjust zone after consecutive overloads: %v", err)
						}
					}()
				}
			} else {
				// Not overloaded, reset counter
				lb.stats.ConsecutiveOverloads = 0
			}
			
			// Update hot keys - age out old hot keys
			now := time.Now()
			for key, lastAccessed := range lb.stats.KeyLastAccessed {
				if now.Sub(lastAccessed) > lb.server.Config.HotKeyTTL {
					delete(lb.stats.KeyAccessCounts, key)
					delete(lb.stats.KeyLastAccessed, key)
				}
			}
			
			lb.stats.mu.Unlock()
			
		case <-lb.stop:
			return
		}
	}
}

// calculateCurrentLoad calculates the current load of the node
func (lb *LoadBalancer) calculateCurrentLoad() float64 {
	// Combine multiple factors into a load score:
	// 1. CPU/memory usage (not directly available, use operation rate as proxy)
	// 2. Zone size relative to ideal
	// 3. Number of hot keys
	// 4. Operation rate
	
	// Get cache metrics
	cacheMetrics := lb.server.cache.GetMetrics()
	
	lb.stats.mu.RLock()
	defer lb.stats.mu.RUnlock()
	
	// 1. Operation rate factor (0-1)
	opRateFactor := min(1.0, lb.stats.OperationsPerSecond/1000.0) // Assume 1000 ops/sec is max
	
	// 2. Zone size factor (0-1)
	// Penalize both very large and very small zones
	idealZoneSize := 1.0 / float64(len(lb.server.Node.Neighbors)+1) // Ideal is equal distribution
	zoneSizeFactor := 0.0
	if lb.stats.ZoneSize > idealZoneSize {
		// Larger than ideal
		zoneSizeFactor = min(1.0, lb.stats.ZoneSize/(2*idealZoneSize))
	} else {
		// Smaller than ideal - smaller penalty
		zoneSizeFactor = max(0.0, 1.0-idealZoneSize/lb.stats.ZoneSize)
	}
	
	// 3. Hot keys factor (0-1)
	hotKeyCount := 0
	for _, count := range lb.stats.KeyAccessCounts {
		if count > lb.hotKeyThreshold {
			hotKeyCount++
		}
	}
	hotKeyFactor := min(1.0, float64(hotKeyCount)/20.0) // Assume 20 hot keys is max
	
	// 4. Data count factor (0-1)
	dataCountFactor := min(1.0, float64(len(lb.server.Node.Data))/10000.0) // Assume 10000 items is max
	
	// Combine factors with weights
	totalLoad := (opRateFactor*0.4 + zoneSizeFactor*0.2 + hotKeyFactor*0.2 + dataCountFactor*0.2)
	
	// Bound between 0 and 1
	return min(1.0, max(0.0, totalLoad))
}

// GetLoadStats returns current load statistics for the node
func (lb *LoadBalancer) GetLoadStats() *pb.GetLoadStatsResponse {
	lb.stats.mu.RLock()
	defer lb.stats.mu.RUnlock()
	
	// Count hot keys
	hotKeyCount := 0
	for _, count := range lb.stats.KeyAccessCounts {
		if count > lb.hotKeyThreshold {
			hotKeyCount++
		}
	}
	
	return &pb.GetLoadStatsResponse{
		NodeLoad:            lb.stats.NodeLoad,
		ZoneSize:            lb.stats.ZoneSize,
		OperationsPerSecond: lb.stats.OperationsPerSecond,
		HotKeyCount:         int32(hotKeyCount),
		DataItemCount:       int32(len(lb.server.Node.Data)),
	}
}

// notifyNeighborsOfZoneChange notifies neighbors about our zone change
func (lb *LoadBalancer) notifyNeighborsOfZoneChange() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	lb.server.Node.RLock()
	ourNodeInfo := &pb.NodeInfo{
		Id:      string(lb.server.Node.ID),
		Address: lb.server.Node.Address,
		Zone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: lb.server.Node.Zone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: lb.server.Node.Zone.MaxPoint,
			},
		},
	}
	neighbors := lb.server.Node.GetNeighbors()
	lb.server.Node.RUnlock()
	
	// Notify each neighbor
	for _, neighbor := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for zone update: %v", neighbor.ID, err)
			continue
		}
		
		// Send the update
		req := &pb.UpdateNeighborsRequest{
			NodeId: string(lb.server.Node.ID),
			Neighbors: []*pb.NodeInfo{
				ourNodeInfo,
			},
		}
		
		_, err = client.UpdateNeighbors(ctx, req)
		conn.Close()
		
		if err != nil {
			log.Printf("Failed to notify neighbor %s about zone change: %v", neighbor.ID, err)
			continue
		}
	}
}

// Helper functions

// calculateZoneSize calculates the size of a zone relative to the entire coordinate space
func calculateZoneSize(zone *node.Zone, dimensions int) float64 {
	size := 1.0
	for dim := 0; dim < dimensions; dim++ {
		size *= (zone.MaxPoint[dim] - zone.MinPoint[dim])
	}
	return size
}

// shuffleNeighbors randomly shuffles a slice of neighbors
func shuffleNeighbors(neighbors []*node.NeighborInfo) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := len(neighbors) - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
	}
}

// min returns the minimum of two float64 values
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two float64 values
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

