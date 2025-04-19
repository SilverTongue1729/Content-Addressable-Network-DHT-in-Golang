package loadbalancer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kevaljain/Content-Addressable-Network-DHT-in-Golang/can-dht/pkg/node"
	"github.com/kevaljain/Content-Addressable-Network-DHT-in-Golang/can-dht/pkg/pb"
)

// LoadBalancer manages zone balancing and fault tolerance in the CAN network
type LoadBalancer struct {
	// Node information and connection details
	nodeID    node.ID
	address   string
	getClient func(address string) (pb.CANServiceClient, error)
	
	// Current state of the node
	currentLoad   int
	maxLoad       int
	extraZones    []*node.Zone
	extraZoneLock sync.RWMutex
	
	// Metrics for tracking load balancing
	metrics *LoadBalancingMetrics
	
	// Channel for load balancing signals
	balanceChan chan struct{}
	
	// For controlling the periodic load checking
	balanceInterval time.Duration
	stopChan        chan struct{}

	// New fields for hot key detection and zone adjustment
	mu sync.RWMutex
	KeyAccessCounts map[string]int
	KeyAccessTimes  map[string]time.Time
	HotKeys         map[string]HotKeyInfo
	HotKeyThreshold int
	HotKeyCheckInterval time.Duration
	HotKeyTTL         time.Duration
	HotKeyRateThreshold float64
	MaxHotKeyReplicas int
	PruneInterval     time.Duration
}

// LoadBalancingMetrics tracks metrics related to load balancing
type LoadBalancingMetrics struct {
	// Zone-related metrics
	ZoneCount     int
	ZoneVolume    float64
	ZoneVariance  float64
	
	// Load-related metrics
	DataItemCount int
	LoadVariance  float64
	
	// Fault tolerance metrics
	FailedNodeCount     int
	ReassignmentCount   int
	RecoveryTimeMillis  int64
	
	// Synchronization
	mu sync.RWMutex
}

// HotKeyInfo tracks information about a hot key
type HotKeyInfo struct {
	Key           string
	AccessCount   int
	LastAccess    time.Time
	ReplicaCount  int
	ReplicaNodes  []string // NodeIDs where this key is replicated
}

// New creates a new LoadBalancer
func New(nodeID node.ID, address string, getClient func(address string) (pb.CANServiceClient, error)) *LoadBalancer {
	return &LoadBalancer{
		nodeID:          nodeID,
		address:         address,
		getClient:       getClient,
		currentLoad:     0,
		maxLoad:         1000, // Default max load, can be configured
		extraZones:      make([]*node.Zone, 0),
		metrics:         &LoadBalancingMetrics{},
		balanceChan:     make(chan struct{}, 10),
		balanceInterval: 30 * time.Second,
		stopChan:        make(chan struct{}),
		KeyAccessCounts: make(map[string]int),
		KeyAccessTimes:  make(map[string]time.Time),
		HotKeys:         make(map[string]HotKeyInfo),
		HotKeyThreshold: 10,
		HotKeyCheckInterval: 10 * time.Second,
		HotKeyTTL:         1 * time.Minute,
		HotKeyRateThreshold: 10,
		MaxHotKeyReplicas: 3,
		PruneInterval:     1 * time.Hour,
	}
}

// Start begins the load balancer's periodic operations
func (lb *LoadBalancer) Start(ctx context.Context) {
	go lb.balanceLoop(ctx)
}

// Stop stops the load balancer's operations
func (lb *LoadBalancer) Stop() {
	close(lb.stopChan)
}

// AddExtraZone adds a zone to this node's responsibility (usually after node failure)
func (lb *LoadBalancer) AddExtraZone(zone *node.Zone) {
	lb.extraZoneLock.Lock()
	defer lb.extraZoneLock.Unlock()
	
	lb.extraZones = append(lb.extraZones, zone)
	log.Printf("Added extra zone to node %s, now responsible for %d extra zones", lb.nodeID, len(lb.extraZones))
	
	// Trigger load balancing
	select {
	case lb.balanceChan <- struct{}{}:
		// Signal sent
	default:
		// Channel full, which is fine - balancing will happen eventually
	}
}

// HasExtraZones returns true if this node is responsible for extra zones
func (lb *LoadBalancer) HasExtraZones() bool {
	lb.extraZoneLock.RLock()
	defer lb.extraZoneLock.RUnlock()
	
	return len(lb.extraZones) > 0
}

// GetExtraZones returns a copy of the extra zones
func (lb *LoadBalancer) GetExtraZones() []*node.Zone {
	lb.extraZoneLock.RLock()
	defer lb.extraZoneLock.RUnlock()
	
	zones := make([]*node.Zone, len(lb.extraZones))
	copy(zones, lb.extraZones)
	return zones
}

// RemoveExtraZone removes a zone from extra zones (after reassignment)
func (lb *LoadBalancer) RemoveExtraZone(zone *node.Zone) bool {
	lb.extraZoneLock.Lock()
	defer lb.extraZoneLock.Unlock()
	
	for i, z := range lb.extraZones {
		if z.Equals(zone) {
			// Remove from slice
			lb.extraZones = append(lb.extraZones[:i], lb.extraZones[i+1:]...)
			log.Printf("Removed extra zone from node %s, now responsible for %d extra zones", lb.nodeID, len(lb.extraZones))
			return true
		}
	}
	
	return false
}

// SetCurrentLoad updates the current load metric
func (lb *LoadBalancer) SetCurrentLoad(load int) {
	lb.currentLoad = load
}

// SetMaxLoad updates the maximum load threshold
func (lb *LoadBalancer) SetMaxLoad(maxLoad int) {
	lb.maxLoad = maxLoad
}

// IsOverloaded checks if the node is overloaded
func (lb *LoadBalancer) IsOverloaded() bool {
	return lb.currentLoad > lb.maxLoad
}

// UpdateNodeFailure updates metrics when a node failure is detected
func (lb *LoadBalancer) UpdateNodeFailure() {
	lb.metrics.mu.Lock()
	defer lb.metrics.mu.Unlock()
	
	lb.metrics.FailedNodeCount++
}

// UpdateZoneReassignment updates metrics when a zone is reassigned
func (lb *LoadBalancer) UpdateZoneReassignment() {
	lb.metrics.mu.Lock()
	defer lb.metrics.mu.Unlock()
	
	lb.metrics.ReassignmentCount++
}

// GetMetrics returns a copy of the current metrics
func (lb *LoadBalancer) GetMetrics() LoadBalancingMetrics {
	lb.metrics.mu.RLock()
	defer lb.metrics.mu.RUnlock()
	
	return *lb.metrics
}

// balanceLoop is the main loop for periodic load balancing
func (lb *LoadBalancer) balanceLoop(ctx context.Context) {
	ticker := time.NewTicker(lb.balanceInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-lb.stopChan:
			return
		case <-ticker.C:
			// Periodic check
			lb.checkAndBalance(ctx)
		case <-lb.balanceChan:
			// Triggered balance (e.g., after adding extra zone)
			lb.checkAndBalance(ctx)
		}
	}
}

// checkAndBalance checks if load balancing is needed and performs it
func (lb *LoadBalancer) checkAndBalance(ctx context.Context) {
	// Check if we have extra zones to reassign
	lb.extraZoneLock.RLock()
	extraZoneCount := len(lb.extraZones)
	lb.extraZoneLock.RUnlock()
	
	if extraZoneCount > 0 {
		log.Printf("Node %s has %d extra zones, attempting reassignment", lb.nodeID, extraZoneCount)
		lb.reassignExtraZones(ctx)
	}
	
	// Check if we're overloaded and need to split our zone
	if lb.IsOverloaded() {
		log.Printf("Node %s is overloaded (load: %d, max: %d), considering zone splitting", 
			lb.nodeID, lb.currentLoad, lb.maxLoad)
		// Implementation of zone splitting could go here
	}
}

// reassignExtraZones attempts to reassign extra zones to suitable neighbors
func (lb *LoadBalancer) reassignExtraZones(ctx context.Context) {
	// Get our neighbors
	neighbors, err := lb.getNeighbors(ctx)
	if err != nil {
		log.Printf("Failed to get neighbors for reassignment: %v", err)
		return
	}
	
	if len(neighbors) == 0 {
		log.Printf("No neighbors available for zone reassignment")
		return
	}
	
	// For each extra zone, try to find the best neighbor to reassign to
	lb.extraZoneLock.Lock()
	defer lb.extraZoneLock.Unlock()
	
	// Nothing to do if we have no extra zones
	if len(lb.extraZones) == 0 {
		return
	}
	
	// Process each extra zone
	var remainingZones []*node.Zone
	
	for _, zone := range lb.extraZones {
		// Find the best neighbor for this zone
		bestNeighbor, score := lb.findBestNeighborForZone(zone, neighbors)
		
		if bestNeighbor == nil || score < 0.3 { // Threshold for a good match
			log.Printf("No suitable neighbor found for zone, keeping it for now")
			remainingZones = append(remainingZones, zone)
			continue
		}
		
		// Try to transfer the zone
		success := lb.transferZoneToNeighbor(ctx, zone, bestNeighbor)
		if !success {
			remainingZones = append(remainingZones, zone)
		}
	}
	
	// Update our list of extra zones
	lb.extraZones = remainingZones
}

// getNeighbors fetches the current neighbors of this node
func (lb *LoadBalancer) getNeighbors(ctx context.Context) ([]*pb.NodeInfo, error) {
	// Connect to ourselves to get neighbors
	client, err := lb.getClient(lb.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create client to self: %w", err)
	}
	
	resp, err := client.GetNeighbors(ctx, &pb.GetNeighborsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get neighbors: %w", err)
	}
	
	return resp.Neighbors, nil
}

// findBestNeighborForZone finds the most suitable neighbor for a given zone
func (lb *LoadBalancer) findBestNeighborForZone(zone *node.Zone, neighbors []*pb.NodeInfo) (*pb.NodeInfo, float64) {
	if len(neighbors) == 0 {
		return nil, -1
	}
	
	type scoredNeighbor struct {
		neighbor *pb.NodeInfo
		score    float64
	}
	
	// Calculate a score for each neighbor
	scores := make([]scoredNeighbor, 0, len(neighbors))
	
	for _, nbr := range neighbors {
		// Convert neighbor zone to our internal format
		nbrMinPoint := make(node.Point, len(nbr.Zone.MinPoint.Coordinates))
		nbrMaxPoint := make(node.Point, len(nbr.Zone.MaxPoint.Coordinates))
		copy(nbrMinPoint, nbr.Zone.MinPoint.Coordinates)
		copy(nbrMaxPoint, nbr.Zone.MaxPoint.Coordinates)
		
		nbrZone, err := node.NewZone(nbrMinPoint, nbrMaxPoint)
		if err != nil {
			log.Printf("Skip neighbor %s: failed to create zone: %v", nbr.Id, err)
			continue
		}
		
		// Calculate adjacency score
		score := lb.calculateZoneAdjacency(zone, nbrZone)
		if score > 0 {
			scores = append(scores, scoredNeighbor{
				neighbor: nbr,
				score:    score,
			})
		}
	}
	
	// If no valid neighbors, return nil
	if len(scores) == 0 {
		return nil, -1
	}
	
	// Sort by score (highest first)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})
	
	return scores[0].neighbor, scores[0].score
}

// calculateZoneAdjacency computes how well two zones fit together
// Higher score means better fit (more adjacency)
func (lb *LoadBalancer) calculateZoneAdjacency(zone1, zone2 *node.Zone) float64 {
	dimensions := zone1.MinPoint.Dimensions()
	if dimensions != zone2.MinPoint.Dimensions() {
		return 0
	}
	
	// Count dimensions where zones are adjacent
	adjacentDims := 0
	overlapDims := 0
	
	for dim := 0; dim < dimensions; dim++ {
		// Check if zones are adjacent in this dimension
		if zone1.MaxPoint[dim] == zone2.MinPoint[dim] || zone2.MaxPoint[dim] == zone1.MinPoint[dim] {
			adjacentDims++
		}
		
		// Check if zones overlap in this dimension
		overlap := min(zone1.MaxPoint[dim], zone2.MaxPoint[dim]) - max(zone1.MinPoint[dim], zone2.MinPoint[dim])
		if overlap > 0 {
			overlapDims++
		}
	}
	
	// Perfect score if they can form a single zone (adjacent in exactly one dimension,
	// and perfectly aligned in all other dimensions)
	canMerge := (adjacentDims == 1)
	if canMerge {
		for dim := 0; dim < dimensions; dim++ {
			if !(zone1.MaxPoint[dim] == zone2.MinPoint[dim] || zone2.MaxPoint[dim] == zone1.MinPoint[dim]) {
				// For non-adjacent dimensions, check if they are aligned
				if zone1.MinPoint[dim] != zone2.MinPoint[dim] || zone1.MaxPoint[dim] != zone2.MaxPoint[dim] {
					canMerge = false
					break
				}
			}
		}
	}
	
	if canMerge {
		return 1.0
	}
	
	// Otherwise, score based on adjacency and overlap
	baseScore := float64(adjacentDims) / float64(dimensions)
	
	// Penalize for overlap (we prefer adjacent but non-overlapping zones)
	if overlapDims > 0 {
		baseScore *= 0.5
	}
	
	return baseScore
}

// transferZoneToNeighbor attempts to transfer an extra zone to a neighbor
func (lb *LoadBalancer) transferZoneToNeighbor(ctx context.Context, zone *node.Zone, neighbor *pb.NodeInfo) bool {
	// Get all the keys in this zone
	keys, err := lb.getKeysInZone(ctx, zone)
	if err != nil {
		log.Printf("Failed to get keys in zone for transfer: %v", err)
		return false
	}
	
	// Create zone transfer request
	transferID := uuid.New().String()
	
	zoneTransferReq := &pb.ZoneTransferRequest{
		TransferId: transferID,
		SourceId:   string(lb.nodeID),
		Zone: &pb.Zone{
			MinPoint: &pb.Point{Coordinates: zone.MinPoint},
			MaxPoint: &pb.Point{Coordinates: zone.MaxPoint},
		},
		Keys: keys,
	}
	
	// Connect to the neighbor
	client, err := lb.getClient(neighbor.Address)
	if err != nil {
		log.Printf("Failed to connect to neighbor %s: %v", neighbor.Id, err)
		return false
	}
	
	// Send zone transfer request
	resp, err := client.ReceiveZoneTransfer(ctx, zoneTransferReq)
	if err != nil {
		log.Printf("Failed to transfer zone to neighbor %s: %v", neighbor.Id, err)
		return false
	}
	
	if !resp.Success {
		log.Printf("Neighbor %s rejected zone transfer: %s", neighbor.Id, resp.Message)
		return false
	}
	
	log.Printf("Successfully transferred zone to neighbor %s", neighbor.Id)
	lb.UpdateZoneReassignment()
	
	return true
}

// getKeysInZone retrieves all keys that fall within a specified zone
func (lb *LoadBalancer) getKeysInZone(ctx context.Context, zone *node.Zone) ([]string, error) {
	// Connect to ourselves to get keys
	client, err := lb.getClient(lb.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create client to self: %w", err)
	}
	
	pbZone := &pb.Zone{
		MinPoint: &pb.Point{Coordinates: zone.MinPoint},
		MaxPoint: &pb.Point{Coordinates: zone.MaxPoint},
	}
	
	resp, err := client.GetKeysInZone(ctx, &pb.GetKeysInZoneRequest{Zone: pbZone})
	if err != nil {
		return nil, fmt.Errorf("failed to get keys in zone: %w", err)
	}
	
	return resp.Keys, nil
}

// min returns the smaller of two float64 values
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// max returns the larger of two float64 values
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// TrackKeyAccess tracks access frequency for a key to detect hot keys
func (lb *LoadBalancer) TrackKeyAccess(key string, isWrite bool) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Initialize maps if they don't exist
	if lb.KeyAccessCounts == nil {
		lb.KeyAccessCounts = make(map[string]int)
	}
	if lb.KeyAccessTimes == nil {
		lb.KeyAccessTimes = make(map[string]time.Time)
	}
	if lb.HotKeys == nil {
		lb.HotKeys = make(map[string]HotKeyInfo)
	}

	// Update access count
	lb.KeyAccessCounts[key]++
	lb.KeyAccessTimes[key] = time.Now()

	// Check if this key has become "hot"
	if lb.KeyAccessCounts[key] > lb.HotKeyThreshold {
		// Only replicate every N accesses to avoid excessive replication
		if lb.KeyAccessCounts[key]%lb.HotKeyCheckInterval == 0 {
			isHot := lb.checkHotKey(key, lb.KeyAccessCounts[key])
			if isHot {
				// Update hot key info
				lb.HotKeys[key] = HotKeyInfo{
					Key:         key,
					AccessCount: lb.KeyAccessCounts[key],
					LastAccess:  lb.KeyAccessTimes[key],
					ReplicaCount: 0, // Will be updated by ReplicateHotKey
				}

				// Schedule replication
				go lb.replicateHotKey(context.Background(), key)
			}
		}
	}
}

// checkHotKey determines if a key should be considered "hot"
func (lb *LoadBalancer) checkHotKey(key string, accessCount int) bool {
	// If it's already marked as hot, check if it's still hot
	if info, exists := lb.HotKeys[key]; exists {
		// If it hasn't been accessed in a while, it's no longer hot
		if time.Since(info.LastAccess) > lb.HotKeyTTL {
			delete(lb.HotKeys, key)
			return false
		}
		return true
	}

	// New potential hot key - check access rate
	accessTime, exists := lb.KeyAccessTimes[key]
	if !exists {
		return false
	}

	// Calculate access rate (accesses per second)
	timeSinceFirstAccess := time.Since(accessTime)
	if timeSinceFirstAccess < 1*time.Second {
		timeSinceFirstAccess = 1 * time.Second // Avoid division by zero
	}
	accessRate := float64(accessCount) / timeSinceFirstAccess.Seconds()

	// Return true if the access rate exceeds the threshold
	return accessRate > lb.HotKeyRateThreshold
}

// replicateHotKey replicates a hot key to reduce load
func (lb *LoadBalancer) replicateHotKey(ctx context.Context, key string) {
	lb.mu.Lock()
	// Check if the key is still hot
	hotKeyInfo, exists := lb.HotKeys[key]
	if !exists {
		lb.mu.Unlock()
		return
	}
	// Don't exceed max replicas
	if hotKeyInfo.ReplicaCount >= lb.MaxHotKeyReplicas {
		lb.mu.Unlock()
		return
	}
	lb.mu.Unlock()

	// Get the value to replicate
	value, err := lb.getValue(ctx, key)
	if err != nil {
		log.Printf("Failed to get value for hot key %s: %v", key, err)
		return
	}

	// Get suitable neighbors for replication
	neighbors, err := lb.getNeighborsForHotKeyReplication(ctx)
	if err != nil {
		log.Printf("Failed to get neighbors for hot key replication: %v", err)
		return
	}

	// Track how many replicas we've created
	replicaCount := 0
	replicaNodes := make([]string, 0)

	// Replicate to selected neighbors
	for _, neighbor := range neighbors {
		// Don't exceed max replicas
		if replicaCount >= lb.MaxHotKeyReplicas {
			break
		}

		// Create a replica on this neighbor
		success := lb.createHotKeyReplica(ctx, key, value, neighbor)
		if success {
			replicaCount++
			replicaNodes = append(replicaNodes, string(neighbor.ID))
		}
	}

	// Update hot key info with replication status
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if hotKeyInfo, exists := lb.HotKeys[key]; exists {
		hotKeyInfo.ReplicaCount = replicaCount
		hotKeyInfo.ReplicaNodes = replicaNodes
		lb.HotKeys[key] = hotKeyInfo
	}
}

// getValue gets the value for a key from local storage
func (lb *LoadBalancer) getValue(ctx context.Context, key string) ([]byte, error) {
	// This would typically interact with your storage system
	// For this example, we'll simulate it with a gRPC call to ourselves
	req := &pb.GetRequest{
		Key:     key,
		Forward: false,
	}

	// Create a client to ourselves
	client, conn, err := lb.getClient(lb.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer conn.Close()

	// Get the value
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get request failed: %w", err)
	}

	if !resp.Success || !resp.Exists {
		return nil, fmt.Errorf("key not found or access denied")
	}

	return resp.Value, nil
}

// getNeighborsForHotKeyReplication gets suitable neighbors for hot key replication
func (lb *LoadBalancer) getNeighborsForHotKeyReplication(ctx context.Context) ([]*node.NeighborInfo, error) {
	// Get all neighbors
	req := &pb.GetNeighborsRequest{}

	// Create a client to ourselves
	client, conn, err := lb.getClient(lb.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer conn.Close()

	// Get neighbors
	resp, err := client.GetNeighbors(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get neighbors request failed: %w", err)
	}

	// Convert to neighbor info objects
	neighbors := make([]*node.NeighborInfo, 0, len(resp.Neighbors))
	for _, n := range resp.Neighbors {
		zone, err := node.NewZone(
			node.Point(n.Zone.MinPoint.Coordinates),
			node.Point(n.Zone.MaxPoint.Coordinates),
		)
		if err != nil {
			continue
		}

		neighbors = append(neighbors, &node.NeighborInfo{
			ID:      node.NodeID(n.Id),
			Address: n.Address,
			Zone:    zone,
		})
	}

	// Sort by load if load info is available
	// For now, we'll just return all neighbors
	return neighbors, nil
}

// createHotKeyReplica creates a replica of a hot key on a neighbor
func (lb *LoadBalancer) createHotKeyReplica(ctx context.Context, key string, value []byte, neighbor *node.NeighborInfo) bool {
	// Connect to the neighbor
	client, conn, err := lb.getClient(neighbor.Address)
	if err != nil {
		log.Printf("Failed to connect to neighbor %s: %v", neighbor.ID, err)
		return false
	}
	defer conn.Close()

	// Create a put request with the replica flag
	req := &pb.PutRequest{
		Key:       key,
		Value:     value,
		Forward:   true,  // Store locally regardless of zone
		IsReplica: true,  // Mark as a replica
		IsHotKey:  true,  // Mark as a hot key replica
	}

	// Send the request
	resp, err := client.Put(ctx, req)
	if err != nil {
		log.Printf("Failed to create hot key replica on %s: %v", neighbor.ID, err)
		return false
	}

	return resp.Success
}

// pruneAccessData periodically cleans up old key access data
func (lb *LoadBalancer) pruneAccessData() {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Get the current time
	now := time.Now()

	// Track keys to prune
	keysToPrune := make([]string, 0)

	// Find keys that haven't been accessed in a while
	for key, timestamp := range lb.KeyAccessTimes {
		if now.Sub(timestamp) > lb.PruneInterval {
			keysToPrune = append(keysToPrune, key)
		}
	}

	// Prune the keys
	for _, key := range keysToPrune {
		delete(lb.KeyAccessCounts, key)
		delete(lb.KeyAccessTimes, key)
		delete(lb.HotKeys, key)
	}

	if len(keysToPrune) > 0 {
		log.Printf("Pruned %d keys from access tracking", len(keysToPrune))
	}
}

// AdjustZoneForLoad adjusts a node's zone based on its load
func (lb *LoadBalancer) AdjustZoneForLoad(ctx context.Context) error {
	// Check if we're overloaded
	if !lb.IsOverloaded() {
		return nil
	}

	// Find neighbors that might be able to take some of our load
	neighbors, err := lb.getSuitableNeighborsForLoadBalancing(ctx)
	if err != nil {
		return fmt.Errorf("failed to find suitable neighbors: %w", err)
	}

	if len(neighbors) == 0 {
		return fmt.Errorf("no suitable neighbors found for load balancing")
	}

	// Sort neighbors by load (ascending)
	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i].LoadPercent < neighbors[j].LoadPercent
	})

	// Try to adjust zone with the least loaded neighbor
	for _, neighbor := range neighbors {
		err := lb.adjustZoneWithNeighbor(ctx, neighbor)
		if err == nil {
			// Successfully adjusted zone
			return nil
		}
		log.Printf("Failed to adjust zone with neighbor %s: %v", neighbor.ID, err)
	}

	return fmt.Errorf("failed to adjust zone with any neighbor")
}

// getSuitableNeighborsForLoadBalancing gets neighbors suitable for load balancing
func (lb *LoadBalancer) getSuitableNeighborsForLoadBalancing(ctx context.Context) ([]*NeighborWithLoad, error) {
	// Get all neighbors
	req := &pb.GetNeighborsRequest{}

	// Create a client to ourselves
	client, conn, err := lb.getClient(lb.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer conn.Close()

	// Get neighbors
	resp, err := client.GetNeighbors(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get neighbors request failed: %w", err)
	}

	// Get load info for each neighbor
	neighbors := make([]*NeighborWithLoad, 0, len(resp.Neighbors))
	for _, n := range resp.Neighbors {
		// Skip failed nodes
		if n.Failed {
			continue
		}

		// Get load info
		loadInfo, err := lb.getNeighborLoad(ctx, n.Address)
		if err != nil {
			log.Printf("Failed to get load info for neighbor %s: %v", n.Id, err)
			continue
		}

		// Skip overloaded neighbors
		if loadInfo.LoadPercent > 70 {
			continue
		}

		zone, err := node.NewZone(
			node.Point(n.Zone.MinPoint.Coordinates),
			node.Point(n.Zone.MaxPoint.Coordinates),
		)
		if err != nil {
			continue
		}

		neighbors = append(neighbors, &NeighborWithLoad{
			ID:          node.NodeID(n.Id),
			Address:     n.Address,
			Zone:        zone,
			LoadPercent: loadInfo.LoadPercent,
		})
	}

	return neighbors, nil
}

// NeighborWithLoad extends NeighborInfo with load information
type NeighborWithLoad struct {
	ID          node.NodeID
	Address     string
	Zone        *node.Zone
	LoadPercent float64
}

// getNeighborLoad gets the load information for a neighbor
func (lb *LoadBalancer) getNeighborLoad(ctx context.Context, address string) (*LoadInfo, error) {
	// Connect to the neighbor
	client, conn, err := lb.getClient(address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to neighbor: %w", err)
	}
	defer conn.Close()

	// Get load info
	req := &pb.GetLoadRequest{}
	resp, err := client.GetLoad(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get load request failed: %w", err)
	}

	return &LoadInfo{
		LoadPercent: resp.LoadPercent,
		KeyCount:    int(resp.KeyCount),
		CPUPercent:  resp.CpuPercent,
		MemPercent:  resp.MemPercent,
	}, nil
}

// LoadInfo contains load information for a node
type LoadInfo struct {
	LoadPercent float64
	KeyCount    int
	CPUPercent  float64
	MemPercent  float64
}

// adjustZoneWithNeighbor adjusts zone boundaries with a neighbor to balance load
func (lb *LoadBalancer) adjustZoneWithNeighbor(ctx context.Context, neighbor *NeighborWithLoad) error {
	// Find the dimension along which to adjust the boundary
	adjustmentDimension, err := lb.findAdjustmentDimension(lb.currentZone, neighbor.Zone)
	if err != nil {
		return fmt.Errorf("failed to find adjustment dimension: %w", err)
	}

	// Calculate new boundary positions
	oldBoundary, newBoundary := lb.calculateNewBoundaries(lb.currentZone, neighbor.Zone, adjustmentDimension)

	// Create zone adjustment request
	req := &pb.AdjustZoneRequest{
		NeighborId:           string(neighbor.ID),
		AdjustmentDimension:  int32(adjustmentDimension),
		OldBoundaryPosition:  oldBoundary,
		NewBoundaryPosition:  newBoundary,
		TransferKeyThreshold: lb.currentLoad - lb.maxLoad/2, // Transfer enough to get below 50% of max
	}

	// Create a client to ourselves
	client, conn, err := lb.getClient(lb.address)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer conn.Close()

	// Send the request
	resp, err := client.AdjustZone(ctx, req)
	if err != nil {
		return fmt.Errorf("adjust zone request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("zone adjustment unsuccessful: %s", resp.Error)
	}

	log.Printf("Successfully adjusted zone with neighbor %s, transferred %d keys", 
		neighbor.ID, resp.KeysTransferred)

	return nil
}

// findAdjustmentDimension finds the dimension along which to adjust the boundary
func (lb *LoadBalancer) findAdjustmentDimension(ourZone, neighborZone *node.Zone) (int, error) {
	dimensions := len(ourZone.MinPoint)
	
	// Find the dimension where the zones are adjacent
	for dim := 0; dim < dimensions; dim++ {
		// Check if zones are adjacent along this dimension
		if ourZone.MaxPoint[dim] == neighborZone.MinPoint[dim] || 
		   ourZone.MinPoint[dim] == neighborZone.MaxPoint[dim] {
			// Verify they overlap in all other dimensions
			overlap := true
			for otherDim := 0; otherDim < dimensions; otherDim++ {
				if otherDim == dim {
					continue
				}
				// Check if there's overlap in this other dimension
				if ourZone.MaxPoint[otherDim] <= neighborZone.MinPoint[otherDim] ||
				   ourZone.MinPoint[otherDim] >= neighborZone.MaxPoint[otherDim] {
					overlap = false
					break
				}
			}
			if overlap {
				return dim, nil
			}
		}
	}
	
	return 0, fmt.Errorf("zones are not adjacent along any dimension")
}

// calculateNewBoundaries calculates the new boundary position for zone adjustment
func (lb *LoadBalancer) calculateNewBoundaries(ourZone, neighborZone *node.Zone, dim int) (float64, float64) {
	var oldBoundary, newBoundary float64
	
	// Determine if we're adjusting our max or min boundary
	if ourZone.MaxPoint[dim] == neighborZone.MinPoint[dim] {
		// We're adjusting our max boundary
		oldBoundary = ourZone.MaxPoint[dim]
		
		// Calculate the midpoint, but weighted by the load difference
		// Our load is higher, so we want to shrink our zone more
		ourSize := ourZone.MaxPoint[dim] - ourZone.MinPoint[dim]
		adjustmentRatio := 0.3 // Start with reducing by 30%
		
		// Adjust more aggressively if we're more overloaded
		if lb.currentLoad > lb.maxLoad*1.5 {
			adjustmentRatio = 0.4
		}
		
		newBoundary = ourZone.MaxPoint[dim] - (ourSize * adjustmentRatio)
	} else {
		// We're adjusting our min boundary
		oldBoundary = ourZone.MinPoint[dim]
		
		// Calculate the midpoint, but weighted by the load difference
		ourSize := ourZone.MaxPoint[dim] - ourZone.MinPoint[dim]
		adjustmentRatio := 0.3 // Start with reducing by 30%
		
		// Adjust more aggressively if we're more overloaded
		if lb.currentLoad > lb.maxLoad*1.5 {
			adjustmentRatio = 0.4
		}
		
		newBoundary = ourZone.MinPoint[dim] + (ourSize * adjustmentRatio)
	}
	
	return oldBoundary, newBoundary
} 