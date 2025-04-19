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
)

// RequestDistributor is the interface for distributing requests across nodes
type RequestDistributor interface {
	// Distribute a PUT request
	DistributePut(ctx context.Context, key string, value []byte) (*pb.PutResponse, error)
	// Distribute a GET request
	DistributeGet(ctx context.Context, key string) (*pb.GetResponse, error)
	// Distribute a DELETE request
	DistributeDelete(ctx context.Context, key string) (*pb.DeleteResponse, error)
}

// RequestDistributor manages the distribution of requests across nodes
type RequestDistributor struct {
	// Reference to the server
	server *CANServer
	
	// Node load statistics
	nodeLoads map[node.NodeID]float64
	
	// Last time node loads were updated
	lastUpdate time.Time
	
	// Update interval
	updateInterval time.Duration
	
	// Lock for thread safety
	mu sync.RWMutex
}

// NewRequestDistributor creates a new request distributor
func NewRequestDistributor(server *CANServer) *RequestDistributor {
	return &RequestDistributor{
		server:         server,
		nodeLoads:      make(map[node.NodeID]float64),
		lastUpdate:     time.Now(),
		updateInterval: 1 * time.Minute,
	}
}

// Start starts the request distributor
func (rd *RequestDistributor) Start(ctx context.Context) {
	ticker := time.NewTicker(rd.updateInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rd.updateNodeLoads()
		}
	}
}

// updateNodeLoads collects load information from neighbors
func (rd *RequestDistributor) updateNodeLoads() {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	
	// Update local node load
	rd.nodeLoads[rd.server.Node.ID] = rd.server.getNodeLoadScore(rd.server.Node.ID)
	
	// Update neighbor loads
	rd.server.mu.RLock()
	neighbors := rd.server.Node.GetNeighbors()
	rd.server.mu.RUnlock()
	
	for _, neighbor := range neighbors {
		// Get load from neighbor through gRPC
		load, err := rd.getNeighborLoad(context.Background(), neighbor.ID, neighbor.Address)
		if err != nil {
			log.Printf("Failed to get load from neighbor %s: %v", neighbor.ID, err)
			continue
		}
		
		rd.nodeLoads[neighbor.ID] = load
	}
	
	rd.lastUpdate = time.Now()
}

// getNeighborLoad gets the load score from a neighbor node
func (rd *RequestDistributor) getNeighborLoad(ctx context.Context, nodeID node.NodeID, address string) (float64, error) {
	// Connect to the node
	client, conn, err := rd.server.ConnectToNode(ctx, address)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	
	// Get node info
	resp, err := client.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	if err != nil {
		return 0, err
	}
	
	return resp.LoadScore, nil
}

// SelectForwardNode selects a node to forward a request to based on the distribution strategy
func (rd *RequestDistributor) SelectForwardNode(ctx context.Context, key string, candidates []*node.NeighborInfo) (*node.NeighborInfo, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	
	// Choose distribution strategy based on configuration
	strategy := rd.server.Config.RequestDistributionStrategy
	
	switch strategy {
	case DistributionStrategyRandom:
		return rd.randomStrategy(candidates), nil
	case DistributionStrategyRoundRobin:
		return rd.roundRobinStrategy(candidates), nil
	case DistributionStrategyLeastLoaded:
		return rd.leastLoadedStrategy(candidates), nil
	case DistributionStrategyWeightedRandom:
		return rd.weightedRandomStrategy(candidates), nil
	default:
		return rd.leastLoadedStrategy(candidates), nil
	}
}

// randomStrategy randomly selects a node
func (rd *RequestDistributor) randomStrategy(candidates []*node.NeighborInfo) *node.NeighborInfo {
	idx := rand.Intn(len(candidates))
	return candidates[idx]
}

// Global counter for round robin
var roundRobinCounter = struct {
	count int
	mu    sync.Mutex
}{count: 0}

// roundRobinStrategy rotates through candidates in sequence
func (rd *RequestDistributor) roundRobinStrategy(candidates []*node.NeighborInfo) *node.NeighborInfo {
	roundRobinCounter.mu.Lock()
	defer roundRobinCounter.mu.Unlock()
	
	idx := roundRobinCounter.count % len(candidates)
	roundRobinCounter.count++
	
	return candidates[idx]
}

// leastLoadedStrategy selects the node with the lowest load
func (rd *RequestDistributor) leastLoadedStrategy(candidates []*node.NeighborInfo) *node.NeighborInfo {
	rd.mu.RLock()
	defer rd.mu.RUnlock()
	
	var bestNode *node.NeighborInfo
	lowestLoad := math.MaxFloat64
	
	for _, node := range candidates {
		load, exists := rd.nodeLoads[node.ID]
		if !exists {
			// If we don't have load info, use a default
			load = 0.5
		}
		
		if load < lowestLoad {
			lowestLoad = load
			bestNode = node
		}
	}
	
	return bestNode
}

// weightedRandomStrategy selects nodes with probability inversely proportional to load
func (rd *RequestDistributor) weightedRandomStrategy(candidates []*node.NeighborInfo) *node.NeighborInfo {
	rd.mu.RLock()
	defer rd.mu.RUnlock()
	
	// Calculate weights (inverse of load)
	weights := make([]float64, len(candidates))
	totalWeight := 0.0
	
	for i, node := range candidates {
		load, exists := rd.nodeLoads[node.ID]
		if !exists {
			// If we don't have load info, use a default
			load = 0.5
		}
		
		// Invert load to get weight (higher load = lower weight)
		weight := 1.0 / (load + 0.1) // Add small constant to avoid division by zero
		weights[i] = weight
		totalWeight += weight
	}
	
	// Select a node based on weights
	r := rand.Float64() * totalWeight
	cumulativeWeight := 0.0
	
	for i, weight := range weights {
		cumulativeWeight += weight
		if r <= cumulativeWeight {
			return candidates[i]
		}
	}
	
	// Fallback to the last candidate
	return candidates[len(candidates)-1]
}

// Distribution strategies
const (
	DistributionStrategyRandom        = "random"
	DistributionStrategyRoundRobin    = "round_robin"
	DistributionStrategyLeastLoaded   = "least_loaded"
	DistributionStrategyWeightedRandom = "weighted_random"
)

// These methods are required for role-based request forwarding

// DistributePut forwards a PUT request to a node that can handle write operations
func (d *DirectDistributor) DistributePut(ctx context.Context, key string, value []byte) (*pb.PutResponse, error) {
	// Find a suitable node that can handle write operations
	suitable := d.findNodesWithRole(RoleStandard, RoleWriteOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.PutResponse{
			Success: false,
			Message: "no suitable nodes found that can handle write operations",
		}, fmt.Errorf("no suitable nodes found for write operations")
	}

	// Select a random node from the suitable nodes
	node := suitable[rand.Intn(len(suitable))]
	
	// Forward the request
	return d.server.ForwardPutToNode(ctx, key, value, node)
}

// DistributeGet forwards a GET request to a node that can handle read operations
func (d *DirectDistributor) DistributeGet(ctx context.Context, key string) (*pb.GetResponse, error) {
	// Find a suitable node that can handle read operations
	suitable := d.findNodesWithRole(RoleStandard, RoleReadOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.GetResponse{
			Success: false,
			Message: "no suitable nodes found that can handle read operations",
		}, fmt.Errorf("no suitable nodes found for read operations")
	}

	// Select a random node from the suitable nodes
	node := suitable[rand.Intn(len(suitable))]
	
	// Forward the request
	return d.server.ForwardGetToNode(ctx, key, node)
}

// DistributeDelete forwards a DELETE request to a node that can handle delete operations
func (d *DirectDistributor) DistributeDelete(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	// Find a suitable node that can handle delete operations
	suitable := d.findNodesWithRole(RoleStandard, RoleWriteOnly)
	if len(suitable) == 0 {
		return &pb.DeleteResponse{
			Success: false,
			Message: "no suitable nodes found that can handle delete operations",
		}, fmt.Errorf("no suitable nodes found for delete operations")
	}

	// Select a random node from the suitable nodes
	node := suitable[rand.Intn(len(suitable))]
	
	// Forward the request
	return d.server.ForwardDeleteToNode(ctx, key, node)
}

// findNodesWithRole returns a list of neighboring nodes with the specified roles
func (d *DirectDistributor) findNodesWithRole(roles ...NodeRole) []*node.NodeInfo {
	var suitable []*node.NodeInfo
	
	// Get all the neighboring nodes
	neighbors := d.server.Node.GetNeighbors()
	
	// For each neighbor, check if it has a suitable role
	for _, neighbor := range neighbors {
		// Get the node's role from the server's knowledge
		// If we don't know, we assume it's a standard node
		role := d.server.getNodeRole(neighbor.ID)
		
		// Check if the role is suitable
		for _, r := range roles {
			if role == r {
				suitable = append(suitable, neighbor)
				break
			}
		}
	}
	
	return suitable
}

// DistributePut for RandomDistributor
func (d *RandomDistributor) DistributePut(ctx context.Context, key string, value []byte) (*pb.PutResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleWriteOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.PutResponse{
			Success: false,
			Message: "no suitable nodes found that can handle write operations",
		}, fmt.Errorf("no suitable nodes found for write operations")
	}

	// Select a random node
	node := suitable[rand.Intn(len(suitable))]
	return d.server.ForwardPutToNode(ctx, key, value, node)
}

// DistributeGet for RandomDistributor
func (d *RandomDistributor) DistributeGet(ctx context.Context, key string) (*pb.GetResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleReadOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.GetResponse{
			Success: false,
			Message: "no suitable nodes found that can handle read operations",
		}, fmt.Errorf("no suitable nodes found for read operations")
	}

	// Select a random node
	node := suitable[rand.Intn(len(suitable))]
	return d.server.ForwardGetToNode(ctx, key, node)
}

// DistributeDelete for RandomDistributor
func (d *RandomDistributor) DistributeDelete(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleWriteOnly)
	if len(suitable) == 0 {
		return &pb.DeleteResponse{
			Success: false,
			Message: "no suitable nodes found that can handle delete operations",
		}, fmt.Errorf("no suitable nodes found for delete operations")
	}

	// Select a random node
	node := suitable[rand.Intn(len(suitable))]
	return d.server.ForwardDeleteToNode(ctx, key, node)
}

// DistributePut for RoundRobinDistributor
func (d *RoundRobinDistributor) DistributePut(ctx context.Context, key string, value []byte) (*pb.PutResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleWriteOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.PutResponse{
			Success: false,
			Message: "no suitable nodes found that can handle write operations",
		}, fmt.Errorf("no suitable nodes found for write operations")
	}

	// Use round robin to select a node
	d.mu.Lock()
	index := d.currentIndex % len(suitable)
	d.currentIndex++
	d.mu.Unlock()

	node := suitable[index]
	return d.server.ForwardPutToNode(ctx, key, value, node)
}

// DistributeGet for RoundRobinDistributor
func (d *RoundRobinDistributor) DistributeGet(ctx context.Context, key string) (*pb.GetResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleReadOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.GetResponse{
			Success: false,
			Message: "no suitable nodes found that can handle read operations",
		}, fmt.Errorf("no suitable nodes found for read operations")
	}

	// Use round robin to select a node
	d.mu.Lock()
	index := d.currentIndex % len(suitable)
	d.currentIndex++
	d.mu.Unlock()

	node := suitable[index]
	return d.server.ForwardGetToNode(ctx, key, node)
}

// DistributeDelete for RoundRobinDistributor
func (d *RoundRobinDistributor) DistributeDelete(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleWriteOnly)
	if len(suitable) == 0 {
		return &pb.DeleteResponse{
			Success: false,
			Message: "no suitable nodes found that can handle delete operations",
		}, fmt.Errorf("no suitable nodes found for delete operations")
	}

	// Use round robin to select a node
	d.mu.Lock()
	index := d.currentIndex % len(suitable)
	d.currentIndex++
	d.mu.Unlock()

	node := suitable[index]
	return d.server.ForwardDeleteToNode(ctx, key, node)
}

// DistributePut for LeastLoadedDistributor
func (d *LeastLoadedDistributor) DistributePut(ctx context.Context, key string, value []byte) (*pb.PutResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleWriteOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.PutResponse{
			Success: false,
			Message: "no suitable nodes found that can handle write operations",
		}, fmt.Errorf("no suitable nodes found for write operations")
	}

	// Find the least loaded node
	node := d.findLeastLoadedNode(suitable)
	return d.server.ForwardPutToNode(ctx, key, value, node)
}

// DistributeGet for LeastLoadedDistributor
func (d *LeastLoadedDistributor) DistributeGet(ctx context.Context, key string) (*pb.GetResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleReadOnly, RoleEdge)
	if len(suitable) == 0 {
		return &pb.GetResponse{
			Success: false,
			Message: "no suitable nodes found that can handle read operations",
		}, fmt.Errorf("no suitable nodes found for read operations")
	}

	// Find the least loaded node
	node := d.findLeastLoadedNode(suitable)
	return d.server.ForwardGetToNode(ctx, key, node)
}

// DistributeDelete for LeastLoadedDistributor
func (d *LeastLoadedDistributor) DistributeDelete(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	// Find suitable nodes
	suitable := findNodesWithRole(d.server, RoleStandard, RoleWriteOnly)
	if len(suitable) == 0 {
		return &pb.DeleteResponse{
			Success: false,
			Message: "no suitable nodes found that can handle delete operations",
		}, fmt.Errorf("no suitable nodes found for delete operations")
	}

	// Find the least loaded node
	node := d.findLeastLoadedNode(suitable)
	return d.server.ForwardDeleteToNode(ctx, key, node)
}

// Helper function to find nodes with specific roles
func findNodesWithRole(server *CANServer, roles ...NodeRole) []*node.NodeInfo {
	var suitable []*node.NodeInfo
	
	// Get all the neighboring nodes
	neighbors := server.Node.GetNeighbors()
	
	// For each neighbor, check if it has a suitable role
	for _, neighbor := range neighbors {
		// Get the node's role from the server's knowledge
		role := server.getNodeRole(neighbor.ID)
		
		// Check if the role is suitable
		for _, r := range roles {
			if role == r {
				suitable = append(suitable, neighbor)
				break
			}
		}
	}
	
	return suitable
} 