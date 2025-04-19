package node

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// NodeID is a unique identifier for a node
type NodeID string

// Node represents a node in the CAN network
type Node struct {
	// ID is the unique identifier of the node
	ID NodeID

	// Address is the network address of the node (IP:port)
	Address string

	// Zone is the area of the coordinate space that this node is responsible for
	Zone *Zone

	// Neighbors is a map of neighboring nodes
	Neighbors map[NodeID]*NeighborInfo

	// ExtendedNeighbors tracks nodes that are multiple hops away
	ExtendedNeighbors map[NodeID]*ExtendedNeighborInfo

	// Data is the key-value store managed by this node
	Data map[string]string

	// Dimensions is the dimensionality of the coordinate space
	Dimensions int

	// Heartbeats is the time of the last heartbeat received from each neighbor
	Heartbeats map[NodeID]time.Time

	mu sync.RWMutex
}

// NeighborInfo contains information about a neighboring node
type NeighborInfo struct {
	ID      NodeID
	Address string
	Zone    *Zone
}

// ExtendedNeighborInfo contains information about nodes that are multiple hops away
type ExtendedNeighborInfo struct {
	*NeighborInfo
	HopCount int
	Path     []NodeID // Path to reach this node
}

// NewNode creates a new node with the given parameters
func NewNode(id NodeID, address string, zone *Zone, dimensions int) *Node {
	return &Node{
		ID:                id,
		Address:           address,
		Zone:              zone,
		Neighbors:         make(map[NodeID]*NeighborInfo),
		ExtendedNeighbors: make(map[NodeID]*ExtendedNeighborInfo),
		Data:              make(map[string]string),
		Dimensions:        dimensions,
		Heartbeats:        make(map[NodeID]time.Time),
	}
}

// AddNeighbor adds a neighbor to the node's neighbor list
func (n *Node) AddNeighbor(id NodeID, address string, zone *Zone) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Neighbors[id] = &NeighborInfo{
		ID:      id,
		Address: address,
		Zone:    zone,
	}
}

// AddExtendedNeighbor adds a multi-hop neighbor to the node's extended neighbor list
func (n *Node) AddExtendedNeighbor(id NodeID, address string, zone *Zone, hopCount int, path []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Don't add our immediate neighbors to extended neighbors
	if _, exists := n.Neighbors[id]; exists {
		return
	}

	// Don't add ourselves
	if id == n.ID {
		return
	}

	n.ExtendedNeighbors[id] = &ExtendedNeighborInfo{
		NeighborInfo: &NeighborInfo{
			ID:      id,
			Address: address,
			Zone:    zone,
		},
		HopCount: hopCount,
		Path:     path,
	}
}

// RemoveNeighbor removes a neighbor from the node's neighbor list
func (n *Node) RemoveNeighbor(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.Neighbors, id)
	delete(n.Heartbeats, id)
}

// RemoveExtendedNeighbor removes a node from the extended neighbor list
func (n *Node) RemoveExtendedNeighbor(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.ExtendedNeighbors, id)
}

// UpdateHeartbeat updates the last heartbeat time for a neighbor
func (n *Node) UpdateHeartbeat(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Heartbeats[id] = time.Now()
}

// IsNeighborZone checks if a zone is adjacent to this node's zone
func (n *Node) IsNeighborZone(zone *Zone) bool {
	if n.Zone == nil || zone == nil {
		return false
	}

	// Two zones are neighbors if they share a (d-1)-dimensional hyperplane
	adjacentCount := 0
	nonOverlapCount := 0

	for i := 0; i < n.Dimensions; i++ {
		// Check if the zones are adjacent along dimension i
		if n.Zone.MaxPoint[i] == zone.MinPoint[i] || n.Zone.MinPoint[i] == zone.MaxPoint[i] {
			adjacentCount++
		}

		// Check if the zones overlap along dimension i
		if n.Zone.MinPoint[i] >= zone.MaxPoint[i] || n.Zone.MaxPoint[i] <= zone.MinPoint[i] {
			nonOverlapCount++
		}
	}

	// Zones are neighbors if they are adjacent along exactly one dimension
	// and have overlap in all other dimensions
	return adjacentCount == 1 && nonOverlapCount == 1
}

// Put stores a key-value pair in the node's data store
func (n *Node) Put(key, value string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Data[key] = value
}

// Get retrieves a value from the node's data store
func (n *Node) Get(key string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	value, exists := n.Data[key]
	return value, exists
}

// Delete removes a key-value pair from the node's data store
func (n *Node) Delete(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.Data[key]; !exists {
		return false
	}

	delete(n.Data, key)
	return true
}

// GetNeighbors returns a copy of the node's neighbor list
func (n *Node) GetNeighbors() map[NodeID]*NeighborInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	neighbors := make(map[NodeID]*NeighborInfo, len(n.Neighbors))
	for id, info := range n.Neighbors {
		neighbors[id] = info
	}
	return neighbors
}

// Split splits the node's zone and returns a new zone for a joining node
func (n *Node) Split() (*Zone, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Choose the dimension with the longest side for splitting
	maxDim := 0
	maxSize := n.Zone.MaxPoint[0] - n.Zone.MinPoint[0]

	for dim := 1; dim < n.Dimensions; dim++ {
		size := n.Zone.MaxPoint[dim] - n.Zone.MinPoint[dim]
		if size > maxSize {
			maxSize = size
			maxDim = dim
		}
	}

	// Split the zone
	myNewZone, newNodeZone, err := n.Zone.Split(maxDim)
	if err != nil {
		return nil, fmt.Errorf("failed to split zone: %w", err)
	}

	// Create a map to store keys that should be moved to the new zone
	keysToMove := make([]string, 0)

	// For each key in our data store
	for key := range n.Data {
		// Hash the key to get a point in the coordinate space
		// For simplicity, we'll use a simple hash function that maps to [0,1] range
		// In a real implementation, this would be a proper consistent hash function
		hashValue := float64(len(key)) / 100.0 // Simple hash for testing

		// Create a point in the coordinate space
		point := make(Point, n.Dimensions)
		for i := 0; i < n.Dimensions; i++ {
			if i == maxDim {
				// Use the hash value for the splitting dimension
				point[i] = hashValue
			} else {
				// For other dimensions, use a value in the middle of the zone
				point[i] = (n.Zone.MinPoint[i] + n.Zone.MaxPoint[i]) / 2
			}
		}

		// Check if the point belongs to the new zone
		if newNodeZone.Contains(point) {
			keysToMove = append(keysToMove, key)
		}
	}

	// Remove the keys that belong to the new zone
	for _, key := range keysToMove {
		delete(n.Data, key)
	}

	// Update node's zone
	n.Zone = myNewZone

	return newNodeZone, nil
}

// ProactiveSplit splits the node's zone for load balancing
// Unlike regular Split, this is initiated by the node itself, not by a joining node
func (n *Node) ProactiveSplit() (*Zone, error) {
	if n.Zone == nil {
		return nil, fmt.Errorf("node has no zone")
	}

	// Find the dimension with the largest span
	maxDim := 0
	maxSpan := n.Zone.MaxPoint[0] - n.Zone.MinPoint[0]
	for i := 1; i < len(n.Zone.MinPoint); i++ {
		span := n.Zone.MaxPoint[i] - n.Zone.MinPoint[i]
		if span > maxSpan {
			maxSpan = span
			maxDim = i
		}
	}

	// Split at the midpoint of the chosen dimension
	splitPoint := n.Zone.MinPoint[maxDim] + maxSpan/2
	
	// Create the new zone
	newMin := make(Point, len(n.Zone.MinPoint))
	newMax := make(Point, len(n.Zone.MaxPoint))
	copy(newMin, n.Zone.MinPoint)
	copy(newMax, n.Zone.MaxPoint)
	
	// Adjust boundaries of the new zone
	newMin[maxDim] = splitPoint
	
	newZone, err := NewZone(newMin, newMax)
	if err != nil {
		return nil, fmt.Errorf("failed to create new zone: %w", err)
	}
	
	// Update this node's zone
	n.Zone.MaxPoint[maxDim] = splitPoint
	
	// Log the split
	log.Printf("Node %s proactively split zone at dimension %d, point %f", n.ID, maxDim, splitPoint)
	
	return newZone, nil
}
