package routing

import (
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/can-dht/pkg/node"
)

// Router handles routing in the CAN DHT
type Router struct {
	Dimensions int
}

// NewRouter creates a new router with the specified number of dimensions
func NewRouter(dimensions int) *Router {
	return &Router{
		Dimensions: dimensions,
	}
}

// HashToPoint converts a key to a point in the coordinate space
func (r *Router) HashToPoint(key string) node.Point {
	// Create a hash of the key
	hash := sha256.Sum256([]byte(key))

	// Use portions of the hash to generate coordinates
	point := make(node.Point, r.Dimensions)

	for i := 0; i < r.Dimensions && i < 8; i++ {
		// Use 4 bytes of the hash for each dimension
		// This gives us more granularity than using a single byte
		startIdx := i * 4
		if startIdx >= len(hash) {
			startIdx = i % len(hash)
		}

		// Convert 4 bytes to a uint32
		val := binary.BigEndian.Uint32(hash[startIdx : startIdx+4])

		// Normalize to [0, 1) range
		point[i] = float64(val) / math.MaxUint32
	}

	return point
}

// FindResponsibleNode finds the node responsible for a given key
func (r *Router) FindResponsibleNode(localNode *node.Node, key string) (*node.NeighborInfo, bool) {
	// Hash the key to a point in the coordinate space
	point := r.HashToPoint(key)

	// Check if the local node's zone contains the point
	if localNode.Zone.Contains(point) {
		return nil, true // Local node is responsible
	}

	// Find the neighbor closest to the point using greedy routing
	var closestNeighbor *node.NeighborInfo
	var minDistance float64 = math.MaxFloat64

	for _, neighbor := range localNode.GetNeighbors() {
		dist, err := node.DistanceToZone(point, neighbor.Zone)
		if err != nil {
			continue
		}

		if dist < minDistance {
			minDistance = dist
			closestNeighbor = neighbor
		}
	}

	return closestNeighbor, false
}

// RouteToPoint finds the next hop towards a point
func (r *Router) RouteToPoint(localNode *node.Node, point node.Point) (*node.NeighborInfo, bool) {
	// Check if the local node's zone contains the point
	if localNode.Zone.Contains(point) {
		return nil, true // Local node is responsible
	}

	// Find the neighbor closest to the point
	var closestNeighbor *node.NeighborInfo
	var minDistance float64 = math.MaxFloat64

	for _, neighbor := range localNode.GetNeighbors() {
		dist, err := node.DistanceToZone(point, neighbor.Zone)
		if err != nil {
			continue
		}

		if dist < minDistance {
			minDistance = dist
			closestNeighbor = neighbor
		}
	}

	return closestNeighbor, false
}

// FindJoinNode finds a node to handle a join request
func (r *Router) FindJoinNode(entryNode *node.Node, joinPoint node.Point) (*node.NeighborInfo, bool) {
	return r.RouteToPoint(entryNode, joinPoint)
}
