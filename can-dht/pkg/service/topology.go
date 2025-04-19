package service

import (
	"context"
	"log"
	"time"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/internal/proto"
)

// DiscoverNetworkTopology performs a breadth-first search of the network
// to discover nodes up to maxHops away from the current node
func (s *CANServer) DiscoverNetworkTopology(ctx context.Context, maxHops int) error {
	log.Printf("Starting network topology discovery (max hops: %d)", maxHops)

	// Reset the extended neighbors list but preserve immediate neighbors
	s.mu.Lock()
	s.Node.ExtendedNeighbors = make(map[node.NodeID]*node.ExtendedNeighborInfo)
	s.mu.Unlock()

	// Start with immediate neighbors
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	// Track visited nodes to avoid cycles
	visited := make(map[node.NodeID]bool)
	visited[s.Node.ID] = true

	// Process immediate neighbors first (hop count 1)
	nodesToProcess := make(map[node.NodeID]*node.ExtendedNeighborInfo)
	for id, nbrInfo := range neighbors {
		visited[id] = true
		nodesToProcess[id] = &node.ExtendedNeighborInfo{
			NeighborInfo: nbrInfo,
			HopCount:     1,
			Path:         []node.NodeID{id},
		}
	}

	// BFS through the network up to maxHops
	for currentHop := 1; currentHop < maxHops; currentHop++ {
		nextHopNodes := make(map[node.NodeID]*node.ExtendedNeighborInfo)

		// Process all nodes at the current hop level
		for id, info := range nodesToProcess {
			// Skip if we can't process any further
			if info.HopCount > currentHop {
				nextHopNodes[id] = info
				continue
			}

			// Connect to this node and get its neighbors
			client, conn, err := ConnectToNode(ctx, info.Address)
			if err != nil {
				log.Printf("Failed to connect to node %s during topology discovery: %v", id, err)
				continue
			}

			// Request neighbors from this node
			req := &pb.GetNeighborsRequest{
				RequestingNodeId: string(s.Node.ID),
			}

			resp, err := client.GetNeighbors(ctx, req)
			conn.Close()

			if err != nil {
				log.Printf("Failed to get neighbors from node %s: %v", id, err)
				continue
			}

			// Process this node's neighbors
			for _, nbrEntry := range resp.Neighbors {
				nbrID := node.NodeID(nbrEntry.NodeId)

				// Skip if we've already visited this node
				if visited[nbrID] {
					continue
				}

				// Mark as visited
				visited[nbrID] = true

				// Convert coordinates to a Zone
				minPoint := make(node.Point, s.Config.Dimensions)
				maxPoint := make(node.Point, s.Config.Dimensions)

				for i := 0; i < s.Config.Dimensions; i++ {
					if i < len(nbrEntry.MinCoordinates) {
						minPoint[i] = nbrEntry.MinCoordinates[i]
					}
					if i < len(nbrEntry.MaxCoordinates) {
						maxPoint[i] = nbrEntry.MaxCoordinates[i]
					}
				}

				zone, err := node.NewZone(minPoint, maxPoint)
				if err != nil {
					log.Printf("Invalid zone for node %s: %v", nbrID, err)
					continue
				}

				// Create a new path by appending this node to the existing path
				path := make([]node.NodeID, len(info.Path)+1)
				copy(path, info.Path)
				path[len(info.Path)] = nbrID

				// Add to next hop nodes to process
				nextHopNodes[nbrID] = &node.ExtendedNeighborInfo{
					NeighborInfo: &node.NeighborInfo{
						ID:      nbrID,
						Address: nbrEntry.Address,
						Zone:    zone,
					},
					HopCount: info.HopCount + 1,
					Path:     path,
				}

				// Add to our extended neighbors list
				s.mu.Lock()
				s.Node.AddExtendedNeighbor(nbrID, nbrEntry.Address, zone, info.HopCount+1, path)
				s.mu.Unlock()
			}
		}

		// Update the nodes to process for the next iteration
		nodesToProcess = nextHopNodes
		if len(nodesToProcess) == 0 {
			break // No more nodes to discover
		}
	}

	log.Printf("Network topology discovery completed. Found %d extended neighbors.", len(s.Node.ExtendedNeighbors))
	return nil
}

// startTopologyDiscovery starts periodic network topology discovery
func (s *CANServer) startTopologyDiscovery() {
	// Skip if topology discovery is disabled
	if s.Config.TopologyRefreshInterval <= 0 || s.Config.MaxReplicationHops <= 1 {
		return
	}

	ticker := time.NewTicker(s.Config.TopologyRefreshInterval)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), s.Config.TopologyRefreshInterval/2)
		err := s.DiscoverNetworkTopology(ctx, s.Config.MaxReplicationHops)
		if err != nil {
			log.Printf("Topology discovery error: %v", err)
		}
		cancel()
	}
}

// GetExtendedNeighbors returns both immediate and extended neighbors
func (s *CANServer) GetExtendedNeighbors() map[node.NodeID]struct {
	Address  string
	HopCount int
	Path     []node.NodeID
} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[node.NodeID]struct {
		Address  string
		HopCount int
		Path     []node.NodeID
	})

	// Add immediate neighbors
	for id, info := range s.Node.Neighbors {
		result[id] = struct {
			Address  string
			HopCount int
			Path     []node.NodeID
		}{
			Address:  info.Address,
			HopCount: 1,
			Path:     []node.NodeID{id},
		}
	}

	// Add extended neighbors
	for id, info := range s.Node.ExtendedNeighbors {
		result[id] = struct {
			Address  string
			HopCount int
			Path     []node.NodeID
		}{
			Address:  info.Address,
			HopCount: info.HopCount,
			Path:     info.Path,
		}
	}

	return result
} 