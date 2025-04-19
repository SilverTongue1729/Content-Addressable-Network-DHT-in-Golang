package service

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"
	
	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HotKeyRoutingInfo stores routing information for hot keys
type HotKeyRoutingInfo struct {
	// Replicas contains the nodeIDs of all known replicas
	Replicas []node.NodeID
	
	// LastUpdated tracks when this routing info was last refreshed
	LastUpdated time.Time
	
	// UseCount tracks how many times this routing info has been used
	UseCount int
}

// RoutingTable stores routing information for probabilistic routing
type RoutingTable struct {
	// HotKeyRoutes maps keys to their routing information
	HotKeyRoutes map[string]*HotKeyRoutingInfo
	
	// NeighborLoads tracks load information about neighbors
	NeighborLoads map[node.NodeID]float64
	
	// LastUpdated tracks when the routing table was last updated
	LastUpdated time.Time
}

// NewRoutingTable creates a new routing table
func NewRoutingTable() *RoutingTable {
	return &RoutingTable{
		HotKeyRoutes:  make(map[string]*HotKeyRoutingInfo),
		NeighborLoads: make(map[node.NodeID]float64),
		LastUpdated:   time.Now(),
	}
}

// InitProbabilisticRouting initializes the probabilistic routing table
func (s *CANServer) InitProbabilisticRouting() {
	if s.RoutingTable == nil {
		s.RoutingTable = NewRoutingTable()
	}
	
	// Start a goroutine to periodically update the routing table
	go s.routingTableMaintenance()
}

// routingTableMaintenance periodically updates the routing table
func (s *CANServer) routingTableMaintenance() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			s.updateRoutingTable()
		}
	}
}

// updateRoutingTable updates the routing table with current information
func (s *CANServer) updateRoutingTable() {
	// Get current hot keys
	hotKeys := s.identifyHotKeys()
	if len(hotKeys) == 0 {
		return
	}
	
	// Update routing information for each hot key
	for _, key := range hotKeys {
		s.updateKeyRoutingInfo(key)
	}
	
	// Clean up stale entries
	s.cleanupRoutingTable()
}

// updateKeyRoutingInfo updates routing information for a specific key
func (s *CANServer) updateKeyRoutingInfo(key string) {
	// Get all replicas for this key from the replica tracker
	var replicas []node.NodeID
	
	if s.ReplicaTracker != nil {
		replicaInfo := s.ReplicaTracker.GetReplicas(key)
		replicas = make([]node.NodeID, 0, len(replicaInfo))
		
		for nodeID := range replicaInfo {
			replicas = append(replicas, nodeID)
		}
	}
	
	// Add this node if it owns the key
	point := s.Router.HashToPoint(key)
	if s.Node.Zone.Contains(point) {
		replicas = append(replicas, s.Node.ID)
	}
	
	// If we have routing info, update it; otherwise create new
	if s.RoutingTable.HotKeyRoutes[key] == nil {
		s.RoutingTable.HotKeyRoutes[key] = &HotKeyRoutingInfo{
			Replicas:    replicas,
			LastUpdated: time.Now(),
			UseCount:    0,
		}
	} else {
		s.RoutingTable.HotKeyRoutes[key].Replicas = replicas
		s.RoutingTable.HotKeyRoutes[key].LastUpdated = time.Now()
	}
}

// cleanupRoutingTable removes stale entries from the routing table
func (s *CANServer) cleanupRoutingTable() {
	now := time.Now()
	staleThreshold := 30 * time.Minute
	
	// Remove routing information for keys that haven't been updated recently
	for key, info := range s.RoutingTable.HotKeyRoutes {
		if now.Sub(info.LastUpdated) > staleThreshold {
			delete(s.RoutingTable.HotKeyRoutes, key)
		}
	}
}

// ShouldForwardRequest determines if a request should be forwarded to a replica
// instead of being processed locally
func (s *CANServer) ShouldForwardRequest(key string) (bool, node.NodeID) {
	// If we don't have a routing table or the key isn't hot, don't forward
	if s.RoutingTable == nil || s.RoutingTable.HotKeyRoutes[key] == nil {
		return false, ""
	}
	
	routeInfo := s.RoutingTable.HotKeyRoutes[key]
	
	// If this is the only replica, don't forward
	if len(routeInfo.Replicas) <= 1 {
		return false, ""
	}
	
	// Calculate forwarding probability based on load
	// The more loaded we are, the more likely we are to forward
	forwardingProbability := 0.0
	
	s.LoadStats.mu.RLock()
	if s.LoadStats.ZoneLoadFactor > 1.0 {
		// If we're more loaded than average, increase probability
		forwardingProbability = 0.5 * s.LoadStats.ZoneLoadFactor
	}
	s.LoadStats.mu.RUnlock()
	
	// Hard cap at 90% to ensure some requests are still processed locally
	if forwardingProbability > 0.9 {
		forwardingProbability = 0.9
	}
	
	// Decide whether to forward
	if rand.Float64() < forwardingProbability {
		// Select a replica to forward to (excluding ourselves)
		candidates := make([]node.NodeID, 0, len(routeInfo.Replicas))
		
		for _, nodeID := range routeInfo.Replicas {
			if nodeID != s.Node.ID {
				candidates = append(candidates, nodeID)
			}
		}
		
		if len(candidates) > 0 {
			// Pick a random replica
			selectedIdx := rand.Intn(len(candidates))
			routeInfo.UseCount++
			return true, candidates[selectedIdx]
		}
	}
	
	return false, ""
}

// GetWithProbabilisticRouting retrieves a value by key using probabilistic routing
func (s *CANServer) GetWithProbabilisticRouting(ctx context.Context, key string) ([]byte, error) {
	// Record request for load balancing
	s.RecordRequest(key, false)
	
	// Track key access frequency if enabled
	if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
		s.HotKeyTracker.RecordAccess(key)
	}
	
	// Check cache first if enabled
	if s.Config.EnableCache && s.Cache != nil {
		if cachedValue, found := s.Cache.Get(key); found {
			log.Printf("Cache hit for key %s", key)
			return cachedValue, nil
		}
		log.Printf("Cache miss for key %s", key)
	}
	
	// Check if we have a frequency-based replica of this key
	if s.Config.EnableFrequencyBasedReplication && s.ReplicaTracker != nil {
		replicas := s.ReplicaTracker.GetReplicas(key)
		if info, exists := replicas[s.Node.ID]; exists && info.IsFrequencyBased {
			// Record access to this replica
			s.ReplicaTracker.RecordReplicaAccess(key, s.Node.ID)
			
			// Hot keys always have higher probability of local serving
			if rand.Float64() < s.Config.HotKeyServingProbability {
				// Retrieve the value
				value, exists, err := s.Store.Get(key)
				if err == nil && exists {
					log.Printf("Serving hot key %s from local frequency-based replica", key)
					return s.processRetrievedValue(key, value)
				}
			}
		}
	}
	
	// First check if we should forward this request to a replica
	shouldForward, replicaID := s.ShouldForwardRequest(key)
	
	if shouldForward && replicaID != "" {
		log.Printf("Probabilistically forwarding GET for hot key %s to replica %s", key, replicaID)
		
		// Get replica address
		var replicaAddress string
		
		s.mu.RLock()
		if neighbor, exists := s.Node.Neighbors[replicaID]; exists {
			replicaAddress = neighbor.Address
		} else if extNeighbor, exists := s.Node.ExtendedNeighbors[replicaID]; exists {
			replicaAddress = extNeighbor.Address
		}
		s.mu.RUnlock()
		
		if replicaAddress != "" {
			// Forward to the replica
			client, conn, err := ConnectToNode(ctx, replicaAddress)
			if err != nil {
				log.Printf("Failed to connect to replica %s: %v", replicaID, err)
				// Fall back to normal routing if connection fails
			} else {
				defer conn.Close()
				
				// Create the request with the forward flag set to true
				// This tells the receiving node to check its local store
				// even if it's not normally responsible for the key
				req := &pb.GetRequest{
					Key:     key,
					Forward: true,
				}
				
				// Send the request
				resp, err := client.Get(ctx, req)
				if err == nil && resp.Success && resp.Exists {
					// Add to cache if enabled
					if s.Config.EnableCache && s.Cache != nil {
						s.Cache.Put(key, resp.Value)
					}
					
					// Record access to this replica
					if s.ReplicaTracker != nil {
						s.ReplicaTracker.RecordReplicaAccess(key, replicaID)
					}
					
					return resp.Value, nil
				}
				
				// If forwarding fails, fall back to normal routing
				log.Printf("Forwarded request to replica failed, falling back to normal routing")
			}
		}
	}
	
	// Fall back to normal Get behavior
	return s.standardGet(ctx, key)
}

// standardGet implements the original Get method logic
func (s *CANServer) standardGet(ctx context.Context, key string) ([]byte, error) {
	// Record request for load balancing
	s.RecordRequest(key, false)

	// Track key access frequency if enabled
	if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
		s.HotKeyTracker.RecordAccess(key)
	}

	// Check cache first if enabled
	if s.Config.EnableCache && s.Cache != nil {
		if cachedValue, found := s.Cache.Get(key); found {
			log.Printf("Cache hit for key %s", key)
			return cachedValue, nil
		}
		log.Printf("Cache miss for key %s", key)
	}
	
	// Hash the key to find the responsible node
	point := s.Router.HashToPoint(key)
	
	// Check if we have a frequency-based replica of this key
	var hasFrequencyReplica bool
	if s.Config.EnableFrequencyBasedReplication && s.ReplicaTracker != nil {
		// Check if we have a frequency-based replica
		replicas := s.ReplicaTracker.GetReplicas(key)
		if info, exists := replicas[s.Node.ID]; exists && info.IsFrequencyBased {
			hasFrequencyReplica = true
			
			// Record access to this replica
			s.ReplicaTracker.RecordReplicaAccess(key, s.Node.ID)
			
			// Use random probability to decide if we serve locally
			if rand.Float64() < s.Config.HotKeyServingProbability {
				// Retrieve the value
				value, exists, err := s.Store.Get(key)
				if err != nil {
					return nil, fmt.Errorf("failed to retrieve replicated hot key: %w", err)
				}
				
				if exists {
					log.Printf("Serving hot key %s from local frequency-based replica", key)
					
					// Decrypt if needed, cache, and return
					return s.processRetrievedValue(key, value)
				}
			}
		}
	}
	
	// Check if the local node is responsible for this point
	if s.Node.Zone.Contains(point) {
		// Retrieve the value
		value, exists, err := s.Store.Get(key)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve value: %w", err)
		}
		
		if !exists {
			return nil, status.Errorf(codes.NotFound, "key not found")
		}
		
		// Decrypt, cache, and return the value
		return s.processRetrievedValue(key, value)
	}
	
	// If not responsible, find the next hop
	nextHop, isResponsible := s.Router.FindResponsibleNode(s.Node, key)
	if isResponsible {
		// This should not happen, as we already checked if the local node is responsible
		return nil, fmt.Errorf("internal error: router says local node is responsible but zone check failed")
	}
	
	if nextHop == nil {
		return nil, fmt.Errorf("no route to responsible node")
	}
	
	// Forward the request to the next hop
	client, conn, err := ConnectToNode(ctx, nextHop.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to next hop: %w", err)
	}
	defer conn.Close()
	
	// Create the request
	req := &pb.GetRequest{
		Key:     key,
		Forward: false, // Not a forwarded request yet
	}
	
	// Send the request
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to forward GET request: %w", err)
	}
	
	if !resp.Success {
		return nil, fmt.Errorf("GET request was unsuccessful")
	}
	
	if !resp.Exists {
		return nil, status.Errorf(codes.NotFound, "key not found")
	}
	
	// Add to cache if enabled
	if s.Config.EnableCache && s.Cache != nil {
		s.Cache.Put(key, resp.Value)
	}
	
	return resp.Value, nil
}

// processRetrievedValue handles decryption and caching of retrieved values
func (s *CANServer) processRetrievedValue(key string, value []byte) ([]byte, error) {
	var finalValue []byte
	
	// Decrypt the value if encryption is enabled
	if s.Config.EnableEncryption && s.KeyManager != nil {
		// Deserialize the secure data
		secureData, err := crypto.DeserializeSecureData(string(value))
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize secure data: %w", err)
		}
		
		// Decrypt and verify
		plaintext, err := s.KeyManager.DecryptAndVerify(secureData)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt value: %w", err)
		}
		
		finalValue = plaintext
	} else {
		finalValue = value
	}
	
	// Add to cache if enabled
	if s.Config.EnableCache && s.Cache != nil {
		s.Cache.Put(key, finalValue)
	}
	
	return finalValue, nil
} 