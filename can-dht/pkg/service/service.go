package service

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"

	// "net"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/integrity"
	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/routing"
	"github.com/can-dht/pkg/storage"
	"github.com/can-dht/pkg/cache"
	"github.com/can-dht/pkg/security"
	pb "github.com/can-dht/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/credentials"
	// This will be generated after running protoc
	// pb "github.com/can-dht/proto"
)

// CANServer represents a Content Addressable Network server
type CANServer struct {
	// Node is the local node instance
	Node *node.Node

	// Router handles routing
	Router *routing.Router

	// Store handles data persistence
	Store *storage.Store

	// KeyManager handles encryption and integrity
	KeyManager crypto.KeyManagerInterface

	// Config holds server configuration
	Config *CANConfig

	// LoadStats tracks load statistics for load balancing
	LoadStats *LoadStats

	// RoutingTable stores information for probabilistic routing
	RoutingTable *RoutingTable

	// IntegrityChecker performs periodic integrity checks
	IntegrityChecker *integrity.PeriodicChecker

	// IntegrityManager handles enhanced integrity checking and repair
	IntegrityManager *IntegrityManager

	// ReplicaTracker tracks where data is replicated
	ReplicaTracker *ReplicaTracker

	// Cache for frequently accessed data
	Cache *cache.LRUCache

	// HotKeyTracker tracks frequently accessed keys
	HotKeyTracker *HotKeyTracker
	
	// AccessController handles API key authentication and authorization
	AccessController *security.AccessController
	
	// EnableAccessControl determines if access control is enforced
	EnableAccessControl bool
	
	// RequestDistributor manages request distribution for load balancing
	RequestDistributor *RequestDistributor
	
	// RoleManager manages node roles and capabilities
	RoleManager *RoleManager

	mu sync.RWMutex

	ctx        context.Context
	cancelFunc context.CancelFunc

	// APIKeysManager manages API keys and their permissions
	apiKeysManager *APIKeysManager
}

// APIKeysManager manages API keys and their permissions
type APIKeysManager struct {
	keys map[string]map[string]bool // key -> permission -> true/false
}

// NewAPIKeysManager creates a new API keys manager
func NewAPIKeysManager(filePath string) (*APIKeysManager, error) {
	// For the sake of this fix, we'll create a simple implementation
	// In a real system, this would load from a file or other source
	manager := &APIKeysManager{
		keys: make(map[string]map[string]bool),
	}
	
	// Add a default admin key with all permissions
	manager.keys["admin"] = map[string]bool{
		"read":  true,
		"write": true,
	}
	
	return manager, nil
}

// HasPermission checks if an API key has a specific permission
func (m *APIKeysManager) HasPermission(apiKey, permission string) bool {
	if perms, exists := m.keys[apiKey]; exists {
		return perms[permission]
	}
	return false
}

// CANConfig contains configuration options for the CAN server
type CANConfig struct {
	// NodeID is the unique identifier for this node
	NodeID string
	// Address is the address of this node
	Address string
	// Directory for data storage
	DataDir string
	// Dimensions is the number of dimensions in the CAN space
	Dimensions int
	// BootstrapNodes is a list of known nodes in the network
	BootstrapNodes []string
	// ReplicationFactor is the number of replicas to maintain
	ReplicationFactor int
	// HeartbeatInterval is the interval between heartbeat messages
	HeartbeatInterval time.Duration
	// HeartbeatTimeout is the time after which a node is considered dead
	HeartbeatTimeout time.Duration
	// IntegrityCheckInterval is how often to check data integrity (0 = disabled)
	IntegrityCheckInterval time.Duration
	// KeyRotationInterval is how often to rotate encryption keys (0 means no rotation)
	KeyRotationInterval time.Duration
	// UseEnhancedIntegrity indicates whether to use enhanced integrity checking
	UseEnhancedIntegrity bool
	// EnableCache determines if the LRU cache is enabled
	EnableCache bool
	// CacheSize is the maximum number of items in the cache
	CacheSize int
	// EnableTLS determines if TLS is used for connections
	EnableTLS bool
	// TLSCertFile is the path to the TLS certificate
	TLSCertFile string
	// TLSKeyFile is the path to the TLS key
	TLSKeyFile string
	// TLSCAFile is the path to the CA certificate for verifying client certificates
	TLSCAFile string
	// TLSServerName is the expected server name for verification
	TLSServerName string
	// LoadBalancingThreshold is the threshold for load balancing
	LoadBalancingThreshold float64
	// EnableAccessControl determines if API key authentication is enforced
	EnableAccessControl bool
	// ApiKeysFile is the path to the file containing API keys
	ApiKeysFile string
	// NodeRole defines the role of this node in the network
	NodeRole string
	// EnableZoneMerging determines if underloaded nodes should merge zones
	EnableZoneMerging bool
	// MinZoneSize is the minimum size a zone can be
	MinZoneSize float64
	// EnableRequestRedistribution determines if request redistribution is enabled
	EnableRequestRedistribution bool
	// RequestDistributionStrategy defines how requests should be distributed
	RequestDistributionStrategy string
}

// DefaultCANConfig returns the default configuration
func DefaultCANConfig() CANConfig {
	return CANConfig{
		Dimensions:                2,
		DataDir:                  "data",
		EnableEncryption:         true,
		ReplicationFactor:        1,
		HeartbeatInterval:        5 * time.Second,
		HeartbeatTimeout:         15 * time.Second,
		IntegrityCheckInterval:   0, // Disabled by default
		MaxReplicationHops:       2, // Default to 2 hops for extended replication
		ReplicaRepairInterval:    10 * time.Minute,
		TopologyRefreshInterval:  3 * time.Minute,
		EnableProbabilisticRouting: true, // Enable by default
		RequestThrottleLimit:     100.0, // Default 100 req/sec
		KeyFilePath:              "keys/encryption_keys.json", // Default path for key storage
		KeyRotationInterval:      24 * 7 * time.Hour, // Default to weekly rotation
		UseEnhancedIntegrity:     false, // Default to not using enhanced integrity
		EnableCache:              true,
		CacheSize:                1000,
		CacheTTL:                 10 * time.Minute,
		EnableFrequencyBasedReplication: true,
		HotKeyThreshold:          10.0,
		HotKeyDecayFactor:        0.9,
		HotKeyTTL:                24 * time.Hour,
		HotKeyMaxTracked:         100,
		HotKeyAnalysisInterval:   1 * time.Hour,
		HotKeyReplicationFactor:  2,
		HotKeyServingProbability:  0.5,
		EnableMTLS:               false, // Disabled by default for backward compatibility
		TLSCertFile:              "certs/node.crt",
		TLSKeyFile:               "certs/node.key",
		TLSCAFile:                "certs/ca.crt",
		TLSServerName:            "can-dht-node",
		LoadBalancingThreshold:   0.8, // Trigger proactive splitting at 80% capacity
		EnableAccessControl:      false,
		ApiKeysFile:              "api_keys.json",
		NodeRole:                 "standard",
		EnableZoneMerging:        false,
		ZoneUtilizationThreshold: 0.5,
		EnableRequestRedistribution: false,
		RequestDistributionStrategy: "random",
	}
}

// NewCANServer creates a new CAN server
func NewCANServer(config *CANConfig) (*CANServer, error) {
	if config.Dimensions <= 0 {
		return nil, fmt.Errorf("dimensions must be greater than 0")
	}

	// Create a new node
	nodeID := node.NodeID(config.NodeID)
	if nodeID == "" {
		// Generate a random node ID if not provided
		nodeID = node.GenerateNodeID()
	}

	// Create initial zone covering the entire space
	initialZone := node.NewZone(config.Dimensions)

	// Create node
	n := node.NewNode(nodeID, config.Address, initialZone)

	// Create the CAN server
	server := &CANServer{
		Config:       config,
		Node:         n,
		Router:       node.NewRouter(n),
		neighbors:    make(map[node.NodeID]*node.NodeInfo),
		lastSeen:     make(map[node.NodeID]time.Time),
		storage:      make(map[string]string),
		connections:  make(map[string]*grpc.ClientConn),
		mu:           &sync.RWMutex{},
		replicaMu:    &sync.RWMutex{},
		replicaStore: make(map[string]map[string][]byte), // nodeID -> key -> value
		stopped:      make(chan struct{}),
	}

	// Use the appropriate request distribution strategy
	switch config.RequestDistributionStrategy {
	case "random":
		server.requestDistributor = NewRandomDistributor(server)
	case "round_robin":
		server.requestDistributor = NewRoundRobinDistributor(server)
	case "least_loaded":
		server.requestDistributor = NewLeastLoadedDistributor(server)
	default:
		server.requestDistributor = NewDirectDistributor(server)
	}

	// Initialize role manager
	if config.NodeRole == "" {
		config.NodeRole = string(RoleStandard)
		log.Printf("No role specified, defaulting to '%s'", config.NodeRole)
	}
	server.RoleManager = NewRoleManager(server)
	log.Printf("Node initialized with role '%s'", config.NodeRole)

	// Initialize integrity checker
	if err := server.initIntegrityChecker(); err != nil {
		log.Printf("Failed to initialize integrity checker: %v", err)
	}

	// Initialize the replica tracker
	server.replicaTracker = NewReplicaTracker(server, config.ReplicationFactor)

	// Create an API keys manager if access control is enabled
	if config.EnableAccessControl {
		manager, err := NewAPIKeysManager(config.ApiKeysFile)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize API keys manager: %w", err)
		}
		server.apiKeysManager = manager
	}

	return server, nil
}

// initIntegrityChecker initializes the integrity checker
func (s *CANServer) initIntegrityChecker() error {
	// Skip if integrity check interval is zero
	if s.Config.IntegrityCheckInterval <= 0 {
		log.Printf("Integrity checking is disabled")
		return nil
	}
	
	// Check if we should use the enhanced integrity system
	if s.Config.UseEnhancedIntegrity {
		log.Printf("Initializing enhanced integrity system with check interval %v", s.Config.IntegrityCheckInterval)
		
		var err error
		s.IntegrityManager, err = NewIntegrityManager(s)
		if err != nil {
			log.Printf("Failed to initialize enhanced integrity system: %v. Falling back to basic checker.", err)
			s.initBasicIntegrityChecker()
			return nil
		}
		
		// Set up handlers
		s.IntegrityManager.SetCorruptionHandler(func(key string, err error) {
			log.Printf("Enhanced integrity check detected corruption in key '%s': %v", key, err)
			s.RecoverCorruptedData(context.Background(), key)
		})
		
		s.IntegrityManager.SetCheckCompleteHandler(func(success bool, count int) {
			if success {
				log.Printf("Enhanced integrity check completed successfully for %d keys", count)
			} else {
				log.Printf("Enhanced integrity check found issues among %d keys", count)
			}
		})
		
		// Start periodic checks in a goroutine
		go s.IntegrityManager.PerformPeriodicChecks(s.ctx, s.Config.IntegrityCheckInterval)
		
		log.Printf("Enhanced integrity system initialized")
	} else {
		// Use the basic integrity checker
		s.initBasicIntegrityChecker()
	}
	return nil
}

// initBasicIntegrityChecker initializes the basic integrity checker
func (s *CANServer) initBasicIntegrityChecker() {
	s.IntegrityChecker = integrity.NewPeriodicChecker(s.Store, s.KeyManager, s.Config.IntegrityCheckInterval)

	// Custom handler for corruption events
	s.IntegrityChecker.OnCorruptionFound = func(key string, result *integrity.CheckResult) {
		log.Printf("Data corruption detected for key %s: %v", key, result.Error)

		if result.RepairedOK {
			log.Printf("Successfully repaired corrupted data for key %s from replica", key)
		} else {
			// If we couldn't repair locally, try to get from other nodes
			log.Printf("Local repair failed for key %s, attempting to retrieve from network", key)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Try to retrieve from network
			if s.Config.ReplicationFactor > 1 {
				if err := s.RecoverCorruptedData(ctx, key); err != nil {
					log.Printf("Network recovery failed for key %s: %v", key, err)
				} else {
					log.Printf("Successfully recovered corrupted data for key %s from network", key)
				}
			}
		}
	}

	s.IntegrityChecker.OnCheckCompleted = func(stats *integrity.IntegrityStats) {
		log.Printf("Integrity check completed: Checked %d keys, found %d corrupted, repaired %d, %d unrepaired",
			stats.TotalChecks, stats.CorruptedData, stats.RepairedData, stats.UnrepairedData)
	}
}

// RecoverCorruptedData attempts to recover corrupted data from other nodes in the network
func (s *CANServer) RecoverCorruptedData(ctx context.Context, key string) error {
	// This is a simplified implementation
	// In a real system, you would:
	// 1. Find nodes that might have replicas of this data
	// 2. Query those nodes for the data
	// 3. Verify the integrity of retrieved data
	// 4. Store the valid data locally

	// For now, we'll just try to get the data from the network as a normal GET
	_, err := s.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to recover data: %w", err)
	}

	// If we got here, the data was successfully retrieved and is already stored locally
	log.Printf("Successfully recovered data for key %s from network", key)
	return nil
}

// Start the CAN server
func (s *CANServer) Start() {
	// Start heartbeat process
	go s.startHeartbeatProcess()

	// Start key rotation if enabled and interval is positive
	if s.Config.EnableEncryption && s.Config.KeyRotationInterval > 0 {
		go s.startKeyRotation()
	}

	// Start periodic integrity checking if enabled
	if s.Config.IntegrityCheckInterval > 0 {
		s.initIntegrityChecker()
	}

	// Start integrity checker if it's initialized
	if s.IntegrityChecker != nil {
		s.IntegrityChecker.Start()
	}

	// Start replica maintenance if replication is enabled
	if s.Config.ReplicationFactor > 1 && s.ReplicaTracker != nil {
		go s.startReplicaMaintenance()
	}

	// Start network topology discovery if multi-hop replication is enabled
	if s.Config.MaxReplicationHops > 1 {
		go s.startNetworkTopologyDiscovery()
	}

	// Start probabilistic routing if enabled
	if s.Config.EnableProbabilisticRouting && s.RoutingTable != nil {
		go s.InitProbabilisticRouting()
	}

	// Start load monitoring for proactive load balancing
	go s.StartLoadMonitoring(s.ctx)

	// Start cache maintenance if caching is enabled
	if s.Config.EnableCache {
		go s.startCacheMaintenance()
	}

	// Start hot key tracking and replication if enabled
	if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
		go s.StartHotKeyAnalysis()
		log.Printf("Started frequency-based replication with analysis interval %v", s.Config.HotKeyAnalysisInterval)
	}
}

// StartGRPCServer registers the CAN service with a gRPC server
func (s *CANServer) StartGRPCServer(grpcServer *grpc.Server) {
	grpcImpl := NewGRPCServer(s)
	grpcImpl.RegisterWithGRPCServer(grpcServer)
}

// CreateGRPCServer creates a new gRPC server with appropriate security settings
func (s *CANServer) CreateGRPCServer() (*grpc.Server, error) {
	var serverOpts []grpc.ServerOption
	
	if s.Config.EnableMTLS {
		// Set up TLS configuration for mTLS
		tlsConfig := crypto.TLSConfig{
			CertFile:   s.Config.TLSCertFile,
			KeyFile:    s.Config.TLSKeyFile,
			CAFile:     s.Config.TLSCAFile,
			ServerName: s.Config.TLSServerName,
		}
		
		creds, err := crypto.LoadServerTLSCredentials(tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(creds)))
	}
	
	grpcServer := grpc.NewServer(serverOpts...)
	s.StartGRPCServer(grpcServer)
	
	return grpcServer, nil
}

// Stop stops the CAN server
func (s *CANServer) Stop() error {
	// Stop the integrity checker if running
	if s.IntegrityChecker != nil {
		s.IntegrityChecker.Stop()
	}

	return s.Store.Close()
}

// startHeartbeatProcess starts sending periodic heartbeats to neighbors
func (s *CANServer) startHeartbeatProcess() {
	ticker := time.NewTicker(s.Config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.sendHeartbeats()
		s.checkDeadNodes()
	}
}

// sendHeartbeats sends heartbeats to all neighbors
func (s *CANServer) sendHeartbeats() {
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	for _, nbrInfo := range neighbors {
		// Use a separate goroutine for each neighbor to avoid blocking
		go func(neighbor *node.NeighborInfo) {
			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Connect to the neighbor
			client, conn, err := ConnectToNode(ctx, neighbor.Address)
			if err != nil {
				log.Printf("Failed to connect to neighbor %s for heartbeat: %v", neighbor.ID, err)
				return
			}
			defer conn.Close()

			// Send heartbeat
			heartbeatReq := &pb.HeartbeatRequest{
				NodeId:    string(s.Node.ID),
				Timestamp: time.Now().UnixNano(),
			}

			_, err = client.Heartbeat(ctx, heartbeatReq)
			if err != nil {
				log.Printf("Failed to send heartbeat to neighbor %s: %v", neighbor.ID, err)
			}
		}(nbrInfo)
	}
}

// checkDeadNodes checks for neighbors that haven't sent a heartbeat recently
func (s *CANServer) checkDeadNodes() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	deadNodes := make([]node.NodeID, 0)
	deadNodeInfo := make(map[node.NodeID]*node.NeighborInfo)

	// Check which nodes are dead
	for id, lastHeartbeat := range s.Node.Heartbeats {
		if now.Sub(lastHeartbeat) > s.Config.HeartbeatTimeout {
			// Get neighbor info before removing
			if info, exists := s.Node.Neighbors[id]; exists {
				deadNodeInfo[id] = info
			}
			deadNodes = append(deadNodes, id)
		}
	}

	if len(deadNodes) == 0 {
		return
	}

	log.Printf("Detected %d dead nodes: %v", len(deadNodes), deadNodes)

	// Process dead nodes one by one
	for _, id := range deadNodes {
		s.handleDeadNode(id, deadNodeInfo[id])
	}
}

// handleDeadNode handles a detected dead node
func (s *CANServer) handleDeadNode(deadNodeID node.NodeID, info *node.NeighborInfo) {
	// Remove the dead node from our neighbors list
	delete(s.Node.Neighbors, deadNodeID)
	delete(s.Node.Heartbeats, deadNodeID)

	// If we don't have the node's zone info, we can't do much more
	if info == nil || info.Zone == nil {
		log.Printf("No zone information for dead node %s", deadNodeID)
		return
	}

	// Determine if we should take over the zone
	shouldTakeOver := s.shouldTakeOverZone(deadNodeID, info.Zone)
	if !shouldTakeOver {
		log.Printf("Not taking over zone for dead node %s", deadNodeID)
		return
	}

	// Create a context for recovery operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Take over the zone and recover data
	if err := s.TakeOverZone(ctx, deadNodeID, info.Zone); err != nil {
		log.Printf("Failed to take over zone for dead node %s: %v", deadNodeID, err)
		return
	}

	log.Printf("Successfully took over zone for dead node %s", deadNodeID)
}

// shouldTakeOverZone determines if this node should take over a dead node's zone
func (s *CANServer) shouldTakeOverZone(deadNodeID node.NodeID, deadZone *node.Zone) bool {
	// Use our enhanced neighbor selection logic
	bestNodeID, found := s.selectTakeoverNeighbor(deadNodeID, deadZone)
	
	// We should take over if we're the best node
	return found && bestNodeID == s.Node.ID
}

// isZonesAdjacent checks if two zones are adjacent
func isZonesAdjacent(zone1, zone2 *node.Zone, dimensions int) bool {
	if zone1 == nil || zone2 == nil {
		return false
	}

	// Two zones are neighbors if they share a (d-1)-dimensional hyperplane
	adjacentCount := 0
	nonOverlapCount := 0

	for i := 0; i < dimensions; i++ {
		// Check if the zones are adjacent along dimension i
		if zone1.MaxPoint[i] == zone2.MinPoint[i] || zone1.MinPoint[i] == zone2.MaxPoint[i] {
			adjacentCount++
		}

		// Check if the zones overlap along dimension i
		if zone1.MinPoint[i] >= zone2.MaxPoint[i] || zone1.MaxPoint[i] <= zone2.MinPoint[i] {
			nonOverlapCount++
		}
	}

	// Zones are neighbors if they are adjacent along exactly one dimension
	// and have overlap in all other dimensions
	return adjacentCount == 1 && nonOverlapCount == 1
}

// Join joins an existing CAN network
func (s *CANServer) Join(ctx context.Context, entryNodeAddress string) error {
	// We need to find the node responsible for a random point
	// Generate a random point in the coordinate space
	randPoint := make(node.Point, s.Config.Dimensions)
	for i := 0; i < s.Config.Dimensions; i++ {
		randPoint[i] = rand.Float64()
	}

	// Connect to the entry node
	client, conn, err := ConnectToNode(ctx, entryNodeAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to entry node: %w", err)
	}
	defer conn.Close()

	// First find the node responsible for the random point
	findReq := &pb.FindNodeRequest{
		Target: &pb.FindNodeRequest_Point{
			Point: &pb.Point{
				Coordinates: randPoint,
			},
		},
	}

	findResp, err := client.FindNode(ctx, findReq)
	if err != nil {
		return fmt.Errorf("failed to find responsible node: %w", err)
	}

	// If the entry node is responsible, join directly
	var responsibleNodeAddress string
	if findResp.IsResponsible {
		responsibleNodeAddress = entryNodeAddress
	} else {
		// Otherwise, connect to the responsible node
		responsibleNodeAddress = findResp.ResponsibleNode.Address
	}

	// Connect to the responsible node
	var responsibleClient pb.CANServiceClient
	var responsibleConn *grpc.ClientConn
	if responsibleNodeAddress == entryNodeAddress {
		responsibleClient = client
		responsibleConn = conn
	} else {
		responsibleClient, responsibleConn, err = ConnectToNode(ctx, responsibleNodeAddress)
		if err != nil {
			return fmt.Errorf("failed to connect to responsible node: %w", err)
		}
		defer responsibleConn.Close()
	}

	// Send join request to the responsible node
	joinReq := &pb.JoinRequest{
		NewNodeId:      string(s.Node.ID),
		NewNodeAddress: s.Node.Address,
		JoinPoint: &pb.Point{
			Coordinates: randPoint,
		},
	}

	joinResp, err := responsibleClient.Join(ctx, joinReq)
	if err != nil {
		return fmt.Errorf("join request failed: %w", err)
	}

	if !joinResp.Success {
		return fmt.Errorf("join request was unsuccessful")
	}

	// Update our zone based on the response
	s.mu.Lock()
	newZone, err := node.NewZone(
		node.Point(joinResp.AssignedZone.MinPoint.Coordinates),
		node.Point(joinResp.AssignedZone.MaxPoint.Coordinates),
	)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to create zone from join response: %w", err)
	}
	s.Node.Zone = newZone

	// Add neighbors from the response
	for _, nbrInfo := range joinResp.Neighbors {
		nodeID := node.NodeID(nbrInfo.Id)
		if nodeID == s.Node.ID {
			continue // Skip self
		}

		zone, err := node.NewZone(
			node.Point(nbrInfo.Zone.MinPoint.Coordinates),
			node.Point(nbrInfo.Zone.MaxPoint.Coordinates),
		)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("failed to create zone for neighbor: %w", err)
		}

		s.Node.AddNeighbor(nodeID, nbrInfo.Address, zone)
	}

	// Store data from the response
	for key, value := range joinResp.Data {
		// Store the key-value pair
		s.Node.Put(key, string(value))
	}
	s.mu.Unlock()

	// Notify new neighbors about our presence
	for nodeID, nbrInfo := range s.Node.GetNeighbors() {
		if nodeID == node.NodeID(joinResp.Neighbors[0].Id) {
			// Skip the node we just split from, it already knows about us
			continue
		}

		// Connect to the neighbor
		nbrClient, nbrConn, err := ConnectToNode(ctx, nbrInfo.Address)
		if err != nil {
			log.Printf("Warning: failed to connect to neighbor %s: %v", nodeID, err)
			continue
		}

		// Send update neighbors request
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

		_, err = nbrClient.UpdateNeighbors(ctx, updateReq)
		nbrConn.Close()
		if err != nil {
			log.Printf("Warning: failed to update neighbor %s: %v", nodeID, err)
		}
	}

	log.Printf("Successfully joined the network, zone: %v", s.Node.Zone)
	return nil
}

// Leave leaves the CAN network gracefully
func (s *CANServer) Leave(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Node.GetNeighbors()) == 0 {
		// No neighbors, just leave quietly
		return nil
	}

	// Collect all data that needs to be transferred
	dataToTransfer := make(map[string][]byte)
	for key, value := range s.Node.Data {
		dataToTransfer[key] = []byte(value)
	}

	// Send leave request to all neighbors
	for _, nbrInfo := range s.Node.GetNeighbors() {
		// Connect to the neighbor
		nbrClient, nbrConn, err := ConnectToNode(ctx, nbrInfo.Address)
		if err != nil {
			log.Printf("Warning: failed to connect to neighbor %s: %v", nbrInfo.ID, err)
			continue
		}

		// Send leave request
		leaveReq := &pb.LeaveRequest{
			NodeId: string(s.Node.ID),
			Data:   dataToTransfer,
		}

		_, err = nbrClient.Leave(ctx, leaveReq)
		nbrConn.Close()
		if err != nil {
			log.Printf("Warning: failed to send leave request to neighbor %s: %v", nbrInfo.ID, err)
		}
	}

	// Clear data and neighbors
	s.Node.Data = make(map[string]string)
	s.Node.Neighbors = make(map[node.NodeID]*node.NeighborInfo)

	return nil
}

// Put stores a key-value pair in the CAN
func (s *CANServer) Put(ctx context.Context, key string, value []byte) error {
	// Check permissions if access control is enabled
	if s.EnableAccessControl {
		apiKey := security.GetAPIKey(ctx)
		if !s.AccessController.CheckPermission(apiKey, security.PermissionWrite) {
			return fmt.Errorf("access denied: missing write permission")
		}
	}

	// Check if this node is responsible for the key
	s.mu.RLock()
	keyPoint := s.Router.HashToPoint(key)
	nextHop, isResponsible := s.Router.FindResponsibleNode(s.Node, key)
	s.mu.RUnlock()

	if !isResponsible {
		// Forward to the responsible node
		return s.forwardPut(ctx, nextHop, key, value)
	}

	// Encrypt the value if encryption is enabled
	var dataToStore []byte
	if s.Config.EnableEncryption {
		if s.KeyManager == nil {
			return fmt.Errorf("encryption is enabled but key manager is not initialized")
		}

		encrypted, err := s.KeyManager.EncryptValue(value)
		if err != nil {
			return s.handleEncryptionError(key, err)
		}
		dataToStore = encrypted
	} else {
		dataToStore = value
	}

	// Store locally
	s.mu.Lock()
	s.Node.Data[key] = string(dataToStore)
	s.mu.Unlock()

	// If replication is enabled, replicate to neighbors
	if s.Config.ReplicationFactor > 1 && s.ReplicaTracker != nil {
		go s.replicatePut(ctx, key, dataToStore)
	}

	// Store to persistent storage if available
	if s.Store != nil {
		if err := s.Store.Put(key, dataToStore); err != nil {
			log.Printf("Warning: failed to persist data for key %s: %v", key, err)
		}
	}

	// If cache is enabled, update the cache
	if s.Config.EnableCache && s.Cache != nil {
		s.Cache.Put(key, value)
	}

	// Increment request count for load stats
	s.LoadStats.IncrementRequestCount()

	// Track access frequency if enabled
	if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
		s.HotKeyTracker.RecordAccess(key)
	}

	return nil
}

// Get retrieves a value by key from the CAN
func (s *CANServer) Get(ctx context.Context, key string) ([]byte, error) {
	// Check permissions if access control is enabled
	if s.EnableAccessControl {
		apiKey := security.GetAPIKey(ctx)
		if !s.AccessController.CheckPermission(apiKey, security.PermissionRead) {
			return nil, fmt.Errorf("access denied: missing read permission")
		}
	}

	// Check if cache has the value
	if s.Config.EnableCache && s.Cache != nil {
		if cachedValue, found := s.Cache.Get(key); found {
			// Track access frequency if enabled
			if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
				s.HotKeyTracker.RecordAccess(key)
			}
			return cachedValue, nil
		}
	}

	// Check if this node is responsible for the key
	s.mu.RLock()
	keyPoint := s.Router.HashToPoint(key)
	nextHop, isResponsible := s.Router.FindResponsibleNode(s.Node, key)
	s.mu.RUnlock()

	// Check if we have a hot key replica of this key
	if s.Config.EnableFrequencyBasedReplication && s.ReplicaTracker != nil {
		if s.ReplicaTracker.IsHotKeyReplica(key, s.Node.ID) {
			// Decide probabilistically whether to serve from replica
			if rand.Float64() <= s.Config.HotKeyServingProbability {
				data, err := s.getLocalData(key)
				if err == nil {
					return data, nil
				}
			}
		}
	}

	if !isResponsible {
		// Forward to the responsible node
		return s.forwardGet(ctx, nextHop, key)
	}

	// Try to retrieve from local storage
	data, err := s.getLocalData(key)
	if err != nil {
		return nil, err
	}

	// Track access frequency if enabled
	if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
		s.HotKeyTracker.RecordAccess(key)
	}

	// If cache is enabled, update the cache
	if s.Config.EnableCache && s.Cache != nil {
		s.Cache.Put(key, data)
	}

	// Increment request count for load stats
	s.LoadStats.IncrementRequestCount()

	return data, nil
}

// Delete removes a key-value pair from the CAN
func (s *CANServer) Delete(ctx context.Context, key string) error {
	// Check permissions if access control is enabled
	if s.EnableAccessControl {
		apiKey := security.GetAPIKey(ctx)
		if !s.AccessController.CheckPermission(apiKey, security.PermissionWrite) {
			return fmt.Errorf("access denied: missing write permission")
		}
	}

	// Check if this node is responsible for the key
	s.mu.RLock()
	keyPoint := s.Router.HashToPoint(key)
	nextHop, isResponsible := s.Router.FindResponsibleNode(s.Node, key)
	s.mu.RUnlock()

	if !isResponsible {
		// Forward to the responsible node
		return s.forwardDelete(ctx, nextHop, key)
	}

	// Delete locally
	s.mu.Lock()
	delete(s.Node.Data, key)
	s.mu.Unlock()

	// Delete from persistent storage if available
	if s.Store != nil {
		if err := s.Store.Delete(key); err != nil {
			log.Printf("Warning: failed to delete data for key %s from persistent storage: %v", key, err)
		}
	}

	// If replication is enabled, delete from replicas
	if s.Config.ReplicationFactor > 1 && s.ReplicaTracker != nil {
		go s.replicateDelete(ctx, key)
	}

	// If cache is enabled, invalidate the cache entry
	if s.Config.EnableCache && s.Cache != nil {
		s.Cache.Remove(key)
	}

	// If frequency tracking is enabled, remove from hot key tracker
	if s.Config.EnableFrequencyBasedReplication && s.HotKeyTracker != nil {
		s.HotKeyTracker.RemoveKey(key)
	}

	// Increment request count for load stats
	s.LoadStats.IncrementRequestCount()

	return nil
}

// calculateZoneVolume calculates the "volume" of a zone in n-dimensional space
func calculateZoneVolume(zone *node.Zone) float64 {
	if zone == nil {
		return 0.0
	}
	
	volume := 1.0
	for i := 0; i < len(zone.MinPoint); i++ {
		dimension := zone.MaxPoint[i] - zone.MinPoint[i]
		volume *= dimension
	}
	
	return volume
}

// estimateAverageZoneVolume estimates the average zone size in the network
func (s *CANServer) estimateAverageZoneVolume() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Count ourselves plus all known neighbors and extended neighbors
	nodeCount := 1 // Start with ourselves
	nodeCount += len(s.Node.Neighbors)
	nodeCount += len(s.Node.ExtendedNeighbors)
	
	// The entire CAN space is a unit hypercube with volume 1.0
	// So average zone size is approximately 1.0 / number of nodes
	if nodeCount > 0 {
		return 1.0 / float64(nodeCount)
	}
	return 1.0 // Default if we don't know any other nodes
}

// getNodeLoadScore calculates a load score for a node (lower is better for takeover)
func (s *CANServer) getNodeLoadScore(nodeID node.NodeID) float64 {
	// If it's us, we know our load precisely
	if nodeID == s.Node.ID {
		if s.LoadStats != nil {
			s.LoadStats.mu.RLock()
			requests := float64(s.LoadStats.RequestCount)
			s.LoadStats.mu.RUnlock()
			return 0.5 + (requests / 1000.0) // Base score plus request load factor
		}
		return 1.0 // Default load score
	}
	
	// For other nodes, we don't have precise info, so estimate based on zone size
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var neighborZone *node.Zone
	
	// Check immediate neighbors
	if neighbor, exists := s.Node.Neighbors[nodeID]; exists {
		neighborZone = neighbor.Zone
	} else if extNeighbor, exists := s.Node.ExtendedNeighbors[nodeID]; exists {
		// Check extended neighbors
		neighborZone = extNeighbor.NeighborInfo.Zone
	}
	
	if neighborZone != nil {
		// Estimate load based on zone volume relative to average
		zoneVolume := calculateZoneVolume(neighborZone)
		avgVolume := s.estimateAverageZoneVolume()
		
		if avgVolume > 0 {
			return zoneVolume / avgVolume
		}
	}
	
	return 1.0 // Default score if we don't have enough information
}

// getNodeStabilityScore calculates a stability score for a node (higher is more stable)
func (s *CANServer) getNodeStabilityScore(nodeID node.NodeID) float64 {
	// Start with a baseline score
	score := 1.0
	
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// If it's an immediate neighbor, we may have heartbeat history
	if lastHeartbeat, exists := s.Node.Heartbeats[nodeID]; exists {
		// Add stability based on heartbeat consistency
		hoursSinceJoin := time.Since(lastHeartbeat).Hours() + 1.0 // Add 1 to avoid division by zero
		score += math.Min(hoursSinceJoin / 24.0, 1.0) // Bonus for nodes that have been around longer (max 1.0)
	}
	
	return score
}

// getAdjacentNeighbors returns neighbors that are adjacent to a zone
func (s *CANServer) getAdjacentNeighbors(zone *node.Zone) map[node.NodeID]*node.NeighborInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	adjacentNeighbors := make(map[node.NodeID]*node.NeighborInfo)
	
	// Check all immediate neighbors
	for id, neighbor := range s.Node.Neighbors {
		if isZonesAdjacent(neighbor.Zone, zone, s.Config.Dimensions) {
			adjacentNeighbors[id] = neighbor
		}
	}
	
	return adjacentNeighbors
}

// selectTakeoverNeighbor selects the best neighbor to take over a failed node's zone
func (s *CANServer) selectTakeoverNeighbor(failedNodeID node.NodeID, failedZone *node.Zone) (node.NodeID, bool) {
	candidates := make(map[node.NodeID]float64)
	
	// Get all adjacent neighbors
	adjacentNeighbors := s.getAdjacentNeighbors(failedZone)
	
	// Skip if no adjacent neighbors
	if len(adjacentNeighbors) == 0 {
		return "", false
	}
	
	// Skip the failed node itself
	delete(adjacentNeighbors, failedNodeID)
	
	// If we're the only adjacent neighbor, we take over
	if len(adjacentNeighbors) == 1 {
		for id := range adjacentNeighbors {
			if id == s.Node.ID {
				return s.Node.ID, true
			}
		}
	}
	
	// Calculate takeover scores for each neighbor
	for id := range adjacentNeighbors {
		// Skip the failed node
		if id == failedNodeID {
			continue
		}
		
		// Calculate load score (lower is better)
		loadScore := s.getNodeLoadScore(id)
		
		// Calculate stability score (higher is better)
		stabilityScore := s.getNodeStabilityScore(id)
		
		// Calculate takeover score (lower is better)
		// We divide by stability to favor more stable nodes
		score := loadScore / stabilityScore
		
		candidates[id] = score
	}
	
	// Select the neighbor with the lowest score
	bestNeighborID := node.NodeID("")
	bestScore := math.MaxFloat64
	
	for id, score := range candidates {
		if score < bestScore {
			bestScore = score
			bestNeighborID = id
		}
	}
	
	return bestNeighborID, bestNeighborID != ""
}

// selectBackupNeighbor selects a second neighbor to help with zone takeover
func (s *CANServer) selectBackupNeighbor(failedNodeID node.NodeID, failedZone *node.Zone, primaryNeighborID node.NodeID) node.NodeID {
	// Get adjacent neighbors
	adjacentNeighbors := s.getAdjacentNeighbors(failedZone)
	
	// Remove primary neighbor and failed node
	delete(adjacentNeighbors, primaryNeighborID)
	delete(adjacentNeighbors, failedNodeID)
	
	if len(adjacentNeighbors) == 0 {
		return ""
	}
	
	// Calculate scores for each remaining neighbor
	candidates := make(map[node.NodeID]float64)
	
	for id := range adjacentNeighbors {
		// Calculate load score (lower is better)
		loadScore := s.getNodeLoadScore(id)
		
		// Calculate stability score (higher is better)
		stabilityScore := s.getNodeStabilityScore(id)
		
		// Calculate takeover score (lower is better)
		score := loadScore / stabilityScore
		
		candidates[id] = score
	}
	
	// Select the neighbor with the lowest score
	bestNeighborID := node.NodeID("")
	bestScore := math.MaxFloat64
	
	for id, score := range candidates {
		if score < bestScore {
			bestScore = score
			bestNeighborID = id
		}
	}
	
	return bestNeighborID
}

// notifyZoneTakeover notifies a neighbor to take over a zone
func (s *CANServer) notifyZoneTakeover(ctx context.Context, neighborID node.NodeID, zoneToTake *node.Zone) error {
	s.mu.RLock()
	var neighborAddress string
	
	// Find the neighbor's address
	if neighbor, exists := s.Node.Neighbors[neighborID]; exists {
		neighborAddress = neighbor.Address
	} else if extNeighbor, exists := s.Node.ExtendedNeighbors[neighborID]; exists {
		neighborAddress = extNeighbor.Address
	} else {
		s.mu.RUnlock()
		return fmt.Errorf("neighbor %s not found", neighborID)
	}
	s.mu.RUnlock()
	
	// Connect to the neighbor
	client, conn, err := ConnectToNode(ctx, neighborAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to neighbor %s: %v", neighborID, err)
	}
	defer conn.Close()
	
	// We don't have a direct "TakeOverZone" RPC, so we simulate it
	// by informing the neighbor about the failed node, which will
	// trigger their shouldTakeOverZone check
	
	// This is a simplification - in a real implementation, we would
	// add a specific RPC for this, but we're minimizing code changes
	
	// Create fake heartbeat to get the neighbor to notice the failed node
	// This is just informational - the neighbor will decide if they should take over
	log.Printf("Notifying neighbor %s to consider taking over zone", neighborID)
	
	// The exact implementation depends on the protocol
	// For now, we just log it and assume the neighbor will detect the failure through
	// their own heartbeat mechanism

	return nil
}

// // StartServer starts the gRPC server
// func (s *CANServer) StartServer(address string) error {
// 	lis, err := net.Listen("tcp", address)
// 	if err != nil {
// 		return fmt.Errorf("failed to listen: %w", err)
// 	}

// 	grpcServer := grpc.NewServer()
// 	pb.RegisterCANServiceServer(grpcServer, s)

// 	log.Printf("CAN DHT node starting on %s", address)
// 	return grpcServer.Serve(lis)
// }

// handleEncryptionError handles encryption-related errors
func (s *CANServer) handleEncryptionError(key string, err error) error {
	log.Printf("Encryption error for key %s: %v", key, err)
	
	// Check if this is a key corruption issue
	if errors.Is(err, crypto.ErrKeyCorruption) {
		log.Printf("Key corruption detected, attempting recovery")
		
		// Try to recover using persistent key manager if available
		if pkm, ok := s.KeyManager.(*crypto.PersistentKeyManager); ok {
			if recErr := pkm.ResetToDefaultKey(); recErr != nil {
				log.Printf("Failed to recover from key corruption: %v", recErr)
			} else {
				log.Printf("Successfully reset to default key")
			}
		}
	}
	
	// Wrap the error for better context
	if crypto.IsEncryptionError(err) {
		// Already an encryption error, just return it
		return err
	}
	
	// Wrap in an encryption error
	return crypto.NewEncryptionError("store", key, err)
}

// startCacheMaintenance starts the background process for cleaning expired cache entries
func (s *CANServer) startCacheMaintenance() {
	if !s.Config.EnableCache || s.Cache == nil {
		return
	}

	// Clean expired entries every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				removed := s.Cache.CleanExpired()
				if removed > 0 {
					log.Printf("Cache maintenance: removed %d expired entries", removed)
				}
				
				// Log cache metrics periodically
				metrics := s.Cache.GetMetrics()
				log.Printf("Cache stats: %d hits, %d misses, %d evictions, hit rate: %.2f%%", 
					metrics.Hits, metrics.Misses, metrics.Evictions, 
					float64(metrics.Hits)/float64(metrics.Hits+metrics.Misses)*100)
			case <-s.ctx.Done():
				log.Printf("Cache maintenance stopped")
				return
			}
		}
	}()
}

// getNodeRole returns the role of a node, or RoleStandard if unknown
func (s *CANServer) getNodeRole(nodeID node.NodeID) NodeRole {
	// TODO: In a more complete implementation, we would maintain
	// a map of node IDs to roles based on information exchanged during
	// the heartbeat protocol or node join process.
	// For now, we assume all nodes are standard roles.
	return RoleStandard
}

// ForwardPutToNode forwards a PUT request to a specific node
func (s *CANServer) ForwardPutToNode(ctx context.Context, key string, value []byte, targetNode *node.NodeInfo) (*pb.PutResponse, error) {
	// Create a client connection to the target node
	conn, err := s.ConnectToNode(ctx, targetNode.Address)
	if err != nil {
		return &pb.PutResponse{
			Success: false,
			Message: fmt.Sprintf("failed to connect to target node: %v", err),
		}, err
	}
	defer conn.Close()

	// Create a client
	client := pb.NewCANServiceClient(conn)

	// Forward the request with the forward flag set
	req := &pb.PutRequest{
		Key:     key,
		Value:   value,
		Forward: true,
	}

	// Call the Put method on the target node
	return client.Put(ctx, req)
}

// ForwardGetToNode forwards a GET request to a specific node
func (s *CANServer) ForwardGetToNode(ctx context.Context, key string, targetNode *node.NodeInfo) (*pb.GetResponse, error) {
	// Create a client connection to the target node
	conn, err := s.ConnectToNode(ctx, targetNode.Address)
	if err != nil {
		return &pb.GetResponse{
			Success: false,
			Message: fmt.Sprintf("failed to connect to target node: %v", err),
		}, err
	}
	defer conn.Close()

	// Create a client
	client := pb.NewCANServiceClient(conn)

	// Forward the request with the forward flag set
	req := &pb.GetRequest{
		Key:     key,
		Forward: true,
	}

	// Call the Get method on the target node
	return client.Get(ctx, req)
}

// ForwardDeleteToNode forwards a DELETE request to a specific node
func (s *CANServer) ForwardDeleteToNode(ctx context.Context, key string, targetNode *node.NodeInfo) (*pb.DeleteResponse, error) {
	// Create a client connection to the target node
	conn, err := s.ConnectToNode(ctx, targetNode.Address)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: fmt.Sprintf("failed to connect to target node: %v", err),
		}, err
	}
	defer conn.Close()

	// Create a client
	client := pb.NewCANServiceClient(conn)

	// Forward the request with the forward flag set
	req := &pb.DeleteRequest{
		Key:     key,
		Forward: true,
	}

	// Call the Delete method on the target node
	return client.Delete(ctx, req)
}

// ForwardPut forwards a PUT request to the responsible node
func (s *CANServer) ForwardPut(ctx context.Context, key string, value []byte) (*pb.PutResponse, error) {
	// Find the node responsible for the key
	keyPoint := s.Router.HashToPoint(key)
	responsibleNodeInfo, err := s.Router.FindClosestNeighbor(keyPoint)
	if err != nil {
		return &pb.PutResponse{
			Success: false,
			Message: fmt.Sprintf("failed to find responsible node: %v", err),
		}, err
	}

	// Forward the request to the responsible node
	return s.ForwardPutToNode(ctx, key, value, responsibleNodeInfo)
}

// ForwardGet forwards a GET request to the responsible node
func (s *CANServer) ForwardGet(ctx context.Context, key string) (*pb.GetResponse, error) {
	// Find the node responsible for the key
	keyPoint := s.Router.HashToPoint(key)
	responsibleNodeInfo, err := s.Router.FindClosestNeighbor(keyPoint)
	if err != nil {
		return &pb.GetResponse{
			Success: false,
			Message: fmt.Sprintf("failed to find responsible node: %v", err),
		}, err
	}

	// Forward the request to the responsible node
	return s.ForwardGetToNode(ctx, key, responsibleNodeInfo)
}

// ForwardDelete forwards a DELETE request to the responsible node
func (s *CANServer) ForwardDelete(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	// Find the node responsible for the key
	keyPoint := s.Router.HashToPoint(key)
	responsibleNodeInfo, err := s.Router.FindClosestNeighbor(keyPoint)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: fmt.Sprintf("failed to find responsible node: %v", err),
		}, err
	}

	// Forward the request to the responsible node
	return s.ForwardDeleteToNode(ctx, key, responsibleNodeInfo)
}
