package service

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	// "net"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/integrity"
	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/routing"
	"github.com/can-dht/pkg/storage"
	pb "github.com/can-dht/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// This will be generated after running protoc
	// pb "github.com/can-dht/proto"
)

// CANServer implements the CAN service
type CANServer struct {
	// Node is the local node instance
	Node *node.Node

	// Router handles routing
	Router *routing.Router

	// Store handles data persistence
	Store *storage.Store

	// KeyManager handles encryption and integrity
	KeyManager *crypto.KeyManager
	
	// AuthManager handles user authentication and access control
	AuthManager *crypto.EnhancedAuthManager

	// Config holds server configuration
	Config *CANConfig

	// LoadStats tracks load statistics for load balancing
	LoadStats *LoadStats

	// IntegrityChecker performs periodic integrity checks
	IntegrityChecker *integrity.PeriodicChecker

	// FailureCoordination tracks ongoing failure handling
	FailureCoordination map[node.NodeID]*FailureCoordinationInfo

	// For canceling takeover timers
	takeoverTimers map[node.NodeID]*time.Timer
	
	// KeyAccessCounts tracks access frequency for hot key detection
	KeyAccessCounts map[string]int
	
	// LastAccessTime tracks when keys were last accessed
	lastAccessTime map[string]time.Time
	
	// Cache for frequently accessed keys
	cache *LRUCache

	mu sync.RWMutex
}

// CANConfig holds the configuration for the CAN DHT server
type CANConfig struct {
	// Number of dimensions in the CAN space
	Dimensions int
	// Directory for data storage
	DataDir string
	// Enable encryption for stored data
	EnableEncryption bool
	// Enable authentication and access control
	EnableAuthentication bool
	// Replication factor for fault tolerance (1 = no replication)
	ReplicationFactor int
	// Interval between heartbeat messages
	HeartbeatInterval time.Duration
	// Timeout for considering a node dead
	HeartbeatTimeout time.Duration
	// Interval between integrity checks (0 = disabled)
	IntegrityCheckInterval time.Duration
	// Interval between replication checks for ensuring fault tolerance
	ReplicationCheckInterval time.Duration
	// Maximum number of hops for multi-hop replication
	MaxReplicationHops int
	// TTL for replicated data
	ReplicaTTL time.Duration
	// Threshold for considering a key "hot"
	HotKeyThreshold int
	// Maximum number of replicas for hot keys
	MaxHotKeyReplicas int
	// TTL for hot key tracking
	HotKeyTTL time.Duration
	// Check interval for hot key replication
	HotKeyCheckInterval int
	// Size of the LRU cache
	CacheSize int
	// TTL for cached data
	CacheTTL time.Duration
	// Delay for taking over a failed zone
	TakeoverDelay time.Duration
}

// FailureCoordinationInfo tracks coordination information for a node failure
type FailureCoordinationInfo struct {
	// The failed node's zone
	FailedZone *node.Zone
	
	// Have we cancelled our takeover timer?
	TimerCancelled bool
	
	// Have we initiated a takeover?
	TakeoverInitiated bool
}

// DefaultCANConfig returns a default configuration
func DefaultCANConfig() CANConfig {
	return CANConfig{
		Dimensions:             2,
		DataDir:                "data",
		EnableEncryption:       true,
		EnableAuthentication:   true,
		ReplicationFactor:      1,
		HeartbeatInterval:      5 * time.Second,
		HeartbeatTimeout:       15 * time.Second,
		IntegrityCheckInterval: 1 * time.Hour, // Default to hourly checks
		ReplicationCheckInterval: 10 * time.Minute, // Check replication every 10 minutes
		MaxReplicationHops:     3,              // Default to 3 hops for multi-hop replication
		ReplicaTTL:             24 * time.Hour, // Default to 24 hour TTL for replicas
		HotKeyThreshold:        100,            // Consider a key hot after 100 accesses
		MaxHotKeyReplicas:      10,             // Maximum of 10 replicas for hot keys
		HotKeyTTL:              6 * time.Hour,  // Reset hot key tracking after 6 hours
		HotKeyCheckInterval:    20,             // Check for replication every 20 accesses
		CacheSize:              1000,           // Cache up to 1000 keys
		CacheTTL:               30 * time.Minute, // Cache entries expire after 30 minutes
		TakeoverDelay:          5 * time.Second,  // Default to 5 seconds
	}
}

// LRUNode represents a node in the LRU cache
type LRUNode struct {
	Key       string
	Value     []byte
	ExpiresAt time.Time
	Next      *LRUNode
	Prev      *LRUNode
}

// LRUCache implements a Least Recently Used cache with TTL
type LRUCache struct {
	// Map for O(1) lookup
	cache map[string]*LRUNode
	
	// Doubly linked list for LRU ordering
	head *LRUNode
	tail *LRUNode
	
	// Maximum capacity
	capacity int
	
	// Default TTL for entries
	defaultTTL time.Duration
	
	// Current size
	size int
	
	// Access statistics for cache effectiveness
	hits int
	misses int
	
	mu sync.RWMutex
}

// NewLRUCache creates a new LRU cache with the given capacity and TTL
func NewLRUCache(capacity int, ttl time.Duration) *LRUCache {
	cache := &LRUCache{
		cache:      make(map[string]*LRUNode),
		capacity:   capacity,
		defaultTTL: ttl,
	}
	
	// Initialize head and tail sentinels
	cache.head = &LRUNode{}
	cache.tail = &LRUNode{}
	cache.head.Next = cache.tail
	cache.tail.Prev = cache.head
	
	return cache
}

// NewCANServer creates a new CAN server
func NewCANServer(nodeID node.NodeID, address string, config *CANConfig) (*CANServer, error) {
	// Create the local node
	// Initially, the node owns the entire coordinate space
	minPoint := make(node.Point, config.Dimensions)
	maxPoint := make(node.Point, config.Dimensions)
	for i := 0; i < config.Dimensions; i++ {
		minPoint[i] = 0.0
		maxPoint[i] = 1.0
	}
	
	zone, err := node.NewZone(minPoint, maxPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create zone: %w", err)
	}
	
	localNode := node.NewNode(nodeID, address, zone, config.Dimensions)
	
	// Set up routing
	router := routing.NewRouter(config.Dimensions)
	
	// Set up storage
	store, err := storage.NewStore(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}
	
	// Set up key manager for encryption and integrity
	keyManager, err := crypto.NewKeyManager()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize key manager: %w", err)
	}
	
	// Set up authentication manager if enabled
	var authManager *crypto.EnhancedAuthManager
	if config.EnableAuthentication {
		authManager = crypto.NewEnhancedAuthManager()
		// Register a default admin user
		if err := authManager.RegisterUser("admin", "admin123"); err != nil {
			log.Printf("WARNING: Failed to register default admin user: %v", err)
		} else {
			log.Printf("Created default admin user 'admin' with password 'admin123'")
		}
	}
	
	// Create enhanced cache with adaptive TTL
	cache := NewLRUCache(config.CacheSize, config.CacheTTL)
	
	// Create server
	server := &CANServer{
		Node:               localNode,
		Router:             router,
		Store:              store,
		KeyManager:         keyManager,
		AuthManager:        authManager,
		Config:             config,
		FailureCoordination: make(map[node.NodeID]*FailureCoordinationInfo),
		takeoverTimers:     make(map[node.NodeID]*time.Timer),
		KeyAccessCounts:    make(map[string]int),
		lastAccessTime:     make(map[string]time.Time),
		cache:              cache,
	}
	
	// Set up cache prefetch callback
	cache.SetPrefetchCallback(func(key string) ([]byte, error) {
		// This will fetch the value from storage or remote nodes if necessary
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		// We'll bypass the regular Get method to avoid infinite recursion
		// Instead, we'll just check if we have it in our local storage
		value, err := server.Store.Get(key)
		if err == nil && value != nil {
			return value, nil
		}
		
		// If not in local storage, it could be on another node
		// For prefetching, we'll skip remote access to avoid excess network traffic
		return nil, fmt.Errorf("not in local storage")
	})
	
	// Set up integrity checker if enabled
	if config.IntegrityCheckInterval > 0 {
		server.initIntegrityChecker()
	}
	
	return server, nil
}

// initIntegrityChecker initializes the periodic integrity checker
func (s *CANServer) initIntegrityChecker() {
	if s.Config.IntegrityCheckInterval <= 0 {
		// Integrity checking is disabled
		log.Printf("Integrity checking is disabled")
		return
	}

	// Create a storage adapter for the integrity checker
	storageAdapter := &integrityStorageAdapter{
		server: s,
	}

	// Create the integrity checker
	s.IntegrityChecker = integrity.NewPeriodicChecker(
		storageAdapter,
		s.KeyManager,
		s.Config.IntegrityCheckInterval,
	)

	// Set the recovery handler
	s.IntegrityChecker.SetRecoveryHandler(s.RecoverCorruptedData)

	// Start the integrity checker
	ctx := context.Background()
	go s.IntegrityChecker.Start(ctx)

	log.Printf("Started integrity checker with interval %v", s.Config.IntegrityCheckInterval)
}

// integrityStorageAdapter adapts the CANServer to the integrity.StorageInterface
type integrityStorageAdapter struct {
	server *CANServer
}

// GetAllKeys returns all keys in the storage
func (a *integrityStorageAdapter) GetAllKeys() ([]string, error) {
	a.server.mu.RLock()
	defer a.server.mu.RUnlock()

	keys := make([]string, 0, len(a.server.Node.Data))
	for key := range a.server.Node.Data {
		keys = append(keys, key)
	}
	return keys, nil
}

// Get retrieves a value by key
func (a *integrityStorageAdapter) Get(key string) ([]byte, error) {
	return a.server.Store.Get(key)
}

// Put stores a value with a key
func (a *integrityStorageAdapter) Put(key string, value []byte) error {
	return a.server.Store.Put(key, value)
}

// RecoverCorruptedData attempts to recover corrupted data
func (s *CANServer) RecoverCorruptedData(ctx context.Context, key string) error {
	log.Printf("Attempting to recover corrupted data for key %s", key)

	// Try to recover from replicas
	// First, find which node should own this key
	keyPoint := s.Router.HashToPoint(key)

	// Check if we own this key
	if s.Node.Zone.Contains(keyPoint) {
		// We own this key, try to recover from replicas
		return s.recoverFromReplicas(ctx, key)
	}

	// We don't own this key, route to the responsible node
	return s.routeRecoveryRequest(ctx, key, keyPoint)
}

// recoverFromReplicas attempts to recover data from replicas
func (s *CANServer) recoverFromReplicas(ctx context.Context, key string) error {
	// Get neighbors that might have replicas
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	log.Printf("Attempting to recover key %s from %d neighbors", key, len(neighbors))

	// Try each neighbor
	for _, neighbor := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s: %v", neighbor.ID, err)
			continue
		}

		// Request the value
		req := &pb.GetRequest{
			Key:     key,
			Forward: true, // Force the neighbor to check locally
		}

		resp, err := client.Get(ctx, req)
		conn.Close()

		if err != nil || !resp.Success || !resp.Exists {
			log.Printf("Neighbor %s doesn't have the key %s", neighbor.ID, key)
			continue
		}

		// Found a replica, update our copy with proper integrity
		value := resp.Value

		// Verify and store
		err = s.storeWithIntegrity(ctx, key, value)
		if err != nil {
			log.Printf("Failed to store recovered value with integrity: %v", err)
			continue
		}

		log.Printf("Successfully recovered key %s from neighbor %s", key, neighbor.ID)
		return nil
	}

	// If we got here, we couldn't recover from direct neighbors
	// Try multi-hop recovery
	return s.MultiHopRecovery(ctx, key)
}

// MultiHopRecovery attempts to recover data by asking nodes that are multiple hops away
func (s *CANServer) MultiHopRecovery(ctx context.Context, key string) error {
	// Get all nodes within MaxReplicationHops
	neighbors, err := s.GetNeighborsWithinHops(ctx, s.Config.MaxReplicationHops)
	if err != nil {
		return fmt.Errorf("failed to get multi-hop neighbors: %w", err)
	}

	log.Printf("Attempting to recover key %s from %d multi-hop neighbors", key, len(neighbors))

	// Skip the neighbors we already tried
	s.mu.RLock()
	directNeighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	// Try each neighbor
	for _, neighbor := range neighbors {
		// Skip direct neighbors
		if _, isDirect := directNeighbors[neighbor.ID]; isDirect {
			continue
		}

		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to multi-hop neighbor %s: %v", neighbor.ID, err)
			continue
		}

		// Request the value
		req := &pb.GetRequest{
			Key:     key,
			Forward: true,
		}

		resp, err := client.Get(ctx, req)
		conn.Close()

		if err != nil || !resp.Success || !resp.Exists {
			continue
		}

		// Found a replica, update our copy with proper integrity
		value := resp.Value

		// Verify and store
		err = s.storeWithIntegrity(ctx, key, value)
		if err != nil {
			log.Printf("Failed to store recovered value with integrity: %v", err)
			continue
		}

		log.Printf("Successfully recovered key %s from multi-hop neighbor %s", key, neighbor.ID)
		return nil
	}

	return fmt.Errorf("failed to recover data for key %s from any neighbor", key)
}

// routeRecoveryRequest routes a recovery request to the responsible node
func (s *CANServer) routeRecoveryRequest(ctx context.Context, key string, keyPoint node.Point) error {
	// Find the closest neighbor to the key
	closestNeighbor := s.Router.FindClosestNeighbor(keyPoint)
	if closestNeighbor == nil {
		return fmt.Errorf("unable to find responsible node for key %s", key)
	}

	// Connect to the neighbor
	client, conn, err := ConnectToNode(ctx, closestNeighbor.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to neighbor %s: %w", closestNeighbor.ID, err)
	}
	defer conn.Close()

	// Request recovery
	req := &pb.RecoverRequest{
		Key: key,
	}

	resp, err := client.Recover(ctx, req)
	if err != nil {
		return fmt.Errorf("recovery request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("recovery failed: %s", resp.Error)
	}

	return nil
}

// storeWithIntegrity stores a value with integrity protection
func (s *CANServer) storeWithIntegrity(ctx context.Context, key string, value []byte) error {
	// First, prepare the value with integrity information
	dataWithIntegrity, err := s.IntegrityChecker.PrepareDataWithIntegrity(value)
	if err != nil {
		return fmt.Errorf("failed to prepare integrity data: %w", err)
	}

	// Store in the Store
	if err := s.Store.Put(key, dataWithIntegrity); err != nil {
		return fmt.Errorf("failed to store data: %w", err)
	}

	// Also update the in-memory map
	s.mu.Lock()
	s.Node.Data[key] = string(value)
	s.mu.Unlock()

	return nil
}

// Start starts the CAN server
func (s *CANServer) Start() {
	// Start the heartbeat process
	go s.startHeartbeatProcess()

	// Start the integrity checker if configured
	if s.IntegrityChecker != nil {
		log.Printf("Starting periodic integrity checks with interval %v", s.Config.IntegrityCheckInterval)
		s.IntegrityChecker.Start()
	}
	
	// Start the replication check process if replication is enabled
	if s.Config.ReplicationFactor > 1 {
		log.Printf("Starting periodic replication checks with interval %v", s.Config.ReplicationCheckInterval)
		go s.CheckAndRebalanceReplicas(context.Background())
	}
}

// StartGRPCServer starts the gRPC server
func (s *CANServer) StartGRPCServer(grpcServer *grpc.Server) {
	// Create a gRPC server implementation
	grpcImpl := NewGRPCServer(s)

	// Register with the gRPC server
	grpcImpl.RegisterWithGRPCServer(grpcServer)
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

// handleDeadNode manages the process of handling a dead node's zone and data
func (s *CANServer) handleDeadNode(deadNodeID node.NodeID, info *node.NeighborInfo) {
	log.Printf("Node %s detected that node %s has failed", s.Node.ID, deadNodeID)

	s.mu.Lock()
	// Check if we're already handling this node's failure
	if _, exists := s.FailureCoordination[deadNodeID]; exists {
		s.mu.Unlock()
		log.Printf("Already handling failure of node %s", deadNodeID)
		return
	}

	// Record that we're handling this failure
	s.FailureCoordination[deadNodeID] = &FailureCoordinationInfo{
		FailedZone:       info.Zone,
		TimerCancelled:   false,
		TakeoverInitiated: false,
	}
	s.mu.Unlock()

	// Update metrics
	s.LoadStats.UpdateNodeFailure()

	// Calculate our node's "suitability" to take over the failed zone
	// This is based on zone volume, load, and adjacency
	var ourSuitability float64 = 0.0

	// Calculate zone volume (smaller is better)
	ourZoneVolume := calculateZoneVolume(s.Node.Zone)

	// Calculate load (lower is better)
	ourLoad := s.LoadStats.GetCurrentLoad()
	loadFactor := 1.0 - (ourLoad / 100.0) // Normalize to 0-1 where 1 is best (no load)

	// Calculate adjacency score (higher is better)
	adjacencyScore := s.calculateZoneAdjacency(s.Node.Zone, info.Zone)

	// Calculate overall suitability score
	// This is a weighted combination of factors
	// We weight adjacency highest since it's most important for routing efficiency
	ourSuitability = (0.2 * (1.0 - ourZoneVolume)) + (0.3 * loadFactor) + (0.5 * adjacencyScore)

	// Broadcast takeover message to neighbors
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First broadcast our intent to take over
	s.broadcastTakeoverMessage(ctx, deadNodeID, info.Zone, ourSuitability)

	// Start a timer to actually take over after a delay
	// The delay gives other nodes time to respond with their suitability
	takeoverTimer := time.NewTimer(s.Config.TakeoverDelay)
	s.mu.Lock()
	s.takeoverTimers[deadNodeID] = takeoverTimer
	s.mu.Unlock()

	go func() {
		select {
		case <-takeoverTimer.C:
			// Check if we should still proceed with takeover
			s.mu.Lock()
			failureInfo, exists := s.FailureCoordination[deadNodeID]
			if !exists || failureInfo.TimerCancelled {
				s.mu.Unlock()
				return
			}

			// Update the coordination info
			failureInfo.TakeoverInitiated = true
			s.mu.Unlock()

			// Proceed with takeover
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := s.TakeOverZone(ctx, deadNodeID, info.Zone); err != nil {
				log.Printf("Failed to take over zone of dead node %s: %v", deadNodeID, err)
			} else {
				log.Printf("Successfully took over zone of dead node %s", deadNodeID)
			}
		}
	}()
}

// ProcessTakeoverMessage handles a takeover message from another node
func (s *CANServer) ProcessTakeoverMessage(ctx context.Context, failedNodeID node.NodeID, senderNodeID node.NodeID, senderSuitability float64, failedZone *node.Zone) (bool, float64) {
	log.Printf("Received takeover message from %s for failed node %s with suitability %.4f", senderNodeID, failedNodeID, senderSuitability)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Calculate our suitability
	ourZoneVolume := calculateZoneVolume(s.Node.Zone)
	ourLoad := s.LoadStats.GetCurrentLoad()
	loadFactor := 1.0 - (ourLoad / 100.0)
	adjacencyScore := s.calculateZoneAdjacency(s.Node.Zone, failedZone)
	ourSuitability := (0.2 * (1.0 - ourZoneVolume)) + (0.3 * loadFactor) + (0.5 * adjacencyScore)

	// Check if we're already coordinating for this node
	failureInfo, weAreCoordinating := s.FailureCoordination[failedNodeID]
	
	// If we're already in the process of taking over this zone
	if weAreCoordinating && failureInfo.TakeoverInitiated {
		// We've already started the takeover, so we're going to continue
		log.Printf("Already initiated takeover for %s, ignoring message from %s", failedNodeID, senderNodeID)
		return false, ourSuitability
	}

	// Compare suitability scores
	if senderSuitability > ourSuitability {
		// The sender has a better suitability score
		log.Printf("Node %s has higher suitability for taking over %s (%.4f > %.4f)", senderNodeID, failedNodeID, senderSuitability, ourSuitability)
		
		// If we were planning to take over, cancel our timer
		if weAreCoordinating && !failureInfo.TimerCancelled {
			if timer, exists := s.takeoverTimers[failedNodeID]; exists && timer != nil {
				timer.Stop()
				failureInfo.TimerCancelled = true
				log.Printf("Cancelled our takeover timer for %s", failedNodeID)
			}
		}
		
		// The sender should take over
		return false, ourSuitability
	} else {
		// We have a better suitability score
		log.Printf("We have higher suitability for taking over %s (%.4f < %.4f)", failedNodeID, senderSuitability, ourSuitability)
		
		// We should take over, make sure we're coordinating
		if !weAreCoordinating {
			// Start coordinating now
			s.FailureCoordination[failedNodeID] = &FailureCoordinationInfo{
				FailedZone:        failedZone,
				TimerCancelled:    false,
				TakeoverInitiated: false,
			}
			
			// Start a timer for takeover
			takeoverTimer := time.NewTimer(s.Config.TakeoverDelay)
			s.takeoverTimers[failedNodeID] = takeoverTimer
			
			go func() {
				select {
				case <-takeoverTimer.C:
					// Check if we should still proceed with takeover
					s.mu.Lock()
					failureInfo, exists := s.FailureCoordination[failedNodeID]
					if !exists || failureInfo.TimerCancelled {
						s.mu.Unlock()
						return
					}

					// Update the coordination info
					failureInfo.TakeoverInitiated = true
					s.mu.Unlock()

					// Proceed with takeover
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					if err := s.TakeOverZone(ctx, failedNodeID, failedZone); err != nil {
						log.Printf("Failed to take over zone of dead node %s: %v", failedNodeID, err)
					} else {
						log.Printf("Successfully took over zone of dead node %s", failedNodeID)
					}
				}
			}()
		}
		
		// We should take over
		return true, ourSuitability
	}
}

// Calculate the suitability of a node for taking over a failed zone
func (s *CANServer) calculateTakeoverSuitability(ourZone, failedZone *node.Zone) float64 {
	// Calculate zone volume (smaller is better)
	ourZoneVolume := calculateZoneVolume(ourZone)

	// Calculate load (lower is better)
	ourLoad := s.LoadStats.GetCurrentLoad()
	loadFactor := 1.0 - (ourLoad / 100.0) // Normalize to 0-1 where 1 is best (no load)

	// Calculate adjacency score (higher is better)
	adjacencyScore := s.calculateZoneAdjacency(ourZone, failedZone)

	// Calculate overall suitability score
	// This is a weighted combination of factors
	return (0.2 * (1.0 - ourZoneVolume)) + (0.3 * loadFactor) + (0.5 * adjacencyScore)
}

// TakeOverZone takes over the zone and data of a failed node
func (s *CANServer) TakeOverZone(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone) error {
	log.Printf("Taking over zone of failed node %s", failedNodeID)

	// First, remove the dead node from our neighbor list
	s.mu.Lock()
	s.Node.RemoveNeighbor(failedNodeID)
	s.mu.Unlock()

	// Use the existing replication mechanism to recover data that would be in the failed zone
	if err := s.RecoverData(ctx, failedNodeID, failedZone); err != nil {
		return fmt.Errorf("failed to recover data: %w", err)
	}

	// Now add the failed zone to our list of zones
	s.LoadStats.RecordZoneAdjustment(s.Node.Zone, failedZone, "node_failure", s.LoadStats.GetCurrentLoad(), 0)

	// Try to merge the failedZone with our zone if possible
	canMerge, mergedZone := tryMergeZones(s.Node.Zone, failedZone)
	if canMerge {
		// If we can merge, update our zone
		s.mu.Lock()
		s.Node.Zone = mergedZone
		s.mu.Unlock()
		log.Printf("Merged failed zone with our zone")
	} else {
		// If we can't merge, add it as an extra zone
		s.LoadBalancer.AddExtraZone(failedZone)
		log.Printf("Added failed zone as an extra zone")
	}

	// Update our neighbor list to include failed node's neighbors
	if err := s.discoverAndAddFailedNodesNeighbors(ctx, failedNodeID, failedZone); err != nil {
		log.Printf("Warning: Error discovering failed node's neighbors: %v", err)
	}

	// Notify our neighbors about our zone change
	if err := s.NotifyNeighborsOfZoneChange(ctx); err != nil {
		log.Printf("Warning: Error notifying neighbors of zone change: %v", err)
	}

	// Update metrics
	s.LoadStats.UpdateZoneReassignment()

	return nil
}

// broadcastTakeoverMessage broadcasts a takeover message to all neighbors
func (s *CANServer) broadcastTakeoverMessage(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone, ourSuitability float64) {
	// Get neighbors
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	s.mu.RUnlock()

	// Prepare the takeover request
	takeoverReq := &pb.TakeoverRequest{
		FailedNodeId: string(failedNodeID),
		SenderNodeId: string(s.Node.ID),
		Suitability:  ourSuitability,
		FailedZone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: failedZone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: failedZone.MaxPoint,
			},
		},
	}

	// Track if any node with better suitability was found
	betterNodeFound := false
	var wg sync.WaitGroup
	var mu sync.Mutex // Mutex to protect betterNodeFound

	// Send takeover message to all neighbors
	for _, neighbor := range neighbors {
		// Skip the failed node
		if neighbor.ID == failedNodeID {
			continue
		}

		wg.Add(1)
		go func(neighbor *node.NeighborInfo) {
			defer wg.Done()

			// Connect to neighbor
			client, conn, err := ConnectToNode(ctx, neighbor.Address)
			if err != nil {
				log.Printf("Failed to connect to neighbor %s for takeover broadcast: %v", neighbor.ID, err)
				return
			}
			defer conn.Close()

			// Send takeover message
			resp, err := client.Takeover(ctx, takeoverReq)
			if err != nil {
				log.Printf("Failed to send takeover message to %s: %v", neighbor.ID, err)
				return
			}

			// Check if this node has better suitability
			if resp.ShouldTakeOver && resp.Suitability > ourSuitability {
				mu.Lock()
				betterNodeFound = true
				mu.Unlock()
				log.Printf("Node %s has better suitability (%.4f > %.4f)", neighbor.ID, resp.Suitability, ourSuitability)
			}
		}(neighbor)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// If a better node was found, cancel our takeover timer
	if betterNodeFound {
		s.mu.Lock()
		if failureInfo, exists := s.FailureCoordination[failedNodeID]; exists && !failureInfo.TimerCancelled {
			if timer, exists := s.takeoverTimers[failedNodeID]; exists && timer != nil {
				timer.Stop()
				failureInfo.TimerCancelled = true
				log.Printf("Cancelled our takeover timer for %s due to better node", failedNodeID)
			}
		}
		s.mu.Unlock()
	}
}

// Join connects to an existing CAN network or creates a new one if this is the first node
func (s *CANServer) Join(ctx context.Context, entryAddr string) error {
	// If no entry address provided, this is the first node in the network
	if entryAddr == "" {
		s.mu.Lock()
		defer s.mu.Unlock()
		
		// Take the entire coordinate space for the first node
		dimensions := s.Router.GetDimensions()
		minPoint := make(node.Point, dimensions)
		maxPoint := make(node.Point, dimensions)
		
		for i := 0; i < dimensions; i++ {
			minPoint[i] = 0.0
			maxPoint[i] = 1.0
		}
		
		zone, err := node.NewZone(minPoint, maxPoint)
		if err != nil {
			return fmt.Errorf("failed to create initial zone: %w", err)
		}
		
		s.Node.Zone = zone
		log.Printf("First node in network, taking entire coordinate space")
		return nil
	}
	
	// Connect to the entry node
	client, conn, err := ConnectToNode(ctx, entryAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to entry node %s: %w", entryAddr, err)
	}
	defer conn.Close()
	
	// Find node with largest zone volume for better load balancing
	nodesToCheck := []*pb.NodeInfo{}
	
	// First get the entry node's info
	entryInfo, err := client.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	if err != nil {
		return fmt.Errorf("failed to get entry node info: %w", err)
	}
	nodesToCheck = append(nodesToCheck, entryInfo.NodeInfo)
	
	// Get the entry node's neighbors too
	neighborsResp, err := client.GetNeighbors(ctx, &pb.GetNeighborsRequest{})
	if err != nil {
		return fmt.Errorf("failed to get entry node neighbors: %w", err)
	}
	nodesToCheck = append(nodesToCheck, neighborsResp.Neighbors...)
	
	// Find the node with the largest zone volume
	var targetNode *pb.NodeInfo
	var maxVolume float64 = -1
	
	for _, nodeInfo := range nodesToCheck {
		// Convert to our internal zone format to calculate volume
		minPoint := make(node.Point, len(nodeInfo.Zone.MinPoint.Coordinates))
		maxPoint := make(node.Point, len(nodeInfo.Zone.MaxPoint.Coordinates))
		copy(minPoint, nodeInfo.Zone.MinPoint.Coordinates)
		copy(maxPoint, nodeInfo.Zone.MaxPoint.Coordinates)
		
		zone, err := node.NewZone(minPoint, maxPoint)
		if err != nil {
			log.Printf("Warning: Failed to create zone for node %s: %v", nodeInfo.Id, err)
			continue
		}
		
		volume := calculateZoneVolume(zone)
		if volume > maxVolume {
			maxVolume = volume
			targetNode = nodeInfo
		}
	}
	
	if targetNode == nil {
		return fmt.Errorf("failed to find a node with valid zone")
	}
	
	// Generate a random point within the largest zone for more uniform partitioning
	targetClient, targetConn, err := ConnectToNode(ctx, targetNode.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to target node %s: %w", targetNode.Id, err)
	}
	defer targetConn.Close()
	
	// Create a random point in the coordinate space
	dimensions := len(targetNode.Zone.MinPoint.Coordinates)
	randPoint := make([]float64, dimensions)
	
	// Generate a random point within the target zone
	for i := 0; i < dimensions; i++ {
		min := targetNode.Zone.MinPoint.Coordinates[i]
		max := targetNode.Zone.MaxPoint.Coordinates[i]
		randPoint[i] = min + rand.Float64()*(max-min)
	}
	
	// Route to the node responsible for the random point
	routeReq := &pb.RouteRequest{
		Point: &pb.Point{Coordinates: randPoint},
	}
	
	routeResp, err := targetClient.Route(ctx, routeReq)
	if err != nil {
		return fmt.Errorf("failed to route to random point: %w", err)
	}
	
	// Connect to the node responsible for this point
	respNode, respConn, err := ConnectToNode(ctx, routeResp.NodeInfo.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to responsible node %s: %w", routeResp.NodeInfo.Id, err)
	}
	defer respConn.Close()
	
	// Request zone split from the responsible node
	joinReq := &pb.JoinRequest{
		NodeId:   string(s.Node.ID),
		Address:  s.Node.Address,
		JoinedAt: timestamppb.Now(),
	}
	
	joinResp, err := respNode.RequestJoin(ctx, joinReq)
	if err != nil {
		return fmt.Errorf("join request failed: %w", err)
	}
	
	// Process the assigned zone and data
	assignedZone := joinResp.AssignedZone
	if assignedZone == nil {
		return fmt.Errorf("no zone was assigned during join")
	}
	
	minPoint := make(node.Point, len(assignedZone.MinPoint.Coordinates))
	maxPoint := make(node.Point, len(assignedZone.MaxPoint.Coordinates))
	copy(minPoint, assignedZone.MinPoint.Coordinates)
	copy(maxPoint, assignedZone.MaxPoint.Coordinates)
	
	zone, err := node.NewZone(minPoint, maxPoint)
	if err != nil {
		return fmt.Errorf("failed to create zone from assigned zone: %w", err)
	}
	
	// Update our zone
	s.mu.Lock()
	s.Node.Zone = zone
	s.mu.Unlock()
	
	// Add data that belongs to our new zone
	for key, value := range joinResp.Data {
		if err := s.Store.Put(key, value); err != nil {
			log.Printf("Warning: Failed to store data for key %s: %v", key, err)
		}
	}
	
	log.Printf("Successfully joined the network with zone: %v", zone)
	
	// Discover neighbors
	return s.DiscoverNeighbors(ctx)
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

// Get handles Get requests for key-value pairs
func (s *CANServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	key := req.Key
	
	// If this is a forwarded request, directly check storage
	if req.Forward {
		// Check cache first for faster response
		cachedValue, found := s.cache.Get(key)
		if found {
			return &pb.GetResponse{
				Success: true,
				Value:   cachedValue,
				CacheHit: true,
			}, nil
		}
		
		// Track access for this key
		s.trackKeyAccess(key)
		
		// Not in cache, check storage
		value, err := s.Store.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				return &pb.GetResponse{
					Success: false,
					Error:   "Key not found",
				}, nil
			}
			
			return &pb.GetResponse{
				Success: false,
				Error:   fmt.Sprintf("Storage error: %v", err),
			}, status.Error(codes.Internal, fmt.Sprintf("storage error: %v", err))
		}
		
		// Found in storage, cache it for future requests
		s.cache.Put(key, value, s.Config.CacheTTL)
		
		return &pb.GetResponse{
			Success: true,
			Value:   value,
			CacheHit: false,
		}, nil
	}
	
	// Check cache first
	cachedValue, found := s.cache.Get(key)
	if found {
		s.trackKeyAccess(key)
		return &pb.GetResponse{
			Success: true,
			Value:   cachedValue,
			CacheHit: true,
		}, nil
	}
	
	// Not in cache, check if this key is in our zone
	keyPoint := s.Router.HashToPoint(key)
	if s.Node.Zone.Contains(keyPoint) {
		// The key should be in our storage
		s.trackKeyAccess(key)
		
		// Check for hot key replicas
		if s.isHotKeyReplica(key) {
			value, err := s.Store.Get(key)
			if err == nil {
				// Found in storage, cache it
				s.cache.Put(key, value, s.Config.CacheTTL)
				return &pb.GetResponse{
					Success: true,
					Value:   value,
					CacheHit: false,
				}, nil
			}
		}
		
		// Not a hot key or not found, check regular storage
		value, err := s.Store.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				return &pb.GetResponse{
					Success: false,
					Error:   "Key not found",
				}, nil
			}
			
			return &pb.GetResponse{
				Success: false,
				Error:   fmt.Sprintf("Storage error: %v", err),
			}, status.Error(codes.Internal, fmt.Sprintf("storage error: %v", err))
		}
		
		// Found in storage, cache it
		s.cache.Put(key, value, s.Config.CacheTTL)
		
		return &pb.GetResponse{
			Success: true,
			Value:   value,
			CacheHit: false,
		}, nil
	}
	
	// The key is not in our zone, find the closest neighbor
	closestNodeID, err := s.Router.FindClosestNode(keyPoint, s.Node.GetNeighborZones())
	if err != nil {
		return &pb.GetResponse{
			Success: false,
			Error:   fmt.Sprintf("Routing error: %v", err),
		}, status.Error(codes.Internal, fmt.Sprintf("routing error: %v", err))
	}
	
	// Get the neighbor's information
	neighborInfo, exists := s.Node.GetNeighbor(closestNodeID)
	if !exists {
		return &pb.GetResponse{
			Success: false,
			Error:   fmt.Sprintf("Neighbor node %s not found", closestNodeID),
		}, status.Error(codes.Internal, fmt.Sprintf("neighbor node %s not found", closestNodeID))
	}
	
	// Forward the request to the neighbor
	client, conn, err := ConnectToNode(ctx, neighborInfo.Address)
	if err != nil {
		return &pb.GetResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to connect to node %s: %v", closestNodeID, err),
		}, status.Error(codes.Unavailable, fmt.Sprintf("failed to connect to node %s: %v", closestNodeID, err))
	}
	defer conn.Close()
	
	// Forward with the Forward flag set
	forwardedReq := &pb.GetRequest{
		Key:     key,
		Forward: true,
	}
	
	resp, err := client.Get(ctx, forwardedReq)
	if err != nil {
		return &pb.GetResponse{
			Success: false,
			Error:   fmt.Sprintf("Forward request error: %v", err),
		}, status.Error(codes.Internal, fmt.Sprintf("forward request error: %v", err))
	}
	
	// If successful, cache the result
	if resp.Success {
		s.cache.Put(key, resp.Value, s.Config.CacheTTL)
	}
	
	return resp, nil
}

// isHotKeyReplica checks if the key is a hot key replica on this node
func (s *CANServer) isHotKeyReplica(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if this key is marked as a hot key replica
	meta, exists := s.keyMetadata[key]
	return exists && meta.IsHotKeyReplica
}

// KeyMetadata stores additional information about keys
type KeyMetadata struct {
	IsReplica       bool
	IsHotKeyReplica bool
	LastAccessed    time.Time
	AccessCount     int
}

// trackKeyAccess updates key metadata to track access patterns
func (s *CANServer) trackKeyAccess(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Initialize metadata map if needed
	if s.keyMetadata == nil {
		s.keyMetadata = make(map[string]KeyMetadata)
	}

	// Update or create metadata
	meta, exists := s.keyMetadata[key]
	if !exists {
		meta = KeyMetadata{
			LastAccessed: time.Now(),
			AccessCount:  1,
		}
	} else {
		meta.LastAccessed = time.Now()
		meta.AccessCount++
	}

	s.keyMetadata[key] = meta
}

// AdjustZone handles zone adjustment requests for load balancing
func (s *CANServer) AdjustZone(ctx context.Context, req *pb.AdjustZoneRequest) (*pb.AdjustZoneResponse, error) {
	log.Printf("Received zone adjustment request from %s, dimension: %d, old: %f, new: %f", 
		req.NeighborId, req.AdjustmentDimension, req.OldBoundaryPosition, req.NewBoundaryPosition)

	// Validate request
	if req.NeighborId == "" {
		return &pb.AdjustZoneResponse{
			Success: false,
			Error:   "neighbor ID cannot be empty",
		}, nil
	}

	// Check if we can adjust the zone
	if !s.canModifyZone {
		return &pb.AdjustZoneResponse{
			Success: false,
			Error:   "node is not allowed to modify its zone",
		}, nil
	}

	// Get the neighbor to validate the request
	var neighbor *node.NeighborInfo
	for _, n := range s.GetNeighborsUnsafe() {
		if string(n.ID) == req.NeighborId {
			neighbor = n
			break
		}
	}

	if neighbor == nil {
		return &pb.AdjustZoneResponse{
			Success: false,
			Error:   fmt.Sprintf("neighbor %s not found", req.NeighborId),
		}, nil
	}

	// Validate the dimension
	dim := int(req.AdjustmentDimension)
	if dim < 0 || dim >= len(s.zone.MinPoint) {
		return &pb.AdjustZoneResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid dimension: %d", dim),
		}, nil
	}

	// Validate the boundary position
	if s.zone.MinPoint[dim] != req.OldBoundaryPosition && s.zone.MaxPoint[dim] != req.OldBoundaryPosition {
		return &pb.AdjustZoneResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid boundary position: %f", req.OldBoundaryPosition),
		}, nil
	}

	// Create new zone
	newMinPoint := make(node.Point, len(s.zone.MinPoint))
	newMaxPoint := make(node.Point, len(s.zone.MaxPoint))
	copy(newMinPoint, s.zone.MinPoint)
	copy(newMaxPoint, s.zone.MaxPoint)

	// Adjust the boundary
	if s.zone.MinPoint[dim] == req.OldBoundaryPosition {
		newMinPoint[dim] = req.NewBoundaryPosition
	} else {
		newMaxPoint[dim] = req.NewBoundaryPosition
	}

	// Create the new zone
	newZone, err := node.NewZone(newMinPoint, newMaxPoint)
	if err != nil {
		return &pb.AdjustZoneResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create new zone: %v", err),
		}, nil
	}

	// Identify keys that need to be transferred
	keysToTransfer := make([]string, 0)
	s.mu.RLock()
	for key := range s.store {
		// Skip replicas since they're not zone-dependent
		meta, hasMeta := s.keyMetadata[key]
		if hasMeta && (meta.IsReplica || meta.IsHotKeyReplica) {
			continue
		}

		// Check if the key falls in the area we're giving up
		keyPoint, err := s.hashToPoint(key)
		if err != nil {
			continue
		}

		// If the key is no longer in our zone, add it to transfer list
		if !newZone.Contains(keyPoint) && s.zone.Contains(keyPoint) {
			keysToTransfer = append(keysToTransfer, key)
		}
	}
	s.mu.RUnlock()

	// If we have keys to transfer, do the transfer
	keysTransferred := 0
	if len(keysToTransfer) > 0 {
		log.Printf("Transferring %d keys to neighbor %s due to zone adjustment", 
			len(keysToTransfer), req.NeighborId)

		// Connect to the neighbor
		client, conn, err := s.getClient(neighbor.Address)
		if err != nil {
			return &pb.AdjustZoneResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to connect to neighbor: %v", err),
			}, nil
		}
		defer conn.Close()

		// Transfer each key
		for _, key := range keysToTransfer {
			s.mu.RLock()
			value, exists := s.store[key]
			s.mu.RUnlock()

			if !exists {
				continue
			}

			// Create a put request
			putReq := &pb.PutRequest{
				Key:     key,
				Value:   value,
				Forward: true, // Store locally
			}

			// Send the request
			putResp, err := client.Put(ctx, putReq)
			if err != nil || !putResp.Success {
				log.Printf("Failed to transfer key %s: %v", key, err)
				continue
			}

			// Delete the key from our store
			s.mu.Lock()
			delete(s.store, key)
			delete(s.keyMetadata, key)
			s.mu.Unlock()

			keysTransferred++
		}
	}

	// Update our zone
	s.mu.Lock()
	s.zone = newZone
	s.mu.Unlock()

	log.Printf("Zone adjusted: old zone %v, new zone %v, transferred %d keys", 
		s.zone, newZone, keysTransferred)

	// Update our neighbors about the zone change
	go s.notifyNeighborsOfZoneChange()

	return &pb.AdjustZoneResponse{
		Success:         true,
		KeysTransferred: int32(keysTransferred),
	}, nil
}

// notifyNeighborsOfZoneChange notifies all neighbors about our zone change
func (s *CANServer) notifyNeighborsOfZoneChange() {
	s.mu.RLock()
	neighbors := s.GetNeighborsUnsafe()
	currentZone := s.zone
	s.mu.RUnlock()

	for _, neighbor := range neighbors {
		// Skip if we can't connect
		client, conn, err := s.getClient(neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s: %v", neighbor.ID, err)
			continue
		}

		// Create update request
		req := &pb.UpdateNeighborRequest{
			Id:      string(s.nodeID),
			Address: s.address,
			Zone: &pb.Zone{
				MinPoint: &pb.Point{
					Coordinates: currentZone.MinPoint,
				},
				MaxPoint: &pb.Point{
					Coordinates: currentZone.MaxPoint,
				},
			},
		}

		// Send update
		_, err = client.UpdateNeighbor(context.Background(), req)
		conn.Close()
		if err != nil {
			log.Printf("Failed to notify neighbor %s of zone change: %v", neighbor.ID, err)
		}
	}
}

