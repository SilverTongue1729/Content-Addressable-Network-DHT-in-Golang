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
	// Replication factor for fault tolerance (1 = no replication)
	ReplicationFactor int
	// Interval between heartbeat messages
	HeartbeatInterval time.Duration
	// Timeout for considering a node dead
	HeartbeatTimeout time.Duration
	// Interval between integrity checks (0 = disabled)
	IntegrityCheckInterval time.Duration
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
		ReplicationFactor:      1,
		HeartbeatInterval:      5 * time.Second,
		HeartbeatTimeout:       15 * time.Second,
		IntegrityCheckInterval: 1 * time.Hour, // Default to hourly checks
	}
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

	// Create the router
	router := routing.NewRouter(config.Dimensions)

	// Create the store
	storeOpts := storage.DefaultStoreOptions()
	storeOpts.DataDir = config.DataDir
	store, err := storage.NewStore(storeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	// Create the key manager if encryption is enabled
	var keyManager *crypto.KeyManager
	if config.EnableEncryption {
		keyManager, err = crypto.NewKeyManager()
		if err != nil {
			return nil, fmt.Errorf("failed to create key manager: %w", err)
		}
	}

	// Initialize failure coordination maps
	server := &CANServer{
		Node:                localNode,
		Router:              router,
		Store:               store,
		KeyManager:          keyManager,
		Config:              config,
		FailureCoordination: make(map[node.NodeID]*FailureCoordinationInfo),
		takeoverTimers:      make(map[node.NodeID]*time.Timer),
	}

	// Initialize the integrity checker if interval is non-zero
	if config.IntegrityCheckInterval > 0 {
		server.initIntegrityChecker()
	}

	return server, nil
}

// initIntegrityChecker initializes the integrity checker
func (s *CANServer) initIntegrityChecker() {
	if s.KeyManager == nil {
		// No integrity checker needed if encryption is disabled
		return
	}

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

	// Custom handler for check completion
	s.IntegrityChecker.OnCheckCompleted = func(stats *integrity.IntegrityStats) {
		log.Printf("Integrity check completed: %d total, %d corrupted, %d repaired, %d unrepaired",
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

// Start starts the CAN server
func (s *CANServer) Start() {
	// Start the heartbeat process
	go s.startHeartbeatProcess()

	// Start the integrity checker if configured
	if s.IntegrityChecker != nil {
		log.Printf("Starting periodic integrity checks with interval %v", s.Config.IntegrityCheckInterval)
		s.IntegrityChecker.Start()
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

	// Check if we're actually a neighbor of the failed node
	if !s.Node.IsNeighborZone(info.Zone) {
		log.Printf("Not a direct neighbor of dead node %s, not participating in takeover", deadNodeID)
		return
	}

	log.Printf("Detected dead node %s, initiating takeover coordination", deadNodeID)

	// Create failure coordination info
	s.FailureCoordination[deadNodeID] = &FailureCoordinationInfo{
		FailedZone:        info.Zone,
		TimerCancelled:    false,
		TakeoverInitiated: false,
	}

	// Calculate our zone volume
	ourVolume := calculateZoneVolume(s.Node.Zone)

	// Set timer proportional to our zone volume (scaled by a constant)
	// Smaller zones get smaller timeouts, so they attempt takeover first
	timerDuration := time.Duration(ourVolume*1000) * time.Millisecond

	log.Printf("Starting takeover timer for node %s with duration %v", deadNodeID, timerDuration)

	// Create a context with timeout for the takeover attempt
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	// Start the timer
	timer := time.AfterFunc(timerDuration, func() {
		// This will execute when the timer expires
		s.mu.Lock()
		info := s.FailureCoordination[deadNodeID]

		// Check if our timer was cancelled
		if info == nil || info.TimerCancelled {
			s.mu.Unlock()
			cancel()
			return
		}

		// Update our state
		info.TakeoverInitiated = true
		s.mu.Unlock()

		// Send TAKEOVER messages to all neighbors of the failed node
		s.broadcastTakeoverMessage(ctx, deadNodeID, info.FailedZone, ourVolume)

		// Wait briefly for responses
		time.Sleep(1 * time.Second)

		// Check if we should proceed with takeover
		s.mu.Lock()
		info = s.FailureCoordination[deadNodeID]
		shouldProceed := info != nil && !info.TimerCancelled && info.TakeoverInitiated
		s.mu.Unlock()

		if shouldProceed {
			// We won the coordination, proceed with takeover
			log.Printf("Proceeding with takeover for node %s", deadNodeID)
			if err := s.TakeOverZone(ctx, deadNodeID, info.FailedZone); err != nil {
				log.Printf("Failed to take over zone for dead node %s: %v", deadNodeID, err)
			}
		}

		cancel()
	})

	// Store the timer so we can cancel it if needed
	s.takeoverTimers[deadNodeID] = timer
}

// broadcastTakeoverMessage sends TAKEOVER messages to all neighbors of the failed node
func (s *CANServer) broadcastTakeoverMessage(ctx context.Context, failedNodeID node.NodeID, failedZone *node.Zone, ourVolume float64) {
	// Get all our neighbors
	neighbors := s.Node.GetNeighbors()

	// Prepare the takeover request
	takeoverReq := &pb.TakeoverRequest{
		SenderNodeId: string(s.Node.ID),
		FailedNodeId: string(failedNodeID),
		ZoneVolume:   ourVolume,
		FailedZone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: failedZone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: failedZone.MaxPoint,
			},
		},
	}

	// Send to all neighbors
	for _, neighbor := range neighbors {
		// Skip neighbors that are not relevant (not neighbors of the failed node)
		if !s.Node.IsNeighborZone(failedZone) {
			continue
		}

		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, neighbor.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for takeover coordination: %v", neighbor.ID, err)
			continue
		}

		// Send the takeover message
		resp, err := client.Takeover(ctx, takeoverReq)
		conn.Close()

		if err != nil {
			log.Printf("Failed to send takeover message to neighbor %s: %v", neighbor.ID, err)
			continue
		}

		// Process the response
		if !resp.AcceptTakeover {
			// They have a smaller zone, we should cancel our takeover
			s.mu.Lock()
			info := s.FailureCoordination[failedNodeID]
			if info != nil {
				info.TimerCancelled = true
				if timer := s.takeoverTimers[failedNodeID]; timer != nil {
					timer.Stop()
					delete(s.takeoverTimers, failedNodeID)
				}
			}
			s.mu.Unlock()

			log.Printf("Cancelling takeover for node %s due to smaller neighbor (volume: %f)",
				failedNodeID, resp.ResponderZoneVolume)
			return
		}
	}
}

// ProcessTakeoverMessage handles incoming TAKEOVER messages from other nodes
func (s *CANServer) ProcessTakeoverMessage(ctx context.Context, failedNodeID node.NodeID, senderNodeID node.NodeID, senderZoneVolume float64, failedZone *node.Zone) (bool, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Calculate our zone volume
	ourVolume := calculateZoneVolume(s.Node.Zone)

	// If the sender's zone is smaller than ours, we should accept their takeover
	if senderZoneVolume < ourVolume {
		// Cancel our timer if we have one
		info := s.FailureCoordination[failedNodeID]
		if info != nil {
			info.TimerCancelled = true
			if timer := s.takeoverTimers[failedNodeID]; timer != nil {
				timer.Stop()
				delete(s.takeoverTimers, failedNodeID)
			}
		}

		log.Printf("Accepting takeover from %s for node %s (their volume: %f, our volume: %f)",
			senderNodeID, failedNodeID, senderZoneVolume, ourVolume)

		// Accept their takeover
		return true, ourVolume
	}

	// Our zone is smaller, so we reject their takeover
	log.Printf("Rejecting takeover from %s for node %s (their volume: %f, our volume: %f)",
		senderNodeID, failedNodeID, senderZoneVolume, ourVolume)

	return false, ourVolume
}

// Join joins an existing CAN network
func (s *CANServer) Join(ctx context.Context, entryNodeAddress string) error {
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

	// Find the node responsible for the random point
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

	// Get the address of the node responsible for the random point
	var responsibleNodeAddress string
	if findResp.IsResponsible {
		responsibleNodeAddress = entryNodeAddress
	} else {
		// Connect to the responsible node
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

	// Track new neighbors
	newNeighbors := make(map[node.NodeID]*node.NeighborInfo)

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

		// Add to neighbors map
		neighborInfo := &node.NeighborInfo{
			ID:      nodeID,
			Address: nbrInfo.Address,
			Zone:    zone,
		}

		s.Node.AddNeighbor(nodeID, nbrInfo.Address, zone)
		newNeighbors[nodeID] = neighborInfo
	}

	// Store data from the response
	for key, value := range joinResp.Data {
		// Store the key-value pair
		s.Node.Put(key, string(value))

		// Also store in persistent storage if available
		if s.Store != nil {
			if err := s.Store.Put(key, value); err != nil {
				log.Printf("Warning: failed to store key %s in persistent storage: %v", key, err)
			}
		}
	}
	s.mu.Unlock()

	// Notify all potential neighbors about our presence
	// This helps update routing tables and ensures proper network connectivity
	for _, neighborInfo := range newNeighbors {
		// Connect to the neighbor
		nbrClient, nbrConn, err := ConnectToNode(ctx, neighborInfo.Address)
		if err != nil {
			log.Printf("Warning: failed to connect to neighbor %s: %v", neighborInfo.ID, err)
			continue
		}

		// Send update neighbors request with our info
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
			log.Printf("Warning: failed to update neighbor %s: %v", neighborInfo.ID, err)
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

// Put stores a key-value pair
func (s *CANServer) Put(ctx context.Context, key string, value []byte) error {
	// Record request for load balancing
	s.RecordRequest(key, true)

	// Hash the key to find the responsible node
	point := s.Router.HashToPoint(key)

	// Check if the local node is responsible for this point
	if s.Node.Zone.Contains(point) {
		// Encrypt the value if encryption is enabled
		var dataToStore []byte

		if s.Config.EnableEncryption && s.KeyManager != nil {
			secureData, encErr := s.KeyManager.EncryptAndAuthenticate(value)
			if encErr != nil {
				return fmt.Errorf("failed to encrypt value: %w", encErr)
			}

			// Serialize the secure data
			serialized := crypto.SerializeSecureData(secureData)
			dataToStore = []byte(serialized)
		} else {
			dataToStore = value
		}

		// Store the value
		if err := s.Store.Put(key, dataToStore); err != nil {
			return fmt.Errorf("failed to store value: %w", err)
		}

		// Replicate to neighbors if replication is enabled
		if s.Config.ReplicationFactor > 1 {
			if err := s.ReplicateData(ctx, key, dataToStore); err != nil {
				log.Printf("Warning: replication failed for key %s: %v", key, err)
				// Continue even if replication fails
			}
		}

		return nil
	}

	// If not responsible, find the next hop
	nextHop, isResponsible := s.Router.FindResponsibleNode(s.Node, key)
	if isResponsible {
		// This should not happen, as we already checked if the local node is responsible
		return fmt.Errorf("internal error: router says local node is responsible but zone check failed")
	}

	if nextHop == nil {
		return fmt.Errorf("no route to responsible node")
	}

	// Forward the request to the next hop
	client, conn, err := ConnectToNode(ctx, nextHop.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to next hop: %w", err)
	}
	defer conn.Close()

	// Create the request
	req := &pb.PutRequest{
		Key:     key,
		Value:   value,
		Forward: false, // Not a forwarded request yet
	}

	// Send the request
	resp, err := client.Put(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to forward PUT request: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("PUT request was unsuccessful")
	}

	return nil
}

// Get retrieves a value by key
func (s *CANServer) Get(ctx context.Context, key string) ([]byte, error) {
	// Record request for load balancing
	s.RecordRequest(key, false)

	// Hash the key to find the responsible node
	point := s.Router.HashToPoint(key)

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

			return plaintext, nil
		}

		return value, nil
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

	return resp.Value, nil
}

// Delete removes a key-value pair
func (s *CANServer) Delete(ctx context.Context, key string) error {
	// Record request for load balancing
	s.RecordRequest(key, true)

	// Hash the key to find the responsible node
	point := s.Router.HashToPoint(key)

	// Check if the local node is responsible for this point
	if s.Node.Zone.Contains(point) {
		// Delete the value
		if err := s.Store.Delete(key); err != nil {
			return fmt.Errorf("failed to delete value: %w", err)
		}

		// Delete from replicas if replication is enabled
		if s.Config.ReplicationFactor > 1 {
			if err := s.UpdatedDeleteWithReplication(ctx, key); err != nil {
				log.Printf("Warning: delete replication failed for key %s: %v", key, err)
				// Continue even if replication fails
			}
		}

		return nil
	}

	// If not responsible, find the next hop
	nextHop, isResponsible := s.Router.FindResponsibleNode(s.Node, key)
	if isResponsible {
		// This should not happen, as we already checked if the local node is responsible
		return fmt.Errorf("internal error: router says local node is responsible but zone check failed")
	}

	if nextHop == nil {
		return fmt.Errorf("no route to responsible node")
	}

	// Forward the request to the next hop
	client, conn, err := ConnectToNode(ctx, nextHop.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to next hop: %w", err)
	}
	defer conn.Close()

	// Create the request
	req := &pb.DeleteRequest{
		Key:     key,
		Forward: false, // Not a forwarded request yet
	}

	// Send the request
	resp, err := client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to forward DELETE request: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("DELETE request was unsuccessful")
	}

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
