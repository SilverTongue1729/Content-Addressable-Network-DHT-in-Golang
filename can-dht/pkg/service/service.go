package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/routing"
	"github.com/can-dht/pkg/storage"
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

	mu sync.RWMutex
}

// CANConfig contains server configuration
type CANConfig struct {
	// Dimensions is the number of dimensions in the coordinate space
	Dimensions int

	// HeartbeatInterval is the frequency of heartbeat messages
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is how long to wait before considering a node dead
	HeartbeatTimeout time.Duration

	// EnableEncryption enables data encryption
	EnableEncryption bool

	// DataDir is the directory where data is stored
	DataDir string
}

// DefaultCANConfig returns the default configuration
func DefaultCANConfig() *CANConfig {
	return &CANConfig{
		Dimensions:        2,
		HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout:  15 * time.Second,
		EnableEncryption:  true,
		DataDir:           "data",
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

	return &CANServer{
		Node:       localNode,
		Router:     router,
		Store:      store,
		KeyManager: keyManager,
		Config:     config,
	}, nil
}

// Start starts the CAN server
func (s *CANServer) Start() {
	// Start the heartbeat process
	go s.startHeartbeatProcess()
}

// Stop stops the CAN server
func (s *CANServer) Stop() error {
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
	// For now, this is a placeholder
	// In a real implementation, this would use gRPC to send heartbeats
	// to all neighbors
}

// checkDeadNodes checks for neighbors that haven't sent a heartbeat recently
func (s *CANServer) checkDeadNodes() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	deadNodes := make([]node.NodeID, 0)

	for id, lastHeartbeat := range s.Node.Heartbeats {
		if now.Sub(lastHeartbeat) > s.Config.HeartbeatTimeout {
			deadNodes = append(deadNodes, id)
		}
	}

	// Handle dead nodes (remove them from neighbors, reassign data, etc.)
	for _, id := range deadNodes {
		s.Node.RemoveNeighbor(id)
		// In a real implementation, we would also need to:
		// 1. Take over the dead node's zone if we're the closest neighbor
		// 2. Update our neighbors
		// 3. Reassign data that was owned by the dead node
	}
}

// Join joins an existing CAN network
func (s *CANServer) Join(ctx context.Context, entryNodeAddress string) error {
	// This is a placeholder for joining logic
	// In a real implementation, this would:
	// 1. Find a random point in the coordinate space
	// 2. Route to the node responsible for that point
	// 3. Ask that node to split its zone and give half to the new node
	// 4. Update neighbors
	// 5. Receive relevant data from the split node
	return nil
}

// Leave leaves the CAN network gracefully
func (s *CANServer) Leave(ctx context.Context) error {
	// This is a placeholder for leaving logic
	// In a real implementation, this would:
	// 1. Transfer its zone and data to a neighbor or neighbors
	// 2. Notify neighbors of its departure
	return nil
}

// Put stores a key-value pair
func (s *CANServer) Put(ctx context.Context, key string, value []byte) error {
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

	// In a real implementation, this would forward the put request to the next hop
	// For now, we just return an error
	return status.Errorf(codes.Unimplemented, "forwarding not implemented yet")
}

// Get retrieves a value by key
func (s *CANServer) Get(ctx context.Context, key string) ([]byte, error) {
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

	// In a real implementation, this would forward the get request to the next hop
	// For now, we just return an error
	return nil, status.Errorf(codes.Unimplemented, "forwarding not implemented yet")
}

// Delete removes a key-value pair
func (s *CANServer) Delete(ctx context.Context, key string) error {
	// Hash the key to find the responsible node
	point := s.Router.HashToPoint(key)

	// Check if the local node is responsible for this point
	if s.Node.Zone.Contains(point) {
		// Delete the value
		if err := s.Store.Delete(key); err != nil {
			return fmt.Errorf("failed to delete value: %w", err)
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

	// In a real implementation, this would forward the delete request to the next hop
	// For now, we just return an error
	return status.Errorf(codes.Unimplemented, "forwarding not implemented yet")
}
