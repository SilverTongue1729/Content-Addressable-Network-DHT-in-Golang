package service

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/can-dht/pkg/crypto"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/can-dht/pkg/security"
)

// GRPCServer implements the CAN service gRPC interface
type GRPCServer struct {
	pb.UnimplementedCANServiceServer
	canServer *CANServer
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(canServer *CANServer) *GRPCServer {
	return &GRPCServer{
		canServer: canServer,
	}
}

// RegisterWithGRPCServer registers the CAN service with a gRPC server
func (s *GRPCServer) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	pb.RegisterCANServiceServer(grpcServer, s)
}

// ConnectToNode connects to a remote node via gRPC
func (s *CANServer) ConnectToNode(ctx context.Context, address string) (pb.CANServiceClient, *grpc.ClientConn, error) {
	var dialOpts []grpc.DialOption

	if s.Config.EnableMTLS {
		// Set up TLS configuration for mTLS
		tlsConfig := crypto.TLSConfig{
			CertFile:   s.Config.TLSCertFile,
			KeyFile:    s.Config.TLSKeyFile,
			CAFile:     s.Config.TLSCAFile,
			ServerName: s.Config.TLSServerName,
		}
		
		creds, err := crypto.LoadClientTLSCredentials(tlsConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(creds)))
	} else {
		// Use insecure credentials if mTLS is not enabled
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	
	// Add block option to wait for connection
	dialOpts = append(dialOpts, grpc.WithBlock())
	
	conn, err := grpc.DialContext(
		ctx,
		address,
		dialOpts...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to node at %s: %w", address, err)
	}

	client := pb.NewCANServiceClient(conn)
	return client, conn, nil
}

// Helper function for backward compatibility with existing code
func ConnectToNode(ctx context.Context, address string) (pb.CANServiceClient, *grpc.ClientConn, error) {
	// Use insecure connection for backward compatibility
	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to node at %s: %w", address, err)
	}

	client := pb.NewCANServiceClient(conn)
	return client, conn, nil
}

// convertNodeInfoToProto converts a node.NeighborInfo to a protobuf NodeInfo
func convertNodeInfoToProto(info *node.NeighborInfo) *pb.NodeInfo {
	return &pb.NodeInfo{
		Id:      string(info.ID),
		Address: info.Address,
		Zone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: info.Zone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: info.Zone.MaxPoint,
			},
		},
	}
}

// convertProtoToNodeInfo converts a protobuf NodeInfo to a node.NeighborInfo
func convertProtoToNodeInfo(info *pb.NodeInfo) *node.NeighborInfo {
	zone, _ := node.NewZone(
		node.Point(info.Zone.MinPoint.Coordinates),
		node.Point(info.Zone.MaxPoint.Coordinates),
	)

	return &node.NeighborInfo{
		ID:      node.NodeID(info.Id),
		Address: info.Address,
		Zone:    zone,
	}
}

// Join handles join requests from other nodes
func (s *GRPCServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleJoinRequest(ctx); err != nil {
			return &pb.JoinResponse{
				Success: false,
				Error:   err.Error(),
			}, fmt.Errorf("cannot handle join request: %w", err)
		}
	}

	s.canServer.mu.Lock()
	defer s.canServer.mu.Unlock()

	// Check if join point is in this node's zone
	joinPoint := node.Point(req.JoinPoint.Coordinates)
	if !s.canServer.Node.Zone.Contains(joinPoint) {
		return &pb.JoinResponse{
			Success: false,
			Error:   "join point is not in this node's zone",
		}, fmt.Errorf("join point is not in this node's zone")
	}

	// Split the zone
	newZone, err := s.canServer.Node.Split()
	if err != nil {
		return &pb.JoinResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to split zone: %v", err),
		}, fmt.Errorf("failed to split zone: %w", err)
	}

	// Collect data for the new node
	dataToTransfer := make(map[string][]byte)
	for key, value := range s.canServer.Node.Data {
		// Hash the key and check if it belongs to the new zone
		keyPoint := s.canServer.Router.HashToPoint(key)
		if newZone.Contains(keyPoint) {
			dataToTransfer[key] = []byte(value)
			delete(s.canServer.Node.Data, key) // Remove from this node
		}
	}

	// Collect neighbors for the new node
	neighbors := make([]*pb.NodeInfo, 0)
	for _, neighborInfo := range s.canServer.Node.GetNeighbors() {
		protoInfo := convertNodeInfoToProto(neighborInfo)
		neighbors = append(neighbors, protoInfo)
	}

	// Add the new node as a neighbor
	s.canServer.Node.AddNeighbor(
		node.NodeID(req.NewNodeId),
		req.NewNodeAddress,
		newZone,
	)

	// Add this node to the neighbors list for the new node
	selfInfo := &pb.NodeInfo{
		Id:      string(s.canServer.Node.ID),
		Address: s.canServer.Node.Address,
		Zone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: s.canServer.Node.Zone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: s.canServer.Node.Zone.MaxPoint,
			},
		},
	}
	neighbors = append(neighbors, selfInfo)

	return &pb.JoinResponse{
		Success: true,
		AssignedZone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: newZone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: newZone.MaxPoint,
			},
		},
		Neighbors: neighbors,
		Data:      dataToTransfer,
	}, nil
}

// Leave handles leave notifications from other nodes
func (s *GRPCServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleLeaveRequest(ctx); err != nil {
			return &pb.LeaveResponse{
				Success: false,
				Error:   err.Error(),
			}, fmt.Errorf("cannot handle leave request: %w", err)
		}
	}

	log.Printf("Received leave notification from node %s", req.NodeId)

	// Remove the node from neighbors
	s.canServer.Node.RemoveNeighbor(node.NodeID(req.NodeId))

	// Check if this node should take over the leaving node's zone
	if s.canServer.shouldTakeOverZone(node.NodeID(req.NodeId), req.Zone) {
		log.Printf("Taking over zone from leaving node %s", req.NodeId)
		
		// Convert zone from proto
		zone := &node.Zone{
			MinPoint: req.Zone.MinPoint.Coordinates,
			MaxPoint: req.Zone.MaxPoint.Coordinates,
		}
		
		// Initiate takeover process
		go s.canServer.TakeOverZone(node.NodeID(req.NodeId), zone, req.Data)
	}

	return &pb.LeaveResponse{
		Success: true,
	}, nil
}

// Heartbeat handles heartbeat messages from other nodes
func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleCoordinationRequest(ctx); err != nil {
			return &pb.HeartbeatResponse{
				Success: false,
				Error:   err.Error(),
			}, fmt.Errorf("cannot handle heartbeat request: %w", err)
		}
	}

	nodeID := node.NodeID(req.NodeId)
	s.canServer.mu.Lock()
	defer s.canServer.mu.Unlock()

	// Update last seen time for the node
	s.canServer.updateLastSeen(nodeID)

	// Return information about this node
	return &pb.HeartbeatResponse{
		Success: true,
		NodeId:  string(s.canServer.Node.ID),
		Load:    float32(s.canServer.getCurrentLoad()),
		ZoneVolume: float32(s.canServer.Node.Zone.Volume()),
	}, nil
}

// extractAPIKey extracts the API key from the context
func extractAPIKey(ctx context.Context) (string, error) {
	// Check if metadata exists
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no metadata in context")
	}

	// Get API key from metadata
	apiKeys := md.Get("x-api-key")
	if len(apiKeys) == 0 {
		return "", fmt.Errorf("no API key provided")
	}

	// Return the first API key
	return apiKeys[0], nil
}

// checkAccessControl checks if the request has the required permissions
func (s *GRPCServer) checkAccessControl(ctx context.Context, permission security.Permission) error {
	if !s.canServer.EnableAccessControl {
		return nil
	}
	
	apiKey, err := extractAPIKey(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, "API key is required")
	}
	
	if !s.canServer.AccessController.CheckPermission(apiKey, permission) {
		return status.Error(codes.PermissionDenied, "Access denied: insufficient permissions")
	}
	
	return nil
}

// Put handles put requests from clients
func (s *GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleWriteRequest(ctx); err != nil {
			// If this node can't handle write requests and request distribution is enabled, 
			// try to forward the request to a node that can
			if s.canServer.Config.EnableRequestRedistribution && s.canServer.requestDistributor != nil {
				log.Printf("Forwarding write request due to role restrictions: %v", err)
				return s.canServer.requestDistributor.DistributePut(ctx, req.Key, req.Value)
			}
			
			return &pb.PutResponse{
				Success: false,
				Message: err.Error(),
			}, fmt.Errorf("cannot handle write request: %w", err)
		}
	}

	// Extract API key from context if access control is enabled
	if s.canServer.Config.EnableAccessControl && s.canServer.apiKeysManager != nil {
		apiKey, err := extractAPIKey(ctx)
		if err != nil {
			return &pb.PutResponse{
				Success: false,
				Message: fmt.Sprintf("authentication error: %v", err),
			}, err
		}

		// Check if the API key has write permission
		if !s.canServer.apiKeysManager.HasPermission(apiKey, "write") {
			return &pb.PutResponse{
				Success: false,
				Message: "permission denied: write access required",
			}, fmt.Errorf("permission denied: write access required")
		}

		// Add the API key to the context for downstream authorization
		ctx = context.WithValue(ctx, "api_key", apiKey)
	}

	// If this is a forwarded request, handle it locally
	if !req.Forward {
		// Check if the key belongs to this node
		keyPoint := s.canServer.Router.HashToPoint(req.Key)
		if !s.canServer.Node.Zone.Contains(keyPoint) {
			// Forward the request to the right node
			return s.canServer.ForwardPut(ctx, req.Key, req.Value)
		}
	}

	// Put the value
	err := s.canServer.Put(ctx, req.Key, req.Value)
	if err != nil {
		return &pb.PutResponse{
			Success: false,
			Message: fmt.Sprintf("failed to put value: %v", err),
		}, err
	}

	return &pb.PutResponse{
		Success: true,
	}, nil
}

// Get handles get requests from clients
func (s *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleReadRequest(ctx); err != nil {
			// If this node can't handle read requests and request distribution is enabled, 
			// try to forward the request to a node that can
			if s.canServer.Config.EnableRequestRedistribution && s.canServer.requestDistributor != nil {
				log.Printf("Forwarding read request due to role restrictions: %v", err)
				return s.canServer.requestDistributor.DistributeGet(ctx, req.Key)
			}
			
			return &pb.GetResponse{
				Success: false,
				Message: err.Error(),
			}, fmt.Errorf("cannot handle read request: %w", err)
		}
	}

	// Extract API key from context if access control is enabled
	if s.canServer.Config.EnableAccessControl && s.canServer.apiKeysManager != nil {
		apiKey, err := extractAPIKey(ctx)
		if err != nil {
			return &pb.GetResponse{
				Success: false,
				Message: fmt.Sprintf("authentication error: %v", err),
			}, err
		}

		// Check if the API key has read permission
		if !s.canServer.apiKeysManager.HasPermission(apiKey, "read") {
			return &pb.GetResponse{
				Success: false,
				Message: "permission denied: read access required",
			}, fmt.Errorf("permission denied: read access required")
		}

		// Add the API key to the context for downstream authorization
		ctx = context.WithValue(ctx, "api_key", apiKey)
	}

	// If this is a forwarded request, handle it locally
	if !req.Forward {
		// Check if the key belongs to this node
		keyPoint := s.canServer.Router.HashToPoint(req.Key)
		if !s.canServer.Node.Zone.Contains(keyPoint) {
			// Forward the request to the right node
			return s.canServer.ForwardGet(ctx, req.Key)
		}
	}

	// Get the value
	value, err := s.canServer.Get(ctx, req.Key)
	if err != nil {
		// Check if it's a "not found" error
		if err.Error() == "key not found" {
			return &pb.GetResponse{
				Success: true,
				Exists:  false,
			}, nil
		}

		return &pb.GetResponse{
			Success: false,
			Message: fmt.Sprintf("failed to get value: %v", err),
		}, err
	}

	return &pb.GetResponse{
		Success: true,
		Exists:  true,
		Value:   value,
	}, nil
}

// Delete handles delete requests from clients
func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleDeleteRequest(ctx); err != nil {
			// If this node can't handle delete requests and request distribution is enabled, 
			// try to forward the request to a node that can
			if s.canServer.Config.EnableRequestRedistribution && s.canServer.requestDistributor != nil {
				log.Printf("Forwarding delete request due to role restrictions: %v", err)
				return s.canServer.requestDistributor.DistributeDelete(ctx, req.Key)
			}
			
			return &pb.DeleteResponse{
				Success: false,
				Message: err.Error(),
			}, fmt.Errorf("cannot handle delete request: %w", err)
		}
	}

	// Extract API key from context if access control is enabled
	if s.canServer.Config.EnableAccessControl && s.canServer.apiKeysManager != nil {
		apiKey, err := extractAPIKey(ctx)
		if err != nil {
			return &pb.DeleteResponse{
				Success: false,
				Message: fmt.Sprintf("authentication error: %v", err),
			}, err
		}

		// Check if the API key has write permission (write permission includes delete)
		if !s.canServer.apiKeysManager.HasPermission(apiKey, "write") {
			return &pb.DeleteResponse{
				Success: false,
				Message: "permission denied: write access required for delete operations",
			}, fmt.Errorf("permission denied: write access required for delete operations")
		}

		// Add the API key to the context for downstream authorization
		ctx = context.WithValue(ctx, "api_key", apiKey)
	}

	// If this is a forwarded request, handle it locally
	if !req.Forward {
		// Check if the key belongs to this node
		keyPoint := s.canServer.Router.HashToPoint(req.Key)
		if !s.canServer.Node.Zone.Contains(keyPoint) {
			// Forward the request to the right node
			return s.canServer.ForwardDelete(ctx, req.Key)
		}
	}

	// Delete the value
	existed, err := s.canServer.Delete(ctx, req.Key)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: fmt.Sprintf("failed to delete value: %v", err),
		}, err
	}

	return &pb.DeleteResponse{
		Success: true,
		Existed: existed,
	}, nil
}

// FindNode locates the node responsible for the given key
func (s *GRPCServer) FindNode(ctx context.Context, req *pb.FindNodeRequest) (*pb.FindNodeResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleIndexRequest(ctx); err != nil {
			return &pb.FindNodeResponse{
				Success: false,
				Error:   err.Error(),
			}, fmt.Errorf("cannot handle find node request: %w", err)
		}
	}
	
	key := req.Key
	keyPoint := s.canServer.Router.HashToPoint(key)

	// Check if this node owns the key
	if s.canServer.Node.Zone.Contains(keyPoint) {
		return &pb.FindNodeResponse{
			Success: true,
			NodeInfo: &pb.NodeInfo{
				Id:      string(s.canServer.Node.ID),
				Address: s.canServer.Node.Address,
				Zone: &pb.Zone{
					MinPoint: &pb.Point{
						Coordinates: s.canServer.Node.Zone.MinPoint,
					},
					MaxPoint: &pb.Point{
						Coordinates: s.canServer.Node.Zone.MaxPoint,
					},
				},
			},
		}, nil
	}

	// Find the closest neighbor to the key
	closestNeighbor, err := s.canServer.Router.FindClosestNeighbor(keyPoint)
	if err != nil {
		return &pb.FindNodeResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to find closest neighbor: %v", err),
		}, err
	}

	// If this is a recursive call and we've seen this node before, return an error
	visitedNodes := make(map[string]bool)
	for _, visited := range req.VisitedNodes {
		visitedNodes[visited] = true
	}

	if len(visitedNodes) > 0 && visitedNodes[string(closestNeighbor.ID)] {
		return &pb.FindNodeResponse{
			Success: false,
			Error:   "routing loop detected",
		}, fmt.Errorf("routing loop detected")
	}

	// If we're not supposed to forward, return the closest neighbor
	if !req.Forward {
		return &pb.FindNodeResponse{
			Success: true,
			NodeInfo: &pb.NodeInfo{
				Id:      string(closestNeighbor.ID),
				Address: closestNeighbor.Address,
				Zone: &pb.Zone{
					MinPoint: &pb.Point{
						Coordinates: closestNeighbor.Zone.MinPoint,
					},
					MaxPoint: &pb.Point{
						Coordinates: closestNeighbor.Zone.MaxPoint,
					},
				},
			},
		}, nil
	}

	// Forward the request to the closest neighbor
	// Add this node to the visited list
	updatedVisitedNodes := append(req.VisitedNodes, string(s.canServer.Node.ID))

	// Create a client connection to the closest neighbor
	conn, err := s.canServer.ConnectToNode(ctx, closestNeighbor.Address)
	if err != nil {
		return &pb.FindNodeResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to connect to closest neighbor: %v", err),
		}, err
	}
	defer conn.Close()

	// Create a client
	client := pb.NewCANServiceClient(conn)

	// Forward the request
	forwardReq := &pb.FindNodeRequest{
		Key:          key,
		Forward:      true,
		VisitedNodes: updatedVisitedNodes,
	}

	// Call the FindNode method on the neighbor
	resp, err := client.FindNode(ctx, forwardReq)
	if err != nil {
		return &pb.FindNodeResponse{
			Success: false,
			Error:   fmt.Sprintf("forwarded find node request failed: %v", err),
		}, err
	}

	return resp, nil
}

// UpdateNeighbors handles neighbor update requests
func (s *GRPCServer) UpdateNeighbors(ctx context.Context, req *pb.UpdateNeighborsRequest) (*pb.UpdateNeighborsResponse, error) {
	// Update the neighbor list
	for _, protoInfo := range req.Neighbors {
		neighborInfo := convertProtoToNodeInfo(protoInfo)
		s.canServer.Node.AddNeighbor(neighborInfo.ID, neighborInfo.Address, neighborInfo.Zone)
	}

	return &pb.UpdateNeighborsResponse{
		Success: true,
	}, nil
}

// GetNeighbors returns the list of neighbors for a node
func (s *GRPCServer) GetNeighbors(ctx context.Context, req *pb.GetNeighborsRequest) (*pb.GetNeighborsResponse, error) {
	// Get our neighbors
	neighbors := s.canServer.Node.GetNeighbors()
	
	// Convert to neighbor entries
	entries := make([]*pb.NeighborEntry, 0, len(neighbors))
	for id, info := range neighbors {
		// Skip the requesting node
		if node.NodeID(req.RequestingNodeId) == id {
			continue
		}
		
		// Convert zone coordinates
		minCoords := make([]float64, len(info.Zone.MinPoint))
		maxCoords := make([]float64, len(info.Zone.MaxPoint))
		
		for i := 0; i < len(minCoords); i++ {
			minCoords[i] = info.Zone.MinPoint[i]
			maxCoords[i] = info.Zone.MaxPoint[i]
		}
		
		entries = append(entries, &pb.NeighborEntry{
			NodeId:         string(id),
			Address:        info.Address,
			MinCoordinates: minCoords,
			MaxCoordinates: maxCoords,
		})
	}
	
	return &pb.GetNeighborsResponse{
		Neighbors: entries,
	}, nil
}

// ReplicatePut replicates a put operation to this node
func (s *GRPCServer) ReplicatePut(ctx context.Context, req *pb.ReplicatePutRequest) (*pb.ReplicatePutResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleReplicationRequest(ctx); err != nil {
			return &pb.ReplicatePutResponse{
				Success: false,
				Message: err.Error(),
			}, fmt.Errorf("cannot handle replication request: %w", err)
		}
	}

	log.Printf("Received replicate put for key %s (hop %d/%d from %s)",
		req.Key, req.ReplicationHop, req.MaxReplicationHops, req.OriginNodeId)

	// Store the replicated data
	s.canServer.mu.Lock()
	defer s.canServer.mu.Unlock()

	// Get the replica store for the origin node
	nodeID := req.OriginNodeId
	if s.canServer.replicaStore[nodeID] == nil {
		s.canServer.replicaStore[nodeID] = make(map[string][]byte)
	}

	// Store the key-value pair in the replica store
	s.canServer.replicaStore[nodeID][req.Key] = req.Value

	// If we haven't reached max hops, continue replication
	if req.ReplicationHop < req.MaxReplicationHops {
		// Forward to neighbors (except those in the path)
		nextHop := req.ReplicationHop + 1
		visited := make(map[string]bool)
		for _, nodeID := range req.Path {
			visited[nodeID] = true
		}

		// Add this node to the path
		updatedPath := append(req.Path, string(s.canServer.Node.ID))

		// Forward to neighbors not in the path
		for _, neighbor := range s.canServer.Node.GetNeighbors() {
			if !visited[string(neighbor.ID)] {
				// Create a client connection to the neighbor
				conn, err := s.canServer.ConnectToNode(ctx, neighbor.Address)
				if err != nil {
					log.Printf("Failed to connect to neighbor %s for replication: %v", neighbor.ID, err)
					continue
				}
				defer conn.Close()

				// Create a client
				client := pb.NewCANServiceClient(conn)

				// Forward the replication request
				forwardReq := &pb.ReplicatePutRequest{
					Key:               req.Key,
					Value:             req.Value,
					ReplicationHop:    nextHop,
					MaxReplicationHops: req.MaxReplicationHops,
					Path:              updatedPath,
					OriginNodeId:      req.OriginNodeId,
				}

				// Call the ReplicatePut method on the neighbor
				_, err = client.ReplicatePut(ctx, forwardReq)
				if err != nil {
					log.Printf("Failed to forward replication to neighbor %s: %v", neighbor.ID, err)
				}
			}
		}
	}

	return &pb.ReplicatePutResponse{
		Success: true,
	}, nil
}

// ReplicateDelete replicates a delete operation to this node
func (s *GRPCServer) ReplicateDelete(ctx context.Context, req *pb.ReplicateDeleteRequest) (*pb.ReplicateDeleteResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleReplicationRequest(ctx); err != nil {
			return &pb.ReplicateDeleteResponse{
				Success: false,
				Message: err.Error(),
			}, fmt.Errorf("cannot handle replication request: %w", err)
		}
	}

	log.Printf("Received replicate delete for key %s (hop %d/%d from %s)",
		req.Key, req.ReplicationHop, req.MaxReplicationHops, req.OriginNodeId)

	// Remove the data from the replica store
	s.canServer.replicaMu.Lock()
	defer s.canServer.replicaMu.Unlock()

	// Get the replica store for the origin node
	nodeID := req.OriginNodeId
	if s.canServer.replicaStore[nodeID] != nil {
		// Delete the key from the replica store
		delete(s.canServer.replicaStore[nodeID], req.Key)
	}

	// If we haven't reached max hops, continue replication
	if req.ReplicationHop < req.MaxReplicationHops {
		// Forward to neighbors (except those in the path)
		nextHop := req.ReplicationHop + 1
		visited := make(map[string]bool)
		for _, nodeID := range req.Path {
			visited[nodeID] = true
		}

		// Add this node to the path
		updatedPath := append(req.Path, string(s.canServer.Node.ID))

		// Forward to neighbors not in the path
		for _, neighbor := range s.canServer.Node.GetNeighbors() {
			if !visited[string(neighbor.ID)] {
				// Create a client connection to the neighbor
				conn, err := s.canServer.ConnectToNode(ctx, neighbor.Address)
				if err != nil {
					log.Printf("Failed to connect to neighbor %s for replication: %v", neighbor.ID, err)
					continue
				}
				defer conn.Close()

				// Create a client
				client := pb.NewCANServiceClient(conn)

				// Forward the replication request
				forwardReq := &pb.ReplicateDeleteRequest{
					Key:               req.Key,
					ReplicationHop:    nextHop,
					MaxReplicationHops: req.MaxReplicationHops,
					Path:              updatedPath,
					OriginNodeId:      req.OriginNodeId,
				}

				// Call the ReplicateDelete method on the neighbor
				_, err = client.ReplicateDelete(ctx, forwardReq)
				if err != nil {
					log.Printf("Failed to forward replication to neighbor %s: %v", neighbor.ID, err)
				}
			}
		}
	}

	return &pb.ReplicateDeleteResponse{
		Success: true,
	}, nil
}

// SyncReplicaMetadata synchronizes replica metadata between nodes
func (s *GRPCServer) SyncReplicaMetadata(ctx context.Context, req *pb.SyncReplicaMetadataRequest) (*pb.SyncReplicaMetadataResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleReplicationRequest(ctx); err != nil {
			return &pb.SyncReplicaMetadataResponse{
				Success: false,
				Message: err.Error(),
			}, fmt.Errorf("cannot handle replication request: %w", err)
		}
	}

	log.Printf("Received replica metadata sync from node %s with %d entries", 
		req.NodeId, len(req.Entries))

	// Process the metadata entries
	s.canServer.replicaMu.Lock()
	defer s.canServer.replicaMu.Unlock()

	// Update replica metadata based on the received entries
	for _, entry := range req.Entries {
		// Store metadata about where replicas are located
		if s.canServer.replicaTracker != nil {
			s.canServer.replicaTracker.UpdateReplicaInfo(entry.Key, entry.ReplicaNodes, entry.Version)
		}
	}

	return &pb.SyncReplicaMetadataResponse{
		Success: true,
	}, nil
}

// ProposeZoneAdjustment handles zone adjustment proposals
func (s *GRPCServer) ProposeZoneAdjustment(ctx context.Context, req *pb.ZoneAdjustmentProposalRequest) (*pb.ZoneAdjustmentProposalResponse, error) {
	return s.canServer.HandleZoneAdjustmentProposal(req)
}

// TransferKey handles key transfers during zone adjustments
func (s *GRPCServer) TransferKey(ctx context.Context, req *pb.TransferKeyRequest) (*pb.TransferKeyResponse, error) {
	// Store the transferred key locally
	err := s.canServer.Store.Put(req.Key, req.Value)
	if err != nil {
		return &pb.TransferKeyResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	
	log.Printf("Received transferred key: %s during zone adjustment", req.Key)
	
	// If we have replication enabled, we should replicate this key
	if s.canServer.Config.ReplicationFactor > 1 {
		// Start replication in the background
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			
			if err := s.canServer.ReplicateData(ctx, req.Key, req.Value); err != nil {
				log.Printf("Warning: Failed to replicate transferred key %s: %v", req.Key, err)
			}
		}()
	}
	
	return &pb.TransferKeyResponse{
		Success: true,
	}, nil
}

// AddNeighbor adds a new neighbor to this node's neighbor list
func (s *GRPCServer) AddNeighbor(ctx context.Context, req *pb.AddNeighborRequest) (*pb.AddNeighborResponse, error) {
	// Check role capabilities
	if s.canServer.RoleManager != nil {
		if err := s.canServer.RoleManager.CanHandleJoinRequest(ctx); err != nil {
			return &pb.AddNeighborResponse{
				Success: false,
				Error:   err.Error(),
			}, fmt.Errorf("cannot handle add neighbor request: %w", err)
		}
	}

	log.Printf("Adding node %s as neighbor", req.Neighbor.Id)

	// Create zone from proto
	zone := &node.Zone{
		MinPoint: req.Neighbor.Zone.MinPoint.Coordinates,
		MaxPoint: req.Neighbor.Zone.MaxPoint.Coordinates,
	}

	// Add the neighbor
	s.canServer.Node.AddNeighbor(
		node.NodeID(req.Neighbor.Id),
		req.Neighbor.Address,
		zone,
	)

	return &pb.AddNeighborResponse{
		Success: true,
	}, nil
}
