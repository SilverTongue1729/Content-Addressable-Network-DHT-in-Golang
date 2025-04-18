package service

import (
	"context"
	"fmt"

	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCServer implements the gRPC server for the CAN service
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
func ConnectToNode(ctx context.Context, address string) (pb.CANServiceClient, *grpc.ClientConn, error) {
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
	s.canServer.mu.Lock()
	defer s.canServer.mu.Unlock()

	// Check if join point is in this node's zone
	joinPoint := node.Point(req.JoinPoint.Coordinates)
	if !s.canServer.Node.Zone.Contains(joinPoint) {
		return &pb.JoinResponse{
			Success: false,
		}, fmt.Errorf("join point is not in this node's zone")
	}

	// Split the zone
	newZone, err := s.canServer.Node.Split()
	if err != nil {
		return &pb.JoinResponse{
			Success: false,
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

// Leave handles leave requests
func (s *GRPCServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	nodeID := node.NodeID(req.NodeId)

	// Remove the node from neighbors
	s.canServer.Node.RemoveNeighbor(nodeID)

	// Take over the data from the leaving node
	for key, value := range req.Data {
		// Store the data
		s.canServer.Put(ctx, key, value)
	}

	return &pb.LeaveResponse{
		Success: true,
	}, nil
}

// Heartbeat handles heartbeat messages
func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	nodeID := node.NodeID(req.NodeId)

	// Update the last heartbeat time for this node
	s.canServer.Node.UpdateHeartbeat(nodeID)

	return &pb.HeartbeatResponse{
		Success:   true,
		Timestamp: req.Timestamp,
	}, nil
}

// Put handles put requests
func (s *GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Forward {
		// This is a forwarded request, so we should handle it locally
		err := s.canServer.Put(ctx, req.Key, req.Value)
		return &pb.PutResponse{
			Success: err == nil,
		}, err
	}

	// Hash the key to find the responsible node
	point := s.canServer.Router.HashToPoint(req.Key)

	// Check if the local node is responsible for this point
	if s.canServer.Node.Zone.Contains(point) {
		// Handle locally
		err := s.canServer.Put(ctx, req.Key, req.Value)
		return &pb.PutResponse{
			Success: err == nil,
		}, err
	}

	// If not responsible, find the next hop and forward
	nextHop, isResponsible := s.canServer.Router.FindResponsibleNode(s.canServer.Node, req.Key)
	if isResponsible {
		// This should not happen
		return &pb.PutResponse{
			Success: false,
		}, fmt.Errorf("internal error: router says local node is responsible but zone check failed")
	}

	if nextHop == nil {
		return &pb.PutResponse{
			Success: false,
		}, fmt.Errorf("no route to responsible node")
	}

	// Forward to the next hop
	client, conn, err := ConnectToNode(ctx, nextHop.Address)
	if err != nil {
		return &pb.PutResponse{
			Success: false,
		}, err
	}
	defer conn.Close()

	// Set forward to true to avoid infinite forwarding
	req.Forward = true
	return client.Put(ctx, req)
}

// Get handles get requests
func (s *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Forward {
		// This is a forwarded request, so we should handle it locally
		value, err := s.canServer.Get(ctx, req.Key)
		if err != nil {
			return &pb.GetResponse{
				Success: false,
				Exists:  false,
			}, err
		}

		return &pb.GetResponse{
			Success: true,
			Value:   value,
			Exists:  true,
		}, nil
	}

	// Hash the key to find the responsible node
	point := s.canServer.Router.HashToPoint(req.Key)

	// Check if the local node is responsible for this point
	if s.canServer.Node.Zone.Contains(point) {
		// Handle locally
		value, err := s.canServer.Get(ctx, req.Key)
		if err != nil {
			return &pb.GetResponse{
				Success: false,
				Exists:  false,
			}, err
		}

		return &pb.GetResponse{
			Success: true,
			Value:   value,
			Exists:  true,
		}, nil
	}

	// If not responsible, find the next hop and forward
	nextHop, isResponsible := s.canServer.Router.FindResponsibleNode(s.canServer.Node, req.Key)
	if isResponsible {
		// This should not happen
		return &pb.GetResponse{
			Success: false,
			Exists:  false,
		}, fmt.Errorf("internal error: router says local node is responsible but zone check failed")
	}

	if nextHop == nil {
		return &pb.GetResponse{
			Success: false,
			Exists:  false,
		}, fmt.Errorf("no route to responsible node")
	}

	// Forward to the next hop
	client, conn, err := ConnectToNode(ctx, nextHop.Address)
	if err != nil {
		return &pb.GetResponse{
			Success: false,
			Exists:  false,
		}, err
	}
	defer conn.Close()

	// Set forward to true to avoid infinite forwarding
	req.Forward = true
	return client.Get(ctx, req)
}

// Delete handles delete requests
func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Forward {
		// This is a forwarded request, so we should handle it locally
		err := s.canServer.Delete(ctx, req.Key)
		return &pb.DeleteResponse{
			Success: err == nil,
			Existed: err == nil, // Simplified: assume success means it existed
		}, err
	}

	// Hash the key to find the responsible node
	point := s.canServer.Router.HashToPoint(req.Key)

	// Check if the local node is responsible for this point
	if s.canServer.Node.Zone.Contains(point) {
		// Handle locally
		err := s.canServer.Delete(ctx, req.Key)
		return &pb.DeleteResponse{
			Success: err == nil,
			Existed: err == nil, // Simplified: assume success means it existed
		}, err
	}

	// If not responsible, find the next hop and forward
	nextHop, isResponsible := s.canServer.Router.FindResponsibleNode(s.canServer.Node, req.Key)
	if isResponsible {
		// This should not happen
		return &pb.DeleteResponse{
			Success: false,
			Existed: false,
		}, fmt.Errorf("internal error: router says local node is responsible but zone check failed")
	}

	if nextHop == nil {
		return &pb.DeleteResponse{
			Success: false,
			Existed: false,
		}, fmt.Errorf("no route to responsible node")
	}

	// Forward to the next hop
	client, conn, err := ConnectToNode(ctx, nextHop.Address)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Existed: false,
		}, err
	}
	defer conn.Close()

	// Set forward to true to avoid infinite forwarding
	req.Forward = true
	return client.Delete(ctx, req)
}

// FindNode handles find node requests
func (s *GRPCServer) FindNode(ctx context.Context, req *pb.FindNodeRequest) (*pb.FindNodeResponse, error) {
	var point node.Point

	switch x := req.Target.(type) {
	case *pb.FindNodeRequest_Key:
		// Hash the key to a point
		point = s.canServer.Router.HashToPoint(x.Key)
	case *pb.FindNodeRequest_Point:
		// Use the provided point
		point = node.Point(x.Point.Coordinates)
	default:
		return nil, fmt.Errorf("invalid target type")
	}

	// Check if the local node is responsible for this point
	if s.canServer.Node.Zone.Contains(point) {
		return &pb.FindNodeResponse{
			IsResponsible: true,
		}, nil
	}

	// Find the closest neighbor
	nextHop, _ := s.canServer.Router.RouteToPoint(s.canServer.Node, point)
	if nextHop == nil {
		return nil, fmt.Errorf("no route to responsible node")
	}

	return &pb.FindNodeResponse{
		IsResponsible:   false,
		ResponsibleNode: convertNodeInfoToProto(nextHop),
	}, nil
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

// Takeover handles takeover messages for coordinating failure recovery
func (s *GRPCServer) Takeover(ctx context.Context, req *pb.TakeoverRequest) (*pb.TakeoverResponse, error) {
	failedNodeID := node.NodeID(req.FailedNodeId)
	senderNodeID := node.NodeID(req.SenderNodeId)
	senderZoneVolume := req.ZoneVolume

	// Get failed node's zone
	var failedZone *node.Zone
	if req.FailedZone != nil {
		var err error
		failedZone, err = node.NewZone(
			node.Point(req.FailedZone.MinPoint.Coordinates),
			node.Point(req.FailedZone.MaxPoint.Coordinates),
		)
		if err != nil {
			return nil, fmt.Errorf("invalid zone in takeover request: %w", err)
		}
	}

	// Process the takeover message in the CAN server
	acceptTakeover, ourVolume := s.canServer.ProcessTakeoverMessage(ctx, failedNodeID, senderNodeID, senderZoneVolume, failedZone)

	return &pb.TakeoverResponse{
		AcceptTakeover:      acceptTakeover,
		ResponderZoneVolume: ourVolume,
	}, nil
}
