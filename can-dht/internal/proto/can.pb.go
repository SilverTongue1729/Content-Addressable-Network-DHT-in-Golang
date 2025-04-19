// Package proto contains the protocol buffer definitions for the CAN-DHT
package proto

// This is a minimal placeholder implementation
// Normally this would be generated from the .proto file

// Point represents a d-dimensional point in the coordinate space
type Point struct {
	Coordinates []float64 `protobuf:"packed,1,rep,name=coordinates,proto3" json:"coordinates,omitempty"`
}

// Zone represents a region in the coordinate space
type Zone struct {
	MinPoint *Point `protobuf:"bytes,1,opt,name=min_point,json=minPoint,proto3" json:"min_point,omitempty"`
	MaxPoint *Point `protobuf:"bytes,2,opt,name=max_point,json=maxPoint,proto3" json:"max_point,omitempty"`
}

// NodeInfo represents information about a node
type NodeInfo struct {
	Id      string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Address string `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	Zone    *Zone  `protobuf:"bytes,3,opt,name=zone,proto3" json:"zone,omitempty"`
}

// Basic request/response types
type PutRequest struct {
	Key     string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value   []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Forward bool   `protobuf:"varint,3,opt,name=forward,proto3" json:"forward,omitempty"`
	IsHotKey bool   `protobuf:"varint,4,opt,name=is_hot_key,json=isHotKey,proto3" json:"is_hot_key,omitempty"`
	HotKeyExpiry int64 `protobuf:"varint,5,opt,name=hot_key_expiry,json=hotKeyExpiry,proto3" json:"hot_key_expiry,omitempty"`
}

type PutResponse struct {
	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
}

type GetRequest struct {
	Key     string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Forward bool   `protobuf:"varint,2,opt,name=forward,proto3" json:"forward,omitempty"`
}

type GetResponse struct {
	Success bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Value   []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Exists  bool   `protobuf:"varint,3,opt,name=exists,proto3" json:"exists,omitempty"`
}

type DeleteRequest struct {
	Key     string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Forward bool   `protobuf:"varint,2,opt,name=forward,proto3" json:"forward,omitempty"`
}

type DeleteResponse struct {
	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Existed bool `protobuf:"varint,2,opt,name=existed,proto3" json:"existed,omitempty"`
}

// Load balancing request/response types
type ZoneAdjustmentProposalRequest struct {
	ProposerId       string  `protobuf:"bytes,1,opt,name=proposer_id,json=proposerId,proto3" json:"proposer_id,omitempty"`
	TargetId         string  `protobuf:"bytes,2,opt,name=target_id,json=targetId,proto3" json:"target_id,omitempty"`
	Dimension        int32   `protobuf:"varint,3,opt,name=dimension,proto3" json:"dimension,omitempty"`
	ProposedBoundary float64 `protobuf:"fixed64,4,opt,name=proposed_boundary,json=proposedBoundary,proto3" json:"proposed_boundary,omitempty"`
	CurrentLoad      float64 `protobuf:"fixed64,5,opt,name=current_load,json=currentLoad,proto3" json:"current_load,omitempty"`
	ProposalId       string  `protobuf:"bytes,6,opt,name=proposal_id,json=proposalId,proto3" json:"proposal_id,omitempty"`
}

type ZoneAdjustmentProposalResponse struct {
	Accepted bool   `protobuf:"varint,1,opt,name=accepted,proto3" json:"accepted,omitempty"`
	Reason   string `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
}

type TransferKeyRequest struct {
	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

type TransferKeyResponse struct {
	Success bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Error   string `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

// Basic client interfaces
type CANServiceClient interface {
	Put(ctx interface{}, in *PutRequest) (*PutResponse, error)
	Get(ctx interface{}, in *GetRequest) (*GetResponse, error)
	Delete(ctx interface{}, in *DeleteRequest) (*DeleteResponse, error)
	
	// Load balancing operations
	ProposeZoneAdjustment(ctx interface{}, in *ZoneAdjustmentProposalRequest) (*ZoneAdjustmentProposalResponse, error)
	TransferKey(ctx interface{}, in *TransferKeyRequest) (*TransferKeyResponse, error)
} 