package service

import (
	"context"
	"fmt"
	"log"
)

// NodeRole defines the role of a node in the network
type NodeRole string

// Define standard node roles
const (
	// Standard nodes provide all services
	RoleStandard NodeRole = "standard"
	
	// ReadOnly nodes only serve read requests
	RoleReadOnly NodeRole = "read_only"
	
	// WriteOnly nodes only serve write requests
	RoleWriteOnly NodeRole = "write_only"
	
	// Indexer nodes build and maintain search indexes
	RoleIndexer NodeRole = "indexer"
	
	// Edge nodes are optimized for client-facing requests
	RoleEdge NodeRole = "edge"
	
	// Bootstrap nodes help new nodes join the network
	RoleBootstrap NodeRole = "bootstrap"
)

// Role capabilities
type RoleCapabilities struct {
	// CanRead defines if this role can serve read requests
	CanRead bool
	
	// CanWrite defines if this role can serve write requests
	CanWrite bool
	
	// CanDelete defines if this role can serve delete requests
	CanDelete bool
	
	// CanJoin defines if this role can accept join requests
	CanJoin bool
	
	// CanLeave defines if this role can handle leave requests
	CanLeave bool
	
	// CanReplicate defines if this role can replicate data
	CanReplicate bool
	
	// CanIndex defines if this role can build search indexes
	CanIndex bool
	
	// CanCoordinate defines if this role can coordinate cluster operations
	CanCoordinate bool
}

// RoleManager manages node roles and capabilities
type RoleManager struct {
	// Reference to the server
	server *CANServer
	
	// Current node role
	currentRole NodeRole
	
	// Role capabilities
	capabilities RoleCapabilities
}

// NewRoleManager creates a new role manager
func NewRoleManager(server *CANServer) *RoleManager {
	rm := &RoleManager{
		server: server,
	}
	
	role := NodeRole(server.Config.NodeRole)
	rm.currentRole = role
	rm.setCapabilitiesForRole(role)
	
	return rm
}

// setCapabilitiesForRole sets the capabilities for the given role
func (rm *RoleManager) setCapabilitiesForRole(role NodeRole) {
	rm.capabilities = createRoleCapabilities(role)
}

// CanHandleReadRequest checks if this node can handle read requests
func (rm *RoleManager) CanHandleReadRequest(ctx context.Context) error {
	if !rm.capabilities.CanRead {
		return fmt.Errorf("node with role '%s' cannot handle read requests", rm.currentRole)
	}
	return nil
}

// CanHandleWriteRequest checks if this node can handle write requests
func (rm *RoleManager) CanHandleWriteRequest(ctx context.Context) error {
	if !rm.capabilities.CanWrite {
		return fmt.Errorf("node with role '%s' cannot handle write requests", rm.currentRole)
	}
	return nil
}

// CanHandleDeleteRequest checks if this node can handle delete requests
func (rm *RoleManager) CanHandleDeleteRequest(ctx context.Context) error {
	if !rm.capabilities.CanDelete {
		return fmt.Errorf("node with role '%s' cannot handle delete requests", rm.currentRole)
	}
	return nil
}

// CanHandleJoinRequest checks if this node can handle join requests
func (rm *RoleManager) CanHandleJoinRequest(ctx context.Context) error {
	if !rm.capabilities.CanJoin {
		return fmt.Errorf("node with role '%s' cannot handle join requests", rm.currentRole)
	}
	return nil
}

// CanHandleLeaveRequest checks if this node can handle leave requests
func (rm *RoleManager) CanHandleLeaveRequest(ctx context.Context) error {
	if !rm.capabilities.CanLeave {
		return fmt.Errorf("node with role '%s' cannot handle leave requests", rm.currentRole)
	}
	return nil
}

// CanReplicateData checks if this node can replicate data
func (rm *RoleManager) CanReplicateData(ctx context.Context) error {
	if !rm.capabilities.CanReplicate {
		return fmt.Errorf("node with role '%s' cannot replicate data", rm.currentRole)
	}
	return nil
}

// GetNodeRole returns the current node role
func (rm *RoleManager) GetNodeRole() NodeRole {
	return rm.currentRole
}

// ChangeRole changes the node's role
func (rm *RoleManager) ChangeRole(role string) {
	nodeRole := NodeRole(role)
	rm.currentRole = nodeRole
	rm.setCapabilitiesForRole(nodeRole)
}

// createRoleCapabilities returns the capabilities for a specific role
func createRoleCapabilities(role NodeRole) RoleCapabilities {
	switch role {
	case RoleStandard:
		return RoleCapabilities{
			CanRead:       true,
			CanWrite:      true,
			CanDelete:     true,
			CanJoin:       true,
			CanLeave:      true,
			CanReplicate:  true,
			CanIndex:      true,
			CanCoordinate: true,
		}
	case RoleReadOnly:
		return RoleCapabilities{
			CanRead:       true,
			CanWrite:      false,
			CanDelete:     false,
			CanJoin:       true,
			CanLeave:      true,
			CanReplicate:  true,
			CanIndex:      true,
			CanCoordinate: true,
		}
	case RoleWriteOnly:
		return RoleCapabilities{
			CanRead:       false,
			CanWrite:      true,
			CanDelete:     true,
			CanJoin:       true,
			CanLeave:      true,
			CanReplicate:  true,
			CanIndex:      true,
			CanCoordinate: true,
		}
	case RoleIndexer:
		return RoleCapabilities{
			CanRead:       false,
			CanWrite:      false,
			CanDelete:     false,
			CanJoin:       true,
			CanLeave:      true,
			CanReplicate:  false,
			CanIndex:      true,
			CanCoordinate: true,
		}
	case RoleEdge:
		return RoleCapabilities{
			CanRead:       true,
			CanWrite:      true,
			CanDelete:     false,
			CanJoin:       true,
			CanLeave:      true,
			CanReplicate:  true,
			CanIndex:      false,
			CanCoordinate: false,
		}
	case RoleBootstrap:
		return RoleCapabilities{
			CanRead:       true,
			CanWrite:      false,
			CanDelete:     false,
			CanJoin:       true,
			CanLeave:      false,
			CanReplicate:  false,
			CanIndex:      true,
			CanCoordinate: true,
		}
	default:
		// Default to standard role capabilities
		return RoleCapabilities{
			CanRead:       true,
			CanWrite:      true,
			CanDelete:     true,
			CanJoin:       true,
			CanLeave:      true,
			CanReplicate:  true,
			CanIndex:      true,
			CanCoordinate: true,
		}
	}
}

// CanHandleIndexRequest checks if this node can handle index requests
func (rm *RoleManager) CanHandleIndexRequest(ctx context.Context) error {
	role := NodeRole(rm.server.Config.NodeRole)
	capabilities := createRoleCapabilities(role)
	
	if !capabilities.CanIndex {
		return fmt.Errorf("node with role '%s' cannot handle index requests", role)
	}
	
	return nil
}

// CanHandleCoordinationRequest checks if this node can handle coordination requests
func (rm *RoleManager) CanHandleCoordinationRequest(ctx context.Context) error {
	role := NodeRole(rm.server.Config.NodeRole)
	capabilities := createRoleCapabilities(role)
	
	if !capabilities.CanCoordinate {
		return fmt.Errorf("node with role '%s' cannot handle coordination requests", role)
	}
	
	return nil
}

// CanHandleReplicationRequest checks if this node can handle replication requests
func (rm *RoleManager) CanHandleReplicationRequest(ctx context.Context) error {
	role := NodeRole(rm.server.Config.NodeRole)
	capabilities := createRoleCapabilities(role)
	
	if !capabilities.CanReplicate {
		return fmt.Errorf("node with role '%s' cannot handle replication requests", role)
	}
	
	return nil
} 