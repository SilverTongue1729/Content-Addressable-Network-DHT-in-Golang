package service

import (
	"context"
	"testing"
)

func TestRoleCapabilities(t *testing.T) {
	// Create test cases for different roles
	testCases := []struct {
		name             string
		role             NodeRole
		canRead          bool
		canWrite         bool
		canDelete        bool
		canJoin          bool
		canLeave         bool
		canReplicate     bool
		canIndex         bool
		canCoordinate    bool
	}{
		{
			name:             "StandardRole",
			role:             RoleStandard,
			canRead:          true,
			canWrite:         true,
			canDelete:        true,
			canJoin:          true,
			canLeave:         true,
			canReplicate:     true,
			canIndex:         true,
			canCoordinate:    true,
		},
		{
			name:             "ReadOnlyRole",
			role:             RoleReadOnly,
			canRead:          true,
			canWrite:         false,
			canDelete:        false,
			canJoin:          true,
			canLeave:         true,
			canReplicate:     true,
			canIndex:         true,
			canCoordinate:    true,
		},
		{
			name:             "WriteOnlyRole",
			role:             RoleWriteOnly,
			canRead:          false,
			canWrite:         true,
			canDelete:        true,
			canJoin:          true,
			canLeave:         true,
			canReplicate:     true,
			canIndex:         true,
			canCoordinate:    true,
		},
		{
			name:             "IndexerRole",
			role:             RoleIndexer,
			canRead:          false,
			canWrite:         false,
			canDelete:        false,
			canJoin:          true,
			canLeave:         true,
			canReplicate:     false,
			canIndex:         true,
			canCoordinate:    true,
		},
		{
			name:             "EdgeRole",
			role:             RoleEdge,
			canRead:          true,
			canWrite:         true,
			canDelete:        false,
			canJoin:          true,
			canLeave:         true,
			canReplicate:     true,
			canIndex:         false,
			canCoordinate:    false,
		},
		{
			name:             "BootstrapRole",
			role:             RoleBootstrap,
			canRead:          true,
			canWrite:         false,
			canDelete:        false,
			canJoin:          true,
			canLeave:         false,
			canReplicate:     false,
			canIndex:         true,
			canCoordinate:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create capabilities for the role
			capabilities := createRoleCapabilities(tc.role)
			
			// Verify capabilities match expected values
			if capabilities.CanRead != tc.canRead {
				t.Errorf("Expected CanRead to be %v for role %s, got %v", 
					tc.canRead, tc.role, capabilities.CanRead)
			}
			
			if capabilities.CanWrite != tc.canWrite {
				t.Errorf("Expected CanWrite to be %v for role %s, got %v", 
					tc.canWrite, tc.role, capabilities.CanWrite)
			}
			
			if capabilities.CanDelete != tc.canDelete {
				t.Errorf("Expected CanDelete to be %v for role %s, got %v", 
					tc.canDelete, tc.role, capabilities.CanDelete)
			}
			
			if capabilities.CanJoin != tc.canJoin {
				t.Errorf("Expected CanJoin to be %v for role %s, got %v", 
					tc.canJoin, tc.role, capabilities.CanJoin)
			}
			
			if capabilities.CanLeave != tc.canLeave {
				t.Errorf("Expected CanLeave to be %v for role %s, got %v", 
					tc.canLeave, tc.role, capabilities.CanLeave)
			}
			
			if capabilities.CanReplicate != tc.canReplicate {
				t.Errorf("Expected CanReplicate to be %v for role %s, got %v", 
					tc.canReplicate, tc.role, capabilities.CanReplicate)
			}
			
			if capabilities.CanIndex != tc.canIndex {
				t.Errorf("Expected CanIndex to be %v for role %s, got %v", 
					tc.canIndex, tc.role, capabilities.CanIndex)
			}
			
			if capabilities.CanCoordinate != tc.canCoordinate {
				t.Errorf("Expected CanCoordinate to be %v for role %s, got %v", 
					tc.canCoordinate, tc.role, capabilities.CanCoordinate)
			}
		})
	}
}

func TestRoleManager(t *testing.T) {
	// Create a simple server for testing
	config := DefaultCANConfig()
	config.NodeRole = string(RoleReadOnly)
	
	// Creating a minimal server for testing
	server := &CANServer{
		Config: config,
	}
	
	// Create a role manager
	manager := NewRoleManager(server)
	
	// Test Read capability (should be allowed)
	err := manager.CanHandleReadRequest(context.Background())
	if err != nil {
		t.Errorf("ReadOnly role should be able to handle read requests, got error: %v", err)
	}
	
	// Test Write capability (should be denied)
	err = manager.CanHandleWriteRequest(context.Background())
	if err == nil {
		t.Errorf("ReadOnly role should NOT be able to handle write requests")
	}
	
	// Change role to WriteOnly
	config.NodeRole = string(RoleWriteOnly)
	
	// Test Read capability (should be denied)
	err = manager.CanHandleReadRequest(context.Background())
	if err == nil {
		t.Errorf("WriteOnly role should NOT be able to handle read requests")
	}
	
	// Test Write capability (should be allowed)
	err = manager.CanHandleWriteRequest(context.Background())
	if err != nil {
		t.Errorf("WriteOnly role should be able to handle write requests, got error: %v", err)
	}
}

func TestDynamicRoleChange(t *testing.T) {
	// Create a simple server for testing
	config := DefaultCANConfig()
	config.NodeRole = string(RoleStandard)
	
	// Creating a minimal server for testing
	server := &CANServer{
		Config: config,
	}
	
	// Create a role manager
	manager := NewRoleManager(server)
	
	// Test standard role can do everything
	if err := manager.CanHandleReadRequest(context.Background()); err != nil {
		t.Errorf("Standard role should be able to handle read requests, got error: %v", err)
	}
	
	if err := manager.CanHandleWriteRequest(context.Background()); err != nil {
		t.Errorf("Standard role should be able to handle write requests, got error: %v", err)
	}
	
	// Change role dynamically
	config.NodeRole = string(RoleReadOnly)
	
	// Test read should work, write should fail
	if err := manager.CanHandleReadRequest(context.Background()); err != nil {
		t.Errorf("ReadOnly role should be able to handle read requests, got error: %v", err)
	}
	
	if err := manager.CanHandleWriteRequest(context.Background()); err == nil {
		t.Errorf("ReadOnly role should NOT be able to handle write requests")
	}
} 