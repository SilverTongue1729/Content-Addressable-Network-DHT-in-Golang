package service

import (
	"time"
)

// DefaultCANConfig returns a default configuration for a CAN server
func DefaultCANConfig() *CANConfig {
	return &CANConfig{
		Dimensions:                 2,
		DataDir:                    "data",
		ReplicationFactor:          2,
		HeartbeatInterval:          5 * time.Second,
		HeartbeatTimeout:           15 * time.Second,
		IntegrityCheckInterval:     30 * time.Minute,
		KeyRotationInterval:        24 * time.Hour,
		UseEnhancedIntegrity:       true,
		EnableCache:                true,
		CacheSize:                  1000,
		EnableTLS:                  false,
		LoadBalancingThreshold:     0.8,
		EnableAccessControl:        false,
		NodeRole:                   string(RoleStandard),
		EnableZoneMerging:          true,
		MinZoneSize:                0.1,
		EnableRequestRedistribution: true,
		RequestDistributionStrategy: "direct",
	}
}

// ConfigForRole returns a specialized configuration for a specific node role
func ConfigForRole(nodeID, address string, role NodeRole) *CANConfig {
	config := DefaultCANConfig()
	config.NodeID = nodeID
	config.Address = address
	config.NodeRole = string(role)
	
	// Configure based on role
	switch role {
	case RoleReadOnly:
		// Read-only nodes should not handle writes
		config.EnableRequestRedistribution = true
		config.RequestDistributionStrategy = "least_loaded"
		
	case RoleWriteOnly:
		// Write-only nodes should not handle reads
		config.EnableRequestRedistribution = true
		config.RequestDistributionStrategy = "least_loaded"
		
	case RoleEdge:
		// Edge nodes are optimized for fast reads and low latency
		config.EnableCache = true
		config.CacheSize = 5000
		config.ReplicationFactor = 3
		
	case RoleIndexer:
		// Indexer nodes focus on routing and coordination
		config.EnableRequestRedistribution = true
		config.RequestDistributionStrategy = "least_loaded"
		
	case RoleBootstrap:
		// Bootstrap nodes are stable entry points to the network
		config.EnableRequestRedistribution = true
		config.RequestDistributionStrategy = "least_loaded"
	}
	
	return config
} 