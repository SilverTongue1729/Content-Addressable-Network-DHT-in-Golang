package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// NodeState represents the state of a node in the CAN DHT
type NodeState struct {
	ID               string     `json:"id"`
	Zone             [4]float64 `json:"zone"`
	Failed           bool       `json:"failed"`
	LastSeen         time.Time  `json:"lastSeen"`
	Replicas         []string   `json:"replicas"`         // IDs of nodes that have replicas of this node's data
	Load             float64    `json:"load"`             // Current load on the node (0-1)
	RecoveryProgress float64    `json:"recoveryProgress"` // 0-1 indicating recovery progress
	FailureTime      time.Time  `json:"failureTime,omitempty"`
	// Add load balancing fields
	HighLoad        bool              `json:"highLoad"`         // Flag indicating high load
	Balanced        bool              `json:"balanced"`         // Flag indicating recently balanced
	HotKeys         []string          `json:"hotKeys"`          // Hot keys on this node
	BalanceHistory  []BalanceEvent    `json:"balanceHistory"`   // History of balance events
}

// DataItem represents a key-value pair in the DHT
type DataItem struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	NodeID    string    `json:"nodeId"`
	IsReplica bool      `json:"isReplica"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
	AccessCount int     `json:"accessCount"` // Number of accesses (for hot keys)
}

// BalanceEvent represents a load balancing event
type BalanceEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`      // "zone_transfer", "key_replication", etc.
	SourceID  string    `json:"sourceId"`  // Source node ID
	TargetID  string    `json:"targetId"`  // Target node ID (if applicable)
	Details   string    `json:"details"`   // Additional details
}

// ZoneTransfer represents a zone transfer during load balancing
type ZoneTransfer struct {
	SourceNodeId string    `json:"sourceNodeId"`
	TargetNodeId string    `json:"targetNodeId"`
	OldZone      [4]float64 `json:"oldZone"`
	NewZone      [4]float64 `json:"newZone"`
	Timestamp    time.Time  `json:"timestamp"`
	Status       string     `json:"status"` // "in_progress", "completed", "failed"
}

// LoadBalancingMetrics represents metrics for the load balancing visualization
type LoadBalancingMetrics struct {
	ZoneCount       int       `json:"zoneCount"`
	ZoneVariance    float64   `json:"zoneVariance"`
	LoadVariance    float64   `json:"loadVariance"`
	HotKeyCount     int       `json:"hotKeyCount"`
	RecentTransfers int       `json:"recentTransfers"`
	LastBalanced    time.Time `json:"lastBalanced"`
}

// NetworkState represents the entire state of the CAN DHT network
type NetworkState struct {
	Nodes       []NodeState `json:"nodes"`
	Data        []DataItem  `json:"data"`
	Replication []struct {
		SourceNodeID string    `json:"sourceNodeId"`
		TargetNodeID string    `json:"targetNodeId"`
		Key          string    `json:"key"`
		Status       string    `json:"status"` // "in_progress", "completed", "failed"
		StartedAt    time.Time `json:"startedAt"`
		CompletedAt  time.Time `json:"completedAt,omitempty"`
	} `json:"replication"`
	// Add load balancing fields
	ZoneTransfers    []ZoneTransfer      `json:"zoneTransfers"`
	LoadBalanceEvents []BalanceEvent     `json:"loadBalanceEvents"`
}

// Global state to track node status
var (
	networkState NetworkState
	stateMutex   sync.RWMutex
)

func main() {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())
	
	router := gin.Default()

	// Serve static files (CSS, JS)
	router.Static("/static", "./static")

	// Serve the main HTML page
	router.LoadHTMLGlob("templates/*")
	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{
			"title": "CAN DHT Visualization",
		})
	})

	// Initialize the network state
	initializeNetworkState()
	
	// Set up WebSocket manager
	wsManager := NewWebSocketManager()
	wsManager.Start()
	wsManager.StartPeriodicUpdates()
	
	// WebSocket endpoint
	router.GET("/ws", func(c *gin.Context) {
		wsManager.HandleWebSocket(c.Writer, c.Request)
	})

	// API endpoint to get DHT state
	router.GET("/api/state", func(c *gin.Context) {
		stateMutex.RLock()
		defer stateMutex.RUnlock()
		c.JSON(http.StatusOK, networkState)
	})

	// API endpoint to fail a node
	router.POST("/api/nodes/:id/fail", func(c *gin.Context) {
		nodeID := c.Param("id")
		stateMutex.Lock()
		defer stateMutex.Unlock()

		// Find and update the node
		for i := range networkState.Nodes {
			if networkState.Nodes[i].ID == nodeID {
				networkState.Nodes[i].Failed = true
				networkState.Nodes[i].LastSeen = time.Now()
				networkState.Nodes[i].RecoveryProgress = 0.0
				networkState.Nodes[i].FailureTime = time.Now()

				// Trigger replication for data on the failed node
				triggerReplication(nodeID)
				
				// Broadcast node failure event
				go wsManager.BroadcastEvent("node_failure", map[string]string{
					"nodeId": nodeID,
					"time":   time.Now().Format(time.RFC3339),
				})
				
				break
			}
		}

		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})

	// API endpoint to recover a node
	router.POST("/api/nodes/:id/recover", func(c *gin.Context) {
		nodeID := c.Param("id")
		stateMutex.Lock()
		defer stateMutex.Unlock()

		// Find and update the node
		for i := range networkState.Nodes {
			if networkState.Nodes[i].ID == nodeID {
				networkState.Nodes[i].Failed = false
				networkState.Nodes[i].LastSeen = time.Now()
				networkState.Nodes[i].RecoveryProgress = 1.0
				networkState.Nodes[i].FailureTime = time.Time{} // Clear failure time
				
				// Broadcast node recovery event
				go wsManager.BroadcastEvent("node_recovery", map[string]string{
					"nodeId": nodeID,
					"time":   time.Now().Format(time.RFC3339),
				})
				
				break
			}
		}

		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})

	// Add a new endpoint to get recovery progress
	router.GET("/api/nodes/:id/recovery-progress", func(c *gin.Context) {
		nodeID := c.Param("id")
		stateMutex.RLock()
		defer stateMutex.RUnlock()

		for _, node := range networkState.Nodes {
			if node.ID == nodeID {
				c.JSON(http.StatusOK, gin.H{
					"recoveryProgress": node.RecoveryProgress,
					"failureTime":      node.FailureTime,
				})
				return
			}
		}

		c.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
	})

	// API endpoint for load balancing metrics
	router.GET("/api/metrics/loadbalancing", func(c *gin.Context) {
		stateMutex.RLock()
		defer stateMutex.RUnlock()
		
		// Calculate metrics
		metrics := calculateLoadBalancingMetrics()
		c.JSON(http.StatusOK, metrics)
	})
	
	// API endpoint to trigger load balancing
	router.POST("/api/loadbalance", func(c *gin.Context) {
		stateMutex.Lock()
		defer stateMutex.Unlock()
		
		// Simulate load balancing
		adjustments := simulateLoadBalancing()
		
		// Broadcast load balancing event
		go wsManager.BroadcastEvent("load_balancing", map[string]interface{}{
			"adjustments": adjustments,
			"time":        time.Now().Format(time.RFC3339),
		})
		
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
			"adjustments": adjustments,
		})
	})
	
	// API endpoint to simulate load
	router.POST("/api/simulate-load", func(c *gin.Context) {
		stateMutex.Lock()
		defer stateMutex.Unlock()
		
		// Simulate varying load on nodes
		simulateVaryingLoad()
		
		// Broadcast load simulation event
		go wsManager.BroadcastEvent("load_simulation", map[string]string{
			"time": time.Now().Format(time.RFC3339),
		})
		
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
		})
	})
	
	// New endpoint to add a node to the network
	router.POST("/api/nodes/add", func(c *gin.Context) {
		var req struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
		}
		
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}
		
		stateMutex.Lock()
		defer stateMutex.Unlock()
		
		// Generate a new node ID
		nodeID := fmt.Sprintf("node%d", len(networkState.Nodes)+1)
		
		// Create the new node
		newNode := NodeState{
			ID:       nodeID,
			Zone:     [4]float64{req.X, req.Y, req.X + 0.2, req.Y + 0.2}, // Simple zone allocation
			Failed:   false,
			LastSeen: time.Now(),
			Replicas: []string{},
			Load:     0.2, // Initial load
		}
		
		// Add the node to the network
		networkState.Nodes = append(networkState.Nodes, newNode)
		
		// Broadcast node addition event
		go wsManager.BroadcastEvent("node_added", map[string]interface{}{
			"nodeId": nodeID,
			"x":      req.X,
			"y":      req.Y,
			"time":   time.Now().Format(time.RFC3339),
		})
		
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
			"nodeId": nodeID,
		})
	})
	
	// New endpoint to remove a node from the network
	router.DELETE("/api/nodes/:id", func(c *gin.Context) {
		nodeID := c.Param("id")
		
		stateMutex.Lock()
		defer stateMutex.Unlock()
		
		// Find the node
		nodeIndex := -1
		for i, node := range networkState.Nodes {
			if node.ID == nodeID {
				nodeIndex = i
				break
			}
		}
		
		if nodeIndex == -1 {
			c.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
			return
		}
		
		// Remove the node
		networkState.Nodes = append(networkState.Nodes[:nodeIndex], networkState.Nodes[nodeIndex+1:]...)
		
		// Broadcast node removal event
		go wsManager.BroadcastEvent("node_removed", map[string]string{
			"nodeId": nodeID,
			"time":   time.Now().Format(time.RFC3339),
		})
		
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})

	// Start the server
	fmt.Println("Starting visualization server on http://localhost:8090")
	router.Run(":8090")
}

func initializeNetworkState() {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	now := time.Now()

	// Create nodes with some failed and some with high load
	networkState.Nodes = []NodeState{
		{
			ID:       "node1",
			Zone:     [4]float64{0, 0, 0.5, 0.5},
			Failed:   false,
			LastSeen: now.Add(-10 * time.Second),
			Replicas: []string{"node4", "node3"},
			Load:     0.3,
			HotKeys:  []string{},
			BalanceHistory: []BalanceEvent{},
		},
		{
			ID:       "node2",
			Zone:     [4]float64{0.5, 0, 1, 0.5},
			Failed:   true,
			LastSeen: now.Add(-5 * time.Minute),
			Replicas: []string{"node4"},
			Load:     0.0,
			HotKeys:  []string{},
			BalanceHistory: []BalanceEvent{},
		},
		{
			ID:       "node3",
			Zone:     [4]float64{0, 0.5, 0.5, 1},
			Failed:   false,
			LastSeen: now.Add(-15 * time.Second),
			Replicas: []string{"node1"},
			Load:     0.8,
			HighLoad: true,
			HotKeys:  []string{"key3", "key4"},
			BalanceHistory: []BalanceEvent{},
		},
		{
			ID:       "node4",
			Zone:     [4]float64{0.5, 0.5, 1, 1},
			Failed:   false,
			LastSeen: now.Add(-5 * time.Second),
			Replicas: []string{"node1"},
			Load:     0.5,
			HotKeys:  []string{},
			BalanceHistory: []BalanceEvent{},
		},
	}

	// Create data items with replicas
	networkState.Data = []DataItem{
		{
			Key:       "key1",
			Value:     "value1",
			NodeID:    "node1",
			IsReplica: false,
			Created:   now.Add(-1 * time.Hour),
			Updated:   now.Add(-30 * time.Minute),
			AccessCount: 10,
		},
		{
			Key:       "key1_replica1",
			Value:     "value1",
			NodeID:    "node4",
			IsReplica: true,
			Created:   now.Add(-30 * time.Minute),
			Updated:   now.Add(-25 * time.Minute),
			AccessCount: 5,
		},
		{
			Key:       "key1_replica2",
			Value:     "value1",
			NodeID:    "node3",
			IsReplica: true,
			Created:   now.Add(-20 * time.Minute),
			Updated:   now.Add(-15 * time.Minute),
			AccessCount: 5,
		},
		{
			Key:       "key2",
			Value:     "value2",
			NodeID:    "node2",
			IsReplica: false,
			Created:   now.Add(-2 * time.Hour),
			Updated:   now.Add(-1 * time.Hour),
			AccessCount: 10,
		},
		{
			Key:       "key2_replica",
			Value:     "value2",
			NodeID:    "node4",
			IsReplica: true,
			Created:   now.Add(-1 * time.Hour),
			Updated:   now.Add(-45 * time.Minute),
			AccessCount: 5,
		},
		{
			Key:       "key3",
			Value:     "value3",
			NodeID:    "node3",
			IsReplica: false,
			Created:   now.Add(-2 * time.Hour),
			Updated:   now.Add(-1 * time.Hour),
			AccessCount: 150, // Hot key
		},
		{
			Key:       "key4",
			Value:     "value4",
			NodeID:    "node3",
			IsReplica: false,
			Created:   now.Add(-3 * time.Hour),
			Updated:   now.Add(-30 * time.Minute),
			AccessCount: 200, // Hot key
		},
	}

	// Initialize replication events
	networkState.Replication = []struct {
		SourceNodeID string    `json:"sourceNodeId"`
		TargetNodeID string    `json:"targetNodeId"`
		Key          string    `json:"key"`
		Status       string    `json:"status"`
		StartedAt    time.Time `json:"startedAt"`
		CompletedAt  time.Time `json:"completedAt,omitempty"`
	}{}

	// Initialize other load balancing structures
	networkState.ZoneTransfers = []ZoneTransfer{}
	networkState.LoadBalanceEvents = []BalanceEvent{}
}

func triggerReplication(failedNodeID string) {
	now := time.Now()

	// Find data that needs replication
	for _, data := range networkState.Data {
		if data.NodeID == failedNodeID && !data.IsReplica {
			// Find a suitable target node (not failed and not the source)
			var targetNodeID string
			for _, node := range networkState.Nodes {
				if !node.Failed && node.ID != failedNodeID {
					targetNodeID = node.ID
					break
				}
			}

			if targetNodeID != "" {
				// Add replication event
				networkState.Replication = append(networkState.Replication, struct {
					SourceNodeID string    `json:"sourceNodeId"`
					TargetNodeID string    `json:"targetNodeId"`
					Key          string    `json:"key"`
					Status       string    `json:"status"`
					StartedAt    time.Time `json:"startedAt"`
					CompletedAt  time.Time `json:"completedAt,omitempty"`
				}{
					SourceNodeID: failedNodeID,
					TargetNodeID: targetNodeID,
					Key:          data.Key,
					Status:       "in_progress",
					StartedAt:    now,
				})
			}
		}
	}
}

// Function to calculate load balancing metrics
func calculateLoadBalancingMetrics() LoadBalancingMetrics {
	metrics := LoadBalancingMetrics{
		ZoneCount: len(networkState.Nodes),
	}
	
	// Calculate zone variance
	var totalZoneSize float64 = 0
	zoneSizes := make([]float64, len(networkState.Nodes))
	
	for i, node := range networkState.Nodes {
		// Calculate zone size (area in 2D)
		zoneWidth := node.Zone[2] - node.Zone[0]
		zoneHeight := node.Zone[3] - node.Zone[1]
		zoneSize := zoneWidth * zoneHeight
		
		zoneSizes[i] = zoneSize
		totalZoneSize += zoneSize
	}
	
	// Calculate average zone size
	avgZoneSize := totalZoneSize / float64(len(networkState.Nodes))
	
	// Calculate variance
	var varianceSum float64 = 0
	for _, size := range zoneSizes {
		diff := size - avgZoneSize
		varianceSum += diff * diff
	}
	
	metrics.ZoneVariance = varianceSum / float64(len(networkState.Nodes))
	
	// Calculate load variance
	var totalLoad float64 = 0
	for _, node := range networkState.Nodes {
		totalLoad += node.Load
	}
	
	avgLoad := totalLoad / float64(len(networkState.Nodes))
	
	var loadVarianceSum float64 = 0
	for _, node := range networkState.Nodes {
		diff := node.Load - avgLoad
		loadVarianceSum += diff * diff
	}
	
	metrics.LoadVariance = loadVarianceSum / float64(len(networkState.Nodes))
	
	// Count hot keys
	hotKeyMap := make(map[string]bool)
	for _, node := range networkState.Nodes {
		for _, key := range node.HotKeys {
			hotKeyMap[key] = true
		}
	}
	metrics.HotKeyCount = len(hotKeyMap)
	
	// Count recent transfers (last 5 minutes)
	cutoff := time.Now().Add(-5 * time.Minute)
	recentTransfers := 0
	for _, transfer := range networkState.ZoneTransfers {
		if transfer.Timestamp.After(cutoff) {
			recentTransfers++
		}
	}
	metrics.RecentTransfers = recentTransfers
	
	// Find most recent balance event
	if len(networkState.LoadBalanceEvents) > 0 {
		metrics.LastBalanced = networkState.LoadBalanceEvents[len(networkState.LoadBalanceEvents)-1].Timestamp
	}
	
	return metrics
}

// Function to simulate load balancing
func simulateLoadBalancing() []ZoneTransfer {
	// Find high-load nodes
	highLoadNodes := make([]int, 0)
	lowLoadNodes := make([]int, 0)
	
	for i, node := range networkState.Nodes {
		if node.Failed {
			continue
		}
		
		if node.Load > 0.7 {
			highLoadNodes = append(highLoadNodes, i)
		} else if node.Load < 0.3 {
			lowLoadNodes = append(lowLoadNodes, i)
		}
	}
	
	// No balancing needed if no high-load nodes
	if len(highLoadNodes) == 0 {
		return []ZoneTransfer{}
	}
	
	// Perform zone transfers
	adjustments := make([]ZoneTransfer, 0)
	now := time.Now()
	
	for _, highIdx := range highLoadNodes {
		// Skip if no low-load nodes available
		if len(lowLoadNodes) == 0 {
			break
		}
		
		// Get a random low-load node
		lowIdx := lowLoadNodes[0]
		lowLoadNodes = lowLoadNodes[1:]
		
		highNode := &networkState.Nodes[highIdx]
		lowNode := &networkState.Nodes[lowIdx]
		
		// Create a zone transfer
		transfer := ZoneTransfer{
			SourceNodeId: highNode.ID,
			TargetNodeId: lowNode.ID,
			OldZone:      highNode.Zone,
			Timestamp:    now,
			Status:       "completed",
		}
		
		// Simulate transferring part of the zone
		// For simplicity, we'll split the zone horizontally
		originalZone := highNode.Zone
		newHighZone := originalZone
		
		// Calculate the midpoint
		midX := (originalZone[0] + originalZone[2]) / 2
		midY := (originalZone[1] + originalZone[3]) / 2
		
		// Create new zones
		if originalZone[2] - originalZone[0] > originalZone[3] - originalZone[1] {
			// Split horizontally
			newHighZone[2] = midX
			transfer.NewZone = [4]float64{midX, originalZone[1], originalZone[2], originalZone[3]}
		} else {
			// Split vertically
			newHighZone[3] = midY
			transfer.NewZone = [4]float64{originalZone[0], midY, originalZone[2], originalZone[3]}
		}
		
		// Update the zones
		highNode.Zone = newHighZone
		
		// Adjust loads
		originalLoad := highNode.Load
		highNode.Load = originalLoad * 0.6
		lowNode.Load += originalLoad * 0.4
		
		// Set nodes as balanced
		highNode.Balanced = true
		lowNode.Balanced = true
		highNode.HighLoad = false
		
		// Add to balance history
		balanceEvent := BalanceEvent{
			Timestamp: now,
			Type:      "zone_transfer",
			SourceID:  highNode.ID,
			TargetID:  lowNode.ID,
			Details:   "Load balancing",
		}
		
		highNode.BalanceHistory = append(highNode.BalanceHistory, balanceEvent)
		lowNode.BalanceHistory = append(lowNode.BalanceHistory, balanceEvent)
		
		// Add to network state
		networkState.ZoneTransfers = append(networkState.ZoneTransfers, transfer)
		networkState.LoadBalanceEvents = append(networkState.LoadBalanceEvents, balanceEvent)
		
		adjustments = append(adjustments, transfer)
	}
	
	return adjustments
}

// Function to simulate varying load on nodes
func simulateVaryingLoad() {
	// Add random load to nodes
	for i := range networkState.Nodes {
		if networkState.Nodes[i].Failed {
			continue
		}
		
		// Random load increase or decrease
		loadChange := (rand.Float64() * 0.4) - 0.2 // -0.2 to 0.2
		
		// Apply the change
		newLoad := networkState.Nodes[i].Load + loadChange
		
		// Clamp to 0-1 range
		if newLoad < 0 {
			newLoad = 0
		} else if newLoad > 1 {
			newLoad = 1
		}
		
		networkState.Nodes[i].Load = newLoad
		
		// Update high load status
		networkState.Nodes[i].HighLoad = newLoad > 0.7
		
		// Add some hot keys to high-load nodes
		if newLoad > 0.7 && len(networkState.Nodes[i].HotKeys) == 0 {
			// Find some keys to mark as hot
			hotKeys := make([]string, 0)
			for _, data := range networkState.Data {
				if data.NodeID == networkState.Nodes[i].ID && !data.IsReplica {
					hotKeys = append(hotKeys, data.Key)
					if len(hotKeys) >= 2 {
						break
					}
				}
			}
			networkState.Nodes[i].HotKeys = hotKeys
		}
	}
}
