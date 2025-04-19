package main

import (
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
}

// DataItem represents a key-value pair in the DHT
type DataItem struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	NodeID    string    `json:"nodeId"`
	IsReplica bool      `json:"isReplica"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
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
}

// Global state to track node status
var (
	networkState NetworkState
	stateMutex   sync.RWMutex
)

func main() {
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
		},
		{
			ID:       "node2",
			Zone:     [4]float64{0.5, 0, 1, 0.5},
			Failed:   true,
			LastSeen: now.Add(-5 * time.Minute),
			Replicas: []string{"node4"},
			Load:     0.0,
		},
		{
			ID:       "node3",
			Zone:     [4]float64{0, 0.5, 0.5, 1},
			Failed:   false,
			LastSeen: now.Add(-15 * time.Second),
			Replicas: []string{"node1"},
			Load:     0.8,
		},
		{
			ID:       "node4",
			Zone:     [4]float64{0.5, 0.5, 1, 1},
			Failed:   false,
			LastSeen: now.Add(-5 * time.Second),
			Replicas: []string{"node1"},
			Load:     0.5,
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
		},
		{
			Key:       "key1_replica1",
			Value:     "value1",
			NodeID:    "node4",
			IsReplica: true,
			Created:   now.Add(-30 * time.Minute),
			Updated:   now.Add(-25 * time.Minute),
		},
		{
			Key:       "key1_replica2",
			Value:     "value1",
			NodeID:    "node3",
			IsReplica: true,
			Created:   now.Add(-20 * time.Minute),
			Updated:   now.Add(-15 * time.Minute),
		},
		{
			Key:       "key2",
			Value:     "value2",
			NodeID:    "node2",
			IsReplica: false,
			Created:   now.Add(-2 * time.Hour),
			Updated:   now.Add(-1 * time.Hour),
		},
		{
			Key:       "key2_replica",
			Value:     "value2",
			NodeID:    "node4",
			IsReplica: true,
			Created:   now.Add(-1 * time.Hour),
			Updated:   now.Add(-45 * time.Minute),
		},
		{
			Key:       "key3",
			Value:     "value3",
			NodeID:    "node3",
			IsReplica: false,
			Created:   now.Add(-30 * time.Minute),
			Updated:   now.Add(-10 * time.Minute),
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
