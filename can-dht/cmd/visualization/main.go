package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/routing"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// SimulationState holds the current state of the CAN DHT simulation
type SimulationState struct {
	Nodes             map[node.NodeID]*node.Node
	KeyValuePairs     map[string]KeyValueInfo
	RoutingPaths      []RoutingPath
	NodeJoinPoints    map[string]node.Point // Store join points by node ID
	ReplicationFactor int                   // Number of replicas to maintain
	mu                sync.RWMutex
}

// KeyValueInfo contains information about a key-value pair in the DHT
type KeyValueInfo struct {
	Key           string
	Value         string
	Point         node.Point
	NodeID        node.NodeID
	Color         string
	Encrypted     bool          // Add encryption status field
	ReplicaNodes  []node.NodeID // List of nodes containing replicas
	IsPrimaryCopy bool          // Whether this is the primary copy or a replica
}

// KVResponse is the JSON-friendly version of KeyValueInfo for API responses
type KVResponse struct {
	Key           string      `json:"Key"`
	Value         string      `json:"Value"`
	Point         node.Point  `json:"Point"`
	NodeID        node.NodeID `json:"NodeID"`
	Color         string      `json:"Color"`
	Encrypted     bool        `json:"Encrypted"`
	ReplicaNodes  []string    `json:"ReplicaNodes"` // Convert NodeID to string for JSON
	IsPrimaryCopy bool        `json:"IsPrimaryCopy"`
}

// RoutingPath represents a routing path for a request in the DHT
type RoutingPath struct {
	RequestType string // "PUT", "GET", or "DELETE"
	Key         string
	Path        []node.NodeID
	Active      bool
	StartTime   time.Time
	EndTime     time.Time
}

// Global simulation state
var simulation = &SimulationState{
	Nodes:             make(map[node.NodeID]*node.Node),
	KeyValuePairs:     make(map[string]KeyValueInfo),
	RoutingPaths:      make([]RoutingPath, 0),
	NodeJoinPoints:    make(map[string]node.Point), // Store join points by node ID
	ReplicationFactor: 1,                           // Default: no replication, just primary copy
}

// Colors for key-value pairs
var colors = []string{"#ff5733", "#33ff57", "#3357ff", "#f3ff33", "#ff33f3", "#33fff3"}

// Router instance
var router = routing.NewRouter(2) // 2D for visualization

func main() {
	// Initialize the simulation with a root node
	initSimulation()

	// Create Gin router
	r := gin.Default()

	// Configure CORS
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST"},
		AllowHeaders:     []string{"Origin", "Content-Type"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Serve static files
	r.Static("/static", "./cmd/visualization/static")
	r.StaticFile("/", "./cmd/visualization/static/index.html")

	// API Routes
	api := r.Group("/api")
	{
		api.GET("/state", getSimulationState)
		api.POST("/node/add", addNode)
		api.POST("/node/remove/:id", removeNode)
		api.POST("/kv/put", putKeyValue)
		api.GET("/kv/get/:key", getKeyValue)
		api.DELETE("/kv/delete/:key", deleteKeyValue)
		api.POST("/reset", resetSimulation)
		api.POST("/replication/factor", setReplicationFactor)
		api.POST("/replication/replicate", replicateKey)
	}

	// Start the server
	fmt.Println("Starting visualization server on http://localhost:8090")
	r.Run(":8090")
}

// initSimulation initializes the simulation with a root node
func initSimulation() {
	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Clear existing state
	simulation.Nodes = make(map[node.NodeID]*node.Node)
	simulation.KeyValuePairs = make(map[string]KeyValueInfo)
	simulation.RoutingPaths = make([]RoutingPath, 0)
	simulation.NodeJoinPoints = make(map[string]node.Point) // Clear join points too

	// Create the coordinate space for the root node
	minPoint := node.Point{0.0, 0.0}
	maxPoint := node.Point{1.0, 1.0}
	zone, _ := node.NewZone(minPoint, maxPoint)

	// Create the root node
	rootNode := node.NewNode("root", "localhost:9000", zone, 2)
	simulation.Nodes[rootNode.ID] = rootNode
}

// getSimulationState returns the current state of the simulation
func getSimulationState(c *gin.Context) {
	simulation.mu.RLock()
	defer simulation.mu.RUnlock()

	// Debug output
	fmt.Printf("\n========== SIMULATION STATE REQUEST ==========\n")
	fmt.Printf("Current state: %d nodes, %d key-value pairs in map\n",
		len(simulation.Nodes), len(simulation.KeyValuePairs))

	// Debug: Print all entries in KeyValuePairs with their type
	fmt.Printf("Key-Value Pairs in map:\n")
	var primaryCount, replicaCount int
	for k, v := range simulation.KeyValuePairs {
		typeStr := "REPLICA"
		if v.IsPrimaryCopy {
			typeStr = "PRIMARY"
			primaryCount++
		} else {
			replicaCount++
		}
		fmt.Printf("  - [%s] '%s': Key='%s', Value='%s', Node=%s, Point=%v\n",
			typeStr, k, v.Key, v.Value, v.NodeID, v.Point)
	}
	fmt.Printf("Total: %d primary, %d replica keys\n", primaryCount, replicaCount)

	// Check if we have a mismatch where nodes have keys but KeyValuePairs is empty
	if len(simulation.KeyValuePairs) == 0 {
		// Count keys in all nodes
		var totalNodeKeys int
		for nodeID, n := range simulation.Nodes {
			nodeKeyCount := len(n.Data)
			totalNodeKeys += nodeKeyCount
			if nodeKeyCount > 0 {
				fmt.Printf("Node %s contains %d keys:\n", nodeID, nodeKeyCount)
				for k, v := range n.Data {
					fmt.Printf("  - '%s' = '%s'\n", k, v)
				}
			}
		}

		if totalNodeKeys > 0 {
			fmt.Printf("WARNING: Nodes have %d keys but KeyValuePairs map is empty. Rebuilding...\n", totalNodeKeys)
			syncKeyValuePairsFromNodes()
		}
	}

	// Prepare response data
	type NodeData struct {
		ID        string     `json:"id"`
		Address   string     `json:"address"`
		Zone      *node.Zone `json:"zone"`
		Neighbors []string   `json:"neighbors"`
	}

	type ResponseData struct {
		Nodes             []NodeData    `json:"nodes"`
		KeyValuePairs     []KVResponse  `json:"keyValuePairs"`
		RoutingPaths      []RoutingPath `json:"routingPaths"`
		ReplicationFactor int           `json:"replicationFactor"`
	}

	response := ResponseData{
		Nodes:             make([]NodeData, 0, len(simulation.Nodes)),
		KeyValuePairs:     make([]KVResponse, 0, len(simulation.KeyValuePairs)),
		RoutingPaths:      make([]RoutingPath, 0),
		ReplicationFactor: simulation.ReplicationFactor,
	}

	// Convert nodes to response format
	for id, n := range simulation.Nodes {
		neighborIDs := make([]string, 0, len(n.Neighbors))
		for nID := range n.Neighbors {
			neighborIDs = append(neighborIDs, string(nID))
		}

		response.Nodes = append(response.Nodes, NodeData{
			ID:        string(id),
			Address:   n.Address,
			Zone:      n.Zone,
			Neighbors: neighborIDs,
		})
	}

	// Add key-value pairs with explicitly mapped fields
	// IMPORTANT: Include ALL key-value pairs, both primaries and replicas
	for key, kv := range simulation.KeyValuePairs {
		// Convert ReplicaNodes to string slice for JSON
		replicaNodeStrings := make([]string, 0, len(kv.ReplicaNodes))
		for _, nodeID := range kv.ReplicaNodes {
			replicaNodeStrings = append(replicaNodeStrings, string(nodeID))
		}

		// Create response object for this key-value pair
		kvResponse := KVResponse{
			Key:           kv.Key,
			Value:         kv.Value,
			Point:         kv.Point,
			NodeID:        kv.NodeID,
			Color:         kv.Color,
			Encrypted:     kv.Encrypted,
			ReplicaNodes:  replicaNodeStrings,
			IsPrimaryCopy: kv.IsPrimaryCopy,
		}

		// Add to response
		response.KeyValuePairs = append(response.KeyValuePairs, kvResponse)

		// Debug log this entry
		typeStr := "replica"
		if kv.IsPrimaryCopy {
			typeStr = "primary"
		}
		fmt.Printf("Added %s key '%s' to response (map key: '%s')\n", typeStr, kv.Key, key)
	}

	// Add active routing paths
	for _, path := range simulation.RoutingPaths {
		if path.Active {
			response.RoutingPaths = append(response.RoutingPaths, path)
		}
	}

	// Final summary
	fmt.Printf("Sending %d key-value pairs to client (%d primary, %d replicas)\n",
		len(response.KeyValuePairs),
		countPrimaryKeys(response.KeyValuePairs),
		len(response.KeyValuePairs)-countPrimaryKeys(response.KeyValuePairs))
	fmt.Printf("========== END SIMULATION STATE REQUEST ==========\n\n")

	c.JSON(http.StatusOK, response)
}

// syncKeyValuePairsFromNodes rebuilds the KeyValuePairs map from actual node data
// Call this with the simulation lock held
func syncKeyValuePairsFromNodes() {
	fmt.Printf("Starting to rebuild KeyValuePairs map from node data\n")

	// Clear existing KeyValuePairs
	simulation.KeyValuePairs = make(map[string]KeyValueInfo)

	// Create a map to track what keys we've already processed
	processedKeys := make(map[string]bool)

	// First pass: create primary copies
	for nodeID, n := range simulation.Nodes {
		fmt.Printf("Checking node %s for key-value pairs\n", nodeID)
		for key, value := range n.Data {
			// Skip if we already processed this key
			if processedKeys[key] {
				continue
			}

			// Mark this key as processed
			processedKeys[key] = true

			// Calculate point for this key
			point := router.HashToPoint(key)

			// Check if this node is responsible for this key
			if n.Zone.Contains(point) {
				// This is the primary node for this key
				colorIndex := len(simulation.KeyValuePairs) % len(colors)
				color := colors[colorIndex]

				// For now, we assume no encryption and no replicas
				primaryKV := KeyValueInfo{
					Key:           key,
					Value:         value,
					Point:         point,
					NodeID:        nodeID,
					Color:         color,
					Encrypted:     false,
					ReplicaNodes:  []node.NodeID{},
					IsPrimaryCopy: true,
				}

				// Add to KeyValuePairs map
				simulation.KeyValuePairs[key] = primaryKV
				fmt.Printf("Restored primary key '%s' on node %s\n", key, nodeID)
			}
		}
	}

	// Second pass: find replicas
	// For each primary key, check other nodes for replicas
	for key, primaryKV := range simulation.KeyValuePairs {
		if !primaryKV.IsPrimaryCopy {
			continue // Skip replicas
		}

		// Look for replicas on other nodes
		replicaNodes := []node.NodeID{}

		for nodeID, n := range simulation.Nodes {
			// Skip the primary node
			if nodeID == primaryKV.NodeID {
				continue
			}

			// Check if this node has this key
			if _, exists := n.Data[key]; exists {
				replicaNodes = append(replicaNodes, nodeID)

				// Create a replica entry in KeyValuePairs
				replicaKey := fmt.Sprintf("%s_replica_%d", key, len(replicaNodes))
				replicaKV := KeyValueInfo{
					Key:           key,
					Value:         primaryKV.Value,
					Point:         primaryKV.Point,
					NodeID:        nodeID,
					Color:         primaryKV.Color,
					Encrypted:     primaryKV.Encrypted,
					ReplicaNodes:  nil,
					IsPrimaryCopy: false,
				}

				simulation.KeyValuePairs[replicaKey] = replicaKV
				fmt.Printf("Restored replica key '%s' on node %s\n", key, nodeID)
			}
		}

		// Update primary key with replica nodes
		if len(replicaNodes) > 0 {
			primaryKV.ReplicaNodes = replicaNodes
			simulation.KeyValuePairs[key] = primaryKV
		}
	}

	fmt.Printf("Finished rebuilding KeyValuePairs map: %d entries\n", len(simulation.KeyValuePairs))
}

// Helper to count primary keys
func countPrimaryKeys(kvs []KVResponse) int {
	count := 0
	for _, kv := range kvs {
		if kv.IsPrimaryCopy {
			count++
		}
	}
	return count
}

// addNode adds a new node to the CAN DHT
func addNode(c *gin.Context) {
	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Parse request
	var request struct {
		JoinPoint []float64 `json:"joinPoint"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		// Just log the error, don't return - we'll use a random point if binding fails
		fmt.Printf("Failed to bind join point: %v\n", err)
	}

	// Generate a node ID
	nodeID := fmt.Sprintf("node-%d", len(simulation.Nodes))
	address := fmt.Sprintf("localhost:%d", 9000+len(simulation.Nodes))

	// Determine join point - either from request, cached, or generate new one
	var joinPoint node.Point
	if request.JoinPoint != nil && len(request.JoinPoint) == 2 {
		// Use the provided join point
		joinPoint = node.Point{request.JoinPoint[0], request.JoinPoint[1]}
	} else if cachedPoint, exists := simulation.NodeJoinPoints[nodeID]; exists {
		// Use the cached join point for this node ID
		joinPoint = cachedPoint
	} else {
		// Generate a new random join point
		joinPoint = node.Point{rand.Float64(), rand.Float64()}
		// Save it for future reference
		simulation.NodeJoinPoints[nodeID] = joinPoint
	}

	// Find the node responsible for that point (bootstrap node)
	var responsibleNode *node.Node
	for _, n := range simulation.Nodes {
		if n.Zone.Contains(joinPoint) {
			responsibleNode = n
			break
		}
	}

	if responsibleNode == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not find a responsible node"})
		return
	}

	// The responsible node splits its zone and the new node takes one half
	newNodeZone, err := responsibleNode.Split()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to split zone"})
		return
	}

	// Create the new node with the new zone
	newNode := node.NewNode(node.NodeID(nodeID), address, newNodeZone, 2)
	simulation.Nodes[newNode.ID] = newNode

	// Update neighbor relationships for all affected nodes
	updateNeighbors()

	// Transfer key-value pairs that now belong to the new node's zone
	redistributeKeyValuePairs()

	// Record the join point to visualize in the frontend
	joinPointInfo := map[string]interface{}{
		"id":    nodeID,
		"point": joinPoint,
	}

	c.JSON(http.StatusOK, joinPointInfo)
}

// removeNode removes a node from the CAN DHT
func removeNode(c *gin.Context) {
	nodeID := node.NodeID(c.Param("id"))

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Check if the node exists
	if _, exists := simulation.Nodes[nodeID]; !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
		return
	}

	// Remove the node
	delete(simulation.Nodes, nodeID)

	// TODO: Implement proper zone merging
	// For now, we'll just reset the simulation if only one node remains
	if len(simulation.Nodes) <= 1 {
		initSimulation()
		c.JSON(http.StatusOK, gin.H{"message": "Node removed and simulation reset"})
		return
	}

	// Update neighbors
	updateNeighbors()

	// Redistribute key-value pairs
	redistributeKeyValuePairs()

	c.JSON(http.StatusOK, gin.H{"message": "Node removed"})
}

// putKeyValue adds a key-value pair to the DHT
func putKeyValue(c *gin.Context) {
	var request struct {
		Key     string `json:"key"`
		Value   string `json:"value"`
		Encrypt bool   `json:"encrypt"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Clean the input key (prevent whitespace issues)
	key := strings.TrimSpace(request.Key)
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Key cannot be empty"})
		return
	}

	fmt.Printf("\n========== PUT OPERATION ==========\n")
	fmt.Printf("Adding key: '%s', value: '%s', encrypt: %v\n", key, request.Value, request.Encrypt)
	fmt.Printf("Current state: %d nodes, %d key-value pairs\n",
		len(simulation.Nodes), len(simulation.KeyValuePairs))

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Hash the key to get a point
	point := router.HashToPoint(key)
	fmt.Printf("Hashed point for key '%s': %v\n", key, point)

	// Find the responsible node
	var responsibleNode *node.Node
	for _, n := range simulation.Nodes {
		if n.Zone.Contains(point) {
			responsibleNode = n
			break
		}
	}

	if responsibleNode == nil {
		fmt.Printf("ERROR: Could not find a responsible node for key '%s'\n", key)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not find a responsible node"})
		return
	}

	fmt.Printf("Responsible node for key '%s': %s\n", key, responsibleNode.ID)

	// Simulate routing
	path := simulateRouting("PUT", key, responsibleNode.ID)

	// First, remove any existing copies of this key
	for k, kv := range simulation.KeyValuePairs {
		if kv.Key == key {
			delete(simulation.KeyValuePairs, k)
			fmt.Printf("Removed existing key '%s' from key-value map\n", k)
		}
	}

	// Store the key-value pair on the primary node
	responsibleNode.Put(key, request.Value)
	fmt.Printf("Stored key '%s' in responsible node %s (primary)\n", key, responsibleNode.ID)

	// Choose a color for this key-value pair
	colorIndex := len(simulation.KeyValuePairs) % len(colors)
	color := colors[colorIndex]

	// Get neighbor nodes for replication
	replicaNodes := []node.NodeID{}

	// Replicate to neighbors if replication factor > 1
	if simulation.ReplicationFactor > 1 {
		fmt.Printf("Replicating key '%s' with factor %d\n", key, simulation.ReplicationFactor)
		// Get all neighbors of the responsible node
		for neighborID := range responsibleNode.Neighbors {
			neighbor := simulation.Nodes[neighborID]
			if neighbor == nil {
				continue
			}

			// Add this neighbor to replica nodes
			replicaNodes = append(replicaNodes, neighborID)

			// Store the key-value pair on the neighbor node
			neighbor.Put(key, request.Value)
			fmt.Printf("Stored key '%s' in neighbor node %s (replica)\n", key, neighborID)

			// Limit replicas based on replication factor
			if len(replicaNodes) >= simulation.ReplicationFactor-1 {
				break
			}
		}
	}

	// Add primary copy to simulation state
	primaryKV := KeyValueInfo{
		Key:           key,
		Value:         request.Value,
		Point:         point,
		NodeID:        responsibleNode.ID,
		Color:         color,
		Encrypted:     request.Encrypt,
		ReplicaNodes:  replicaNodes,
		IsPrimaryCopy: true,
	}

	simulation.KeyValuePairs[key] = primaryKV
	fmt.Printf("Added primary key '%s' to key-value map\n", key)

	// Add replica entries if needed (with different map keys to distinguish them)
	for i, replicaNodeID := range replicaNodes {
		replicaKey := fmt.Sprintf("%s_replica_%d", key, i+1)
		replicaKV := KeyValueInfo{
			Key:           key, // Use the original key name for the key field
			Value:         request.Value,
			Point:         point,
			NodeID:        replicaNodeID,
			Color:         color,
			Encrypted:     request.Encrypt,
			ReplicaNodes:  nil, // No further nesting of replicas
			IsPrimaryCopy: false,
		}
		simulation.KeyValuePairs[replicaKey] = replicaKV
		fmt.Printf("Added replica key '%s' to key-value map\n", replicaKey)
	}

	// Debug: Verify key-value pairs after operation
	fmt.Printf("Key-value pairs after PUT operation: %d\n", len(simulation.KeyValuePairs))
	for k, v := range simulation.KeyValuePairs {
		fmt.Printf("  - Map key '%s': Key='%s', Primary=%v, Node=%s\n",
			k, v.Key, v.IsPrimaryCopy, v.NodeID)
	}
	fmt.Printf("========== END PUT OPERATION ==========\n\n")

	c.JSON(http.StatusOK, gin.H{
		"key":               key,
		"value":             request.Value,
		"node":              string(responsibleNode.ID),
		"path":              path,
		"encrypted":         request.Encrypt,
		"replicaNodes":      replicaNodes,
		"replicationFactor": simulation.ReplicationFactor,
	})
}

// getKeyValue retrieves a key-value pair from the DHT
func getKeyValue(c *gin.Context) {
	key := c.Param("key")

	simulation.mu.RLock()
	defer simulation.mu.RUnlock()

	kvInfo, exists := simulation.KeyValuePairs[key]
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
		return
	}

	// Simulate routing
	path := simulateRouting("GET", key, kvInfo.NodeID)

	c.JSON(http.StatusOK, gin.H{
		"key":       key,
		"value":     kvInfo.Value,
		"node":      string(kvInfo.NodeID),
		"path":      path,
		"encrypted": kvInfo.Encrypted,
	})
}

// deleteKeyValue removes a key-value pair from the DHT
func deleteKeyValue(c *gin.Context) {
	key := c.Param("key")

	fmt.Printf("\n========== DELETE OPERATION ==========\n")
	fmt.Printf("Deleting key: '%s'\n", key)
	fmt.Printf("Current state: %d nodes, %d key-value pairs\n",
		len(simulation.Nodes), len(simulation.KeyValuePairs))

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// First, find the primary copy of the key
	var primaryKV KeyValueInfo
	var primaryExists bool
	var originalKey string

	// Check if the provided key is directly a primary key
	if kv, exists := simulation.KeyValuePairs[key]; exists && kv.IsPrimaryCopy {
		primaryKV = kv
		primaryExists = true
		originalKey = key
		fmt.Printf("Found primary key at '%s'\n", key)
	} else {
		// The provided key might be a replica key ID or the logical key name of a replica
		// We need to find the primary copy by checking all keys
		for k, kv := range simulation.KeyValuePairs {
			if kv.IsPrimaryCopy && (kv.Key == key || k == key) {
				primaryKV = kv
				primaryExists = true
				originalKey = k
				fmt.Printf("Found primary key at '%s' for key name '%s'\n", k, key)
				break
			}
		}
	}

	if !primaryExists {
		errorMsg := fmt.Sprintf("Key '%s' not found (neither as primary nor referenced by replica)", key)
		fmt.Printf("ERROR: %s\n", errorMsg)
		fmt.Printf("========== END DELETE OPERATION (FAILED) ==========\n\n")
		c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
		return
	}

	// Now we have the primary key, get its replica nodes list
	replicaNodeIDs := primaryKV.ReplicaNodes
	fmt.Printf("Primary key '%s' has %d replicas\n", originalKey, len(replicaNodeIDs))

	// Simulate routing to the primary node
	path := simulateRouting("DELETE", originalKey, primaryKV.NodeID)

	// Delete from the primary node
	primaryNode := simulation.Nodes[primaryKV.NodeID]
	if primaryNode != nil {
		primaryNode.Delete(primaryKV.Key)
		fmt.Printf("Deleted key '%s' from primary node %s\n", primaryKV.Key, primaryKV.NodeID)
	}

	// Delete from all replica nodes
	for _, replicaNodeID := range replicaNodeIDs {
		replicaNode := simulation.Nodes[replicaNodeID]
		if replicaNode != nil {
			replicaNode.Delete(primaryKV.Key)
			fmt.Printf("Deleted key '%s' from replica node %s\n", primaryKV.Key, replicaNodeID)
		}
	}

	// Find and delete all entries in KeyValuePairs related to this key
	keysToDelete := []string{}
	for k, kv := range simulation.KeyValuePairs {
		if kv.Key == primaryKV.Key {
			keysToDelete = append(keysToDelete, k)
		}
	}

	for _, k := range keysToDelete {
		delete(simulation.KeyValuePairs, k)
		fmt.Printf("Deleted entry '%s' from KeyValuePairs map\n", k)
	}

	fmt.Printf("Total of %d entries deleted\n", len(keysToDelete))
	fmt.Printf("========== END DELETE OPERATION (SUCCESS) ==========\n\n")

	c.JSON(http.StatusOK, gin.H{
		"key":           primaryKV.Key,
		"node":          string(primaryKV.NodeID),
		"path":          path,
		"replicasCount": len(replicaNodeIDs),
		"totalDeleted":  len(keysToDelete),
	})
}

// resetSimulation resets the simulation to its initial state
func resetSimulation(c *gin.Context) {
	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Clear all routing paths explicitly before initializing
	simulation.RoutingPaths = make([]RoutingPath, 0)

	// Initialize the simulation
	initSimulation()

	c.JSON(http.StatusOK, gin.H{"message": "Simulation reset"})
}

// updateNeighbors updates the neighbor relationships between nodes
func updateNeighbors() {
	// Reset all neighbor relationships
	for _, n := range simulation.Nodes {
		n.Neighbors = make(map[node.NodeID]*node.NeighborInfo)
	}

	// Recalculate neighbors
	for id1, n1 := range simulation.Nodes {
		for id2, n2 := range simulation.Nodes {
			if id1 == id2 {
				continue
			}

			if n1.IsNeighborZone(n2.Zone) {
				n1.AddNeighbor(id2, n2.Address, n2.Zone)
				n2.AddNeighbor(id1, n1.Address, n1.Zone)
			}
		}
	}
}

// redistributeKeyValuePairs redistributes key-value pairs to their responsible nodes
func redistributeKeyValuePairs() {
	// Clear all data from nodes
	for _, n := range simulation.Nodes {
		n.Data = make(map[string]string)
	}

	// Collect primary keys only along with their custom replica configurations
	primaryKeys := make(map[string]KeyValueInfo)
	customReplicatedKeys := make(map[string]bool) // Track which keys have custom replication

	for key, kv := range simulation.KeyValuePairs {
		if kv.IsPrimaryCopy {
			primaryKeys[key] = kv

			// Check if this key has custom replication (more replicas than the default factor would create)
			if len(kv.ReplicaNodes) > (simulation.ReplicationFactor - 1) {
				customReplicatedKeys[key] = true
				fmt.Printf("Found key with custom replication: %s with %d replicas\n",
					key, len(kv.ReplicaNodes))
			}
		}
	}

	// Clear all existing key-value pairs
	simulation.KeyValuePairs = make(map[string]KeyValueInfo)

	// Redistribute key-value pairs (primary and replicas)
	for key, kv := range primaryKeys {
		point := router.HashToPoint(key)

		var responsibleNode *node.Node
		for _, n := range simulation.Nodes {
			if n.Zone.Contains(point) {
				responsibleNode = n
				break
			}
		}

		if responsibleNode != nil {
			replicaNodes := []node.NodeID{}

			// Handle replication differently based on whether this is a custom replicated key
			if customReplicatedKeys[key] {
				// For custom replicated keys, try to preserve the replication pattern
				// Get all available nodes except the responsible node
				availableNodes := []node.NodeID{}
				for nodeID := range simulation.Nodes {
					if nodeID != responsibleNode.ID {
						availableNodes = append(availableNodes, nodeID)
					}
				}

				// Shuffle the available nodes for randomized selection
				rand.Shuffle(len(availableNodes), func(i, j int) {
					availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
				})

				// Determine how many replicas to create
				// Take the original number of replicas, but limit to available nodes
				originalReplicaCount := len(kv.ReplicaNodes)
				replicasNeeded := min(originalReplicaCount, len(availableNodes))

				fmt.Printf("Redistributing custom replicated key %s with %d replicas\n",
					key, replicasNeeded)

				// Create replicas on the selected nodes
				for i := 0; i < replicasNeeded; i++ {
					replicaNodeID := availableNodes[i]
					replicaNode := simulation.Nodes[replicaNodeID]

					// Add to replica nodes list
					replicaNodes = append(replicaNodes, replicaNodeID)

					// Store on the replica node
					replicaNode.Put(key, kv.Value)
				}
			} else {
				// Standard replication based on replication factor
				if simulation.ReplicationFactor > 1 {
					for neighborID := range responsibleNode.Neighbors {
						neighbor := simulation.Nodes[neighborID]
						if neighbor == nil {
							continue
						}

						// Add this neighbor to replica nodes
						replicaNodes = append(replicaNodes, neighborID)

						// Store on the neighbor node
						neighbor.Put(key, kv.Value)

						// Limit replicas based on replication factor
						if len(replicaNodes) >= simulation.ReplicationFactor-1 {
							break
						}
					}
				}
			}

			// Update the primary copy
			primaryKV := KeyValueInfo{
				Key:           key,
				Value:         kv.Value,
				Point:         point,
				NodeID:        responsibleNode.ID,
				Color:         kv.Color,
				Encrypted:     kv.Encrypted,
				ReplicaNodes:  replicaNodes,
				IsPrimaryCopy: true,
			}

			// Store in the node and simulation
			responsibleNode.Put(key, kv.Value)
			simulation.KeyValuePairs[key] = primaryKV

			// Add replica entries
			for i, replicaNodeID := range replicaNodes {
				replicaNode := simulation.Nodes[replicaNodeID]

				// Get center point of the replica's node zone
				replicaZone := replicaNode.Zone
				replicaPoint := node.Point{
					(replicaZone.MinPoint[0] + replicaZone.MaxPoint[0]) / 2,
					(replicaZone.MinPoint[1] + replicaZone.MaxPoint[1]) / 2,
				}

				replicaKey := fmt.Sprintf("%s_replica_%d", key, i+1)
				replicaKV := KeyValueInfo{
					Key:           key,
					Value:         kv.Value,
					Point:         replicaPoint,
					NodeID:        replicaNodeID,
					Color:         kv.Color,
					Encrypted:     kv.Encrypted,
					ReplicaNodes:  nil,
					IsPrimaryCopy: false,
				}
				simulation.KeyValuePairs[replicaKey] = replicaKV
			}
		}
	}
}

// Helper function to get minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// simulateRouting simulates the routing process and returns the path
func simulateRouting(requestType, key string, targetNodeID node.NodeID) []node.NodeID {
	// Choose a random starting node
	var nodes []node.NodeID
	for id := range simulation.Nodes {
		nodes = append(nodes, id)
	}

	if len(nodes) == 0 {
		return []node.NodeID{}
	}

	startNodeID := nodes[rand.Intn(len(nodes))]

	// If the start node is the target, just return it
	if startNodeID == targetNodeID {
		return []node.NodeID{startNodeID}
	}

	// Create a path
	path := []node.NodeID{startNodeID}

	// Get the target point
	point := router.HashToPoint(key)

	// Simulate greedy routing
	currentNodeID := startNodeID
	for currentNodeID != targetNodeID {
		currentNode := simulation.Nodes[currentNodeID]

		// Find the neighbor closest to the target
		var nextNodeID node.NodeID
		minDistance := float64(10) // Any value larger than the maximum possible distance in our coordinate space

		for nID, neighbor := range currentNode.Neighbors {
			// Skip if we've already visited this node
			if containsNodeID(path, nID) {
				continue
			}

			neighborNode := simulation.Nodes[nID]
			if neighborNode == nil {
				continue
			}

			dist, _ := node.DistanceToZone(point, neighbor.Zone)
			if dist < minDistance {
				minDistance = dist
				nextNodeID = nID
			}
		}

		// If we can't find a better neighbor, break (this shouldn't happen in a proper CAN)
		if nextNodeID == "" || len(path) > 10 { // Prevent infinite loops
			break
		}

		// Add the next node to the path
		path = append(path, nextNodeID)
		currentNodeID = nextNodeID
	}

	// Record the routing path
	routingPath := RoutingPath{
		RequestType: requestType,
		Key:         key,
		Path:        path,
		Active:      true,
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(5 * time.Second), // Path will be active for 5 seconds
	}

	simulation.RoutingPaths = append(simulation.RoutingPaths, routingPath)

	// Start a goroutine to deactivate the path after its end time
	go func(index int) {
		time.Sleep(5 * time.Second)
		simulation.mu.Lock()
		defer simulation.mu.Unlock()

		if index < len(simulation.RoutingPaths) {
			simulation.RoutingPaths[index].Active = false
		}
	}(len(simulation.RoutingPaths) - 1)

	return path
}

// containsNodeID checks if a node ID is in a slice
func containsNodeID(slice []node.NodeID, id node.NodeID) bool {
	for _, item := range slice {
		if item == id {
			return true
		}
	}
	return false
}

// Add a new API endpoint to set replication factor
func setReplicationFactor(c *gin.Context) {
	var request struct {
		Factor int `json:"factor"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate factor
	if request.Factor < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Replication factor must be at least 1"})
		return
	}

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Set the new factor
	simulation.ReplicationFactor = request.Factor

	// Redistribute key-value pairs with new replication factor
	redistributeKeyValuePairs()

	c.JSON(http.StatusOK, gin.H{
		"replicationFactor": simulation.ReplicationFactor,
		"message":           fmt.Sprintf("Replication factor set to %d", request.Factor),
	})
}

// replicateKey handles replications of a specific key
func replicateKey(c *gin.Context) {
	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	var requestData struct {
		Key      string `json:"key"`
		Replicas int    `json:"replicas"`
	}

	if err := c.ShouldBindJSON(&requestData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	keyToReplicate := requestData.Key
	replicasToCreate := requestData.Replicas

	if replicasToCreate <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Number of replicas must be positive"})
		return
	}

	fmt.Printf("\n========== REPLICATION REQUEST ==========\n")
	fmt.Printf("Request to replicate key '%s' with %d copies\n", keyToReplicate, replicasToCreate)
	fmt.Printf("Current system state: %d nodes, %d key-value pairs in map\n",
		len(simulation.Nodes), len(simulation.KeyValuePairs))

	// Find the primary key entry and its owner node
	fmt.Printf("Looking for primary key entry for '%s'...\n", keyToReplicate)
	var primaryKey KeyValueInfo
	var primaryKeyMapID string
	var foundPrimary bool

	// Look through all key-value pairs to find the primary copy
	for k, kv := range simulation.KeyValuePairs {
		if kv.Key == keyToReplicate && kv.IsPrimaryCopy {
			primaryKey = kv
			primaryKeyMapID = k
			foundPrimary = true
			fmt.Printf("Found primary key '%s' at map position '%s'\n", keyToReplicate, k)
			fmt.Printf("  - Node: %s, Point: %v, Value: '%s'\n", kv.NodeID, kv.Point, kv.Value)
			break
		}
	}

	if !foundPrimary {
		errorMsg := fmt.Sprintf("Primary key '%s' not found in visualization", keyToReplicate)
		fmt.Printf("ERROR: %s\n", errorMsg)
		fmt.Printf("Available keys in map (%d total):\n", len(simulation.KeyValuePairs))
		for k, kv := range simulation.KeyValuePairs {
			fmt.Printf("  - Map key '%s': Key='%s', Primary=%v, Node=%s\n",
				k, kv.Key, kv.IsPrimaryCopy, kv.NodeID)
		}
		fmt.Printf("========== END REPLICATION REQUEST (FAILED) ==========\n\n")
		c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
		return
	}

	// Get the actual node to ensure it exists
	primaryNode, exists := simulation.Nodes[node.NodeID(primaryKey.NodeID)]
	if !exists {
		errorMsg := fmt.Sprintf("Primary node '%s' for key '%s' doesn't exist", primaryKey.NodeID, keyToReplicate)
		fmt.Printf("ERROR: %s\n", errorMsg)
		fmt.Printf("========== END REPLICATION REQUEST (FAILED) ==========\n\n")
		c.JSON(http.StatusInternalServerError, gin.H{"error": errorMsg})
		return
	}

	// Check if the key exists in the node's data
	val, exists := primaryNode.Data[keyToReplicate]
	if !exists {
		errorMsg := fmt.Sprintf("Key '%s' not found in node '%s' data", keyToReplicate, primaryKey.NodeID)
		fmt.Printf("ERROR: %s\n", errorMsg)
		fmt.Printf("Node '%s' has %d keys:\n", primaryKey.NodeID, len(primaryNode.Data))
		for k, v := range primaryNode.Data {
			fmt.Printf("  - '%s' = '%s'\n", k, v)
		}
		fmt.Printf("========== END REPLICATION REQUEST (FAILED) ==========\n\n")
		c.JSON(http.StatusNotFound, gin.H{"error": "Key not found in node"})
		return
	}

	fmt.Printf("Key '%s' found in node '%s' with value: '%s'\n", keyToReplicate, primaryKey.NodeID, val)

	// Step 1: Remove any existing replicas for this key
	// Keep track of existing replicas to remove them
	keysToRemove := []string{}
	for k, kv := range simulation.KeyValuePairs {
		if kv.Key == keyToReplicate && !kv.IsPrimaryCopy {
			keysToRemove = append(keysToRemove, k)
			// Also remove from node's data
			if replicaNode, ok := simulation.Nodes[kv.NodeID]; ok {
				delete(replicaNode.Data, keyToReplicate)
				fmt.Printf("Removed existing replica of '%s' from node '%s'\n", keyToReplicate, kv.NodeID)
			}
		}
	}

	// Now remove from the simulation's key-value map
	for _, k := range keysToRemove {
		delete(simulation.KeyValuePairs, k)
		fmt.Printf("Removed existing replica entry '%s' from key-value map\n", k)
	}

	// Step 2: Find all nodes that don't have the primary key
	availableNodes := []node.NodeID{}
	for nodeID := range simulation.Nodes {
		if nodeID != node.NodeID(primaryKey.NodeID) {
			availableNodes = append(availableNodes, nodeID)
		}
	}

	fmt.Printf("Found %d nodes available for replication (excluding primary node)\n", len(availableNodes))

	// Step 3: Determine how to distribute the replicas
	physicalReplicas := replicasToCreate
	virtualReplicas := 0

	if physicalReplicas > len(availableNodes) {
		// We need some virtual replicas
		virtualReplicas = physicalReplicas - len(availableNodes)
		physicalReplicas = len(availableNodes)
		fmt.Printf("Will create %d physical replicas and %d virtual replicas\n",
			physicalReplicas, virtualReplicas)
	}

	// Keep track of all replica nodes for the primary key
	allReplicaNodeIDs := []node.NodeID{}

	// Step 4: Create physical replicas on other nodes
	if physicalReplicas > 0 {
		// Shuffle the available nodes to randomize distribution
		rand.Shuffle(len(availableNodes), func(i, j int) {
			availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
		})

		fmt.Printf("Creating %d physical replicas on separate nodes:\n", physicalReplicas)

		for i := 0; i < physicalReplicas; i++ {
			targetNodeID := availableNodes[i]
			targetNode := simulation.Nodes[targetNodeID]

			// Add the key-value to the target node
			targetNode.Data[keyToReplicate] = val

			// Get center point of the replica's node zone
			replicaZone := targetNode.Zone
			replicaPoint := node.Point{
				(replicaZone.MinPoint[0] + replicaZone.MaxPoint[0]) / 2,
				(replicaZone.MinPoint[1] + replicaZone.MaxPoint[1]) / 2,
			}

			// Create a unique map key for this replica
			replicaMapKey := fmt.Sprintf("%s_replica_%d", keyToReplicate, i+1)

			// Create the replica entry
			replicaInfo := KeyValueInfo{
				Key:           keyToReplicate,
				Value:         val,
				Point:         replicaPoint,
				NodeID:        targetNodeID,
				Color:         primaryKey.Color,
				Encrypted:     primaryKey.Encrypted,
				ReplicaNodes:  nil, // Replicas don't have their own replicas
				IsPrimaryCopy: false,
			}

			// Add to the KeyValuePairs map
			simulation.KeyValuePairs[replicaMapKey] = replicaInfo
			fmt.Printf("  - Created physical replica on node '%s' at center point %v\n",
				targetNodeID, replicaPoint)

			// Add to our list of replica nodes
			allReplicaNodeIDs = append(allReplicaNodeIDs, targetNodeID)
		}
	}

	// Step 5: Create virtual replicas on the primary node if needed
	if virtualReplicas > 0 {
		fmt.Printf("Creating %d virtual replicas on primary node:\n", virtualReplicas)

		// Calculate the radius for the virtual replicas layout
		// We'll place them in a circle around the primary key
		radius := 0.05 // Adjust this value to control the spread
		primaryPoint := primaryKey.Point

		for i := 0; i < virtualReplicas; i++ {
			// Calculate position around the primary key in a circle
			angle := (2 * math.Pi * float64(i)) / float64(virtualReplicas)
			offsetX := radius * math.Cos(angle)
			offsetY := radius * math.Sin(angle)

			virtualPoint := node.Point{
				primaryPoint[0] + offsetX,
				primaryPoint[1] + offsetY,
			}

			// Ensure point stays within bounds
			virtualPoint[0] = math.Max(0.01, math.Min(0.99, virtualPoint[0]))
			virtualPoint[1] = math.Max(0.01, math.Min(0.99, virtualPoint[1]))

			// Create a unique map key for this virtual replica
			virtualReplicaMapKey := fmt.Sprintf("%s_virtual_replica_%d", keyToReplicate, i+1)

			// Create the virtual replica entry
			virtualReplicaInfo := KeyValueInfo{
				Key:           keyToReplicate,
				Value:         val,
				Point:         virtualPoint,
				NodeID:        node.NodeID(primaryKey.NodeID), // Same as primary
				Color:         primaryKey.Color,
				Encrypted:     primaryKey.Encrypted,
				ReplicaNodes:  nil,
				IsPrimaryCopy: false, // It's still a replica, just virtual
			}

			// Add to the KeyValuePairs map
			simulation.KeyValuePairs[virtualReplicaMapKey] = virtualReplicaInfo
			fmt.Printf("  - Created virtual replica on primary node at offset point %v\n", virtualPoint)

			// Primary node already has the key, so we don't need to add it again
			// But we count the primary node as a replica for tracking purposes
			allReplicaNodeIDs = append(allReplicaNodeIDs, node.NodeID(primaryKey.NodeID))
		}
	}

	// Step 6: Update the primary key's replica nodes list
	primaryKey.ReplicaNodes = allReplicaNodeIDs
	simulation.KeyValuePairs[primaryKeyMapID] = primaryKey

	fmt.Printf("Updated primary key entry with %d replica nodes\n", len(allReplicaNodeIDs))
	fmt.Printf("Total entries in KeyValuePairs map: %d\n", len(simulation.KeyValuePairs))
	fmt.Printf("========== END REPLICATION REQUEST (SUCCESS) ==========\n\n")

	// Convert node IDs to strings for JSON response
	replicaNodeStrings := make([]string, len(allReplicaNodeIDs))
	for i, nodeID := range allReplicaNodeIDs {
		replicaNodeStrings[i] = string(nodeID)
	}

	// Prepare response - make sure key is exactly "replicaNodes" as expected by frontend
	// Avoid using gin.H here to ensure the exact key name is preserved
	var responseData struct {
		Message      string   `json:"message"`
		ReplicaNodes []string `json:"replicaNodes"` // Exact match with frontend expectation
	}
	responseData.Message = fmt.Sprintf("Replicated key '%s' with %d physical and %d virtual replicas",
		keyToReplicate, physicalReplicas, virtualReplicas)
	responseData.ReplicaNodes = replicaNodeStrings

	// Debug log
	fmt.Printf("Sending response: %+v\n", responseData)
	fmt.Printf("replicaNodes type: %T, length: %d\n", responseData.ReplicaNodes, len(replicaNodeStrings))
	fmt.Printf("JSON marshalled: %s\n", func() string {
		bytes, _ := json.Marshal(responseData)
		return string(bytes)
	}())

	c.JSON(http.StatusOK, responseData)
}
