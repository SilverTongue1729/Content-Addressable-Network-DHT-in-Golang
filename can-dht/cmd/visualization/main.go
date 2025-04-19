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

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/routing"
	"github.com/can-dht/pkg/service"
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
	FailedNodes       map[node.NodeID]bool  // Track which nodes have failed
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
	Owner         string        // Owner of the data (for access control)
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
	Owner         string      `json:"Owner"`
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
	FailedNodes:       make(map[node.NodeID]bool),  // Track failed nodes
}

// Colors for key-value pairs
var colors = []string{"#ff5733", "#33ff57", "#3357ff", "#f3ff33", "#ff33f3", "#33fff3"}

// Router instance
var router = routing.NewRouter(2) // 2D for visualization

// Authentication manager for access control
var authManager *crypto.EnhancedAuthManager

func main() {
	// Initialize the authentication manager
	authManager = crypto.NewEnhancedAuthManager()

	// Register a default admin user
	err := authManager.RegisterUser("admin", "admin123")
	if err != nil {
		fmt.Printf("Error registering admin user: %v\n", err)
	} else {
		fmt.Printf("Created admin user: admin/admin123\n")
	}

	// Register a regular user
	err = authManager.RegisterUser("user", "user123")
	if err != nil {
		fmt.Printf("Error registering regular user: %v\n", err)
	} else {
		fmt.Printf("Created regular user: user/user123\n")
	}

	// Initialize the simulation with a root node
	initSimulation()

	// Create Gin router
	r := gin.Default()

	// Configure CORS with more permissive settings
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "DELETE", "PUT", "PATCH", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length", "Content-Type"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Serve static files
	r.Static("/static", "./cmd/visualization/static")
	r.StaticFile("/", "./cmd/visualization/static/index.html")

	// Create authentication middleware
	authMiddleware := service.NewAuthMiddleware(authManager)

	// API Routes
	api := r.Group("/api")
	{
		// Public endpoints
		api.GET("/state", getSimulationState)
		
		// Endpoints that require authentication
		secureAPI := api.Group("/")
		secureAPI.Use(authMiddleware.Authenticate())
		{
			secureAPI.POST("/node/add", addNode)
			secureAPI.POST("/node/remove/:id", removeNode)
			secureAPI.POST("/replication/factor", setReplicationFactor)
			secureAPI.POST("/replication/replicate", replicateKey)
			secureAPI.POST("/node/fail/:id", failNode)
			secureAPI.POST("/node/recover/:id", recoverNode)
			
			// Simple reset endpoint (admin only)
			adminAPI := secureAPI.Group("/admin")
			adminAPI.Use(authMiddleware.AdminOnly())
			{
				adminAPI.POST("/reset", resetSimulation)
			}
		}
	}
	
	// Secure Key-Value operations with authentication
	kvAPI := r.Group("/kv")
	kvAPI.Use(authMiddleware.Authenticate())
	{
		// User's own data operations
		kvAPI.POST("/put", secureKeyValuePut)
		kvAPI.GET("/get/:key", secureKeyValueGet)
		kvAPI.DELETE("/delete/:key", secureKeyValueDelete)
		
		// Shared data operations (with permission checks)
		sharedAPI := kvAPI.Group("/shared")
		{
			sharedAPI.POST("/:owner/put", sharedKeyValuePut)
			sharedAPI.GET("/:owner/get/:key", sharedKeyValueGet)
			sharedAPI.DELETE("/:owner/delete/:key", sharedKeyValueDelete)
		}
		
		// Permission management
		permAPI := kvAPI.Group("/permissions")
		{
			permAPI.POST("/:key/:username", grantPermission)
			permAPI.DELETE("/:key/:username", revokePermission)
		}
	}
	
	// Authentication API
	authAPI := r.Group("/auth")
	{
		// Register endpoint (public)
		authAPI.POST("/register", registerUser)
		
		// Secure auth endpoints
		secureAuthAPI := authAPI.Group("/")
		secureAuthAPI.Use(authMiddleware.Authenticate())
		{
			secureAuthAPI.GET("/me", getUserInfo)
		}
	}

	// Start the server
	fmt.Println("Starting visualization server on http://localhost:8090")
	
	// Register the authentication API endpoints
	if authManager != nil {
		fmt.Println("Authentication is enabled - registering auth endpoints")
		// Create auth middleware
		authMiddleware := service.NewAuthMiddleware(authManager)
		
		// Register auth API routes
		authAPI := r.Group("/auth")
		{
			// Register endpoint (public)
			authAPI.POST("/register", registerUser)
			
			// Secure auth endpoints
			secureAuthAPI := authAPI.Group("/")
			secureAuthAPI.Use(authMiddleware.Authenticate())
			{
				secureAuthAPI.GET("/me", getUserInfo)
			}
		}
		
		fmt.Println("Authentication API endpoints registered")
	} else {
		fmt.Println("WARNING: Authentication is disabled")
	}
	
	r.Run(":8090")
}

// registerUser handles user registration requests
func registerUser(c *gin.Context) {
	// Check if authentication is enabled
	if authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "authentication is not enabled",
		})
		return
	}

	// Parse request body
	var req struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// Register the user
	err := authManager.RegisterUser(req.Username, req.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "user registered successfully",
		"username": req.Username,
	})
}

// getUserInfo returns information about the current user
func getUserInfo(c *gin.Context) {
	username, exists := c.Get("username")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "not authenticated",
		})
		return
	}

	// In a real implementation, we might fetch more user information here
	c.JSON(http.StatusOK, gin.H{
		"username": username,
		"isAdmin": username == "admin", // Simple check for now
	})
}

// secureKeyValuePut handles putting a key-value pair with authentication
func secureKeyValuePut(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)
	
	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()
	
	var req struct {
		Key   string `json:"key" binding:"required"`
		Value string `json:"value" binding:"required"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}
	
	// Check if user can access the data
	err := authManager.CreateOwnData(usernameStr, password, req.Key, []byte(req.Value))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to create data: " + err.Error(),
		})
		return
	}
	
	// Add to the simulation
	simulation.mu.Lock()
	defer simulation.mu.Unlock()
	
	// Find the point for this key
	point := router.Hash(req.Key)
	
	// Find the node responsible for this point
	nodeID := findNodeForPoint(point)
	
	// Generate a color for this key
	colorIndex := len(simulation.KeyValuePairs) % len(colors)
	color := colors[colorIndex]
	
	// Store the key-value pair
	kvInfo := KeyValueInfo{
		Key:           req.Key,
		Value:         req.Value,
		Point:         point,
		NodeID:        nodeID,
		Color:         color,
		Encrypted:     true,
		ReplicaNodes:  []node.NodeID{}, // Start with no replicas
		IsPrimaryCopy: true,
		Owner:         usernameStr,
	}
	
	// Add to simulation state and target node
	simulation.KeyValuePairs[req.Key] = kvInfo
	if node, exists := simulation.Nodes[nodeID]; exists {
		node.Data[req.Key] = req.Value
	}
	
	// Add routing path animation
	path := simulateRouting("PUT", req.Key, nodeID)
	routingPath := RoutingPath{
		RequestType: "PUT",
		Key:         req.Key,
		Path:        path,
		Active:      true,
		StartTime:   time.Now(),
	}
	simulation.RoutingPaths = append(simulation.RoutingPaths, routingPath)
	
	// Auto-replicate if needed
	if simulation.ReplicationFactor > 1 {
		performReplication(req.Key, usernameStr)
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Key-value pair stored securely",
		"key":     req.Key,
		"nodeID":  nodeID,
	})
}

// secureKeyValueGet handles getting a key-value pair with authentication
func secureKeyValueGet(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)
	
	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()
	
	key := c.Param("key")
	
	// Check if the key exists in our simulation
	simulation.mu.RLock()
	kvInfo, exists := simulation.KeyValuePairs[key]
	simulation.mu.RUnlock()
	
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Key not found",
		})
		return
	}
	
	// Check if user owns the data or has permission
	if kvInfo.Owner != usernameStr {
		// If not the owner, check permissions
		permission, err := authManager.GetPermission(usernameStr, password, kvInfo.Owner, key)
		if err != nil || permission&crypto.PermissionRead == 0 {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "You don't have permission to access this data",
			})
			return
		}
	}
	
	// Get the data
	value, err := authManager.ReadOwnData(usernameStr, password, key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to read data: " + err.Error(),
		})
		return
	}
	
	// Add routing path animation
	simulation.mu.Lock()
	path := simulateRouting("GET", key, kvInfo.NodeID)
	routingPath := RoutingPath{
		RequestType: "GET",
		Key:         key,
		Path:        path,
		Active:      true,
		StartTime:   time.Now(),
	}
	simulation.RoutingPaths = append(simulation.RoutingPaths, routingPath)
	simulation.mu.Unlock()
	
	c.JSON(http.StatusOK, gin.H{
		"key":     key,
		"value":   string(value),
		"nodeID":  kvInfo.NodeID,
		"owner":   kvInfo.Owner,
	})
}

// secureKeyValueDelete handles deleting a key-value pair with authentication
func secureKeyValueDelete(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)
	
	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()
	
	key := c.Param("key")
	
	// Check if the key exists in our simulation
	simulation.mu.RLock()
	kvInfo, exists := simulation.KeyValuePairs[key]
	simulation.mu.RUnlock()
	
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Key not found",
		})
		return
	}
	
	// Check if user owns the data or has permission
	if kvInfo.Owner != usernameStr {
		// If not the owner, check permissions
		permission, err := authManager.GetPermission(usernameStr, password, kvInfo.Owner, key)
		if err != nil || permission&crypto.PermissionDelete == 0 {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "You don't have permission to delete this data",
			})
			return
		}
	}
	
	// Delete the data
	err := authManager.DeleteOwnData(usernameStr, password, key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to delete data: " + err.Error(),
		})
		return
	}
	
	// Remove from simulation
	simulation.mu.Lock()
	delete(simulation.KeyValuePairs, key)
	if node, exists := simulation.Nodes[kvInfo.NodeID]; exists {
		delete(node.Data, key)
	}
	
	// Remove from replica nodes too
	for _, replicaNodeID := range kvInfo.ReplicaNodes {
		if replicaNode, exists := simulation.Nodes[replicaNodeID]; exists {
			delete(replicaNode.Data, key)
		}
	}
	
	// Add routing path animation
	path := simulateRouting("DELETE", key, kvInfo.NodeID)
	routingPath := RoutingPath{
		RequestType: "DELETE",
		Key:         key,
		Path:        path,
		Active:      true,
		StartTime:   time.Now(),
	}
	simulation.RoutingPaths = append(simulation.RoutingPaths, routingPath)
	simulation.mu.Unlock()
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Key-value pair deleted",
		"key":     key,
	})
}

// sharedKeyValuePut handles putting a shared key-value pair
func sharedKeyValuePut(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "Shared key-value PUT not implemented yet",
	})
}

// sharedKeyValueGet handles getting a shared key-value pair
func sharedKeyValueGet(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)
	
	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()
	
	owner := c.Param("owner")
	key := c.Param("key")
	
	// Get the data
	value, err := authManager.ReadData(usernameStr, password, owner, key)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Failed to read shared data: " + err.Error(),
		})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"key":     key,
		"value":   string(value),
		"owner":   owner,
	})
}

// sharedKeyValueDelete handles deleting a shared key-value pair
func sharedKeyValueDelete(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "Shared key-value DELETE not implemented yet",
	})
}

// grantPermission grants permission to another user for a key
func grantPermission(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)
	
	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()
	
	key := c.Param("key")
	targetUser := c.Param("username")
	
	var req struct {
		Permission uint8 `json:"permission" binding:"required"`
	}
	
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}
	
	// Grant permission
	err := authManager.ModifyPermissions(usernameStr, password, targetUser, usernameStr, key, crypto.Permission(req.Permission))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Permission granted",
		"key":     key,
		"user":    targetUser,
		"permission": req.Permission,
	})
}

// revokePermission revokes permission from another user for a key
func revokePermission(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)
	
	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()
	
	key := c.Param("key")
	targetUser := c.Param("username")
	
	// Revoke by setting to PermissionNone
	err := authManager.ModifyPermissions(usernameStr, password, targetUser, usernameStr, key, crypto.PermissionNone)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Permission revoked",
		"key":     key,
		"user":    targetUser,
	})
}

// findNodeForPoint finds the node responsible for a point
func findNodeForPoint(point node.Point) node.NodeID {
	for id, n := range simulation.Nodes {
		if !simulation.FailedNodes[id] && n.Zone.Contains(point) {
			return id
		}
	}
	// Default to root if no node is found
	return "root"
}

// performReplication handles replicating a key to multiple nodes
func performReplication(key string, owner string) {
	kvInfo, exists := simulation.KeyValuePairs[key]
	if !exists {
		return
	}
	
	primaryNodeID := kvInfo.NodeID
	
	// Get nodes for replication (excluding primary)
	replicaNodes := selectNodesForReplication(primaryNodeID, simulation.ReplicationFactor-1)
	
	// Update replica nodes in key info
	kvInfo.ReplicaNodes = replicaNodes
	simulation.KeyValuePairs[key] = kvInfo
	
	// Add replicas to nodes
	for _, replicaNodeID := range replicaNodes {
		if node, exists := simulation.Nodes[replicaNodeID]; exists {
			// Create replica in node's data store
			node.Data[key] = kvInfo.Value
			
			// Create replica entry in key-value pairs
			replicaKey := fmt.Sprintf("%s:replica:%s", key, replicaNodeID)
			simulation.KeyValuePairs[replicaKey] = KeyValueInfo{
				Key:           key,
				Value:         kvInfo.Value,
				Point:         kvInfo.Point, // Same point
				NodeID:        replicaNodeID,
				Color:         kvInfo.Color, // Same color
				Encrypted:     true,
				ReplicaNodes:  []node.NodeID{},
				IsPrimaryCopy: false,
				Owner:         owner,
			}
		}
	}
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
	simulation.FailedNodes = make(map[node.NodeID]bool)     // Clear failed nodes

	// Create the coordinate space for the root node
	minPoint := node.Point{0.0, 0.0}
	maxPoint := node.Point{1.0, 1.0}
	zone, _ := node.NewZone(minPoint, maxPoint)

	// Create the root node
	rootNode := node.NewNode("root", "localhost:9000", zone, 2)
	simulation.Nodes[rootNode.ID] = rootNode

	fmt.Printf("Simulation initialized with root node\n")
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
		Failed    bool       `json:"failed"` // Add failed status
	}

	type ResponseData struct {
		Nodes             []NodeData    `json:"nodes"`
		KeyValuePairs     []KVResponse  `json:"keyValuePairs"`
		RoutingPaths      []RoutingPath `json:"routingPaths"`
		ReplicationFactor int           `json:"replicationFactor"`
		FailedNodes       []string      `json:"failedNodes"` // Add list of failed nodes
	}

	// Prepare response
	response := ResponseData{
		Nodes:             make([]NodeData, 0, len(simulation.Nodes)),
		KeyValuePairs:     make([]KVResponse, 0, len(simulation.KeyValuePairs)),
		RoutingPaths:      simulation.RoutingPaths,
		ReplicationFactor: simulation.ReplicationFactor,
		FailedNodes:       make([]string, 0, len(simulation.FailedNodes)),
	}

	// Prepare nodes data
	for id, n := range simulation.Nodes {
		// Get neighbors as strings (for JSON)
		neighborIDs := make([]string, 0, len(n.Neighbors))
		for nID := range n.Neighbors {
			neighborIDs = append(neighborIDs, string(nID))
		}

		// Add the node to the response
		response.Nodes = append(response.Nodes, NodeData{
			ID:        string(id),
			Address:   n.Address,
			Zone:      n.Zone,
			Neighbors: neighborIDs,
			Failed:    simulation.FailedNodes[id], // Set failed status
		})
	}

	// Add failed nodes list
	for id := range simulation.FailedNodes {
		response.FailedNodes = append(response.FailedNodes, string(id))
	}

	// Prepare key-value pairs data
	for _, kv := range simulation.KeyValuePairs {
		// Convert NodeID slices to string slices for JSON
		replicaNodes := make([]string, len(kv.ReplicaNodes))
		for i, id := range kv.ReplicaNodes {
			replicaNodes[i] = string(id)
		}

		// Add the key-value pair to the response
		response.KeyValuePairs = append(response.KeyValuePairs, KVResponse{
			Key:           kv.Key,
			Value:         kv.Value,
			Point:         kv.Point,
			NodeID:        kv.NodeID,
			Color:         kv.Color,
			Encrypted:     kv.Encrypted,
			ReplicaNodes:  replicaNodes,
			IsPrimaryCopy: kv.IsPrimaryCopy,
			Owner:         kv.Owner,
		})
	}

	// Return the response
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
					Owner:         "",
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
					Owner:         "",
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
		Owner:         "",
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
			Owner:         "",
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

	// Debug: Print all entries for the specified key
	fmt.Printf("\n===== GET KEY REQUEST FOR '%s' =====\n", key)

	// Find the primary entry for the key
	kvInfo, exists := simulation.KeyValuePairs[key]
	if !exists {
		fmt.Printf("Key %s not found\n", key)
		c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
		return
	}

	fmt.Printf("Found primary entry: NodeID=%s, IsFailed=%v\n",
		kvInfo.NodeID, simulation.FailedNodes[kvInfo.NodeID])

	// First check: is the primary node failed?
	if simulation.FailedNodes[kvInfo.NodeID] {
		fmt.Printf("Node %s holding key %s has failed\n", kvInfo.NodeID, key)

		// Check for proper replicas (those listed in ReplicaNodes)
		fmt.Printf("Key has %d explicit replicas\n", len(kvInfo.ReplicaNodes))

		if len(kvInfo.ReplicaNodes) == 0 {
			// No replicas configured for this key
			errorMsg := fmt.Sprintf("Key %s is unavailable - node %s has failed and no replicas were created", key, kvInfo.NodeID)
			fmt.Println(errorMsg)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": errorMsg})
			return
		}

		// Try to find a healthy explicit replica
		foundHealthyReplica := false
		var replicaNodeID node.NodeID
		var replicaValue string
		var replicaMapKey string

		// Check all explicit replicas
		for _, nodeID := range kvInfo.ReplicaNodes {
			if !simulation.FailedNodes[nodeID] {
				// Found a healthy replica node
				foundHealthyReplica = true
				replicaNodeID = nodeID

				// Find the replica in the KV store
				for mapKey, replicaInfo := range simulation.KeyValuePairs {
					if replicaInfo.Key == key && !replicaInfo.IsPrimaryCopy && replicaInfo.NodeID == nodeID {
						replicaValue = replicaInfo.Value
						replicaMapKey = mapKey
						fmt.Printf("Found explicit replica on healthy node %s (mapKey: %s)\n",
							nodeID, mapKey)
						break
					}
				}

				// If we found the node ID but not the actual replica in the KV map
				// get the value from the node's data directly
				if replicaValue == "" && simulation.Nodes[nodeID] != nil {
					replicaValue = simulation.Nodes[nodeID].Data[key]
					fmt.Printf("Got replica value directly from node %s\n", nodeID)
				}

				break
			} else {
				fmt.Printf("Replica node %s is also failed\n", nodeID)
			}
		}

		if !foundHealthyReplica {
			// No healthy replica found among the explicit replicas
			errorMsg := fmt.Sprintf("Key %s is unavailable - node %s has failed and all replicas are also on failed nodes", key, kvInfo.NodeID)
			fmt.Println(errorMsg)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": errorMsg})
			return
		}

		// Required but replica value is missing (shouldn't happen)
		if replicaValue == "" {
			errorMsg := fmt.Sprintf("Key %s is unavailable - replica found on node %s but value is missing", key, replicaNodeID)
			fmt.Println(errorMsg)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": errorMsg})
			return
		}

		// Use the replica instead
		// Simulate routing to the replica node
		path := simulateRouting("GET", key, replicaNodeID)

		fmt.Printf("Returning value from replica on node %s\n", replicaNodeID)
		fmt.Printf("===== END GET KEY REQUEST =====\n\n")

		c.JSON(http.StatusOK, gin.H{
			"key":         key,
			"value":       replicaValue,
			"node":        string(replicaNodeID),
			"path":        path,
			"encrypted":   kvInfo.Encrypted,
			"fromReplica": true,
			"replicaKey":  replicaMapKey,
		})
		return
	}

	// The primary node is healthy, proceed normally
	fmt.Printf("Node %s is healthy, proceeding normally\n", kvInfo.NodeID)
	fmt.Printf("===== END GET KEY REQUEST =====\n\n")

	// Simulate routing
	path := simulateRouting("GET", key, kvInfo.NodeID)

	c.JSON(http.StatusOK, gin.H{
		"key":         key,
		"value":       kvInfo.Value,
		"node":        string(kvInfo.NodeID),
		"path":        path,
		"encrypted":   kvInfo.Encrypted,
		"fromReplica": false,
	})
}

// deleteKeyValue removes a key-value pair from the DHT
func deleteKeyValue(c *gin.Context) {
	key := c.Param("key")

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	fmt.Printf("\n===== DELETE KEY REQUEST FOR '%s' =====\n", key)

	// Find the primary key entry
	kvInfo, exists := simulation.KeyValuePairs[key]
	if !exists {
		fmt.Printf("Key %s not found\n", key)
		c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
		return
	}

	fmt.Printf("Found primary entry: NodeID=%s, IsFailed=%v\n",
		kvInfo.NodeID, simulation.FailedNodes[kvInfo.NodeID])

	// Check if the primary node is failed
	var sourceNodeID node.NodeID
	if simulation.FailedNodes[kvInfo.NodeID] {
		fmt.Printf("Primary node %s for key %s is failed\n", kvInfo.NodeID, key)

		// If primary node is failed, we can still delete through any replica
		// This is so that users can clean up data even if the primary node is down
		sourceNodeID = ""

		// Try to find a healthy replica node to simulate the deletion path
		for mapKey, replicaInfo := range simulation.KeyValuePairs {
			if replicaInfo.Key == key && !replicaInfo.IsPrimaryCopy && !simulation.FailedNodes[replicaInfo.NodeID] {
				sourceNodeID = replicaInfo.NodeID
				fmt.Printf("Found healthy replica on node %s for routing deletion (mapKey: %s)\n",
					sourceNodeID, mapKey)
				break
			}
		}

		if sourceNodeID == "" {
			// No healthy replica found, but we'll still delete the data
			// We'll use a random healthy node for the routing path
			for nID, failed := range simulation.FailedNodes {
				if !failed {
					sourceNodeID = nID
					break
				}
			}

			if sourceNodeID == "" {
				// All nodes are failed - this shouldn't happen in practice
				fmt.Printf("WARNING: All nodes appear to be failed!\n")
				// We'll use the primary node anyway for routing simulation
				sourceNodeID = kvInfo.NodeID
			}
		}
	} else {
		// Primary node is healthy, use it as the source
		sourceNodeID = kvInfo.NodeID
	}

	// Simulate routing
	path := simulateRouting("DELETE", key, sourceNodeID)

	// Delete from primary node if it's healthy
	if !simulation.FailedNodes[kvInfo.NodeID] {
	primaryNode := simulation.Nodes[kvInfo.NodeID]
	if primaryNode != nil {
		primaryNode.Delete(key)
			fmt.Printf("Deleted key %s from primary node %s\n", key, kvInfo.NodeID)
		}
	} else {
		fmt.Printf("Primary node %s is failed, key %s will be deleted from state only\n",
			kvInfo.NodeID, key)
	}

	// Delete all replicas
	// First, find all replica entries to delete
	keysToRemove := []string{key} // Start with the primary key
	for mapKey, kv := range simulation.KeyValuePairs {
		if kv.Key == key && !kv.IsPrimaryCopy {
			// This is a replica of our target key
			keysToRemove = append(keysToRemove, mapKey)

			// Delete from the replica node if it's healthy
			if !simulation.FailedNodes[kv.NodeID] {
			if replicaNode := simulation.Nodes[kv.NodeID]; replicaNode != nil {
				replicaNode.Delete(key)
					fmt.Printf("Deleted key %s from replica node %s\n", key, kv.NodeID)
				}
			} else {
				fmt.Printf("Replica node %s is failed, key %s replica will be deleted from state only\n",
					kv.NodeID, key)
			}
		}
	}

	// Delete all entries from simulation state
	for _, mapKey := range keysToRemove {
		delete(simulation.KeyValuePairs, mapKey)
		fmt.Printf("Removed key %s from simulation state\n", mapKey)
	}

	fmt.Printf("===== END DELETE KEY REQUEST =====\n\n")

	c.JSON(http.StatusOK, gin.H{
		"key":  key,
		"node": string(sourceNodeID),
		"path": path,
	})
}

// resetSimulation resets the simulation to its initial state
func resetSimulation(c *gin.Context) {
	// Acquire lock
	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	// Create new empty data structures instead of clearing existing ones
	simulation.Nodes = make(map[node.NodeID]*node.Node)
	simulation.KeyValuePairs = make(map[string]KeyValueInfo)
	simulation.RoutingPaths = make([]RoutingPath, 0)
	simulation.NodeJoinPoints = make(map[string]node.Point)
	simulation.FailedNodes = make(map[node.NodeID]bool)
	simulation.ReplicationFactor = 1 // Reset to default

	// Initialize with root node
	minPoint := node.Point{0.0, 0.0}
	maxPoint := node.Point{1.0, 1.0}
	zone, _ := node.NewZone(minPoint, maxPoint)
	rootNode := node.NewNode("root", "localhost:9000", zone, 2)
	simulation.Nodes[rootNode.ID] = rootNode

	// Simple success response
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Simulation reset successfully",
	})
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
				Owner:         "",
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
					Owner:         "",
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
	fmt.Printf("Simulating %s routing for key %s to target node %s\n", requestType, key, targetNodeID)

	// Choose a random starting node that isn't failed
	var availableNodes []node.NodeID
	for id := range simulation.Nodes {
		if !simulation.FailedNodes[id] {
			availableNodes = append(availableNodes, id)
		}
	}

	if len(availableNodes) == 0 {
		fmt.Printf("No available nodes for routing (all failed)\n")
		return []node.NodeID{}
	}

	startNodeID := availableNodes[rand.Intn(len(availableNodes))]
	fmt.Printf("Selected start node for routing: %s\n", startNodeID)

	// If the start node is the target, just return it
	if startNodeID == targetNodeID {
		fmt.Printf("Start node is target node, no routing needed\n")
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
		fmt.Printf("Current routing node: %s\n", currentNodeID)

		// Find the neighbor closest to the target that isn't failed
		var nextNodeID node.NodeID
		minDistance := float64(10) // Any value larger than the maximum possible distance in our coordinate space

		for nID, neighbor := range currentNode.Neighbors {
			// Skip if we've already visited this node
			if containsNodeID(path, nID) {
				continue
			}

			// Skip failed nodes
			if simulation.FailedNodes[nID] {
				fmt.Printf("  Skipping failed neighbor: %s\n", nID)
				continue
			}

			neighborNode := simulation.Nodes[nID]
			if neighborNode == nil {
				continue
			}

			dist, _ := node.DistanceToZone(point, neighbor.Zone)
			fmt.Printf("  Neighbor %s distance: %f\n", nID, dist)
			if dist < minDistance {
				minDistance = dist
				nextNodeID = nID
			}
		}

		// If we can't find a better neighbor, break (this shouldn't happen in a proper CAN)
		if nextNodeID == "" {
			fmt.Printf("No valid next hop found in routing, path may be incomplete\n")
			break
		}

		// Add the next node to the path
		fmt.Printf("  Selecting next hop: %s (distance: %f)\n", nextNodeID, minDistance)
		path = append(path, nextNodeID)
		currentNodeID = nextNodeID

		// Prevent infinite loops
		if len(path) > 10 {
			fmt.Printf("Routing exceeded max path length (10), breaking\n")
			break
		}
	}

	fmt.Printf("Routing complete. Path: %v\n", path)

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
				Owner:         "",
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
				Owner:         "",
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

// failNode marks a node as failed and triggers data replication
func failNode(c *gin.Context) {
	nodeID := node.NodeID(c.Param("id"))

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	fmt.Printf("\n===== FAILING NODE %s =====\n", nodeID)

	if _, exists := simulation.Nodes[nodeID]; !exists {
		fmt.Printf("Node %s not found\n", nodeID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
		return
	}

	// Check if node is already failed
	if simulation.FailedNodes[nodeID] {
		fmt.Printf("Node %s is already marked as failed\n", nodeID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Node is already failed"})
		return
	}

	// Mark node as failed
	fmt.Printf("Marking node %s as failed\n", nodeID)
	simulation.FailedNodes[nodeID] = true

	// Get list of keys that this node is responsible for (primary copies)
	var primaryKeys []string
	for mapKey, kv := range simulation.KeyValuePairs {
		if kv.NodeID == nodeID && kv.IsPrimaryCopy {
			primaryKeys = append(primaryKeys, mapKey)
			fmt.Printf("Node %s is primary for key %s\n", nodeID, mapKey)
		}
	}
	fmt.Printf("Node %s is primary for %d keys\n", nodeID, len(primaryKeys))

	// Get list of keys where this node holds replicas
	var replicaKeys []string
	for mapKey, kv := range simulation.KeyValuePairs {
		if kv.NodeID == nodeID && !kv.IsPrimaryCopy {
			replicaKeys = append(replicaKeys, mapKey)
			fmt.Printf("Node %s holds replica for %s\n", nodeID, kv.Key)
		}
	}
	fmt.Printf("Node %s holds %d replicas\n", nodeID, len(replicaKeys))

	// Trigger data replication for the failed node
	replicateDataFromFailedNode(nodeID)

	fmt.Printf("===== NODE %s FAILED =====\n\n", nodeID)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("Node %s marked as failed", nodeID)})
}

// recoverNode recovers a failed node
func recoverNode(c *gin.Context) {
	nodeID := node.NodeID(c.Param("id"))

	simulation.mu.Lock()
	defer simulation.mu.Unlock()

	fmt.Printf("\n===== RECOVERING NODE %s =====\n", nodeID)

	if _, exists := simulation.Nodes[nodeID]; !exists {
		fmt.Printf("Node %s not found\n", nodeID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
		return
	}

	// Check if node is actually failed
	if !simulation.FailedNodes[nodeID] {
		fmt.Printf("Node %s is not failed, nothing to recover\n", nodeID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Node is not failed"})
		return
	}

	// Mark node as recovered
	fmt.Printf("Marking node %s as recovered\n", nodeID)
	delete(simulation.FailedNodes, nodeID)

	// Verify that node is no longer marked as failed
	fmt.Printf("Node %s failed status after recovery: %v\n", nodeID, simulation.FailedNodes[nodeID])

	// Restore data back to the recovered node
	restoreDataToRecoveredNode(nodeID)

	fmt.Printf("===== NODE %s RECOVERED =====\n\n", nodeID)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("Node %s has recovered", nodeID)})
}

// replicateDataFromFailedNode handles data replication from a failed node
func replicateDataFromFailedNode(failedNodeID node.NodeID) {
	// Get all keys that belong to the failed node
	var keysWithoutReplicas []string

	// First identify all primary keys on the failed node
	for key, kvInfo := range simulation.KeyValuePairs {
		if kvInfo.NodeID == failedNodeID && kvInfo.IsPrimaryCopy {
			// Check if this key already has replicas
			if len(kvInfo.ReplicaNodes) == 0 {
				// This is a key without replicas
				keysWithoutReplicas = append(keysWithoutReplicas, key)
				fmt.Printf("Key %s on failed node %s has no replicas. It will be inaccessible.\n",
					key, failedNodeID)
			} else {
				// This key has replicas - check if any replicas are on healthy nodes
				hasHealthyReplica := false
				for _, replicaNodeID := range kvInfo.ReplicaNodes {
					if !simulation.FailedNodes[replicaNodeID] {
						hasHealthyReplica = true
						fmt.Printf("Key %s has a healthy replica on node %s\n",
							key, replicaNodeID)
						break
					}
				}

				if !hasHealthyReplica {
					fmt.Printf("Key %s has replicas, but all replica nodes are also failed\n", key)
					keysWithoutReplicas = append(keysWithoutReplicas, key)
				}
			}
		}
	}

	fmt.Printf("Found %d keys on failed node %s that will be inaccessible (no healthy replicas)\n",
		len(keysWithoutReplicas), failedNodeID)

	// DO NOT create new replicas - this would defeat the purpose of explicit replication
	// for fault tolerance demonstration

	// Instead, just log that these keys will be inaccessible
	if len(keysWithoutReplicas) > 0 {
		fmt.Printf("The following keys will be inaccessible until node %s recovers:\n", failedNodeID)
		for _, key := range keysWithoutReplicas {
			fmt.Printf("  - %s\n", key)
		}
		fmt.Printf("Use explicit replication before failing nodes to maintain data availability\n")
	}
}

// restoreDataToRecoveredNode restores data to a recovered node
func restoreDataToRecoveredNode(recoveredNodeID node.NodeID) {
	fmt.Printf("Restoring data to recovered node %s\n", recoveredNodeID)

	// Track keys that need to be restored
	restoredKeys := 0

	// 1. First restore primary keys that belong to this node
	for key, kvInfo := range simulation.KeyValuePairs {
		if kvInfo.NodeID == recoveredNodeID && kvInfo.IsPrimaryCopy {
			// Check if the key exists in the node's data
			if _, exists := simulation.Nodes[recoveredNodeID].Data[key]; !exists {
				// Key doesn't exist in node data, restore it
				simulation.Nodes[recoveredNodeID].Data[key] = kvInfo.Value
				restoredKeys++
				fmt.Printf("Restored primary key %s to recovered node %s\n", key, recoveredNodeID)
			}
		}
	}

	// 2. Restore replica keys that should be on this node
	// Check all primary key entries to see which ones have this node as a replica
	for key, kvInfo := range simulation.KeyValuePairs {
		if !kvInfo.IsPrimaryCopy {
			continue // Skip non-primary entries
		}

		// Check if this node is listed as a replica for this key
		isReplica := false
		for _, replicaNodeID := range kvInfo.ReplicaNodes {
			if replicaNodeID == recoveredNodeID {
				isReplica = true
				break
			}
		}

		if isReplica {
			// This node should have a replica of this key
			if _, exists := simulation.Nodes[recoveredNodeID].Data[key]; !exists {
				// Replica doesn't exist in node data, restore it
				simulation.Nodes[recoveredNodeID].Data[key] = kvInfo.Value
				restoredKeys++
				fmt.Printf("Restored replica key %s to recovered node %s\n", key, recoveredNodeID)
			}
		}
	}

	// Find any missing replica entries in the KeyValuePairs map and recreate them
	for key, kvInfo := range simulation.KeyValuePairs {
		if !kvInfo.IsPrimaryCopy {
			continue // Skip non-primary entries
		}

		// Check if this node is listed as a replica but doesn't have a replica entry
		for _, replicaNodeID := range kvInfo.ReplicaNodes {
			if replicaNodeID == recoveredNodeID {
				// Check if we have a replica entry for this in KeyValuePairs
				hasReplicaEntry := false
				for checkKey, replicaInfo := range simulation.KeyValuePairs {
					if !replicaInfo.IsPrimaryCopy && replicaInfo.Key == key && replicaInfo.NodeID == recoveredNodeID {
						hasReplicaEntry = true
						fmt.Printf("Found existing replica entry at key %s\n", checkKey)
						break
					}
				}

				if !hasReplicaEntry {
					// We need to create a replica entry
					replicaMapKey := fmt.Sprintf("%s_replica_%s", key, recoveredNodeID)

					// Get center point of the replica's node zone
					recoveredNode := simulation.Nodes[recoveredNodeID]
					zoneCenter := node.Point{
						(recoveredNode.Zone.MinPoint[0] + recoveredNode.Zone.MaxPoint[0]) / 2,
						(recoveredNode.Zone.MinPoint[1] + recoveredNode.Zone.MaxPoint[1]) / 2,
					}

					// Create the replica entry
					replicaInfo := KeyValueInfo{
						Key:           key,
						Value:         kvInfo.Value,
						Point:         zoneCenter,
						NodeID:        recoveredNodeID,
						Color:         kvInfo.Color,
						Encrypted:     kvInfo.Encrypted,
						ReplicaNodes:  nil,
						IsPrimaryCopy: false,
						Owner:         "",
					}

					simulation.KeyValuePairs[replicaMapKey] = replicaInfo
					fmt.Printf("Recreated replica entry for key %s on node %s\n", key, recoveredNodeID)
				}
			}
		}
	}

	fmt.Printf("Data restoration complete for node %s: restored %d keys\n", recoveredNodeID, restoredKeys)
}
