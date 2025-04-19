package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/google/uuid"
)

// Basic types for the CAN DHT
type Point []float64
type NodeID string

// Zone represents a region in the coordinate space
type Zone struct {
	MinPoint Point
	MaxPoint Point
}

// Node represents a node in the CAN network
type Node struct {
	ID        NodeID
	Address   string
	Zone      *Zone
	Data      map[string]string
	Neighbors map[NodeID]NeighborInfo
	mu        sync.RWMutex
}

// NeighborInfo contains information about a neighboring node
type NeighborInfo struct {
	ID      NodeID
	Address string
	Zone    *Zone
}

// Simple hash function to map a key to coordinates
func hashToPoint(key string, dimensions int) Point {
	result := make(Point, dimensions)
	hash := 0
	
	// Simple hash function - for demonstration only
	for i := 0; i < len(key); i++ {
		hash = hash*31 + int(key[i])
	}
	
	r := rand.New(rand.NewSource(int64(hash)))
	for i := 0; i < dimensions; i++ {
		result[i] = r.Float64()
	}
	
	return result
}

// Check if a point is in a zone
func (z *Zone) Contains(p Point) bool {
	for i := 0; i < len(p); i++ {
		if p[i] < z.MinPoint[i] || p[i] >= z.MaxPoint[i] {
			return false
		}
	}
	return true
}

// Create a new node
func NewNode(address string, dimensions int) *Node {
	id := NodeID(uuid.New().String())
	
	// Create a zone covering the entire coordinate space
	minPoint := make(Point, dimensions)
	maxPoint := make(Point, dimensions)
	
	for i := 0; i < dimensions; i++ {
		minPoint[i] = 0.0
		maxPoint[i] = 1.0
	}
	
	return &Node{
		ID:        id,
		Address:   address,
		Zone:      &Zone{MinPoint: minPoint, MaxPoint: maxPoint},
		Data:      make(map[string]string),
		Neighbors: make(map[NodeID]NeighborInfo),
	}
}

// Store a key-value pair
func (n *Node) Put(key, value string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.Data[key] = value
	log.Printf("Node %s stored key: %s = %s", n.ID, key, value)
	return true
}

// Retrieve a value by key
func (n *Node) Get(key string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	value, exists := n.Data[key]
	if exists {
		log.Printf("Node %s retrieved key: %s = %s", n.ID, key, value)
	} else {
		log.Printf("Node %s: key not found: %s", n.ID, key)
	}
	return value, exists
}

// Delete a key-value pair
func (n *Node) Delete(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	_, exists := n.Data[key]
	if !exists {
		return false
	}
	
	delete(n.Data, key)
	log.Printf("Node %s deleted key: %s", n.ID, key)
	return true
}

// Start listening for connections
func (n *Node) Start() error {
	log.Printf("Node %s starting on %s", n.ID, n.Address)
	log.Printf("Zone: Min=%v, Max=%v", n.Zone.MinPoint, n.Zone.MaxPoint)
	
	// In a real implementation, we would start a gRPC server here
	// For this simplified version, we'll just simulate the node running
	
	return nil
}

// Join the network through another node
func (n *Node) Join(contactNodeAddr string) error {
	log.Printf("Node %s joining through %s", n.ID, contactNodeAddr)
	
	// In a real implementation, we would contact the other node,
	// get a part of its zone, and update our neighbors
	
	return nil
}

func main() {
	// Parse command-line arguments
	port := 8080
	dimensions := 2
	joinAddr := ""
	
	if len(os.Args) > 1 {
		if p, err := strconv.Atoi(os.Args[1]); err == nil {
			port = p
		}
	}
	
	if len(os.Args) > 2 {
		if d, err := strconv.Atoi(os.Args[2]); err == nil {
			dimensions = d
		}
	}
	
	if len(os.Args) > 3 {
		joinAddr = os.Args[3]
	}
	
	// Create and start the node
	address := fmt.Sprintf("localhost:%d", port)
	node := NewNode(address, dimensions)
	
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}
	
	// If a join address is provided, join the network
	if joinAddr != "" {
		if err := node.Join(joinAddr); err != nil {
			log.Fatalf("Failed to join network: %v", err)
		}
	}
	
	// Simulate some operations
	node.Put("hello", "world")
	node.Put("foo", "bar")
	
	if val, exists := node.Get("hello"); exists {
		fmt.Printf("Value for 'hello': %s\n", val)
	}
	
	if node.Delete("hello") {
		fmt.Println("Deleted 'hello'")
	}
	
	if _, exists := node.Get("hello"); !exists {
		fmt.Println("Verified 'hello' is deleted")
	}
	
	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	fmt.Println("Node running. Press Ctrl+C to exit.")
	<-sigCh
	
	fmt.Println("Shutting down...")
} 