package main

import (
	"fmt"
	"log"

	"github.com/can-dht/internal/proto"
	"github.com/google/uuid"
)

func main() {
	// Create a simple test of our proto package
	fmt.Println("Testing CAN-DHT proto package")

	// Create some test objects
	point := &proto.Point{
		Coordinates: []float64{0.5, 0.5},
	}

	zone := &proto.Zone{
		MinPoint: &proto.Point{Coordinates: []float64{0.0, 0.0}},
		MaxPoint: &proto.Point{Coordinates: []float64{1.0, 1.0}},
	}

	nodeID := uuid.New().String()

	nodeInfo := &proto.NodeInfo{
		Id:      nodeID,
		Address: "localhost:8080",
		Zone:    zone,
	}

	// Print information
	log.Printf("Created node with ID: %s", nodeInfo.Id)
	log.Printf("Node zone: min=%v, max=%v", 
		nodeInfo.Zone.MinPoint.Coordinates,
		nodeInfo.Zone.MaxPoint.Coordinates)
	log.Printf("Test point: %v", point.Coordinates)

	fmt.Println("Test completed successfully")
} 