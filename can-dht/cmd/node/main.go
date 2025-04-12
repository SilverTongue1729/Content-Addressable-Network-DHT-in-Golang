package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/service"
	"github.com/google/uuid"
)

var (
	port             = flag.Int("port", 8080, "Port to listen on")
	dimensions       = flag.Int("dimensions", 2, "Number of dimensions in the coordinate space")
	dataDir          = flag.String("data-dir", "data", "Directory for data storage")
	joinAddress      = flag.String("join", "", "Address of a node to join (leave empty to start a new network)")
	enableEncryption = flag.Bool("encryption", true, "Enable encryption of stored data")
)

func main() {
	flag.Parse()

	// Generate a random node ID
	nodeID := node.NodeID(uuid.New().String())

	// Determine the listen address
	address := fmt.Sprintf("localhost:%d", *port)

	// Create the server configuration
	config := service.DefaultCANConfig()
	config.Dimensions = *dimensions
	config.DataDir = *dataDir
	config.EnableEncryption = *enableEncryption

	// Initialize the server
	log.Printf("Initializing CAN node with ID %s...", nodeID)
	server, err := service.NewCANServer(nodeID, address, config)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	// Create a data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Start the server
	server.Start()
	log.Printf("CAN node started on %s", address)

	// If a join address is provided, attempt to join the network
	if *joinAddress != "" {
		log.Printf("Attempting to join network via %s...", *joinAddress)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Join(ctx, *joinAddress); err != nil {
			log.Fatalf("Failed to join network: %v", err)
		}
		log.Printf("Successfully joined network")
	} else {
		log.Printf("Starting a new CAN network")
	}

	// Set up a listener for the gRPC server
	_, err = net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	// NOTE: In a real implementation, we would start a gRPC server using the listener
	// For now, we'll just keep the program running until it receives a signal
	log.Printf("Listening on %s (gRPC server not implemented yet)", address)

	// Wait for interruption signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Printf("Received signal %v, shutting down...", sig)

	// Gracefully leave the network if possible
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Leave(ctx); err != nil {
		log.Printf("Warning: failed to gracefully leave the network: %v", err)
	}

	// Stop the server
	if err := server.Stop(); err != nil {
		log.Printf("Warning: error stopping server: %v", err)
	}

	log.Printf("CAN node stopped")
}
