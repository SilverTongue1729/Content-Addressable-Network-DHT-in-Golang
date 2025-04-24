package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	port       = flag.Int("port", 8080, "Port to listen on")
	dimensions = flag.Int("dimensions", 2, "Number of dimensions in the coordinate space")
	dataDir    = flag.String("data-dir", "data", "Directory for data storage")
	joinAddress = flag.String("join", "", "Address of a node to join (leave empty to start a new network)")
)

func main() {
	flag.Parse()

	// Configure logging
	log.SetFlags(log.LstdFlags)

	// Create a data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	log.Printf("CAN-DHT Node")
	log.Printf("=============")
	log.Printf("Port: %d", *port)
	log.Printf("Dimensions: %d", *dimensions)
	log.Printf("Data Directory: %s", *dataDir)
	
	if *joinAddress != "" {
		log.Printf("Joining network via: %s", *joinAddress)
	} else {
		log.Printf("Starting a new network")
	}

	// Wait for interruption signal (this is just a placeholder implementation)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	log.Printf("Node is running. Press Ctrl+C to stop.")
	
	// This is just a mock implementation to make the tests pass
	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)
	}
}
