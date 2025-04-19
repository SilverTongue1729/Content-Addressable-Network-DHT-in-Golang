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
	"github.com/can-dht/pkg/crypto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	port              = flag.Int("port", 8080, "Port to listen on")
	dimensions        = flag.Int("dimensions", 2, "Number of dimensions in the coordinate space")
	dataDir           = flag.String("data-dir", "data", "Directory for data storage")
	joinAddress       = flag.String("join", "", "Address of a node to join (leave empty to start a new network)")
	enableEncryption  = flag.Bool("encryption", true, "Enable encryption of stored data")
	replicationFactor = flag.Int("replication", 1, "Replication factor for fault tolerance")
	heartbeatInterval = flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	heartbeatTimeout  = flag.Duration("timeout", 15*time.Second, "Heartbeat timeout")
	debug             = flag.Bool("debug", false, "Enable debug logging")
	nodeID            = flag.String("id", "", "Node ID (auto-generated if not provided)")
	certDir           = flag.String("cert-dir", "certificates", "Directory for TLS certificates")
	enableTLS         = flag.Bool("tls", true, "Enable TLS for gRPC connections")
)

func main() {
	flag.Parse()

	// Configure logging
	if *debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(log.LstdFlags)
	}

	// Generate or use provided node ID
	var nodeIdentifier node.NodeID
	if *nodeID == "" {
		nodeIdentifier = node.NodeID(uuid.New().String())
	} else {
		nodeIdentifier = node.NodeID(*nodeID)
	}

	// Determine the listen address
	address := fmt.Sprintf("localhost:%d", *port)

	// Create the server configuration
	config := service.DefaultCANConfig()
	config.Dimensions = *dimensions
	config.DataDir = *dataDir
	config.EnableEncryption = *enableEncryption
	config.ReplicationFactor = *replicationFactor
	config.HeartbeatInterval = *heartbeatInterval
	config.HeartbeatTimeout = *heartbeatTimeout

	// Initialize the server
	log.Printf("Initializing CAN node with ID %s...", nodeIdentifier)
	server, err := service.NewCANServer(nodeIdentifier, address, &config)
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

	// Set up a gRPC server with optional TLS
	var grpcServer *grpc.Server
	if *enableTLS {
		// Set up TLS
		tlsManager, err := crypto.NewTLSManager(nodeIdentifier, *certDir)
		if err != nil {
			log.Fatalf("Failed to initialize TLS manager: %v", err)
		}

		tlsConfig, err := tlsManager.GetTLSConfig(address)
		if err != nil {
			log.Fatalf("Failed to get TLS config: %v", err)
		}

		creds := credentials.NewTLS(tlsConfig)
		grpcServer = grpc.NewServer(grpc.Creds(creds))
		log.Printf("TLS enabled for gRPC connections")
	} else {
		grpcServer = grpc.NewServer()
		log.Printf("TLS disabled for gRPC connections")
	}

	server.StartGRPCServer(grpcServer)

	// Set up a listener for the gRPC server
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	// Start the gRPC server in a separate goroutine
	go func() {
		log.Printf("gRPC server listening on %s", address)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

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

	// Print node information
	log.Printf("Node Information:")
	log.Printf("- ID: %s", nodeIdentifier)
	log.Printf("- Address: %s", address)
	log.Printf("- Dimensions: %d", *dimensions)
	log.Printf("- Replication Factor: %d", *replicationFactor)
	log.Printf("- Encryption: %v", *enableEncryption)
	log.Printf("- TLS: %v", *enableTLS)

	// Wait for interruption signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Printf("Received signal %v, shutting down...", sig)

	// Gracefully stop the gRPC server
	grpcServer.GracefulStop()

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
