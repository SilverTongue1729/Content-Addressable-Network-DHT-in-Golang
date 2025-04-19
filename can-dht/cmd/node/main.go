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
	"google.golang.org/grpc"
)

var (
	port                     = flag.Int("port", 8080, "Port to listen on")
	dimensions               = flag.Int("dimensions", 2, "Number of dimensions in the coordinate space")
	dataDir                  = flag.String("data-dir", "data", "Directory for data storage")
	joinAddress              = flag.String("join", "", "Address of a node to join (leave empty to start a new network)")
	enableEncryption         = flag.Bool("encryption", true, "Enable encryption of stored data")
	replicationFactor        = flag.Int("replication", 1, "Replication factor for fault tolerance")
	heartbeatInterval        = flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	heartbeatTimeout         = flag.Duration("timeout", 15*time.Second, "Heartbeat timeout")
	debug                    = flag.Bool("debug", false, "Enable debug logging")
	nodeID                   = flag.String("id", "", "Node ID (auto-generated if not provided)")
	enableMTLS               = flag.Bool("mtls", false, "Enable mutual TLS for secure communication")
	tlsCertFile              = flag.String("tls-cert", "certs/node.crt", "Path to TLS certificate file")
	tlsKeyFile               = flag.String("tls-key", "certs/node.key", "Path to TLS key file")
	tlsCAFile                = flag.String("tls-ca", "certs/ca.crt", "Path to CA certificate file")
	role                     = flag.String("role", "standard", "Node role (standard, read-only, write-only, indexer, edge, bootstrap)")
	enableIntegrityChecking  = flag.Bool("integrity", true, "Enable integrity checking")
	useEnhancedIntegrity     = flag.Bool("enhanced-integrity", true, "Use enhanced integrity checking")
	enableAccessControl      = flag.Bool("access-control", false, "Enable API key authentication")
	apiKeysFile              = flag.String("api-keys", "config/api_keys.json", "Path to API keys file")
	enableRequestDistribution = flag.Bool("request-distribution", true, "Enable request distribution")
	distributionStrategy     = flag.String("distribution-strategy", "direct", "Request distribution strategy (direct, random, round_robin, least_loaded)")
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

	// Create node with role
	log.Printf("Initializing CAN node with ID %s and role %s...", nodeIdentifier, *role)
	
	// Convert string role to NodeRole type
	var nodeRole service.NodeRole
	switch *role {
	case "standard":
		nodeRole = service.RoleStandard
	case "read-only":
		nodeRole = service.RoleReadOnly
	case "write-only":
		nodeRole = service.RoleWriteOnly
	case "indexer":
		nodeRole = service.RoleIndexer
	case "edge":
		nodeRole = service.RoleEdge
	case "bootstrap":
		nodeRole = service.RoleBootstrap
	default:
		log.Printf("Unknown role '%s', defaulting to 'standard'", *role)
		nodeRole = service.RoleStandard
	}

	// Get role-based config
	config := service.ConfigForRole(string(nodeIdentifier), address, nodeRole)
	
	// Override with command line parameters if provided
	config.Dimensions = *dimensions
	config.DataDir = *dataDir
	config.ReplicationFactor = *replicationFactor
	config.HeartbeatInterval = *heartbeatInterval
	config.HeartbeatTimeout = *heartbeatTimeout
	
	if *enableIntegrityChecking {
		config.IntegrityCheckInterval = 30 * time.Minute
	} else {
		config.IntegrityCheckInterval = 0
	}
	
	config.UseEnhancedIntegrity = *useEnhancedIntegrity
	config.EnableAccessControl = *enableAccessControl
	config.ApiKeysFile = *apiKeysFile
	config.EnableRequestRedistribution = *enableRequestDistribution
	config.RequestDistributionStrategy = *distributionStrategy

	// Initialize the server
	server, err := service.NewCANServer(config)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	// Create a data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Start the server
	server.Start()
	log.Printf("CAN node started on %s", address)

	// Set up a gRPC server
	grpcServer, err := server.CreateGRPCServer()
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
	}

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
	log.Printf("- Role: %s", nodeRole)
	log.Printf("- Address: %s", address)
	log.Printf("- Dimensions: %d", config.Dimensions)
	log.Printf("- Replication Factor: %d", config.ReplicationFactor)
	log.Printf("- Request Distribution: %s", config.RequestDistributionStrategy)

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
