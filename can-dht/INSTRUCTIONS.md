# CAN-DHT: Building and Testing Instructions

This document provides instructions for building and testing the CAN-DHT implementation.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Go 1.16 or later**
- **Git**
- **Protocol Buffer Compiler (protoc)** - Only needed if you want to regenerate the gRPC code

## Building the Project

1. **Clone the repository (if not already done)**

   ```bash
   git clone <repository-url>
   cd can-dht
   ```

2. **Install dependencies**

   ```bash
   go mod tidy
   ```

3. **Build the node binary**

   ```bash
   go build -o bin/can-node ./cmd/node
   ```

4. **Build the test client**

   ```bash
   go build -o bin/test-client ./cmd/test-client
   ```

## Testing Options

You have two ways to test the CAN-DHT implementation:

### Option 1: Run the Automated Test Client

The test client provides a simple way to verify basic DHT operations:

```bash
# Create a directory for test data
mkdir -p test-data

# Run the test client
./bin/test-client
```

The test client will:
- Initialize a single CAN node
- Test PUT, GET, and DELETE operations
- Verify data encryption and decryption
- Test concurrent operations
- Report any failures or success

### Option 2: Manual Testing with Multiple Nodes

For a more interactive experience or to test node-to-node communication:

1. **Start the first node (creates a new network)**

   ```bash
   mkdir -p data/node1
   ./bin/can-node --port 8080 --dimensions 2 --data-dir ./data/node1
   ```

2. **Start a second node (joins the first node's network)**

   In a new terminal:
   ```bash
   mkdir -p data/node2
   ./bin/can-node --port 8081 --dimensions 2 --data-dir ./data/node2 --join localhost:8080
   ```

3. **Start additional nodes as needed**

   ```bash
   mkdir -p data/node3
   ./bin/can-node --port 8082 --dimensions 2 --data-dir ./data/node3 --join localhost:8080
   ```

4. **Observe node behavior**

   Watch the log output to see:
   - Node initialization
   - Join operations (when applicable)
   - Heartbeat messages
   - Zone splitting

## Limitations of Current Implementation

Currently, the implementation has several limitations:

1. **Incomplete gRPC Implementation**: While the protobuf definitions are in place, the gRPC server is not fully implemented yet. This means you cannot yet interact with the nodes via gRPC calls.

2. **Simplified Node Join/Leave**: The joining and leaving protocols are placeholders that need to be completed.

3. **No External Client**: There's no external client to send PUT/GET/DELETE requests to the network. The test client directly tests the node logic but doesn't test network communication.

## Testing the Core Functionality

Even with these limitations, you can verify the core functionality works:

1. **Virtual Coordinate Space**: The test client verifies that the coordinate space is correctly implemented by checking that keys are properly hashed to coordinates.

2. **Basic Node Operations**: You can observe zone creation and management when starting nodes.

3. **Key-Value Operations**: The test client verifies that PUT, GET, and DELETE operations work correctly.

4. **Storage Engine**: The test client verifies that data is properly stored and retrieved.

5. **Security**: The test client verifies that encryption and decryption work correctly.

## Running Tests

To run the Go tests for individual packages:

```bash
go test ./pkg/node
go test ./pkg/routing
go test ./pkg/storage
go test ./pkg/crypto
```

Or run all tests:

```bash
go test ./...
```

## Troubleshooting

If you encounter issues:

1. **Data directory permissions**: Ensure the data directories are writable.

2. **Port conflicts**: If ports are in use, specify different port numbers.

3. **BadgerDB errors**: If you see BadgerDB errors, ensure the data directory is not corrupted. You can delete it and create a new one.

4. **Dependency issues**: Run `go mod tidy` to ensure all dependencies are correctly resolved. 