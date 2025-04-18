# CAN-DHT: Content Addressable Network Distributed Hash Table

A decentralized DHT implementation based on CAN (Content Addressable Network) principles.

## Project Structure

```
can-dht/
├── cmd/                # Main applications
│   └── node/           # CAN node implementation
├── pkg/                # Reusable packages
│   ├── node/           # Node operations and management
│   ├── routing/        # Routing protocols and algorithms
│   ├── storage/        # Key-value storage engines
│   ├── crypto/         # Security and encryption
│   ├── service/        # gRPC service implementation
│   └── visualization/  # Visualization components (future)
└── proto/              # Protocol buffer definitions
```

## Implementation Status

This implementation follows an incremental approach, starting with core features and progressively adding more complex functionality.

### Currently Implemented

1. **Virtual Coordinate Space**
   - d-dimensional coordinate system
   - Zone representation and management
   - Zone splitting algorithm

2. **Basic Node Operations**
   - Node representation with unique IDs
   - Zone ownership
   - Neighbor management
   - Zone splitting on node join

3. **Routing Protocol**
   - Key to coordinate hashing
   - Greedy routing based on Euclidean distance
   - Next-hop selection

4. **Storage Engine**
   - Key-value storage with persistence (BadgerDB)
   - LRU caching for performance
   - Metadata storage

5. **Security**
   - AES-GCM encryption for stored data
   - HMAC-SHA256 for data integrity
   - Secure data serialization

6. **Basic DHT Operations**
   - PUT: Store a key-value pair
   - GET: Retrieve a value by key
   - DELETE: Remove a key-value pair

7. **gRPC Communication**
   - Complete gRPC server implementation
   - Client-to-node and node-to-node communication

8. **Advanced Node Operations**
   - Join protocol with zone transfer
   - Graceful node departure
   - Neighbor discovery and maintenance

9. **Fault Tolerance**
   - Heartbeat mechanism
   - Data replication to neighbors
   - Basic recovery from node failures

10. **Load Balancing**
    - Hot-spot detection
    - Request distribution
    - Simple replication-based load balancing

### Planned Enhancements

1. **Load Balancing Improvements**
   - Advanced dynamic zone adjustment
   - Comprehensive load balancing strategies

2. **Fault Tolerance Enhancements**
   - Advanced recovery mechanisms
   - Automated network healing

3. **Visualization**
   - 2D visualization of the coordinate space
   - Node and zone visualization
   - Request routing visualization

4. **Performance Optimizations**
   - Advanced caching strategies
   - Protocol optimizations

## Security Features

The CAN DHT implementation includes several security features to protect stored data:

### Data Encryption

Values stored in the DHT are encrypted using AES-GCM (Galois/Counter Mode), which provides both confidentiality and integrity verification:

- **Values are encrypted**: The actual content of key-value pairs is encrypted before storage
- **Keys are not encrypted**: Keys are hashed to determine their location in the coordinate space
- **Data Integrity**: The AES-GCM mode provides built-in authentication to protect against tampering

### Usage Example

```go
// Enable encryption in the configuration
config := &CANConfig{
    EnableEncryption: true,
    // other config options...
}

// Create a CAN server with encryption enabled
server, err := NewCANServer("node1", "localhost:8080", config)
if err != nil {
    log.Fatalf("Failed to create server: %v", err)
}

// Store data securely
ctx := context.Background()
err = server.SecurePut(ctx, "user123", []byte("sensitive_data"))
if err != nil {
    log.Fatalf("Failed to store data: %v", err)
}

// Retrieve and decrypt data
data, err := server.SecureGet(ctx, "user123")
if err != nil {
    log.Fatalf("Failed to retrieve data: %v", err)
}
fmt.Printf("Retrieved: %s\n", string(data))
```

### How It Works

1. **Encryption Process (PUT operation)**:
   - A random encryption key is generated when the node starts
   - For each value to be stored, the value is encrypted using AES-GCM
   - A random nonce (number used once) is generated for each encryption
   - The nonce is stored with the ciphertext
   - An HMAC is generated for integrity verification
   - The ciphertext, nonce, and HMAC are serialized and stored

2. **Decryption Process (GET operation)**:
   - The serialized data is retrieved from storage
   - The ciphertext, nonce, and HMAC are extracted
   - The HMAC is verified to ensure data integrity
   - If verification passes, the data is decrypted using AES-GCM
   - The decrypted plaintext is returned

### Security Guarantees

- **Confidentiality**: Even if an attacker gains access to the stored data, they cannot read the values without the encryption key
- **Integrity**: Any tampering with the encrypted data will be detected during decryption
- **Authentication**: The HMAC ensures that only authorized entities can modify the data

## Building and Running

### Prerequisites

- Go 1.16 or later
- Protocol Buffer Compiler (protoc)
- Go plugins for Protocol Buffers

### Building

```bash
# Install protoc plugins (if not already installed)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate code from protocol buffers
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/can.proto

# Build the node binary
go build -o bin/can-node ./cmd/node
```

### Running

To start a new CAN network:

```bash
./bin/can-node --port 8080 --dimensions 2 --data-dir ./data/node1
```

To join an existing network:

```bash
./bin/can-node --port 8081 --dimensions 2 --data-dir ./data/node2 --join localhost:8080
```

## Future Development

The next development phases will focus on:

1. Implementing the complete gRPC service for node-to-node communication
2. Enhancing the join and leave protocols for network stability
3. Adding replication and fault tolerance mechanisms
4. Implementing load balancing strategies
5. Creating the visualization tool

## Contribution

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Visualization Tool

A web-based visualization tool is available to help understand and demonstrate the CAN DHT system. The visualization provides an interactive interface that shows:

- Nodes displayed as labeled points in a 2D coordinate space
- Zones shown as rectangles around each node to indicate partitions
- Key-Value pairs marked as colored dots within zones
- Routing paths animated as lines showing GET/PUT/DELETE request paths between nodes

### Running the Visualization

```bash
go run cmd/visualization/main.go
```

Then open your browser and navigate to http://localhost:8090.

The visualization allows you to:
- Add and remove nodes to see dynamic zone splitting
- Perform key-value operations (PUT, GET, DELETE)
- Observe routing paths in real-time
- View detailed information about nodes and key-value pairs

For more details, see the [Visualization README](cmd/visualization/README.md).
