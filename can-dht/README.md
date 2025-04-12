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
