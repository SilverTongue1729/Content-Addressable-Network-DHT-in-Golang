# Simplified CAN-DHT Implementation

This is a simplified implementation of a Content-Addressable Network (CAN) Distributed Hash Table (DHT). It provides basic functionality to demonstrate the concepts without the full complexity of the original implementation.

## Features

- Basic node implementation with zone management
- Simple key-value operations (PUT, GET, DELETE)
- Command-line client for interacting with nodes
- Support for creating a new network or joining an existing one

## Building

```bash
# Navigate to the simplified directory
cd simplified

# Install dependencies
go mod tidy

# Build the node
go build -o bin/node cmd/node.go

# Build the client
go build -o bin/client cmd/client.go
```

## Running

### Start First Node (Bootstrap Node)

```bash
# Start a node on port 8080 with 2 dimensions
./bin/node 8080 2
```

### Start Additional Nodes (Joining the Network)

```bash
# Start a node on port 8081, join via the node on port 8080
./bin/node 8081 2 localhost:8080

# Start a third node on port 8082
./bin/node 8082 2 localhost:8080
```

### Use the Client

```bash
./bin/client
```

Follow the prompts to interact with the DHT.

## Command-Line Arguments

### Node

```bash
./bin/node [port] [dimensions] [join_address]
```

- `port`: Port number to listen on (default: 8080)
- `dimensions`: Number of dimensions for the coordinate space (default: 2)
- `join_address`: Address of a node to join (optional, if not provided, creates a new network)

### Client

The client is interactive and will prompt for the node address to connect to.

## Example Session

1. Start the first node:
   ```bash
   ./bin/node 8080 2
   ```

2. In another terminal, start a second node:
   ```bash
   ./bin/node 8081 2 localhost:8080
   ```

3. In a third terminal, run the client:
   ```bash
   ./bin/client
   ```

4. Enter commands in the client:
   ```
   > put hello world
   PUT hello = world
   Success
   
   > get hello
   GET hello
   Value: world
   
   > delete hello
   DELETE hello
   Success
   ```

## Limitations

This simplified implementation does not include the following features from the full implementation:

- Actual network communication between nodes
- Data replication and fault tolerance
- Security features (encryption, authentication)
- Load balancing
- Caching

It is intended for educational purposes only, to demonstrate the basic concepts of a CAN DHT. 