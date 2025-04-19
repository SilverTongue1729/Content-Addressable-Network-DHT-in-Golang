# Setting Up a Multi-Role CAN DHT Network

This guide demonstrates how to set up a Content-Addressable Network (CAN) with nodes having different roles for specialized tasks.

## Prerequisites

- Go 1.16 or later installed
- The CAN-DHT project cloned and built

## Overview of Node Roles

The CAN DHT supports the following node roles:

- **Standard**: General-purpose nodes that handle all types of requests
- **ReadOnly**: Nodes optimized for read operations, cannot handle writes
- **WriteOnly**: Nodes that only handle write operations, cannot handle reads
- **Indexer**: Specialized nodes for network coordination and key lookup
- **Edge**: Edge nodes optimized for fast read/write operations at the network boundary
- **Bootstrap**: Stable entry points to the network, primarily for joining

## Role-Based Capability Control

Each role has specific capabilities that define what operations it can perform:

| Role      | Read | Write | Delete | Join | Leave | Replicate | Index | Coordinate |
|-----------|------|-------|--------|------|-------|-----------|-------|------------|
| Standard  | ✓    | ✓     | ✓      | ✓    | ✓     | ✓         | ✓     | ✓          |
| ReadOnly  | ✓    | ✗     | ✗      | ✓    | ✓     | ✓         | ✓     | ✓          |
| WriteOnly | ✗    | ✓     | ✓      | ✓    | ✓     | ✓         | ✓     | ✓          |
| Indexer   | ✗    | ✗     | ✗      | ✓    | ✓     | ✗         | ✓     | ✓          |
| Edge      | ✓    | ✓     | ✗      | ✓    | ✓     | ✓         | ✗     | ✗          |
| Bootstrap | ✓    | ✗     | ✗      | ✓    | ✗     | ✗         | ✓     | ✓          |

## Request Forwarding System

When a node receives a request it cannot handle due to role limitations, it automatically forwards the request to an appropriate node through the request distribution system. For example, when a ReadOnly node receives a PUT request, it uses the configured distribution strategy to forward it to a node that can handle writes.

The system supports multiple distribution strategies:

- **Direct**: Forwards directly to the responsible node based on key location
- **Random**: Randomly selects a node with appropriate capabilities
- **Round Robin**: Distributes requests in a round-robin fashion
- **Least Loaded**: Selects the node with the lowest current load

## Setting Up a Multi-Role Network

In this example, we'll create a small network with:

1. A bootstrap node (first node)
2. A read-only node
3. A write-only node
4. An edge node

### Step 1: Start the Bootstrap Node

```bash
# Start a bootstrap node on port 8080
./can-dht --port 8080 --role bootstrap --data-dir data/bootstrap
```

This will start a bootstrap node that serves as the entry point to the network. The bootstrap node has these characteristics:
- Stable entry point for nodes joining the network
- Cannot handle write or delete operations
- Used primarily for network coordination

### Step 2: Add a Read-Only Node

```bash
# Start a read-only node on port 8081
./can-dht --port 8081 --role read-only --join localhost:8080 --data-dir data/readonly
```

This will start a read-only node that joins the network through the bootstrap node. The read-only node:
- Can handle read requests but not write requests
- Uses request distribution to forward write requests to appropriate nodes
- Optimized for read-heavy workloads

### Step 3: Add a Write-Only Node

```bash
# Start a write-only node on port 8082
./can-dht --port 8082 --role write-only --join localhost:8080 --data-dir data/writeonly
```

This will start a write-only node that handles write operations. The write-only node:
- Can handle write and delete requests but not read requests
- Uses request distribution to forward read requests to appropriate nodes
- Optimized for write-heavy workloads

### Step 4: Add an Edge Node

```bash
# Start an edge node on port 8083
./can-dht --port 8083 --role edge --join localhost:8080 --data-dir data/edge
```

This will start an edge node that sits at the boundary of the network. The edge node:
- Has a larger cache for fast responses
- Optimized for both reads and writes but not coordination
- Has higher replication factor for reliability

## Testing the Network

You can use the test client to verify that the network is working correctly:

```bash
# Put a key-value pair (should succeed on write-only and edge nodes)
./test-client --address localhost:8082 put mykey "Hello, multi-role network!"

# Get the value (should succeed on read-only and edge nodes)
./test-client --address localhost:8081 get mykey
```

If you try to put a value through the read-only node or get a value through the write-only node, these operations will be automatically forwarded to appropriate nodes via the request distribution system.

## Role-Based Behavior Examples

Here are some examples of how the roles affect node behavior:

1. **Test read-only restrictions**:
   ```bash
   # Try to write to read-only node (should be forwarded)
   ./test-client --address localhost:8081 put testkey "This should be forwarded"
   ```

2. **Test write-only restrictions**:
   ```bash
   # Try to read from write-only node (should be forwarded)
   ./test-client --address localhost:8082 get mykey
   ```

3. **Test bootstrap node restrictions**:
   ```bash
   # Try to write to bootstrap node (should be forwarded)
   ./test-client --address localhost:8080 put anotherkey "This should be forwarded"
   ```

## Monitoring Node Status

You can check the status of each node to verify their roles:

```bash
# Check bootstrap node status
curl http://localhost:8080/status

# Check read-only node status
curl http://localhost:8081/status

# Check write-only node status
curl http://localhost:8082/status

# Check edge node status
curl http://localhost:8083/status
```

The status endpoint will show the node's role, its current neighbors, and other configuration details.

## Advanced Configuration

You can further customize the node roles by setting additional parameters:

```bash
# Start an indexer node with custom parameters
./can-dht --port 8084 --role indexer --join localhost:8080 \
  --dimensions 3 \
  --replication 2 \
  --distribution-strategy least_loaded \
  --data-dir data/indexer
```

## Conclusion

A multi-role CAN DHT network allows for specialization of nodes based on their intended use. By distributing responsibilities across specialized nodes, you can optimize the network for different workloads and achieve better overall performance and reliability. 