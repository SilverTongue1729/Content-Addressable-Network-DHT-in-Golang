# CAN DHT Visualization Tool

This is a web-based visualization tool for the Content Addressable Network (CAN) Distributed Hash Table (DHT). It provides an interactive interface to demonstrate the core concepts and operations of a CAN DHT.

## Features

- **2D Coordinate Space Visualization**: Shows the partitioning of the coordinate space among nodes
- **Node Operations**: Add and remove nodes to see dynamic zone splitting and merging
- **Key-Value Operations**: Perform PUT, GET, and DELETE operations and watch data routing
- **Routing Path Animation**: Visualize how requests are routed through the network
- **Interactive UI**: Click on nodes and key-value pairs to see detailed information

## Technical Details

The visualization tool consists of:

- **Backend**: Golang with Gin web framework
- **Frontend**: HTML, CSS, JavaScript with D3.js for visualization
- **State Management**: Simulates the CAN DHT behavior with real-time updates

## Running the Visualization

1. Ensure you have Go installed (version 1.16 or later)
2. Navigate to the project root directory
3. Run the visualization server:

```bash
go run cmd/visualization/main.go
```

4. Open your browser and navigate to `http://localhost:8090`

## Usage Guide

### Network Operations

- **Add Node**: Adds a new node to the network, causing zone splitting
- **Remove Node**: Removes a random node from the network
- **Reset**: Resets the simulation to its initial state with a single root node

### Key-Value Operations

- **PUT**: Store a key-value pair in the DHT
- **GET**: Retrieve a value by its key
- **DELETE**: Remove a key-value pair from the DHT

### Visualization Elements

- **Blue Rectangles**: Zones owned by nodes
- **Blue Circles**: Nodes in the network
- **Colored Dots**: Key-value pairs stored in the DHT
- **Colored Lines**: Routing paths for requests (green for PUT, blue for GET, red for DELETE)

## Understanding CAN DHT

A Content Addressable Network (CAN) is a distributed hash table that maps keys to values in a multi-dimensional coordinate space:

1. **Coordinate Space**: The entire system operates within a virtual d-dimensional coordinate space
2. **Zone Ownership**: Each node owns a distinct zone within the coordinate space
3. **Key Mapping**: Keys are hashed to coordinates within the space
4. **Greedy Routing**: Requests are routed to the node closest to the target coordinates
5. **Node Joining**: When a node joins, it splits an existing zone
6. **Fault Tolerance**: Data can be replicated to handle node failures

This visualization focuses on demonstrating these core concepts in an interactive way. 