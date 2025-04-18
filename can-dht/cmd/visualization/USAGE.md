# CAN DHT Visualization Usage Guide

This guide explains how to use the CAN DHT visualization tool effectively.

## Understanding the Interface

The visualization interface is divided into several sections:

1. **Coordinate Space View**: The main visualization area showing the 2D coordinate space
2. **Control Panel**: Buttons and inputs for interacting with the visualization
3. **Information Panel**: Displays system statistics and details about selected entities

## Visualization Elements

- **Blue Rectangles**: Represent zones owned by different nodes
- **Blue Circles**: Represent the nodes in the network
- **Colored Dots**: Represent key-value pairs stored in the DHT
- **Colored Lines**: Show routing paths for requests
  - Green lines: PUT operations
  - Blue lines: GET operations
  - Red lines: DELETE operations

## Basic Operations

### Network Operations

#### Adding a Node (CAN Join Protocol)

There are two ways to add a node:

1. **Direct Join**:
   - Simply click the "Add Node" button
   - The system will automatically select a random point in the coordinate space
   - The node that owns that zone (responsible node) is identified
   - The zone split and node creation happens automatically

2. **Preview Join** (for better visualization):
   - Click the "Preview Join Point" button first
   - A random point will be visualized with a yellow circle
   - The zone owner will be highlighted with a dashed border
   - An arrow will show which node owns the join point
   - Hover over the yellow circle to see details about the join point
   - Then click "Add Node" to complete the join operation

During the join process:
1. The responsible node splits its zone along the dimension with the longest side
2. The new node takes ownership of one half of the split zone
3. The responsible node keeps the other half
4. Key-value pairs in the split zone are redistributed to their correct owners
5. Neighbor relationships are updated automatically based on zone adjacency

#### Removing a Node

1. Click the "Remove Random Node" button
2. A random node will be removed from the network
3. Its zone will be reassigned (simplified in this visualization)
4. Key-value pairs will be redistributed to their new responsible nodes

#### Resetting the Simulation

1. Click the "Reset Simulation" button
2. The network will return to its initial state with a single root node
3. All key-value pairs will be removed

### Key-Value Operations

#### Putting a Key-Value Pair

1. Enter a key in the "Key" input field
2. Enter a value in the "Value" input field
3. Click the "PUT" button
4. Watch the routing path animation (green line)
5. A colored dot will appear in the coordinate space representing the key-value pair

#### Getting a Value

1. Enter an existing key in the "Key" input field
2. Click the "GET" button
3. Watch the routing path animation (blue line)
4. The value will appear in the "Value" field
5. The "Last Action" section will show the operation details

#### Deleting a Key-Value Pair

1. Enter an existing key in the "Key" input field
2. Click the "DELETE" button
3. Watch the routing path animation (red line)
4. The key-value pair (colored dot) will disappear from the visualization
5. The "Last Action" section will show the operation details

## Interactive Features

### Hovering and Tooltips

- Hover over a node to see its ID
- Hover over a zone to see its coordinate boundaries
- Hover over a key-value pair to see its key and value

### Clicking for Details

- Click on a node to see detailed information in the "Selected Entity Info" panel
- Click on a key-value pair to see its details
- The information panel will show relevant details about the selected item

## Common Scenarios to Observe

### Node Join and Zone Splitting

1. Start with a single node
2. Add several nodes one by one
3. Observe how zones split and the coordinate space gets partitioned
4. Notice how neighbor relationships form based on adjacent zones

### Key Distribution and Routing

1. Add several nodes to create a complex network
2. Add multiple key-value pairs with different keys
3. Observe how keys are distributed across the coordinate space
4. Perform GET operations and watch how routing works through multiple hops

### Node Removal and Data Redistribution

1. Create a network with multiple nodes and key-value pairs
2. Remove a node that owns some key-value pairs
3. Observe how the key-value pairs get redistributed
4. Verify you can still retrieve the values after redistribution

## Troubleshooting

- **Keys Not Found**: Make sure you're using the exact same key that was stored
- **Routing Paths Don't Appear**: The animation may be quick or the routing path might only involve one node
- **Nodes Not Connecting**: In a real CAN DHT, this shouldn't happen. Try resetting the simulation 