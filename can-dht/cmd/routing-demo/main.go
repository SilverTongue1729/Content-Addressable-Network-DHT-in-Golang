package main

import (
	"fmt"
	"math"

	"github.com/can-dht/pkg/node"
	"github.com/can-dht/pkg/routing"
)

// Helper function to print a separator line
func printSection(title string) {
	fmt.Println("\n" + title)
	fmt.Println(string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}))
}

// Function to print a simple 2D visualization of the CAN coordinate space
func visualizeCoordinateSpace(nodes []*node.Node, points map[string]node.Point) {
	fmt.Println("\nCoordinate Space Visualization:")
	fmt.Println("-------------------------------")

	// Create a 40x20 grid for visualization
	grid := make([][]string, 20)
	for i := range grid {
		grid[i] = make([]string, 40)
		for j := range grid[i] {
			grid[i][j] = " "
		}
	}

	// Draw zone boundaries
	for _, n := range nodes {
		minX := int(n.Zone.MinPoint[0] * 40)
		minY := int(n.Zone.MinPoint[1] * 20)
		maxX := int(n.Zone.MaxPoint[0]*40) - 1
		maxY := int(n.Zone.MaxPoint[1]*20) - 1

		// Draw horizontal boundaries
		for x := minX; x <= maxX; x++ {
			if minY >= 0 && minY < 20 {
				grid[minY][x] = "-"
			}
			if maxY >= 0 && maxY < 20 {
				grid[maxY][x] = "-"
			}
		}

		// Draw vertical boundaries
		for y := minY; y <= maxY; y++ {
			if minX >= 0 && minX < 40 {
				grid[y][minX] = "|"
			}
			if maxX >= 0 && maxX < 40 {
				grid[y][maxX] = "|"
			}
		}

		// Mark node ID at center of zone
		centerX := (minX + maxX) / 2
		centerY := (minY + maxY) / 2
		if centerX >= 0 && centerX < 40 && centerY >= 0 && centerY < 20 {
			nodeLabel := string(n.ID)
			if len(nodeLabel) > 0 {
				grid[centerY][centerX] = nodeLabel[0:1]
			}
		}
	}

	// Mark key points
	for key, point := range points {
		x := int(point[0] * 40)
		y := int(point[1] * 20)

		if x >= 0 && x < 40 && y >= 0 && y < 20 {
			// Use a star as the marker for keys
			grid[y][x] = "*"

			// Optionally print first character of key if space allows
			if x+1 < 40 {
				keyChar := string([]rune(key)[0])
				grid[y][x+1] = keyChar
			}
		}
	}

	// Print the grid
	fmt.Println("+" + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + "+")
	for _, row := range grid {
		fmt.Print("|")
		for _, cell := range row {
			fmt.Print(cell)
		}
		fmt.Println("|")
	}
	fmt.Println("+" + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + "+")
	fmt.Println("* = Key location, A/B/C/D = Node zones")
}

// Function to simulate routing a request from a source node to the destination node
func simulateRouting(router *routing.Router, nodes []*node.Node, sourceNodeIndex int, key string) {
	fmt.Printf("\nSimulating routing for key '%s' from node %s\n", key, nodes[sourceNodeIndex].ID)

	// Hash the key to get its coordinate
	point := router.HashToPoint(key)
	fmt.Printf("Key '%s' hashes to coordinates (%.4f, %.4f)\n", key, point[0], point[1])

	// Start from the source node
	currentNode := nodes[sourceNodeIndex]
	hopCount := 0

	fmt.Printf("Starting at node %s (zone: min=(%.2f, %.2f), max=(%.2f, %.2f))\n",
		currentNode.ID,
		currentNode.Zone.MinPoint[0], currentNode.Zone.MinPoint[1],
		currentNode.Zone.MaxPoint[0], currentNode.Zone.MaxPoint[1])

	// Continue routing until we find the responsible node
	for {
		// Check if current node is responsible
		if currentNode.Zone.Contains(point) {
			fmt.Printf("→ Node %s is responsible for the key (hop count: %d)\n", currentNode.ID, hopCount)
			break
		}

		// Find next hop
		nextHop, _ := router.RouteToPoint(currentNode, point)
		if nextHop == nil {
			fmt.Printf("→ No route found to responsible node! Routing failed.\n")
			break
		}

		// Get the actual node object for the next hop
		var nextNode *node.Node
		for _, n := range nodes {
			if n.ID == nextHop.ID {
				nextNode = n
				break
			}
		}

		if nextNode == nil {
			fmt.Printf("→ Error: couldn't find node with ID %s\n", nextHop.ID)
			break
		}

		// Move to next node
		fmt.Printf("→ Forwarding to node %s (zone: min=(%.2f, %.2f), max=(%.2f, %.2f))\n",
			nextNode.ID,
			nextNode.Zone.MinPoint[0], nextNode.Zone.MinPoint[1],
			nextNode.Zone.MaxPoint[0], nextNode.Zone.MaxPoint[1])

		currentNode = nextNode
		hopCount++

		// Prevent infinite loops in the demo
		if hopCount > 10 {
			fmt.Printf("→ Too many hops, stopping simulation\n")
			break
		}
	}
}

func main() {
	fmt.Println("CAN-DHT Routing Demonstration")
	fmt.Println("=============================")
	fmt.Println("This demo shows how the CAN DHT routes requests to the appropriate")
	fmt.Println("node based on the key hash and coordinate space partitioning.")

	// Number of dimensions in our coordinate space
	dimensions := 2

	// Create a router
	router := routing.NewRouter(dimensions)

	printSection("1. Initial Network Setup - Single Node")

	// Create the initial node that owns the entire coordinate space
	initialZone, _ := node.NewZone(
		node.Point{0, 0},
		node.Point{1, 1},
	)
	nodeA := node.NewNode("A", "192.168.1.1:8000", initialZone, dimensions)

	fmt.Printf("Created initial node A with zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		nodeA.Zone.MinPoint[0], nodeA.Zone.MinPoint[1],
		nodeA.Zone.MaxPoint[0], nodeA.Zone.MaxPoint[1])

	// Visualize the initial state
	nodes := []*node.Node{nodeA}
	visualizeCoordinateSpace(nodes, nil)

	printSection("2. Node Join - Split Coordinate Space")

	// Node B selects a random point to join
	joinPointB := node.Point{0.25, 0.5} // For demonstration, use a fixed point
	fmt.Printf("Node B selects join point (%.2f, %.2f) in the coordinate space\n",
		joinPointB[0], joinPointB[1])

	// Find the node responsible for this point (currently only Node A exists)
	fmt.Printf("Routing join request to find the node responsible for the join point...\n")
	var responsibleNode *node.Node = nodeA // In this case, it's clearly Node A

	fmt.Printf("Node %s is responsible for the join point\n", responsibleNode.ID)
	fmt.Printf("Node %s will split its zone to accommodate Node B\n", responsibleNode.ID)

	// Split the zone for node B joining
	zoneB, zoneA, err := initialZone.Split(0) // split along x-axis
	if err != nil {
		fmt.Printf("Error splitting zone: %v\n", err)
		return
	}

	// Update nodeA's zone
	nodeA.Zone = zoneA

	nodeB := node.NewNode("B", "192.168.1.2:8000", zoneB, dimensions)
	fmt.Printf("Node B joins and gets zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		nodeB.Zone.MinPoint[0], nodeB.Zone.MinPoint[1],
		nodeB.Zone.MaxPoint[0], nodeB.Zone.MaxPoint[1])

	fmt.Printf("Node A now has zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		nodeA.Zone.MinPoint[0], nodeA.Zone.MinPoint[1],
		nodeA.Zone.MaxPoint[0], nodeA.Zone.MaxPoint[1])

	// Determine which node now owns the join point
	var ownerNode string
	if nodeA.Zone.Contains(joinPointB) {
		ownerNode = "A"
	} else if nodeB.Zone.Contains(joinPointB) {
		ownerNode = "B"
	} else {
		ownerNode = "unknown"
	}
	fmt.Printf("After splitting, join point (%.2f, %.2f) is now in Node %s's zone\n",
		joinPointB[0], joinPointB[1], ownerNode)

	// Setup neighbors
	fmt.Println("Updating neighbor relationships:")
	fmt.Println("- Node A adds Node B as neighbor")
	fmt.Println("- Node B adds Node A as neighbor")
	nodeA.AddNeighbor(nodeB.ID, nodeB.Address, nodeB.Zone)
	nodeB.AddNeighbor(nodeA.ID, nodeA.Address, nodeA.Zone)

	// Visualize the join point
	joinPoints := map[string]node.Point{
		"B's join point": joinPointB,
	}

	// Visualize with two nodes
	nodes = []*node.Node{nodeA, nodeB}
	visualizeCoordinateSpace(nodes, joinPoints)

	printSection("3. More Nodes Join - Further Partitioning")

	// Node C selects a random point
	joinPointC := node.Point{0.75, 0.25} // For demonstration, use a fixed point
	fmt.Printf("Node C selects join point (%.2f, %.2f) in the coordinate space\n",
		joinPointC[0], joinPointC[1])

	// In a real system, we would route to find the responsible node
	fmt.Println("Routing join request to find the node responsible for the join point...")
	fmt.Printf("Starting at Node A (entry point to the network)\n")

	// Simulate routing to find responsible node
	if nodeA.Zone.Contains(joinPointC) {
		fmt.Printf("Node A is responsible for the join point\n")
		responsibleNode = nodeA
	} else if nodeB.Zone.Contains(joinPointC) {
		fmt.Printf("Forwarding to Node B\n")
		fmt.Printf("Node B is responsible for the join point\n")
		responsibleNode = nodeB
	} else {
		fmt.Printf("Error: No node found responsible for this point\n")
		return
	}

	fmt.Printf("Node %s will split its zone to accommodate Node C\n", responsibleNode.ID)

	// Based on the responsible node, split the appropriate zone
	var newZoneC, newResponsibleZone *node.Zone
	if responsibleNode.ID == "A" {
		// Split node A's zone for node C
		newZoneC, newResponsibleZone, err = nodeA.Zone.Split(1) // split along y-axis
		if err != nil {
			fmt.Printf("Error splitting zone: %v\n", err)
			return
		}
		nodeA.Zone = newResponsibleZone
	} else {
		// This shouldn't happen in our demo, but handle it anyway
		newZoneC, newResponsibleZone, err = nodeB.Zone.Split(1)
		if err != nil {
			fmt.Printf("Error splitting zone: %v\n", err)
			return
		}
		nodeB.Zone = newResponsibleZone
	}

	nodeC := node.NewNode("C", "192.168.1.3:8000", newZoneC, dimensions)
	fmt.Printf("Node C joins and gets zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		nodeC.Zone.MinPoint[0], nodeC.Zone.MinPoint[1],
		nodeC.Zone.MaxPoint[0], nodeC.Zone.MaxPoint[1])

	fmt.Printf("Node %s now has zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		responsibleNode.ID,
		responsibleNode.Zone.MinPoint[0], responsibleNode.Zone.MinPoint[1],
		responsibleNode.Zone.MaxPoint[0], responsibleNode.Zone.MaxPoint[1])

	// Determine which node now owns the join point
	if nodeA.Zone.Contains(joinPointC) {
		ownerNode = "A"
	} else if nodeB.Zone.Contains(joinPointC) {
		ownerNode = "B"
	} else if nodeC.Zone.Contains(joinPointC) {
		ownerNode = "C"
	} else {
		ownerNode = "unknown"
	}
	fmt.Printf("After splitting, join point (%.2f, %.2f) is now in Node %s's zone\n",
		joinPointC[0], joinPointC[1], ownerNode)

	// Node D selects a random point
	joinPointD := node.Point{0.25, 0.75} // For demonstration, use a fixed point
	fmt.Printf("\nNode D selects join point (%.2f, %.2f) in the coordinate space\n",
		joinPointD[0], joinPointD[1])

	// Simulate routing for Node D
	fmt.Println("Routing join request to find the node responsible for the join point...")
	fmt.Printf("Starting at Node A (entry point to the network)\n")

	// Determine which node contains this point
	if nodeA.Zone.Contains(joinPointD) {
		fmt.Printf("Node A is responsible for the join point\n")
		responsibleNode = nodeA
	} else if nodeB.Zone.Contains(joinPointD) {
		fmt.Printf("Forwarding to Node B\n")
		fmt.Printf("Node B is responsible for the join point\n")
		responsibleNode = nodeB
	} else if nodeC.Zone.Contains(joinPointD) {
		fmt.Printf("Forwarding to Node C\n")
		fmt.Printf("Node C is responsible for the join point\n")
		responsibleNode = nodeC
	} else {
		fmt.Printf("Error: No node found responsible for this point\n")
		return
	}

	fmt.Printf("Node %s will split its zone to accommodate Node D\n", responsibleNode.ID)

	// Split the responsible node's zone
	var newZoneD, newResponsibleZoneD *node.Zone
	if responsibleNode.ID == "A" {
		newZoneD, newResponsibleZoneD, err = nodeA.Zone.Split(1)
		if err != nil {
			fmt.Printf("Error splitting zone: %v\n", err)
			return
		}
		nodeA.Zone = newResponsibleZoneD
	} else if responsibleNode.ID == "B" {
		newZoneD, newResponsibleZoneD, err = nodeB.Zone.Split(1)
		if err != nil {
			fmt.Printf("Error splitting zone: %v\n", err)
			return
		}
		nodeB.Zone = newResponsibleZoneD
	} else {
		newZoneD, newResponsibleZoneD, err = nodeC.Zone.Split(1)
		if err != nil {
			fmt.Printf("Error splitting zone: %v\n", err)
			return
		}
		nodeC.Zone = newResponsibleZoneD
	}

	nodeD := node.NewNode("D", "192.168.1.4:8000", newZoneD, dimensions)
	fmt.Printf("Node D joins and gets zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		nodeD.Zone.MinPoint[0], nodeD.Zone.MinPoint[1],
		nodeD.Zone.MaxPoint[0], nodeD.Zone.MaxPoint[1])

	fmt.Printf("Node %s now has zone: min=(%.2f, %.2f), max=(%.2f, %.2f)\n",
		responsibleNode.ID,
		responsibleNode.Zone.MinPoint[0], responsibleNode.Zone.MinPoint[1],
		responsibleNode.Zone.MaxPoint[0], responsibleNode.Zone.MaxPoint[1])

	// Show all join points
	allJoinPoints := map[string]node.Point{
		"B's join point": joinPointB,
		"C's join point": joinPointC,
		"D's join point": joinPointD,
	}

	// Update neighbors
	fmt.Println("\nUpdating neighbor relationships after all joins:")

	// Node A neighbors
	nodeA.RemoveNeighbor(nodeB.ID) // No longer neighbors after split
	nodeA.AddNeighbor(nodeC.ID, nodeC.Address, nodeC.Zone)
	nodeA.AddNeighbor(nodeD.ID, nodeD.Address, nodeD.Zone)

	// Node B neighbors
	nodeB.RemoveNeighbor(nodeA.ID) // No longer neighbors after split
	nodeB.AddNeighbor(nodeC.ID, nodeC.Address, nodeC.Zone)
	nodeB.AddNeighbor(nodeD.ID, nodeD.Address, nodeD.Zone)

	// Node C neighbors
	nodeC.AddNeighbor(nodeA.ID, nodeA.Address, nodeA.Zone)
	nodeC.AddNeighbor(nodeB.ID, nodeB.Address, nodeB.Zone)
	nodeC.AddNeighbor(nodeD.ID, nodeD.Address, nodeD.Zone)

	// Node D neighbors
	nodeD.AddNeighbor(nodeA.ID, nodeA.Address, nodeA.Zone)
	nodeD.AddNeighbor(nodeB.ID, nodeB.Address, nodeB.Zone)
	nodeD.AddNeighbor(nodeC.ID, nodeC.Address, nodeC.Zone)

	// Visualize with four nodes
	nodes = []*node.Node{nodeA, nodeB, nodeC, nodeD}
	visualizeCoordinateSpace(nodes, allJoinPoints)

	printSection("4. Key Hashing and Routing")

	// Define some example keys
	keys := []string{
		"user1",
		"file.txt",
		"image.jpg",
		"document.pdf",
		"settings.json",
	}

	// Hash keys to coordinates and visualize
	keyPoints := make(map[string]node.Point)

	fmt.Println("Key Hashing Results:")
	for _, key := range keys {
		point := router.HashToPoint(key)
		keyPoints[key] = point

		// Find responsible node
		var responsibleNode *node.Node
		for _, n := range nodes {
			if n.Zone.Contains(point) {
				responsibleNode = n
				break
			}
		}

		if responsibleNode != nil {
			fmt.Printf("Key '%s' hashes to (%.4f, %.4f) → Node %s is responsible\n",
				key, point[0], point[1], responsibleNode.ID)
		} else {
			fmt.Printf("Key '%s' hashes to (%.4f, %.4f) → No responsible node found\n",
				key, point[0], point[1])
		}
	}

	// Visualize the coordinate space with keys
	visualizeCoordinateSpace(nodes, keyPoints)

	printSection("5. Routing Simulation - Greedy Routing")

	fmt.Println("CAN uses greedy routing to forward requests to the node responsible for a key.")
	fmt.Println("Each node forwards the request to the neighbor closest to the key coordinates.")

	// Simulate routing for each key from different starting nodes
	for i, key := range keys {
		// Start from a different node each time to show routing
		sourceNodeIndex := i % len(nodes)
		simulateRouting(router, nodes, sourceNodeIndex, key)
	}

	printSection("6. Routing Metrics")

	fmt.Println("Routing Performance in CAN DHT:")
	fmt.Printf("- Number of nodes: %d\n", len(nodes))
	fmt.Printf("- Number of dimensions: %d\n", dimensions)
	fmt.Printf("- Expected average path length: O(n^(1/d)) = O(%d^(1/%d)) ≈ O(%.2f)\n",
		len(nodes), dimensions, math.Pow(float64(len(nodes)), 1.0/float64(dimensions)))
	fmt.Println("- With more dimensions, routing becomes more efficient (shorter paths)")
	fmt.Println("- With more nodes, the coordinate space becomes more finely partitioned")
	fmt.Println("- Each node maintains O(d) neighbors in a d-dimensional coordinate space")

	fmt.Println("\nCAN-DHT Routing Demonstration Complete")
}
