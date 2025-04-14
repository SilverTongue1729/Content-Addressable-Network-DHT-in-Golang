package routing

import (
    "testing"
    "github.com/can-dht/pkg/node"
)

func TestHashToPoint(t *testing.T) {
    router := NewRouter(2)
    
    // Test same key produces same point
    point1 := router.HashToPoint("test")
    point2 := router.HashToPoint("test")
    
    if len(point1) != 2 {
        t.Errorf("Expected 2 dimensions, got %d", len(point1))
    }
    
    if point1[0] != point2[0] || point1[1] != point2[1] {
        t.Error("Same key produced different points")
    }
    
    // Test different keys produce different points
    point3 := router.HashToPoint("test2")
    if point1[0] == point3[0] && point1[1] == point3[1] {
        t.Error("Different keys produced same points")
    }
    
    // Test point values are in [0,1) range
    if point1[0] < 0 || point1[0] >= 1 || point1[1] < 0 || point1[1] >= 1 {
        t.Error("Point coordinates not in [0,1) range")
    }
}

func TestRouting(t *testing.T) {
    // Create a router
    router := NewRouter(2)
    
    // Create zones
    zone1, err := node.NewZone(node.Point{0, 0}, node.Point{0.5, 1})
    if err != nil {
        t.Fatalf("Failed to create zone1: %v", err)
    }
    
    zone2, err := node.NewZone(node.Point{0.5, 0}, node.Point{1, 1})
    if err != nil {
        t.Fatalf("Failed to create zone2: %v", err)
    }
    
    // Create nodes
    node1 := node.NewNode("node1", "localhost:8080", zone1, 2)
    node2 := node.NewNode("node2", "localhost:8081", zone2, 2)
    
    // Add neighbors
    node1.AddNeighbor(node2.ID, node2.Address, node2.Zone)
    node2.AddNeighbor(node1.ID, node1.Address, node1.Zone)
    
    // Test routing to point in local zone
    localPoint := node.Point{0.25, 0.5}
    nextHop, isLocal := router.RouteToPoint(node1, localPoint)
    if !isLocal {
        t.Error("Expected point to be in local zone")
    }
    if nextHop != nil {
        t.Error("Expected no next hop for local point")
    }
    
    // Test routing to point in neighbor's zone
    remotePoint := node.Point{0.75, 0.5}
    nextHop, isLocal = router.RouteToPoint(node1, remotePoint)
    if isLocal {
        t.Error("Expected point to be in remote zone")
    }
    if nextHop == nil {
        t.Error("Expected next hop for remote point")
    }
    if nextHop.ID != node2.ID {
        t.Error("Expected routing to correct neighbor")
    }
}

// Helper function to determine which zone a key belongs to
func findZoneForKey(router *Router, key string, zones ...*node.Zone) *node.Zone {
    point := router.HashToPoint(key)
    
    for _, zone := range zones {
        if zone.Contains(point) {
            return zone
        }
    }
    
    return nil
}

func TestFindResponsibleNode(t *testing.T) {
    router := NewRouter(2)
    
    // Create zones
    zone1, err := node.NewZone(node.Point{0, 0}, node.Point{0.5, 1})
    if err != nil {
        t.Fatalf("Failed to create zone1: %v", err)
    }
    
    zone2, err := node.NewZone(node.Point{0.5, 0}, node.Point{1, 1})
    if err != nil {
        t.Fatalf("Failed to create zone2: %v", err)
    }
    
    // Create nodes
    node1 := node.NewNode("node1", "localhost:8080", zone1, 2)
    node2 := node.NewNode("node2", "localhost:8081", zone2, 2)
    
    // Add neighbors
    node1.AddNeighbor(node2.ID, node2.Address, node2.Zone)
    node2.AddNeighbor(node1.ID, node1.Address, node1.Zone)
    
    // Find a key that belongs to zone1
    var key1 string
    for i := 0; i < 100; i++ {
        testKey := "key1_" + string(rune(i))
        if findZoneForKey(router, testKey, zone1, zone2) == zone1 {
            key1 = testKey
            break
        }
    }
    
    if key1 == "" {
        t.Fatal("Could not find a key that belongs to zone1")
    }
    
    // Find a key that belongs to zone2
    var key2 string
    for i := 0; i < 100; i++ {
        testKey := "key2_" + string(rune(i))
        if findZoneForKey(router, testKey, zone1, zone2) == zone2 {
            key2 = testKey
            break
        }
    }
    
    if key2 == "" {
        t.Fatal("Could not find a key that belongs to zone2")
    }
    
    // Test finding responsible node for key1 (should be node1)
    responsible, isLocal := router.FindResponsibleNode(node1, key1)
    if !isLocal {
        t.Error("Expected node1 to be responsible for key1")
    }
    
    // Test finding responsible node for key2 (should be node2)
    responsible, isLocal = router.FindResponsibleNode(node1, key2)
    if isLocal {
        t.Error("Expected node2 to be responsible for key2")
    }
    if responsible.ID != node2.ID {
        t.Error("Expected node2 to be responsible for key2")
    }
}