package node

import (
	"math"
	"testing"
)

func TestNewZone(t *testing.T) {
	// Test valid zone creation
	min := Point{0.0, 0.0}
	max := Point{1.0, 1.0}
	zone, err := NewZone(min, max)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if zone.MinPoint[0] != 0.0 || zone.MinPoint[1] != 0.0 {
		t.Errorf("Expected min point [0.0, 0.0], got: %v", zone.MinPoint)
	}

	if zone.MaxPoint[0] != 1.0 || zone.MaxPoint[1] != 1.0 {
		t.Errorf("Expected max point [1.0, 1.0], got: %v", zone.MaxPoint)
	}

	// Test invalid dimensions
	min = Point{0.0}
	max = Point{1.0, 1.0}
	_, err = NewZone(min, max)

	if err == nil {
		t.Error("Expected error for mismatched dimensions, got none")
	}

	// Test invalid min/max
	min = Point{1.0, 1.0}
	max = Point{0.5, 1.0}
	_, err = NewZone(min, max)

	if err == nil {
		t.Error("Expected error for min > max, got none")
	}
}

func TestZoneContains(t *testing.T) {
	min := Point{0.0, 0.0}
	max := Point{1.0, 1.0}
	zone, _ := NewZone(min, max)

	// Test point inside zone
	p := Point{0.5, 0.5}
	if !zone.Contains(p) {
		t.Errorf("Expected zone to contain point %v", p)
	}

	// Test point on min boundary (should be contained)
	p = Point{0.0, 0.0}
	if !zone.Contains(p) {
		t.Errorf("Expected zone to contain point %v", p)
	}

	// Test point on max boundary (should not be contained, as boundary is exclusive)
	p = Point{1.0, 1.0}
	if zone.Contains(p) {
		t.Errorf("Expected zone to not contain point %v", p)
	}

	// Test point outside zone
	p = Point{1.5, 0.5}
	if zone.Contains(p) {
		t.Errorf("Expected zone to not contain point %v", p)
	}

	// Test point with wrong dimensions
	p = Point{0.5, 0.5, 0.5}
	if zone.Contains(p) {
		t.Errorf("Expected zone to not contain point %v with different dimensions", p)
	}
}

func TestZoneSplit(t *testing.T) {
	min := Point{0.0, 0.0}
	max := Point{1.0, 1.0}
	zone, _ := NewZone(min, max)

	// Split along first dimension
	first, second, err := zone.Split(0)

	if err != nil {
		t.Fatalf("Expected no error on split, got: %v", err)
	}

	// Check first half
	if first.MinPoint[0] != 0.0 || first.MinPoint[1] != 0.0 {
		t.Errorf("Expected first half min [0.0, 0.0], got: %v", first.MinPoint)
	}

	if first.MaxPoint[0] != 0.5 || first.MaxPoint[1] != 1.0 {
		t.Errorf("Expected first half max [0.5, 1.0], got: %v", first.MaxPoint)
	}

	// Check second half
	if second.MinPoint[0] != 0.5 || second.MinPoint[1] != 0.0 {
		t.Errorf("Expected second half min [0.5, 0.0], got: %v", second.MinPoint)
	}

	if second.MaxPoint[0] != 1.0 || second.MaxPoint[1] != 1.0 {
		t.Errorf("Expected second half max [1.0, 1.0], got: %v", second.MaxPoint)
	}

	// Test invalid dimension
	_, _, err = zone.Split(2)
	if err == nil {
		t.Error("Expected error for invalid dimension, got none")
	}
}

func TestDistance(t *testing.T) {
	p1 := Point{0.0, 0.0}
	p2 := Point{3.0, 4.0}

	d, err := Distance(p1, p2)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if math.Abs(d-5.0) > 1e-9 {
		t.Errorf("Expected distance 5.0, got: %v", d)
	}

	// Test different dimensions
	p3 := Point{1.0, 2.0, 3.0}
	_, err = Distance(p1, p3)

	if err == nil {
		t.Error("Expected error for different dimensions, got none")
	}
}

func TestDistanceToZone(t *testing.T) {
	min := Point{1.0, 1.0}
	max := Point{3.0, 3.0}
	zone, _ := NewZone(min, max)

	// Test point inside zone (distance should be 0)
	p := Point{2.0, 2.0}
	d, err := DistanceToZone(p, zone)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if d != 0.0 {
		t.Errorf("Expected distance 0.0 for point inside zone, got: %v", d)
	}

	// Test point outside zone
	p = Point{0.0, 0.0}
	d, err = DistanceToZone(p, zone)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Distance should be sqrt(2) = distance from (0,0) to (1,1)
	expectedDist := math.Sqrt(2.0)
	if math.Abs(d-expectedDist) > 1e-9 {
		t.Errorf("Expected distance %v, got: %v", expectedDist, d)
	}

	// Test different dimensions
	p = Point{1.0, 2.0, 3.0}
	_, err = DistanceToZone(p, zone)

	if err == nil {
		t.Error("Expected error for different dimensions, got none")
	}
}
