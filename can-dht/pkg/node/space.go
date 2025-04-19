package node

import (
	"fmt"
	"math"
)

// Point represents a point in the d-dimensional coordinate space
type Point []float64

// Zone represents a zone in the CAN coordinate space
type Zone struct {
	// MinPoint represents the minimum coordinates of the zone
	MinPoint Point
	// MaxPoint represents the maximum coordinates of the zone
	MaxPoint Point
}

// NewZone creates a new zone with the given min and max points
func NewZone(min, max Point) (*Zone, error) {
	if len(min) != len(max) {
		return nil, fmt.Errorf("min and max points must have the same dimensionality")
	}

	for i := range min {
		if min[i] > max[i] {
			return nil, fmt.Errorf("min point must be smaller than max point in all dimensions")
		}
	}

	return &Zone{
		MinPoint: min,
		MaxPoint: max,
	}, nil
}

// Contains checks if a point is contained within the zone
func (z *Zone) Contains(p Point) bool {
	if len(p) != len(z.MinPoint) {
		return false
	}

	for i := range p {
		if p[i] < z.MinPoint[i] || p[i] >= z.MaxPoint[i] {
			return false
		}
	}
	return true
}

// Split splits the zone into two along the specified dimension
func (z *Zone) Split(dimension int) (*Zone, *Zone, error) {
	if dimension >= len(z.MinPoint) {
		return nil, nil, fmt.Errorf("invalid dimension for splitting: %d", dimension)
	}

	// Find the midpoint along the specified dimension
	mid := (z.MinPoint[dimension] + z.MaxPoint[dimension]) / 2

	// Create the first half
	firstHalfMin := make(Point, len(z.MinPoint))
	firstHalfMax := make(Point, len(z.MaxPoint))
	copy(firstHalfMin, z.MinPoint)
	copy(firstHalfMax, z.MaxPoint)
	firstHalfMax[dimension] = mid

	// Create the second half
	secondHalfMin := make(Point, len(z.MinPoint))
	secondHalfMax := make(Point, len(z.MaxPoint))
	copy(secondHalfMin, z.MinPoint)
	copy(secondHalfMax, z.MaxPoint)
	secondHalfMin[dimension] = mid

	first, err := NewZone(firstHalfMin, firstHalfMax)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create first half: %w", err)
	}

	second, err := NewZone(secondHalfMin, secondHalfMax)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create second half: %w", err)
	}

	return first, second, nil
}

// Distance calculates the Euclidean distance between two points
func Distance(p1, p2 Point) (float64, error) {
	if len(p1) != len(p2) {
		return 0, fmt.Errorf("points must have the same dimensionality")
	}

	var sum float64
	for i := range p1 {
		diff := p1[i] - p2[i]
		sum += diff * diff
	}
	return math.Sqrt(sum), nil
}

// DistanceToZone calculates the minimum Euclidean distance from a point to any point in a zone
func DistanceToZone(p Point, z *Zone) (float64, error) {
	if len(p) != len(z.MinPoint) {
		return 0, fmt.Errorf("point and zone must have the same dimensionality")
	}

	// Calculate the closest point in the zone to p
	closestPoint := make(Point, len(p))
	for i := range p {
		if p[i] < z.MinPoint[i] {
			closestPoint[i] = z.MinPoint[i]
		} else if p[i] >= z.MaxPoint[i] {
			closestPoint[i] = z.MaxPoint[i]
		} else {
			closestPoint[i] = p[i]
		}
	}

	return Distance(p, closestPoint)
}

// MergeZones creates a new zone that encompasses both input zones
func MergeZones(zone1, zone2 *Zone) (*Zone, error) {
	if zone1 == nil || zone2 == nil {
		return nil, fmt.Errorf("cannot merge nil zones")
	}
	
	if len(zone1.MinPoint) != len(zone2.MinPoint) {
		return nil, fmt.Errorf("cannot merge zones with different dimensions")
	}
	
	minPoint := make(Point, len(zone1.MinPoint))
	maxPoint := make(Point, len(zone1.MaxPoint))
	
	for i := 0; i < len(minPoint); i++ {
		minPoint[i] = math.Min(zone1.MinPoint[i], zone2.MinPoint[i])
		maxPoint[i] = math.Max(zone1.MaxPoint[i], zone2.MaxPoint[i])
	}
	
	return NewZone(minPoint, maxPoint)
}

// OptimallySplitMergedZone takes a primary zone and a zone to be merged, and splits them optimally
func OptimallySplitMergedZone(primaryZone, secondaryZone *Zone) (*Zone, *Zone, error) {
	if primaryZone == nil || secondaryZone == nil {
		return nil, nil, fmt.Errorf("cannot split nil zones")
	}
	
	// First create the merged zone
	mergedZone, err := MergeZones(primaryZone, secondaryZone)
	if err != nil {
		return nil, nil, err
	}
	
	// Find the dimension with the longest side for splitting
	maxDim := 0
	maxSize := mergedZone.MaxPoint[0] - mergedZone.MinPoint[0]
	
	for dim := 1; dim < len(mergedZone.MinPoint); dim++ {
		size := mergedZone.MaxPoint[dim] - mergedZone.MinPoint[dim]
		if size > maxSize {
			maxSize = size
			maxDim = dim
		}
	}
	
	// Split the merged zone along the longest dimension
	// Try to make the split position reflect the relative sizes of the original zones
	primaryVolume := 1.0
	for i := 0; i < len(primaryZone.MinPoint); i++ {
		primaryVolume *= (primaryZone.MaxPoint[i] - primaryZone.MinPoint[i])
	}
	
	secondaryVolume := 1.0
	for i := 0; i < len(secondaryZone.MinPoint); i++ {
		secondaryVolume *= (secondaryZone.MaxPoint[i] - secondaryZone.MinPoint[i])
	}
	
	totalVolume := primaryVolume + secondaryVolume
	primaryRatio := primaryVolume / totalVolume
	
	// Calculate split point based on volume ratio
	splitPoint := mergedZone.MinPoint[maxDim] + primaryRatio*(mergedZone.MaxPoint[maxDim]-mergedZone.MinPoint[maxDim])
	
	// Create the two new zones
	firstMinPoint := make(Point, len(mergedZone.MinPoint))
	firstMaxPoint := make(Point, len(mergedZone.MaxPoint))
	secondMinPoint := make(Point, len(mergedZone.MinPoint))
	secondMaxPoint := make(Point, len(mergedZone.MaxPoint))
	
	for i := 0; i < len(mergedZone.MinPoint); i++ {
		firstMinPoint[i] = mergedZone.MinPoint[i]
		firstMaxPoint[i] = mergedZone.MaxPoint[i]
		secondMinPoint[i] = mergedZone.MinPoint[i]
		secondMaxPoint[i] = mergedZone.MaxPoint[i]
	}
	
	// Set the split dimension
	firstMaxPoint[maxDim] = splitPoint
	secondMinPoint[maxDim] = splitPoint
	
	// Create the two new zones
	firstZone, err := NewZone(firstMinPoint, firstMaxPoint)
	if err != nil {
		return nil, nil, err
	}
	
	secondZone, err := NewZone(secondMinPoint, secondMaxPoint)
	if err != nil {
		return nil, nil, err
	}
	
	return firstZone, secondZone, nil
}
