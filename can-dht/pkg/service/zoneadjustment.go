package service

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"
	
	"github.com/can-dht/pkg/node"
	pb "github.com/can-dht/internal/proto"
)

// ZoneAdjustmentProposal represents a proposed zone boundary adjustment
type ZoneAdjustmentProposal struct {
	// ProposerID is the node proposing the adjustment
	ProposerID node.NodeID
	
	// TargetID is the node targeted for adjustment
	TargetID node.NodeID
	
	// Dimension is which dimension to adjust
	Dimension int
	
	// ProposedBoundary is the proposed new boundary position
	ProposedBoundary float64
	
	// CurrentLoad is the load factor of the proposer
	CurrentLoad float64
	
	// ProposalID is a unique identifier for this proposal
	ProposalID string
	
	// Timestamp of the proposal
	Timestamp time.Time
}

// identifyOverloadedDimensions finds dimensions where the node's zone is larger than average
func (s *CANServer) identifyOverloadedDimensions() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	dimensions := make([]int, 0, s.Config.Dimensions)
	
	// For each dimension, calculate ratio of our zone size to "fair" size
	avgZoneVolume := s.estimateAverageZoneVolume()
	fairSizePerDimension := math.Pow(avgZoneVolume, 1.0/float64(s.Config.Dimensions))
	
	for dim := 0; dim < s.Config.Dimensions; dim++ {
		ourSize := s.Node.Zone.MaxPoint[dim] - s.Node.Zone.MinPoint[dim]
		
		// If our dimension is significantly larger than fair share, add it to the list
		if ourSize > fairSizePerDimension*1.5 {
			dimensions = append(dimensions, dim)
		}
	}
	
	return dimensions
}

// calculateOptimalBoundary determines the ideal boundary for zone adjustment
func (s *CANServer) calculateOptimalBoundary(
	neighborID node.NodeID, 
	dimension int, 
	neighborZone *node.Zone) float64 {
	
	// Get the load scores
	ourLoadFactor := 1.0
	neighborLoadFactor := 1.0
	
	s.LoadStats.mu.RLock()
	ourLoadFactor = s.LoadStats.ZoneLoadFactor
	s.LoadStats.mu.RUnlock()
	
	// For neighbor, use our estimation function
	neighborLoadFactor = s.getNodeLoadScore(neighborID)
	
	// Calculate the current boundary position
	var currentBoundary float64
	
	// Determine the boundary orientation
	if s.Node.Zone.MaxPoint[dimension] == neighborZone.MinPoint[dimension] {
		// We are on the "left" of the neighbor
		currentBoundary = s.Node.Zone.MaxPoint[dimension]
	} else if s.Node.Zone.MinPoint[dimension] == neighborZone.MaxPoint[dimension] {
		// We are on the "right" of the neighbor
		currentBoundary = s.Node.Zone.MinPoint[dimension]
	} else {
		// Not adjacent on this dimension
		return -1.0
	}
	
	// Calculate a new boundary that would balance load
	// If we're more loaded, we want to move the boundary to make our zone smaller
	// If they're more loaded, we want to move the boundary to make their zone smaller
	if ourLoadFactor > neighborLoadFactor*1.3 {
		// We're significantly more loaded, shrink our zone
		// Calculate how much to move the boundary (10-30% of our dimension size)
		ourDimSize := s.Node.Zone.MaxPoint[dimension] - s.Node.Zone.MinPoint[dimension]
		moveAmount := ourDimSize * math.Min(0.3, (ourLoadFactor-neighborLoadFactor)/ourLoadFactor)
		
		if s.Node.Zone.MaxPoint[dimension] == currentBoundary {
			// Move boundary left (decrease our max)
			return currentBoundary - moveAmount
		} else {
			// Move boundary right (increase our min)
			return currentBoundary + moveAmount
		}
	} else if neighborLoadFactor > ourLoadFactor*1.3 {
		// Neighbor is significantly more loaded, expand our zone
		// Calculate how much to move the boundary (10-30% of neighbor dimension size)
		neighborDimSize := neighborZone.MaxPoint[dimension] - neighborZone.MinPoint[dimension]
		moveAmount := neighborDimSize * math.Min(0.3, (neighborLoadFactor-ourLoadFactor)/neighborLoadFactor)
		
		if s.Node.Zone.MaxPoint[dimension] == currentBoundary {
			// Move boundary right (increase our max)
			return currentBoundary + moveAmount
		} else {
			// Move boundary left (decrease our min)
			return currentBoundary - moveAmount
		}
	}
	
	// If loads are similar, don't adjust
	return currentBoundary
}

// AdjustZone implements the zone adjustment mechanism for load balancing
func (s *CANServer) AdjustZone(ctx context.Context, neighborID node.NodeID) error {
	log.Printf("Considering zone adjustment with neighbor %s", neighborID)
	
	s.mu.RLock()
	
	// Check if the neighbor is still valid
	neighborInfo, exists := s.Node.Neighbors[neighborID]
	if !exists {
		s.mu.RUnlock()
		return fmt.Errorf("neighbor %s is no longer valid", neighborID)
	}
	
	// Identify overloaded dimensions
	overloadedDimensions := s.identifyOverloadedDimensions()
	if len(overloadedDimensions) == 0 {
		s.mu.RUnlock()
		return fmt.Errorf("no overloaded dimensions to adjust")
	}
	
	// Find a dimension where we're adjacent to this neighbor
	var adjustDimension int = -1
	for _, dim := range overloadedDimensions {
		// Check if we share a boundary on this dimension
		if (s.Node.Zone.MaxPoint[dim] == neighborInfo.Zone.MinPoint[dim]) ||
		   (s.Node.Zone.MinPoint[dim] == neighborInfo.Zone.MaxPoint[dim]) {
			adjustDimension = dim
			break
		}
	}
	
	if adjustDimension == -1 {
		s.mu.RUnlock()
		return fmt.Errorf("no suitable dimension for adjustment with neighbor %s", neighborID)
	}
	
	// Calculate the optimal boundary position
	newBoundary := s.calculateOptimalBoundary(neighborID, adjustDimension, neighborInfo.Zone)
	if newBoundary < 0 {
		s.mu.RUnlock()
		return fmt.Errorf("failed to calculate optimal boundary")
	}
	
	// If the boundary hasn't changed significantly, skip adjustment
	var currentBoundary float64
	if s.Node.Zone.MaxPoint[adjustDimension] == neighborInfo.Zone.MinPoint[adjustDimension] {
		currentBoundary = s.Node.Zone.MaxPoint[adjustDimension]
	} else {
		currentBoundary = s.Node.Zone.MinPoint[adjustDimension]
	}
	
	if math.Abs(newBoundary-currentBoundary) < 0.05 {
		s.mu.RUnlock()
		return fmt.Errorf("calculated boundary adjustment too small to implement")
	}
	
	// Create a zone adjustment proposal
	proposal := &ZoneAdjustmentProposal{
		ProposerID:       s.Node.ID,
		TargetID:         neighborID,
		Dimension:        adjustDimension,
		ProposedBoundary: newBoundary,
		CurrentLoad:      s.LoadStats.ZoneLoadFactor,
		ProposalID:       fmt.Sprintf("%s-%s-%d-%d", s.Node.ID, neighborID, adjustDimension, time.Now().UnixNano()),
		Timestamp:        time.Now(),
	}
	
	// Get neighbor address for later
	neighborAddress := neighborInfo.Address
	
	s.mu.RUnlock()
	
	// Propose the zone adjustment to the neighbor
	return s.proposeZoneAdjustment(ctx, neighborAddress, proposal)
}

// proposeZoneAdjustment sends a zone adjustment proposal to a neighbor
func (s *CANServer) proposeZoneAdjustment(ctx context.Context, neighborAddress string, proposal *ZoneAdjustmentProposal) error {
	log.Printf("Proposing zone adjustment to %s: dim=%d, newBoundary=%.4f", 
		proposal.TargetID, proposal.Dimension, proposal.ProposedBoundary)
	
	// Connect to the neighbor
	client, conn, err := ConnectToNode(ctx, neighborAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to neighbor for zone adjustment: %w", err)
	}
	defer conn.Close()
	
	// Create the proposal request
	req := &pb.ZoneAdjustmentProposalRequest{
		ProposerId:       string(proposal.ProposerID),
		TargetId:         string(proposal.TargetID),
		Dimension:        int32(proposal.Dimension),
		ProposedBoundary: proposal.ProposedBoundary,
		CurrentLoad:      proposal.CurrentLoad,
		ProposalId:       proposal.ProposalID,
	}
	
	// Send the proposal
	resp, err := client.ProposeZoneAdjustment(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send zone adjustment proposal: %w", err)
	}
	
	if !resp.Accepted {
		return fmt.Errorf("zone adjustment proposal rejected: %s", resp.Reason)
	}
	
	// If accepted, perform the adjustment
	return s.executeZoneAdjustment(proposal)
}

// ExecuteZoneAdjustment performs the actual zone adjustment
func (s *CANServer) executeZoneAdjustment(proposal *ZoneAdjustmentProposal) error {
	log.Printf("Executing zone adjustment: dim=%d, newBoundary=%.4f", 
		proposal.Dimension, proposal.ProposedBoundary)
	
	s.mu.Lock()
	
	// Check again if the neighbor is still valid
	neighborInfo, exists := s.Node.Neighbors[proposal.TargetID]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("neighbor %s is no longer valid", proposal.TargetID)
	}
	
	// Determine the direction of adjustment
	isAdjustingMax := false
	if s.Node.Zone.MaxPoint[proposal.Dimension] == neighborInfo.Zone.MinPoint[proposal.Dimension] {
		isAdjustingMax = true
	}
	
	// Create the new zones
	oldZone := s.Node.Zone
	newZone := *oldZone // Make a copy
	
	// Create the new neighbor zone
	oldNeighborZone := neighborInfo.Zone
	newNeighborZone := *oldNeighborZone // Make a copy
	
	// Adjust the zones
	if isAdjustingMax {
		// We're adjusting our max point and neighbor's min point
		newZone.MaxPoint[proposal.Dimension] = proposal.ProposedBoundary
		newNeighborZone.MinPoint[proposal.Dimension] = proposal.ProposedBoundary
	} else {
		// We're adjusting our min point and neighbor's max point
		newZone.MinPoint[proposal.Dimension] = proposal.ProposedBoundary
		newNeighborZone.MaxPoint[proposal.Dimension] = proposal.ProposedBoundary
	}
	
	// Identify keys that need to be transferred
	keysToTransfer := make([]string, 0)
	for key := range s.Node.Data {
		point := s.Router.HashToPoint(key)
		// If the point is no longer in our zone, transfer it
		if !newZone.Contains(point) && oldNeighborZone.Contains(point) {
			keysToTransfer = append(keysToTransfer, key)
		}
	}
	
	// Update our zone
	s.Node.Zone = &newZone
	
	// Update the neighbor's zone in our records
	neighborInfo.Zone = &newNeighborZone
	
	s.mu.Unlock()
	
	// Transfer the necessary keys
	if len(keysToTransfer) > 0 {
		log.Printf("Transferring %d keys to neighbor %s due to zone adjustment", 
			len(keysToTransfer), proposal.TargetID)
		
		go s.transferKeysToNeighbor(context.Background(), proposal.TargetID, keysToTransfer)
	}
	
	// Update our load factor
	s.UpdateZoneLoadFactor()
	
	// Notify other neighbors about the zone change
	go s.broadcastZoneChange()
	
	return nil
}

// transferKeysToNeighbor transfers keys to a neighbor after zone adjustment
func (s *CANServer) transferKeysToNeighbor(ctx context.Context, neighborID node.NodeID, keys []string) {
	s.mu.RLock()
	neighborInfo, exists := s.Node.Neighbors[neighborID]
	if !exists {
		s.mu.RUnlock()
		log.Printf("Cannot transfer keys: neighbor %s no longer exists", neighborID)
		return
	}
	neighborAddress := neighborInfo.Address
	s.mu.RUnlock()
	
	// Connect to the neighbor
	client, conn, err := ConnectToNode(ctx, neighborAddress)
	if err != nil {
		log.Printf("Failed to connect to neighbor %s for key transfer: %v", neighborID, err)
		return
	}
	defer conn.Close()
	
	// Transfer each key
	for _, key := range keys {
		// Get the key value
		value, exists, err := s.Store.Get(key)
		if err != nil || !exists {
			log.Printf("Failed to get key %s for transfer: %v", key, err)
			continue
		}
		
		// Create the transfer request
		req := &pb.TransferKeyRequest{
			Key:   key,
			Value: value,
		}
		
		// Send the transfer request
		_, err = client.TransferKey(ctx, req)
		if err != nil {
			log.Printf("Failed to transfer key %s to neighbor %s: %v", key, neighborID, err)
			continue
		}
		
		// Delete the key locally after successful transfer
		if err := s.Store.Delete(key); err != nil {
			log.Printf("Warning: Failed to delete key %s locally after transfer: %v", key, err)
		}
	}
}

// broadcastZoneChange notifies all neighbors about a zone change
func (s *CANServer) broadcastZoneChange() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	s.mu.RLock()
	neighbors := s.Node.GetNeighbors()
	nodeInfo := &pb.NodeInfo{
		Id:      string(s.Node.ID),
		Address: s.Node.Address,
		Zone: &pb.Zone{
			MinPoint: &pb.Point{
				Coordinates: s.Node.Zone.MinPoint,
			},
			MaxPoint: &pb.Point{
				Coordinates: s.Node.Zone.MaxPoint,
			},
		},
	}
	s.mu.RUnlock()
	
	// Notify each neighbor
	for id, info := range neighbors {
		// Connect to the neighbor
		client, conn, err := ConnectToNode(ctx, info.Address)
		if err != nil {
			log.Printf("Failed to connect to neighbor %s for zone change notification: %v", id, err)
			continue
		}
		
		// Create the notification request
		req := &pb.UpdateNeighborsRequest{
			NodeId: string(s.Node.ID),
			Neighbors: []*pb.NodeInfo{nodeInfo},
		}
		
		// Send the notification
		_, err = client.UpdateNeighbors(ctx, req)
		conn.Close()
		
		if err != nil {
			log.Printf("Failed to notify neighbor %s about zone change: %v", id, err)
		}
	}
}

// HandleZoneAdjustmentProposal handles a zone adjustment proposal from a neighbor
func (s *CANServer) HandleZoneAdjustmentProposal(proposal *pb.ZoneAdjustmentProposalRequest) (*pb.ZoneAdjustmentProposalResponse, error) {
	log.Printf("Received zone adjustment proposal from %s: dim=%d, newBoundary=%.4f", 
		proposal.ProposerId, proposal.Dimension, proposal.ProposedBoundary)
	
	// Get our current load factor
	s.LoadStats.mu.RLock()
	ourLoadFactor := s.LoadStats.ZoneLoadFactor
	s.LoadStats.mu.RUnlock()
	
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Quick validation checks
	if proposal.TargetId != string(s.Node.ID) {
		return &pb.ZoneAdjustmentProposalResponse{
			Accepted: false,
			Reason:   "proposal targeted at a different node",
		}, nil
	}
	
	// Check if the dimension is valid
	if int(proposal.Dimension) >= s.Config.Dimensions {
		return &pb.ZoneAdjustmentProposalResponse{
			Accepted: false,
			Reason:   "invalid dimension",
		}, nil
	}
	
	// Check if the proposer is our neighbor
	neighborInfo, isNeighbor := s.Node.Neighbors[node.NodeID(proposal.ProposerId)]
	if !isNeighbor {
		return &pb.ZoneAdjustmentProposalResponse{
			Accepted: false,
			Reason:   "proposer is not a neighbor",
		}, nil
	}
	
	// Determine which boundary we're adjusting
	isAdjustingMin := false
	isAdjustingMax := false
	
	if s.Node.Zone.MinPoint[proposal.Dimension] == neighborInfo.Zone.MaxPoint[proposal.Dimension] {
		isAdjustingMin = true
	} else if s.Node.Zone.MaxPoint[proposal.Dimension] == neighborInfo.Zone.MinPoint[proposal.Dimension] {
		isAdjustingMax = true
	}
	
	if (!isAdjustingMin && !isAdjustingMax) {
		return &pb.ZoneAdjustmentProposalResponse{
			Accepted: false,
			Reason:   "not adjacent on the proposed dimension",
		}, nil
	}
	
	// Check if the boundary position is valid
	if isAdjustingMin {
		// We're adjusting our min point, make sure it doesn't go below 0 or above our max
		if proposal.ProposedBoundary < 0 || proposal.ProposedBoundary >= s.Node.Zone.MaxPoint[proposal.Dimension] {
			return &pb.ZoneAdjustmentProposalResponse{
				Accepted: false,
				Reason:   "invalid boundary position for min point",
			}, nil
		}
	} else {
		// We're adjusting our max point, make sure it doesn't go above 1 or below our min
		if proposal.ProposedBoundary > 1 || proposal.ProposedBoundary <= s.Node.Zone.MinPoint[proposal.Dimension] {
			return &pb.ZoneAdjustmentProposalResponse{
				Accepted: false,
				Reason:   "invalid boundary position for max point",
			}, nil
		}
	}
	
	// Make sure the adjustment doesn't make our zone too small
	newSize := s.Node.Zone.MaxPoint[proposal.Dimension] - s.Node.Zone.MinPoint[proposal.Dimension]
	if isAdjustingMin {
		newSize = s.Node.Zone.MaxPoint[proposal.Dimension] - proposal.ProposedBoundary
	} else {
		newSize = proposal.ProposedBoundary - s.Node.Zone.MinPoint[proposal.Dimension]
	}
	
	if newSize < 0.1 {
		return &pb.ZoneAdjustmentProposalResponse{
			Accepted: false,
			Reason:   "adjustment would make zone too small",
		}, nil
	}
	
	// If we're already more loaded than them, and they want to make our zone bigger, reject
	if ourLoadFactor > proposal.CurrentLoad*1.2 &&
		((isAdjustingMin && proposal.ProposedBoundary < s.Node.Zone.MinPoint[proposal.Dimension]) ||
		 (isAdjustingMax && proposal.ProposedBoundary > s.Node.Zone.MaxPoint[proposal.Dimension])) {
		return &pb.ZoneAdjustmentProposalResponse{
			Accepted: false,
			Reason:   "rejecting zone increase when already more loaded than proposer",
		}, nil
	}
	
	// Accept the proposal
	return &pb.ZoneAdjustmentProposalResponse{
		Accepted: true,
		Reason:   "",
	}, nil
} 