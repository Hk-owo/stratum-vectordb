package raft

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
)

// InternalServiceServer serves node-to-node control-plane traffic — currently
// only Propose, the funnel that lets any node initiate a Raft proposal
// (Stratum_设计文档v13.md §7.3).
type InternalServiceServer struct {
	pb.UnimplementedInternalServiceServer

	node *RaftNodeImpl
}

// NewInternalServiceServer returns the handler for node's internal service.
func NewInternalServiceServer(node *RaftNodeImpl) *InternalServiceServer {
	return &InternalServiceServer{node: node}
}

var _ pb.InternalServiceServer = (*InternalServiceServer)(nil)

// Propose decodes a forwarded command and runs it through this node's Raft.
//
// A node that is not the leader answers with a redirect (leader_id) instead of
// forwarding onwards: a chained forward would multiply the work and could loop
// forever between two peers that each believe the other leads.
func (s *InternalServiceServer) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
	cmd, err := decodeCommand(req.GetCommand())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "internal: decode forwarded command: %v", err)
	}

	// Refuse rather than forward: whoever asked us should ask the real leader.
	// A leadership change between this check and the append below is fine — the
	// append itself reports ErrNotLeader, and that is surfaced as Unavailable.
	if leaderID, known := s.node.raft.LeaderID(); !known || leaderID != s.node.nodeID {
		return &pb.ProposeResponse{LeaderId: leaderID}, nil
	}

	res, err := s.node.proposeAndWait(ctx, cmd)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "internal: propose: %v", err)
	}
	return &pb.ProposeResponse{
		VersionId:         res.VersionID,
		DeletedVersionIds: res.DeletedVersionIDs,
		ErrorName:         stratumerrors.Name(res.Err),
		ErrorMessage:      errorMessage(res.Err),
	}, nil
}

// errorMessage renders err for the wire, or "" when there is nothing to say.
func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
