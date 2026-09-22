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

// ListDeletedVersions answers the tombstones this node's state machine holds for
// one knowledge base — the read a storage node needs to tell "deleted" from
// "absent" (§7.5, docs/known-gaps.md §B).
//
// No leader requirement, unlike Propose: the verdict is replicated state, so any
// node holding it answers the same thing, and making the caller find the leader
// would add a redirect hop to a read.
func (s *InternalServiceServer) ListDeletedVersions(ctx context.Context, req *pb.ListDeletedVersionsRequest) (*pb.ListDeletedVersionsResponse, error) {
	ids, err := s.node.DeletionsInRange(ctx, req.GetKnowledgeBaseId(), req.GetFromExclusive(), req.GetToInclusive())
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	return &pb.ListDeletedVersionsResponse{VersionIds: ids}, nil
}

// VersionLiveness answers "which of these versions are still alive, and how far has
// allocation got" — the liveness read that does not decay. Unlike the tombstones above,
// neither fact has a lifetime, so pruning cannot take the evidence away
// (docs/known-gaps.md §B).
//
// No leader requirement, for the same reason ListDeletedVersions has none: the fact is
// replicated state, so any node holding it answers the same thing.
func (s *InternalServiceServer) VersionLiveness(ctx context.Context, req *pb.VersionLivenessRequest) (*pb.VersionLivenessResponse, error) {
	alive, lastAllocated, err := s.node.VersionLiveness(ctx, req.GetKnowledgeBaseId(), req.FromExclusive, req.ToInclusive)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	return &pb.VersionLivenessResponse{
		AliveVersionIds:      alive,
		LastAllocatedVersion: lastAllocated,
	}, nil
}

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
