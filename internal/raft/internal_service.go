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

// VersionLiveness answers "which of these versions are still alive, and how far has
// allocation got" — the liveness read that does not decay: neither fact has a
// lifetime, so nothing can take the evidence away (docs/known-gaps.md §B).
//
// No leader requirement, unlike Propose: the fact is replicated state, so any node
// holding it answers the same thing, and finding the leader would add a redirect hop.
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

// DocIDSetHashReader adapts any RaftNode's per-version view onto the one fact the sync
// paths need from the replicated metadata: the document-set digest its writer committed.
//
// It exists because an empty data transfer is ambiguous — a version with no documents and
// a source with no data for the version both arrive as an empty stream that reports
// success — and the writer's digest is the only thing that separates them (see
// sync.Follower.confirmVersionIsEmpty). Any RaftNode answers it: a voter reads its own
// state machine, and a storage node asks the control tier through GetVersion.
type DocIDSetHashReader struct{ Node RaftNode }

// DocIDSetHash implements sync.VersionDocIDSetHash.
func (r DocIDSetHashReader) DocIDSetHash(ctx context.Context, kbID string, versionID int64) (string, error) {
	v, err := r.Node.GetVersion(ctx, kbID, versionID)
	if err != nil {
		return "", err
	}
	return v.DocIDSetHash, nil
}

// errorMessage renders err for the wire, or "" when there is nothing to say.
func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
