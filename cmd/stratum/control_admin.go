package main

import (
	"context"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/raft"
)

// controlAdminService is the slice of AdminService a CONTROL node can answer.
//
// The split is not arbitrary. Every other admin call reads the local stores —
// indexes, documents, chunks, the WAL — so only a node holding data can answer
// it, and that is why the service was registered on storage nodes alone.
// GetClusterStatus is the exception: it reads the Raft view, and the Raft view
// lives ONLY on control nodes. Registering the service on storage nodes and not
// on control nodes therefore made the one call that needs the control group
// unreachable from outside it.
//
// Measured: a storage node's cursor reporter (internal/sync's
// DataVersionReporter) resolves the leader through GetClusterStatus, and every
// attempt failed with
//
//	Unimplemented: unknown service stratum.AdminService
//
// so §7.13.4's periodic report never left the node — taking the chain tails
// (lag catch-up), the "who holds version V" holder view, and the station's read
// routing with it.
//
// Embedding UnimplementedAdminServiceServer keeps the rest of the surface
// honest: those methods answer Unimplemented here, which is exactly what "this
// node holds no stores" means.
type controlAdminService struct {
	pb.UnimplementedAdminServiceServer

	nodeID   int64
	raftNode raft.RaftNode
}

func newControlAdminService(nodeID int64, rn raft.RaftNode) *controlAdminService {
	return &controlAdminService{nodeID: nodeID, raftNode: rn}
}

// GetClusterStatus reports this node's view of the Raft cluster: whether it has
// a leader, which member that is, and how many members there are. It carries no
// data-plane facts, which is why a control node can answer it in full.
//
// The leader id is what the routing layer resolves into the leader's gRPC
// address, so this is the call every storage node makes to find the node its
// cursor report must reach.
func (s *controlAdminService) GetClusterStatus(ctx context.Context, _ *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	cluster, err := s.raftNode.GetClusterStatus(ctx)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	return &pb.GetClusterStatusResponse{
		NodeId:      s.nodeID,
		HasLeader:   cluster.HasLeader,
		LeaderId:    cluster.LeaderID,
		MemberCount: int64(cluster.MemberCount),
	}, nil
}
