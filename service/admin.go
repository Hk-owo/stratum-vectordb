package service

import (
	"context"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/wal"
)

// AdminServiceImpl implements pb.AdminServiceServer.
type AdminServiceImpl struct {
	pb.UnimplementedAdminServiceServer

	raftNode     raft.RaftNode
	indexManager index.IndexManager
	wal          wal.WAL
}

// NewAdminService constructs an AdminServiceImpl.
func NewAdminService(
	rn raft.RaftNode,
	im index.IndexManager,
	w wal.WAL,
) *AdminServiceImpl {
	return &AdminServiceImpl{
		raftNode:     rn,
		indexManager: im,
		wal:          w,
	}
}

// HealthCheck implements AdminServiceServer.
func (s *AdminServiceImpl) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	status := pb.HealthStatus_HEALTH_STATUS_HEALTHY
	details := ""

	// Check Raft connectivity.
	cluster, err := s.raftNode.GetClusterStatus(ctx)
	if err != nil || !cluster.HasLeader {
		status = pb.HealthStatus_HEALTH_STATUS_DEGRADED
		details = "raft: no leader"
	}

	// Check IndexManager.
	if err := s.indexManager.Ping(ctx); err != nil {
		if status == pb.HealthStatus_HEALTH_STATUS_HEALTHY {
			status = pb.HealthStatus_HEALTH_STATUS_DEGRADED
		}
		if details != "" {
			details += "; "
		}
		details += "index manager: " + err.Error()
	}

	if status == pb.HealthStatus_HEALTH_STATUS_HEALTHY && details == "" {
		details = "ok"
	}

	return &pb.HealthCheckResponse{Status: status, Details: details}, nil
}

// GetSystemStatus implements AdminServiceServer.
func (s *AdminServiceImpl) GetSystemStatus(ctx context.Context, req *pb.GetSystemStatusRequest) (*pb.GetSystemStatusResponse, error) {
	health, _ := s.HealthCheck(ctx, &pb.HealthCheckRequest{})

	// Scan all KBs for stuck versions (FAILED status for extended period).
	// In a real implementation this would scan all KBs' versions.
	// For now, return an empty list.

	// WAL replay counters.
	var walAlerts []*pb.WALAlert
	for _, rc := range s.wal.GetReplayCounters() {
		desc := "WAL record stuck"
		if rc.Record.Type == 0 { // PendingRecordTypeDeleteMark
			desc = "delete mark for " + rc.Record.KBID
		}
		walAlerts = append(walAlerts, &pb.WALAlert{
			Description: desc,
			RetryCount:  int32(rc.RetryCount),
		})
	}

	return &pb.GetSystemStatusResponse{
		Health:      health,
		WalAlerts:   walAlerts,
		ResourceUsage: &pb.ResourceUsage{},
	}, nil
}

// RebuildIndex implements AdminServiceServer.
func (s *AdminServiceImpl) RebuildIndex(ctx context.Context, req *pb.RebuildIndexRequest) (*pb.RebuildIndexResponse, error) {
	// Set status to PENDING, then trigger build.
	if err := s.raftNode.ProposeUpdateVersionStatus(ctx, req.VersionId, 0); err != nil { // 0 = IndexStatusPending
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	if err := s.indexManager.TriggerBuild(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.RebuildIndexResponse{Success: true}, nil
}

// WarmupVersion implements AdminServiceServer.
func (s *AdminServiceImpl) WarmupVersion(ctx context.Context, req *pb.WarmupVersionRequest) (*pb.WarmupVersionResponse, error) {
	// Warmup loads the index (Search with a dummy vector triggers load).
	// Use a zero-vector search with topK=0 just to force the index load.
	// A real implementation would have a dedicated LoadIndex RPC, but
	// the current interface uses Search for this purpose.
	if err := s.indexManager.TriggerBuild(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.WarmupVersionResponse{Success: true}, nil
}

var _ pb.AdminServiceServer = (*AdminServiceImpl)(nil)
