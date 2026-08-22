package service

import (
	"context"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// AdminServiceImpl implements pb.AdminServiceServer.
type AdminServiceImpl struct {
	pb.UnimplementedAdminServiceServer

	nodeID       int64
	raftNode     raft.RaftNode
	indexManager index.IndexManager
	docStore     docstore.DocStore
	chunkStore   chunkstore.ChunkStore
	wal          wal.WAL
}

// NewAdminService constructs an AdminServiceImpl. nodeID identifies this
// node in the Raft cluster; it is reported by GetClusterStatus so the
// routing layer can resolve the leader's gRPC address from its node list.
func NewAdminService(
	nodeID int64,
	rn raft.RaftNode,
	im index.IndexManager,
	ds docstore.DocStore,
	cs chunkstore.ChunkStore,
	w wal.WAL,
) *AdminServiceImpl {
	return &AdminServiceImpl{
		nodeID:       nodeID,
		raftNode:     rn,
		indexManager: im,
		docStore:     ds,
		chunkStore:   cs,
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

	// Scan all KBs for stuck (FAILED) versions and delete-failed KBs.
	var stuckVersions []*pb.StuckVersion
	var deleteFailed []string
	if kbs, err := s.raftNode.ListKnowledgeBases(ctx); err == nil {
		for _, kb := range kbs {
			if kb.Status == types.KBStatusDeleteFailed {
				deleteFailed = append(deleteFailed, kb.KBID)
			}
			versions, err := s.raftNode.ListVersions(ctx, kb.KBID)
			if err != nil {
				continue
			}
			for _, v := range versions {
				if v.IndexStatus == types.IndexStatusFailed {
					stuckVersions = append(stuckVersions, &pb.StuckVersion{
						KbId:        v.KBID,
						VersionId:   v.VersionID,
						IndexStatus: pb.IndexStatus(v.IndexStatus),
						// VersionMeta has no separate status-updated timestamp;
						// use the creation time as the best available proxy.
						UpdatedAt: v.CreatedAt,
					})
				}
			}
		}
	}

	// WAL replay counters.
	var walAlerts []*pb.WALAlert
	for _, rc := range s.wal.GetReplayCounters() {
		desc := "WAL record stuck"
		if rc.Record.Type == types.PendingRecordTypeDeleteMark {
			desc = "delete mark for " + rc.Record.KBID
		}
		walAlerts = append(walAlerts, &pb.WALAlert{
			Description: desc,
			RetryCount:  int32(rc.RetryCount),
		})
	}

	// Resource usage snapshot.
	resourceUsage := &pb.ResourceUsage{
		LoadedIndexCount: int32(s.indexManager.LoadedCount()),
	}
	if n, err := s.docStore.DiskUsage(ctx); err == nil {
		resourceUsage.DocStoreBytes = int64(n)
	}
	if n, err := s.chunkStore.DiskUsage(ctx); err == nil {
		resourceUsage.ChunkStoreBytes = int64(n)
	}

	return &pb.GetSystemStatusResponse{
		Health:          health,
		StuckVersions:   stuckVersions,
		DeleteFailedKbs: deleteFailed,
		WalAlerts:       walAlerts,
		ResourceUsage:   resourceUsage,
	}, nil
}

// GetClusterStatus implements AdminServiceServer.
func (s *AdminServiceImpl) GetClusterStatus(ctx context.Context, req *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
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
	// Warmup rebuilds and re-homes the version's index in memory. Mark the
	// version PENDING first so the console shows "warming up" and refuses
	// rollback/parenting while the async build runs, then trigger the build
	// exactly like RebuildIndex. Completion is reported via the registered
	// BuildCompleteCallback, which flips the status back to READY/FAILED.
	if err := s.raftNode.ProposeUpdateVersionStatus(ctx, req.VersionId, types.IndexStatusPending); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	if err := s.indexManager.TriggerBuild(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.WarmupVersionResponse{Success: true}, nil
}

var _ pb.AdminServiceServer = (*AdminServiceImpl)(nil)
