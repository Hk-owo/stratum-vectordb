package service

import (
	"context"
	"time"

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

	// replicas lists the addresses of the nodes that may hold a version's
	// data, and presence asks them whether they do. Both are optional: when
	// either is nil the data-missing check is skipped (a single-node
	// deployment has no other replica to ask).
	replicas func() []string
	presence PresenceChecker

	// gcPressure reports versions whose §8.6(d) dead-weight collection is blocked
	// because too few replicas would remain serving. Optional: a control node
	// keeps no index manager and has nothing to report.
	gcPressure GCPressureReporter
}

// GCPressureReporter reports the versions whose §8.6(d) dead-weight collection is
// blocked behind the service-capacity check.
//
// Declared here, by the consumer, for the same reason PresenceChecker is: the
// service layer should depend on the one method it uses rather than on the whole
// index manager. *index.IndexManagerImpl satisfies it.
type GCPressureReporter interface {
	BlockedCollections() []index.GCPressure
}

// SetGCPressureReporter wires the §8.6(d) blocked-collection report.
func (s *AdminServiceImpl) SetGCPressureReporter(r GCPressureReporter) {
	s.gcPressure = r
}

// dataMissingMinAgeSec is how long a PENDING version may legitimately still be
// mid-write before the control layer probes its replicas. Without it every
// status call would probe versions that were allocated a moment ago.
//
// Placeholder: the value belongs with the other consistency-budget numbers
// (Stratum_设计文档v13.md §10.4) once they are calibrated by measurement.
// It is a var (not a const) only so tests can shorten it.
var dataMissingMinAgeSec int64 = 120

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
	replicas func() []string,
	presence PresenceChecker,
) *AdminServiceImpl {
	return &AdminServiceImpl{
		nodeID:       nodeID,
		raftNode:     rn,
		indexManager: im,
		docStore:     ds,
		chunkStore:   cs,
		wal:          w,
		replicas:     replicas,
		presence:     presence,
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

	// Scan all KBs for stuck (FAILED) versions, Deleting versions, and
	// delete-failed KBs.
	var stuckVersions []*pb.StuckVersion
	var deletingVersions []*pb.StuckVersion
	var pendingVersions []types.VersionMeta
	var failedPermanent []*pb.FailedVersion
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
				if v.Deleting {
					deletingVersions = append(deletingVersions, &pb.StuckVersion{
						KbId:      v.KBID,
						VersionId: v.VersionID,
						// UpdatedAt carries the version's creation time as
						// the best available proxy for "when did this
						// deletion start".
						UpdatedAt: v.CreatedAt,
					})
					continue
				}
				if v.IndexStatus == types.IndexStatusFailed {
					stuckVersions = append(stuckVersions, &pb.StuckVersion{
						KbId:        v.KBID,
						VersionId:   v.VersionID,
						IndexStatus: pb.IndexStatus(v.IndexStatus),
						// VersionMeta has no separate status-updated timestamp;
						// use the creation time as the best available proxy.
						UpdatedAt: v.CreatedAt,
					})
					continue
				}
				if v.IndexStatus == types.IndexStatusPending {
					pendingVersions = append(pendingVersions, v)
				}
				// FAILED_PERMANENT is the control layer's terminal verdict
				// (Stratum_设计文档v13.md §10.1): it is reported separately from
				// the retryable FAILED above, and carries the cause chain an
				// operator needs.
				//
				// Either SIDE's verdict counts: a version whose data will never
				// arrive is just as dead as one whose index never built, and
				// leaving the data side out would hide exactly the failures the
				// storage layer reports most often (§10.1b).
				if v.IndexStatus == types.IndexStatusFailedPermanent ||
					v.DataStatus == types.DataStatusFailedPermanent {
					failedPermanent = append(failedPermanent, &pb.FailedVersion{
						KbId:         v.KBID,
						VersionId:    v.VersionID,
						Reason:       v.FailureReason,
						FailureCount: v.FailureCount,
						// The side the verdict settled, recorded at apply time
						// rather than inferred from the two statuses: a version
						// can end up terminal on BOTH sides, and then only the
						// stored side says which one the cause chain describes.
						Side: pb.FailureSide(v.FailureSide),
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

	// A PENDING version that no candidate replica holds cannot become READY on
	// its own: its data never landed, so it is only recoverable by the writer
	// re-sending its changes under the same client_request_id
	// (Stratum_设计文档v13.md §7.12). Surface it instead of leaving it to look
	// like an index build that is merely slow.
	dataMissing := s.collectDataMissingVersions(ctx, pendingVersions)

	return &pb.GetSystemStatusResponse{
		Health:                  health,
		StuckVersions:           stuckVersions,
		DeleteFailedKbs:         deleteFailed,
		DeletingVersions:        deletingVersions,
		WalAlerts:               walAlerts,
		ResourceUsage:           resourceUsage,
		DataMissingVersions:     dataMissing,
		FailedPermanentVersions: failedPermanent,
		// §8.6(d): an active version whose dead weight cannot be collected because
		// too few replicas would remain serving. Nothing else will clear it — the
		// remedy is a larger replica count — so it belongs next to the other
		// "needs a human" signals rather than in a log nobody tails.
		GcBlockedVersions: s.gcBlockedVersions(),
	}, nil
}

// gcBlockedVersions maps the index manager's §8.6(d) blocked collections onto the
// status payload.
//
// Absent reporter or nothing blocked are the same answer — an empty list — and
// both are normal: a control node keeps no index manager, and a deployment with
// slack never blocks.
func (s *AdminServiceImpl) gcBlockedVersions() []*pb.GCBlockedVersion {
	if s.gcPressure == nil {
		return nil
	}
	blocked := s.gcPressure.BlockedCollections()
	if len(blocked) == 0 {
		return nil
	}
	out := make([]*pb.GCBlockedVersion, 0, len(blocked))
	for _, b := range blocked {
		out = append(out, &pb.GCBlockedVersion{
			KbId:            b.KBID,
			VersionId:       b.VersionID,
			DeadShare:       b.DeadShare,
			OthersServing:   int32(b.OthersServing),
			MinimumRequired: int32(b.MinimumRequired),
			BlockedSince:    b.Since.Unix(),
		})
	}
	return out
}

// collectDataMissingVersions probes the candidate replicas for every PENDING
// version and reports the ones nobody holds. It is a no-op when the node has
// no replica set or no presence checker.
func (s *AdminServiceImpl) collectDataMissingVersions(ctx context.Context, versions []types.VersionMeta) []*pb.StuckVersion {
	if s.presence == nil || s.replicas == nil || len(versions) == 0 {
		return nil
	}
	missing := dataMissingVersions(ctx, versions, s.replicas(), s.presence, time.Now().Unix(), dataMissingMinAgeSec)
	if len(missing) == 0 {
		return nil
	}
	out := make([]*pb.StuckVersion, 0, len(missing))
	for _, v := range missing {
		out = append(out, &pb.StuckVersion{
			KbId:        v.KBID,
			VersionId:   v.VersionID,
			IndexStatus: pb.IndexStatus(v.IndexStatus),
			UpdatedAt:   v.CreatedAt,
		})
	}
	return out
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
	// nodeID 0: this is the control layer's verdict about the version, not a
	// replica reporting that it serves it.
	if err := s.raftNode.ProposeUpdateVersionStatus(ctx, req.VersionId, 0, 0); err != nil { // 0 = IndexStatusPending
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	if err := s.indexManager.TriggerBuild(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Register the request with the retention policy: an explicit rebuild is
	// "this version is wanted here, now", and without it the artifact it builds
	// is dropped by the next retention pass (the version is normally outside the
	// newest-N window — that is why someone had to rebuild it).
	s.indexManager.RecordInterest(req.KnowledgeBaseId, req.VersionId)

	return &pb.RebuildIndexResponse{Success: true}, nil
}

// WarmupVersion implements AdminServiceServer.
func (s *AdminServiceImpl) WarmupVersion(ctx context.Context, req *pb.WarmupVersionRequest) (*pb.WarmupVersionResponse, error) {
	// Warmup rebuilds and re-homes the version's index in memory. Mark the
	// version PENDING first so the console shows "warming up" and refuses
	// rollback/parenting while the async build runs, then trigger the build
	// exactly like RebuildIndex. Completion is reported via the registered
	// BuildCompleteCallback, which flips the status back to READY/FAILED.
	if err := s.raftNode.ProposeUpdateVersionStatus(ctx, req.VersionId, types.IndexStatusPending, 0); err != nil { // nodeID 0: see RebuildIndex
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	if err := s.indexManager.TriggerBuild(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Same as RebuildIndex: warming a version up is asking the node to keep it
	// ready, so the retention policy must not drop the artifact afterwards.
	s.indexManager.RecordInterest(req.KnowledgeBaseId, req.VersionId)

	return &pb.WarmupVersionResponse{Success: true}, nil
}

var _ pb.AdminServiceServer = (*AdminServiceImpl)(nil)
