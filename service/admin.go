package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
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

	// storageGate answers whether the storage layer can still meet a write's
	// durability contract (docs/storage-degradation-signal-plan.md §4.5).
	// Optional, and nil means "unknown" — which reports nothing rather than
	// vouching for a storage layer it cannot see.
	storageGate StorageDegradationSource

	// reclaimDiag answers "which required replicas keep a knowledge base's §7.5 reclaim
	// watermark unavailable". Optional: nil means this node cannot tell, which is
	// reported as "judgement unavailable" rather than as "nothing is blocked".
	reclaimDiag ReclaimDiagnosticsSource

	// deleteVersionCoord runs the asynchronous cleanup ForceAbandonVersion starts.
	// Optional: without it that RPC refuses rather than marking a version Deleting
	// and leaving the reclamation to whoever happens to call DeleteVersion next.
	deleteVersionCoord coordinator.DeleteVersionCoordinator

	// logger records what an operator-facing action did. An abandoned version's
	// cleanup failing must not be silent — the version stays DELETING, and this
	// line is what connects that state to the decision that put it there. Never
	// nil; see NewAdminService.
	logger *zap.Logger
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

// ReclaimBlockedReplica names one required replica that keeps a knowledge base's §7.5
// reclaim watermark unavailable, and why. It mirrors plane.ReclaimBlocker rather than
// importing it, for the reason StorageDegradationSource mirrors the redundancy verdict
// instead of reusing plane's enum: the service layer carries the answer, and the plane's
// vocabulary does not cross that boundary.
type ReclaimBlockedReplica struct {
	NodeID int64
	// Reason is one of three stable wire names (see the ReclaimBlocker message in
	// admin.proto): "never_reported", "no_cursor_for_kb" or "stale".
	Reason string
	// Reached is its last reported cursor for this knowledge base, 0 when it has none.
	Reached int64
	// ReportedAt is when that report arrived; the zero time means it has never reported.
	ReportedAt time.Time
}

// ReclaimDiagnosticsSource answers "which required replicas keep this knowledge base's
// reclaim watermark unavailable". Optional: a node that does not lead cannot answer, and
// a node with no control plane has nothing to ask.
type ReclaimDiagnosticsSource interface {
	ReclaimBlockedReplicas(kbID string) ([]ReclaimBlockedReplica, bool)
}

// SetReclaimDiagnosticsSource wires the §7.5 watermark diagnosis that GetSystemStatus
// reports.
func (s *AdminServiceImpl) SetReclaimDiagnosticsSource(src ReclaimDiagnosticsSource) {
	s.reclaimDiag = src
}

// SetLogger wires the logger the operator-facing actions write to. Optional:
// without it those lines go nowhere, which is the pre-existing behaviour.
func (s *AdminServiceImpl) SetLogger(l *zap.Logger) {
	if l != nil {
		s.logger = l
	}
}

// SetDeleteVersionCoordinator wires the cleanup flow ForceAbandonVersion drives.
func (s *AdminServiceImpl) SetDeleteVersionCoordinator(c coordinator.DeleteVersionCoordinator) {
	s.deleteVersionCoord = c
}

// SetStorageDegradationSource wires the storage-redundancy verdict that
// HealthCheck reports in its details. Optional: without it health says nothing
// about storage rather than reporting it as fine.
func (s *AdminServiceImpl) SetStorageDegradationSource(src StorageDegradationSource) {
	s.storageGate = src
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
		logger:       zap.NewNop(),
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

	// Storage redundancy (docs/storage-degradation-signal-plan.md §4.5).
	//
	// It belongs in Details and NOT in the status, deliberately: a probe that
	// turned UNHEALTHY here would have a load balancer pull traffic off a node
	// whose READS are perfectly fine. Below quorum, a replica that still holds the
	// data still serves it, and keeping that is the availability trade-off §2
	// lists as a non-goal to change. The write side fails with a named, retryable
	// error instead; this line is for whoever has to diagnose it.
	//
	// The knowledge base is passed empty because the verdict is currently
	// KB-independent — the replica topology is cluster-wide, so every KB gets the
	// same answer (§3.1). Per-KB placement (§10.2) is what would make naming
	// individual knowledge bases meaningful.
	if s.storageGate != nil {
		// Either tier is worth reporting here: this line exists so an operator can
		// see the storage layer's state without attempting a write, and "nobody is
		// answering" matters at least as much as "one short". Which tier it was is
		// in the detail; the refusal's NAME is what distinguishes them, and that
		// belongs on the write (§4.1).
		if degraded, detail, ok := s.storageGate.StorageDegraded(""); ok && degraded {
			if details != "" {
				details += "; "
			}
			details += "storage: " + detail
		} else if unavailable, detail, ok := s.storageGate.StorageUnavailable(""); ok && unavailable {
			if details != "" {
				details += "; "
			}
			details += "storage: " + detail
		}
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
	// §7.5: why the reclaim watermark is not moving, per knowledge base. Collected in the
	// same walk as the rest, because the question is asked of the same knowledge-base list.
	var reclaimBlocked []*pb.ReclaimBlockedKB
	reclaimJudgeable := false
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
				// 只认可重试的 FAILED（stuck = 卡在那儿等重试）。这里不能用
				// IndexStatus.IsFailed()：本分支以 continue 结尾，把终态的
				// FAILED_PERMANENT 一并收进来，就会让下面专门收集终态的
				// failedPermanent 永远收不到东西。
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
			}
			// The terminal verdicts of this knowledge base, collected by the same
			// function ListFailedVersions answers from — so the two cannot drift
			// apart about what "waiting for a human" means.
			failedPermanent = append(failedPermanent, failedVersionsOf(versions)...)

			// §7.5: which required replicas keep THIS knowledge base's reclaim watermark
			// unavailable. Asked per knowledge base, because the inputs are per knowledge
			// base: a replica can report others and say nothing about this one.
			if s.reclaimDiag != nil {
				if blockers, ok := s.reclaimDiag.ReclaimBlockedReplicas(kb.KBID); ok {
					reclaimJudgeable = true
					if entry := reclaimBlockedKB(kb.KBID, blockers); entry != nil {
						reclaimBlocked = append(reclaimBlocked, entry)
					}
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
		// §7.5: the reclaim watermark is deliberately conservative, so a required replica
		// that is merely away holds it still — which stops WAL reclamation for that
		// knowledge base, and stops it silently. The remedy
		// (dropping the node from storage.nodes) is an operator's decision, so the
		// diagnosis belongs beside the other "needs a human" signals rather than only in a
		// log line.
		Reclaim: &pb.ReclaimDiagnostics{
			JudgementAvailable: reclaimJudgeable,
			Blocked:            reclaimBlocked,
		},
	}, nil
}

// reclaimBlockedKB maps one knowledge base's watermark blockers onto the status payload.
// Nil when nothing blocks it, so a healthy fleet does not carry an entry per knowledge
// base — the list only ever names what is actually stuck.
func reclaimBlockedKB(kbID string, blockers []ReclaimBlockedReplica) *pb.ReclaimBlockedKB {
	if len(blockers) == 0 {
		return nil
	}
	out := &pb.ReclaimBlockedKB{KbId: kbID, Blockers: make([]*pb.ReclaimBlocker, 0, len(blockers))}
	for _, b := range blockers {
		// Age, not a timestamp: the reader wants "how stale", and the clock that matters is
		// the one that stamped the report. -1 means "never reported", the convention the
		// field's own comment documents.
		ageMS := int64(-1)
		if !b.ReportedAt.IsZero() {
			ageMS = time.Since(b.ReportedAt).Milliseconds()
		}
		out.Blockers = append(out.Blockers, &pb.ReclaimBlocker{
			NodeId:          b.NodeID,
			Reason:          b.Reason,
			ReachedVersion:  b.Reached,
			LastReportAgeMs: ageMS,
		})
	}
	return out
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

	// Register the request with the retention policy BEFORE triggering the build.
	// An explicit rebuild is "this version is wanted here, now", and the version
	// is normally outside the newest-N window — that is why someone had to
	// rebuild it. The order matters: builds are asynchronous, and a small one can
	// finish — running the post-build retention pass — before the registration
	// would have executed, leaving the artifact unprotected at the exact moment
	// it is freshest.
	s.indexManager.RecordInterest(req.KnowledgeBaseId, req.VersionId)

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
	if err := s.raftNode.ProposeUpdateVersionStatus(ctx, req.VersionId, types.IndexStatusPending, 0); err != nil { // nodeID 0: see RebuildIndex
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Same as RebuildIndex — warming a version up is asking the node to keep it
	// ready, so the retention policy must not drop the artifact afterwards — and
	// for the same reason the registration goes first: the build it starts is
	// asynchronous, so the shield has to be on disk before that build can finish.
	s.indexManager.RecordInterest(req.KnowledgeBaseId, req.VersionId)

	if err := s.indexManager.TriggerBuild(ctx, req.KnowledgeBaseId, req.VersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.WarmupVersionResponse{Success: true}, nil
}

// failedVersionsOf collects the versions the control layer declared
// FAILED_PERMANENT on either side (Stratum_设计文档v13.md §10.1, §10.1b).
//
// Either side's verdict counts: a version whose data will never arrive is just as
// dead as one whose index never built, and leaving the data side out would hide
// exactly the failures the storage layer reports most often (§10.1b). A version
// already marked Deleting is excluded — it is on its way out and is reported as such
// by deleting_versions, so listing it as "waiting for an operator" would ask someone
// to act on something already being removed.
//
// One function for both callers (GetSystemStatus and ListFailedVersions), so the two
// cannot drift apart about what "waiting for a human" means.
func failedVersionsOf(versions []types.VersionMeta) []*pb.FailedVersion {
	var out []*pb.FailedVersion
	for _, v := range versions {
		if v.Deleting {
			continue
		}
		if v.IndexStatus != types.IndexStatusFailedPermanent &&
			v.DataStatus != types.DataStatusFailedPermanent {
			continue
		}
		out = append(out, &pb.FailedVersion{
			KbId:         v.KBID,
			VersionId:    v.VersionID,
			Reason:       v.FailureReason,
			FailureCount: v.FailureCount,
			// The side the verdict settled, recorded at apply time rather than
			// inferred from the two statuses: a version can end up terminal on BOTH
			// sides, and then only the stored side says which one the cause chain
			// describes.
			Side: pb.FailureSide(v.FailureSide),
		})
	}
	return out
}

// ListFailedVersions implements AdminServiceServer: the queue of versions waiting
// for a human (Stratum_设计文档v13.md §10.1). Nothing retries them automatically, so
// this list IS the work — an operator should not have to read it out of
// GetSystemStatus's much larger payload.
//
// A read of the replicated metadata, which is why any node holding it can answer.
// The knowledge base is optional because the two questions are different ones:
// "this knowledge base has a version stuck" (kb_id set) and "is anything else
// stuck?" (kb_id empty).
func (s *AdminServiceImpl) ListFailedVersions(ctx context.Context, req *pb.ListFailedVersionsRequest) (*pb.ListFailedVersionsResponse, error) {
	if kbID := req.GetKnowledgeBaseId(); kbID != "" {
		versions, err := s.raftNode.ListVersions(ctx, kbID)
		if err != nil {
			return nil, stratumerrors.ToGRPCStatus(err)
		}
		return &pb.ListFailedVersionsResponse{Versions: failedVersionsOf(versions)}, nil
	}

	kbs, err := s.raftNode.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	var out []*pb.FailedVersion
	for _, kb := range kbs {
		versions, err := s.raftNode.ListVersions(ctx, kb.KBID)
		if err != nil {
			// One unreadable knowledge base must not hide the others: this call is
			// made when something is already wrong, and a partial answer beats an
			// error that names nothing.
			continue
		}
		out = append(out, failedVersionsOf(versions)...)
	}
	return &pb.ListFailedVersionsResponse{Versions: out}, nil
}

// ForceRetryVersion implements AdminServiceServer: an operator revoking the index
// side's terminal verdict and asking for another build
// (Stratum_设计文档v13.md §10.1).
//
// The DATA side is not retried here, and that refusal is explicit rather than quiet:
// its verdict says the version's data will never arrive, so a rebuild would have
// nothing to build from. The operator's answer there is ForceAbandonVersion, and a
// caller who asks anyway is told exactly that instead of getting a success that
// means nothing.
//
// A version that is not index-terminal is refused by the state machine
// (ErrVersionNotFailedPermanent), which is where that rule belongs: every replica
// reaches the same verdict there, while a proposer that decided it could disagree
// with the state it is writing into.
func (s *AdminServiceImpl) ForceRetryVersion(ctx context.Context, req *pb.ForceRetryVersionRequest) (*pb.ForceRetryVersionResponse, error) {
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if kbID == "" || versionID == 0 {
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf("%w: knowledge_base_id and version_id are required", stratumerrors.ErrInvalidArgument))
	}
	if s.indexManager == nil {
		return nil, status.Error(codes.Unimplemented, "this node holds no index, so it cannot rebuild a version")
	}

	// §F: one version, one read — see RaftNode.GetVersion.
	v, err := s.raftNode.GetVersion(ctx, kbID, versionID)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	if v.DataStatus == types.DataStatusFailedPermanent {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version %d's data side is FAILED_PERMANENT: its data will never arrive, so its index cannot be rebuilt — abandon the version instead (ForceAbandonVersion)", versionID)
	}

	if err := s.raftNode.ProposeRetryVersion(ctx, kbID, versionID, types.FailureSideIndex); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Register the request with the retention policy BEFORE triggering the build,
	// exactly as RebuildIndex does and for the same reason: a version someone had to
	// retry is normally outside the newest-N window — that is why a human was needed —
	// and the build is asynchronous, so a small one could finish (running the
	// post-build retention pass) before a later registration would have protected its
	// artifact.
	s.indexManager.RecordInterest(kbID, versionID)
	if err := s.indexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	return &pb.ForceRetryVersionResponse{Success: true, Side: pb.FailureSide(types.FailureSideIndex)}, nil
}

// ForceAbandonVersion implements AdminServiceServer: the other answer to a terminal
// verdict — the version leaves the chain (Stratum_设计文档v13.md §10.1).
//
// It is DeleteVersion under SINGLE semantics plus one admission rule: the version
// must actually carry a FAILED_PERMANENT verdict. That rule is why this is its own
// RPC rather than documentation telling people to call DeleteVersion — "abandon" is
// an operation on a failure, and a name that also removes healthy versions invites
// the wrong call.
//
// SINGLE rather than SUBTREE because the operator named ONE version: a child, if
// there is one, is spliced onto its parent rather than removed with it.
//
// It is also the only cleanup path that reaches a STORAGE NODE at runtime for a terminal
// verdict. The verdict itself travels the Raft log, so every member learns it from its
// own apply — but a storage node does not participate in Raft, and its own sweep of
// terminal versions (ReclaimTerminalVersions) runs at start-up only. What does reach it
// is this flow's delete broadcast, because a version DELETE is broadcast to every
// candidate replica. So abandoning a dead version is what clears the bytes the verdict
// alone would have left on that node until its next restart — which is the practical
// reason "declare dead, then abandon" is the operator's path, and why nothing periodic
// was added on the verdict's behalf.
func (s *AdminServiceImpl) ForceAbandonVersion(ctx context.Context, req *pb.ForceAbandonVersionRequest) (*pb.ForceAbandonVersionResponse, error) {
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if kbID == "" || versionID == 0 {
		return nil, stratumerrors.ToGRPCStatus(fmt.Errorf("%w: knowledge_base_id and version_id are required", stratumerrors.ErrInvalidArgument))
	}
	if s.deleteVersionCoord == nil {
		return nil, status.Error(codes.Unimplemented, "this node cannot run the delete-version cleanup, so it cannot abandon a version")
	}

	// §F: one version, one read — see RaftNode.GetVersion.
	v, err := s.raftNode.GetVersion(ctx, kbID, versionID)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	if v.IndexStatus != types.IndexStatusFailedPermanent &&
		v.DataStatus != types.DataStatusFailedPermanent {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version %d carries no FAILED_PERMANENT verdict (index=%s, data=%s): a healthy version is DeleteVersion's business",
			versionID, v.IndexStatus, v.DataStatus)
	}

	deleted, err := markVersionDeletingThenCleanUp(ctx, s.raftNode, s.deleteVersionCoord, s.logger,
		kbID, versionID, types.VersionDeleteSingle)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.ForceAbandonVersionResponse{Success: true, DeletedVersionIds: deleted}, nil
}

var _ pb.AdminServiceServer = (*AdminServiceImpl)(nil)
