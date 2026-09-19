// Package service implements Stratum's three external gRPC services
// (KnowledgeBaseService, QueryService, AdminService), per
// Stratum_接口设计v9.md and Stratum_实现顺序.md Phase 6.
//
// Each service is a thin layer: validate inputs, convert between proto
// messages and internal types, delegate to the coordinator/raft/index
// layer, convert errors to gRPC status codes via errors.ToGRPCStatus, and
// return responses. No business logic lives here.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/coordinator"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/raft"
	"stratum/internal/types"
)

// KnowledgeBaseServiceImpl implements pb.KnowledgeBaseServiceServer.
type KnowledgeBaseServiceImpl struct {
	pb.UnimplementedKnowledgeBaseServiceServer

	raftNode           raft.RaftNode
	writeCoord         coordinator.WriteCoordinator
	deleteCoord        coordinator.DeleteCoordinator
	deleteVersionCoord coordinator.DeleteVersionCoordinator

	// versionHolders is the control leader's data-version aggregate, exposed to
	// the service station's route table (storage-coordination-and-service-station
	// -design.md §3.1). Optional: without it the RPC answers known=false and the
	// station keeps its previous behaviour.
	versionHolders VersionHolderSource

	// storageGate answers the other half of the same question, from the same
	// aggregate: can the storage layer still meet a WRITE's durability contract?
	// Optional, and an unwired gate means "unknown" — which callers read as
	// "allow" (docs/storage-degradation-signal-plan.md §3.3).
	storageGate StorageDegradationSource

	// logger carries the per-stage timings of a write, the control-plane half of
	// the chain stage 5 continues on the storage node (see CreateVersion). Debug
	// level and silent by default: one line per write at info would be noise, but
	// the alternative — today — is that a write's cost is broken down only AFTER
	// it has been handed off, with nothing measuring the part the client actually
	// waited for. See SetLogger.
	logger *zap.Logger

	// presence + replicas feed the await path's data_missing probe (see
	// await_version.go). Optional: without them the probe is skipped and
	// data_missing stays false — "unknown", never a claim that the data is
	// there.
	presence PresenceChecker
	replicas func() []string

	// probeCache remembers probe verdicts and bounds how many probes run at
	// once (docs/await-version-plan.md §6.4).
	probeCache *awaitProbe

	// versionWatcher is the event-driven half of the await path
	// (docs/await-version-plan.md §12 item 1). Optional: without it await polls
	// the state machine, which is correct but pays up to one interval of latency.
	versionWatcher VersionWatcher
}

// VersionHolder is one node that reported holding a version, with the address it
// reported for itself. It mirrors plane's holder rather than importing it: this
// is the control-layer service, and its caller is the station.
type VersionHolder struct {
	NodeID  int64
	Address string
}

// VersionHolderSource answers "which nodes reported holding kbID at or past
// versionID". ok=false means the answering node is not the control leader, so an
// empty list is "I have heard from nobody", never "nobody has it" — the two
// justify opposite actions, since the second would license deleting data (§10.6).
//
// *plane.LocalControlPlane implements it; declared narrow here so this package
// does not depend on plane.
type VersionHolderSource interface {
	DataVersionHolders(kbID string, versionID int64) ([]VersionHolder, bool)
}

// SetVersionHolderSource wires the aggregate the holders RPC answers from.
func (s *KnowledgeBaseServiceImpl) SetVersionHolderSource(src VersionHolderSource) {
	s.versionHolders = src
}

// StorageDegradationSource answers whether the storage layer can still meet a
// WRITE's durability contract for a knowledge base: are enough of the replicas
// that must hold a version still alive?
//
// It reports the two failure tiers separately, because they produce different
// refusals (docs/storage-degradation-signal-plan.md §4.1):
//
//   - StorageDegraded: short of a quorum, with at least one replica still
//     answering — "this knowledge base cannot be written right now";
//   - StorageUnavailable: NOT ONE required replica is live — "the storage layer
//     is gone".
//
// They are separate calls rather than one tri-state so this package does not have
// to import plane's state enum (see the note below); a caller asking "can a write
// land" consults both, and the order it consults them in decides which name the
// refusal carries.
//
// ok=false means the judgement cannot be made — this node is not the control
// leader, the replica topology is not wired, or the aggregate has folded no
// report yet — and callers MUST read that as "allow" rather than as
// "unavailable". The verdict is soft state, so letting a leadership change refuse
// writes would turn a failover into an outage (§3.3).
//
// detail is the diagnosis a refusal carries: how many replicas are live, the
// quorum they must reach, and how long the silent ones have been quiet. A refusal
// nobody can act on is worse than the retry it saves (§4.3).
//
// *plane.LocalControlPlane implements it; declared narrow here so this package
// does not depend on plane.
type StorageDegradationSource interface {
	StorageDegraded(kbID string) (degraded bool, detail string, ok bool)
	StorageUnavailable(kbID string) (unavailable bool, detail string, ok bool)
}

// SetStorageDegradationSource wires the redundancy verdict that the CreateVersion
// gate and the holders RPC both read.
func (s *KnowledgeBaseServiceImpl) SetStorageDegradationSource(src StorageDegradationSource) {
	s.storageGate = src
}

// GetDataVersionHolders answers the station's two route-table questions about
// kbID: which nodes reported a contiguous cursor reaching versionID, and whether
// the storage layer can still meet a write's durability contract.
//
// Deliberately not an error when this node is not the leader: it answers
// known=false, which the station reads as "ask someone else" rather than as a
// fact about where data lives. A station treating a follower's empty list as
// "nobody has it" would route every query away from nodes that do hold it.
//
// The redundancy half rides along instead of getting an RPC of its own
// (docs/storage-degradation-signal-plan.md §4.2): the station already polls this
// call on every route refresh, and both answers come from the same leader-side
// aggregate. It is filled even when the holders half cannot answer — the two
// share an aggregate but not a precondition, and the station's write gate must
// not go blind merely because the leader has no holder to name.
func (s *KnowledgeBaseServiceImpl) GetDataVersionHolders(ctx context.Context, req *pb.GetDataVersionHoldersRequest) (*pb.GetDataVersionHoldersResponse, error) {
	kbID := req.GetKnowledgeBaseId()
	resp := &pb.GetDataVersionHoldersResponse{}

	if s.storageGate != nil {
		degraded, detail, ok := s.storageGate.StorageDegraded(kbID)
		// Degraded is false whenever ok is false, so a reader that ignores
		// degradation_known still lands on "allow" — see the field's comment in
		// the proto.
		resp.Degraded = degraded
		resp.DegradationKnown = ok
		if ok {
			resp.DegradationDetail = detail
		}
		// The cluster tier travels on the same response, so the station names the
		// refusal the same way the control layer does (§4.1) instead of calling
		// "the storage layer is gone" a KB-level problem. Its detail is the more
		// specific one, so it replaces the diagnosis above when it fires.
		if unavailable, unavailableDetail, ok := s.storageGate.StorageUnavailable(kbID); ok && unavailable {
			resp.StorageUnavailable = true
			resp.DegradationDetail = unavailableDetail
		}
	}

	if s.versionHolders == nil {
		return resp, nil
	}
	holders, ok := s.versionHolders.DataVersionHolders(kbID, req.GetVersionId())
	if !ok {
		return resp, nil
	}
	resp.Known = true
	resp.Holders = make([]*pb.DataVersionHolder, 0, len(holders))
	for _, h := range holders {
		resp.Holders = append(resp.Holders, &pb.DataVersionHolder{NodeId: h.NodeID, Address: h.Address})
	}
	return resp, nil
}

// checkStorageWritable refuses a write whose durability contract the storage
// layer cannot currently meet (docs/storage-degradation-signal-plan.md §4.3).
//
// It runs BEFORE the write coordinator, and that placement is the whole point:
// letting the write proceed spends a version number and a Raft entry on an
// attempt that must fail at fan-out, and then bills it to the version's retry
// budget until it is declared FAILED_PERMANENT (§1.1). Failing here costs one
// RPC.
//
// An unknown verdict (ok=false) ALLOWS. The storage layer's redundancy is soft
// state that a leadership change empties, so refusing on "cannot tell" would turn
// a failover into an outage; §3.3 takes the write instead.
func (s *KnowledgeBaseServiceImpl) checkStorageWritable(kbID string) error {
	if s.storageGate == nil {
		return nil
	}
	// The extreme first. "The storage layer is gone" is a KB-level verdict only in
	// the sense that it applies to every KB; naming it kb_storage_degraded would be
	// true but unhelpful, and the refusal is identical either way (retryable,
	// codes.Unavailable) — only the name and the detail differ (§4.1).
	if unavailable, detail, ok := s.storageGate.StorageUnavailable(kbID); ok && unavailable {
		return fmt.Errorf("%w: %s", stratumerrors.ErrStorageUnavailable, detail)
	}
	degraded, detail, ok := s.storageGate.StorageDegraded(kbID)
	if !ok || !degraded {
		return nil
	}
	return fmt.Errorf("%w: %s", stratumerrors.ErrKBStorageDegraded, detail)
}

// NewKnowledgeBaseService constructs a KnowledgeBaseServiceImpl.
func NewKnowledgeBaseService(
	rn raft.RaftNode,
	wc coordinator.WriteCoordinator,
	dc coordinator.DeleteCoordinator,
	dvc coordinator.DeleteVersionCoordinator,
) *KnowledgeBaseServiceImpl {
	return &KnowledgeBaseServiceImpl{
		raftNode:           rn,
		writeCoord:         wc,
		deleteCoord:        dc,
		deleteVersionCoord: dvc,
		logger:             zap.NewNop(),
		probeCache:         newAwaitProbe(),
	}
}

// SetLogger wires the logger that carries CreateVersion's per-stage timings.
// Optional: without it those timings go nowhere, which is the pre-existing
// behaviour (nothing about correctness depends on them).
func (s *KnowledgeBaseServiceImpl) SetLogger(l *zap.Logger) {
	if l != nil {
		s.logger = l
	}
}

// CreateKnowledgeBase implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) CreateKnowledgeBase(ctx context.Context, req *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
	kbID := generateKBID(req.Name)

	indexType := req.IndexType.String()
	if req.IndexType == pb.IndexType_INDEX_TYPE_HNSW || indexType == "" || indexType == "INDEX_TYPE_HNSW" {
		indexType = "HNSW"
	}
	similarity := req.Similarity.String()
	if similarity == "" || similarity == "SIMILARITY_COSINE" {
		similarity = "COSINE"
	}
	quantizer := quantizerFromProto(req.Quantizer)
	pqM := int(req.PqM)
	if quantizer == "PQ" && pqM <= 0 {
		pqM = 96 // default
	}
	pqNBits := int(req.PqNbits)
	if quantizer == "PQ" && pqNBits <= 0 {
		pqNBits = 8 // default
	}

	windowSize := int(req.ChunkWindowSize)
	if windowSize <= 0 {
		windowSize = 512 // default
	}
	overlapSize := int(req.ChunkOverlapSize)
	if overlapSize < 0 {
		overlapSize = 64
	}

	kb := types.KnowledgeBaseMeta{
		KBID:             kbID,
		Name:             req.Name,
		ChunkWindowSize:  windowSize,
		ChunkOverlapSize: overlapSize,
		IndexType:        indexType,
		Similarity:       similarity,
		QuantizerType:    quantizer,
		QuantizerPQM:     pqM,
		QuantizerPQNBits: pqNBits,
		EmbedConfig: types.EmbedConfig{
			ServiceAddr: req.EmbedConfig.GetServiceAddr(),
			ModelID:     req.EmbedConfig.GetModelId(),
		},
		Status: types.KBStatusActive,
	}

	if err := s.raftNode.ProposeCreateKB(ctx, kb); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// NO version is created here (docs/cursor-persistence-plan.md §5): the knowledge
	// base becomes visible with an empty version chain, and its first version comes
	// from the client's first non-empty write. Creating a change-less version here
	// was what conflated "the document set is empty" with "this version changed
	// nothing" — a version's document set is inherited from its parent, so the two
	// only coincide at the root of a chain — and both layers carried the special
	// case for it.
	//
	// An empty knowledge base remains QUERYABLE: with no active version the query
	// path answers an empty result instead of refusing (docs/cursor-persistence-plan.md
	// §5.1), so a client that creates and queries one sees no behaviour change.
	return &pb.CreateKnowledgeBaseResponse{
		KnowledgeBaseId: kbID,
		// InitialVersionId is 0: no version exists yet. The field stays on the wire
		// for compatibility, and 0 is the honest answer — a version appears only once
		// something is written.
		InitialVersionId: 0,
	}, nil
}

// DeleteKnowledgeBase implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) DeleteKnowledgeBase(ctx context.Context, req *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
	kbID := req.KnowledgeBaseId

	// Mark the KB as deleting.
	if err := s.raftNode.ProposeMarkKBDeleting(ctx, kbID); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Launch async cleanup.
	go func() {
		_ = s.deleteCoord.Execute(context.Background(), kbID)
	}()

	return &pb.DeleteKnowledgeBaseResponse{Success: true}, nil
}

// CreateVersion implements KnowledgeBaseServiceServer.
//
// It is the authoritative half of the storage gate (docs/storage-degradation-signal-plan.md
// §4.3): a request that reaches the control layer directly never passed the
// service station, and one that did may have passed a station whose snapshot was
// stale. The check therefore has to live here too, and it has to run before the
// write coordinator allocates anything — a version number, a Raft entry and the
// version's retry budget are all spent by an attempt that cannot reach quorum,
// and the caller gets only a PENDING version that later disappears.
//
// The verdict is soft state, so the refusal is retryable (ErrKBStorageDegraded
// maps to codes.Unavailable): the next attempt is the one that gets the
// authoritative answer once the aggregate has caught up.
func (s *KnowledgeBaseServiceImpl) CreateVersion(ctx context.Context, req *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
	// Per-stage timings of one write, at debug level.
	//
	// Why it exists: this is the entry the client's clock is measuring, and the
	// chain it starts is split across three processes — the station in front
	// (router), this control node, and the storage node that ends up owning the
	// transaction (write: stage timings). kb_id + version_id are what join the
	// three lines back into one request; the version id is filled in by the
	// deferred function below, so a write that fails before the version exists
	// still reports its line with the id absent rather than not at all.
	//
	// What this line adds over the coordinator's own (coordinator: write execute
	// timings) is the boundary: gate_us is the storage-degradation check, which
	// happens before any version number is spent, and convert_us is the proto
	// conversion. execute_us is the coordinator's total, so the two lines agree
	// by construction — which is what makes a disagreement a real finding rather
	// than a rounding difference.
	start := time.Now()
	var tGate, tConvert, tExecute time.Duration
	var versionID int64
	defer func() {
		s.logger.Debug("service: create version timings",
			zap.String("kb_id", req.GetKnowledgeBaseId()),
			zap.String("client_request_id", req.GetClientRequestId()),
			zap.Int64("version_id", versionID),
			zap.Int("changes", len(req.GetChanges())),
			zap.Int64("gate_us", tGate.Microseconds()),
			zap.Int64("convert_us", tConvert.Microseconds()),
			zap.Int64("execute_us", tExecute.Microseconds()),
			zap.Int64("total_us", time.Since(start).Microseconds()))
	}()

	stepStart := time.Now()
	err := s.checkStorageWritable(req.KnowledgeBaseId)
	tGate = time.Since(stepStart)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	stepStart = time.Now()
	changes := make([]types.DocChange, len(req.Changes))
	for i, c := range req.Changes {
		op := types.ChangeOpAdd
		switch c.Op {
		case pb.ChangeOp_CHANGE_OP_DELETE:
			op = types.ChangeOpDelete
		case pb.ChangeOp_CHANGE_OP_UPDATE:
			op = types.ChangeOpUpdate
		}
		changes[i] = types.DocChange{
			Op:      op,
			DocID:   c.DocId,
			Content: c.Content,
		}
	}
	tConvert = time.Since(stepStart)

	stepStart = time.Now()
	// §7 Step 4: the key is settled HERE, before the proposal, because the caller
	// is told about it in the response. Leaving it to the coordinator's own
	// "generate one if it is empty" branch produced a key nobody outside this
	// process could see, which is what left a client that omitted one unable to
	// re-send under it (docs/await-version-plan.md §7 Step 4).
	requestID := req.GetClientRequestId()
	if requestID == "" {
		requestID = coordinator.NewDispatchID()
	}
	versionID, err = s.writeCoord.Execute(ctx, req.KnowledgeBaseId, req.ParentVersionId, changes, requestID)
	tExecute = time.Since(stepStart)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.CreateVersionResponse{VersionId: versionID, ClientRequestId: requestID}, nil
}

// ListVersions implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) ListVersions(ctx context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	versions, err := s.raftNode.ListVersions(ctx, req.KnowledgeBaseId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	out := make([]*pb.VersionInfo, len(versions))
	for i, v := range versions {
		out[i] = versionInfoToProto(v)
	}
	return &pb.ListVersionsResponse{Versions: out}, nil
}

// RollbackVersion implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) RollbackVersion(ctx context.Context, req *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
	// Validate target version exists and is READY.
	versions, err := s.raftNode.ListVersions(ctx, req.KnowledgeBaseId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	found := false
	for _, v := range versions {
		if v.VersionID == req.TargetVersionId {
			found = true
			if v.IndexStatus == types.IndexStatusPending {
				return nil, status.Error(codes.FailedPrecondition, "target version is PENDING")
			}
			if v.IndexStatus.IsFailed() {
				return nil, status.Error(codes.FailedPrecondition, "target version index is FAILED")
			}
			break
		}
	}
	if !found {
		return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionNotFound)
	}

	if err := s.raftNode.ProposeRollback(ctx, req.KnowledgeBaseId, req.TargetVersionId); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	return &pb.RollbackVersionResponse{Success: true}, nil
}

// DeleteVersion implements KnowledgeBaseServiceServer.
//
// Marks the version set selected by req.Mode relative to req.VersionId as
// Deleting, then launches the asynchronous cleanup (index discard,
// VersionDocList / DocStore removal, metadata removal). The three modes
// cover "remove this subtree" (default), "remove just this middle version,
// splicing its children onto its parent", and "remove every preceding
// version, making this one the new base".
//
// All constraint checks — the version exists and belongs to the KB, and no
// version in the selected set is the active version or still PENDING — are
// enforced deterministically in the Raft state machine's apply phase, so
// this method performs no additional validation. The response echoes the
// exact set of versions marked for deletion.
func (s *KnowledgeBaseServiceImpl) DeleteVersion(ctx context.Context, req *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	mode, err := versionDeleteModeFromProto(req.Mode)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	deleted, err := s.raftNode.ProposeMarkVersionDeleting(ctx, req.KnowledgeBaseId, req.VersionId, mode)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	// Launch async cleanup. The coordinator re-discovers every Deleting
	// version of the KB (including any left over from a previous crashed
	// cleanup) and is idempotent end-to-end.
	go func() {
		_ = s.deleteVersionCoord.Execute(context.Background(), req.KnowledgeBaseId)
	}()

	return &pb.DeleteVersionResponse{Success: true, DeletedVersionIds: deleted}, nil
}

// DiscardVersion implements KnowledgeBaseServiceServer.
//
// The caller is declaring that it is abandoning a version whose write never
// landed (docs/await-version-plan.md §7 Step 6). Whether that is admissible is
// decided by the state machine at apply time, not here: this RPC relays the
// declaration, which is what keeps the check from racing the very write it is
// meant to protect (§5 contract 7).
func (s *KnowledgeBaseServiceImpl) DiscardVersion(ctx context.Context, req *pb.DiscardVersionRequest) (*pb.DiscardVersionResponse, error) {
	err := s.raftNode.ProposeDiscardVersion(ctx, req.GetKnowledgeBaseId(), req.GetVersionId())
	if err == nil {
		s.logDiscard(req, true)
		return &pb.DiscardVersionResponse{Discarded: true}, nil
	}
	if errors.Is(err, stratumerrors.ErrVersionNotFound) {
		// Already gone: the caller's intent is satisfied, so report that rather
		// than an error. A retry after a lost response then costs nothing, which
		// is the whole point of admitting this RPC as idempotent.
		s.logDiscard(req, false)
		return &pb.DiscardVersionResponse{Discarded: false}, nil
	}
	return nil, stratumerrors.ToGRPCStatus(err)
}

// logDiscard is §12 item 6's audit trail.
//
// Raft's own log carries the command (cmdDiscardVersion) and is the authoritative
// record; this line is what makes the event visible to whoever reads a service
// log. Without it an abandoned version is indistinguishable from a version number
// that was never valid — the version simply stops appearing, and "the caller gave
// up on it" is not something anyone can reconstruct after the fact.
//
// It also explains a later surprise: the discarding caller may have had a write
// in flight whose chunks still land, so those chunks can arrive after their
// metadata is gone. They become orphans for the chunk GC, and this line is the
// only place that connects them to the decision that caused them.
func (s *KnowledgeBaseServiceImpl) logDiscard(req *pb.DiscardVersionRequest, removed bool) {
	s.logger.Info("version discarded by the caller",
		zap.String("kb_id", req.GetKnowledgeBaseId()),
		zap.Int64("version_id", req.GetVersionId()),
		zap.Bool("metadata_removed", removed))
}

// versionDeleteModeFromProto maps the wire enum onto the internal type.
// The zero value (VERSION_DELETE_MODE_SUBTREE, also the value an unset
// field carries) preserves the historical DeleteVersion semantics.
//
// An unrecognized value is rejected rather than silently downgraded to
// SUBTREE: the caller clearly meant something specific, and guessing wrong
// would turn a mistyped mode into a destructive "delete the whole subtree".
func versionDeleteModeFromProto(m pb.VersionDeleteMode) (types.VersionDeleteMode, error) {
	switch m {
	case pb.VersionDeleteMode_VERSION_DELETE_MODE_SUBTREE:
		return types.VersionDeleteSubtree, nil
	case pb.VersionDeleteMode_VERSION_DELETE_MODE_SINGLE:
		return types.VersionDeleteSingle, nil
	case pb.VersionDeleteMode_VERSION_DELETE_MODE_ANCESTORS:
		return types.VersionDeleteAncestors, nil
	default:
		return types.VersionDeleteSubtree, fmt.Errorf("unknown version delete mode %d: %w", m, stratumerrors.ErrInvalidArgument)
	}
}

// ListKnowledgeBases implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) ListKnowledgeBases(ctx context.Context, _ *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
	kbs, err := s.raftNode.ListKnowledgeBases(ctx)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}

	out := make([]*pb.KnowledgeBaseInfo, 0, len(kbs))
	for _, kb := range kbs {
		out = append(out, kbToProto(kb))
	}
	return &pb.ListKnowledgeBasesResponse{KnowledgeBases: out}, nil
}

// GetKnowledgeBase implements KnowledgeBaseServiceServer.
func (s *KnowledgeBaseServiceImpl) GetKnowledgeBase(ctx context.Context, req *pb.GetKnowledgeBaseRequest) (*pb.GetKnowledgeBaseResponse, error) {
	kb, err := s.raftNode.GetKB(ctx, req.KnowledgeBaseId)
	if err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	return &pb.GetKnowledgeBaseResponse{KnowledgeBase: kbToProto(kb)}, nil
}

// kbToProto converts internal knowledge base metadata to its console-facing
// proto representation.
func kbToProto(kb types.KnowledgeBaseMeta) *pb.KnowledgeBaseInfo {
	return &pb.KnowledgeBaseInfo{
		KnowledgeBaseId:  kb.KBID,
		Name:             kb.Name,
		ChunkWindowSize:  int32(kb.ChunkWindowSize),
		ChunkOverlapSize: int32(kb.ChunkOverlapSize),
		IndexType:        indexTypeToProto(kb.IndexType),
		Similarity:       similarityToProto(kb.Similarity),
		Quantizer:        quantizerToProto(kb.QuantizerType),
		PqM:              int32(kb.QuantizerPQM),
		PqNbits:          int32(kb.QuantizerPQNBits),
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: kb.EmbedConfig.ServiceAddr,
			ModelId:     kb.EmbedConfig.ModelID,
		},
		ActiveVersionId: kb.ActiveVersionID,
		Status:          kbStatusToProto(kb.Status),
	}
}

func indexTypeToProto(s string) pb.IndexType {
	switch s {
	case "IVF":
		return pb.IndexType_INDEX_TYPE_IVF
	case "FLAT":
		return pb.IndexType_INDEX_TYPE_FLAT
	default:
		return pb.IndexType_INDEX_TYPE_HNSW
	}
}

func similarityToProto(s string) pb.Similarity {
	switch s {
	case "EUCLIDEAN":
		return pb.Similarity_SIMILARITY_EUCLIDEAN
	case "INNER_PRODUCT":
		return pb.Similarity_SIMILARITY_INNER_PRODUCT
	default:
		return pb.Similarity_SIMILARITY_COSINE
	}
}

// quantizerFromProto maps the console-facing proto enum to the short
// internal name stored in KnowledgeBaseMeta ("" / "QUANTIZER_OFF" ⇒ OFF).
func quantizerFromProto(q pb.QuantizerType) string {
	switch q {
	case pb.QuantizerType_QUANTIZER_SQ8:
		return "SQ8"
	case pb.QuantizerType_QUANTIZER_SQ_BF16:
		return "SQ_BF16"
	case pb.QuantizerType_QUANTIZER_SQ_FP16:
		return "SQ_FP16"
	case pb.QuantizerType_QUANTIZER_PQ:
		return "PQ"
	default:
		return "OFF"
	}
}

func quantizerToProto(s string) pb.QuantizerType {
	switch s {
	case "SQ8":
		return pb.QuantizerType_QUANTIZER_SQ8
	case "SQ_BF16":
		return pb.QuantizerType_QUANTIZER_SQ_BF16
	case "SQ_FP16":
		return pb.QuantizerType_QUANTIZER_SQ_FP16
	case "PQ":
		return pb.QuantizerType_QUANTIZER_PQ
	default:
		return pb.QuantizerType_QUANTIZER_OFF
	}
}

func kbStatusToProto(s types.KBStatus) pb.KBStatus {
	switch s {
	case types.KBStatusDeleting:
		return pb.KBStatus_KB_STATUS_DELETING
	case types.KBStatusDeleteFailed:
		return pb.KBStatus_KB_STATUS_DELETE_FAILED
	default:
		return pb.KBStatus_KB_STATUS_ACTIVE
	}
}

// generateKBID produces a unique knowledge base ID. Uses a simple
// counter-based approach; in production, a UUID library would be used,
// but the design docs do not specify a particular ID scheme, and a
// short ID is friendlier for debugging/ops. The name is folded in for
// human readability.
var kbIDCounter int

func generateKBID(name string) string {
	kbIDCounter++
	short := name
	if len(short) > 20 {
		short = short[:20]
	}
	return fmt.Sprintf("%s-%d", short, kbIDCounter)
}

// Ensure interface compliance.
var _ pb.KnowledgeBaseServiceServer = (*KnowledgeBaseServiceImpl)(nil)
