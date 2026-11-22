package sync

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// PushHandler is this node's data-plane service: it receives pushed versions
// and answers presence queries about what it holds. Writes go through exactly
// the logic the pull path uses (Follower.applyEntry), so both directions
// converge on the same local state.
type PushHandler struct {
	executionGate *storage.ExecutionGate
	directory     storage.Reader
	placements    storage.Resolver
	pb.UnimplementedDataSyncServiceServer

	follower *Follower
	nodeID   int64

	// localVersion reports this node's contiguous data cursor, so peers can
	// ask how far its history reaches (docs/参考/历史/Stratum_设计文档v13.md §7.6). Optional:
	// without it the node answers 0 ("nothing known"), which makes it a
	// useless backfill source rather than a wrong one.
	localVersion LocalVersionReporter

	// advanceVersion moves that cursor once this node has received a version's
	// records. Optional, but a node without it answers 0 forever: the records
	// arrived, the data is complete, and the station's freshness check (§9.3(2))
	// still refuses this replica with "local history reaches version 0".
	advanceVersion LocalVersionAdvancer

	// dropper reclaims a version's physical data on request (v13 §10.6).
	dropper VersionDataDropper

	// serving is the index surface a migration target needs before it can be
	// activated. Optional: without it the serving step refuses to run, because
	// "index readiness could not be checked" must not be reported as ready.
	serving MigrationServing

	// tombstones is this node's record of versions it has been told are deleted.
	// Two jobs: DeleteVersionData writes it (before reclaiming), and the landing
	// paths read it to refuse a version whose metadata no longer exists anywhere
	// else. Optional: without it a late push is written and only the reverse
	// reconciliation ever notices.
	tombstones VersionTombstones

	// watcher is the §7.3 takeover hook: this node received a version, so it
	// should be ready to stand up for it if the coordinator goes quiet.
	watcher VersionWriteWatcher

	// logger records the receive side's own decisions. Nop unless wired, but
	// wiring it matters for one decision in particular: a §8.4(a) probe that
	// could not be answered turns a skip back into a full transfer, and without
	// a log line that looks identical to "the peer really did not have it".
	logger *zap.Logger

	// dataSources, when set, records which peer announced it holds a version's
	// data (§8.5): the confirmation carries the writer's own address, and the
	// cleanup that reclaims a version's data drops the entry again. Optional:
	// without it this node learns nothing and keeps resolving sources the
	// pre-§8.5 way (ask the leader).
	dataSources DataSourceRegistry

	// members is the replica set owner sets are computed over, and inactiveK is
	// the k used for versions that are not the active one
	// (docs/已实施/index-owner-sharding-plan.md §3/§5.2). Optional: with no members this
	// handler builds on every landing, which is exactly the pre-sharding
	// behaviour — the safe default for a deployment that has not opted in.
	//
	// activeVersion answers §8.6b's question, and it has to be asked HERE rather
	// than only in the apply hook: a pure storage node (role=storage) runs no
	// Raft and gets no OnVersionCreated, so this is its only chance to have the
	// active version warm before a query arrives.

	// writeExecutor, when set, lets this node act as the coordinator the
	// control layer dispatched a write to (§7.13.2): the local write
	// transaction runs here, and the fan-out to the other replicas starts here.
	writeExecutor VersionWriteExecutor

	// versionChanges, when set, answers a lagging peer's request for the changes
	// recorded over a version range (§7.5). Optional: without it this node cannot
	// serve delta backfill at all, and peers fall back to pulling full records.
	versionChanges VersionChangesReader

	// heartbeats, when set, receives the storage nodes' heartbeats; isLeader gates it
	// (only the control leader keeps the record).
	heartbeats HeartbeatRecorder
	isLeader   func() bool

	// chainTails, when set, supplies the version at the TAIL of each knowledge
	// base's replicated chain — the newest version the control layer has accepted.
	// A node whose own cursor is behind it has fallen behind and may start catching up;
	// whether and when it does is the node's own decision
	// (docs/已实施/active-lag-detection-design.md §5).
	chainTails ChainTailSource
}

// ChainTailSource answers "what is the newest version the control layer has accepted"
// for every knowledge base, and names the state of that answer so a node can tell when it
// has to ask again. *plane.LocalControlPlane implements it. Declared here because plane
// imports this package.
type ChainTailSource interface {
	ChainGeneration() (term, seq uint64, ok bool)
	ChainTails(ctx context.Context) (map[string]int64, error)
}

// HeartbeatRecorder is the control leader's record of who is alive, reduced to the one
// call the receive side makes. *plane.LivenessRegistry implements it. Declared here,
// rather than importing plane, because plane imports this package.
type HeartbeatRecorder interface {
	Record(nodeID int64)
}

// VersionWriteExecutor runs the storage layer's write transaction for a version
// the control layer has already committed — what a dispatched coordinator does
// (§7.13.2). *plane.LocalDataPlane implements it. Declared here, rather than
// importing plane, because plane imports this package.
type VersionWriteExecutor interface {
	WriteVersionData(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) error
}

// WithVersionWriteExecutor wires the local write path a dispatched coordinator
// runs (§7.13.2).
func WithVersionWriteExecutor(e VersionWriteExecutor) PushHandlerOption {
	return func(h *PushHandler) { h.writeExecutor = e }
}

// VersionChangesReader reads back the changes this node recorded for a range of
// versions — what a lagging peer asks for when it catches up by replaying the
// delta instead of pulling each version's full record set (§7.5). *wal.FileWAL
// implements it; declared narrow here so this package does not depend on the WAL
// implementation.
type VersionChangesReader interface {
	ChangesInRange(ctx context.Context, kbID string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error)
}

// WithVersionChangesReader wires the §7.5 delta source.
func WithVersionChangesReader(r VersionChangesReader) PushHandlerOption {
	return func(h *PushHandler) { h.versionChanges = r }
}

// WithHeartbeatRecorder wires the heartbeat sink and the leader check that decides
// whether this node should keep the record at all. Both are passed together because they
// are two halves of one decision: a node that records without knowing whether it is the
// leader would store heartbeats nobody reads, and one that knows without recording would
// answer accepted=true and drop them. isLeader may be nil (treated as "never the leader":
// the single-node default for tests and for nodes with no control leadership).
func WithHeartbeatRecorder(rec HeartbeatRecorder, isLeader func() bool) PushHandlerOption {
	return func(h *PushHandler) {
		h.heartbeats = rec
		h.isLeader = isLeader
	}
}

// WithChainTails wires the source of the chain generation carried back on a heartbeat's
// response and of the tails GetChainTails answers with
// (docs/已实施/active-lag-detection-design.md).
func WithChainTails(src ChainTailSource) PushHandlerOption {
	return func(h *PushHandler) { h.chainTails = src }
}

// DataSourceRegistry is the slice of plane.DataSourceRegistry that the receive
// side needs: record an announcement, forget one when the data goes away. It is
// declared here rather than importing plane, so the sync package stays
// independent of the storage layer's types.
type DataSourceRegistry interface {
	Register(kbID string, versionID int64, addr string)
	ForgetVersion(kbID string, versionID int64)
}

// WithDataSourceRegistry wires the §8.5 announcement sink.
func WithDataSourceRegistry(reg DataSourceRegistry) PushHandlerOption {
	return func(h *PushHandler) { h.dataSources = reg }
}

// LocalVersionReporter reports the highest version a node holds contiguously
// for a knowledge base (0 = nothing known yet).
type LocalVersionReporter interface {
	LocalVersionOf(kbID string) int64
}

// VersionDataDropper removes one version's physical data on this node. The
// write path falls back to the local stores (not the replicated metadata)
// because §10.6's cleanup reclaims exactly the physical data whose
// acknowledgement was lost.
type VersionDataDropper interface {
	DropVersionStorage(ctx context.Context, kbID string, versionID int64) error
}

// WithVersionDataDropper wires the cleanup target for DeleteVersionData.
func WithVersionDataDropper(d VersionDataDropper) PushHandlerOption {
	return func(h *PushHandler) { h.dropper = d }
}

// VersionTombstoneReader answers "does this node already know the version was
// deleted?" — the one question every landing path asks before writing anything.
//
// It is deliberately a LOCAL question with a local answer. After a deletion
// completes its metadata row is gone, so a late push or a backfill running behind
// arrives with nothing at the control layer that could name it; the receiving
// node is the only one that can still refuse it, and only if it kept a record
// (internal/tombstone).
type VersionTombstoneReader interface {
	Contains(ctx context.Context, kbID string, versionID int64) (bool, error)
	// ContainsKB reports whether the whole knowledge base is known deleted. A version
	// of a deleted KB has no entry of its own — the KB-level entry is what refuses it.
	ContainsKB(ctx context.Context, kbID string) (bool, error)
}

// VersionTombstoneRecorder is the write half: the deletion path records what it
// was told, and marks it finished once this node's own reclaim succeeds.
//
// Both writes travel through the same value because they have to agree about
// what an entry MEANS — "told, not cleaned up yet" refuses data exactly like
// "cleaned up" does, which is why Note is called before the reclaim rather than
// after it.
type VersionTombstoneRecorder interface {
	Note(ctx context.Context, kbID string, versionID int64, at time.Time) error
	MarkDone(ctx context.Context, kbID string, versionID int64, at time.Time) error
}

// VersionTombstones is what the receive side needs from this node's table.
type VersionTombstones interface {
	VersionTombstoneReader
	VersionTombstoneRecorder
}

// WithVersionTombstones wires this node's record of deleted versions. Optional,
// and a node without one behaves exactly as it did before the table existed:
// late arrivals are written and the reverse reconciliation finds them later.
func WithVersionTombstones(t VersionTombstones) PushHandlerOption {
	return func(h *PushHandler) { h.tombstones = t }
}

// VersionWriteWatcher is the §7.3 takeover hook. The receiving side tells the
// storage layer "I have just taken delivery of version V", and the storage
// layer watches for the coordinator's confirmation — announcing the version
// itself if that confirmation never comes. The two calls are a pair: every
// Watch should end in a Confirm or in a takeover.
type VersionWriteWatcher interface {
	WatchVersionWrite(kbID string, versionID int64)
	ConfirmVersionWrite(kbID string, versionID int64)
}

// WithVersionWriteWatcher wires the §7.3 takeover hook.
func WithVersionWriteWatcher(w VersionWriteWatcher) PushHandlerOption {
	return func(h *PushHandler) { h.watcher = w }
}

// WithLogger wires the receive side's log. Optional, but a §8.4(a) probe that
// fails is otherwise invisible: "I could not find out" and "it is not there"
// lead to the same behaviour — accept the push — and only one of them is
// routine.
func WithLogger(l *zap.Logger) PushHandlerOption {
	return func(h *PushHandler) {
		if l != nil {
			h.logger = l
		}
	}
}

// PushHandlerOption configures a PushHandler.
type PushHandlerOption func(*PushHandler)

// WithLocalVersion wires the node's data cursor.
func WithLocalVersion(r LocalVersionReporter) PushHandlerOption {
	return func(h *PushHandler) { h.localVersion = r }
}

// LocalVersionAdvancer moves the cursor LocalVersionReporter answers from.
//
// Receiving a version's records — over the push or the pull path — is what
// makes this node hold it, and the cursor is how it says so (§7.6/§9.3(2)).
// The reporter alone is not enough, and its absence is silent in the worst
// way: a replica with every record on disk answered "version 0", the station's
// freshness check refused it as stale, and reads fell through to whichever
// candidate happened to be as far behind.
type LocalVersionAdvancer interface {
	MarkVersionContiguous(kbID string, versionID int64)
}

// WithLocalVersionAdvancer wires the cursor update a received version performs.
func WithLocalVersionAdvancer(a LocalVersionAdvancer) PushHandlerOption {
	return func(h *PushHandler) { h.advanceVersion = a }
}

// NewPushHandler returns a PushHandler applying through follower.
func NewPushHandler(follower *Follower, nodeID int64, opts ...PushHandlerOption) *PushHandler {
	h := &PushHandler{
		follower: follower, nodeID: nodeID, logger: zap.NewNop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	return h
}

var _ pb.DataSyncServiceServer = (*PushHandler)(nil)

// PushVersionData implements DataSyncService.PushVersionData.
func (h *PushHandler) PushVersionData(stream pb.DataSyncService_PushVersionDataServer) error {
	// Applying a pushed version needs the local stores. A control node has none,
	// and it still serves this service for the cursor reports it owns as leader
	// — so the answer is "this node does not do that", not a nil dereference.
	if h.follower == nil {
		return status.Errorf(codes.Unimplemented, "sync: PushVersionData: this node holds no storage")
	}
	ctx := stream.Context()

	var kbID string
	var versionID int64
	// deletedHere records that this node already knows the version is gone. The
	// rest of the stream is drained and the call closes SUCCESSFULLY: the sender has
	// nothing to fix, and an error would make it retry a version that cannot be
	// wanted again. Nothing of it is written, and this node does not claim it — no
	// cursor move, no bloom rebuild, no takeover timer.
	deletedHere := false
	var streamKB string
	var streamVersion int64
	var pendingDocIDs []string
	var pendingDocVersion int64
	releaseMutation := func() {}
	defer func() { releaseMutation() }()
	// RPC metadata names even an empty version, which has no document frames.
	// Raw legacy streams still learn the target from VersionDocList frames.
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		kbs, versions := md.Get("x-stratum-push-kb"), md.Get("x-stratum-push-version")
		if len(kbs) > 0 || len(versions) > 0 {
			if len(kbs) != 1 || len(versions) != 1 || kbs[0] == "" {
				return status.Error(codes.InvalidArgument, "sync: invalid push identity")
			}
			v, err := strconv.ParseInt(versions[0], 10, 64)
			if err != nil || v <= 0 {
				return status.Error(codes.InvalidArgument, "sync: invalid push version")
			}
			streamKB, kbID, streamVersion, versionID = kbs[0], kbs[0], v, v
			ctx, releaseMutation, err = h.enterMutation(ctx, kbID)
			if err != nil {
				return stratumerrors.ToGRPCStatus(err)
			}
		}
	}
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// Only some entry types carry version_id (chunk records do not), so take the
		// last non-zero one rather than the last record's value. Recorded before the
		// check below, because the FIRST frame is what names the version.
		kbID = entry.GetKbId()
		if streamKB == "" {
			streamKB = kbID
			var guardErr error
			ctx, releaseMutation, guardErr = h.enterMutation(ctx, kbID)
			if guardErr != nil {
				return stratumerrors.ToGRPCStatus(guardErr)
			}
		} else if kbID != streamKB {
			return status.Error(codes.InvalidArgument, "sync: a push cannot span knowledge bases")
		}
		if v := entry.GetVersionId(); v != 0 && entry.GetEntryType() == pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST {
			if streamVersion != 0 && streamVersion != v {
				return status.Error(codes.InvalidArgument, "sync: a push cannot span versions")
			}
			streamVersion = v
			versionID = v
		} else if v != 0 && streamVersion == 0 {
			// MVCC document rows may name an older version. Once the document
			// set named this transfer's target, these rows cannot change it.
			versionID = v
		}
		if !deletedHere && versionID != 0 && h.tombstones != nil {
			if kbGone, kbErr := h.tombstones.ContainsKB(ctx, kbID); kbErr == nil && kbGone {
				deletedHere = true
				h.logger.Info("sync: refusing a pushed version of a knowledge base this node knows was deleted",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
			}
		}
		if !deletedHere && versionID != 0 && h.tombstones != nil {
			gone, tombErr := h.tombstones.Contains(ctx, kbID, versionID)
			switch {
			case tombErr != nil:
				// "I could not read the table" is not "the version is alive". An
				// unreadable answer must not silently turn into a refusal (the push is
				// a legitimate arrival in every case but the one it could not check),
				// and it must not be silent either — the reverse reconciliation still
				// covers this node, which is exactly what that pass is for.
				h.logger.Warn("sync: could not read the tombstone table; accepting the pushed version",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(tombErr))
			case gone:
				deletedHere = true
				h.logger.Info("sync: refusing a pushed version this node knows was deleted",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
			}
		}
		if deletedHere {
			continue
		}
		// Defer VERSION_DOC_LIST until all DocStore/ChunkDocMapper/ChunkVector
		// frames have applied and flushed so concurrent readers never observe
		// a complete DocIDSetHash before the version's chunks and documents land.
		if entry.GetEntryType() == pb.SyncEntryType_SYNC_ENTRY_TYPE_VERSION_DOC_LIST {
			v := entry.GetVersionId()
			if v == 0 {
				v = versionID
			}
			if pendingDocVersion == 0 {
				pendingDocVersion = v
			}
			if entry.GetKbId() == kbID && v == pendingDocVersion {
				pendingDocIDs = append(pendingDocIDs, entry.GetDocId())
				continue
			}
		}
		if err := h.follower.applyEntry(ctx, entry); err != nil {
			// Logged HERE, before the error becomes a gRPC status: the sender only
			// ever sees "stream ended early" (a Send that returns io.EOF once this
			// handler returns), which is unactionable on its own. Measured on the
			// 3+3 cluster: a fan-out push failed with `stream ChunkDocMapper
			// forward: EOF` while the receiver logged nothing at all, because this
			// return value never reaches a log on either side.
			h.logger.Warn("sync: a pushed entry could not be applied; the sender will see the stream end early",
				zap.String("kb_id", entry.GetKbId()), zap.Int64("version_id", entry.GetVersionId()),
				zap.String("entry_type", entry.GetEntryType().String()), zap.Error(err))
			return fmt.Errorf("sync: push apply (%s v%d): %w", entry.GetKbId(), entry.GetVersionId(), err)
		}
	}

	if deletedHere {
		return stream.SendAndClose(&pb.PushVersionDataResponse{
			KnowledgeBaseId: kbID,
			VersionId:       versionID,
			AcceptorId:      h.nodeID,
		})
	}

	if kbID != "" {
		// ACK and the persisted contiguous cursor must not outlive chunk data,
		// and VersionDocList must not become visible before chunk/doc rows land.
		if err := h.follower.flushAndCommitVersionDocList(ctx, kbID, pendingDocVersion, pendingDocIDs); err != nil {
			return err
		}
		// Same post-apply step as the pull path: the records are in the local
		// stores, so this node holds the version — move the cursor, then
		// schedule this node's own index build.
		if h.advanceVersion != nil {
			h.advanceVersion.MarkVersionContiguous(kbID, versionID)
		}
		// §8.4: a replica INSTALLS the coordinator's artifact rather than building
		// its own. Building here anyway is what made distribution a no-op — this
		// handler runs the moment the DATA lands, while the coordinator still has
		// to finish building before it can ship anything, so the replica always
		// got there first and §8.4(a)'s probe then found an artifact already held.
		//
		// The build is therefore restricted to the replica that OWNS the version
		// (docs/已实施/index-owner-sharding-plan.md §5.2): owners() is a pure function of
		// (kbID, versionID) and the member set, so every replica computes the same
		// answer without asking anyone.
		//
		// NO BUILD HAPPENS HERE, and that is §8.6(b)'s lazy construction rather
		// than an omission. Landing data is not a reason to build an index: the
		// only version worth building eagerly is the ACTIVE one, and that decision
		// belongs to the apply hook, which knows which version is active
		// (cmd/stratum's version-created callback, §8.6b). Every other version is
		// built when something asks for it — a query (service/query.go) or an
		// explicit operator request — and the router's owner set decides which
		// nodes those requests reach (docs/已实施/index-owner-sharding-plan.md §5).
		//
		// This handler used to build here for every landing. Whatever gate it had
		// (the owner set, the active version, nothing at all) was answering the
		// wrong question: it made every push arrival a build, which is the eager
		// behaviour §8.6(b) had already replaced.
		// Start the §7.3 takeover timer. The data is here; the only thing that
		// can still go missing is the coordinator's word that a quorum holds
		// it, and this is how a replica notices that word never arrived.
		if h.watcher != nil {
			h.watcher.WatchVersionWrite(kbID, versionID)
		}
	}

	return stream.SendAndClose(&pb.PushVersionDataResponse{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		AcceptorId:      h.nodeID,
	})
}

// ConfirmVersionWrite implements DataSyncService.ConfirmVersionWrite: the
// coordinator says the version reached quorum, so this node stands down its
// §7.3 takeover timer (docs/参考/历史/Stratum_设计文档v13.md §7.3). The same message is §8.5's
// data-source announcement — req.source_addr names the writer — so this node
// records where the version's data lives; later readers resolve it from that
// fact instead of asking the leader (see plane.DataSourceRegistry).
//
// It is also this node's cue that the version exists — a replica that missed the
// push learns of it here, which is why it does not merely stop a timer.
func (h *PushHandler) ConfirmVersionWrite(ctx context.Context, req *pb.ConfirmVersionWriteRequest) (*pb.ConfirmVersionWriteResponse, error) {
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if _, err := h.bindPlacement(ctx, kbID); err != nil {
		return nil, stratumerrors.ToGRPCStatus(err)
	}
	if h.placements != nil && req.GetSourceAddr() != "" {
		snap, err := storage.Resolve(ctx, h.placements, kbID)
		if err != nil {
			return nil, stratumerrors.ToGRPCStatus(err)
		}
		if !snap.HasAddress(req.GetSourceAddr()) {
			return nil, stratumerrors.ToGRPCStatus(stratumerrors.ErrNotKBReplica)
		}
	}
	if h.watcher != nil {
		h.watcher.ConfirmVersionWrite(kbID, versionID)
	}
	if h.dataSources != nil && req.GetSourceAddr() != "" {
		h.dataSources.Register(kbID, versionID, req.GetSourceAddr())
	}
	return &pb.ConfirmVersionWriteResponse{NodeId: h.nodeID}, nil
}

// ExecuteVersionWrite implements DataSyncService.ExecuteVersionWrite: the
// control layer picked this node as the write's coordinator (§7.13.2), so the
// storage-layer write transaction runs here — the data lands on this node, and
// the fan-out to the remaining replicas starts from here as well. Idempotent:
// the local transaction is keyed and replayable (§7.12), so a dispatched retry
// and a client retry of the same write converge.
func (h *PushHandler) ExecuteVersionWrite(ctx context.Context, req *pb.ExecuteVersionWriteRequest) (*pb.ExecuteVersionWriteResponse, error) {
	ctx, release, enterErr := h.enterMutation(ctx, req.GetKnowledgeBaseId())
	if enterErr != nil {
		return nil, stratumerrors.ToGRPCStatus(enterErr)
	}
	defer release()
	var guardErr error
	ctx, guardErr = h.bindPlacement(ctx, req.GetKnowledgeBaseId())
	if guardErr != nil {
		return nil, stratumerrors.ToGRPCStatus(guardErr)
	}
	if h.writeExecutor == nil {
		// Loud, not silent: a dispatch that lands nowhere would leave the
		// version's data missing while the control layer believes it was
		// handed off.
		return nil, status.Errorf(codes.FailedPrecondition,
			"sync: ExecuteVersionWrite: no write executor wired")
	}
	kbID, versionID, parentID := req.GetKnowledgeBaseId(), req.GetVersionId(), req.GetParentVersionId()
	if err := h.writeExecutor.WriteVersionData(ctx, kbID, versionID, parentID, docChangesFromProto(req.GetChanges())); err != nil {
		return nil, status.Errorf(codes.Internal,
			"sync: ExecuteVersionWrite(%s v%d): %v", kbID, versionID, err)
	}
	return &pb.ExecuteVersionWriteResponse{NodeId: h.nodeID, Completed: true}, nil
}

// PullVersionChanges implements DataSyncService.PullVersionChanges: it streams the
// recorded changes for (fromExclusive, toInclusive], so a peer that is behind can
// replay the delta instead of transferring each version's full record set (§7.5).
//
// Versions this node holds no record for are simply skipped — the caller has to
// SEE the gap and fall back to a full state transfer, rather than replay a range
// that silently omits versions.
func (h *PushHandler) PullVersionChanges(req *pb.PullVersionChangesRequest, stream pb.DataSyncService_PullVersionChangesServer) error {
	if h.versionChanges == nil {
		return status.Errorf(codes.FailedPrecondition, "sync: PullVersionChanges: no changes reader wired")
	}
	kbID := req.GetKnowledgeBaseId()
	deltas, err := h.versionChanges.ChangesInRange(stream.Context(), kbID, req.GetFromExclusive(), req.GetToInclusive())
	if err != nil {
		return status.Errorf(codes.Internal, "sync: PullVersionChanges(%s, (%d,%d]): %v",
			kbID, req.GetFromExclusive(), req.GetToInclusive(), err)
	}

	// Ascending version order: the peer replays in sequence, so it should not have
	// to buffer and sort the whole range first.
	versions := make([]int64, 0, len(deltas))
	for v := range deltas {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	for _, v := range versions {
		d := deltas[v]
		if err := stream.Send(&pb.VersionChanges{
			VersionId:       d.VersionID,
			ParentVersionId: d.ParentVersionID,
			Changes:         DocChangesToProto(d.Changes),
		}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteVersionData implements DataSyncService.DeleteVersionData: it reclaims
// the version's physical data here, on the control layer's instruction
// (docs/参考/历史/Stratum_设计文档v13.md §10.6).
//
// A node with NO dropper wired is an ERROR, not a "nothing to drop" answer. The
// two are indistinguishable on the wire otherwise, and that is exactly how the
// window in docs/未实施/known-gaps.md §B/§C opens: the broadcaster reads "success", the
// control layer goes on to remove the version's metadata, and whatever bytes this
// node still holds lose the last row that could name them — the terminal-reclaim
// queue, the delete flow's re-discovery and the startup reconcile all key off
// metadata. §10.6 states the principle for the local path ("一个悄悄什么都不做的
// 清理，正是孤儿数据活下来的方式"); this is the same rule on the receiving end.
//
// A node that simply never received the version stays a SUCCESS: DropVersionStorage
// is an idempotent prefix delete, so "I hold nothing" is an ordinary outcome and
// must not be reported as a failure.
func (h *PushHandler) DeleteVersionData(ctx context.Context, req *pb.DeleteVersionDataRequest) (*pb.DeleteVersionDataResponse, error) {
	if h.executionGate != nil {
		if err := storage.RequireRequestEpoch(ctx, req.GetKnowledgeBaseId()); err != nil {
			return nil, stratumerrors.ToGRPCStatus(err)
		}
	}
	ctx, release, enterErr := storage.EnterCleanup(ctx, h.executionGate, h.directory, h.nodeID, req.GetKnowledgeBaseId())
	if enterErr != nil {
		return nil, stratumerrors.ToGRPCStatus(enterErr)
	}
	defer release()
	kbID, versionID := req.GetKnowledgeBaseId(), req.GetVersionId()
	if h.dropper == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"sync: DeleteVersionData(%s v%d): no version-data dropper is wired on this node, so it cannot reclaim the version and must not be reported as cleaned up", kbID, versionID)
	}

	// Record the deletion BEFORE reclaiming the bytes. The two orders differ in one
	// window that matters, and it is the whole reason this table exists: a crash or a
	// failed reclaim in between leaves either "told, not cleaned up" — resumed by this
	// node's own retry loop, and already refusing data — or "cleaned up, not recorded",
	// which is the state a late push re-lands data into with nothing left to notice it.
	if h.tombstones != nil {
		if err := h.tombstones.Note(ctx, kbID, versionID, time.Now()); err != nil {
			return nil, status.Errorf(codes.Internal, "sync: DeleteVersionData(%s v%d): record the tombstone: %v", kbID, versionID, err)
		}
	}
	if err := h.dropper.DropVersionStorage(ctx, kbID, versionID); err != nil {
		return nil, status.Errorf(codes.Internal, "sync: DeleteVersionData(%s v%d): %v", kbID, versionID, err)
	}
	if h.tombstones != nil {
		// A failure here leaves the entry PENDING, which is the conservative side: the
		// reclaim already succeeded, so the retry loop's next attempt is an idempotent
		// prefix delete over nothing.
		if err := h.tombstones.MarkDone(ctx, kbID, versionID, time.Now()); err != nil {
			h.logger.Warn("sync: could not mark a tombstone done; the retry loop finishes it",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}
	// The version's data is gone here, so any §8.5 announcement pointing at
	// this node is stale: drop it, or a later pull would aim at a peer that no
	// longer holds the version.
	if h.dataSources != nil {
		h.dataSources.ForgetVersion(kbID, versionID)
	}
	return &pb.DeleteVersionDataResponse{Dropped: true, NodeId: h.nodeID}, nil
}
