package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"stratum/internal/coordinator"
	"stratum/internal/plane"
	"stratum/internal/raft"
	stratumsync "stratum/internal/sync"
	"stratum/internal/tombstone"
	"stratum/internal/types"
	"stratum/internal/versionview"
	"stratum/service"
)

// loggerAtLevel returns base rebuilt at the configured level (zap's names:
// debug/info/warn/error).
//
// Empty means "keep base". An unknown level also keeps base and says so: a typo
// in the config must not take the node down, and this field's whole purpose is
// observability. Before this existed the setting was read by nobody — the node
// built an info-level logger before the config file was even parsed — so
// `logging.level: debug` silently had no effect and no debug line was reachable
// (that is how the per-stage query timings stayed invisible, v13 §5 #18).
func loggerAtLevel(base *zap.Logger, level string) *zap.Logger {
	if level == "" {
		return base
	}
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		base.Warn("unknown logging level; keeping the current one",
			zap.String("level", level), zap.Error(err))
		return base
	}
	pc := zap.NewProductionConfig()
	pc.Level = zap.NewAtomicLevelAt(lvl)
	rebuilt, err := pc.Build()
	if err != nil {
		base.Warn("could not rebuild the logger at the configured level",
			zap.String("level", level), zap.Error(err))
		return base
	}
	return rebuilt
}

// controlAddrsFromPeers is the control cluster's address table as a storage node
// needs it: every Raft peer by ID. The peers list is the control layer's own
// membership, which is exactly who can answer a metadata read or accept a
// forwarded proposal.
func controlAddrsFromPeers(peers []raft.PeerConfig) map[int64]string {
	addrs := make(map[int64]string, len(peers))
	for _, peer := range peers {
		if peer.ServiceAddr != "" {
			addrs[peer.ID] = peer.ServiceAddr
		}
	}
	return addrs
}

// replicaPusher adapts stratumsync.Pusher to plane.VersionPusher: the storage
// layer only needs the acknowledging replica's node ID, not the whole
// response.
type replicaPusher struct {
	pusher *stratumsync.Pusher
}

func (r replicaPusher) PushVersion(ctx context.Context, targetAddr, kbID string, versionID int64) (int64, error) {
	ack, err := r.pusher.PushVersion(ctx, targetAddr, kbID, versionID)
	if err != nil {
		return 0, err
	}
	return ack.GetAcceptorId(), nil
}

// reportIndexStatus maps an index build outcome onto the ControlPlane
// contract: a finished build reports the index ready, a failed one reports a
// failure on the INDEX side, which is where a build's retry budget and its
// terminal verdict live (docs/参考/历史/Stratum_设计文档v13.md §10.1, §10.1b).
//
// It deliberately no longer goes through the storage layer's availability
// channel (plane.ControlPlane had a ReportAvailability method, now removed):
// that channel described the version's abstract availability, a different
// question from "did this build fail". Routing build failures through it set a
// retryable FAILED with no budget behind it — so nothing ever reached a verdict
// for the index side, and a build that kept failing simply kept reporting.
// deletionLogSource narrows a raft node to the incremental deletion-log read, or nil
// when this shape cannot answer it. The sweep treats nil as "no log reachable" and
// stays on the live-chain path, which is the behaviour before the log existed.
func deletionLogSource(rn raft.RaftNode) plane.DeletionLogSource {
	if src, ok := rn.(plane.DeletionLogSource); ok {
		return src
	}
	return nil
}

// tombstoneRecorder returns the delete-version coordinator's tombstone recorder, or nil
// when this node keeps no table.
//
// The nil has to be an untyped interface nil: handing back the nil *PebbleStore would
// give the coordinator a NON-nil interface holding a nil pointer, and the first call
// would dereference it.
func versionViewCleaner(s *versionview.Store) coordinator.VersionViewCleaner {
	if s == nil {
		return nil
	}
	return s
}

func tombstoneRecorder(t *tombstone.PebbleStore) coordinator.VersionTombstoneRecorder {
	if t == nil {
		return nil
	}
	return t
}

// tombstoneOption returns the receive side's tombstone option, or nil when this
// node keeps no table. NewPushHandler skips nil options, which keeps the call site
// free of a branch wrapped around a single argument.
func tombstoneOption(t *tombstone.PebbleStore) stratumsync.PushHandlerOption {
	if t == nil {
		return nil
	}
	return stratumsync.WithVersionTombstones(t)
}

func tombstoneTable(t *tombstone.PebbleStore) plane.VersionTombstones {
	if t == nil {
		return nil
	}
	return t
}

// lateWriteCoordBridge breaks the construction cycle between LocalDataPlane and
// WriteCoordinatorImpl in cmd/stratum: LocalDataPlane receives it at construction
// time as VersionWriteExecutor and VersionDataDropper, WriteCoordinatorImpl is
// constructed with that LocalDataPlane, and bind() wires the coordinator before
// any write or recovery starts (failing fast if ever called unbound).
type lateWriteCoordBridge struct {
	coord *coordinator.WriteCoordinatorImpl
}

func (b *lateWriteCoordBridge) WriteVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) ([]string, error) {
	if b.coord == nil {
		return nil, errors.New("stratum: write coordinator bridge not bound")
	}
	return b.coord.WriteVersionStorage(ctx, kbID, parentVersionID, versionID, changes)
}

func (b *lateWriteCoordBridge) DropVersionStorage(ctx context.Context, kbID string, versionID int64) error {
	if b.coord == nil {
		return errors.New("stratum: write coordinator bridge not bound")
	}
	return b.coord.DropVersionStorage(ctx, kbID, versionID)
}

// raftApplyGate lets RaftNodeImpl register its apply callbacks as construction-time
// options before runApplyLoop starts while holding any early replayed callbacks
// until the node assembly finishes wiring dataPlane and coordinators.
type raftApplyGate struct {
	ready     chan struct{}
	stop      chan struct{}
	closeOnce sync.Once
	abortOnce sync.Once

	onCreated           func(kbID string, versionID int64)
	onFailed            func(kbID string, versionID int64)
	onCommittedAsLeader func(kbID string, versionID, parentVersionID int64, clientRequestID string)
}

func newRaftApplyGate() *raftApplyGate {
	return &raftApplyGate{
		ready: make(chan struct{}),
		stop:  make(chan struct{}),
	}
}

func (g *raftApplyGate) activate(
	onCreated func(kbID string, versionID int64),
	onFailed func(kbID string, versionID int64),
	onCommittedAsLeader func(kbID string, versionID, parentVersionID int64, clientRequestID string),
) {
	g.onCreated = onCreated
	g.onFailed = onFailed
	g.onCommittedAsLeader = onCommittedAsLeader
	g.closeOnce.Do(func() { close(g.ready) })
}

func (g *raftApplyGate) abort() {
	g.abortOnce.Do(func() { close(g.stop) })
}

func (g *raftApplyGate) wait() bool {
	select {
	case <-g.ready:
		return true
	case <-g.stop:
		return false
	}
}

func (g *raftApplyGate) handleCreated(kbID string, versionID int64) {
	if !g.wait() || g.onCreated == nil {
		return
	}
	g.onCreated(kbID, versionID)
}

func (g *raftApplyGate) handleFailed(kbID string, versionID int64) {
	if !g.wait() || g.onFailed == nil {
		return
	}
	g.onFailed(kbID, versionID)
}

func (g *raftApplyGate) handleCommittedAsLeader(kbID string, versionID, parentVersionID int64, clientRequestID string) {
	if !g.wait() || g.onCommittedAsLeader == nil {
		return
	}
	g.onCommittedAsLeader(kbID, versionID, parentVersionID, clientRequestID)
}

func reportIndexStatus(ctx context.Context, cp plane.ControlPlane, kbID string, versionID int64, status types.IndexStatus) error {
	switch status {
	case types.IndexStatusReady:
		return cp.ReportIndexReady(ctx, kbID, versionID)
	case types.IndexStatusFailed:
		// The terminal flag is deliberately ignored: it means "the DATA side
		// should reclaim its data". An index-side verdict reclaims nothing —
		// the data may be perfectly durable, which is the whole reason the two
		// sides are counted apart.
		_, err := cp.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideIndex,
			types.FailureTransient, "index build failed")
		return err
	case types.IndexStatusFailedPermanent:
		// 确定性失败：再试一次还是同一批数据、同一个参数，所以声明 FailureFatalGlobal
		// ——它短路重试预算，直接把版本推到终态（internal/plane/local_control_plane.go
		// 的 ReportVersionFailure）。少了这一条，这类失败会以「暂时失败」的名义留在
		// PENDING 上，直到预算被耗光，而预算根本不该花在它身上。
		_, err := cp.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideIndex,
			types.FailureFatalGlobal, "index build failed (non-retryable)")
		return err
	default:
		return fmt.Errorf("index build reported unexpected status %v for version %d", status, versionID)
	}
}

// reclaimDiagnosticsSource adapts the data plane's reclaim diagnosis
// (plane.ReclaimBlocker) to the shape the admin service reports
// (service.ReclaimBlockedReplica). It is bridged here because service does not import
// plane, the same reason storageGateSource exists.
type reclaimDiagnosticsSource struct {
	dp *plane.LocalDataPlane
}

// ReclaimBlockedReplicas reports which replicas kept kbID's reclaim watermark unavailable
// on this node's last pass. ok=false means no pass has judged anything yet: the admin
// service reports that as "judgement unavailable", never as "nothing is blocked".
func (s reclaimDiagnosticsSource) ReclaimBlockedReplicas(kbID string) ([]service.ReclaimBlockedReplica, bool) {
	blockers, ok := s.dp.ReclaimBlockers(kbID)
	if !ok {
		return nil, false
	}
	out := make([]service.ReclaimBlockedReplica, 0, len(blockers))
	for _, b := range blockers {
		out = append(out, service.ReclaimBlockedReplica{Address: b.Address, Reason: b.Reason})
	}
	return out, true
}

// storageGateSource adapts the control plane's redundancy verdict to the shape the
// service layer asks for. It exists to keep the dependency pointing one way: service does
// not import plane's state enum, and both service callers that need the verdict — the
// CreateVersion gate and HealthCheck — take this one adapter.
type storageGateSource struct {
	cp *plane.LocalControlPlane
}

// StorageDegraded reports whether the storage layer is short of a quorum for
// kbID, with a real replica still answering.
//
// ok=false means the verdict is unavailable — this node does not lead, or its
// replica topology is not wired — and every caller treats that as "allow". See
// service.StorageDegradationSource: the verdict is soft state, so it may only
// ever cost a retry, never a refused write that would otherwise have succeeded.
func (s storageGateSource) StorageDegraded(kbID string) (bool, string, bool) {
	return s.cp.StorageDegraded(kbID)
}

// StorageUnavailable is the other tier: not one required replica is live. The two
// are separate methods because service declares its own narrow interface rather
// than importing plane's state enum, and the pair of answers is what decides which
// sentinel a refusal carries (§4.1).
func (s storageGateSource) StorageUnavailable(kbID string) (bool, string, bool) {
	return s.cp.StorageUnavailable(kbID)
}

// pendingDispatchRegistry is the part of the write coordinator the §7.13.2
// hand-off uses: the once-only take of a write's changes, the question that tells
// a lost race from an orphaned version, and the give-up path it deliberately no
// longer calls on a miss.
type pendingDispatchRegistry interface {
	TakePendingDispatch(kbID, clientRequestID string) (int64, []types.DocChange, bool)
	DispatchClaimed(kbID, clientRequestID string) bool
	AbandonDispatch(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string)
}

// versionWriteDispatcher is the one call the hand-off makes on the dispatcher,
// declared narrowly so a test can stand in for a live one.
type versionWriteDispatcher interface {
	Dispatch(ctx context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) (string, error)
}

// dispatchCommittedVersion is the §7.13.2 hand-off, extracted from the apply
// callback so the case Step 0 is about can be tested without a live cluster
// (docs/已实施/await-version-plan.md §4.3, §7 Step 0).
//
// The changes live in THIS process's memory — the Raft command deliberately
// carries none (§7.7) — so a missing TakePendingDispatch means one of two
// things. The registry tells them apart (DispatchClaimed):
//
//   - the background dispatch already took them (Execute dispatches the moment
//     the version is committed; Take is once-only), so the write is on its way.
//     This is a DESIGNED outcome, not a fault: the hook and Execute race, and
//     whichever arrives first wins. Logged at debug.
//   - nobody holds them at all: the process that proposed them is gone (a
//     restarted or deposed leader). Nothing is in flight, and this is the case
//     worth a warning.
//
// Before DispatchClaimed existed the hook could not tell the two apart, so it
// warned on both — and since the first cause happens on every write, the warning
// was noise that hid the second. The distinction is what makes the message mean
// something.
//
// It used to report a transient failure on the miss, and that was wrong twice
// over: an in-memory miss on one node proves neither case, so the retry budget
// could spend a version whose write was in fact in flight; and declaring a
// version dead is the caller's decision, not this node's. What settles such a
// version is the DATA-side existence probe (the same one GetSystemStatus uses),
// which the await path exposes as data_missing, followed by the caller either
// re-sending the changes or discarding the version. Until then it stays
// PENDING — which is the truth for the orphaned case.
func dispatchCommittedVersion(
	registry pendingDispatchRegistry,
	dispatcher versionWriteDispatcher,
	logger *zap.Logger,
	kbID string,
	versionID, parentVersionID int64,
	clientRequestID string,
) {
	regParentID, changes, ok := registry.TakePendingDispatch(kbID, clientRequestID)
	if !ok {
		// A miss has two causes and only one of them is worth a warning. When the
		// proposer's own background dispatch took the registration, the write is
		// already being handled: this hook lost a race it is DESIGNED to lose
		// sometimes, because Execute dispatches as soon as the version is
		// committed and whichever side arrives first wins. Warning there fired on
		// every single write, which buried the cause that matters.
		//
		// That other cause — nobody holds the changes, because the process that
		// proposed them is gone — is what actually leaves a version PENDING with
		// no owner. It is the one worth saying out loud.
		if registry.DispatchClaimed(kbID, clientRequestID) {
			logger.Debug("version committed here; the proposer's dispatch is already in flight",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("client_request_id", clientRequestID))
			return
		}
		logger.Warn("version committed here without a pending dispatch; leaving it PENDING",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.String("client_request_id", clientRequestID))
		return
	}
	if regParentID != 0 {
		parentVersionID = regParentID
	}
	// Its own context: this runs from the apply loop's callback, so it must not
	// be cancelled by whatever request happened to carry the write in.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := dispatcher.Dispatch(ctx, kbID, versionID, parentVersionID, changes); err != nil {
		logger.Warn("write dispatch failed; the client's retry or the retry budget takes over",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}
