// Package raft implements Stratum's RaftNode interface (defined in
// raft.go) on top of internal/kvraft's Raft consensus library, per
// Stratum_实现顺序.md 阶段 3: "依赖现有 kvserver Raft，扩展状态机支持知识库和
// 版本元数据".
//
// Architecture: internal/kvraft handles consensus (leader election, log
// replication, snapshotting) over an opaque []byte command stream; this
// package defines that command encoding (command.go), the deterministic
// state machine that interprets committed commands (state_machine.go),
// and RaftNodeImpl, which wires the two together and implements the
// RaftNode interface's Propose-and-wait semantics on top of kvraft's
// non-blocking Propose primitive.
//
// Propose-and-wait: kvraft.Raft.Propose returns immediately with an
// assigned (index, term), without waiting for the entry to commit or
// apply. RaftNodeImpl's apply-dispatch loop (runApplyLoop) consumes
// kvraft's applyCh, deterministically applies each command to the state
// machine, and — if this node was the one that originally proposed that
// index — delivers the result to whichever Propose* call is blocked
// waiting for it (via the pending map, keyed by index, additionally
// guarded by a term check: if the term of the entry actually applied at
// that index differs from the term Propose returned, the original
// proposal was superseded by a different leader's entry at the same
// index, a normal Raft occurrence after a leadership change).
package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/kvraft"
	"stratum/internal/kvstorage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// errSuperseded is returned to a waiting Propose* call when the log index
// it was waiting on ended up holding a different leader's entry instead
// of the one this node proposed (the original entry was never committed).
// Callers should treat this the same as any other failed propose attempt
// and may retry.
var errSuperseded = errors.New("raft: proposal superseded by a different leader's entry at the same index")

// PeerConfig identifies one other cluster member's Raft RPC address.
type PeerConfig struct {
	ID          int64
	RaftAddr    string
	ServiceAddr string // Stratum-level gRPC address used for data sync (leader→follower)
}

// Config configures a RaftNodeImpl.
type Config struct {
	NodeID   int64
	DataDir  string // base directory for this node's kvraft persistence (term/log/snapshot)
	RaftAddr string // address this node's kvraft gRPC server binds to
	Peers    []PeerConfig

	// WAL is the Stratum write-ahead log (internal/wal), distinct from
	// kvraft's own internal Persister (which only persists Raft's hard
	// state, not Stratum's write-path crash-consistency records). Required.
	WAL wal.WAL

	Logger *zap.Logger

	// Optional tuning knobs; zero values fall back to internal/kvraft's
	// own defaults.
	MaxLogLength       uint64
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// pendingProposal tracks a single in-flight Propose* call waiting for its
// entry to be applied.
type pendingProposal struct {
	term     uint64
	resultCh chan applyResult
}

// RaftNodeImpl is the real RaftNode implementation, built on
// internal/kvraft.
type RaftNodeImpl struct {
	raft      *kvraft.Raft
	wal       wal.WAL
	sm        *stateMachine
	logger    *zap.Logger
	persister *kvstorage.Persister // kvraft's on-disk hard state + snapshots

	applyCh chan kvraft.ApplyMsg

	pendingMu sync.Mutex
	pending   map[uint64]*pendingProposal

	applyLoopDone chan struct{}
	callbackWG    sync.WaitGroup

	// onVersionCreated is an optional callback invoked after a
	// cmdCreateVersion is applied on this node when this node is NOT
	// the proposer (i.e. it is a follower receiving the entry via
	// Raft replication). The leader (proposer) does its own
	// storage-layer writes inline during the coordinator write flow
	// and does not need this hook.
	//
	// Set by the startup wiring (main.go) to trigger data sync from
	// the leader via internal/sync.FollowerSync.PullVersion.
	onVersionCreated func(kbID string, versionID int64)
}

// NewRaftNodeImpl constructs and starts a RaftNodeImpl: it starts the
// underlying kvraft node's gRPC server and background consensus loops,
// and starts this package's own apply-dispatch loop. Returns once
// everything is running (does not wait for a leader to be elected —
// callers should poll, e.g. via GetClusterStatus, if they need to know
// when this node believes there's a leader).
func NewRaftNodeImpl(cfg Config) (*RaftNodeImpl, error) {
	if cfg.WAL == nil {
		return nil, fmt.Errorf("raft: Config.WAL is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	transport := kvraft.NewTransport(logger)
	for _, p := range cfg.Peers {
		if p.ID == cfg.NodeID {
			continue // skip self — the transport only needs OTHER peers
		}
		if err := transport.AddPeer(p.ID, p.RaftAddr, ""); err != nil {
			return nil, fmt.Errorf("raft: add peer %d at %s: %w", p.ID, p.RaftAddr, err)
		}
	}

	applyCh := make(chan kvraft.ApplyMsg, 256)
	persister := kvstorage.NewPersister(filepath.Join(cfg.DataDir, "raft"))

	opts := []kvraft.Option{kvraft.WithLogger(logger)}
	if cfg.MaxLogLength > 0 {
		opts = append(opts, kvraft.WithMaxLogLength(cfg.MaxLogLength))
	}
	if cfg.HeartbeatInterval > 0 {
		opts = append(opts, kvraft.WithHeartbeatInterval(cfg.HeartbeatInterval))
	}
	if cfg.ElectionTimeoutMin > 0 && cfg.ElectionTimeoutMax > 0 {
		opts = append(opts, kvraft.WithElectionTimeoutRange(cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax))
	}

	rf := kvraft.NewRaft(cfg.NodeID, transport, applyCh, persister, opts...)

	impl := &RaftNodeImpl{
		raft:          rf,
		wal:           cfg.WAL,
		sm:            newStateMachine(),
		logger:        logger,
		persister:     persister,
		applyCh:       applyCh,
		pending:       make(map[uint64]*pendingProposal),
		applyLoopDone: make(chan struct{}),
	}

	// Restore the state machine from any persisted snapshot (taken
	// locally before a shutdown, or installed from a leader) BEFORE the
	// apply loop starts. Without this, a restart after log compaction
	// would boot an empty state machine whose lastApplied bookkeeping
	// (also restored from the snapshot by kvraft.NewRaft) claims all the
	// compacted entries were applied — subsequent commands would then be
	// applied on top of nothing, losing every pre-snapshot KB/version.
	if data, _, err := persister.LoadSnapshot(); err != nil {
		impl.logger.Error("failed to load persisted snapshot; starting with empty state machine",
			zap.Int64("node_id", cfg.NodeID), zap.Error(err))
	} else if data != nil {
		if err := impl.sm.restore(data); err != nil {
			impl.logger.Error("failed to restore state machine from persisted snapshot; starting with empty state machine",
				zap.Int64("node_id", cfg.NodeID), zap.Error(err))
		}
	}

	// Provide snapshot data when a peer falls behind the start of this
	// node's trimmed log: prefer the durable snapshot (its index is at
	// least log[0].Index, since every local compaction persists one at
	// LastApplied >= log[0].Index); fall back to serializing the live
	// state machine, which covers everything applied so far.
	transport.SetSnapshotHandler(func(peerID int64, lastIndex uint64) []byte {
		data, snapIndex, err := persister.LoadSnapshot()
		if err == nil && len(data) > 0 && snapIndex >= lastIndex {
			return data
		}
		data, err = impl.sm.serialize()
		if err != nil {
			impl.logger.Error("failed to serialize state machine for snapshot transfer",
				zap.Int64("node_id", cfg.NodeID), zap.Int64("peer_id", peerID), zap.Error(err))
			return nil
		}
		return data
	})

	if err := rf.StartGRPC(cfg.RaftAddr); err != nil {
		return nil, fmt.Errorf("raft: start gRPC on %s: %w", cfg.RaftAddr, err)
	}
	rf.Run()
	go impl.runApplyLoop()

	return impl, nil
}

// Stop gracefully shuts down this node: stops the underlying kvraft node
// (which itself waits for its background goroutines and gRPC server to
// finish), then waits for the apply-dispatch loop to exit, and finally
// waits for any onVersionCreated callback goroutines the apply loop may
// have launched to finish — so that a caller which closes the storage
// layer after Stop returns can never race an in-flight callback touching
// an already-closed store. Safe to call multiple times.
func (impl *RaftNodeImpl) Stop() {
	impl.raft.Stop()
	<-impl.applyLoopDone
	impl.callbackWG.Wait()
}

// SetOnVersionCreated registers a callback that is invoked when a
// cmdCreateVersion is applied on a non-proposer node (follower). The
// leader does its own storage-layer writes inline and does not use this
// hook. Call before the first propose; not safe for concurrent use after
// the apply loop has started.
func (impl *RaftNodeImpl) SetOnVersionCreated(fn func(kbID string, versionID int64)) {
	impl.onVersionCreated = fn
}

// runApplyLoop consumes committed entries (and snapshot requests) from
// kvraft, applies them to the state machine, and delivers results to any
// locally-waiting Propose* call. Exits when the underlying kvraft node's
// Done channel closes.
func (impl *RaftNodeImpl) runApplyLoop() {
	defer close(impl.applyLoopDone)
	for {
		select {
		case msg := <-impl.applyCh:
			if msg.IsSnapshot {
				impl.handleSnapshotMsg(msg)
				continue
			}
			impl.handleEntryMsg(msg)
		case <-impl.raft.Done():
			return
		}
	}
}

func (impl *RaftNodeImpl) handleEntryMsg(msg kvraft.ApplyMsg) {
	if len(msg.Command) == 0 {
		// kvraft's automatic no-op entry, proposed internally whenever a
		// node becomes leader (see internal/kvraft's SendVoteRequest doc
		// comment) so the new leader's commit index can advance past
		// older-term entries without waiting for real application
		// traffic. Nothing for the state machine to do; no Stratum
		// caller is ever waiting on this index either, since RaftNodeImpl
		// never proposes one itself.
		return
	}

	cmd, err := decodeCommand(msg.Command)
	var result applyResult
	if err != nil {
		impl.logger.Error("failed to decode committed command; this should never happen for a self-generated command stream",
			zap.Uint64("index", msg.Index), zap.Error(err))
		result = applyResult{Err: fmt.Errorf("raft: corrupt command at index %d: %w", msg.Index, err)}
	} else {
		result = impl.sm.apply(context.Background(), cmd, impl.wal, impl.logger)
	}

	impl.pendingMu.Lock()
	waiter, ok := impl.pending[msg.Index]
	if ok {
		delete(impl.pending, msg.Index)
	}
	impl.pendingMu.Unlock()

	if !ok {
		// This node is not waiting on this index — it did not propose
		// it. If the command was a CreateVersion, notify the data-sync
		// layer so this follower pulls storage-layer data from the
		// leader.
		if err == nil && cmd.Type == cmdCreateVersion && impl.onVersionCreated != nil {
			// Run asynchronously: pulling storage-layer data can block
			// (dial + stream + retries), and a blocked apply loop would
			// stall every subsequent committed entry — most visible when
			// a follower replays its log after a restart.
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				impl.onVersionCreated(cmd.KBID, result.VersionID)
			}()
		}
		return // no local caller waiting (e.g. this is a follower)
	}
	if waiter.term != msg.Term {
		// The entry originally proposed at this index under waiter.term
		// was never committed; what got committed instead is a different
		// leader's entry that happens to share the same index.
		waiter.resultCh <- applyResult{Err: errSuperseded}
		return
	}
	waiter.resultCh <- result
}

func (impl *RaftNodeImpl) handleSnapshotMsg(msg kvraft.ApplyMsg) {
	if msg.SnapshotData == nil {
		// Locally triggered: the log has grown past the configured
		// threshold. Deep-copy the state machine (fast, under RLock),
		// then serialize + persist asynchronously so a slow snapshot can
		// never stall the apply loop (the "snapshot blocking" hang).
		// The snapshot index is the one frozen by kvraft when the request
		// was sent — not LastApplied() read later, which may have
		// advanced past what the copied data covers.
		snap := impl.sm.deepCopy()
		snapIndex := msg.SnapshotIndex
		if snapIndex == 0 {
			// Fallback for a zero value (shouldn't happen with the
			// current kvraft, which always fills it).
			snapIndex = impl.raft.LastApplied()
		}
		impl.callbackWG.Add(1)
		go func() {
			defer impl.callbackWG.Done()
			data, err := encodeSnapshot(snap)
			if err != nil {
				impl.logger.Error("failed to serialize state machine for snapshot", zap.Error(err))
			} else {
				impl.raft.Snapshot(snapIndex, data)
			}
			impl.raft.ResetSnapshotting()
		}()
		return
	}

	// A snapshot pushed by the leader because this node had fallen too
	// far behind for incremental replication to catch up.
	if err := impl.sm.restore(msg.SnapshotData); err != nil {
		impl.logger.Error("failed to restore state machine from installed snapshot",
			zap.Uint64("snapshot_index", msg.SnapshotIndex), zap.Error(err))
		return
	}
	// Persist the installed snapshot: InstallDone (via trimLogLocked)
	// discards every covered log entry, so without durable snapshot data
	// a restart would find a trimmed log but no matching state-machine
	// state — the state machine would boot empty while lastApplied claims
	// the compacted entries were applied.
	if err := impl.persister.SaveSnapshot(msg.SnapshotData, msg.SnapshotIndex); err != nil {
		impl.logger.Error("failed to persist installed snapshot; restart after compaction may not recover state",
			zap.Uint64("snapshot_index", msg.SnapshotIndex), zap.Error(err))
	}
	impl.raft.InstallDone(msg.SnapshotIndex, msg.SnapshotTerm)

	// The installed snapshot restored every pre-compaction version into
	// the state machine, but those entries were never applied individually
	// on this node, so the per-version onVersionCreated hook (which drives
	// the DataSync pull of storage-layer data + index build) would never
	// fire for them. Trigger it explicitly for every snapshot-covered
	// version, exactly like handleEntryMsg does for a freshly replicated
	// cmdCreateVersion. PullVersion is idempotent (re-pulls are no-ops),
	// and the callback runs asynchronously so the apply loop is not
	// blocked; Stop() waits for these goroutines via callbackWG.
	if impl.onVersionCreated != nil {
		for _, v := range impl.sm.listAllVersions() {
			impl.callbackWG.Add(1)
			go func(kbID string, versionID int64) {
				defer impl.callbackWG.Done()
				impl.onVersionCreated(kbID, versionID)
			}(v.KBID, v.VersionID)
		}
	}
}

// proposeAndWait encodes cmd, proposes it via kvraft, registers a waiter
// for the resulting index, and blocks until either the entry is applied
// (returning its applyResult) or ctx is done.
func (impl *RaftNodeImpl) proposeAndWait(ctx context.Context, cmd command) (applyResult, error) {
	data, err := encodeCommand(cmd)
	if err != nil {
		return applyResult{}, err
	}

	index, term, err := impl.raft.Propose(ctx, data)
	if err != nil {
		return applyResult{}, err
	}

	resultCh := make(chan applyResult, 1)
	impl.pendingMu.Lock()
	impl.pending[index] = &pendingProposal{term: term, resultCh: resultCh}
	impl.pendingMu.Unlock()

	select {
	case res := <-resultCh:
		return res, nil
	case <-ctx.Done():
		impl.pendingMu.Lock()
		delete(impl.pending, index)
		impl.pendingMu.Unlock()
		return applyResult{}, ctx.Err()
	}
}

func (impl *RaftNodeImpl) ProposeCreateKB(ctx context.Context, kb types.KnowledgeBaseMeta) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdCreateKB, KB: &kb})
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeMarkKBDeleting(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdMarkKBDeleting, KBID: kbID})
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdMarkKBDeleteFailed, KBID: kbID})
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeRemoveKBMeta(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdRemoveKBMeta, KBID: kbID})
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64) (int64, error) {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdCreateVersion, KBID: kbID, ParentVersionID: parentVersionID})
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		return 0, res.Err
	}
	return res.VersionID, nil
}

func (impl *RaftNodeImpl) ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdUpdateVersionStatus, VersionID: versionID, Status: status})
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeUpdateVersionSummary implements RaftNode.
func (impl *RaftNodeImpl) ProposeUpdateVersionSummary(ctx context.Context, versionID int64, docIDSetHash string) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdUpdateVersionSummary, VersionID: versionID, DocIDSetHash: docIDSetHash})
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdRollback, KBID: kbID, TargetVersionID: targetVersionID})
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeMarkVersionDeleting implements RaftNode.
func (impl *RaftNodeImpl) ProposeMarkVersionDeleting(ctx context.Context, kbID string, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdMarkVersionDeleting, KBID: kbID, VersionID: versionID})
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeRemoveVersionMeta implements RaftNode.
func (impl *RaftNodeImpl) ProposeRemoveVersionMeta(ctx context.Context, kbID string, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, command{Type: cmdRemoveVersionMeta, KBID: kbID, VersionID: versionID})
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	kb, ok := impl.sm.kbs[kbID]
	if !ok {
		return types.KnowledgeBaseMeta{}, stratumerrors.ErrKnowledgeBaseNotFound
	}
	return kb, nil
}

func (impl *RaftNodeImpl) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return nil, stratumerrors.ErrKnowledgeBaseNotFound
	}
	ids := impl.sm.versionsByKB[kbID]
	out := make([]types.VersionMeta, 0, len(ids))
	for _, id := range ids {
		out = append(out, impl.sm.versions[id])
	}
	return out, nil
}

// ListKnowledgeBases returns metadata for every knowledge base in the
// state machine. Order is not specified (map iteration).
func (impl *RaftNodeImpl) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	out := make([]types.KnowledgeBaseMeta, 0, len(impl.sm.kbs))
	for _, kb := range impl.sm.kbs {
		out = append(out, kb)
	}
	// map 遍历顺序随机，按 KBID 字典序排序，保证列表顺序稳定可预期。
	sort.Slice(out, func(i, j int) bool { return out[i].KBID < out[j].KBID })
	return out, nil
}

// GetClusterStatus reports Raft cluster connectivity, independent of any
// specific knowledge base.
//
// Note: this reads kvraft's locally-known leader/term state directly,
// without a linearizable read-index protocol — on a node whose log has
// fallen behind (a lagging follower), this can report stale information
// for a brief window after a real leader change. Acceptable for a
// connectivity probe (HealthCheck's use case); GetKB/ListVersions share
// the same characteristic for the same reason and would need a
// read-index or leader-routing mechanism to offer linearizable reads from
// any node, which is not required by anything in the current design docs
// and is left as a documented known limitation rather than implemented
// ahead of need.
func (impl *RaftNodeImpl) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	leaderID, known := impl.raft.LeaderID()
	return types.ClusterStatus{
		HasLeader:   known,
		MemberCount: impl.raft.ClusterSize(),
		LeaderID:    leaderID,
	}, nil
}

var _ RaftNode = (*RaftNodeImpl)(nil)
