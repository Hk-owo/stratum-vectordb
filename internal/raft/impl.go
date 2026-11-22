// Package raft implements Stratum's RaftNode interface (defined in
// raft.go) on top of internal/kvraft's Raft consensus library, per
// docs/已实施/Stratum_实现顺序.md 阶段 3: "依赖现有 kvserver Raft，扩展状态机支持知识库和
// 版本元数据".
//
// Architecture: internal/kvraft handles consensus (leader election, log
// replication, snapshotting) over an opaque []byte command stream; this
// package defines that command encoding (command.go), the deterministic
// state machine that interprets committed commands (sm_*.go),
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
	"sync"
	"time"

	"go.uber.org/zap"

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

// errApplyTimeout is returned to a Propose* call whose entry was appended and then
// never applied within proposeApplyTimeout. The entry may still commit later, so
// this is "unknown outcome", not "failed" — callers must be safe to retry (see the
// note in proposeAndWait).
var errApplyTimeout = errors.New("raft: proposal appended but not applied within the wait budget; its outcome is unknown")

const (
	// proposalApplyPoll is how often a waiting proposal re-checks that it can still
	// commit at all. The granularity is the one this cluster already uses to notice
	// anything (takeoverTimeout in the service layer): fast enough that a leadership
	// change unblocks a waiting caller before a request deadline does, slow enough
	// that a burst of waiters costs nothing.
	proposalApplyPoll = 200 * time.Millisecond

	// defaultProposeApplyTimeout bounds how long a Propose* call waits for its entry
	// to be applied. Generous on purpose: an apply is normally sub-millisecond, and
	// the cases that legitimately take longer (snapshot install, a slow disk behind
	// the apply loop) must not be turned into failures. What this bound exists for
	// is the case with no other exit — a leader that lost quorum and still believes
	// it leads — where waiting forever means one stuck proposal holds its caller
	// (and, through the build pool, a node's shutdown) indefinitely.
	defaultProposeApplyTimeout = 60 * time.Second
)

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

	// DeletedLogLimit bounds the control layer's deletion log — the bounded record a
	// storage node consumes incrementally (see RaftNodeImpl.DeletionsSince). Zero
	// means defaultDeletedLogLimit.
	//
	// It is a trade: a bigger log means a node can fall further behind before it has
	// to re-judge from the live chain, and every entry rides in each snapshot.
	DeletedLogLimit int

	// ProposeApplyTimeout bounds how long one Propose* call waits for its entry to
	// be applied before reporting an UNKNOWN outcome (see errApplyTimeout). Zero
	// means defaultProposeApplyTimeout. A smaller value makes a node give up sooner
	// on a proposal whose leader has lost quorum; it does not make this node stop
	// waiting for leadership changes, which is polled separately.
	ProposeApplyTimeout time.Duration

	// RPCTimeout bounds one peer-to-peer Raft RPC (RequestVote, AppendEntries,
	// the InstallSnapshot trigger). Zero means kvraft's own 1 s default. These
	// are timer-driven background calls, so the bound is what keeps one hung
	// peer from holding a replication round open.
	RPCTimeout time.Duration
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

	// deletingDirty names the knowledge bases that have had a version marked Deleting
	// since a sweep last consumed them.
	//
	// It records "ask again about this one", NOT "these versions are being deleted" —
	// that question stays with the metadata (§B's state-over-events). The distinction is
	// why a version-id WATERMARK would be wrong here: a deletion's marks can be anywhere
	// in the chain (SINGLE marks a middle version, ANCESTORS a prefix), so "everything at
	// or below V is clean" is not a fact any version id can express — a middle version
	// marked later would sit below the watermark forever. A set of knowledge bases is
	// the conservative shape: anything that was marked is in it.
	deletingDirtyMu sync.Mutex
	deletingDirty   map[string]bool

	applyCh chan kvraft.ApplyMsg

	pendingMu sync.Mutex
	pending   map[uint64]*pendingProposal

	// settled carries applied entries whose proposer had not registered a waiter
	// yet — see rememberSettledLocked. Guarded by pendingMu and bounded by
	// settledResultsLimit (apply indexes are monotonic, so the bound trims by index).
	settled     map[uint64]settledResult
	settledSeen uint64 // highest index ever settled (trim watermark)

	// nodeID is this node's own ID, needed to tell "the leader is me" from
	// "the leader is elsewhere" before forwarding.
	nodeID int64
	// forwarder carries proposals to the leader when this node is not it
	// (WithForwarder). Optional: without it a non-leader proposal fails the
	// way it always did.
	forwarder ProposeForwarder

	applyLoopDone chan struct{}
	callbackWG    sync.WaitGroup

	// proposeApplyTimeout bounds one Propose* call's wait for its entry to be
	// applied (Config.ProposeApplyTimeout, or defaultProposeApplyTimeout).
	proposeApplyTimeout time.Duration

	// The three apply/snapshot lifecycle hooks are supplied at construction via
	// RaftOption before runApplyLoop starts, making them immutable once the
	// background loop is running.
	onVersionCreated           func(kbID string, versionID int64)
	onVersionFailedPermanent   func(kbID string, versionID int64)
	onVersionCommittedAsLeader func(kbID string, versionID, parentVersionID int64, clientRequestID string)

	// versionWatchers are the local waiters registered through WatchVersion,
	// keyed by version ID. Its own mutex, not sm.mu: notifications come from the
	// apply loop while registration and cancellation come from whichever
	// goroutine is serving the client call, and the apply path must never wait on
	// a client.
	versionWatchersMu sync.Mutex
	versionWatchers   map[int64]map[chan struct{}]struct{}
}

// RaftOption configures optional collaborators and apply hooks on RaftNodeImpl
// before its background apply loop starts.
type RaftOption func(*RaftNodeImpl)

// WithNodeID overrides the node's own ID used to distinguish "leader is me"
// from "leader is elsewhere" when forwarding proposals.
func WithNodeID(id int64) RaftOption {
	return func(impl *RaftNodeImpl) { impl.nodeID = id }
}

// WithForwarder wires the path that carries a proposal to the leader when this
// node is not leading.
func WithForwarder(f ProposeForwarder) RaftOption {
	return func(impl *RaftNodeImpl) { impl.forwarder = f }
}

// WithOnVersionCreated registers the callback invoked when cmdCreateVersion is
// applied (including snapshot restores).
func WithOnVersionCreated(fn func(kbID string, versionID int64)) RaftOption {
	return func(impl *RaftNodeImpl) { impl.onVersionCreated = fn }
}

// WithOnVersionFailedPermanent registers the callback invoked when a terminal
// DATA-side failure verdict is applied.
func WithOnVersionFailedPermanent(fn func(kbID string, versionID int64)) RaftOption {
	return func(impl *RaftNodeImpl) { impl.onVersionFailedPermanent = fn }
}

// WithOnVersionCommittedAsLeader registers the §7.13.2 dispatch hook invoked
// when cmdCreateVersion applies while this node leads.
func WithOnVersionCommittedAsLeader(fn func(kbID string, versionID, parentVersionID int64, clientRequestID string)) RaftOption {
	return func(impl *RaftNodeImpl) { impl.onVersionCommittedAsLeader = fn }
}

// NewRaftNodeImpl constructs and starts a RaftNodeImpl: it starts the
// underlying kvraft node's gRPC server and background consensus loops,
// and starts this package's own apply-dispatch loop. Returns once
// everything is running (does not wait for a leader to be elected —
// callers should poll, e.g. via GetClusterStatus, if they need to know
// when this node believes there's a leader).
func NewRaftNodeImpl(cfg Config, opts ...RaftOption) (*RaftNodeImpl, error) {
	if cfg.WAL == nil {
		return nil, fmt.Errorf("raft: Config.WAL is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	transport := kvraft.NewTransportWithRPCTimeout(logger, cfg.RPCTimeout)
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

	kvOpts := []kvraft.Option{kvraft.WithLogger(logger)}
	if cfg.MaxLogLength > 0 {
		kvOpts = append(kvOpts, kvraft.WithMaxLogLength(cfg.MaxLogLength))
	}
	if cfg.HeartbeatInterval > 0 {
		kvOpts = append(kvOpts, kvraft.WithHeartbeatInterval(cfg.HeartbeatInterval))
	}
	if cfg.ElectionTimeoutMin > 0 && cfg.ElectionTimeoutMax > 0 {
		kvOpts = append(kvOpts, kvraft.WithElectionTimeoutRange(cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax))
	}

	rf, err := kvraft.NewRaft(cfg.NodeID, transport, applyCh, persister, kvOpts...)
	if err != nil {
		return nil, fmt.Errorf("raft: node %d: %w", cfg.NodeID, err)
	}

	impl := &RaftNodeImpl{
		nodeID:        cfg.NodeID,
		raft:          rf,
		wal:           cfg.WAL,
		sm:            newStateMachineWithDeletedLogLimit(cfg.DeletedLogLimit),
		logger:        logger,
		persister:     persister,
		applyCh:       applyCh,
		pending:       make(map[uint64]*pendingProposal),
		settled:       make(map[uint64]settledResult),
		applyLoopDone: make(chan struct{}),

		proposeApplyTimeout: cfg.ProposeApplyTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(impl)
		}
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
	// Nobody is left to settle a waiter: the apply loop has exited, so an entry
	// that has not been applied by now never will be on this node. A caller whose
	// context has no deadline — every build-complete callback is one, it reports
	// with context.Background() — would otherwise block for the rest of the
	// process's life, which is exactly how "stop a node" turned into a hang that
	// only the test binary's timeout ended (integration
	// TestRealStack_ThreeNodeCluster_FaultTolerance parked for 9 minutes in
	// index.IndexManagerImpl.Close → buildPool.Close → this waiter).
	impl.failAllPending(kvraft.ErrStopped)
	impl.callbackWG.Wait()
}

// failAllPending settles every waiter as failed and empties the table. Used by
// Stop: after the apply loop is gone, waiting is no longer a thing that can
// succeed, and telling the callers is the difference between a shutdown and a
// deadlock.
func (impl *RaftNodeImpl) failAllPending(err error) {
	impl.pendingMu.Lock()
	pending := impl.pending
	impl.pending = make(map[uint64]*pendingProposal)
	impl.pendingMu.Unlock()

	for _, p := range pending {
		// resultCh is buffered (size 1), so this never blocks even if the waiter
		// gave up on its own (ctx cancellation, apply budget) a moment ago.
		p.resultCh <- applyResult{Err: err}
	}
}

// IsLeader reports whether this node currently believes it leads the cluster.
// Read at apply time by the §7.13.2 dispatch hook: the answer that matters is
// the one at the moment the entry is applied, not when it was proposed.
func (impl *RaftNodeImpl) IsLeader() bool {
	return impl.raft.IsLeader()
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

// WatchVersion returns a channel signalled whenever versionID's replicated
// metadata changes, plus the function that stops watching. stop is idempotent,
// and the caller MUST call it: a registration outlives the wait otherwise.
//
// Keyed by version ID alone. Versions live in one state-machine map allocated
// from one counter, so the id is unique across the cluster; "unique within a
// knowledge base" is the contract clients may rely on, not the limit of the data
// structure.
//
// The channel is signal-only (one pending token per version, coalesced): a waiter
// re-reads the state machine after every signal, so a coalesced duplicate costs
// nothing and no change is missed as long as the waiter registered BEFORE the
// read it is waiting on.
func (impl *RaftNodeImpl) WatchVersion(versionID int64) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)

	impl.versionWatchersMu.Lock()
	if impl.versionWatchers == nil {
		impl.versionWatchers = make(map[int64]map[chan struct{}]struct{})
	}
	waiters, ok := impl.versionWatchers[versionID]
	if !ok {
		waiters = make(map[chan struct{}]struct{})
		impl.versionWatchers[versionID] = waiters
	}
	waiters[ch] = struct{}{}
	impl.versionWatchersMu.Unlock()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			impl.versionWatchersMu.Lock()
			defer impl.versionWatchersMu.Unlock()
			waiters, ok := impl.versionWatchers[versionID]
			if !ok {
				return
			}
			delete(waiters, ch)
			if len(waiters) == 0 {
				delete(impl.versionWatchers, versionID)
			}
		})
	}
	return ch, stop
}

// notifyVersionChanged wakes every local waiter for versionID.
//
// It runs on the apply loop, so it must not block under any circumstance: a
// waiter that has not drained its previous signal re-reads the state machine
// anyway and needs nothing more from this call.
func (impl *RaftNodeImpl) notifyVersionChanged(versionID int64) {
	if versionID == 0 {
		return
	}

	impl.versionWatchersMu.Lock()
	waiters := impl.versionWatchers[versionID]
	impl.versionWatchersMu.Unlock()

	for ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
			// Already holding a signal: coalescing is the contract.
		}
	}
}

// versionIDOf is the version an applied command is about: the command's own
// VersionID, or the one cmdCreateVersion just allocated. Zero means "no single
// version" — a knowledge-base command, or a command that failed to decode.
func versionIDOf(cmd command, result applyResult) int64 {
	if cmd.Type == cmdCreateVersion {
		return result.VersionID
	}
	return cmd.VersionID
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

	// Wake local awaiters: this version's replicated state just moved
	// (docs/已实施/await-version-plan.md §12 item 1). Cheap and unconditional — a map
	// lookup plus a non-blocking send — and it fires for every applied command
	// that names a version, which is exactly the set that can change what a
	// waiter is looking at.
	impl.notifyVersionChanged(versionIDOf(cmd, result))

	// A version marked Deleting is the one state a delete can reach and never leave (see
	// DeletingVersionSweeper), so remember the knowledge base and let the next sweep ask
	// about it. Only on a SUCCESSFUL apply: a rejected mark (the version is active, or
	// PENDING) left nothing to resume.
	if result.Err == nil && cmd.Type == cmdMarkVersionDeleting {
		impl.MarkDeletingDirty(cmd.KBID)
	}

	impl.pendingMu.Lock()
	waiter, ok := impl.pending[msg.Index]
	if ok {
		delete(impl.pending, msg.Index)
	} else {
		// Nobody is waiting *yet*, and that is normal rather than suspicious:
		// Propose returns an index before its caller can register a waiter for it,
		// so the entry can be applied first. Dropping the result here is what made
		// such a proposal wait for its context (see rememberSettledLocked).
		impl.rememberSettledLocked(msg.Index, msg.Term, result)
	}
	impl.pendingMu.Unlock()

	// Notify the data-sync layer about every applied CreateVersion — including
	// the node that proposed it (§8.5).
	//
	// This used to be inside the "!ok" branch below, i.e. it skipped the
	// proposer, because the proposer was assumed to be the one that wrote the
	// data. That assumption only holds while the coordinator is the leader. Once
	// any node may coordinate (§8.5), the proposer and the data's holder come
	// apart in two ways: a forwarded proposal has a waiter on *both* the
	// originating node and the leader (so the leader — which holds no data —
	// would be skipped), and a non-leader coordinator holds the data without
	// anyone else knowing. Who needs to pull is a data-plane question, not a
	// Raft one: the plane already skips the pull when its own cursor covers the
	// version, so announcing every apply is both correct and cheap.
	// result.Err as well as err: err only says the command decoded, and a command the
	// state machine REFUSED changed nothing, so nobody may act as if it had.
	if err == nil && result.Err == nil && cmd.Type == cmdCreateVersion {
		// Run asynchronously: pulling storage-layer data can block
		// (dial + stream + retries), and a blocked apply loop would
		// stall every subsequent committed entry — most visible when
		// a follower replays its log after a restart.
		if fn := impl.onVersionCreated; fn != nil {
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				fn(cmd.KBID, result.VersionID)
			}()
		}
	}

	if err == nil && result.Err == nil && cmd.Type == cmdMarkVersionFailedPermanent &&
		cmd.FailureSide == types.FailureSideData {
		if fn := impl.onVersionFailedPermanent; fn != nil {
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				fn(cmd.KBID, cmd.VersionID)
			}()
		}
	}

	if err == nil && result.Err == nil && cmd.Type == cmdCreateVersion && impl.IsLeader() {
		if fn := impl.onVersionCommittedAsLeader; fn != nil {
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				fn(cmd.KBID, result.VersionID, cmd.ParentVersionID, cmd.ClientRequestID)
			}()
		}
	}

	if !ok {
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

	// Persist the installed snapshot BEFORE anything below depends on it: InstallDone
	// (via trimLogLocked) discards every covered log entry, and sm.restore replaces
	// the state machine, so without durable snapshot data a restart would find a
	// trimmed log with no matching state — the state machine boots empty while
	// lastApplied claims the compacted entries were applied, and the metadata they
	// carried is gone from both places (H5 of docs/参考/code-review-2026-10-18.md).
	// Failing here leaves the node untouched; the leader will send the snapshot
	// again.
	if err := impl.persister.SaveSnapshot(msg.SnapshotData, msg.SnapshotIndex); err != nil {
		impl.logger.Error("failed to persist installed snapshot; refusing to install it",
			zap.Uint64("snapshot_index", msg.SnapshotIndex), zap.Error(err))
		return
	}
	if err := impl.sm.restore(msg.SnapshotData); err != nil {
		impl.logger.Error("failed to restore state machine from installed snapshot",
			zap.Uint64("snapshot_index", msg.SnapshotIndex), zap.Error(err))
		return
	}
	// The versions a snapshot carries were never applied one at a time on this node, so a
	// deletion mark among them was never announced here either. Treat every knowledge base
	// as dirty: the next sweep asks about all of them, which is what it did before the
	// dirty set existed.
	impl.MarkAllDeletingDirty()
	impl.raft.InstallDone(msg.SnapshotIndex, msg.SnapshotTerm)

	// The installed snapshot restored every pre-compaction version into
	// the state machine, but those entries were never applied individually
	// on this node, so the per-version onVersionCreated hook (which drives
	// the DataSync pull of storage-layer data + index build) would never
	// fire for them. Trigger it explicitly for every snapshot-covered
	// version, exactly like handleEntryMsg does for a freshly replicated
	// cmdCreateVersion. PullVersionData is idempotent (re-pulls are no-ops),
	// and the callback runs asynchronously so the apply loop is not
	// blocked; Stop() waits for these goroutines via callbackWG.
	if fn := impl.onVersionCreated; fn != nil {
		for _, v := range impl.sm.listAllVersions() {
			impl.callbackWG.Add(1)
			go func(kbID string, versionID int64) {
				defer impl.callbackWG.Done()
				fn(kbID, versionID)
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
		// Raft appends on the leader only, but any node may have a fact worth
		// reporting. The access layer carries it to whoever can append — the
		// Raft core stays unaware of the network (docs/参考/历史/Stratum_设计文档v13.md §7.3).
		if errors.Is(err, kvraft.ErrNotLeader) && impl.forwarder != nil {
			if leaderID, known := impl.raft.LeaderID(); known && leaderID != impl.nodeID {
				forwarded, ferr := impl.forwarder.ForwardPropose(ctx, leaderID, data)
				if ferr != nil {
					return applyResult{}, ferr
				}
				return applyResult{
					VersionID:         forwarded.VersionID,
					DeletedVersionIDs: forwarded.DeletedVersionIDs,
					Err:               forwarded.Err,
				}, nil
			}
		}
		return applyResult{}, err
	}

	resultCh := make(chan applyResult, 1)
	impl.pendingMu.Lock()
	// A result may already be waiting here: the entry can be applied before this
	// goroutine gets as far as registering (see settleApply). Without this check the
	// apply side would have found no waiter, dropped the result, and this call would
	// block until ctx — for a caller with no deadline, forever.
	if s, done := impl.takeSettledLocked(index); done {
		impl.pendingMu.Unlock()
		if s.term != term {
			// Same verdict a registered waiter gets in handleEntryMsg: the entry
			// committed at this index is a different leader's, not the one proposed.
			return applyResult{Err: errSuperseded}, nil
		}
		return s.res, nil
	}
	impl.pending[index] = &pendingProposal{term: term, resultCh: resultCh}
	impl.pendingMu.Unlock()

	// The wait below is bounded twice, and both bounds exist for the same reason:
	// waiting for an apply that cannot happen must not outlive the reason to wait.
	//
	//   - a poll that gives up as soon as THIS node stops being the leader at the
	//     term it proposed in. A stale leader — one that has not yet heard from the
	//     new one — can append entries forever and never commit any of them, and
	//     nothing else in this call would ever notice. This is the common case and
	//     it resolves in one poll interval;
	//   - an absolute budget for the rest: a leader that still believes it leads
	//     but has lost quorum keeps appending without committing, and no signal
	//     available here distinguishes it from a slow apply.
	//
	// Giving up is reported as "the outcome is unknown", never as "it failed": the
	// entry may still commit later. Every caller in this package is safe on that
	// reading because the writes carry their own idempotency (a CreateVersion under
	// the same client_request_id returns the same version; a status report is a
	// set, not an increment).
	poll := time.NewTicker(proposalApplyPoll)
	defer poll.Stop()
	budget := impl.proposeApplyTimeout
	if budget <= 0 {
		budget = defaultProposeApplyTimeout
	}
	deadline := time.NewTimer(budget)
	defer deadline.Stop()

	for {
		select {
		case res := <-resultCh:
			return res, nil
		case <-ctx.Done():
			impl.forgetPending(index)
			return applyResult{}, ctx.Err()
		case <-impl.applyLoopDone:
			// The apply loop is gone, so nothing can settle this waiter any more.
			// Checked here and not only in Stop because both shutdown orders are
			// real: a node may stop kvraft before this layer, and Stop may race a
			// proposal that is already in the table.
			impl.forgetPending(index)
			return applyResult{Err: kvraft.ErrStopped}, nil
		case <-poll.C:
			if termNow := impl.raft.Term(); termNow != term || !impl.raft.IsLeader() {
				impl.logger.Debug("proposal abandoned: this node is no longer the leader at the term it proposed in",
					zap.Uint64("index", index),
					zap.Uint64("proposed_term", term),
					zap.Uint64("current_term", termNow))
				impl.forgetPending(index)
				return applyResult{Err: errSuperseded}, nil
			}
		case <-deadline.C:
			impl.logger.Warn("proposal abandoned: appended but not applied within the budget; its outcome is unknown",
				zap.Uint64("index", index),
				zap.Uint64("term", term),
				zap.Duration("budget", budget))
			impl.forgetPending(index)
			return applyResult{}, errApplyTimeout
		}
	}
}

// forgetPending removes one waiter from the table. Safe when it is already gone
// (the apply side or Stop settled it first).
func (impl *RaftNodeImpl) forgetPending(index uint64) {
	impl.pendingMu.Lock()
	delete(impl.pending, index)
	impl.pendingMu.Unlock()
}

// settledResult is an applied entry's outcome kept for a proposer that has not
// registered a waiter yet (see rememberSettledLocked). The term travels with it so
// that registration can apply the same "this index holds another leader's entry"
// check a registered waiter gets in handleEntryMsg.
type settledResult struct {
	term uint64
	res  applyResult
}

// settledResultsLimit bounds the settled table. Apply indexes are monotonic, so the
// table can always be trimmed from the bottom.
const settledResultsLimit = 1024

// rememberSettledLocked keeps an applied entry for a proposer that is not waiting
// yet. The caller must hold pendingMu.
//
// Why this exists: a proposal's index is known only after Propose returns, and the
// entry can be applied before the proposer gets back to register its waiter — on a
// loaded machine that window is wide enough to hit (a 200 ms artificial delay in
// front of the registration reproduces it every time). Dropping the result there
// makes the proposal wait for its context instead: it reports a timeout for an
// entry that WAS committed and applied, i.e. the state changed and the caller was
// told it did not. On CI this surfaced as internal/raft hitting the package's 180 s
// timeout, because the suite proposes with context.Background().
//
// Also covers the snapshot window: kvraft snapshots at lastApplied, so an index a
// snapshot just covered was applied before it.
func (impl *RaftNodeImpl) rememberSettledLocked(index, term uint64, res applyResult) {
	impl.settled[index] = settledResult{term: term, res: res}
	if index > impl.settledSeen {
		impl.settledSeen = index
	}
	if len(impl.settled) > settledResultsLimit {
		cutoff := impl.settledSeen - settledResultsLimit
		for i := range impl.settled {
			if i <= cutoff {
				delete(impl.settled, i)
			}
		}
	}
}

// takeSettledLocked consumes a settled entry. The caller must hold pendingMu.
func (impl *RaftNodeImpl) takeSettledLocked(index uint64) (settledResult, bool) {
	s, ok := impl.settled[index]
	if ok {
		delete(impl.settled, index)
	}
	return s, ok
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

var (
	_ ClusterReader = (*RaftNodeImpl)(nil)
	_ MetaReader    = (*RaftNodeImpl)(nil)
	_ KBWriter      = (*RaftNodeImpl)(nil)
	_ VersionWriter = (*RaftNodeImpl)(nil)
	_ RaftNode      = (*RaftNodeImpl)(nil)
)
