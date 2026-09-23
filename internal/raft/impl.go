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
	"sync/atomic"
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

	// settled carries applied entries whose proposer had not registered a waiter
	// yet — see rememberSettledLocked. Guarded by pendingMu and bounded by
	// settledResultsLimit (apply indexes are monotonic, so the bound trims by index).
	settled     map[uint64]settledResult
	settledSeen uint64 // highest index ever settled (trim watermark)

	// nodeID is this node's own ID, needed to tell "the leader is me" from
	// "the leader is elsewhere" before forwarding.
	nodeID int64
	// forwarder carries proposals to the leader when this node is not it
	// (SetForwarder). Optional: without it a non-leader proposal fails the
	// way it always did.
	forwarder ProposeForwarder

	applyLoopDone chan struct{}
	callbackWG    sync.WaitGroup

	// onVersionCreated is an optional callback invoked after a cmdCreateVersion
	// is applied on this node — on EVERY applier, the proposer included. It is a
	// data-plane notification ("this version now exists; make sure you hold it"),
	// so each node has to react for itself, and the plane skips the pull when its
	// own cursor already covers the version. Set by the startup wiring (main.go)
	// to trigger data sync (internal/sync.FollowerSync.PullVersion).
	//
	// The three hooks here are read by the apply loop and written by that
	// startup wiring, which runs AFTER NewRaftNodeImpl has already started the
	// loop — and on a restart the loop is replaying the log at that moment. So
	// they are held atomically rather than as plain fields: a plain field was a
	// real data race (the detector caught SetOnVersionCreated against
	// handleEntryMsg), not a theoretical one.
	onVersionCreated atomic.Pointer[func(kbID string, versionID int64)]

	// onVersionFailedPermanent is an optional callback invoked after a
	// cmdMarkVersionFailedPermanent is applied on this node — on EVERY applier, and
	// for a different reason than onVersionCreated: §10.6's reclaim is a broadcast to
	// the candidate replicas and is best-effort, so a replica that is partitioned (or
	// restarting) when it fires never hears it, and its data stays behind with nothing
	// left to notice it. Every node applies the same Raft entry, so the apply path is
	// how a verdict reaches the nodes that have to act on it. The reclaim itself is
	// idempotent — a node that never held the version runs a prefix delete over
	// nothing — and it runs off the apply loop, because it broadcasts.
	onVersionFailedPermanent atomic.Pointer[func(kbID string, versionID int64)]

	// onVersionCommittedAsLeader is an optional callback invoked after a
	// cmdCreateVersion is applied on a node that, AT APPLY TIME, believes it is
	// the leader. Unlike onVersionCreated it must fire on exactly one node: it
	// triggers external side effects — choosing the write's coordinator and
	// having that node run the write (§7.13.2) — which would otherwise be
	// attempted once per replica. The leadership check happens in the apply loop
	// rather than at propose time, because a forwarded proposal is applied by
	// whoever leads then, and a since-deposed leader must not dispatch.
	onVersionCommittedAsLeader atomic.Pointer[func(kbID string, versionID, parentVersionID int64, clientRequestID string)]

	// versionWatchers are the local waiters registered through WatchVersion,
	// keyed by version ID. Its own mutex, not sm.mu: notifications come from the
	// apply loop while registration and cancellation come from whichever
	// goroutine is serving the client call, and the apply path must never wait on
	// a client.
	versionWatchersMu sync.Mutex
	versionWatchers   map[int64]map[chan struct{}]struct{}
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
		settled:       make(map[uint64]settledResult),
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
// hook. Safe to call at any time, including after the apply loop has
// started — see the field comment for why that matters.
func (impl *RaftNodeImpl) SetOnVersionCreated(fn func(kbID string, versionID int64)) {
	impl.onVersionCreated.Store(&fn)
}

// SetOnVersionFailedPermanent registers the callback invoked when a version's
// terminal DATA-side verdict is applied on this node — every applier, not only the
// one that reported the failure. Safe to call at any time, including after the
// apply loop has started.
func (impl *RaftNodeImpl) SetOnVersionFailedPermanent(fn func(kbID string, versionID int64)) {
	impl.onVersionFailedPermanent.Store(&fn)
}

// SetOnVersionCommittedAsLeader registers the §7.13.2 dispatch hook: invoked
// when a cmdCreateVersion is applied on a node that believes it leads at that
// moment. Safe to call at any time, including after the apply loop has started.
func (impl *RaftNodeImpl) SetOnVersionCommittedAsLeader(fn func(kbID string, versionID, parentVersionID int64, clientRequestID string)) {
	impl.onVersionCommittedAsLeader.Store(&fn)
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
	// (docs/await-version-plan.md §12 item 1). Cheap and unconditional — a map
	// lookup plus a non-blocking send — and it fires for every applied command
	// that names a version, which is exactly the set that can change what a
	// waiter is looking at.
	impl.notifyVersionChanged(versionIDOf(cmd, result))

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
	if err == nil && cmd.Type == cmdCreateVersion {
		// Run asynchronously: pulling storage-layer data can block
		// (dial + stream + retries), and a blocked apply loop would
		// stall every subsequent committed entry — most visible when
		// a follower replays its log after a restart.
		if fn := impl.onVersionCreated.Load(); fn != nil {
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				(*fn)(cmd.KBID, result.VersionID)
			}()
		}
	}

	if err == nil && cmd.Type == cmdMarkVersionFailedPermanent &&
		cmd.FailureSide == types.FailureSideData {
		// Only the DATA side reaches this hook. Its consumers reclaim the version's
		// PHYSICAL data, and an index-side verdict says a BUILD failed, not that the
		// data is gone — a version with durable data and a dead index is exactly the
		// state §10.1b keeps apart, and reclaiming storage on its behalf would delete
		// good records that ForceRetryVersion may yet need. This is the same filter
		// ReclaimTerminalVersions applies to its startup sweep; leaving it out here
		// made the apply path the one place that ignored the side.
		//
		// Asynchronous for the same reason as the notification above: reclaiming a
		// version's data can block (a local prefix delete, and on a control node a
		// broadcast), and a blocked apply loop would stall every later committed entry.
		if fn := impl.onVersionFailedPermanent.Load(); fn != nil {
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				(*fn)(cmd.KBID, cmd.VersionID)
			}()
		}
	}

	// The §7.13.2 dispatch hook, unlike the notification above, must fire on
	// exactly one node: it triggers external side effects (picking the write's
	// coordinator and having it run the write), which must not be attempted once
	// per replica. "Am I the leader" is asked HERE, at apply time — not when the
	// entry was proposed — because a forwarded proposal is applied by whoever
	// leads then, and a since-deposed leader must not dispatch. The narrow window
	// where two nodes both believe they lead is accepted: writes are
	// content-addressed and idempotent (§1.4), and the dispatcher's take-once
	// registration (§7.13.2) makes a repeated dispatch a no-op.
	if err == nil && cmd.Type == cmdCreateVersion && impl.IsLeader() {
		if fn := impl.onVersionCommittedAsLeader.Load(); fn != nil {
			impl.callbackWG.Add(1)
			go func() {
				defer impl.callbackWG.Done()
				(*fn)(cmd.KBID, result.VersionID, cmd.ParentVersionID, cmd.ClientRequestID)
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
	if fn := impl.onVersionCreated.Load(); fn != nil {
		for _, v := range impl.sm.listAllVersions() {
			impl.callbackWG.Add(1)
			go func(kbID string, versionID int64) {
				defer impl.callbackWG.Done()
				(*fn)(kbID, versionID)
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
		// Raft core stays unaware of the network (Stratum_设计文档v13.md §7.3).
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

func (impl *RaftNodeImpl) ProposeCreateKB(ctx context.Context, kb types.KnowledgeBaseMeta) error {
	res, err := impl.proposeAndWait(ctx, newCreateKBCommand(kb))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeMarkKBDeleting(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, newMarkKBDeletingCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, newMarkKBDeleteFailedCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeRemoveKBMeta(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, newRemoveKBMetaCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64, opts ...ProposeOption) (int64, error) {
	o := resolveProposeOptions(opts)
	res, err := impl.proposeAndWait(ctx, newCreateVersionCommand(kbID, parentVersionID, o.clientRequestID))
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		return 0, res.Err
	}
	return res.VersionID, nil
}

// SetNodeID records this node's own ID, needed to tell "the leader is me" from
// "the leader is elsewhere" before forwarding a proposal.
func (impl *RaftNodeImpl) SetNodeID(id int64) {
	impl.nodeID = id
}

// SetForwarder wires the path that carries a proposal to the leader when this
// node is not it (Stratum_设计文档v13.md §7.3). Without it, a non-leader
// proposal fails exactly as before.
func (impl *RaftNodeImpl) SetForwarder(f ProposeForwarder) {
	impl.forwarder = f
}

// ProposeMarkVersionFailedPermanent implements RaftNode: the terminal verdict
// for one side of a version whose Saga has spent its retry budget
// (Stratum_设计文档v13.md §10.1, §10.1b).
func (impl *RaftNodeImpl) ProposeMarkVersionFailedPermanent(ctx context.Context, kbID string, versionID int64, side types.FailureSide, reason string, count int32) error {
	res, err := impl.proposeAndWait(ctx, newMarkVersionFailedPermanentCommand(kbID, versionID, side, reason, count))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeRetryVersion implements RaftNode: an operator's revocation of one side's
// terminal verdict (Stratum_设计文档v13.md §10.1). The narrow rules (index side only,
// terminal side only, cause chain dropped only once nothing is terminal any more)
// live in the state machine's apply, where every replica reaches the same verdict,
// rather than here: a proposer that decided them would let two callers disagree.
func (impl *RaftNodeImpl) ProposeRetryVersion(ctx context.Context, kbID string, versionID int64, side types.FailureSide) error {
	res, err := impl.proposeAndWait(ctx, newRetryVersionCommand(kbID, versionID, side))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus, nodeID int64) error {
	res, err := impl.proposeAndWait(ctx, newUpdateVersionStatusCommand(versionID, status, nodeID))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeUpdateVersionSummary implements RaftNode.
func (impl *RaftNodeImpl) ProposeUpdateVersionSummary(ctx context.Context, versionID int64, docIDSetHash string) error {
	res, err := impl.proposeAndWait(ctx, newUpdateVersionSummaryCommand(versionID, docIDSetHash))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeMarkVersionDataDurable implements RaftNode: the control layer's own
// promotion of a version's data side (§10.1b).
func (impl *RaftNodeImpl) ProposeMarkVersionDataDurable(ctx context.Context, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, newMarkDataDurableCommand(versionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error {
	res, err := impl.proposeAndWait(ctx, newRollbackCommand(kbID, targetVersionID))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeMarkVersionDeleting implements RaftNode.
func (impl *RaftNodeImpl) ProposeMarkVersionDeleting(ctx context.Context, kbID string, versionID int64, mode types.VersionDeleteMode) ([]int64, error) {
	res, err := impl.proposeAndWait(ctx, newMarkVersionDeletingCommand(kbID, versionID, mode))
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return nil, res.Err
	}
	return res.DeletedVersionIDs, nil
}

// ProposeRemoveVersionMeta implements RaftNode.
func (impl *RaftNodeImpl) ProposeRemoveVersionMeta(ctx context.Context, kbID string, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, newRemoveVersionMetaCommand(kbID, versionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeDiscardVersion(ctx context.Context, kbID string, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, newDiscardVersionCommand(kbID, versionID))
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

// ListVersionsInRange implements RaftNode: the same set, narrowed.
//
// A linear walk with two comparisons per version, and that is deliberate — the cost
// worth saving here is the WIRE (89 B per version), not this. The version set is a
// slice in allocation order that deletions splice, so a binary search would be buying
// microseconds with a new invariant to keep.
func (impl *RaftNodeImpl) ListVersionsInRange(ctx context.Context, kbID string, fromExclusive, toInclusive *int64) ([]types.VersionMeta, error) {
	all, err := impl.ListVersions(ctx, kbID)
	if err != nil {
		return nil, err
	}
	out := make([]types.VersionMeta, 0, len(all))
	for _, v := range all {
		if fromExclusive != nil && v.VersionID <= *fromExclusive {
			continue
		}
		if toInclusive != nil && v.VersionID > *toInclusive {
			continue
		}
		out = append(out, v)
	}
	return out, nil
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

// GetVersion returns one version's metadata, read straight out of the state
// machine under the same RLock ListVersions takes. It is the O(1) sibling the
// await path polls (docs/await-version-plan.md §7 Step 2).
func (impl *RaftNodeImpl) GetVersion(_ context.Context, kbID string, versionID int64) (types.VersionMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return types.VersionMeta{}, stratumerrors.ErrKnowledgeBaseNotFound
	}
	v, ok := impl.sm.versions[versionID]
	if !ok || v.KBID != kbID {
		// One answer for "no such version" and for "that version belongs to a
		// different knowledge base": the caller asked about (kbID, versionID)
		// and from its side both mean "not in this KB".
		return types.VersionMeta{}, stratumerrors.ErrVersionNotFound
	}
	return v, nil
}

// LastVersionID implements RaftNode: O(1), because the version set is kept in
// allocation order (see the interface comment).
func (impl *RaftNodeImpl) LastVersionID(_ context.Context, kbID string) (int64, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return 0, stratumerrors.ErrKnowledgeBaseNotFound
	}
	ids := impl.sm.versionsByKB[kbID]
	if len(ids) == 0 {
		return 0, nil
	}
	// Append-ordered, and deletions splice the slice in place, so the last element
	// IS the highest surviving id — no scan, and no second invariant to keep in
	// step with anything else.
	return ids[len(ids)-1], nil
}

// VersionLiveness answers "which of these versions are still alive, and how far has
// allocation got" for one knowledge base.
//
// Not on the RaftNode interface: its consumers are the readers that judge leftovers,
// and widening the interface would oblige
// every shape (including a storage node that has no state machine) to answer a question
// only a state machine can.
//
// Both facts come from ONE lock hold, because they must agree: "id <= lastAllocated" is
// what proves this view has already applied that id's allocation, so only then does "and
// it is not alive" mean removed rather than "not seen yet".
func (impl *RaftNodeImpl) VersionLiveness(_ context.Context, kbID string, fromExclusive, toInclusive *int64) ([]int64, int64, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return nil, 0, stratumerrors.ErrKnowledgeBaseNotFound
	}
	// versionsByKB is in allocation order, so the range can be walked once and cut short.
	ids := impl.sm.versionsByKB[kbID]
	alive := make([]int64, 0, len(ids))
	for _, id := range ids {
		if fromExclusive != nil && id <= *fromExclusive {
			continue
		}
		if toInclusive != nil && id > *toInclusive {
			break
		}
		alive = append(alive, id)
	}
	return alive, impl.sm.nextVersionID - 1, nil
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
