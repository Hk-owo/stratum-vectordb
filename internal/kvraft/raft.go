// Package kvraft is a Raft consensus implementation: leader election, log
// replication, and snapshotting, exposed as a reusable library independent
// of any particular state machine. The Stratum-specific state machine
// (knowledge base and version metadata) is built on top of this package in
// internal/raft (RaftNodeImpl), per Stratum_实现顺序.md 阶段 3.
//
// Originally adapted from the KVServer project's internal/raft package
// (an MIT 6.5840-style teaching implementation that pairs this consensus
// layer with a simple string key-value state machine via
// internal/protocol.Command). Stratum reuses only the consensus layer —
// Propose/ApplyMsg below are deliberately state-machine-agnostic (Command
// is an opaque []byte), so the KV-specific protocol/dispatch layer was not
// brought over; Stratum defines its own command encoding and apply
// dispatch in internal/raft.
//
// Several correctness and robustness fixes were made relative to the
// original during this adaptation:
//   - Single-node clusters could never elect a leader or advance their
//     commit index (the majority-vote tally and commit-index advancement
//     were only evaluated from inside peer-response handlers, which never
//     fire when there are zero peers). Fixed: a candidate's self-vote is
//     checked against the majority threshold immediately, and Propose
//     attempts to advance the commit index synchronously right after
//     appending, both independent of receiving any peer responses.
//   - AppendEntries' heartbeat path (empty Entries) skipped the
//     PrevLogIndex/PrevLogTerm consistency check that the entries-bearing
//     path performs, which could let a follower advance its commit index
//     past a point where its own log has actually diverged from the
//     leader's. Fixed: heartbeats now go through the same consistency
//     check.
//   - InstallSnapshot sent on the (potentially blocking, potentially
//     unbuffered) apply channel while holding the node's main mutex,
//     risking a deadlock against any other goroutine that needs that
//     mutex while the channel send is blocked. Fixed: the lock is
//     released before sending.
//   - No graceful shutdown synchronization existed (Kill only set a flag;
//     background goroutines exited via polling with no way for the
//     caller to know they'd actually stopped). Fixed: Stop waits for all
//     background goroutines via a sync.WaitGroup and closes a Done()
//     channel only after they've exited.
//   - No way to query leader/term/cluster-size state externally. Fixed:
//     added IsLeader, Term, LeaderID, NodeID, ClusterSize.
//   - Propose did not accept a context and returned only an error, not
//     the assigned log index — callers had no way to correlate a Propose
//     call with the ApplyMsg it eventually produces. Fixed: Propose now
//     accepts ctx (checked for cancellation up front) and returns the
//     assigned (index, term).
package kvraft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	kvraftpb "stratum/api/proto/kvraft"
	"stratum/internal/kvstorage"
)

// Raft node states.
const (
	Leader = iota
	Candidate
	Follower
)

const (
	defaultMaxLogLength       = 1000
	defaultHeartbeatInterval  = 100 * time.Millisecond
	defaultElectionTimeoutMin = 150 * time.Millisecond
	defaultElectionTimeoutMax = 300 * time.Millisecond
)

// Raft is a single node's Raft consensus state machine.
type Raft struct {
	kvraftpb.UnimplementedRaftServer
	grpcServer *grpc.Server

	logger *zap.Logger

	mu        sync.Mutex
	me        int64
	transport *Transport
	state     int
	dead      int32

	persister       *kvstorage.Persister
	log             []*kvraftpb.Entry
	voteFor         int64
	term            uint64
	leaderID        int64 // -1 if unknown
	snapshotting    bool
	snapshotPending map[int64]bool

	lastHeartbeat      time.Time
	electionTimeout    time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	nextIndex  map[int64]uint64
	matchIndex map[int64]uint64

	lastCommitIndex uint64
	lastApplied     uint64

	maxLogLength uint64
	applyCh      chan ApplyMsg

	wg       sync.WaitGroup
	done     chan struct{}
	doneOnce sync.Once
}

// Option configures optional NewRaft parameters.
type Option func(*Raft)

// WithLogger sets the structured logger used for Raft's internal
// lifecycle events (leader election, term changes, snapshot triggers).
// Defaults to a no-op logger if not supplied.
func WithLogger(l *zap.Logger) Option {
	return func(rf *Raft) { rf.logger = l }
}

// WithMaxLogLength sets how many log entries accumulate before Raft asks
// the state-machine layer (via an ApplyMsg{IsSnapshot: true,
// SnapshotData: nil} message) to take a snapshot. Defaults to 1000; tests
// that want to exercise snapshotting without proposing 1000 entries
// should set this much lower.
func WithMaxLogLength(n uint64) Option {
	return func(rf *Raft) { rf.maxLogLength = n }
}

// WithHeartbeatInterval sets how often a leader sends heartbeats to its
// peers. Defaults to 100ms.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(rf *Raft) { rf.heartbeatInterval = d }
}

// WithElectionTimeoutRange sets the [min, max) range Raft randomizes its
// election timeout within (re-randomized on every reset, to minimize
// repeated split votes across retries). Defaults to [150ms, 300ms).
func WithElectionTimeoutRange(min, max time.Duration) Option {
	return func(rf *Raft) {
		rf.electionTimeoutMin = min
		rf.electionTimeoutMax = max
	}
}

// NewRaft constructs a Raft node with the given ID, communicating with
// peers via transport, delivering committed entries (and snapshot
// requests) on applyCh, and persisting hard state via persister. Any
// prior persisted state and snapshot are loaded synchronously before
// NewRaft returns. The node does not start participating in the cluster
// (no elections, no heartbeats) until Run is called.
func NewRaft(id int64, transport *Transport, applyCh chan ApplyMsg, persister *kvstorage.Persister, opts ...Option) *Raft {
	rf := &Raft{
		me:                 id,
		transport:          transport,
		applyCh:            applyCh,
		persister:          persister,
		state:              Follower,
		voteFor:            -1,
		leaderID:           -1,
		lastHeartbeat:      time.Now(),
		maxLogLength:       defaultMaxLogLength,
		heartbeatInterval:  defaultHeartbeatInterval,
		electionTimeoutMin: defaultElectionTimeoutMin,
		electionTimeoutMax: defaultElectionTimeoutMax,
		log:                []*kvraftpb.Entry{{Index: 0, Term: 0}},
		nextIndex:          make(map[int64]uint64),
		matchIndex:         make(map[int64]uint64),
		snapshotPending:    make(map[int64]bool),
		done:               make(chan struct{}),
	}
	for _, opt := range opts {
		opt(rf)
	}
	if rf.logger == nil {
		rf.logger = zap.NewNop()
	}
	rf.electionTimeout = randomElectionTimeout(rf.electionTimeoutMin, rf.electionTimeoutMax)

	if term, voteFor, log, err := persister.LoadState(); err != nil {
		rf.logger.Error("failed to load persisted Raft state; starting fresh", zap.Int64("node_id", id), zap.Error(err))
	} else if term > 0 || len(log) > 0 {
		rf.term = term
		rf.voteFor = voteFor
		if len(log) > 0 {
			rf.log = log
		}
	}

	if data, lastIndex, err := persister.LoadSnapshot(); err != nil {
		rf.logger.Error("failed to load persisted snapshot", zap.Int64("node_id", id), zap.Error(err))
	} else if data != nil {
		rf.lastApplied = lastIndex
		rf.lastCommitIndex = lastIndex
	}

	return rf
}

func randomElectionTimeout(minDur, maxDur time.Duration) time.Duration {
	if maxDur <= minDur {
		return minDur
	}
	return minDur + time.Duration(rand.Int63n(int64(maxDur-minDur)))
}

// Run starts the node's background goroutines (election/heartbeat timer,
// apply loop, heartbeat sender). Call exactly once.
func (rf *Raft) Run() {
	rf.wg.Add(3)
	go rf.runLoop()
	go rf.applyLoop()
	go rf.heartbeatLoop()
}

// Stop halts the node: no more elections, heartbeats, or applies. Stops
// the gRPC server first (waiting for any in-flight RPC handlers to
// finish, so none of them can still be trying to use node state mid-
// shutdown), then waits for all background goroutines to exit, then
// closes the channel returned by Done. Safe to call multiple times.
func (rf *Raft) Stop() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.StopGRPC()
	rf.wg.Wait()
	rf.doneOnce.Do(func() { close(rf.done) })
}

// Done returns a channel that is closed once Stop has fully completed
// (all background goroutines have exited). Consumers of applyCh should
// select on this alongside applyCh to know when no further messages will
// ever arrive, rather than relying on applyCh itself being closed (it
// intentionally never is — see the package-level correctness notes on
// avoiding a multiple-writer channel-close hazard).
func (rf *Raft) Done() <-chan struct{} {
	return rf.done
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) runLoop() {
	defer rf.wg.Done()
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		timeout := rf.electionTimeout
		rf.mu.Unlock()

		switch state {
		case Follower, Candidate:
			if time.Since(lastHeartbeat) > timeout {
				rf.startElection()
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		case Leader:
			time.Sleep(rf.heartbeatInterval)
		}
	}
}

func (rf *Raft) applyLoop() {
	defer rf.wg.Done()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastCommitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if rf.lastApplied < rf.log[0].Index {
			rf.lastApplied = rf.log[0].Index // resume from the snapshot base
		}
		for rf.lastCommitIndex > rf.lastApplied {
			if rf.lastApplied+1 > rf.lastLogIndex() {
				break
			}
			rf.lastApplied++
			entry := rf.logEntry(rf.lastApplied)
			msg := ApplyMsg{Index: entry.Index, Term: entry.Term, Command: entry.Data}
			rf.mu.Unlock()
			select {
			case rf.applyCh <- msg:
			case <-rf.done:
				return
			}
			rf.mu.Lock()
		}
		// Ensure only one snapshot request is in flight at a time.
		if uint64(len(rf.log)) > rf.maxLogLength && !rf.snapshotting {
			rf.snapshotting = true
			rf.logger.Info("requesting snapshot: log length threshold exceeded",
				zap.Int64("node_id", rf.me), zap.Int("log_len", len(rf.log)), zap.Uint64("threshold", rf.maxLogLength))
			// Carry the frozen lastApplied with the request: the consumer
			// serializes its state asynchronously, so it must bind the
			// snapshot data to THIS index — not to LastApplied() read
			// later, which may have advanced past what the data covers.
			snapIndex := rf.lastApplied
			rf.mu.Unlock()
			select {
			case rf.applyCh <- ApplyMsg{IsSnapshot: true, SnapshotIndex: snapIndex}:
			case <-rf.done:
				return
			}
			continue
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatLoop() {
	defer rf.wg.Done()
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if isLeader {
			rf.transport.SendHeartBeat(rf)
		}
		time.Sleep(rf.heartbeatInterval)
	}
}

// startElection transitions rf to Candidate, votes for itself, persists,
// resets the election timer (re-randomized, to minimize repeated split
// votes across retries), and fans out RequestVote to every peer.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.leaderID = -1
	rf.term++
	rf.persist()
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomElectionTimeout(rf.electionTimeoutMin, rf.electionTimeoutMax)
	req := &kvraftpb.RequestVoteRequest{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.logger.Info("starting election", zap.Int64("node_id", rf.me), zap.Uint64("term", rf.term))
	rf.mu.Unlock()

	rf.transport.SendVoteRequest(rf, req)
}

func (rf *Raft) persist() {
	if err := rf.persister.SaveState(rf.term, rf.voteFor, rf.log); err != nil {
		rf.logger.Error("persist failed", zap.Int64("node_id", rf.me), zap.Error(err))
	}
}

// Propose appends data as a new log entry, if this node is currently the
// leader, and returns the (index, term) assigned to it. It does not wait
// for the entry to be committed or applied — see the package doc comment
// on why this is a deliberate, standard-practice design choice. Returns
// ErrNotLeader if this node does not believe itself to be the leader at
// the time of the call (the caller should not retry against the same
// node; a real leadership change may also still invalidate this entry
// later, which the caller's apply-side correlation logic must handle).
func (rf *Raft) Propose(ctx context.Context, data []byte) (index uint64, term uint64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if rf.killed() {
		return 0, 0, ErrStopped
	}

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	index, term = rf.appendEntryLocked(data)
	rf.mu.Unlock()

	rf.transport.ReplicateLog(rf)
	return index, term, nil
}

// appendEntryLocked appends data as a new log entry under the current
// term, persists, and attempts to synchronously advance the commit index
// (see the comment on advanceCommitIndex's single-node rationale below).
// Must be called with rf.mu held; the caller is responsible for
// confirming this node is currently the leader before calling. Returns
// the assigned (index, term); does not itself trigger replication to
// peers — callers must do that after releasing rf.mu, exactly as Propose
// does.
func (rf *Raft) appendEntryLocked(data []byte) (index uint64, term uint64) {
	entry := &kvraftpb.Entry{
		Term:  rf.term,
		Index: rf.lastLogIndex() + 1,
		Data:  data,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	index, term = entry.Index, entry.Term

	// Attempt to advance the commit index synchronously: for a
	// zero-peer ("single-node cluster") deployment, nothing else will
	// ever do this (advanceCommitIndex is otherwise only triggered from
	// peer AppendEntries responses, which never arrive when there are no
	// peers), so without this call a single-node Raft node could accept
	// proposals forever without ever committing or applying any of them.
	// Harmless no-op for a multi-node cluster at this point (peer
	// matchIndex values haven't changed yet), so unconditionally safe to
	// call here regardless of cluster size.
	rf.advanceCommitIndex()
	return index, term
}

// advanceCommitIndex must be called with rf.mu held. It advances
// lastCommitIndex to the highest index N such that N is replicated on a
// majority of nodes (including this leader implicitly) and log[N].Term
// equals the current term (the standard Raft safety restriction: a
// leader only directly commits entries from its own term, never an older
// term's entry purely by replication count, to avoid the "committed
// entry could be overwritten by a future leader" hazard described in the
// Raft paper §5.4.2).
func (rf *Raft) advanceCommitIndex() {
	start := rf.lastCommitIndex + 1
	if start <= rf.log[0].Index {
		start = rf.log[0].Index + 1
	}
	for n := start; n <= rf.lastLogIndex(); n++ {
		if rf.logEntry(n).Term != rf.term {
			continue
		}
		count := 1 // the leader itself
		for _, match := range rf.matchIndex {
			if match >= n {
				count++
			}
		}
		if count > (len(rf.matchIndex)+1)/2 {
			rf.lastCommitIndex = n
		}
	}
}

// logIndex converts a global log index to an offset into rf.log (which is
// trimmed by snapshotting, so index 0 of the slice does not necessarily
// correspond to global index 0).
func (rf *Raft) logIndex(globalIndex uint64) uint64 {
	return globalIndex - rf.log[0].Index
}

func (rf *Raft) logEntry(globalIndex uint64) *kvraftpb.Entry {
	return rf.log[rf.logIndex(globalIndex)]
}

func (rf *Raft) lastLogIndex() uint64 {
	return rf.log[0].Index + uint64(len(rf.log)) - 1
}

func (rf *Raft) lastLogTerm() uint64 {
	return rf.log[len(rf.log)-1].Term
}

// LastApplied returns the highest log index this node has applied to its
// state machine so far.
func (rf *Raft) LastApplied() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

// LogBase returns the (index, term) of the first entry in this node's
// in-memory log — normally 0/0, but after log compaction (local snapshot
// or an installed one) it is the snapshot's last included entry. A peer
// whose nextIndex has fallen to or below this index needs a snapshot, not
// incremental replication, to catch up.
func (rf *Raft) LogBase() (index, term uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[0].Index, rf.log[0].Term
}

// IsLeader reports whether this node currently believes itself to be the
// Raft leader.
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

// Term returns this node's current Raft term.
func (rf *Raft) Term() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term
}

// LeaderID returns the node ID this node currently believes to be the
// Raft leader, and whether it actually knows (false if no leader is
// currently known — e.g. mid-election, or this node hasn't heard from
// anyone yet).
func (rf *Raft) LeaderID() (id int64, known bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.leaderID < 0 {
		return 0, false
	}
	return rf.leaderID, true
}

// NodeID returns this node's own ID. Immutable for the node's lifetime;
// safe to call without locking.
func (rf *Raft) NodeID() int64 {
	return rf.me
}

// ClusterSize returns the total number of nodes in the cluster, including
// this one.
func (rf *Raft) ClusterSize() int {
	return rf.transport.PeerCount() + 1
}

// stateName returns a human-readable name for a Raft state constant, for
// logging.
func stateName(s int) string {
	switch s {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}
