package kvraft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvraftpb "stratum/api/proto/kvraft"
)

// defaultRPCTimeout bounds how long a single peer-to-peer Raft RPC
// (RequestVote, AppendEntries, InstallSnapshot trigger) is allowed to
// take before Transport gives up on it. These are background,
// timer-driven calls with no external caller waiting on a specific
// context (heartbeats and replication are Raft's own internal clock, not
// something a Stratum-level caller is blocked on) — see the package doc
// comment on why these do not thread an external context through.
const defaultRPCTimeout = 1 * time.Second

// peerConn is a single peer's Raft gRPC client connection.
type peerConn struct {
	client kvraftpb.RaftClient
	kvAddr string // address of the peer's Stratum-level service, used for snapshot transfer
}

// Transport manages this node's gRPC connections to its Raft peers and
// drives the RPC fan-out for heartbeats, log replication, and vote
// requests.
//
// Concurrency invariant: AddPeer must be called for every peer before
// Run is called on the owning Raft instance. Transport's peer map is
// built once at startup and then only read (concurrently, by the
// background replication/heartbeat/election goroutines) — there is no
// support for adding peers after the cluster is running (matching the
// project's "改配置 + 重启" membership-change model, per
// Stratum_代码风格v2.md's 配置文件 Schema section: cluster membership
// changes are not an online operation).
type Transport struct {
	logger *zap.Logger

	rpcTimeout time.Duration

	mu             sync.RWMutex
	peers          map[int64]*peerConn
	onNeedSnapshot func(peerID int64, lastIndex uint64)
}

// NewTransport constructs a Transport. logger may be nil, in which case a
// no-op logger is used (convenient for tests that don't care about Raft's
// internal logging).
func NewTransport(logger *zap.Logger) *Transport {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Transport{
		logger:     logger,
		rpcTimeout: defaultRPCTimeout,
		peers:      make(map[int64]*peerConn),
	}
}

// AddPeer registers a peer's Raft RPC address (raftAddr) and Stratum-level
// service address (kvAddr, used only for out-of-band snapshot transfer —
// see SetSnapshotHandler). See the Transport doc comment for the
// call-before-Run invariant.
func (t *Transport) AddPeer(id int64, raftAddr string, kvAddr string) error {
	conn, err := grpc.NewClient(raftAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("kvraft: connect peer %d at %s: %w", id, raftAddr, err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = &peerConn{
		client: kvraftpb.NewRaftClient(conn),
		kvAddr: kvAddr,
	}
	return nil
}

// PeerCount returns the number of registered peers (not counting this
// node itself).
func (t *Transport) PeerCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.peers)
}

// SetSnapshotHandler registers the callback invoked when a peer falls far
// enough behind that it needs a full snapshot rather than incremental log
// replication. The Stratum-level node wiring (analogous to KVServer's
// Node.sendSnapshot) is responsible for actually transferring the
// snapshot out-of-band and then calling SnapshotDone.
func (t *Transport) SetSnapshotHandler(fn func(peerID int64, lastIndex uint64)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onNeedSnapshot = fn
}

func (t *Transport) peer(id int64) (*peerConn, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.peers[id]
	return p, ok
}

func (t *Transport) peerIDs() []int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids := make([]int64, 0, len(t.peers))
	for id := range t.peers {
		ids = append(ids, id)
	}
	return ids
}

// replicateToPeer sends whatever peer id needs next: a snapshot trigger
// if it has fallen behind the start of this node's log, or an
// AppendEntries carrying any log entries it's missing (which doubles as a
// heartbeat when there are none). Called with rf.mu held; releases it
// before making the network call and does not require it held on return.
func (t *Transport) replicateToPeer(rf *Raft, id int64, peer *peerConn) {
	if rf.nextIndex[id] <= rf.log[0].Index {
		if rf.snapshotPending[id] {
			// A snapshot transfer is already in flight for this peer;
			// send an empty heartbeat to keep the connection alive
			// without triggering a duplicate snapshot.
			req := &kvraftpb.AppendEntriesRequest{
				Term:         rf.term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.log[0].Index,
				PrevLogTerm:  rf.log[0].Term,
				LeaderCommit: rf.lastCommitIndex,
			}
			rf.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), t.rpcTimeout)
			defer cancel()
			_, _ = peer.client.AppendEntries(ctx, req)
			return
		}
		rf.snapshotPending[id] = true
		lastIndex := rf.log[0].Index
		rf.mu.Unlock()
		t.SendInstallSnapshot(id, lastIndex)
		return
	}

	nextIndex := rf.nextIndex[id]
	if nextIndex == 0 {
		nextIndex = 1
	}
	prev := rf.logEntry(nextIndex - 1)
	entries := rf.log[rf.logIndex(nextIndex):]
	req := &kvraftpb.AppendEntriesRequest{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		Entries:      entries,
		LeaderCommit: rf.lastCommitIndex,
	}
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), t.rpcTimeout)
	defer cancel()
	resp, err := peer.client.AppendEntries(ctx, req)
	if err != nil {
		t.logger.Debug("AppendEntries RPC failed", zap.Int64("peer_id", id), zap.Error(err))
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if resp.Term > rf.term {
		t.logger.Info("stepping down: discovered higher term from AppendEntries response",
			zap.Int64("node_id", rf.me), zap.Uint64("old_term", rf.term), zap.Uint64("new_term", resp.Term))
		rf.term = resp.Term
		rf.state = Follower
		rf.voteFor = -1
		rf.leaderID = -1
		rf.persist()
		return
	}

	if resp.Success {
		newMatch := req.PrevLogIndex + uint64(len(req.Entries))
		if newMatch > rf.matchIndex[id] {
			rf.matchIndex[id] = newMatch
			rf.nextIndex[id] = newMatch + 1
		}
		rf.advanceCommitIndex()
		if rf.snapshotPending[id] {
			// Peer caught back up on its own (or the snapshot transfer
			// finished); allow a future snapshot trigger if it falls
			// behind again.
			rf.snapshotPending[id] = false
		}
	} else {
		nextIndex := resp.ConflictIndex
		if resp.ConflictTerm != 0 {
			for i := uint64(len(rf.log)) - 1; i >= 1; i-- {
				if rf.log[i].Term == resp.ConflictTerm {
					nextIndex = i + 1
					break
				}
			}
		}
		if nextIndex == 0 {
			nextIndex = 1
		}
		rf.nextIndex[id] = nextIndex
	}
}

// SendHeartBeat fans out an empty AppendEntries (or whatever replication
// is actually needed) to every peer. Called periodically by the leader's
// heartbeat loop.
func (t *Transport) SendHeartBeat(rf *Raft) {
	for _, id := range t.peerIDs() {
		id := id
		peer, ok := t.peer(id)
		if !ok {
			continue
		}
		go func() {
			rf.mu.Lock()
			t.replicateToPeer(rf, id, peer)
		}()
	}
}

// ReplicateLog fans out AppendEntries carrying newly proposed entries to
// every peer. Called once after Propose appends a new entry locally.
func (t *Transport) ReplicateLog(rf *Raft) {
	for _, id := range t.peerIDs() {
		id := id
		peer, ok := t.peer(id)
		if !ok {
			continue
		}
		go func() {
			rf.mu.Lock()
			t.replicateToPeer(rf, id, peer)
		}()
	}
}

// SendInstallSnapshot invokes the registered snapshot handler (see
// SetSnapshotHandler) so the Stratum-level node wiring can transfer a
// snapshot to peerID out-of-band.
func (t *Transport) SendInstallSnapshot(peerID int64, lastIndex uint64) {
	t.mu.RLock()
	handler := t.onNeedSnapshot
	t.mu.RUnlock()
	if handler != nil {
		handler(peerID, lastIndex)
	}
}

// SendVoteRequest fans out RequestVote to every peer and tallies the
// result, transitioning rf to Leader if a majority (including its own
// implicit self-vote) is reached while rf is still a Candidate in the
// requested term.
//
// The self-vote majority check happens immediately, before any peer RPCs
// are even sent: for a single-node "cluster" (zero peers), one vote is
// already a majority of one, and there is no peer response that would
// otherwise ever trigger the transition to Leader.
//
// Becoming leader also appends and replicates a single no-op entry under
// the new term. This is standard Raft practice (and not present in the
// original KVServer this package was adapted from — its absence was a
// real bug): a leader can only ever directly advance its commit index for
// entries from its OWN current term (Raft §5.4.2's safety restriction);
// entries from earlier terms only become committed indirectly, by virtue
// of a later same-term entry committing first. Without proposing
// something in the new term immediately, a freshly elected leader (most
// visibly: a restarted single-node deployment re-electing itself) would
// leave every pre-existing log entry permanently uncommitted — and
// therefore never applied to the state machine — until unrelated new
// write traffic happened to arrive.
func (t *Transport) SendVoteRequest(rf *Raft, req *kvraftpb.RequestVoteRequest) {
	votes := uint64(1) // self-vote

	becomeLeaderLocked := func() {
		t.logger.Info("became leader", zap.Int64("node_id", rf.me), zap.Uint64("term", rf.term))
		rf.state = Leader
		rf.leaderID = rf.me
		for _, peerID := range t.peerIDs() {
			rf.nextIndex[peerID] = rf.lastLogIndex() + 1
			rf.matchIndex[peerID] = 0
			rf.snapshotPending[peerID] = false
		}
		rf.appendEntryLocked(nil) // no-op entry; see doc comment above
	}

	rf.mu.Lock()
	if rf.state == Candidate && rf.term == req.Term && votes > uint64(t.PeerCount()+1)/2 {
		becomeLeaderLocked()
		rf.mu.Unlock()
		rf.transport.ReplicateLog(rf)
		return
	}
	rf.mu.Unlock()

	var voteMu sync.Mutex
	for _, id := range t.peerIDs() {
		peer, ok := t.peer(id)
		if !ok {
			continue
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), t.rpcTimeout)
			defer cancel()
			res, err := peer.client.RequestVote(ctx, req)
			if err != nil {
				return
			}

			rf.mu.Lock()

			if rf.term < res.Term {
				t.logger.Info("stepping down: discovered higher term from RequestVote response",
					zap.Int64("node_id", rf.me), zap.Uint64("old_term", rf.term), zap.Uint64("new_term", res.Term))
				rf.term = res.Term
				rf.voteFor = -1
				rf.leaderID = -1
				rf.persist()
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			if rf.state != Candidate || rf.term != req.Term {
				rf.mu.Unlock()
				return
			}
			if !res.VoteGranted {
				rf.mu.Unlock()
				return
			}

			voteMu.Lock()
			votes++
			v := votes
			voteMu.Unlock()

			becameLeader := false
			if v > uint64(t.PeerCount()+1)/2 {
				becomeLeaderLocked()
				becameLeader = true
			}
			rf.mu.Unlock()

			if becameLeader {
				rf.transport.ReplicateLog(rf)
			}
		}()
	}
}
