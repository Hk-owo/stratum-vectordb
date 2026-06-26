package kvraft

import (
	"context"
	"time"

	"go.uber.org/zap"

	kvraftpb "stratum/api/proto/kvraft"
)

// RequestVote implements kvraftpb.RaftServer: handles an incoming vote
// request from a candidate.
func (rf *Raft) RequestVote(_ context.Context, req *kvraftpb.RequestVoteRequest) (*kvraftpb.RequestVoteResponse, error) {
	if rf.killed() {
		// A node mid-shutdown must not grant votes: Stop's GracefulStop
		// waits for in-flight RPC handlers to finish, so without this
		// check a RequestVote that happened to be executing right as the
		// node was stopping could still succeed and hand out a vote that
		// should never have been cast (observed in practice as
		// intermittent incorrect leader election in minority-partition
		// tests — a "dying" minority node granting one last vote was
		// just enough to push a lone survivor over a stale majority
		// threshold).
		return nil, ErrStopped
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.term {
		return &kvraftpb.RequestVoteResponse{Term: rf.term, VoteGranted: false}, nil
	}

	if req.Term > rf.term {
		rf.term = req.Term
		rf.state = Follower
		rf.voteFor = -1
		rf.leaderID = -1
		rf.persist()
	}

	if rf.voteFor != -1 && rf.voteFor != req.CandidateId {
		return &kvraftpb.RequestVoteResponse{Term: rf.term, VoteGranted: false}, nil
	}

	// Grant the vote only if the candidate's log is at least as up to
	// date as ours (Raft §5.4.1): a strictly later last-log term wins
	// outright; on a tie, a longer (or equal) last-log index wins.
	logOK := req.LastLogTerm > rf.lastLogTerm() ||
		(req.LastLogTerm == rf.lastLogTerm() && req.LastLogIndex >= rf.lastLogIndex())
	if !logOK {
		return &kvraftpb.RequestVoteResponse{Term: rf.term, VoteGranted: false}, nil
	}

	rf.voteFor = req.CandidateId
	rf.lastHeartbeat = time.Now()
	rf.persist()
	return &kvraftpb.RequestVoteResponse{Term: rf.term, VoteGranted: true}, nil
}

// AppendEntries implements kvraftpb.RaftServer: handles both heartbeats
// (Entries empty) and log replication (Entries non-empty) from the
// leader. Both paths perform the same PrevLogIndex/PrevLogTerm
// consistency check before accepting — a heartbeat that doesn't actually
// match this node's log at PrevLogIndex must be rejected exactly like a
// replication request would be, or this node could incorrectly advance
// its commit index past a point where its log has diverged from the
// leader's.
func (rf *Raft) AppendEntries(_ context.Context, req *kvraftpb.AppendEntriesRequest) (*kvraftpb.AppendEntriesResponse, error) {
	if rf.killed() {
		return nil, ErrStopped
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.term {
		return &kvraftpb.AppendEntriesResponse{Term: rf.term, Success: false}, nil
	}
	if req.Term > rf.term {
		rf.term = req.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.leaderID = req.LeaderId
	rf.lastHeartbeat = time.Now()

	// PrevLogIndex predates our snapshot base: we can't verify it, but
	// since it's already covered by our snapshot it's necessarily
	// committed and matching: accept.
	if req.PrevLogIndex < rf.log[0].Index {
		return rf.acceptAppendFrom(req, rf.log[0].Index)
	}

	if req.PrevLogIndex > rf.lastLogIndex() {
		return &kvraftpb.AppendEntriesResponse{
			Term:          rf.term,
			Success:       false,
			ConflictIndex: rf.lastLogIndex() + 1,
			ConflictTerm:  0,
		}, nil
	}

	prevEntry := rf.logEntry(req.PrevLogIndex)
	if prevEntry.Term != req.PrevLogTerm {
		conflictTerm := prevEntry.Term
		conflictIndex := req.PrevLogIndex
		for conflictIndex > rf.log[0].Index+1 && rf.logEntry(conflictIndex-1).Term == conflictTerm {
			conflictIndex--
		}
		return &kvraftpb.AppendEntriesResponse{
			Term:          rf.term,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}, nil
	}

	if len(req.Entries) > 0 {
		localIdx := rf.logIndex(req.PrevLogIndex)
		rf.log = append(rf.log[:localIdx+1], req.Entries...)
		rf.persist()
	}

	return rf.acceptAppendFrom(req, rf.lastLogIndex())
}

// acceptAppendFrom finalizes a successful AppendEntries: advances
// lastCommitIndex per req.LeaderCommit (bounded by upTo, the highest
// index this node can actually vouch for after this call) and returns a
// success response. Must be called with rf.mu held.
func (rf *Raft) acceptAppendFrom(req *kvraftpb.AppendEntriesRequest, upTo uint64) (*kvraftpb.AppendEntriesResponse, error) {
	if req.LeaderCommit > rf.lastCommitIndex {
		if req.LeaderCommit < upTo {
			rf.lastCommitIndex = req.LeaderCommit
		} else {
			rf.lastCommitIndex = upTo
		}
	}
	return &kvraftpb.AppendEntriesResponse{Term: rf.term, Success: true}, nil
}

// InstallSnapshot implements kvraftpb.RaftServer: handles a snapshot
// pushed by the leader when this node has fallen too far behind for
// incremental log replication to catch it up. The actual application of
// the snapshot to the state machine happens asynchronously — this handler
// only hands the snapshot off via applyCh and returns; it must not hold
// rf.mu while doing so (see the package doc comment's note on the
// original lock-held-channel-send deadlock risk this fixes).
func (rf *Raft) InstallSnapshot(_ context.Context, req *kvraftpb.InstallSnapshotRequest) (*kvraftpb.InstallSnapshotResponse, error) {
	if rf.killed() {
		return nil, ErrStopped
	}
	rf.mu.Lock()

	if req.Term < rf.term {
		term := rf.term
		rf.mu.Unlock()
		return &kvraftpb.InstallSnapshotResponse{Term: term}, nil
	}
	if req.Term > rf.term {
		rf.term = req.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.leaderID = req.LeaderId
	rf.lastHeartbeat = time.Now()

	if req.LastIncludedIndex <= rf.log[0].Index {
		// Snapshot is not newer than what we already have; ignore.
		term := rf.term
		rf.mu.Unlock()
		return &kvraftpb.InstallSnapshotResponse{Term: term}, nil
	}

	term := rf.term
	rf.mu.Unlock()

	msg := ApplyMsg{
		IsSnapshot:    true,
		SnapshotData:  req.Data,
		SnapshotIndex: req.LastIncludedIndex,
	}
	select {
	case rf.applyCh <- msg:
	case <-rf.done:
		rf.logger.Debug("InstallSnapshot: node stopped before snapshot could be delivered to applyCh",
			zap.Int64("node_id", rf.me))
	}

	return &kvraftpb.InstallSnapshotResponse{Term: term}, nil
}
