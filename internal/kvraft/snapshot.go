package kvraft

import (
	"go.uber.org/zap"

	kvraftpb "stratum/api/proto/kvraft"
)

// ResetSnapshotPending clears the "snapshot transfer in flight" flag for
// peerID, allowing a future replication round to trigger a fresh snapshot
// for that peer if it's still behind.
func (rf *Raft) ResetSnapshotPending(peerID int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshotPending[peerID] = false
}

// ResetSnapshotting clears the "local snapshot request in flight" flag,
// allowing applyLoop to request another snapshot once the log grows past
// the threshold again. Called by the state-machine layer once it has
// finished handling an ApplyMsg{IsSnapshot: true} request (regardless of
// whether it actually called Snapshot, e.g. if it decided the log hadn't
// grown enough to be worth compacting yet).
func (rf *Raft) ResetSnapshotting() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshotting = false
}

// Snapshot compacts the log up to and including index: every entry at or
// before index is discarded from memory (replaced by a single sentinel
// entry recording index and its term), and data (the state-machine's own
// serialized snapshot) is durably persisted alongside it. A no-op if
// index does not exceed the current snapshot base or exceeds what's
// actually in the log.
func (rf *Raft) Snapshot(index uint64, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log[0].Index {
		return
	}
	offset := rf.logIndex(index)
	if offset >= uint64(len(rf.log)) {
		return
	}

	newLog := []*kvraftpb.Entry{{Index: index, Term: rf.log[offset].Term}}
	newLog = append(newLog, rf.log[offset+1:]...)
	rf.log = newLog

	if err := rf.persister.SaveSnapshot(data, index); err != nil {
		rf.logger.Error("SaveSnapshot failed", zap.Int64("node_id", rf.me), zap.Uint64("index", index), zap.Error(err))
	}
	rf.persist()
}

// SnapshotDone is called by the state-machine layer once an out-of-band
// snapshot transfer to peerID has completed, so Raft can resume normal
// incremental replication to that peer from lastIndex onward.
func (rf *Raft) SnapshotDone(peerID int64, lastIndex uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshotPending[peerID] = false
	rf.nextIndex[peerID] = lastIndex + 1
	rf.matchIndex[peerID] = lastIndex
}

// InstallDone is called by the state-machine layer once it has finished
// applying a snapshot received via InstallSnapshot (an
// ApplyMsg{IsSnapshot: true, SnapshotData: non-nil} message), so Raft can
// advance its own bookkeeping (lastApplied/lastCommitIndex) and compact
// its log to match. lastTerm is the term of the log entry the snapshot
// covers (the leader's log base), used as the sentinel entry's term when
// the snapshot covers more than this node's whole log.
func (rf *Raft) InstallDone(lastIndex, lastTerm uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIndex > rf.lastApplied {
		rf.lastApplied = lastIndex
	}
	if lastIndex > rf.lastCommitIndex {
		rf.lastCommitIndex = lastIndex
	}
	rf.trimLogLocked(lastIndex, lastTerm)
}

// TrimLog compacts the log up to and including index without persisting
// new state-machine snapshot data (used when the snapshot data itself was
// already persisted separately, e.g. right after InstallSnapshot wrote it
// via the state-machine layer's own storage).
func (rf *Raft) TrimLog(index uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.lastLogTerm()
	if offset := rf.logIndex(index); offset < uint64(len(rf.log)) {
		term = rf.log[offset].Term
	}
	rf.trimLogLocked(index, term)
}

// trimLogLocked must be called with rf.mu held. It compacts the log up to
// and including index, replacing the covered prefix with a single sentinel
// entry (index, term). When index is beyond the end of the local log —
// the snapshot covers more than this node has ever held, the typical
// lagging-follower case — the whole log is replaced by the sentinel;
// otherwise the sentinel's term is taken from the log entry at index.
func (rf *Raft) trimLogLocked(index uint64, term uint64) {
	if index <= rf.log[0].Index {
		return
	}
	offset := rf.logIndex(index)
	if offset >= uint64(len(rf.log)) {
		// The snapshot reaches past the end of our log; there is nothing
		// to preserve after it.
		rf.log = []*kvraftpb.Entry{{Index: index, Term: term}}
		rf.persist()
		rf.snapshotting = false
		return
	}
	newLog := []*kvraftpb.Entry{{Index: index, Term: rf.log[offset].Term}}
	newLog = append(newLog, rf.log[offset+1:]...)
	rf.log = newLog
	rf.persist()
	rf.snapshotting = false
}
