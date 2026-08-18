package kvraft

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestSnapshot_LocalCompactionRoundTrip exercises the snapshot machinery
// end to end on a single node: with a small MaxLogLength the apply loop
// emits ApplyMsg{IsSnapshot: true, SnapshotData: nil}; the state-machine
// layer (mimicked here) serializes state, calls Raft.Snapshot, then
// ResetSnapshotting; the log is compacted and the node keeps serving.
func TestSnapshot_LocalCompactionRoundTrip(t *testing.T) {
	rf, _, applyCh := newTestRaft(t, 1, WithMaxLogLength(8), WithLogger(zap.NewNop()))
	rf.Run()

	if !waitFor(t, 2*time.Second, rf.IsLeader) {
		t.Fatalf("single node never became leader")
	}

	ctx := context.Background()
	// Propose until the compaction request arrives.
	var snapMsg ApplyMsg
	deadline := time.After(5 * time.Second)
	proposed := 0
	for snapMsg.SnapshotData == nil && !snapMsg.IsSnapshot {
		select {
		case msg := <-applyCh:
			if msg.IsSnapshot {
				snapMsg = msg
				break
			}
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot request (proposed %d)", proposed)
		}
		if snapMsg.IsSnapshot {
			break
		}
		if _, _, err := rf.Propose(ctx, []byte("cmd")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		proposed++
	}

	// State-machine layer responds: snapshot up to lastApplied.
	lastApplied := rf.LastApplied()
	if lastApplied == 0 {
		t.Fatal("LastApplied = 0, expected entries to have been applied")
	}
	rf.Snapshot(lastApplied, []byte("snapshot-data"))

	// Log must now be a single sentinel entry at lastApplied (a second
	// entry can transiently exist if an entry was proposed but not yet
	// applied when Snapshot ran — assert the invariants that matter).
	rf.mu.Lock()
	if rf.log[0].Index != lastApplied {
		t.Errorf("after Snapshot: log[0].Index = %d, want %d", rf.log[0].Index, lastApplied)
	}
	if len(rf.log) > 2 {
		t.Errorf("after Snapshot: log has %d entries, want <= 2", len(rf.log))
	}
	for _, e := range rf.log[1:] {
		if e.Index <= lastApplied {
			t.Errorf("log still holds entry at index %d (<= snapshot base %d)", e.Index, lastApplied)
		}
	}
	rf.mu.Unlock()

	// Persister must hold the snapshot.
	if data, idx, err := rf.persister.LoadSnapshot(); err != nil || string(data) != "snapshot-data" || idx != lastApplied {
		t.Errorf("LoadSnapshot = (%q, %d, %v), want (snapshot-data, %d, nil)", data, idx, err, lastApplied)
	}

	rf.ResetSnapshotting()

	// The node keeps working after compaction: a new propose applies.
	idx, _, err := rf.Propose(ctx, []byte("after-snapshot"))
	if err != nil {
		t.Fatalf("Propose after snapshot: %v", err)
	}
	_ = readUntilIndex(t, applyCh, idx, 5*time.Second)

	// Second compaction: TrimLog path (no new snapshot data persisted).
	trimAt := rf.LastApplied()
	rf.TrimLog(trimAt)
	rf.mu.Lock()
	if rf.log[0].Index != trimAt {
		t.Errorf("after TrimLog: log[0].Index = %d, want %d", rf.log[0].Index, trimAt)
	}
	rf.mu.Unlock()
}

// TestSnapshot_BookkeepingCovers the remaining snapshot bookkeeping
// helpers (SnapshotDone / InstallDone / ResetSnapshotPending) plus the
// no-op guards on Snapshot/TrimLog for indices outside the log.
func TestSnapshot_Bookkeeping(t *testing.T) {
	rf, _, applyCh := newTestRaft(t, 1, WithMaxLogLength(64), WithLogger(zap.NewNop()))
	rf.Run()
	if !waitFor(t, 2*time.Second, rf.IsLeader) {
		t.Fatalf("single node never became leader")
	}

	// No-ops: index before the log base or beyond the log end.
	rf.mu.Lock()
	base := rf.log[0].Index
	before := len(rf.log)
	rf.mu.Unlock()
	rf.Snapshot(0, []byte("x"))              // index 0 <= log[0].Index → no-op
	rf.TrimLog(0)                            // no-op
	rf.Snapshot(base+1_000_000, []byte("x")) // beyond the log end → no-op
	rf.mu.Lock()
	if len(rf.log) != before {
		t.Errorf("no-op Snapshot calls must not alter the log: before=%d after=%d", before, len(rf.log))
	}
	rf.mu.Unlock()

	// SnapshotDone for a peer: resets pending and advances next/match.
	rf.ResetSnapshotPending(2)
	rf.SnapshotDone(2, 5)
	rf.mu.Lock()
	if rf.snapshotPending[2] {
		t.Error("snapshotPending[2] should be false after SnapshotDone")
	}
	if rf.nextIndex[2] != 6 || rf.matchIndex[2] != 5 {
		t.Errorf("after SnapshotDone: nextIndex=%d matchIndex=%d, want 6/5", rf.nextIndex[2], rf.matchIndex[2])
	}
	rf.mu.Unlock()

	// InstallDone advances applied/commit and trims the log.
	ctx := context.Background()
	idx, _, err := rf.Propose(ctx, []byte("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	_ = readUntilIndex(t, applyCh, idx, 5*time.Second)
	rf.InstallDone(idx)
	rf.mu.Lock()
	if rf.lastApplied < idx || rf.lastCommitIndex < idx {
		t.Errorf("after InstallDone: lastApplied=%d lastCommit=%d, want >= %d", rf.lastApplied, rf.lastCommitIndex, idx)
	}
	rf.mu.Unlock()
}

// TestSnapshot_AccessorsAndStateName covers NodeID / stateName and the
// trivial option setters (WithLogger, WithMaxLogLength).
func TestSnapshot_AccessorsAndStateName(t *testing.T) {
	rf, _, _ := newTestRaft(t, 7, WithLogger(zap.NewNop()), WithMaxLogLength(16))
	if rf.NodeID() != 7 {
		t.Errorf("NodeID = %d, want 7", rf.NodeID())
	}
	if rf.ClusterSize() != 1 {
		t.Errorf("ClusterSize = %d, want 1", rf.ClusterSize())
	}
	if got := stateName(Leader); got != "Leader" {
		t.Errorf("stateName(Leader) = %q", got)
	}
	if got := stateName(Candidate); got != "Candidate" {
		t.Errorf("stateName(Candidate) = %q", got)
	}
	if got := stateName(Follower); got != "Follower" {
		t.Errorf("stateName(Follower) = %q", got)
	}
	if got := stateName(999); got == "Leader" || got == "Candidate" || got == "Follower" {
		t.Errorf("stateName(999) = %q, want Unknown", got)
	}
	// NodeID / accessor smoke: node still constructs and stops cleanly.
	rf.Stop()
}
