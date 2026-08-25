package kvraft

import (
	"context"
	"fmt"
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
	idx, term, err := rf.Propose(ctx, []byte("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	_ = readUntilIndex(t, applyCh, idx, 5*time.Second)
	rf.InstallDone(idx, term)
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

// snapshotConsumer mimics the state-machine layer in front of a kvraft
// node's applyCh: it applies ordinary entries (recorded implicitly via
// the node's LastApplied), answers local snapshot requests by compacting
// the log, and finalizes snapshots installed from a leader.
type snapshotConsumer struct {
	rf        *Raft
	applyCh   chan ApplyMsg
	installed chan struct{} // signaled once per snapshot installed from a leader
	stop      chan struct{}
}

func newSnapshotConsumer(rf *Raft, applyCh chan ApplyMsg) *snapshotConsumer {
	c := &snapshotConsumer{rf: rf, applyCh: applyCh, installed: make(chan struct{}, 16), stop: make(chan struct{})}
	go c.run()
	return c
}

func (c *snapshotConsumer) run() {
	for {
		select {
		case msg := <-c.applyCh:
			if !msg.IsSnapshot {
				continue // ordinary entry: applied implicitly via LastApplied
			}
			if msg.SnapshotData == nil {
				// Locally triggered compaction request: serialize state,
				// hand it to Raft, then allow the next request.
				if idx := c.rf.LastApplied(); idx > 0 {
					c.rf.Snapshot(idx, []byte("snapshot-data"))
				}
				c.rf.ResetSnapshotting()
				continue
			}
			// Snapshot pushed by the leader: install it.
			c.rf.InstallDone(msg.SnapshotIndex, msg.SnapshotTerm)
			c.installed <- struct{}{}
		case <-c.stop:
			return
		}
	}
}

func (c *snapshotConsumer) close() { close(c.stop) }

// restartTestNode rebuilds a Raft node on the same persister and address
// (simulating a process restart) and reconnects it to peers.
func restartTestNode(t *testing.T, old *testNode, peers []*testNode) *testNode {
	t.Helper()
	transport := NewTransport(nil)
	for _, p := range peers {
		if p.id == old.id {
			continue
		}
		if err := transport.AddPeer(p.id, p.addr, "unused"); err != nil {
			t.Fatalf("restart AddPeer(%d -> %d): %v", old.id, p.id, err)
		}
	}
	applyCh := make(chan ApplyMsg, 256)
	rf := NewRaft(old.id, transport, applyCh, old.persister,
		WithElectionTimeoutRange(50*time.Millisecond, 100*time.Millisecond),
		WithHeartbeatInterval(20*time.Millisecond),
	)
	nd := &testNode{id: old.id, addr: old.addr, rf: rf, applyCh: applyCh, transport: transport, persister: old.persister}
	if err := rf.StartGRPC(old.addr); err != nil {
		t.Fatalf("restart StartGRPC(%d): %v", old.id, err)
	}
	rf.Run()
	return nd
}

// TestCluster_SnapshotCatchesUpLaggingFollower exercises the full
// leader→follower snapshot transfer path: a follower that falls behind
// the start of the leader's trimmed log must be caught up via
// InstallSnapshot (not incremental replication), after which ordinary
// replication resumes. Before the transfer path was wired up
// (SetSnapshotHandler was never registered), the lagging follower stayed
// wedged forever.
func TestCluster_SnapshotCatchesUpLaggingFollower(t *testing.T) {
	nodes := newTestCluster(t, 3, WithMaxLogLength(8))
	leader := waitForSingleLeader(t, nodes, 5*time.Second)

	// Register a snapshot data provider on every node (the state-machine
	// layer's role) and start consuming applyCh on every node.
	consumers := make([]*snapshotConsumer, len(nodes))
	laggardIdx := -1
	for i, nd := range nodes {
		if nd != leader {
			laggardIdx = i
		}
		nd := nd
		nd.transport.SetSnapshotHandler(func(peerID int64, lastIndex uint64) []byte {
			data, _, err := nd.persister.LoadSnapshot()
			if err != nil || data == nil {
				return nil
			}
			return data
		})
		consumers[i] = newSnapshotConsumer(nd.rf, nd.applyCh)
	}
	defer func() {
		for _, c := range consumers {
			c.close()
		}
	}()
	if laggardIdx < 0 {
		t.Fatal("failed to pick a laggard")
	}
	laggard := nodes[laggardIdx]
	ctx := context.Background()

	// Phase 1: replicate a common baseline to all three nodes, then
	// disconnect the laggard.
	var baseIndex uint64
	for i := 0; i < 5; i++ {
		idx, _, err := leader.rf.Propose(ctx, []byte(fmt.Sprintf("pre-partition-%d", i)))
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		baseIndex = idx
	}
	for _, nd := range nodes {
		if !waitFor(t, 3*time.Second, func() bool { return nd.rf.LastApplied() >= baseIndex }) {
			t.Fatalf("node %d never applied baseline index %d", nd.id, baseIndex)
		}
	}
	laggardLastApplied := laggard.rf.LastApplied()
	laggard.rf.Stop()

	// Phase 2: keep proposing until the leader's log has been compacted
	// past the laggard's last applied index — from then on the laggard
	// can only be caught up by a snapshot, never by incremental
	// replication.
	var leaderBase uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := leader.rf.Propose(ctx, []byte("post-partition")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		leader.rf.mu.Lock()
		leaderBase = leader.rf.log[0].Index
		leader.rf.mu.Unlock()
		if leaderBase > laggardLastApplied {
			break
		}
	}
	if leaderBase <= laggardLastApplied {
		t.Fatalf("leader log base %d never passed laggard's last applied %d", leaderBase, laggardLastApplied)
	}
	if !waitFor(t, 5*time.Second, func() bool { return leader.rf.LastApplied() >= leaderBase }) {
		t.Fatalf("leader never applied up to its own log base %d", leaderBase)
	}

	// Phase 3: "restart" the laggard on its persisted state. The new
	// node has its own applyCh, so it needs its own consumer.
	restarted := restartTestNode(t, laggard, nodes)
	t.Cleanup(restarted.rf.Stop)
	restartedConsumer := newSnapshotConsumer(restarted.rf, restarted.applyCh)
	defer restartedConsumer.close()

	// Phase 4: the leader must push a snapshot; the laggard must install
	// it and then catch up to the leader's applied index.
	select {
	case <-restartedConsumer.installed:
	case <-time.After(10 * time.Second):
		t.Fatal("laggard never installed a snapshot from the leader")
	}
	if !waitFor(t, 10*time.Second, func() bool { return restarted.rf.LastApplied() >= leaderBase }) {
		t.Fatalf("laggard never caught up: LastApplied=%d, want >= %d", restarted.rf.LastApplied(), leaderBase)
	}

	// Phase 5: normal incremental replication resumes on top of the
	// installed snapshot.
	idx, _, err := leader.rf.Propose(ctx, []byte("after-catchup"))
	if err != nil {
		t.Fatalf("Propose after catch-up: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return restarted.rf.LastApplied() >= idx }) {
		t.Fatalf("laggard never applied post-snapshot index %d (LastApplied=%d)", idx, restarted.rf.LastApplied())
	}
}
