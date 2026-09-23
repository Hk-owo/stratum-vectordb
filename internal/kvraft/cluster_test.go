package kvraft

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"stratum/internal/kvstorage"
)

// testNode bundles everything needed to drive one simulated cluster
// member from a test.
type testNode struct {
	id        int64
	addr      string
	rf        *Raft
	applyCh   chan ApplyMsg
	transport *Transport
	persister *kvstorage.Persister
}

// Timing every multi-node test node is built with. The election timeout
// must sit well above the heartbeat interval — Raft's rule of thumb is an
// order of magnitude, not merely "greater than" — because a follower that
// does not hear a heartbeat inside its timeout campaigns and topples a
// perfectly healthy leader. The margin is not academic: these nodes share
// a loaded CI runner where one goroutine can be descheduled for tens of
// milliseconds, which is why the earlier 20ms/50-100ms pair (a 1:2.5
// ratio) made TestCluster_SnapshotCatchesUpLaggingFollower fail on CI with
// "Propose: kvraft: not leader" — the leader had been voted out by a
// follower that simply missed a heartbeat. Reproduced on a single core:
// 13 of 15 runs failed with that same error.
//
// newTestCluster and restartTestNode both build from these, so a
// restarted node can never end up on a different timer than its peers.
const (
	testHeartbeatInterval  = 25 * time.Millisecond
	testElectionTimeoutMin = 250 * time.Millisecond
	testElectionTimeoutMax = 500 * time.Millisecond
)

// testClusterTiming returns the base options for a multi-node test node;
// callers append their own on top.
func testClusterTiming() []Option {
	return []Option{
		WithElectionTimeoutRange(testElectionTimeoutMin, testElectionTimeoutMax),
		WithHeartbeatInterval(testHeartbeatInterval),
	}
}

// newTestCluster starts n Raft nodes, fully cross-connected, each
// listening on a free loopback port and running its background loops.
// Extra opts (e.g. WithMaxLogLength) are applied to every node. Returns
// the nodes in ID order (1..n).
func newTestCluster(t *testing.T, n int, opts ...Option) []*testNode {
	t.Helper()

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = freeLoopbackAddr(t)
	}

	nodes := make([]*testNode, n)
	for i := 0; i < n; i++ {
		id := int64(i + 1)
		transport := NewTransport(nil)
		applyCh := make(chan ApplyMsg, 256)
		persister := kvstorage.NewPersister(filepath.Join(t.TempDir(), fmt.Sprintf("raft-%d", id)))
		allOpts := append(testClusterTiming(), opts...)
		rf := NewRaft(id, transport, applyCh, persister, allOpts...)
		nodes[i] = &testNode{id: id, addr: addrs[i], rf: rf, applyCh: applyCh, transport: transport, persister: persister}
	}

	// Cross-connect: every node's Transport gets a peer entry for every
	// OTHER node. kvAddr (used for Stratum-level data sync, a concern not
	// exercised by these kvraft-level tests) is left as a placeholder.
	for i, nd := range nodes {
		for j, peer := range nodes {
			if i == j {
				continue
			}
			if err := nd.transport.AddPeer(peer.id, peer.addr, "unused"); err != nil {
				t.Fatalf("AddPeer(%d -> %d): %v", nd.id, peer.id, err)
			}
		}
	}

	for _, nd := range nodes {
		if err := nd.rf.StartGRPC(nd.addr); err != nil {
			t.Fatalf("StartGRPC(%d): %v", nd.id, err)
		}
	}
	for _, nd := range nodes {
		nd.rf.Run()
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.rf.Stop()
		}
	})

	return nodes
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// soleLeader returns the one node that currently believes itself to be
// leader, or nil when that is not exactly one node (an election in flight
// can briefly leave none, or two rivals).
//
// A multi-node test must not hold on to the *testNode it saw leading in an
// earlier phase and keep proposing against it. A follower that misses a
// heartbeat inside its election timeout campaigns and wins — routine on a
// loaded CI runner and under the race detector's slowdown, which is how
// TestCluster_SnapshotCatchesUpLaggingFollower failed on CI with
// "Propose: kvraft: not leader". Proposing against a node that has since
// been deposed returns ErrNotLeader, an artifact of the environment rather
// than of the behavior under test, so the tests resolve the leader afresh
// instead of assuming it is theirs to keep.
func soleLeader(nodes []*testNode) *testNode {
	var leader *testNode
	for _, n := range nodes {
		if !n.rf.IsLeader() {
			continue
		}
		if leader != nil {
			return nil // mid-election: more than one claimant
		}
		leader = n
	}
	return leader
}

// proposeToLeader proposes data on whichever node currently leads, waiting
// up to timeout for a leader to emerge and retrying when leadership moves
// between resolving it and proposing. Returns the assigned index and the
// node that accepted it. See soleLeader for why the returned leader must
// not be assumed to still lead later.
func proposeToLeader(t *testing.T, nodes []*testNode, data []byte, timeout time.Duration) (uint64, *testNode) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := soleLeader(nodes); leader != nil {
			index, _, err := leader.rf.Propose(context.Background(), data)
			switch {
			case err == nil:
				return index, leader
			case err == ErrNotLeader:
				// Deposed between resolving it and proposing; retry.
			default:
				t.Fatalf("Propose: %v", err)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader accepted a proposal within %v", timeout)
	return 0, nil
}

// waitForSingleLeader waits until exactly one node in nodes reports
// itself as leader and returns it, failing the test if that doesn't
// happen within timeout.
func waitForSingleLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := soleLeader(nodes); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not converge to exactly one leader within %v", timeout)
	return nil
}

func TestCluster_ElectsExactlyOneLeader(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForSingleLeader(t, nodes, 5*time.Second)

	term := leader.rf.Term()
	for _, n := range nodes {
		if n == leader {
			continue
		}
		if n.rf.IsLeader() {
			t.Fatalf("node %d also claims leadership while node %d is leader", n.id, leader.id)
		}
		// A follower's term only converges once it has received the
		// leader's first heartbeat, which may lag a freshly won election —
		// wait for convergence instead of asserting immediately.
		if !waitFor(t, 2*time.Second, func() bool { return n.rf.Term() == term }) {
			t.Fatalf("node %d term = %d, want %d (same as leader)", n.id, n.rf.Term(), term)
		}
	}
}

func TestCluster_AllNodesAgreeOnLeaderID(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForSingleLeader(t, nodes, 5*time.Second)

	for _, n := range nodes {
		if !waitFor(t, 2*time.Second, func() bool {
			id, known := n.rf.LeaderID()
			return known && id == leader.id
		}) {
			id, known := n.rf.LeaderID()
			t.Fatalf("node %d LeaderID() = (%d, %v), want (%d, true)", n.id, id, known, leader.id)
		}
	}
}

func TestCluster_ProposeReplicatesToAllNodes(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForSingleLeader(t, nodes, 5*time.Second)

	index, _, err := leader.rf.Propose(context.Background(), []byte("replicated-command"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	for _, n := range nodes {
		if !waitFor(t, 3*time.Second, func() bool { return n.rf.LastApplied() >= index }) {
			t.Fatalf("node %d never applied index %d (LastApplied=%d)", n.id, index, n.rf.LastApplied())
		}
	}

	// Every node's applyCh should have delivered the same command at the
	// same index (skipping past kvraft's automatic no-op election entry,
	// which lands at an earlier index on whichever node became leader).
	for _, n := range nodes {
		msg := readUntilIndex(t, n.applyCh, index, time.Second)
		if string(msg.Command) != "replicated-command" {
			t.Fatalf("node %d ApplyMsg = %+v, want command=replicated-command", n.id, msg)
		}
	}
}

func TestCluster_OnlyLeaderAcceptsPropose(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := waitForSingleLeader(t, nodes, 5*time.Second)

	for _, n := range nodes {
		if n == leader {
			continue
		}
		_, _, err := n.rf.Propose(context.Background(), []byte("x"))
		if err != ErrNotLeader {
			t.Fatalf("node %d (follower) Propose = %v, want ErrNotLeader", n.id, err)
		}
	}
}

func TestCluster_LeaderFailureTriggersReElectionWithoutDataLoss(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader1 := waitForSingleLeader(t, nodes, 5*time.Second)

	index1, _, err := leader1.rf.Propose(context.Background(), []byte("before-failover"))
	if err != nil {
		t.Fatalf("Propose before failover: %v", err)
	}
	for _, n := range nodes {
		waitFor(t, 3*time.Second, func() bool { return n.rf.LastApplied() >= index1 })
	}

	// Simulate the leader crashing.
	leader1.rf.Stop()

	var survivors []*testNode
	for _, n := range nodes {
		if n != leader1 {
			survivors = append(survivors, n)
		}
	}

	leader2 := waitForSingleLeader(t, survivors, 5*time.Second)
	if leader2.id == leader1.id {
		t.Fatalf("new leader has the same ID as the failed one, want a different node")
	}
	if leader2.rf.Term() <= leader1.rf.Term() {
		t.Fatalf("new leader's term = %d, want > old leader's term %d", leader2.rf.Term(), leader1.rf.Term())
	}

	// The new leader must still be able to serve writes, and the
	// pre-failover entry must not have been lost.
	index2, _, err := leader2.rf.Propose(context.Background(), []byte("after-failover"))
	if err != nil {
		t.Fatalf("Propose after failover: %v", err)
	}
	for _, n := range survivors {
		if !waitFor(t, 3*time.Second, func() bool { return n.rf.LastApplied() >= index2 }) {
			t.Fatalf("surviving node %d never applied post-failover index %d", n.id, index2)
		}
	}
}

func TestCluster_MinorityPartitionCannotMakeProgress(t *testing.T) {
	nodes := newTestCluster(t, 3)
	waitForSingleLeader(t, nodes, 5*time.Second)

	// Stop two of the three nodes, leaving a lone survivor — a minority.
	// Note: the survivor may or may not have been the leader before the
	// partition; by correct Raft semantics, a leader does NOT
	// automatically step down just because it can no longer reach a
	// majority of followers (IsLeader() can legitimately remain true for
	// a "stale" leader that has lost quorum). The property that actually
	// matters, and the one this test checks, is that a minority partition
	// can never get a NEW log entry committed and applied, regardless of
	// whether it locally believes it has a leader.
	var survivor *testNode
	for _, n := range nodes {
		if n == nodes[0] {
			survivor = n
			continue
		}
		n.rf.Stop()
	}

	lastApplied := survivor.rf.LastApplied()

	// Try to propose; this may succeed locally (if the survivor is/была
	// the leader, appending to its own log is always allowed) or fail
	// with ErrNotLeader (if it's a follower that can never win an
	// election alone). Either outcome is acceptable — what must NOT
	// happen is the entry actually committing and applying.
	_, _, _ = survivor.rf.Propose(context.Background(), []byte("should-never-commit"))

	time.Sleep(2 * time.Second)
	if survivor.rf.LastApplied() > lastApplied {
		t.Fatalf("minority partition advanced LastApplied from %d to %d without a quorum — split-brain commit",
			lastApplied, survivor.rf.LastApplied())
	}
}
