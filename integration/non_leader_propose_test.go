package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/kvraft"
	"stratum/internal/raft"
	"stratum/internal/types"
)

// The two cases below cover propose *forwarding* (Stratum_设计文档v13.md §7.3): a
// node that is not the leader initiates a proposal through its own in-process
// RaftNode, and the entry still reaches the replicated log.
//
// That capability is production wiring, not a leftover of the reverted "any node
// can be the coordinator" experiment: cmd/stratum/main.go installs a
// raft.GRPCProposeForwarder, and internal/raft documents that without it a
// non-leader proposal fails outright. What that experiment would have changed is
// *who coordinates a write*, not whether a proposal can be forwarded.
//
// The calls are deliberately in-process rather than gRPC writes: a gRPC write
// would be carried to the leader by internal/router's interceptor, which is
// exactly the path these tests must bypass to observe the forward itself.

// waitForLeaderView polls until n's own raft view names leaderID as the leader.
// A proposer that has not learned the leader yet has nowhere to forward to and
// answers kvraft.ErrNotLeader — the election/heartbeat window. Waiting that view
// out removes the flakiness at its origin, which is what lets the assertions
// below stay strict instead of tolerating "not leader".
func waitForLeaderView(t *testing.T, n *realNode, leaderID int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, err := n.raftNode.GetClusterStatus(context.Background())
		if err == nil && st.HasLeader && st.LeaderID == leaderID {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d never learned that node %d is the leader", n.nodeID, leaderID)
}

// proposeFromNonLeader runs propose, retrying only while the node still has
// nowhere to forward to (kvraft.ErrNotLeader, which a mid-test leader change can
// re-introduce). Every other outcome — success, or a state-machine sentinel — is
// returned immediately, so the caller's assertion is what decides.
func proposeFromNonLeader(t *testing.T, propose func(context.Context) error) error {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := propose(context.Background())
		if !errors.Is(err, kvraft.ErrNotLeader) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRealStack_ThreeNodeCluster_NonLeaderCanPropose proves the forward carries
// a *successful* proposal from a non-leader all the way into every replica's
// state machine.
func TestRealStack_ThreeNodeCluster_NonLeaderCanPropose(t *testing.T) {
	vecAddrs := [3]string{}
	for i := range vecAddrs {
		vecAddrs[i] = startVecstoreServerForTest(t)
	}
	embedURL := startMockEmbedForTest(t, 4)

	var raftAddrs, grpcAddrs [3]string
	for i := 0; i < 3; i++ {
		raftAddrs[i] = freeLoopbackAddr(t)
		grpcAddrs[i] = freeLoopbackAddr(t)
	}

	peers := []raft.PeerConfig{
		{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
		{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
		{ID: 3, RaftAddr: raftAddrs[2], ServiceAddr: grpcAddrs[2]},
	}
	addrByID := map[int64]string{1: grpcAddrs[0], 2: grpcAddrs[1], 3: grpcAddrs[2]}

	var nodes [3]*realNode
	for i := 0; i < 3; i++ {
		nodes[i] = newRealNodeWithAddrsAndDir(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], t.TempDir())
		wireSyncPull(t, nodes[i], addrByID)
		wireProposeForwarding(t, nodes[i], addrByID)
	}

	ctx := context.Background()
	leader := waitForLeader(t, nodes[0], nodes[1], nodes[2])
	t.Logf("initial leader = node %d", leader.nodeID)

	// Pick a node that is definitely not the leader.
	var follower *realNode
	for _, n := range nodes {
		if n.nodeID != leader.nodeID {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no non-leader node found in a three-node cluster")
	}
	waitForLeaderView(t, follower, leader.nodeID)
	t.Logf("proposing from follower node %d", follower.nodeID)

	// In-process propose from the follower. Without the forward this is exactly
	// kvraft.ErrNotLeader — the wall that made a non-leader's reports
	// un-reportable.
	const kbID = "kb-non-leader-propose"
	err := proposeFromNonLeader(t, func(ctx context.Context) error {
		return follower.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
			KBID: kbID, Name: kbID, Status: types.KBStatusActive,
		})
	})
	if err != nil {
		t.Fatalf("a non-leader proposal must succeed through the forward, got: %v", err)
	}

	// Every node must converge on it — that is what distinguishes a real log
	// entry from a local figment.
	//
	// Poll rather than read once: a forwarded proposal waits on the *leader's*
	// apply, so the proposer's own state machine is still catching up when the
	// call returns. Each node gets its own budget (one shared deadline would let
	// a slow first node eat the others' time).
	for _, n := range nodes {
		deadline := time.Now().Add(10 * time.Second)
		var lastErr error
		for {
			kb, err := n.raftNode.GetKB(ctx, kbID)
			if err == nil && kb.KBID == kbID {
				lastErr = nil
				break
			}
			lastErr = err
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if lastErr != nil {
			t.Fatalf("node %d never saw the forwarded knowledge base: %v", n.nodeID, lastErr)
		}
	}
}

// TestRealStack_ThreeNodeCluster_ForwardedErrorIsFaithful proves the forward
// carries the leader's *error* back faithfully, not just its successes.
// Proposing a status update for a version that does not exist is a natural
// sentinel-producing case: the state machine answers ErrVersionNotFound
// (internal/raft/state_machine.go), and that sentinel has to survive the trip
// over gRPC (internal/errors carries sentinels by name).
//
// The assertion is the point of the test: "some error came back" would also be
// satisfied by a forward that never worked at all, so the sentinel itself is
// required.
func TestRealStack_ThreeNodeCluster_ForwardedErrorIsFaithful(t *testing.T) {
	vecAddrs := [3]string{}
	for i := range vecAddrs {
		vecAddrs[i] = startVecstoreServerForTest(t)
	}
	embedURL := startMockEmbedForTest(t, 4)

	var raftAddrs, grpcAddrs [3]string
	for i := 0; i < 3; i++ {
		raftAddrs[i] = freeLoopbackAddr(t)
		grpcAddrs[i] = freeLoopbackAddr(t)
	}

	peers := []raft.PeerConfig{
		{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
		{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
		{ID: 3, RaftAddr: raftAddrs[2], ServiceAddr: grpcAddrs[2]},
	}
	addrByID := map[int64]string{1: grpcAddrs[0], 2: grpcAddrs[1], 3: grpcAddrs[2]}

	var nodes [3]*realNode
	for i := 0; i < 3; i++ {
		nodes[i] = newRealNodeWithAddrsAndDir(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], t.TempDir())
		wireSyncPull(t, nodes[i], addrByID)
		wireProposeForwarding(t, nodes[i], addrByID)
	}

	leader := waitForLeader(t, nodes[0], nodes[1], nodes[2])

	var follower *realNode
	for _, n := range nodes {
		if n.nodeID != leader.nodeID {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no non-leader node found")
	}
	waitForLeaderView(t, follower, leader.nodeID)

	// A version of a knowledge base that does not exist: the state machine
	// answers with a sentinel, and the forward has to deliver that sentinel —
	// not a generic transport error, and not "not leader".
	err := proposeFromNonLeader(t, func(ctx context.Context) error {
		return follower.raftNode.ProposeUpdateVersionStatus(ctx, 4242, types.IndexStatusReady)
	})
	t.Logf("forwarded rejection (as the caller sees it): %v", err)
	if !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Fatalf("want the state machine's sentinel ErrVersionNotFound through the forward, got: %v", err)
	}
}
