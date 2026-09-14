package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
)

// TestRealStack_ThreeNodeCluster_NonLeaderRefusesTheWrite pins §7.13.2's
// admission rule on a real three-node stack: CreateVersion is leader-bound, and
// a client that reaches a follower *directly* is answered with "not leader" so it
// re-resolves the leader (which is exactly what the router does for it) instead
// of the system growing a second, client-side forwarding hop.
//
// The leader's own write, right after, is the control case: it succeeds, its
// data lands on the leader, and every node converges on it. This stack wires the
// coordinator WITHOUT a dispatcher (`WriteCoordinatorConfig.Dispatch == nil`), so
// the leader coordinates the write itself — the pre-§7.13.2 shape that the
// configuration deliberately keeps working. An end-to-end assertion of the
// *dispatched* path (leader picks a replica, that replica writes) needs a stack
// whose write path runs through plane.LocalDataPlane; this stack still writes to
// the stores directly.
func TestRealStack_ThreeNodeCluster_NonLeaderRefusesTheWrite(t *testing.T) {
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

	kbID, v1 := leader.createTestKB(ctx, "non-leader-refused")

	// The assertion: a write sent straight to a follower is refused, and refused
	// in the way that tells the client to re-resolve the leader.
	_, err := follower.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
		},
	})
	if err == nil {
		t.Fatalf("node %d (a follower) must refuse a directly-sent write: a non-leader cannot put it in the log",
			follower.nodeID)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "not leader") {
		t.Errorf("refusal = %v, want it to say the node is not the leader so the client re-resolves", err)
	}
	t.Logf("follower node %d refused the write as expected: %v", follower.nodeID, err)

	// The control case: the same write through the leader succeeds, its data
	// lands there (no dispatcher is wired in this stack), and every node
	// converges on it.
	resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "beta"},
		},
	})
	if err != nil {
		t.Fatalf("the leader must accept the same write: %v", err)
	}
	v2 := resp.VersionId

	leaderDocs, err := leader.versionDoc.ListDocIDs(ctx, kbID, v2)
	if err != nil {
		t.Fatalf("leader: ListDocIDs(%s, v%d): %v", kbID, v2, err)
	}
	if len(leaderDocs) == 0 {
		t.Fatalf("with no dispatcher wired, the leader must coordinate the write itself (v%d has no docs)", v2)
	}

	for _, n := range nodes {
		waitForVersionData(t, n, kbID, v2)
	}
	for _, n := range nodes {
		if r := waitQueryDoc(t, n, kbID, v2, contentVector("alpha", 4), "doc-1"); r.Content != "alpha" {
			t.Errorf("node %d: doc-1 content = %q, want alpha", n.nodeID, r.Content)
		}
		if r := waitQueryDoc(t, n, kbID, v2, contentVector("beta", 4), "doc-2"); r.Content != "beta" {
			t.Errorf("node %d: doc-2 content = %q, want beta", n.nodeID, r.Content)
		}
	}
}

// waitForVersionData polls until n holds versionID's document set, using the
// same check the sync path itself uses (verifyFollowerPull). Keeping it separate
// from the query assertion separates two different failures: "the data never
// arrived" from "the index never became queryable".
func waitForVersionData(t *testing.T, n *realNode, kbID string, versionID int64) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for {
		if verifyFollowerPull(context.Background(), n, kbID, versionID) {
			return
		}
		if time.Now().After(deadline) {
			docs, err := n.versionDoc.ListDocIDs(context.Background(), kbID, versionID)
			t.Fatalf("node %d never received v%d's data: %d docs, err %v", n.nodeID, versionID, len(docs), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
