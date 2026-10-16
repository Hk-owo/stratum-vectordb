package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"stratum/internal/raft"
	"stratum/internal/sync"

	"go.uber.org/zap"

	pb "stratum/api/proto/stratum"
)

// waitUntil polls cond every 50ms until it holds or the timeout passes.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// wireSyncPullDataOnly is wireSyncPull with the §8.6b data-only pull: a node
// fetches the version's documents and chunks but never builds an index of its
// own, so its only route to an index is pulling the owner's artifact (§8.4) —
// which is exactly the artifact the cold reshape later replaces.
//
// The distinction matters for the cold-reshape test for a second reason: a node
// that builds on its own has its (kb, version) entry marked "loading" for as
// long as that build keeps retrying, and the cold policy deliberately leaves
// such a version alone (§8.6a: the build in flight decides its own shape). With
// data-only pulls the reshaping node is never stuck behind its own build.
func wireSyncPullDataOnly(t *testing.T, n *realNode, addrByID map[int64]string) {
	t.Helper()
	syncFollower := sync.NewFollower(n.docStore, n.chunkDoc, n.versionDoc, n.chunkStore, n.indexMgr)
	n.raftNode.SetOnVersionCreated(func(kbID string, versionID int64) {
		ctx := context.Background()
		// §7.5: the same announcement wireSyncPull makes. It matters more here:
		// the data-only variant pulls records without building an index, so the
		// cursor is the only thing that says this node holds the version.
		n.indexDistributor.AnnounceVersion(kbID, versionID)

		deadline := time.Now().Add(20 * time.Second)
		backoff := 50 * time.Millisecond
		for {
			if addr := leaderAddrOf(ctx, n, addrByID); addr != "" {
				_ = syncFollower.PullVersionData(ctx, addr, kbID, versionID)
			}
			// v1 arrives with the KB itself, so it needs only one pull.
			if versionID <= 1 || verifyFollowerPull(ctx, n, kbID, versionID) {
				return
			}
			if time.Now().After(deadline) {
				n.logger.Warn("data-only pull did not converge",
					zap.Int64("node_id", n.nodeID), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID))
				return
			}
			time.Sleep(backoff)
			if backoff < time.Second {
				backoff *= 2
			}
		}
	})
}

// TestRealStack_ColdRebuildReplacesOnlyTheLocalArtifact is the cluster-level proof
// of the last piece of Stratum_设计文档v13.md §8.6(a): a version that has gone
// cold is rebuilt in the graph-free shape, and §8.4's callback chain is what
// puts that rebuild through — no new channel.
//
// What the rebuild does NOT do, now that §8.4(a)'s presence probe has landed, is
// replace a replica's artifact. The probe answers "do you already have it", not
// "is yours the same shape" (docs/index-push-probe-plan.md §3, with the
// content-level tightening that would change that deliberately left to §10.1),
// so a replica already holding the version keeps its own copy. That is the trade
// the plan records as acceptable: the two shapes are retrieval-equivalent
// (§8.6a), so the cost is a resource-suboptimal (still graphed) artifact on the
// replica — never an unservable version.
//
// Only node 1 runs the cold policy (a non-zero threshold); node 2 never reshapes
// anything by itself.
// That is what makes the assertions meaningful in both directions: node 1's
// smaller file can only have come from the reshape, and node 2's UNCHANGED file
// can only have come from the probe having skipped it — node 2 has no other
// route to an artifact at all.
//
// The threshold is set well above the time the first distribution needs, so the
// phases are ordered: the version is graphed and distributed, then it ages
// (nobody queries it, which is exactly what makes it cold), then node 1 reshapes
// it.
func TestRealStack_ColdRebuildReplacesOnlyTheLocalArtifact(t *testing.T) {
	vecAddrs := [2]string{
		startVecstoreServerForTest(t),
		startVecstoreServerForTest(t),
	}
	// A few more dimensions than the other stacks use, so the graph the
	// reshape removes is a meaningful part of the file.
	embedURL := startMockEmbedForTest(t, 16)

	var raftAddrs, grpcAddrs [2]string
	for i := range raftAddrs {
		raftAddrs[i] = freeLoopbackAddr(t)
		grpcAddrs[i] = freeLoopbackAddr(t)
	}
	peers := []raft.PeerConfig{
		{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
		{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
	}
	addrByID := map[int64]string{1: grpcAddrs[0], 2: grpcAddrs[1]}

	baseDirs := [2]string{t.TempDir(), t.TempDir()}
	var nodes [2]*realNode
	for i := 0; i < 2; i++ {
		cfg := realNodeConfig{}
		if i == 0 {
			// Only node 1 reshapes; see the doc comment. The threshold is
			// generous enough that the first distribution is long settled
			// before anything is considered cold.
			cfg.coldThreshold = 10 * time.Second
		}
		nodes[i] = newRealNodeWithAddrsAndDirOpts(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], baseDirs[i], cfg)
		wireSyncPullDataOnly(t, nodes[i], addrByID)
	}

	ctx := context.Background()
	leader := waitForLeader(t, nodes[0], nodes[1])
	t.Logf("leader = node %d", leader.nodeID)

	kbID, v1 := leader.createTestKB(ctx, "cold-reshape-e2e")
	changes := make([]*pb.DocChange, 0, 20)
	for i := 0; i < 20; i++ {
		changes = append(changes, &pb.DocChange{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   fmt.Sprintf("doc-%d", i),
			Content: fmt.Sprintf("alpha beta gamma delta %d", i),
		})
	}
	resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes:         changes,
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	versionID := resp.GetVersionId()
	t.Logf("created %s v%d", kbID, versionID)

	// Node 1 ends up holding an artifact, by either of the two legitimate routes,
	// and the test does not care which: it builds one if it was the node the
	// request reached, and fetches a peer's copy otherwise. Nothing pushes it —
	// distribution is pull-only, and the router decides which nodes are asked at
	// all (§8.4 owner sets).
	//
	// The shared helper waits for either route; which one applies to node 1
	// depends on routing and on who built first, and this test does not need to
	// know.
	//
	// However it arrived, this is also where the cold policy starts counting for
	// node 1: the artifact has just appeared here, so the threshold is ahead.
	nodes[0].waitVersionReady(ctx, kbID, versionID)
	graphed, _ := indexFileOf(baseDirs[0], kbID, versionID)
	// NO byte-for-byte comparison against node 2 here any more. It used to hold
	// (and to be the §8.4 evidence) while a build PUSHED its artifact; with
	// distribution pull-only, node 1's copy is node 2's only when node 1 fetched
	// it, which is one of two legitimate outcomes. The assertion that still
	// matters is later: the reshape must change node 1's artifact and only
	// node 1's.
	t.Logf("graphed index on node 1: %d bytes", len(graphed))

	// §8.6a: the SHAPE is chosen when a build happens, from how long this node has
	// gone without querying the version. A reshape is therefore not something a
	// background timer does — it is what the NEXT build of an idle version looks
	// like. Two things follow for this test:
	//
	//   - the version must sit idle for longer than the threshold (10 s here), which
	//     is real time the test has to spend;
	//   - it must then be BUILT again, which means asking for it.
	//
	// Nothing shields this version: the active pointer is only ever set by
	// RollbackVersion, and this stack never rolls back.
	time.Sleep(12 * time.Second)

	// Ask for it the way a caller would. The build that follows reads the access
	// record BEFORE this query is stamped, so it sees the twelve idle seconds and
	// chooses the graph-free shape.
	vec := make([]float32, 16)
	vec[0] = 1
	nodes[0].queryTolerant(ctx, kbID, versionID, vec, 5)

	// The artifact must differ from, and be smaller than, the graphed one: what the
	// reshape drops is precisely the HNSW graph (edges + level arrays), which the
	// graph-free form does not carry.
	if !waitUntil(90*time.Second, func() bool {
		a, _ := indexFileOf(baseDirs[0], kbID, versionID)
		return len(a) > 0 && !bytes.Equal(a, graphed) && len(a) < len(graphed)
	}) {
		now, _ := indexFileOf(baseDirs[0], kbID, versionID)
		other, _ := indexFileOf(baseDirs[1], kbID, versionID)
		t.Fatalf("node 1 never rebuilt the idle version into a smaller artifact: n1=%d n2=%d graphed=%d",
			len(now), len(other), len(graphed))
	}
	reshaped, _ := indexFileOf(baseDirs[0], kbID, versionID)
	t.Logf("reshaped index: %d bytes (graphed was %d)", len(reshaped), len(graphed))

	// The reshape stays node 1's, and it stays there for a structural reason
	// rather than a probe answer: nothing pushes a rebuilt artifact any more.
	//
	// Node 2 is data-only (wireSyncPullDataOnly): it never builds and never
	// reshapes. So its artifact can only be one of two things — the copy it
	// fetched from node 1 before the reshape (when node 1 was the builder), or
	// nothing at all (when node 2 was the builder and node 1 fetched it). What it
	// must never be is the RESHAPED bytes.
	if !waitUntil(30*time.Second, func() bool {
		a, _ := indexFileOf(baseDirs[0], kbID, versionID)
		return bytes.Equal(a, reshaped)
	}) {
		a, _ := indexFileOf(baseDirs[0], kbID, versionID)
		t.Fatalf("node 1 never settled on the reshaped artifact: n1=%d reshaped=%d", len(a), len(reshaped))
	}
	node2, _ := indexFileOf(baseDirs[1], kbID, versionID)
	switch {
	case bytes.Equal(node2, reshaped):
		t.Errorf("node 2 holds the RESHAPED artifact (%d bytes): a rebuilt artifact must not leave the node that rebuilt it", len(node2))
	case len(node2) == 0:
		t.Logf("node 2 holds no artifact for this version — it never built one, and nothing pushed one to it")
	default:
		t.Logf("node 2 kept the artifact it had (%d bytes), not the reshaped one", len(node2))
	}

	// The point of letting the shapes differ is that it costs nothing a reader can
	// see: §8.6a makes the two retrieval-equivalent, so both nodes must still
	// answer for the reshaped version — one from the graph-free artifact it built,
	// the other from whatever it has (its own copy, or a lazy build on the spot).
	for _, n := range nodes {
		results := n.queryTolerant(ctx, kbID, versionID, vec, 5)
		if len(results) == 0 {
			t.Errorf("node %d returned no results for the reshaped version", n.nodeID)
		} else {
			t.Logf("node %d answered %d results after the reshape (top doc %s)", n.nodeID, len(results), results[0].DocId)
		}
	}
}
