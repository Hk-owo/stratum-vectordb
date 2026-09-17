package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/raft"
	"stratum/internal/wal"
)

// waitUntilHolds polls until n's own stores contain versionID's data, judged the
// same way the rest of this stack judges it (the committed digest, or a non-empty
// document set when no digest was committed).
func waitUntilHolds(t *testing.T, n *realNode, kbID string, versionID int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if verifyFollowerPull(context.Background(), n, kbID, versionID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d never came to hold %s v%d", n.nodeID, kbID, versionID)
}

// waitForCursorRecord polls until the node's WAL records a cursor of at least
// versionID for kbID. The advance is asynchronous by design — the cursor's owner
// must never fsync under the lock its readers take — so a test that is about to
// kill the process has to wait for the record rather than assume it.
func waitForCursorRecord(t *testing.T, w *wal.FileWAL, kbID string, versionID int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		cursors, err := w.RecoverCursors(context.Background())
		if err != nil {
			t.Fatalf("RecoverCursors: %v", err)
		}
		got = cursors[kbID]
		if got >= versionID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the WAL never recorded a cursor of %d for %s (last read: %d)", versionID, kbID, got)
}

// waitUntilMetadataVisible polls until the restarted node's own metadata lists
// kbID. A restarted Raft node replays its log asynchronously, and the startup step
// below walks the knowledge bases the control layer knows about — so reading the
// cursor back before that would be reading it against an empty list.
func waitUntilMetadataVisible(t *testing.T, n *realNode, kbID string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if kbs, err := n.raftNode.ListKnowledgeBases(ctx); err == nil {
			for _, kb := range kbs {
				if kb.KBID == kbID {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d never saw %s in its metadata after the restart", n.nodeID, kbID)
}

// TestRealStack_CursorIsRightImmediatelyAfterARestartWithEveryArtifactDeleted is
// docs/cursor-persistence-plan.md §8's cluster case, and the "变体" of the dec22a0
// scenario the plan asks for: a replica is killed, the majority moves on without
// it, EVERY index artifact it has is deleted, and it is brought back — its cursor
// must be right the moment it starts, with no query, no pull and no lag-catchup
// cycle.
//
// What makes it the acceptance case for §4.3: an artifact is a CACHE (retention
// deletes it on purpose, and a build happens lazily), so a cursor inferred from
// one understates what the node holds. Before the persisted cursor, the node below
// came back reporting version 0 for a knowledge base whose data it held in full,
// which kept it out of the station's holder set and out of the route it should have
// been serving.
func TestRealStack_CursorIsRightImmediatelyAfterARestartWithEveryArtifactDeleted(t *testing.T) {
	// One vecstore subprocess per node (independent RocksDB), one shared mock
	// embed server — the same stack the other real-stack cases run on.
	vecAddrs := [3]string{}
	for i := range vecAddrs {
		vecAddrs[i] = startVecstoreServerForTest(t)
	}
	embedURL := startMockEmbedForTest(t, 4)

	var raftAddrs, grpcAddrs, baseDirs [3]string
	for i := 0; i < 3; i++ {
		raftAddrs[i] = freeLoopbackAddr(t)
		grpcAddrs[i] = freeLoopbackAddr(t)
		baseDirs[i] = filepath.Join(t.TempDir(), "node")
	}
	peers := []raft.PeerConfig{
		{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
		{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
		{ID: 3, RaftAddr: raftAddrs[2], ServiceAddr: grpcAddrs[2]},
	}
	addrByID := map[int64]string{1: grpcAddrs[0], 2: grpcAddrs[1], 3: grpcAddrs[2]}

	nodes := [3]*realNode{}
	for i := 0; i < 3; i++ {
		nodes[i] = newRealNodeWithAddrsAndDir(t, int64(i+1), peers, vecAddrs[i], embedURL,
			raftAddrs[i], grpcAddrs[i], baseDirs[i])
		wireSyncPull(t, nodes[i], addrByID)
	}
	ctx := context.Background()
	leader := waitForLeader(t, nodes[0], nodes[1], nodes[2])
	t.Logf("leader = node %d", leader.nodeID)

	kbID, v1 := leader.createTestKB(ctx, "cursor-restart")

	write := func(parent int64, doc string) int64 {
		t.Helper()
		resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
			KnowledgeBaseId: kbID,
			ParentVersionId: parent,
			Changes: []*pb.DocChange{
				{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: doc, Content: doc + "-content"},
			},
		})
		if err != nil {
			t.Fatalf("CreateVersion(%s): %v", doc, err)
		}
		leader.waitVersionReady(ctx, kbID, resp.VersionId)
		return resp.VersionId
	}
	v2 := write(v1, "doc-a")
	v3 := write(v2, "doc-b")

	// The replica under test is a follower, and it must really hold the run before
	// it dies — otherwise its cursor would legitimately be lower and the test would
	// prove nothing.
	var victim *realNode
	for _, n := range nodes {
		if n.nodeID != leader.nodeID {
			victim = n
			break
		}
	}
	waitUntilHolds(t, victim, kbID, v3)
	// And it has written that fact down: the advance is asynchronous, and the run
	// below is about what a RESTART reads back.
	waitForCursorRecord(t, victim.fileWAL, kbID, v3)
	t.Logf("node %d holds up to v%d and has recorded it", victim.nodeID, v3)

	// Kill it, then let the majority move on without it.
	victim.Stop()
	v4 := write(v3, "doc-c")
	v5 := write(v4, "doc-d")
	if v5 <= v4 {
		t.Fatalf("version ids must advance: v4=%d v5=%d", v4, v5)
	}
	leader.waitVersionReady(ctx, kbID, v5)

	// Delete every artifact this replica has. Retention does exactly this to a
	// version it stops keeping (EnforceDiskRetention drops the <v>.index files), so
	// this is an ordinary state for a node to be restarted in — and it is the state
	// the old judgement read as "this node holds nothing".
	artifactDir := filepath.Join(victim.baseDir, "indexdata", "index", kbID)
	if _, err := os.Stat(artifactDir); err != nil {
		t.Fatalf("precondition: the killed replica should have artifacts on disk: %v", err)
	}
	if err := os.RemoveAll(artifactDir); err != nil {
		t.Fatalf("delete the replica's artifacts: %v", err)
	}

	// Bring it back on the SAME disk (same baseDir, same addresses). Deliberately
	// NOT wired for pulls: this case is about the startup step, and a pull landing
	// first would move the cursor forward and hide whether the record was read back.
	idx := int(victim.nodeID - 1)
	restarted := newRealNodeWithAddrsAndDir(t, victim.nodeID, peers, vecAddrs[idx], embedURL,
		raftAddrs[idx], grpcAddrs[idx], baseDirs[idx])
	defer restarted.Stop()

	// The startup step production runs (cmd/stratum's reconcileIndexStatus): read
	// the cursor back. It is the very step that used to consult the artifacts.
	waitUntilMetadataVisible(t, restarted, kbID)
	if err := restarted.indexDistributor.RecoverLocalCursors(ctx, restarted.raftNode); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	// A read-back, not an inference: the record is in the node's own log. Asserted
	// first so a failure says which half broke — the log or the reader.
	cursors, err := restarted.fileWAL.RecoverCursors(ctx)
	if err != nil {
		t.Fatalf("RecoverCursors: %v", err)
	}
	if cursors[kbID] != v3 {
		t.Fatalf("WAL cursor for %s = %d (all records: %v), want %d", kbID, cursors[kbID], cursors, v3)
	}

	if got := restarted.indexDistributor.LocalVersionOf(kbID); got != v3 {
		t.Fatalf("cursor immediately after the restart = %d, want %d: its own WAL records v%d, and every artifact it could be inferred from is gone",
			got, v3, v3)
	}

	// Not a vacuous pass: the OLD judgement really does answer "no artifact" for
	// every version now. This asserts through the same call the old inference made
	// (IndexExists → vecstore), which is what makes the cursor above the thing being
	// tested rather than an artefact of files that are still there.
	for _, v := range []int64{v1, v2, v3} {
		exists, err := restarted.indexMgr.IndexExists(ctx, kbID, v)
		if err != nil {
			t.Fatalf("IndexExists(%s, %d): %v", kbID, v, err)
		}
		if exists {
			t.Errorf("v%d's artifact is still on disk: the case would then pass without proving anything", v)
		}
	}

	// The cursor is also exactly what the station's freshness check reads (§9.3(2)
	// reads the same value through the data plane): it reaches the version this
	// replica holds, so the replica is routable instead of being refused as stale —
	// which is what "immediately correct" buys, without waiting for a catch-up round.
	if got, want := restarted.indexDistributor.LocalVersionOf(kbID), v3; got < want {
		t.Errorf("freshness: local history reaches version %d, below the required %d", got, want)
	}
}
