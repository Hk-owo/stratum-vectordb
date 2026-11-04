package integration_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "stratum/api/proto/stratum"
	"stratum/internal/plane"
	"stratum/internal/raft"
	stratumsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// startCursorServer hosts just the DataSyncService's cursor endpoint, the way a replica
// answers "how far has your history reached".
func startCursorServer(t *testing.T, nodeID int64, cursors stratumsync.LocalVersionReporter) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterDataSyncServiceServer(srv, stratumsync.NewPushHandler(nil, nodeID, stratumsync.WithLocalVersion(cursors)))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// fixedCursor is a replica whose history reached a fixed version.
type fixedCursor int64

func (c fixedCursor) LocalVersionOf(string) int64 { return int64(c) }

// TestRealStack_NonLeaderWriterReclaimsItsWALFromItsPeersCursors: the node whose WAL grows
// is the one that WROTE the data, and under §7.13.2 that need not be the leader. It asks
// the replicas how far they have reached over a real gRPC connection, and the
// committed-and-reclaimed versions must disappear from the log while newer ones stay.
func TestRealStack_NonLeaderWriterReclaimsItsWALFromItsPeersCursors(t *testing.T) {
	// Two nodes are enough: one leads, one is "not the leader". Keeping this case light
	// matters — it competes for vecstore subprocesses and ports with every other
	// integration case, and an oversized fixture makes the whole package flaky.
	vec1, vec2 := startVecstoreServerForTest(t), startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, 4)

	raftAddrs := []string{freeLoopbackAddr(t), freeLoopbackAddr(t)}
	grpcAddrs := []string{freeLoopbackAddr(t), freeLoopbackAddr(t)}
	peersFor := func(int64) []raft.PeerConfig {
		return []raft.PeerConfig{
			{ID: 1, RaftAddr: raftAddrs[0], ServiceAddr: grpcAddrs[0]},
			{ID: 2, RaftAddr: raftAddrs[1], ServiceAddr: grpcAddrs[1]},
		}
	}
	n1 := newRealNodeWithAddrs(t, 1, peersFor(1), vec1, embedURL, raftAddrs[0], grpcAddrs[0])
	n2 := newRealNodeWithAddrs(t, 2, peersFor(2), vec2, embedURL, raftAddrs[1], grpcAddrs[1])

	leader := waitForLeader(t, n1, n2)
	var writer *realNode
	if leader == n1 {
		writer = n2
	} else {
		writer = n1
	}

	ctx := context.Background()
	// The WAL records below are the whole subject of this case, and reclaiming them has
	// nothing to do with whether the metadata knows the knowledge base — so the ID can
	// be a plain string and no knowledge base needs creating.
	const kbID = "reclaim-watermark"
	const v1 = int64(1)

	// The writing node's own WAL accumulates changes for this KB: v2 and v3 committed,
	// v4 uncommitted (the crash-recovery case that must SURVIVE reclamation).
	begin := func(parent int64, docID string) {
		t.Helper()
		if err := writer.fileWAL.WriteBegin(ctx, kbID, parent, []types.DocChange{
			{Op: types.ChangeOpAdd, DocID: docID, Content: docID},
		}); err != nil {
			t.Fatalf("WriteBegin(%s): %v", docID, err)
		}
	}
	commit := func(versionID int64) {
		t.Helper()
		if err := writer.fileWAL.WriteVersionID(ctx, kbID, versionID); err != nil {
			t.Fatalf("WriteVersionID(%d): %v", versionID, err)
		}
		if err := writer.fileWAL.WriteCommit(ctx, versionID); err != nil {
			t.Fatalf("WriteCommit(%d): %v", versionID, err)
		}
	}
	begin(v1, "doc-2")
	commit(v1 + 1)
	begin(v1+1, "doc-3")
	commit(v1 + 2)
	// v4: BEGIN and VERSION_ID, never committed.
	begin(v1+2, "doc-4")
	if err := writer.fileWAL.WriteVersionID(ctx, kbID, v1+3); err != nil {
		t.Fatalf("WriteVersionID(v4): %v", err)
	}

	// The peer's history reaches v3 but not v4: so v3 and below are reclaimable, v4 is not.
	peer := startCursorServer(t, leader.nodeID, fixedCursor(v1+2))

	// --- Reclaim, through the storage layer's own path. ---
	executor := &d2Executor{}
	dp := plane.NewLocalDataPlane(plane.LocalDataPlaneConfig{
		WAL:             writer.fileWAL,
		Executor:        executor,
		CursorQuerier:   stratumsync.NewLocalVersionQuerier(stratumsync.PresenceCheckerConfig{}),
		ResolveReplicas: func(context.Context) ([]string, error) { return []string{peer}, nil },
		Logger:          zap.NewNop(),
	})
	// This node's own history reaches further than the peer's, so the peer decides.
	dp.MarkVersionContiguous(kbID, v1+3)
	used, err := dp.ReclaimChanges(ctx)
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if used[kbID] != v1+2 {
		t.Fatalf("watermarks used = %v, want %s:%d", used, kbID, v1+2)
	}

	// The reclaimed, committed versions are GONE from the log — a missing key, which is
	// what tells a lagging peer to transfer full state (§7.5). Reopening is deliberate:
	// the in-memory index must not be the thing under test.
	reopened, err := wal.NewFileWAL(filepath.Join(writer.baseDir, "wal"))
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	reclaimed, err := reopened.ChangesInRange(ctx, kbID, v1, v1+2)
	if err != nil {
		t.Fatalf("ChangesInRange: %v", err)
	}
	for _, v := range []int64{v1 + 1, v1 + 2} {
		if _, ok := reclaimed[v]; ok {
			t.Errorf("v%d survived reclamation although every replica holds it", v)
		}
	}
	// The uncommitted one MUST survive: its replay input is the only repair for a crash.
	// Recover is the right probe — it reports exactly the flows that have no COMMIT,
	// which is what compaction must never drop.
	pending, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	found := false
	for _, rec := range pending {
		if rec.VersionID == v1+3 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("the uncommitted v%d was reclaimed; the crash can no longer be repaired (Recover saw %v)", v1+3, pending)
	}
}
