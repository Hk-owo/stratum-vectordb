package integration_test

import (
	"context"
	"fmt"
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

// startWatermarkServer hosts just the DataSyncService's report endpoint, wired the way
// a control leader wires it: it aggregates reports AND carries the reclaim watermarks
// back (§7.5). It is separate from a realNode because this case is about the watermark
// round trip, not about the whole node.
func startWatermarkServer(t *testing.T, nodeID int64, control plane.ControlPlane, isLeader func() bool) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	handler := stratumsync.NewPushHandler(nil, nodeID,
		stratumsync.WithDataVersionAggregator(discardRecorder{}, isLeader),
		stratumsync.WithReclaimWatermarks(control))
	pb.RegisterDataSyncServiceServer(srv, handler)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// discardRecorder satisfies the aggregator; this case drives the reclaim side.
type discardRecorder struct{}

func (discardRecorder) Record(int64, string, map[string]int64) {}

// KnowledgeBases is the aggregator's second half: the leader names every chain
// it knows about, so a reporter that missed one entirely still learns there is
// something to be behind on. This case discards everything, so it knows nothing.
func (discardRecorder) KnowledgeBases() []string { return nil }

// TestRealStack_NonLeaderWriterReclaimsItsWALAfterTheWatermarkComesBack is the point of
// the watermark round trip: the node whose WAL grows is the one that WROTE the data, and
// under §7.13.2 that need not be the leader. It therefore cannot compute the watermark
// locally — it learns it from the leader's response to the report it already sends.
//
// The case runs three real nodes to get a real leader and two real followers, then acts
// out exactly that shape on one follower: changes accumulate in its WAL, the watermark
// arrives by回传, and the committed-and-reclaimed versions must disappear from the log
// while newer ones stay.
func TestRealStack_NonLeaderWriterReclaimsItsWALAfterTheWatermarkComesBack(t *testing.T) {
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
		if err := writer.fileWAL.WriteVersionID(ctx, versionID); err != nil {
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
	if err := writer.fileWAL.WriteVersionID(ctx, v1+3); err != nil {
		t.Fatalf("WriteVersionID(v4): %v", err)
	}

	// --- The leader's side: it holds the reports and computes the watermark. ---
	leaderRegistry := plane.NewDataVersionRegistry()
	leaderGate := plane.NewLeaderGate(func() bool { return true }, leaderRegistry.Reset)
	leaderGate.IsLeader() // prime the takeover clear
	requiredIDs := []int64{n1.nodeID, n2.nodeID}
	leaderControl := plane.NewLocalControlPlane(leader.raftNode,
		plane.WithDataVersionView(leaderRegistry, leaderGate),
		plane.WithRequiredReplicas(func() ([]int64, error) { return requiredIDs, nil }))

	// Every replica reports a cursor that reaches v3 but not v4: so v3 and below are
	// reclaimable, v4 is not.
	for _, id := range requiredIDs {
		leaderRegistry.Record(id, fmt.Sprintf("10.0.0.%d:7000", id), map[string]int64{kbID: v1 + 2})
	}
	watermark, ok := leaderControl.ReclaimableChangesThrough(kbID)
	if !ok || watermark != v1+2 {
		t.Fatalf("leader watermark = (%d, %v), want (%d, true)", watermark, ok, v1+2)
	}

	addr := startWatermarkServer(t, leader.nodeID, leaderControl, func() bool { return true })

	// --- The writer's side: not the leader, so it cannot judge for itself. ---
	writerRegistry := plane.NewDataVersionRegistry()
	writerGate := plane.NewLeaderGate(func() bool { return false }, nil)
	writerControl := plane.NewLocalControlPlane(writer.raftNode,
		plane.WithDataVersionView(writerRegistry, writerGate))
	if _, ok := writerControl.ReclaimableChangesThrough(kbID); ok {
		t.Fatal("a non-leader must not be able to judge the watermark locally")
	}

	reporter := stratumsync.NewDataVersionReporter(stratumsync.DataVersionReporterConfig{
		NodeID:       writer.nodeID,
		DataVersions: func() map[string]int64 { return map[string]int64{kbID: v1 + 2} },
		ResolveLeader: func(context.Context) (string, bool, error) {
			return addr, true, nil
		},
		Watermarks: writerControl,
		Logger:     zap.NewNop(),
	})
	if err := reporter.ReportOnce(ctx); err != nil {
		t.Fatalf("ReportOnce: %v", err)
	}

	// After the round trip the writer CAN judge — from what the leader carried back.
	watermark, ok = writerControl.ReclaimableChangesThrough(kbID)
	if !ok || watermark != v1+2 {
		t.Fatalf("writer watermark after the round trip = (%d, %v), want (%d, true)", watermark, ok, v1+2)
	}

	// --- Reclaim, through the storage layer's own path. ---
	executor := &d2Executor{}
	dp := plane.NewLocalDataPlane(plane.LocalDataPlaneConfig{
		WAL:      writer.fileWAL,
		Executor: executor,
		Control:  writerControl,
		Logger:   zap.NewNop(),
	})
	// No cursor needs seeding: the reclaim scope comes from the WAL's own contents,
	// not from what this node tracks.
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
	for _, v := range []int64{v1 + 1, v1 + 2} {
		if _, ok, err := reopened.ChangesFor(ctx, kbID, v); err != nil {
			t.Fatalf("ChangesFor(v%d): %v", v, err)
		} else if ok {
			t.Errorf("v%d survived reclamation although every replica reported holding it", v)
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
