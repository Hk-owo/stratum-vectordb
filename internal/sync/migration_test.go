package sync

import (
	"context"
	"errors"
	pb "stratum/api/proto/stratum"
	"sync/atomic"
	"testing"
	"time"

	serrors "stratum/internal/errors"
	"stratum/internal/raft"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

func prepareMigrationBackend(t *testing.T) (*raft.MockRaftNode, storage.MigrationRequest) {
	t.Helper()
	r := raft.NewMockRaftNode(wal.NewMockWAL())
	ctx := context.Background()
	if err := r.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}, {ID: 4, Addr: "s4"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: "A"}, []int64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	return r, storage.MigrationRequest{OperationID: "move", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}
}

func TestMigrationFenceRPCDrainsPersistsAndRejectsStaleMetadataAfterRestart(t *testing.T) {
	r, req := prepareMigrationBackend(t)
	ctx := context.Background()
	before, err := r.GetKBPlacement(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	gate, err := storage.NewExecutionGate(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := gate.Enter(ctx, "A", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, _, addr := startPushServer(t, 3, WithStoragePlacement(r.GetKBPlacement), WithExecutionGate(gate, r))
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	tasks, _ := r.ListKBMigrations(ctx)
	task := tasks[0]
	client := &FenceClient{}
	defer client.Close()
	done := make(chan error, 1)
	go func() { done <- client.Fence(ctx, storage.Node{ID: 3, Addr: addr}, task) }()
	select {
	case err := <-done:
		t.Fatal("RPC acknowledged active writer:", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fence RPC did not drain")
	}
	// Even a restarted process reading OLD metadata must honor its disk fence.
	restarted, err := storage.NewExecutionGate(dir)
	if err != nil {
		t.Fatal(err)
	}
	stale := func(context.Context, string) (storage.Snapshot, error) { return before, nil }
	_, _, staleAddr := startPushServer(t, 3, WithStoragePlacement(stale), WithExecutionGate(restarted, r))
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	pusher := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 2})
	if _, err := pusher.PushVersion(storage.Bind(ctx, before), staleAddr, "A", 7); serrors.ReasonOf(err) != "kb_write_paused" {
		t.Fatal("restart with lagging metadata accepted old epoch:", err)
	}
	if err := client.Fence(ctx, storage.Node{ID: 4, Addr: addr}, task); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("wrong node fence ACK accepted:", err)
	}
	if err := r.ProposeCancelKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	now, err := r.GetKBPlacement(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	_, _, resumedAddr := startPushServer(t, 3, WithStoragePlacement(r.GetKBPlacement), WithExecutionGate(restarted, r))
	if _, err := pusher.PushVersion(storage.Bind(ctx, now), resumedAddr, "A", 7); err != nil {
		t.Fatal("cancelled task did not resume at a new epoch:", err)
	}
	if err := client.Fence(ctx, storage.Node{ID: 3, Addr: addr}, task); serrors.ReasonOf(err) != "migration_conflict" {
		t.Fatal("late old task was not isolated:", err)
	}
}

func TestMigrationFenceLearnerRemainsOutsideDataReplicaSet(t *testing.T) {
	r, req := prepareMigrationBackend(t)
	ctx := context.Background()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, _, addr := startPushServer(t, 4, WithStoragePlacement(r.GetKBPlacement), WithExecutionGate(gate, r))
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	tasks, _ := r.ListKBMigrations(ctx)
	client := &FenceClient{}
	defer client.Close()
	if err := client.Fence(ctx, storage.Node{ID: 4, Addr: addr}, tasks[0]); err != nil {
		t.Fatal("learner cannot prepare its fence:", err)
	}
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	snap, _ := r.GetKBPlacement(ctx, "A")
	if _, err := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 2}).PushVersion(storage.Bind(ctx, snap), addr, "A", 1); serrors.ReasonOf(err) != "not_kb_replica" {
		t.Fatal("preparation activated learner:", err)
	}
}

type migrationDropRecorder struct{ calls atomic.Int32 }

func (d *migrationDropRecorder) DropVersionStorage(context.Context, string, int64) error {
	d.calls.Add(1)
	return nil
}

func TestMigrationRemoteMutationRequiresEpochAndRejectsLateCleanup(t *testing.T) {
	r, req := prepareMigrationBackend(t)
	ctx := context.Background()
	before, _ := r.GetKBPlacement(ctx, "A")
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	drop := &migrationDropRecorder{}
	_, _, addr := startPushServer(t, 3, WithStoragePlacement(r.GetKBPlacement), WithExecutionGate(gate, r), WithVersionDataDropper(drop))
	source, _, _ := startLeaderServer(t, newFakeVecstore())
	pusher := NewPusher(PusherConfig{Exporter: source.handler, NodeID: 2})
	if _, err := pusher.PushVersion(ctx, addr, "A", 1); serrors.ReasonOf(err) != "placement_stale" {
		t.Fatal("missing epoch mutation was accepted:", err)
	}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCancelKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	client := dialSyncServer(t, addr)
	if _, err := client.DeleteVersionData(storage.Bind(ctx, before), &pb.DeleteVersionDataRequest{KnowledgeBaseId: "A", VersionId: 1}); serrors.ReasonOf(err) != "placement_stale" {
		t.Fatal("late cleanup borrowed new epoch:", err)
	}
	if drop.calls.Load() != 0 {
		t.Fatal("late cleanup reached physical dropper")
	}
	now, _ := r.GetKBPlacement(ctx, "A")
	if _, err := client.DeleteVersionData(storage.Bind(ctx, now), &pb.DeleteVersionDataRequest{KnowledgeBaseId: "A", VersionId: 1}); err != nil {
		t.Fatal("fresh cleanup failed:", err)
	}
	if drop.calls.Load() != 1 {
		t.Fatal("fresh cleanup never reached dropper")
	}
}
