package sync

import (
	"context"
	"net"
	"os"
	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkstore"
	"stratum/internal/raft"
	"stratum/internal/storage"
	"stratum/internal/wal"
	"testing"
	"time"
)

func liveCutoverFixture(t *testing.T) (*migrationTransferFixture, *NodeHandler, *chunkstore.MockChunkStore) {
	t.Helper()
	f, _, chunks := installedFixture(t)
	h := newServeHandler(t, f, f.root, 3, newFakeCursor(), newFakeIndex())
	h.receiver.follower.chunkStore = chunks
	serve, err := h.PrepareKBMigrationServing(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeRecordKBMigrationServe(context.Background(), "move", serveReceiptFromProto(serve)); err != nil {
		t.Fatal(err)
	}
	return f, h, chunks
}
func TestMigrationCutoverLiveTargetAndCleanupPreserveNewReplica(t *testing.T) {
	f, h, _ := liveCutoverFixture(t)
	ctx := context.Background()
	if _, err := h.ActivateKBMigration(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	snap, err := f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil || snap.Placement.Epoch != 2 || !snap.Placement.Contains(3) || snap.Placement.Contains(2) || snap.Migration != nil {
		t.Fatal("activation did not unfreeze new configuration", snap, err)
	}
	if _, err := h.ActivateKBMigration(ctx, f.request); err != nil {
		t.Fatal("lost activation reply not retryable", err)
	}
	if _, err := h.CleanupStorage(ctx, &pb.CleanupStorageRequest{CleanupId: "migration/move/target"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(migrationStagePath(f.root, "kb", "move")); !os.IsNotExist(err) {
		t.Fatal("stage not reclaimed", err)
	}
	if body, err := f.target.docStore.ReadAt(ctx, "kb", "doc", f.versions[0]); err != nil || string(body) != "old-body" {
		t.Fatal("artifact cleanup removed new replica DATA", string(body), err)
	}
	// A fresh current-epoch write can enter after the target's old import fence.
	_, release, err := h.receiver.executionGate.Enter(ctx, "kb", 2)
	if err != nil {
		t.Fatal(err)
	}
	release()
}
func TestMigrationCutoverRejectsTargetWhichLostInstalledData(t *testing.T) {
	f, h, chunks := liveCutoverFixture(t)
	ctx := context.Background()
	if err := chunks.DeleteByKB(ctx, "kb"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ActivateKBMigration(ctx, f.request); err == nil {
		t.Fatal("historical evidence activated missing DATA")
	}
	snap, _ := f.metadata.GetKBPlacement(ctx, "kb")
	if snap.Placement.Contains(3) || snap.Placement.Epoch != 1 {
		t.Fatal("failed activation changed placement")
	}
}

type migrationTestDropper struct {
	f      *migrationTransferFixture
	chunks *chunkstore.MockChunkStore
	calls  int
}

func (d *migrationTestDropper) DropKnowledgeBase(ctx context.Context, kb string) error {
	d.calls++
	if err := d.f.target.docStore.DeleteByKB(ctx, kb); err != nil {
		return err
	}
	if err := d.f.target.chunkDoc.DeleteByKB(ctx, kb); err != nil {
		return err
	}
	if err := d.f.target.versionDoc.DeleteByKB(ctx, kb); err != nil {
		return err
	}
	return d.chunks.DeleteByKB(ctx, kb)
}
func TestMigrationCancelledTargetCleanupWorksWithoutServingLease(t *testing.T) {
	f, h, chunks := liveCutoverFixture(t)
	ctx := context.Background()
	if err := f.metadata.ProposeCancelKBMigration(ctx, "move"); err != nil {
		t.Fatal(err)
	}
	h.receiver.executionGate.SetServiceLease(storage.NewServiceLease(3, "s3"))
	dropper := &migrationTestDropper{f: f, chunks: chunks}
	h.cleanup = dropper
	req := &pb.CleanupStorageRequest{CleanupId: "migration/move/target"}
	if _, err := h.CleanupStorage(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := h.CleanupStorage(ctx, req); err != nil || dropper.calls != 1 {
		t.Fatal("duplicate cleanup ran again", err, dropper.calls)
	}
	if _, err := f.target.docStore.ReadAt(ctx, "kb", "doc", f.versions[0]); err == nil {
		t.Fatal("cancelled target left formal data")
	}
}

func TestMigrationNewTaskAfterCancelledCleanupRejectsLateOldWork(t *testing.T) {
	f, h, chunks := liveCutoverFixture(t)
	ctx := context.Background()
	if err := f.metadata.ProposeCancelKBMigration(ctx, "move"); err != nil {
		t.Fatal(err)
	}
	h.cleanup = &migrationTestDropper{f: f, chunks: chunks}
	old := &pb.CleanupStorageRequest{CleanupId: "migration/move/target"}
	if _, err := h.CleanupStorage(ctx, old); err != nil {
		t.Fatal(err)
	}
	req := f.operation
	req.OperationID = "move-again"
	req.ExpectedEpoch = 2
	if err := f.metadata.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.receiver.executionGate.Fence(ctx, "kb", 2, req.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3} {
		if err := f.metadata.ProposeAckKBMigrationFence(ctx, req.OperationID, 2, id); err != nil {
			t.Fatal(err)
		}
	}
	transfer := &pb.MigrationTransferRequest{KnowledgeBaseId: "kb", OperationId: req.OperationID, ExpectedEpoch: 2, SourceNodeId: 1}
	stage, err := f.targetHandler.StageKBMigration(ctx, transfer)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeRecordKBMigrationStage(ctx, req.OperationID, stageReceiptFromProto(stage)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.InstallKBMigration(ctx, transfer); err != nil {
		t.Fatal("target could not be reused after cleanup", err)
	}
	if _, err := h.CleanupStorage(ctx, old); err != nil {
		t.Fatal(err)
	}
	if body, err := f.target.docStore.ReadAt(ctx, "kb", "doc", f.versions[0]); err != nil || string(body) != "old-body" {
		t.Fatal("late cleanup deleted new task data", string(body), err)
	}
	if _, err := h.InstallKBMigration(ctx, f.request); err == nil {
		t.Fatal("old install revived after new task")
	}
}

func TestMigrationSchedulerActivatesAndRetainsOfflineCleanup(t *testing.T) {
	f, h, _ := liveCutoverFixture(t)
	ctx := context.Background()
	scheduler := &storage.FenceScheduler{Manager: f.metadata, IsLeader: func() bool { return true }, Activate: func(c context.Context, n storage.Node, m storage.Migration) error {
		_, err := h.ActivateKBMigration(c, f.request)
		return err
	}, Cleanup: func(c context.Context, n storage.Node, task storage.Cleanup) error {
		if n.ID == 2 {
			return context.DeadlineExceeded
		}
		_, err := h.CleanupStorage(c, &pb.CleanupStorageRequest{CleanupId: task.ID})
		return err
	}}
	if err := scheduler.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Step(ctx); err == nil {
		t.Fatal("offline old node's cleanup was treated as complete")
	}
	d, err := f.metadata.GetStorageMaintenance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.Cleanups["migration/move/source"].Done || !d.Cleanups["migration/move/target"].Done || d.Placements["kb"].Epoch != 2 {
		t.Fatal("offline cleanup lost obligation or rolled back cutover", d)
	}
}

func TestMigrationCutoverWithRealControlRaft(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	control, err := raft.NewRaftNodeImpl(raft.Config{NodeID: 10, DataDir: t.TempDir(), RaftAddr: addr, WAL: wal.NewMockWAL(), ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond, HeartbeatInterval: 15 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(control.Stop)
	deadline := time.Now().Add(3 * time.Second)
	for !control.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !control.IsLeader() {
		t.Fatal("control did not elect leader")
	}
	f := newMigrationTransferFixtureWithMetadata(t, control)
	h, _, chunks := newInstallHandler(t, f, f.root, 3)
	ctx := context.Background()
	stage, err := f.targetHandler.StageKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ProposeRecordKBMigrationStage(ctx, "move", stageReceiptFromProto(stage)); err != nil {
		t.Fatal(err)
	}
	install, err := h.InstallKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ProposeRecordKBMigrationInstall(ctx, "move", installReceiptFromProto(install)); err != nil {
		t.Fatal(err)
	}
	serving := newServeHandler(t, f, f.root, 3, newFakeCursor(), newFakeIndex())
	serving.receiver.follower.chunkStore = chunks
	receipt, err := serving.PrepareKBMigrationServing(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ProposeRecordKBMigrationServe(ctx, "move", serveReceiptFromProto(receipt)); err != nil {
		t.Fatal(err)
	}
	if _, err := serving.ActivateKBMigration(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	d, err := control.GetStorageMaintenance(ctx)
	if err != nil || d.Placements["kb"].Epoch != 2 || !d.Placements["kb"].Contains(3) {
		t.Fatal("real Raft did not commit cutover", d, err)
	}
	if _, err := serving.CleanupStorage(ctx, &pb.CleanupStorageRequest{CleanupId: "migration/move/target"}); err != nil {
		t.Fatal(err)
	}
	if _, err := control.ProposeCreateVersion(ctx, "kb", f.versions[2]); err != nil {
		t.Fatal("new configuration did not resume version allocation", err)
	}
}
