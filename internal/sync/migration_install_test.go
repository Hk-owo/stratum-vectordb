package sync

import (
	"context"
	"errors"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"stratum/internal/chunkstore"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func newInstallHandler(t *testing.T, f *migrationTransferFixture, root string, nodeID int64) (*NodeHandler, *storage.ExecutionGate, *chunkstore.MockChunkStore) {
	t.Helper()
	chunks := chunkstore.NewMockChunkStore()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Fence(context.Background(), "kb", f.request.ExpectedEpoch, f.request.OperationId); err != nil {
		t.Fatal(err)
	}
	follower := NewFollower(f.target.docStore, f.target.chunkDoc, f.target.versionDoc, chunks, nil,
		WithFollowerExecutionGate(gate),
	)
	handler := NewNodeHandler(f.target.handler, NewPushHandler(follower, nodeID,
		WithStoragePlacement(f.metadata.GetKBPlacement),
		WithExecutionGate(gate, f.metadata),
	), WithMigrationStaging(f.metadata, root))
	handler.migration.dial = f.targetHandler.migration.dial
	return handler, gate, chunks
}

// stagedInstallFixture stages and records one artifact, leaving the task in
// STAGED with the target holding no formal data yet.
func stagedInstallFixture(t *testing.T, activeIndex ...int) (*migrationTransferFixture, *NodeHandler, *storage.ExecutionGate, *chunkstore.MockChunkStore) {
	t.Helper()
	f := newMigrationTransferFixture(t, activeIndex...)
	handler, gate, chunks := newInstallHandler(t, f, f.root, 3)
	ctx := context.Background()
	staged, err := f.targetHandler.StageKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeRecordKBMigrationStage(ctx, "move", stageReceiptFromProto(staged)); err != nil {
		t.Fatal(err)
	}
	return f, handler, gate, chunks
}

func TestMigrationInstallWritesVerifiedDataAndKeepsTargetIsolated(t *testing.T) {
	f, handler, _, chunks := stagedInstallFixture(t)
	ctx := context.Background()
	receipt, err := handler.InstallKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.GetTargetNodeId() != 3 || receipt.GetSourceNodeId() != 1 || receipt.GetVersionCount() != 3 || receipt.GetDocumentCount() != 2 || receipt.GetChunkCount() != 2 {
		t.Fatal("unexpected installation receipt", receipt)
	}
	if got, err := f.target.docStore.ReadAt(ctx, "kb", "doc", f.versions[0]); err != nil || string(got) != "old-body" {
		t.Fatal("historical MVCC body not installed", string(got), err)
	}
	if got, err := f.target.docStore.ReadAt(ctx, "kb", "doc", f.versions[1]); err != nil || string(got) != "new-body" {
		t.Fatal("current MVCC body not installed", string(got), err)
	}
	if docs, err := f.target.versionDoc.ListDocIDs(ctx, "kb", f.versions[0]); err != nil || len(docs) != 1 || docs[0] != "doc" {
		t.Fatal("document set not installed", docs, err)
	}
	if docs, err := f.target.versionDoc.ListDocIDs(ctx, "kb", f.versions[2]); err != nil || len(docs) != 0 {
		t.Fatal("empty version received data", docs, err)
	}
	for _, chunk := range []string{"old-chunk", "new-chunk"} {
		docs, err := f.target.chunkDoc.ListDocIDs(ctx, "kb", chunk)
		if err != nil || len(docs) != 1 || docs[0] != "doc" {
			t.Fatal("chunk relation not installed", chunk, docs, err)
		}
		exists, err := chunks.Exists(ctx, "kb", chunk)
		if err != nil || !exists {
			t.Fatal("chunk vector not installed", chunk, err)
		}
	}
	// Installation must not be mistaken for activation.
	if _, err := handler.receiver.bindPlacement(ctx, "kb"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Fatal("installed target became an ordinary replica", err)
	}
	snapshot, err := f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil || snapshot.Placement.Contains(3) || snapshot.Placement.Epoch != 1 || snapshot.Migration.Phase != storage.MigrationStaged {
		t.Fatal("installation changed membership or advanced the task on its own", snapshot, err)
	}
	if _, err := os.Stat(migrationInstallPath(f.root, "kb", "move")); err != nil {
		t.Fatal("durable installation record missing", err)
	}
	if err := f.metadata.ProposeRecordKBMigrationInstall(ctx, "move", installReceiptFromProto(receipt)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil || snapshot.Migration.Phase != storage.MigrationInstalled || snapshot.Placement.Contains(3) || snapshot.Placement.Epoch != 1 {
		t.Fatal("recorded installation activated the target", snapshot, err)
	}
}

func TestMigrationInstallIsIdempotentAcrossRetryAndRestart(t *testing.T) {
	f, handler, _, _ := stagedInstallFixture(t)
	ctx := context.Background()
	first, err := handler.InstallKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := handler.InstallKBMigration(ctx, f.request)
	if err != nil || !proto.Equal(first, again) {
		t.Fatal("repeated installation is not idempotent", again, err)
	}
	// A restarted process keeps the staging artifact and the install record; it
	// must not need the source again to answer.
	restored, _, _ := newInstallHandler(t, f, f.root, 3)
	restored.migration.dial = func(context.Context, string) (*grpc.ClientConn, error) {
		t.Fatal("installation retry contacted the source")
		return nil, nil
	}
	afterRestart, err := restored.InstallKBMigration(ctx, f.request)
	if err != nil || !proto.Equal(first, afterRestart) {
		t.Fatal("restart did not verify the installed data", afterRestart, err)
	}
	if err := f.metadata.ProposeRecordKBMigrationInstall(ctx, "move", installReceiptFromProto(first)); err != nil {
		t.Fatal(err)
	}
	// Now the control layer already knows the task as INSTALLED: the same request
	// must still answer from the committed receipt instead of conflicting.
	installed, err := handler.InstallKBMigration(ctx, f.request)
	if err != nil || !proto.Equal(first, installed) {
		t.Fatal("post-install retry refused its own committed receipt", installed, err)
	}
}

func TestMigrationInstallRefusesUnrelatedLocalData(t *testing.T) {
	f, handler, _, _ := stagedInstallFixture(t)
	ctx := context.Background()
	if err := f.target.versionDoc.Write(ctx, "kb", 99, "foreign"); err != nil {
		t.Fatal(err)
	}
	_, err := handler.InstallKBMigration(ctx, f.request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatal("installed over unrelated local data:", err)
	}
	if _, err := os.Stat(migrationInstallPath(f.root, "kb", "move")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused installation left a durable record")
	}
}

func TestMigrationInstallRequiresServiceAuthorization(t *testing.T) {
	f, handler, gate, _ := stagedInstallFixture(t)
	ctx := context.Background()
	gate.SetServiceLease(storage.NewServiceLease(3, "s3"))
	if _, err := handler.InstallKBMigration(ctx, f.request); status.Code(err) != codes.Unavailable {
		t.Fatal("unauthorized node installed data:", err)
	}
}

func TestMigrationInstallRejectsCancelledTaskAndWrongTarget(t *testing.T) {
	f, handler, _, _ := stagedInstallFixture(t)
	ctx := context.Background()
	// A node that is not the task's target cannot install on the target's behalf.
	other, _, _ := newInstallHandler(t, f, f.root, 2)
	if _, err := other.InstallKBMigration(ctx, f.request); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("non-target installed the artifact:", err)
	}
	if err := f.metadata.ProposeCancelKBMigration(ctx, "move"); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.InstallKBMigration(ctx, f.request); status.Code(err) == codes.OK {
		t.Fatal("cancelled task was installed")
	}
	if _, err := os.Stat(migrationInstallPath(f.root, "kb", "move")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled task left a durable install record")
	}
}

func TestMigrationInstallRejectsTruncatedArtifactAndRecoversOnRetry(t *testing.T) {
	f, handler, _, _ := stagedInstallFixture(t)
	ctx := context.Background()
	path := migrationStagePath(f.root, "kb", "move")
	intact, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, intact[:len(intact)/2], 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.InstallKBMigration(ctx, f.request); err == nil {
		t.Fatal("truncated artifact was installed")
	}
	if _, err := os.Stat(migrationInstallPath(f.root, "kb", "move")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("truncated artifact was recorded as installed")
	}
	if err := os.WriteFile(path, intact, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.InstallKBMigration(ctx, f.request); err != nil {
		t.Fatal("retry after a complete artifact failed:", err)
	}
}
