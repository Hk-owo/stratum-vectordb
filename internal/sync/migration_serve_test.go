package sync

import (
	"context"
	"os"
	stdsync "sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"stratum/internal/chunkstore"
	"stratum/internal/storage"
)

type fakeCursor struct {
	mu     stdsync.Mutex
	cursor map[string]int64
	moves  []int64
}

func newFakeCursor() *fakeCursor { return &fakeCursor{cursor: map[string]int64{}} }

func (c *fakeCursor) LocalVersionOf(kbID string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor[kbID]
}
func (c *fakeCursor) MarkVersionContiguous(kbID string, versionID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if versionID > c.cursor[kbID] {
		c.cursor[kbID] = versionID
	}
	c.moves = append(c.moves, versionID)
}

type fakeIndex struct {
	mu     stdsync.Mutex
	exists map[int64]bool
	builds []int64
	never  bool
}

func newFakeIndex() *fakeIndex { return &fakeIndex{exists: map[int64]bool{}} }

func (f *fakeIndex) IndexExists(_ context.Context, _ string, versionID int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exists[versionID], nil
}
func (f *fakeIndex) TriggerBuildBackfill(_ context.Context, _ string, versionID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.builds = append(f.builds, versionID)
	if !f.never {
		f.exists[versionID] = true
	}
	return nil
}

// newServeHandler returns a handler wired with the serving surface: the cursor,
// the document filter store and the index manager a target needs.
func newServeHandler(t *testing.T, f *migrationTransferFixture, root string, nodeID int64, cursor *fakeCursor, index MigrationServing) *NodeHandler {
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
		WithFollowerLocalVersionAdvancer(cursor),
	)
	handler := NewNodeHandler(f.target.handler, NewPushHandler(follower, nodeID,
		WithStoragePlacement(f.metadata.GetKBPlacement),
		WithExecutionGate(gate, f.metadata),
		WithLocalVersion(cursor),
		WithMigrationServing(index),
	), WithMigrationStaging(f.metadata, root))
	handler.migration.dial = f.targetHandler.migration.dial
	return handler
}

// installedFixture stages and installs one artifact, leaving the task INSTALLED
// with the target holding formal data but no serving evidence.
func installedFixture(t *testing.T, activeIndex ...int) (*migrationTransferFixture, *NodeHandler, *chunkstore.MockChunkStore) {
	t.Helper()
	f, handler, _, chunks := stagedInstallFixture(t, activeIndex...)
	ctx := context.Background()
	receipt, err := handler.InstallKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeRecordKBMigrationInstall(ctx, "move", installReceiptFromProto(receipt)); err != nil {
		t.Fatal(err)
	}
	return f, handler, chunks
}

func TestMigrationServeRestoresLocalStateAndWaitsForIndex(t *testing.T) {
	f, installHandler, _ := installedFixture(t, 1)
	ctx := context.Background()
	cursor, index := newFakeCursor(), newFakeIndex()
	handler := newServeHandler(t, f, f.root, 3, cursor, index)
	if _, err := handler.PrepareKBMigrationServing(ctx, f.request); err != nil {
		t.Fatal(err)
	}
	receipt, err := handler.PrepareKBMigrationServing(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.GetTargetNodeId() != 3 || receipt.GetActiveVersion() != f.versions[1] || receipt.GetCursorVersion() != f.versions[2] || receipt.GetBloomVersionCount() != 3 || !receipt.GetIndexReady() {
		t.Fatal("unexpected serving receipt", receipt)
	}
	if cursor.LocalVersionOf("kb") != f.versions[2] {
		t.Fatal("data cursor was not advanced to the active version")
	}
	if len(index.builds) != 1 || index.builds[0] != f.versions[1] {
		t.Fatal("active version index was not built exactly once", index.builds)
	}
	// Serving evidence is recorded, and it still activates nothing.
	if err := f.metadata.ProposeRecordKBMigrationServe(ctx, "move", serveReceiptFromProto(receipt)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil || snapshot.Migration.Phase != storage.MigrationInstalled || snapshot.Migration.Serve == nil || snapshot.Placement.Contains(3) || snapshot.Placement.Epoch != 1 {
		t.Fatal("serving evidence activated the target", snapshot, err)
	}
	// A repeated serving step answers the same evidence rather than conflicting.
	again, err := handler.PrepareKBMigrationServing(ctx, f.request)
	if err != nil || !proto.Equal(receipt, again) {
		t.Fatal("repeated serving step is not idempotent", again, err)
	}
	_ = installHandler
}

func TestMigrationServeWaitsWithinBudgetAndNeverReportsUnreadyIndex(t *testing.T) {
	f, _, _ := installedFixture(t, 1)
	ctx := context.Background()
	cursor := newFakeCursor()
	// Mark the cursor current so the only missing condition is the index.
	cursor.MarkVersionContiguous("kb", f.versions[2])
	index := newFakeIndex()
	index.never = true
	handler := newServeHandler(t, f, f.root, 3, cursor, index)
	handler.migration.serveWait = 100 * time.Millisecond
	if _, err := handler.PrepareKBMigrationServing(ctx, f.request); status.Code(err) != codes.Unavailable {
		t.Fatal("missing index artifact was reported as serviceable:", err)
	}
	snapshot, err := f.metadata.GetKBPlacement(ctx, "kb")
	if err != nil || snapshot.Migration.Serve != nil {
		t.Fatal("unserviceable target recorded serving evidence", snapshot, err)
	}
	if len(index.builds) != 1 {
		t.Fatal("index build was not requested", index.builds)
	}
}

func TestMigrationServeRequiresInstalledData(t *testing.T) {
	f, handler, _, _ := stagedInstallFixture(t)
	ctx := context.Background()
	cursor, index := newFakeCursor(), newFakeIndex()
	serving := newServeHandler(t, f, f.root, 3, cursor, index)
	cursor.MarkVersionContiguous("kb", f.versions[2])
	if _, err := serving.PrepareKBMigrationServing(ctx, f.request); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("serving step ran before installation:", err)
	}
	// A target that never installed, but whose task says INSTALLED, is refused too:
	// the local install record is what ties the stores to the task.
	receipt, err := handler.InstallKBMigration(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.metadata.ProposeRecordKBMigrationInstall(ctx, "move", installReceiptFromProto(receipt)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(migrationInstallPath(f.root, "kb", "move")); err != nil {
		t.Fatal(err)
	}
	if _, err := serving.PrepareKBMigrationServing(ctx, f.request); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("missing local install record was accepted:", err)
	}
}
