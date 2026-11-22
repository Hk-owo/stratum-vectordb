package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	serrors "stratum/internal/errors"
)

func testGate(t *testing.T) *ExecutionGate {
	t.Helper()
	g, err := NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestExecutionFenceDrainsAndSurvivesRestart(t *testing.T) {
	g := testGate(t)
	ctx, release, err := g.Enter(context.Background(), "A", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, nestedRelease, err := g.Enter(ctx, "A", 1)
	if err != nil {
		t.Fatal(err)
	}
	nestedRelease()
	done := make(chan error, 1)
	go func() { done <- g.Fence(context.Background(), "A", 1, "op") }()
	select {
	case err := <-done:
		t.Fatal("fence acknowledged active mutation:", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fence did not drain")
	}
	if _, _, err := g.Enter(context.Background(), "A", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("old epoch accepted:", err)
	}
	restarted, err := NewExecutionGate(g.dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := restarted.Enter(context.Background(), "A", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("restart forgot fence:", err)
	}
	if err := restarted.Fence(context.Background(), "A", 1, "op"); err != nil {
		t.Fatal("lost ACK retry:", err)
	}
	if err := restarted.Fence(context.Background(), "A", 1, "another"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("same epoch reused by another task:", err)
	}
	_, release, err = restarted.Enter(context.Background(), "A", 2)
	if err != nil {
		t.Fatal("cancel epoch cannot resume:", err)
	}
	release()
	_, release, err = restarted.Enter(context.Background(), "B", 1)
	if err != nil {
		t.Fatal("unrelated KB was paused:", err)
	}
	release()
}

func TestExecutionFenceTimeoutCannotAcknowledgeUndrainedWork(t *testing.T) {
	g := testGate(t)
	_, release, err := g.Enter(context.Background(), "A", 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := g.Fence(ctx, "A", 1, "op"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected drain timeout:", err)
	}
	release()
	if err := g.Fence(context.Background(), "A", 1, "op"); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionFenceFailedPersistenceIsNotAnAck(t *testing.T) {
	g := testGate(t)
	// Block the destination rename; the conservative memory fence remains but
	// every retry must still attempt persistence until it actually succeeds.
	if err := os.Mkdir(g.path("A"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := g.Fence(context.Background(), "A", 1, "op"); err == nil {
		t.Fatal("persistence failure was acknowledged")
	}
	if err := g.Fence(context.Background(), "A", 1, "op"); err == nil {
		t.Fatal("failed fsync became a successful retry")
	}
	if _, _, err := g.Enter(context.Background(), "A", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("failed fence reopened old writers:", err)
	}
	if err := os.Remove(g.path("A")); err != nil {
		t.Fatal(err)
	}
	if err := g.Fence(context.Background(), "A", 1, "op"); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionFenceCorruptionFailsClosedAndKBIDCannotEscape(t *testing.T) {
	g := testGate(t)
	if err := g.Fence(context.Background(), "../escaped", 7, "op"); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Fatalf("traversal kb_id fence err = %v, want ErrInvalidArgument", err)
	}
	if filepath.Dir(g.path("../escaped")) != g.dir {
		t.Fatal("KB identity escaped fence directory")
	}
	if err := g.Fence(context.Background(), "kb-corrupt", 7, "op"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(g.path("kb-corrupt"), []byte("bad JSON"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExecutionGate(g.dir); err == nil {
		t.Fatal("corrupt fence silently ignored")
	}
}

type fenceDirectoryReader struct{ d Directory }

func (r fenceDirectoryReader) StorageDirectory(context.Context) (Directory, error) {
	return r.d.Clone(), nil
}
func (r fenceDirectoryReader) GetKBPlacement(_ context.Context, kbID string) (Snapshot, error) {
	return r.d.Resolve(kbID)
}

func TestExecutionFenceLateCleanupCannotBorrowCancelledTaskNewEpoch(t *testing.T) {
	d, req := migrationDirectory(t)
	old, err := d.Resolve("A")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.BeginMigration(req); err != nil {
		t.Fatal(err)
	}
	if err := d.StartFencing(req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.CancelMigration(req.OperationID); err != nil {
		t.Fatal(err)
	}
	g := testGate(t)
	for _, ctx := range []context.Context{
		Bind(context.Background(), old),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs(epochHeader, "1")),
	} {
		if _, _, err := EnterCleanup(ctx, g, fenceDirectoryReader{d}, 1, "A"); !errors.Is(err, serrors.ErrPlacementStale) {
			t.Fatal("late cleanup borrowed current epoch:", err)
		}
	}
	_, release, err := EnterCleanup(context.Background(), g, fenceDirectoryReader{d}, 1, "A")
	if err != nil {
		t.Fatal("fresh authorized cleanup rejected:", err)
	}
	release()
}

func TestExecutionImportRequiresOwnDurableFenceAndExcludesNewEpoch(t *testing.T) {
	g := testGate(t)
	ctx := context.Background()
	if _, _, err := g.EnterImport(ctx, "A", 1, "op"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("unfenced import admitted", err)
	}
	if err := g.Fence(ctx, "A", 1, "op"); err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		epoch uint64
		op    string
	}{{1, "other"}, {2, "op"}, {0, "op"}} {
		if _, _, err := g.EnterImport(ctx, "A", request.epoch, request.op); err == nil {
			t.Fatal("foreign import admitted", request)
		}
	}
	restarted, err := NewExecutionGate(g.dir)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := restarted.EnterImport(ctx, "A", 1, "op")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	wait, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, _, err := restarted.Enter(wait, "A", 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("new-epoch mutation crossed active import", err)
	}
	wait2, cancel2 := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel2()
	if _, _, err := restarted.EnterImport(wait2, "A", 1, "op"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("concurrent import admitted", err)
	}
	release()
	_, leave, err := restarted.Enter(ctx, "A", 2)
	if err != nil {
		t.Fatal(err)
	}
	leave()
	if err := restarted.Fence(ctx, "A", 2, "next"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := restarted.EnterImport(ctx, "A", 1, "op"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Fatal("old import crossed newer fence", err)
	}
}

// TestExecutionCleanupAdmittedAtItsOwnFenceEpoch is the regression for a
// knowledge-base deletion that could never finish.
//
// The delete flow fences a replica under the deletion epoch and then asks it to
// reclaim, so the cleanup arrives with epoch == fence.Epoch. The ordinary entry
// rule requires the fence to be strictly BELOW the epoch being entered, so every
// reclaim was refused with kb_write_paused, the broadcast exhausted its retries,
// and the KB stayed in DELETE_FAILED — measured on the five-storage-node
// acceptance cluster (docs/实施中/storage-topology-plan.md §24).
//
// What must NOT loosen: a cleanup planned before a LATER fence still cannot cross
// it. Without that, the floor that keeps a fenced replica from accepting stale
// work would have a hole shaped like a reclaim.
func TestExecutionCleanupAdmittedAtItsOwnFenceEpoch(t *testing.T) {
	g := testGate(t)
	ctx := context.Background()

	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "n1:7000"}, {ID: 2, Addr: "n2:7000"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.AssignInitial(Placement{KBID: "A", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/A"}); err != nil {
		t.Fatal(err)
	}
	d.Delete("A")
	tomb := d.Tombstones["A"]
	if tomb.Epoch != 2 {
		t.Fatalf("tombstone epoch = %d, want 2", tomb.Epoch)
	}

	// The delete flow persists the fence at the deletion epoch...
	if err := g.Fence(ctx, "A", tomb.Epoch, "delete/A/1"); err != nil {
		t.Fatal(err)
	}
	// ...and ordinary mutations stay out, which is the invariant this must not break.
	if _, _, err := g.Enter(ctx, "A", tomb.Epoch); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatalf("ordinary mutation entered its own fenced epoch: %v", err)
	}

	// The reclaim is admitted at that epoch, on this process and after a restart
	// (a replica that crashes mid-cleanup must be able to finish the job).
	for _, gate := range []*ExecutionGate{g, mustReopenGate(t, g.dir)} {
		_, release, err := EnterCleanup(ctx, gate, fenceDirectoryReader{d}, 1, "A")
		if err != nil {
			t.Fatalf("cleanup refused at its own fence epoch: %v", err)
		}
		release()
	}

	// A fence NEWER than the tombstone still refuses an old cleanup.
	if err := g.Fence(ctx, "A", tomb.Epoch+1, "migration/A/source"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := EnterCleanup(ctx, g, fenceDirectoryReader{d}, 1, "A"); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatalf("a cleanup older than the current fence crossed it: %v", err)
	}
}

// TestExecutionCleanupStillRefusesNonMember: the widened entry must not admit a
// node the tombstone does not name — "cleanup at my own epoch" is an epoch rule,
// not a membership bypass.
func TestExecutionCleanupStillRefusesNonMember(t *testing.T) {
	g := testGate(t)
	ctx := context.Background()
	d := NewDirectory()
	if err := d.RegisterNodes(0, []Node{{ID: 1, Addr: "n1:7000"}}); err != nil {
		t.Fatal(err)
	}
	// A single-replica deployment is legitimate (the plan's §5 requires that it is
	// not mistaken for a parse failure), so a one-node initial placement is exact.
	if err := d.AssignInitial(Placement{KBID: "A", Epoch: 1, ReplicaIDs: []int64{1}, RequiredReplicas: 1, OperationID: "create/A"}); err != nil {
		t.Fatal(err)
	}
	d.Delete("A")
	if err := g.Fence(ctx, "A", d.Tombstones["A"].Epoch, "delete/A/1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := EnterCleanup(ctx, g, fenceDirectoryReader{d}, 2, "A"); !errors.Is(err, serrors.ErrNotKBReplica) {
		t.Fatalf("a node outside the cleanup set was admitted: %v", err)
	}
}

func mustReopenGate(t *testing.T, dir string) *ExecutionGate {
	t.Helper()
	g, err := NewExecutionGate(dir)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
