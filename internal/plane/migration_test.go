package plane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	serrors "stratum/internal/errors"
	"stratum/internal/raft"
	"stratum/internal/storage"
	"stratum/internal/types"
	"stratum/internal/wal"
)

type drainingExecutor struct {
	started chan struct{}
	finish  chan struct{}
	calls   atomic.Int32
}

func (e *drainingExecutor) WriteVersionStorage(ctx context.Context, _ string, _, _ int64, _ []types.DocChange) ([]string, error) {
	if e.calls.Add(1) == 1 {
		close(e.started)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.finish:
		return nil, nil
	}
}

func TestMigrationExecutionFenceWaitsForWholeLocalTransaction(t *testing.T) {
	ctx := context.Background()
	r := raft.NewMockRaftNode(wal.NewMockWAL())
	if err := r.ProposeRegisterStorageNodes(ctx, 0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeCreateKBWithPlacement(ctx, types.KnowledgeBaseMeta{KBID: "A"}, []int64{1}); err != nil {
		t.Fatal(err)
	}
	old, err := r.GetKBPlacement(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	executor := &drainingExecutor{started: make(chan struct{}), finish: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-executor.finish:
		default:
			close(executor.finish)
		}
	})
	d := NewLocalDataPlane(LocalDataPlaneConfig{Placement: r.GetKBPlacement, Directory: r, NodeID: 1, ExecutionGate: gate, WAL: &stubWAL{t: &tracer{}}, Executor: executor})
	written := make(chan error, 1)
	go func() { written <- d.WriteVersionData(ctx, "A", 1, 0, nil) }()
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("write did not enter")
	}
	req := storage.MigrationRequest{OperationID: "move", KBID: "A", ExpectedEpoch: 1, FromID: 1, ToID: 2}
	if err := r.ProposeBeginKBMigration(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.ProposeFenceKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	fenced := make(chan error, 1)
	go func() { fenced <- gate.Fence(ctx, "A", 1, req.OperationID) }()
	select {
	case err := <-fenced:
		t.Fatal("fence ACK overtook local transaction:", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := d.WriteVersionData(ctx, "A", 2, 0, nil); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("new write reached storage during fencing:", err)
	}
	close(executor.finish)
	select {
	case err := <-written:
		if err != nil {
			t.Fatal("in-flight write was not allowed to drain:", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight transaction stuck")
	}
	select {
	case err := <-fenced:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fence did not finish")
	}
	if err := r.ProposeCancelKBMigration(ctx, req.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := d.WriteVersionData(storage.Bind(ctx, old), "A", 3, 0, nil); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("late task with old bound epoch mutated storage:", err)
	}
	if err := d.WriteVersionData(ctx, "A", 4, 0, nil); err != nil {
		t.Fatal("new epoch did not resume:", err)
	}
}

func TestMigrationPreparationProtectsWALRecoverySource(t *testing.T) {
	dir := overlapDirectory(t)
	if err := dir.BeginMigration(storage.MigrationRequest{OperationID: "move", KBID: "A", ExpectedEpoch: 1, FromID: 3, ToID: 4}); err != nil {
		t.Fatal(err)
	}
	w := &recordingCompactorWAL{}
	q := &peerCursors{cursors: map[string]map[string]int64{"s1": {"A": 20}, "s3": {"A": 20}}}
	d := reclaimPlane(t, w, q, nil, "A")
	d.nodeID = 2
	d.placements = resolverOf(dir)
	if used, err := d.ReclaimChanges(context.Background()); err != nil || len(used) != 0 || len(w.compactions) != 0 {
		t.Fatal("active migration lost WAL recovery source:", used, err)
	}
	if len(q.asked) != 0 {
		t.Fatal("protected KB was scanned for reclamation")
	}
}

type migrationDirectoryView struct{ d storage.Directory }

func (v migrationDirectoryView) StorageDirectory(context.Context) (storage.Directory, error) {
	return v.d.Clone(), nil
}
func (v migrationDirectoryView) GetKBPlacement(_ context.Context, kb string) (storage.Snapshot, error) {
	return v.d.Resolve(kb)
}

type cleanupEpochRecorder struct{ epochs []string }

func (r *cleanupEpochRecorder) DeleteVersionData(ctx context.Context, _, _ string, _ int64, _ string) error {
	md, _ := metadata.FromOutgoingContext(ctx)
	values := md.Get("x-stratum-placement-epoch")
	if len(values) == 1 {
		r.epochs = append(r.epochs, values[0])
	}
	return nil
}

func TestMigrationCleanupBroadcastCarriesDeletionEpoch(t *testing.T) {
	dir := overlapDirectory(t)
	old, _ := dir.Resolve("A")
	dir.Delete("A")
	recorder := &cleanupEpochRecorder{}
	d := NewLocalDataPlane(LocalDataPlaneConfig{NodeID: 1, Placement: resolverOf(dir), Directory: migrationDirectoryView{dir}, CleanupBroadcaster: recorder})
	if err := d.DropVersionData(storage.Bind(context.Background(), old), "A", 1); err != nil {
		t.Fatal(err)
	}
	if len(recorder.epochs) != 2 || recorder.epochs[0] != "2" || recorder.epochs[1] != "2" {
		t.Fatal("cleanup requests did not carry tombstone epoch:", recorder.epochs)
	}
}
