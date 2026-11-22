package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"stratum/internal/types"
)

func TestChunkGCMigrationFenceProtectsPhysicalRecordsUntilNewEpoch(t *testing.T) {
	ctx := context.Background()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snap := storage.Snapshot{Placement: storage.Placement{KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{1}, RequiredReplicas: 1}}
	gc, mapper, _, chunks, versions := newVersionReclaimGC(t, map[string][]types.VersionMeta{"kb-1": {}},
		func(c context.Context, kb string) (context.Context, func(), error) {
			return storage.EnterMutation(storage.Bind(c, snap), gate, snap)
		},
	)
	if err := mapper.Write(ctx, "kb-1", "orphan", "gone"); err != nil {
		t.Fatal(err)
	}
	if err := chunks.Write(ctx, "kb-1", "orphan", []float32{1}); err != nil {
		t.Fatal(err)
	}
	if err := versions.Write(ctx, "kb-1", 1, "gone"); err != nil {
		t.Fatal(err)
	}
	if err := gate.Fence(ctx, "kb-1", 1, "move"); err != nil {
		t.Fatal(err)
	}
	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("fenced GC reclaimed records", err)
	}
	if exists, err := chunks.Exists(ctx, "kb-1", "orphan"); err != nil || !exists {
		t.Fatal("vector was deleted", exists, err)
	}
	if docs, err := mapper.ListDocIDs(ctx, "kb-1", "orphan"); err != nil || len(docs) != 1 {
		t.Fatal("relation was deleted", docs, err)
	}
	snap.Placement.Epoch = 2
	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err != nil {
		t.Fatal("new epoch did not resume cleanup", err)
	}
	if exists, err := chunks.Exists(ctx, "kb-1", "orphan"); err != nil || exists {
		t.Fatal("orphan was not reclaimed after resume", exists, err)
	}
}

type migrationBlockingChunkStore struct {
	*gcChunkStore
	entered chan struct{}
	resume  chan struct{}
}

func (s migrationBlockingChunkStore) Delete(ctx context.Context, kb, chunk string) error {
	close(s.entered)
	select {
	case <-s.resume:
		return s.gcChunkStore.Delete(ctx, kb, chunk)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestChunkGCMigrationFenceDrainsEnteredPhysicalDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snap := storage.Snapshot{Placement: storage.Placement{KBID: "kb-1", Epoch: 1, ReplicaIDs: []int64{1}, RequiredReplicas: 1}}
	gc, mapper, _, chunks, versions := newVersionReclaimGC(t, map[string][]types.VersionMeta{"kb-1": {}},
		func(c context.Context, _ string) (context.Context, func(), error) {
			return storage.EnterMutation(storage.Bind(c, snap), gate, snap)
		},
	)
	if err := mapper.Write(ctx, "kb-1", "orphan", "gone"); err != nil {
		t.Fatal(err)
	}
	if err := chunks.Write(ctx, "kb-1", "orphan", []float32{1}); err != nil {
		t.Fatal(err)
	}
	if err := versions.Write(ctx, "kb-1", 1, "gone"); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	gc.cfg.ChunkStore = migrationBlockingChunkStore{gcChunkStore: chunks, entered: entered, resume: resume}
	reclaimed := make(chan error, 1)
	go func() { reclaimed <- gc.ReclaimVersionChunks(ctx, "kb-1", 1) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("physical delete did not start")
	}
	fenced := make(chan error, 1)
	go func() { fenced <- gate.Fence(ctx, "kb-1", 1, "move") }()
	select {
	case err := <-fenced:
		t.Fatal("fence acknowledged while physical delete was active", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-reclaimed; err != nil {
		t.Fatal(err)
	}
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Fatal("fenced epoch reentered GC", err)
	}
}
