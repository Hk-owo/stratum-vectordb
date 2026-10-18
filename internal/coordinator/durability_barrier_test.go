package coordinator

import (
	"context"
	"testing"

	"stratum/internal/bloom"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// The two stores are flushed at different times by design, and the ordering
// between them and the fsynced WAL record is the whole point:
//
//	chunk vectors (vecstore, RocksDB sync=false)  →  flush  →  WAL record (fsync)
//
// Without the flush the record outlives the data it vouches for: a power loss
// keeps "this version's data has landed" and drops the vectors, and recovery
// never replays a version whose COMMIT is present (and chunk vectors are the only
// copy). The tests here pin the order, not the fsync itself — a unit test cannot
// lose power, but it can catch the flush being moved after the record or dropped.
//
// 撤掉修复即变红: remove the FlushWAL call in writeVersionStorage (or move it after
// the caller's WriteCommit) and both assertions below fail.

// orderWAL records the vecstore flush count at the moment each durability claim
// is written. It embeds the mock so every other WAL method behaves normally; the
// counters are read only by the test goroutine, because one Execute serialises
// the whole transaction.
type orderWAL struct {
	*wal.MockWAL

	flushCount       func() int
	atCommit         []int
	atDeleteComplete []int
}

func newOrderWAL(flushCount func() int) *orderWAL {
	return &orderWAL{MockWAL: wal.NewMockWAL(), flushCount: flushCount}
}

func (w *orderWAL) WriteCommit(ctx context.Context, versionID int64) error {
	w.atCommit = append(w.atCommit, w.flushCount())
	return w.MockWAL.WriteCommit(ctx, versionID)
}

func (w *orderWAL) WriteDeleteComplete(ctx context.Context, kbID string) error {
	w.atDeleteComplete = append(w.atDeleteComplete, w.flushCount())
	return w.MockWAL.WriteDeleteComplete(ctx, kbID)
}

// TestWriteCoordinator_FlushesVecstoreBeforeTheWALCommit pins the write path:
// the chunk vectors are flushed AFTER they were written and BEFORE the COMMIT.
func TestWriteCoordinator_FlushesVecstoreBeforeTheWALCommit(t *testing.T) {
	ds := newTestDocStore()
	cdm := newTestChunkDocMapper()
	vdl := newTestVersionDocList()
	cs := newTestChunkStore()
	ec := &testEmbedClient{}
	im := newTestIndexManager()
	rn := newTestRaftNode()
	w := newOrderWAL(cs.flushCount)
	chunkBF := bloom.NewMockBloomFilter()
	splitter := &mockSplitter{windowSize: 100, overlapSize: 20}

	rn.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "test",
		ChunkWindowSize: 100, ChunkOverlapSize: 20,
		EmbedConfig: types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "m1"},
	})

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            splitter,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vdl,
		IndexManager:        im,
	})

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "hello world this is a test document"},
	}
	if _, err := coord.Execute(context.Background(), "kb-1", 0, changes, ""); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(w.atCommit) != 1 {
		t.Fatalf("WAL.WriteCommit called %d time(s), want 1", len(w.atCommit))
	}
	if got := w.atCommit[0]; got < 1 {
		t.Fatalf("the vecstore was flushed %d time(s) before WAL.WriteCommit, want at least 1: the "+
			"COMMIT would vouch for chunk vectors still sitting in the page cache", got)
	}

	// Not just before the COMMIT: after the writes. A flush that ran first would
	// leave the very writes it is supposed to protect unflushed.
	if len(cs.writtenAtFlush) == 0 {
		t.Fatal("ChunkStore.FlushWAL was never called")
	}
	for i, written := range cs.writtenAtFlush {
		if written == 0 {
			t.Fatalf("flush #%d happened before any chunk vector was written", i+1)
		}
	}
}

// TestDeleteCoordinator_FlushesVecstoreBeforeDeleteComplete pins the delete path:
// the knowledge base's vectors are deleted, then flushed, then the flow writes the
// fsynced DELETE_COMPLETE that claims the data is gone.
func TestDeleteCoordinator_FlushesVecstoreBeforeDeleteComplete(t *testing.T) {
	rn := newDeleteTestRaftNode()
	ds := newDeleteTestDocStore()
	cs := newDeleteTestChunkStore()
	cdm := newDeleteTestChunkDocMapper()
	vdl := newDeleteTestVersionDocList()
	im := newDeleteTestIndexManager()
	w := newOrderWAL(cs.flushCount)

	coord := NewDeleteCoordinatorImpl(DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vdl,
		SourceAnnouncements: &forgettingAnnouncements{},
	})

	if err := coord.Execute(context.Background(), "kb-1"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(w.atDeleteComplete) != 1 {
		t.Fatalf("WAL.WriteDeleteComplete called %d time(s), want 1", len(w.atDeleteComplete))
	}
	if got := w.atDeleteComplete[0]; got < 1 {
		t.Fatalf("the vecstore was flushed %d time(s) before WAL.WriteDeleteComplete, want at least 1: "+
			"a power loss could keep the completion record and resurrect every vector it claimed "+
			"deleted, with no mapping left for the orphan GC to find them by", got)
	}
	// And after the deletion itself: a flush before DeleteByKB flushes nothing.
	if len(cs.flushesAfterDelete) == 0 {
		t.Fatal("ChunkStore.FlushWAL was never called")
	}
	for i, deleted := range cs.flushesAfterDelete {
		if deleted == 0 {
			t.Fatalf("flush #%d happened before the knowledge base's vectors were deleted", i+1)
		}
	}
}
