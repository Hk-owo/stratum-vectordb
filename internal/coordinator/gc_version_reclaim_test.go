package coordinator

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"stratum/internal/chunkdoc"
	"stratum/internal/types"
)

// newVersionReclaimGC builds a collector wired for the event-driven path
// (ReclaimVersionChunks): the two stores, the version document list and a static
// version chain.
func newVersionReclaimGC(t *testing.T, versions map[string][]types.VersionMeta) (*ChunkGarbageCollectorImpl, *chunkdoc.PebbleChunkDocMapper, *testDocStore, *gcChunkStore, *testVersionDocList) {
	t.Helper()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	t.Cleanup(func() { _ = cdm.Close() })

	ds := newTestDocStore()
	cs := newGCChunkStore()
	vd := newTestVersionDocList()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         &gcTestRaftNode{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}, versions: versions},
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       cs,
		VersionDocList:   vd,
	})
	return gc, cdm, ds, cs, vd
}

// TestReclaimVersionChunks_ReclaimsWhatOnlyTheDeletedVersionMapped is the
// event-driven happy path: the deleted version's document set names the
// candidates, and the chunks no live version can read go immediately — without
// waiting for a periodic sweep.
func TestReclaimVersionChunks_ReclaimsWhatOnlyTheDeletedVersionMapped(t *testing.T) {
	ctx := context.Background()
	gc, cdm, ds, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
		"kb-1": {{VersionID: 2, KBID: "kb-1"}}, // v1 was deleted; v2 is live
	})

	// chunk-only maps to a document only v1 had; chunk-shared maps to one v2
	// still holds.
	for _, m := range [][2]string{{"chunk-only", "doc-gone"}, {"chunk-shared", "doc-live"}} {
		if err := cdm.Write(ctx, "kb-1", m[0], m[1]); err != nil {
			t.Fatalf("cdm.Write(%s): %v", m[0], err)
		}
		if err := cs.Write(ctx, "kb-1", m[0], nil); err != nil {
			t.Fatalf("cs.Write(%s): %v", m[0], err)
		}
	}
	// doc-gone's record was reclaimed with v1 (DeleteByVersionExceptVisibleFrom),
	// which is why no live version can read it now.
	if err := ds.Write(ctx, "kb-1", "doc-live", 2, []byte("body at v2")); err != nil {
		t.Fatalf("ds.Write: %v", err)
	}
	for _, d := range []string{"doc-gone", "doc-live"} {
		if err := vd.Write(ctx, "kb-1", 1, d); err != nil {
			t.Fatalf("vd.Write v1 %s: %v", d, err)
		}
	}
	if err := vd.Write(ctx, "kb-1", 2, "doc-live"); err != nil {
		t.Fatalf("vd.Write v2: %v", err)
	}

	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err != nil {
		t.Fatalf("ReclaimVersionChunks: %v", err)
	}

	if ok, err := cs.Exists(ctx, "kb-1", "chunk-only"); err != nil || ok {
		t.Errorf("chunk of a deleted version's dead document survived; exists=%v err=%v", ok, err)
	}
	if ids, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-only"); err != nil || len(ids) != 0 {
		t.Errorf("its mapping survived; docs=%v err=%v", ids, err)
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-shared"); err != nil || !ok {
		t.Errorf("a chunk a live version still reads was reclaimed; exists=%v err=%v", ok, err)
	}
	if ids, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-shared"); err != nil || !contains(ids, "doc-live") {
		t.Errorf("its mapping was dropped; docs=%v err=%v", ids, err)
	}
}

// TestReclaimVersionChunks_KeepsWhatAnOlderLiveVersionStillReads is the case the
// event-driven path must not get wrong: deleting the NEWEST version does not
// unpin a chunk an OLDER live version still contains (that version stays
// queryable, and its index rebuild reads the vector back).
func TestReclaimVersionChunks_KeepsWhatAnOlderLiveVersionStillReads(t *testing.T) {
	ctx := context.Background()
	gc, cdm, ds, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
		"kb-1": {{VersionID: 1, KBID: "kb-1"}, {VersionID: 2, KBID: "kb-1"}}, // v3 deleted
	})

	if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	// The document lives at v1; deleting it at v3 removed only v3's own record,
	// so v1 and v2 still read the v1 body.
	if err := ds.Write(ctx, "kb-1", "doc-D", 1, []byte("content at v1")); err != nil {
		t.Fatalf("ds.Write v1: %v", err)
	}
	if err := vd.Write(ctx, "kb-1", 3, "doc-D"); err != nil {
		t.Fatalf("vd.Write v3: %v", err)
	}

	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 3); err != nil {
		t.Fatalf("ReclaimVersionChunks: %v", err)
	}

	if ok, err := cs.Exists(ctx, "kb-1", "chunk-D"); err != nil || !ok {
		t.Errorf("chunk reclaimed while live version v1 still reads doc-D; exists=%v err=%v", ok, err)
	}
	if ids, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-D"); err != nil || len(ids) == 0 {
		t.Errorf("mapping removed while v1 still reads doc-D; docs=%v err=%v", ids, err)
	}
}

// TestReclaimVersionChunks_DeletingVersionsDoNotPinChunks: a version already
// marked Deleting is not queryable and is being reclaimed, so it must not keep
// the chunk alive — otherwise deleting several versions at once would free
// nothing until the last metadata row is gone.
func TestReclaimVersionChunks_DeletingVersionsDoNotPinChunks(t *testing.T) {
	ctx := context.Background()
	gc, cdm, ds, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
		"kb-1": {
			{VersionID: 2, KBID: "kb-1", Deleting: true}, // tombstone for doc-D, being deleted
			{VersionID: 3, KBID: "kb-1", Deleting: true}, // this one is being reclaimed now
		},
	})

	if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	if err := ds.Write(ctx, "kb-1", "doc-D", 3, []byte("content at v3")); err != nil {
		t.Fatalf("ds.Write: %v", err)
	}
	if err := vd.Write(ctx, "kb-1", 3, "doc-D"); err != nil {
		t.Fatalf("vd.Write: %v", err)
	}

	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 3); err != nil {
		t.Fatalf("ReclaimVersionChunks: %v", err)
	}

	if ok, err := cs.Exists(ctx, "kb-1", "chunk-D"); err != nil || ok {
		t.Errorf("a Deleting version pinned the chunk; exists=%v err=%v", ok, err)
	}
}

// TestReclaimVersionChunks_NothingToDoIsANoOp covers the three early returns a
// replica that never received the version takes: no document list wired, an
// empty document list, and a document set whose chunks are all gone already.
func TestReclaimVersionChunks_NothingToDoIsANoOp(t *testing.T) {
	ctx := context.Background()

	t.Run("no version document list", func(t *testing.T) {
		cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
		if err != nil {
			t.Fatalf("chunkdoc: %v", err)
		}
		defer cdm.Close()
		gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
			SweepIntervalSec: 1,
			RaftNode:         &gcTestRaftNode{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}},
			ChunkDocMapper:   cdm,
			DocStore:         newTestDocStore(),
			ChunkStore:       newGCChunkStore(),
		})
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err != nil {
			t.Errorf("ReclaimVersionChunks without a document list: %v", err)
		}
	})

	t.Run("empty document set", func(t *testing.T) {
		gc, _, _, cs, _ := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 1, KBID: "kb-1"}},
		})
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 9); err != nil {
			t.Errorf("ReclaimVersionChunks of an unheld version: %v", err)
		}
		if len(cs.deleted) != 0 {
			t.Errorf("an unheld version reclaimed chunks: %v", cs.deleted)
		}
	})

	t.Run("document set without chunks", func(t *testing.T) {
		gc, _, _, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 2, KBID: "kb-1"}},
		})
		if err := vd.Write(ctx, "kb-1", 1, "doc-no-chunks"); err != nil {
			t.Fatalf("vd.Write: %v", err)
		}
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err != nil {
			t.Errorf("ReclaimVersionChunks of a version with no mapped chunks: %v", err)
		}
		if len(cs.deleted) != 0 {
			t.Errorf("unexpected reclaims: %v", cs.deleted)
		}
	})

	t.Run("invalid version id", func(t *testing.T) {
		gc, _, _, _, _ := newVersionReclaimGC(t, nil)
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 0); err == nil {
			t.Error("version id 0 must be refused rather than treated as a version")
		}
	})
}

// TestReclaimVersionChunks_StoreFailureKeepsChunk: the event-driven path owes the
// same rule as the sweep — a store that cannot answer is not a deletion (H2 of
// docs/code-review-2026-10-18.md), and the error must surface so the delete
// flow's retry runs it again (the version document list is still there because
// the reclaim runs before it is dropped).
func TestReclaimVersionChunks_StoreFailureKeepsChunk(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	cs := newGCChunkStore()
	vd := newTestVersionDocList()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": {{VersionID: 2, KBID: "kb-1"}}},
		},
		ChunkDocMapper: cdm,
		DocStore:       &readErrorDocStore{newTestDocStore()},
		ChunkStore:     cs,
		VersionDocList: vd,
	})

	if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	if err := vd.Write(ctx, "kb-1", 1, "doc-D"); err != nil {
		t.Fatalf("vd.Write: %v", err)
	}

	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err == nil {
		t.Fatal("ReclaimVersionChunks must report a store read failure instead of treating it as a deletion")
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-D"); err != nil || !ok {
		t.Errorf("a read failure reclaimed the chunk; exists=%v err=%v", ok, err)
	}
	if docs, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-D"); err != nil || len(docs) == 0 {
		t.Errorf("a read failure dropped the mapping; docs=%v err=%v", docs, err)
	}
}

// TestReclaimVersionChunks_ReportsAChainReadFailure: the liveness judgement needs
// the control layer, and an unavailable control layer must not be read as "no
// live version", which would reclaim every candidate.
func TestReclaimVersionChunks_ReportsAChainReadFailure(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	cs := newGCChunkStore()
	vd := newTestVersionDocList()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcScriptedChainRaftNode{
			gcTestRaftNode: &gcTestRaftNode{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}},
			answer: func(int) ([]types.VersionMeta, error) {
				return nil, errors.New("raft: no leader")
			},
		},
		ChunkDocMapper: cdm,
		DocStore:       newTestDocStore(),
		ChunkStore:     cs,
		VersionDocList: vd,
	})

	if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	if err := vd.Write(ctx, "kb-1", 1, "doc-D"); err != nil {
		t.Fatalf("vd.Write: %v", err)
	}

	if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err == nil {
		t.Fatal("ReclaimVersionChunks must report the chain-read failure")
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-D"); err != nil || !ok {
		t.Errorf("an unavailable control layer reclaimed the chunk; exists=%v err=%v", ok, err)
	}
}

// failingVersionDocList answers ListDocIDs with a storage failure.
type failingVersionDocList struct {
	*testVersionDocList
	err error
}

func (v *failingVersionDocList) ListDocIDs(context.Context, string, int64) ([]string, error) {
	return nil, v.err
}

// failingReverseMapper forwards to a real mapper but fails the batch reverse
// lookup the event-driven candidate scan uses.
type failingReverseMapper struct {
	chunkdoc.ChunkDocMapper
	err error
}

func (m *failingReverseMapper) ListChunkIDsByDocs(context.Context, string, []string) ([]string, error) {
	return nil, m.err
}

// TestReclaimVersionChunks_ReportsEveryFailure: none of the steps may swallow an
// error, because the delete flow's retry (and the tombstone's PENDING entry) is
// what finishes a reclaim that could not complete — and a silent success would
// let the version's document list be dropped, taking the candidate set with it.
func TestReclaimVersionChunks_ReportsEveryFailure(t *testing.T) {
	ctx := context.Background()

	t.Run("version document list unreadable", func(t *testing.T) {
		gc, cdm, _, cs, _ := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 2, KBID: "kb-1"}},
		})
		gc.cfg.VersionDocList = &failingVersionDocList{
			testVersionDocList: newTestVersionDocList(),
			err:                errors.New("pebble: iterator failed"),
		}
		if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err == nil {
			t.Error("an unreadable version document list must be reported")
		}
	})

	t.Run("reverse mapping unreadable", func(t *testing.T) {
		gc, cdm, _, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 2, KBID: "kb-1"}},
		})
		gc.cfg.ChunkDocMapper = &failingReverseMapper{ChunkDocMapper: cdm, err: errors.New("pebble: iterator failed")}
		if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}
		if err := vd.Write(ctx, "kb-1", 1, "doc-D"); err != nil {
			t.Fatalf("vd.Write: %v", err)
		}
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err == nil {
			t.Error("an unreadable reverse mapping must be reported")
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		gc, cdm, _, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 2, KBID: "kb-1"}},
		})
		if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}
		if err := vd.Write(ctx, "kb-1", 1, "doc-D"); err != nil {
			t.Fatalf("vd.Write: %v", err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := gc.ReclaimVersionChunks(cancelled, "kb-1", 1); err == nil {
			t.Error("a cancelled reclaim must report the cancellation")
		}
	})

	t.Run("vector delete fails", func(t *testing.T) {
		gc, cdm, _, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 2, KBID: "kb-1"}},
		})
		failing := &gcFailingDeleteChunkStore{gcChunkStore: cs}
		gc.cfg.ChunkStore = failing
		if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}
		if err := vd.Write(ctx, "kb-1", 1, "doc-D"); err != nil {
			t.Fatalf("vd.Write: %v", err)
		}
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err == nil {
			t.Error("a failed vector delete must be reported")
		}
	})

	t.Run("logs the reclaim", func(t *testing.T) {
		gc, cdm, _, cs, vd := newVersionReclaimGC(t, map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 2, KBID: "kb-1"}},
		})
		gc.SetLogger(zap.NewNop())
		if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}
		if err := vd.Write(ctx, "kb-1", 1, "doc-D"); err != nil {
			t.Fatalf("vd.Write: %v", err)
		}
		if err := gc.ReclaimVersionChunks(ctx, "kb-1", 1); err != nil {
			t.Fatalf("ReclaimVersionChunks: %v", err)
		}
		if ok, err := cs.Exists(ctx, "kb-1", "chunk-D"); err != nil || ok {
			t.Errorf("chunk not reclaimed; exists=%v err=%v", ok, err)
		}
	})
}
