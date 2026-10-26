package coordinator

import (
	"context"
	"testing"

	"stratum/internal/chunkdoc"
	"stratum/internal/types"
)

// TestChunkGC_KeepsChunkReferencedByOlderLiveVersion: a document deleted at
// the newest version is still part of an OLDER version that has not been
// deleted. Queries can target that version explicitly (QueryRequest.version_id),
// and rebuilding its index (LRU eviction, routing hand-off to another node,
// first lazy build) reads every referenced chunk vector from the ChunkStore.
// So the chunk must survive the sweep until no live version references it.
func TestChunkGC_KeepsChunkReferencedByOlderLiveVersion(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	rn := &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			// v1 is NOT deleted: it is still a live, queryable version.
			"kb-1": {{VersionID: 1, KBID: "kb-1"}, {VersionID: 2, KBID: "kb-1"}},
		},
	}
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       cs,
	})

	// v1: doc-D present, owns chunk-D. v2: doc-D deleted (tombstone).
	if err := cdm.Write(ctx, "kb-1", "chunk-D", "doc-D"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := ds.Write(ctx, "kb-1", "doc-D", 1, []byte("content at v1")); err != nil {
		t.Fatalf("ds.Write v1: %v", err)
	}
	if err := ds.Write(ctx, "kb-1", "doc-D", 2, nil); err != nil {
		t.Fatalf("ds.Write tombstone v2: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-D", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	// From v1's point of view: the document it contains must still be backed
	// by its chunk vector, or a rebuild of v1's index cannot succeed.
	content, err := ds.ReadAt(ctx, "kb-1", "doc-D", 1)
	if err != nil || len(content) == 0 {
		t.Fatalf("precondition: doc-D should be live at v1, got %q, %v", content, err)
	}
	present, err := cs.Exists(ctx, "kb-1", "chunk-D")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !present {
		t.Errorf("chunk-D reclaimed while live version v1 still contains doc-D (deleted=%v)", cs.deleted)
	}
	ids, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-D")
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	if len(ids) == 0 {
		t.Errorf("chunk-D -> doc-D mapping removed while v1 is live")
	}
}
