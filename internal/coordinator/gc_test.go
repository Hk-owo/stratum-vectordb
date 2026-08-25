package coordinator

import (
	"context"
	"sync"
	"testing"

	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/types"
)

// gcTestRaftNode is a minimal raft.RaftNode for ChunkGarbageCollector
// tests: it serves a fixed KB list and per-KB version lists and no-ops
// everything else.
type gcTestRaftNode struct {
	kbs      []types.KnowledgeBaseMeta
	versions map[string][]types.VersionMeta
}

func (r *gcTestRaftNode) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	return r.kbs, nil
}
func (r *gcTestRaftNode) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	return r.versions[kbID], nil
}
func (r *gcTestRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error {
	return nil
}
func (r *gcTestRaftNode) ProposeMarkKBDeleting(_ context.Context, kbID string) error     { return nil }
func (r *gcTestRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, kbID string) error { return nil }
func (r *gcTestRaftNode) ProposeRemoveKBMeta(_ context.Context, kbID string) error       { return nil }
func (r *gcTestRaftNode) ProposeCreateVersion(_ context.Context, kbID string, parentVersionID int64) (int64, error) {
	return 0, nil
}
func (r *gcTestRaftNode) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus) error {
	return nil
}
func (r *gcTestRaftNode) ProposeUpdateVersionSummary(_ context.Context, versionID int64, docIDSetHash string) error {
	return nil
}
func (r *gcTestRaftNode) ProposeRollback(_ context.Context, kbID string, targetVersionID int64) error {
	return nil
}
func (r *gcTestRaftNode) ProposeMarkVersionDeleting(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (r *gcTestRaftNode) ProposeRemoveVersionMeta(_ context.Context, kbID string, versionID int64) error {
	return nil
}
func (r *gcTestRaftNode) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	return types.KnowledgeBaseMeta{}, nil
}
func (r *gcTestRaftNode) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{}, nil
}

// gcChunkStore records Deletes so tests can assert exactly which chunk
// vectors were reclaimed.
type gcChunkStore struct {
	mu      sync.Mutex
	data    map[string]bool // "kbID|chunkID" -> present
	deleted []string
}

func newGCChunkStore() *gcChunkStore {
	return &gcChunkStore{data: make(map[string]bool)}
}

func (s *gcChunkStore) Write(_ context.Context, kbID, chunkID string, _ []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[kbID+"|"+chunkID] = true
	return nil
}
func (s *gcChunkStore) Exists(_ context.Context, kbID, chunkID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[kbID+"|"+chunkID], nil
}
func (s *gcChunkStore) Delete(_ context.Context, kbID, chunkID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, kbID+"|"+chunkID)
	delete(s.data, kbID+"|"+chunkID)
	return nil
}
func (s *gcChunkStore) DeleteByKB(_ context.Context, kbID string) error { return nil }
func (s *gcChunkStore) DiskUsage(_ context.Context) (uint64, error)     { return 0, nil }

func TestChunkGarbageCollector(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	rn := &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}, {KBID: "kb-2"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 1, KBID: "kb-1"}, {VersionID: 2, KBID: "kb-1"}},
			"kb-2": {{VersionID: 1, KBID: "kb-2"}},
		},
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       cs,
	})

	t.Run("orphan chunk reclaimed when every referencing doc is deleted", func(t *testing.T) {
		// chunk-orphan maps to doc-dead; doc-dead was deleted at version 2
		// (tombstone), so the chunk is unreachable from any live version.
		if err := cdm.Write(ctx, "kb-1", "chunk-orphan", "doc-dead"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := ds.Write(ctx, "kb-1", "doc-dead", 1, []byte("live at v1")); err != nil {
			t.Fatalf("ds.Write v1: %v", err)
		}
		if err := ds.Write(ctx, "kb-1", "doc-dead", 2, nil); err != nil { // tombstone
			t.Fatalf("ds.Write tombstone: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-orphan", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}

		if err := gc.Sweep(ctx); err != nil {
			t.Fatalf("Sweep: %v", err)
		}

		// Vector reclaimed.
		found := false
		for _, d := range cs.deleted {
			if d == "kb-1|chunk-orphan" {
				found = true
			}
		}
		if !found {
			t.Errorf("orphan chunk vector not deleted; deleted=%v", cs.deleted)
		}
		// Mapping entries reclaimed: chunk no longer discoverable.
		chunkIDs, err := cdm.ListChunkIDs(ctx, "kb-1")
		if err != nil {
			t.Fatalf("ListChunkIDs: %v", err)
		}
		for _, c := range chunkIDs {
			if c == "chunk-orphan" {
				t.Errorf("orphan chunk mapping not removed: %v", chunkIDs)
			}
		}
	})

	t.Run("live chunk survives sweep", func(t *testing.T) {
		// chunk-live maps to doc-live, which still exists at version 2.
		if err := cdm.Write(ctx, "kb-1", "chunk-live", "doc-live"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := ds.Write(ctx, "kb-1", "doc-live", 1, []byte("still here")); err != nil {
			t.Fatalf("ds.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-1", "chunk-live", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}

		if err := gc.Sweep(ctx); err != nil {
			t.Fatalf("Sweep: %v", err)
		}

		for _, d := range cs.deleted {
			if d == "kb-1|chunk-live" {
				t.Errorf("live chunk was reclaimed: %v", cs.deleted)
			}
		}
		chunkIDs, err := cdm.ListChunkIDs(ctx, "kb-1")
		if err != nil {
			t.Fatalf("ListChunkIDs: %v", err)
		}
		if !contains(chunkIDs, "chunk-live") {
			t.Errorf("live chunk mapping removed: %v", chunkIDs)
		}
	})

	t.Run("chunks of other KBs are untouched", func(t *testing.T) {
		if err := cdm.Write(ctx, "kb-2", "chunk-other", "doc-other"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
		}
		if err := ds.Write(ctx, "kb-2", "doc-other", 1, []byte("other kb")); err != nil {
			t.Fatalf("ds.Write: %v", err)
		}
		if err := cs.Write(ctx, "kb-2", "chunk-other", nil); err != nil {
			t.Fatalf("cs.Write: %v", err)
		}

		if err := gc.Sweep(ctx); err != nil {
			t.Fatalf("Sweep: %v", err)
		}

		for _, d := range cs.deleted {
			if d == "kb-2|chunk-other" {
				t.Errorf("other-KB chunk reclaimed: %v", cs.deleted)
			}
		}
	})
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

var _ chunkstore.ChunkStore = (*gcChunkStore)(nil)
