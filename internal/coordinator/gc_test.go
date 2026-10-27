package coordinator

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/raft"
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

// LastVersionID mirrors the interface: the extreme value, from the same set.
func (r *gcTestRaftNode) LastVersionID(_ context.Context, kbID string) (int64, error) {
	var tail int64
	for _, v := range r.versions[kbID] {
		if v.VersionID > tail {
			tail = v.VersionID
		}
	}
	return tail, nil
}

// ListVersionsInRange mirrors the interface; this stub is not exercised by it.
func (r *gcTestRaftNode) ListVersionsInRange(context.Context, string, *int64, *int64) ([]types.VersionMeta, error) {
	return nil, nil
}

// GetVersion satisfies raft.RaftNode, answering from the same fixture data.
func (r *gcTestRaftNode) GetVersion(_ context.Context, kbID string, versionID int64) (types.VersionMeta, error) {
	for _, v := range r.versions[kbID] {
		if v.VersionID == versionID {
			return v, nil
		}
	}
	return types.VersionMeta{}, nil
}
func (r *gcTestRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error {
	return nil
}
func (r *gcTestRaftNode) ProposeMarkKBDeleting(_ context.Context, kbID string) error     { return nil }
func (r *gcTestRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, kbID string) error { return nil }
func (r *gcTestRaftNode) ProposeRemoveKBMeta(_ context.Context, kbID string) error       { return nil }
func (r *gcTestRaftNode) IsLeader() bool                                                 { return true }

func (r *gcTestRaftNode) ProposeCreateVersion(_ context.Context, kbID string, parentVersionID int64, opts ...raft.ProposeOption) (int64, error) {
	return 0, nil
}
func (r *gcTestRaftNode) ProposeMarkVersionFailedPermanent(_ context.Context, _ string, _ int64, _ types.FailureSide, _ string, _ int32) error {
	return nil
}

// ProposeRetryVersion mirrors the revocation ForceRetryVersion drives (§10.1):
// this stub is not exercised by it, so there is nothing to record.
func (r *gcTestRaftNode) ProposeRetryVersion(_ context.Context, _ string, _ int64, _ types.FailureSide) error {
	return nil
}

func (r *gcTestRaftNode) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus, _ int64) error {
	return nil
}
func (r *gcTestRaftNode) ProposeMarkVersionDataDurable(_ context.Context, _ int64) error {
	return nil
}

func (r *gcTestRaftNode) ProposeUpdateVersionSummary(_ context.Context, versionID int64, docIDSetHash string) error {
	return nil
}
func (r *gcTestRaftNode) ProposeRollback(_ context.Context, kbID string, targetVersionID int64) error {
	return nil
}
func (r *gcTestRaftNode) ProposeMarkVersionDeleting(_ context.Context, _ string, _ int64, _ types.VersionDeleteMode) ([]int64, error) {
	return nil, nil
}
func (r *gcTestRaftNode) ProposeRemoveVersionMeta(_ context.Context, kbID string, versionID int64) error {
	return nil
}

// ProposeDiscardVersion satisfies raft.RaftNode; these tests never discard.
func (r *gcTestRaftNode) ProposeDiscardVersion(_ context.Context, _ string, _ int64) error {
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
	flushes int
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

func (s *gcChunkStore) FlushWAL(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	return nil
}

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
		// chunk-orphan maps to doc-dead, which NO live version of kb-1 can read:
		// v1 never contained it (its content belonged to a version that has since
		// been deleted — the DeleteVersion cleanup already reclaimed those
		// records) and v2 holds the tombstone. Deleting it at v2 alone would NOT
		// be enough: v1 is still live and queryable, and a rebuild of v1's index
		// reads this chunk back (TestChunkGC_KeepsChunkReferencedByOlderLiveVersion).
		if err := cdm.Write(ctx, "kb-1", "chunk-orphan", "doc-dead"); err != nil {
			t.Fatalf("cdm.Write: %v", err)
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

// hookDocStore wraps a testDocStore and runs hook exactly once, on the
// first ReadAt. It lets a test inject a "concurrent committed write"
// deterministically between the GC's liveness read (its stale version
// snapshot) and its reclaim action — reproducing a real interleaving
// without racing on wall-clock timing.
type hookDocStore struct {
	*testDocStore
	once sync.Once
	hook func()
}

func (s *hookDocStore) ReadAt(ctx context.Context, kbID, docID string, maxVersionID int64) ([]byte, error) {
	s.once.Do(s.hook)
	return s.testDocStore.ReadAt(ctx, kbID, docID, maxVersionID)
}

// TestChunkGarbageCollector_StaleSnapshotRace_DeletesCommittedData is a
// regression test for the stale-snapshot race analysed in review: the
// first sweep pass judges liveness against the version list snapshot
// taken at sweepKB start, and a concurrent CreateVersion commits V3
// (resurrecting the document and finishing its storage-layer writes —
// successfully returned to the client) while the sweep is still judging.
// Without the fix this reclaim erased the V3 data (DeleteByDoc removed
// the mapping V3 just wrote, ChunkStore.Delete removed the vector).
//
// The fix (reclaimOrphan) re-validates each candidate against the raft
// CURRENT version while holding the write mutex shared with
// WriteCoordinatorImpl: the reclaim's re-check sees V3 in the version
// list, reads the document as alive, and keeps the chunk. The test
// orchestrates the interleaving deterministically (no wall-clock
// racing): the hook below lands V3's committed writes — including its
// entry in the raft version view — exactly when the first-pass liveness
// read happens on the stale snapshot.
func TestChunkGarbageCollector_StaleSnapshotRace_DeletesCommittedData(t *testing.T) {
	ctx := context.Background()

	const (
		kbID    = "kb-race"
		docID   = "doc-revived"
		chunkID = "chunk-shared"
	)

	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()

	// The GC's version view is (V1, V2), and the doc is dead in BOTH of them:
	// v1 never contained it (its content belonged to an already deleted version)
	// and v2 holds the tombstone. So the chunk is a reclaim candidate.
	if err := ds.Write(ctx, kbID, docID, 2, nil); err != nil { // tombstone
		t.Fatalf("ds.Write tombstone: %v", err)
	}
	if err := cdm.Write(ctx, kbID, chunkID, docID); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, kbID, chunkID, nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}

	// The raft view starts at (V1, V2): this models the first-pass
	// snapshot (taken at sweepKB start) predating V3's apply. The hook
	// below advances the view to include V3 — exactly what happens on a
	// real node when V3 commits between the sweep's snapshot and its
	// reclaim — so the reclaim's current-version re-check must see it.
	rn := &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{
			kbID: {{VersionID: 1, KBID: kbID}, {VersionID: 2, KBID: kbID}},
		},
	}

	// Inject the concurrent V3 commit exactly when the GC performs its
	// first-pass liveness read on the stale snapshot (first ReadAt): the
	// document becomes alive again at V3 with content, mapping and vector
	// written, and V3 enters the raft version view. From the reclaim's
	// perspective this is the race window — V3 committed and returned
	// success while the sweep was mid-flight.
	hooked := &hookDocStore{
		testDocStore: ds,
		hook: func() {
			rn.versions[kbID] = append(rn.versions[kbID], types.VersionMeta{VersionID: 3, KBID: kbID})
			if err := ds.Write(ctx, kbID, docID, 3, []byte("v3-content")); err != nil {
				t.Errorf("concurrent v3 ds.Write: %v", err)
			}
			if err := cdm.Write(ctx, kbID, chunkID, docID); err != nil {
				t.Errorf("concurrent v3 cdm.Write: %v", err)
			}
			if err := cs.Write(ctx, kbID, chunkID, nil); err != nil {
				t.Errorf("concurrent v3 cs.Write: %v", err)
			}
		},
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         hooked,
		ChunkStore:       cs,
	})

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	// Correctness invariant: V3 was committed (the client saw success);
	// a stale-snapshot GC must not erase its data.
	if got, err := ds.ReadAt(ctx, kbID, docID, 3); err != nil || string(got) != "v3-content" {
		t.Errorf("v3 doc content lost after sweep: got %q, err %v", got, err)
	}
	if ok, err := cs.Exists(ctx, kbID, chunkID); err != nil || !ok {
		t.Errorf("BUG reproduced: GC reclaimed a chunk vector committed by a newer version (stale snapshot race); exists=%v err=%v", ok, err)
	}
	docs, err := cdm.ListDocIDs(ctx, kbID, chunkID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	if !contains(docs, docID) {
		t.Errorf("BUG reproduced: GC cleared the (chunk, doc) mapping committed by a newer version (stale snapshot race); docs=%v", docs)
	}
}

// TestChunkGarbageCollector_ReclaimBeforeConcurrentWrite_Recovers pins
// down the other side of the same interleaving: when the GC's reclaim
// finishes BEFORE the concurrent V3 write-path lands its storage writes,
// the content-addressed idempotent write re-creates the vector and
// mapping, and the data ends up intact. This is the only branch
// idempotency actually protects.
func TestChunkGarbageCollector_ReclaimBeforeConcurrentWrite_Recovers(t *testing.T) {
	ctx := context.Background()

	const (
		kbID    = "kb-race"
		docID   = "doc-revived"
		chunkID = "chunk-shared"
	)

	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()

	// Dead in BOTH live versions: v1 never contained it (its content belonged to
	// an already deleted version) and v2 holds the tombstone.
	if err := ds.Write(ctx, kbID, docID, 2, nil); err != nil { // tombstone
		t.Fatalf("ds.Write tombstone: %v", err)
	}
	if err := cdm.Write(ctx, kbID, chunkID, docID); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, kbID, chunkID, nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}

	rn := &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{
			kbID: {{VersionID: 1, KBID: kbID}, {VersionID: 2, KBID: kbID}},
		},
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       cs,
	})

	// GC reclaims first (doc dead at V2): mapping + vector removed.
	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ok, err := cs.Exists(ctx, kbID, chunkID); err != nil || ok {
		t.Fatalf("precondition: chunk should be reclaimed; exists=%v err=%v", ok, err)
	}

	// The concurrent V3 write path lands afterwards and re-creates the
	// content-addressed chunk, mapping and doc content.
	if err := ds.Write(ctx, kbID, docID, 3, []byte("v3-content")); err != nil {
		t.Fatalf("ds.Write v3: %v", err)
	}
	if err := cdm.Write(ctx, kbID, chunkID, docID); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, kbID, chunkID, nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}

	// V3 data is fully intact.
	if got, err := ds.ReadAt(ctx, kbID, docID, 3); err != nil || string(got) != "v3-content" {
		t.Errorf("v3 doc content missing after idempotent re-write: got %q, err %v", got, err)
	}
	if ok, err := cs.Exists(ctx, kbID, chunkID); err != nil || !ok {
		t.Errorf("v3 chunk vector missing after idempotent re-write: exists=%v err=%v", ok, err)
	}
	docs, err := cdm.ListDocIDs(ctx, kbID, chunkID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	if !contains(docs, docID) {
		t.Errorf("v3 (chunk, doc) mapping missing after idempotent re-write: docs=%v", docs)
	}
}

var _ chunkstore.ChunkStore = (*gcChunkStore)(nil)

// TestChunkGarbageCollector_ReclaimsEveryChunkOfADeadDocument pins H3 of
// docs/code-review-2026-10-18.md.
//
// One document maps to several chunks. Reclaiming one of them used to call
// DeleteByDoc, which drops the dead document's mapping to EVERY chunk it has —
// so the sibling chunks lost their forward entries, stopped being sweep
// candidates (ListChunkIDs scans the forward keyspace), and their vectors stayed
// in the store for good. Each chunk has to be reclaimed on its own.
func TestChunkGarbageCollector_ReclaimsEveryChunkOfADeadDocument(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()

	// doc-dead is split into two chunks and is deleted at v2.
	for _, chunkID := range []string{"chunk-a", "chunk-b"} {
		if err := cdm.Write(ctx, "kb-1", chunkID, "doc-dead"); err != nil {
			t.Fatalf("cdm.Write(%s): %v", chunkID, err)
		}
		if err := cs.Write(ctx, "kb-1", chunkID, []float32{1}); err != nil {
			t.Fatalf("cs.Write(%s): %v", chunkID, err)
		}
	}
	if err := ds.Write(ctx, "kb-1", "doc-dead", 1, []byte("live at v1")); err != nil {
		t.Fatalf("ds.Write v1: %v", err)
	}
	if err := ds.Write(ctx, "kb-1", "doc-dead", 2, nil); err != nil { // tombstone
		t.Fatalf("ds.Write tombstone: %v", err)
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": {{VersionID: 2, KBID: "kb-1"}}},
		},
		ChunkDocMapper: cdm,
		DocStore:       ds,
		ChunkStore:     cs,
	})
	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	reclaimed := make(map[string]bool, len(cs.deleted))
	for _, d := range cs.deleted {
		reclaimed[d] = true
	}
	for _, chunkID := range []string{"chunk-a", "chunk-b"} {
		if !reclaimed["kb-1|"+chunkID] {
			t.Errorf("%s's vector was not reclaimed (deleted=%v): one chunk's reclaim must not unpin another",
				chunkID, cs.deleted)
		}
	}
}

// readErrorDocStore answers every ReadAt with a storage failure that is NOT
// "no such version".
type readErrorDocStore struct{ *testDocStore }

func (s *readErrorDocStore) ReadAt(context.Context, string, string, int64) ([]byte, error) {
	return nil, fmt.Errorf("pebble: iterator creation failed")
}

// TestChunkGarbageCollector_StoreReadErrorKeepsChunk pins H2 of
// docs/code-review-2026-10-18.md: only "the document is not there" means a
// reference is dead. A store that cannot answer must not be read as a deletion —
// that reclaims a live document's chunks on the strength of a read failure.
func TestChunkGarbageCollector_StoreReadErrorKeepsChunk(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	defer cdm.Close()

	cs := newGCChunkStore()
	if err := cdm.Write(ctx, "kb-1", "chunk-live", "doc-live"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-live", []float32{1}); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": {{VersionID: 1, KBID: "kb-1"}}},
		},
		ChunkDocMapper: cdm,
		DocStore:       &readErrorDocStore{newTestDocStore()},
		ChunkStore:     cs,
	})

	// The failure has to surface, not be swallowed.
	if err := gc.Sweep(ctx); err == nil {
		t.Error("Sweep must report the store read failure instead of treating it as a deletion")
	}
	if len(cs.deleted) != 0 {
		t.Errorf("a read failure reclaimed chunks: deleted=%v", cs.deleted)
	}
	if docs, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-live"); err != nil || !contains(docs, "doc-live") {
		t.Errorf("the live chunk's mapping was dropped: docs=%v err=%v", docs, err)
	}
}
