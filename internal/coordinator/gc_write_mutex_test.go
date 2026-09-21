// Package coordinator — shared-write-mutex concurrency tests for the
// orphan-chunk GC.
//
// The orphan-chunk GC deletes a candidate chunk's chunk-doc mappings and
// its vecstore vector inside the write mutex it shares with
// WriteCoordinatorImpl (ChunkGarbageCollectorConfig.WriteMu ==
// WriteCoordinatorConfig.WriteMu, both wired from one &writeMu in
// cmd/stratum/main.go). These tests exercise that mutual exclusion
// against the *real* write path — split, embed, chunk-store write,
// mapping write, doc write — instead of calling ChunkDocMapper.Write
// directly, which is all the older races in gc_test.go could do.
//
// The invariant under test: a chunk that a live document still references
// keeps both its mapping and its vector, i.e. a mapping never outlives
// its vector (no dangling mapping) and no live document loses its chunk.
//
// Two orderings are legal and both must converge:
//
//   - write first: the sweep's in-lock re-check reads the document as
//     alive and keeps the chunk;
//   - sweep first: the chunk is reclaimed, then the content-addressed
//     write re-creates the vector and the mapping.
//
// Anything else (e.g. the sweep deleting the vector *between* the
// writer's vector write and its mapping write) produces a dangling
// mapping, which is exactly what the shared mutex rules out.
package coordinator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/raft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// === Raft double serving both the write path and the GC ===

// gcWriteRaftNode is a raft.RaftNode double that serves BOTH the write
// path (ProposeCreateVersion / GetKB / ProposeUpdateVersionSummary) and
// the GC (ListKnowledgeBases / ListVersions) off one shared version view:
// a version committed by ProposeCreateVersion appears in ListVersions
// immediately, so a concurrent CreateVersion becomes visible to a sweep's
// in-lock re-check exactly as it does on a real node.
//
// Neither existing double can express that interleaving: gcTestRaftNode
// serves only the GC with a static version list, and testRaftNode serves
// only the write path and returns a nil version list.
type gcWriteRaftNode struct {
	mu          sync.Mutex
	nextVersion int64
	kbs         map[string]types.KnowledgeBaseMeta
	versions    map[string][]types.VersionMeta
}

func newGCWriteRaftNode() *gcWriteRaftNode {
	return &gcWriteRaftNode{
		nextVersion: 1,
		kbs:         make(map[string]types.KnowledgeBaseMeta),
		versions:    make(map[string][]types.VersionMeta),
	}
}

// IsLeader implements raft.RaftNode: this double models a single-node stack,
// where the node is its own leader.
func (r *gcWriteRaftNode) IsLeader() bool { return true }

func (r *gcWriteRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kbs[kb.KBID] = kb
	return nil
}

func (r *gcWriteRaftNode) ProposeCreateVersion(_ context.Context, kbID string, parentVersionID int64, opts ...raft.ProposeOption) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.kbs[kbID]; !ok {
		return 0, fmt.Errorf("gc write raft double: kb %s not found", kbID)
	}
	v := r.nextVersion
	r.nextVersion++
	r.versions[kbID] = append(r.versions[kbID], types.VersionMeta{
		VersionID:       v,
		ParentVersionID: parentVersionID,
		KBID:            kbID,
		IndexStatus:     types.IndexStatusPending,
	})
	return v, nil
}

func (r *gcWriteRaftNode) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kb, ok := r.kbs[kbID]
	if !ok {
		return types.KnowledgeBaseMeta{}, fmt.Errorf("gc write raft double: kb %s not found", kbID)
	}
	return kb, nil
}

func (r *gcWriteRaftNode) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]types.VersionMeta(nil), r.versions[kbID]...), nil
}

// LastVersionID mirrors the interface: the extreme value, from the same set.
func (r *gcWriteRaftNode) LastVersionID(_ context.Context, kbID string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var tail int64
	for _, v := range r.versions[kbID] {
		if v.VersionID > tail {
			tail = v.VersionID
		}
	}
	return tail, nil
}

// ListVersionsInRange mirrors the interface; this stub is not exercised by it.
func (r *gcWriteRaftNode) ListVersionsInRange(context.Context, string, *int64, *int64) ([]types.VersionMeta, error) {
	return nil, nil
}

// GetVersion satisfies raft.RaftNode, answering from the same fixture data.
func (r *gcWriteRaftNode) GetVersion(_ context.Context, kbID string, versionID int64) (types.VersionMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range r.versions[kbID] {
		if v.VersionID == versionID {
			return v, nil
		}
	}
	return types.VersionMeta{}, nil
}

func (r *gcWriteRaftNode) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]types.KnowledgeBaseMeta, 0, len(r.kbs))
	for _, kb := range r.kbs {
		out = append(out, kb)
	}
	return out, nil
}

func (r *gcWriteRaftNode) ProposeMarkVersionFailedPermanent(_ context.Context, _ string, _ int64, _ types.FailureSide, _ string, _ int32) error {
	return nil
}

// ProposeRetryVersion mirrors the revocation ForceRetryVersion drives (§10.1):
// this stub is not exercised by it, so there is nothing to record.
func (r *gcWriteRaftNode) ProposeRetryVersion(_ context.Context, _ string, _ int64, _ types.FailureSide) error {
	return nil
}

func (r *gcWriteRaftNode) ProposeUpdateVersionStatus(_ context.Context, _ int64, _ types.IndexStatus, _ int64) error {
	return nil
}
func (r *gcWriteRaftNode) ProposeMarkVersionDataDurable(_ context.Context, _ int64) error {
	return nil
}

func (r *gcWriteRaftNode) ProposeUpdateVersionSummary(_ context.Context, _ int64, _ string) error {
	return nil
}
func (r *gcWriteRaftNode) ProposeMarkKBDeleting(_ context.Context, _ string) error     { return nil }
func (r *gcWriteRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, _ string) error { return nil }
func (r *gcWriteRaftNode) ProposeRemoveKBMeta(_ context.Context, _ string) error       { return nil }
func (r *gcWriteRaftNode) ProposeRollback(_ context.Context, _ string, _ int64) error  { return nil }
func (r *gcWriteRaftNode) ProposeMarkVersionDeleting(_ context.Context, _ string, _ int64, _ types.VersionDeleteMode) ([]int64, error) {
	return nil, nil
}
func (r *gcWriteRaftNode) ProposeRemoveVersionMeta(_ context.Context, _ string, _ int64) error {
	return nil
}

// ProposeDiscardVersion satisfies raft.RaftNode; these tests never discard.
func (r *gcWriteRaftNode) ProposeDiscardVersion(_ context.Context, _ string, _ int64) error {
	return nil
}
func (r *gcWriteRaftNode) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{HasLeader: true, MemberCount: 1, LeaderID: 1}, nil
}

// === Chunk store double that can hold the GC inside its critical section ===

// gatedChunkStore wraps gcChunkStore and blocks the first
// ChunkStore.Delete until the test releases it. reclaimOrphan calls
// ChunkStore.Delete while holding the write mutex, so blocking here makes
// "the GC is inside its reclaim critical section" observable and
// controllable from the test without racing on wall-clock timing.
type gatedChunkStore struct {
	*gcChunkStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedChunkStore() *gatedChunkStore {
	return &gatedChunkStore{
		gcChunkStore: newGCChunkStore(),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (s *gatedChunkStore) Delete(ctx context.Context, kbID, chunkID string) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return s.gcChunkStore.Delete(ctx, kbID, chunkID)
}

var _ chunkstore.ChunkStore = (*gatedChunkStore)(nil)

// === Harness ===

// gcMutexHarness holds the storage doubles shared across rounds. Only the
// write mutex and the storage doubles are shared; every round gets its
// own raft double, and therefore its own knowledge base, so each round's
// sweep only scans that round's chunk.
type gcMutexHarness struct {
	mu       *sync.Mutex // the single mutex both coordinators must share
	cdm      *chunkdoc.PebbleChunkDocMapper
	ds       *testDocStore
	vdl      *testVersionDocList
	bloom    *bloom.MockBloomFilter
	splitter *mockSplitter
	embed    *testEmbedClient
	index    *testIndexManager
}

func newGCMutexHarness(t *testing.T) *gcMutexHarness {
	t.Helper()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble chunkdoc: %v", err)
	}
	t.Cleanup(func() { _ = cdm.Close() })

	return &gcMutexHarness{
		mu:       &sync.Mutex{},
		cdm:      cdm,
		ds:       newTestDocStore(),
		vdl:      newTestVersionDocList(),
		bloom:    bloom.NewMockBloomFilter(),
		splitter: &mockSplitter{windowSize: 100, overlapSize: 20},
		embed:    &testEmbedClient{},
		index:    newTestIndexManager(),
	}
}

// gcTestKBMeta returns metadata for a KB whose chunking yields exactly one
// chunk for the short test contents (mockSplitter windows at 100 runes).
func gcTestKBMeta(kbID string) types.KnowledgeBaseMeta {
	return types.KnowledgeBaseMeta{
		KBID:             kbID,
		Name:             "gc-mutex",
		ChunkWindowSize:  100,
		ChunkOverlapSize: 20,
		EmbedConfig:      types.EmbedConfig{ServiceAddr: "emb:8080", ModelID: "gc-mutex-model"},
	}
}

// newNode builds a write coordinator and an orphan-chunk GC for one
// knowledge base, both wired to the harness's single mutex — exactly the
// wiring cmd/stratum/main.go performs in production.
func (h *gcMutexHarness) newNode(t *testing.T, kbID string, cs chunkstore.ChunkStore) (*WriteCoordinatorImpl, *ChunkGarbageCollectorImpl) {
	t.Helper()
	rn := newGCWriteRaftNode()
	if err := rn.ProposeCreateKB(context.Background(), gcTestKBMeta(kbID)); err != nil {
		t.Fatalf("ProposeCreateKB(%s): %v", kbID, err)
	}

	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 1,
		WriteMu:             h.mu, // ← shared with the GC below
		WAL:                 wal.NewMockWAL(),
		RaftNode:            rn,
		Splitter:            h.splitter,
		EmbedClient:         h.embed,
		ChunkBloom:          h.bloom,
		ChunkStore:          cs,
		ChunkDocMapper:      h.cdm,
		DocStore:            h.ds,
		VersionDocList:      h.vdl,
		IndexManager:        h.index,
	})

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		WriteMu:          h.mu, // ← the same instance, as main.go does
		RaftNode:         rn,
		ChunkDocMapper:   h.cdm,
		DocStore:         h.ds,
		ChunkStore:       cs,
	})
	return coord, gc
}

// seedOrphanChunk runs the real write path twice: it adds doc-1 with
// content, then deletes doc-1. That leaves content's chunks referenced
// only by a dead document — a reclaim candidate — and returns the version
// to use as the parent of the next write, plus the chunk IDs content
// mapped to.
func seedOrphanChunk(t *testing.T, coord WriteCoordinator, cdm chunkdoc.ChunkDocMapper, kbID, content string) (parentVersion int64, chunkIDs []string) {
	t.Helper()
	ctx := context.Background()

	v1, err := coord.Execute(ctx, kbID, 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: content},
	}, "")
	if err != nil {
		t.Fatalf("seed add doc-1: %v", err)
	}

	chunkIDs, err = cdm.ListChunkIDs(ctx, kbID)
	if err != nil {
		t.Fatalf("seed ListChunkIDs: %v", err)
	}
	if len(chunkIDs) != 1 {
		t.Fatalf("seed: want exactly 1 chunk for %q, got %d (%v)", content, len(chunkIDs), chunkIDs)
	}

	parentVersion, err = coord.Execute(ctx, kbID, v1, []types.DocChange{
		{Op: types.ChangeOpDelete, DocID: "doc-1"},
	}, "")
	if err != nil {
		t.Fatalf("seed delete doc-1: %v", err)
	}
	return parentVersion, chunkIDs
}

// assertChunkLive asserts that chunkID is fully present for a live
// document: its (chunk, doc) mapping exists in the Pebble-backed
// ChunkDocMapper AND its vector exists in the chunk store. A mapping
// without a vector is the dangling-mapping state the shared mutex
// prevents.
func assertChunkLive(t *testing.T, ctx context.Context, h *gcMutexHarness, cs chunkstore.ChunkStore, kbID, chunkID, docID string) {
	t.Helper()

	docs, err := h.cdm.ListDocIDs(ctx, kbID, chunkID)
	if err != nil {
		t.Fatalf("ListDocIDs(%s, %s): %v", kbID, chunkID, err)
	}
	if !contains(docs, docID) {
		t.Errorf("mapping %s -> %s missing; docs=%v", chunkID, docID, docs)
	}

	ok, err := cs.Exists(ctx, kbID, chunkID)
	if err != nil {
		t.Fatalf("ChunkStore.Exists(%s, %s): %v", kbID, chunkID, err)
	}
	if !ok {
		t.Errorf("dangling mapping: chunk %s has mapping %v but no vector", chunkID, docs)
	}
}

// === Tests ===

// TestGC_SharedWriteMu_BlocksConcurrentWriteDuringReclaim proves the
// mutual exclusion directly: while the GC is inside its reclaim critical
// section (observed by parking it inside ChunkStore.Delete, which
// reclaimOrphan calls under the lock), a CreateVersion must not be able to
// run — it needs the same mutex for its whole BEGIN→COMMIT span.
//
// This is the property that makes "nobody can write a mapping for the
// chunk being reclaimed" true: the only production writer of chunk-doc
// mappings on this node, WriteCoordinatorImpl, cannot proceed while the
// GC holds the lock.
func TestGC_SharedWriteMu_BlocksConcurrentWriteDuringReclaim(t *testing.T) {
	const (
		kbID    = "kb-mutex-block"
		content = "a short document body"
	)
	ctx := context.Background()

	h := newGCMutexHarness(t)
	cs := newGatedChunkStore()
	coord, gc := h.newNode(t, kbID, cs)

	parent, chunkIDs := seedOrphanChunk(t, coord, h.cdm, kbID, content)
	chunkID := chunkIDs[0]

	// Sweep in the background; it parks inside ChunkStore.Delete while
	// holding the shared mutex.
	sweepDone := make(chan error, 1)
	go func() { sweepDone <- gc.Sweep(ctx) }()

	select {
	case <-cs.entered:
		// GC is now inside its reclaim critical section.
	case err := <-sweepDone:
		t.Fatalf("sweep finished without ever reaching ChunkStore.Delete (err=%v): precondition not met", err)
	case <-time.After(10 * time.Second):
		t.Fatal("GC never reached ChunkStore.Delete: the seeded chunk was not judged an orphan")
	}

	// A write that re-introduces the same content (and therefore the same
	// content-addressed chunk) must block on the shared mutex.
	writeDone := make(chan error, 1)
	go func() {
		_, err := coord.Execute(ctx, kbID, parent, []types.DocChange{
			{Op: types.ChangeOpAdd, DocID: "doc-2", Content: content},
		}, "")
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		t.Fatalf("CreateVersion completed while the GC held the shared write mutex (err=%v): WriteMu is not shared between WriteCoordinatorImpl and ChunkGarbageCollectorImpl", err)
	case <-time.After(250 * time.Millisecond):
		// Expected: the writer is blocked behind the reclaim.
	}

	// Release the GC; both must now finish and converge.
	close(cs.release)

	if err := <-sweepDone; err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("concurrent CreateVersion: %v", err)
	}

	assertChunkLive(t, ctx, h, cs, kbID, chunkID, "doc-2")
}

// TestGC_SharedWriteMu_WriteAfterReclaim_RebuildsChunk pins the
// "sweep first" ordering using the real write path: the sweep reclaims the
// orphan outright, then a later CreateVersion that re-introduces the same
// content must re-create both the vector and the mapping. This is the
// content-addressed idempotency that makes reclaim-then-write safe, and it
// exercises the bloom-filter path too (the chunk is still in the bloom
// filter, so the writer must confirm against the authoritative store,
// find it gone, and rewrite).
func TestGC_SharedWriteMu_WriteAfterReclaim_RebuildsChunk(t *testing.T) {
	const (
		kbID    = "kb-mutex-rebuild"
		content = "the very same body text"
	)
	ctx := context.Background()

	h := newGCMutexHarness(t)
	cs := newGCChunkStore()
	coord, gc := h.newNode(t, kbID, cs)

	parent, chunkIDs := seedOrphanChunk(t, coord, h.cdm, kbID, content)
	chunkID := chunkIDs[0]

	// Nothing else running: the orphan is reclaimed outright.
	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ok, err := cs.Exists(ctx, kbID, chunkID); err != nil || ok {
		t.Fatalf("precondition: orphan vector should be gone; exists=%v err=%v", ok, err)
	}
	if docs, err := h.cdm.ListDocIDs(ctx, kbID, chunkID); err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	} else if len(docs) != 0 {
		t.Fatalf("precondition: orphan mapping should be cleared; docs=%v", docs)
	}

	// A later write with the same content rebuilds the chunk.
	if _, err := coord.Execute(ctx, kbID, parent, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-2", Content: content},
	}, ""); err != nil {
		t.Fatalf("re-add after reclaim: %v", err)
	}

	assertChunkLive(t, ctx, h, cs, kbID, chunkID, "doc-2")
}

// TestGC_SharedWriteMu_ConcurrentWriteAndSweep_KeepsLiveChunk runs the
// two orderings many times over with the reclaim and the write genuinely
// racing (no hooks, no injected delays). Whichever order the scheduler
// picks, the shared mutex must leave a consistent terminal state: the
// chunk of a live document always has both its mapping and its vector.
//
// Run under -race this also proves the two paths do not touch the shared
// stores concurrently.
func TestGC_SharedWriteMu_ConcurrentWriteAndSweep_KeepsLiveChunk(t *testing.T) {
	const rounds = 40
	ctx := context.Background()
	h := newGCMutexHarness(t)

	for i := 0; i < rounds; i++ {
		kbID := fmt.Sprintf("kb-mutex-race-%d", i)
		content := fmt.Sprintf("shared body for round %d", i)

		cs := newGCChunkStore()
		coord, gc := h.newNode(t, kbID, cs)

		parent, chunkIDs := seedOrphanChunk(t, coord, h.cdm, kbID, content)
		chunkID := chunkIDs[0]

		var wg sync.WaitGroup
		sweepErr := make(chan error, 1)
		writeErr := make(chan error, 1)

		wg.Add(2)
		go func() {
			defer wg.Done()
			sweepErr <- gc.Sweep(ctx)
		}()
		go func() {
			defer wg.Done()
			_, err := coord.Execute(ctx, kbID, parent, []types.DocChange{
				{Op: types.ChangeOpAdd, DocID: "doc-2", Content: content},
			}, "")
			writeErr <- err
		}()
		wg.Wait()

		if err := <-sweepErr; err != nil {
			t.Fatalf("round %d: Sweep: %v", i, err)
		}
		if err := <-writeErr; err != nil {
			t.Fatalf("round %d: CreateVersion: %v", i, err)
		}

		assertChunkLive(t, ctx, h, cs, kbID, chunkID, "doc-2")
		if t.Failed() {
			t.Fatalf("round %d: the live document's chunk was damaged by a concurrent sweep", i)
		}
	}
}

var _ raft.RaftNode = (*gcWriteRaftNode)(nil)
