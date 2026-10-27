package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"

	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/types"
)

// gcChainCounter counts version-chain reads and tail probes, so a case can pin
// how much a sweep asks the control layer.
//
// Judging a chunk's liveness needs the whole LIVE CHAIN (a chunk an older live
// version still references must survive), and on a storage node that read is a
// remote RPC — so the count matters. The sweep is allowed exactly one chain read
// per knowledge base, plus one O(1) tail probe per reclaim candidate: the probe
// answers "did anything commit since the snapshot", which is the only way the
// chain can gain a version.
type gcChainCounter struct {
	*gcTestRaftNode
	listVersionsCalls  int
	lastVersionIDCalls int
	// beforeTailProbe runs just before each LastVersionID answer, letting a
	// case land a concurrent commit in the reclaim's re-check window.
	beforeTailProbe func()
}

func (c *gcChainCounter) ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error) {
	c.listVersionsCalls++
	return c.gcTestRaftNode.ListVersions(ctx, kbID)
}

func (c *gcChainCounter) LastVersionID(ctx context.Context, kbID string) (int64, error) {
	c.lastVersionIDCalls++
	if c.beforeTailProbe != nil {
		c.beforeTailProbe()
	}
	return c.gcTestRaftNode.LastVersionID(ctx, kbID)
}

// TestChunkGarbageCollector_AsksTheChainOncePerSweep pins the RPC budget of the
// liveness judgement: one whole-chain read per knowledge base, and — per reclaim
// candidate — one tail probe instead of another chain read.
func TestChunkGarbageCollector_AsksTheChainOncePerSweep(t *testing.T) {
	ctx := context.Background()

	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	rn := &gcChainCounter{gcTestRaftNode: &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 1, KBID: "kb-1"}, {VersionID: 7, KBID: "kb-1"}},
		},
	}}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       cs,
	})

	// One chunk whose only document is dead in every live version, so the sweep
	// runs a full judgement AND a reclaim re-check.
	if err := cdm.Write(ctx, "kb-1", "chunk-dead", "doc-dead"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-dead", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	if err := ds.Write(ctx, "kb-1", "doc-dead", 7, nil); err != nil { // tombstone at the tail
		t.Fatalf("ds.Write tombstone: %v", err)
	}

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-dead"); err != nil || ok {
		t.Fatalf("precondition: chunk-dead should have been reclaimed; exists=%v err=%v", ok, err)
	}
	if rn.listVersionsCalls != 1 {
		t.Errorf("ListVersions calls = %d, want 1: one chain read per knowledge base per sweep",
			rn.listVersionsCalls)
	}
	if rn.lastVersionIDCalls == 0 {
		t.Error("LastVersionID was never called: the reclaim re-check must ask whether the chain grew")
	}
}

// TestChunkGarbageCollector_RereadsTheChainWhenAVersionCommitted pins the other
// half: once the tail has moved, the start-of-sweep snapshot cannot see the new
// version, so the reclaim re-check must re-read the chain rather than judge
// against a stale set.
func TestChunkGarbageCollector_RereadsTheChainWhenAVersionCommitted(t *testing.T) {
	ctx := context.Background()

	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	rn := &gcChainCounter{gcTestRaftNode: &gcTestRaftNode{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {{VersionID: 1, KBID: "kb-1"}, {VersionID: 7, KBID: "kb-1"}},
		},
	}}
	// A version commits between the snapshot and the reclaim re-check.
	committed := false
	rn.beforeTailProbe = func() {
		if committed {
			return
		}
		committed = true
		rn.versions["kb-1"] = append(rn.versions["kb-1"], types.VersionMeta{VersionID: 8, KBID: "kb-1"})
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode:         rn,
		ChunkDocMapper:   cdm,
		DocStore:         ds,
		ChunkStore:       cs,
	})

	// Dead in every live version, including the one that just committed.
	if err := cdm.Write(ctx, "kb-1", "chunk-dead", "doc-dead"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, "kb-1", "chunk-dead", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	if err := ds.Write(ctx, "kb-1", "doc-dead", 7, nil); err != nil { // tombstone
		t.Fatalf("ds.Write tombstone: %v", err)
	}

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-dead"); err != nil || ok {
		t.Fatalf("precondition: chunk-dead should have been reclaimed; exists=%v err=%v", ok, err)
	}
	if rn.listVersionsCalls != 2 {
		t.Errorf("ListVersions calls = %d, want 2: the sweep snapshot plus a re-read after the tail moved",
			rn.listVersionsCalls)
	}
}

// gcScriptedChainRaftNode scripts the control-layer answers: the n-th
// ListVersions call is decided by answer, and tail (when non-zero) overrides
// LastVersionID so a case can move the tail independently of the snapshot.
type gcScriptedChainRaftNode struct {
	*gcTestRaftNode
	mu     sync.Mutex
	calls  int
	answer func(call int) ([]types.VersionMeta, error)
	tail   int64
}

func (r *gcScriptedChainRaftNode) ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if r.answer != nil {
		return r.answer(call)
	}
	return r.gcTestRaftNode.ListVersions(ctx, kbID)
}

func (r *gcScriptedChainRaftNode) LastVersionID(ctx context.Context, kbID string) (int64, error) {
	if r.tail != 0 {
		return r.tail, nil
	}
	return r.gcTestRaftNode.LastVersionID(ctx, kbID)
}

// gcFailingDeleteChunkStore fails ChunkStore.Delete, the last thing a reclaim
// does inside the critical section.
type gcFailingDeleteChunkStore struct{ *gcChunkStore }

func (s *gcFailingDeleteChunkStore) Delete(context.Context, string, string) error {
	return errors.New("vecstore: unavailable")
}

// seedDeadChunk writes one mapping whose document is dead in every live version,
// so a sweep reaches the reclaim stage for it.
func seedDeadChunk(t *testing.T, cdm chunkdoc.ChunkDocMapper, ds *testDocStore, cs chunkstore.ChunkStore, kbID string) {
	t.Helper()
	ctx := context.Background()
	if err := cdm.Write(ctx, kbID, "chunk-dead", "doc-dead"); err != nil {
		t.Fatalf("cdm.Write: %v", err)
	}
	if err := cs.Write(ctx, kbID, "chunk-dead", nil); err != nil {
		t.Fatalf("cs.Write: %v", err)
	}
	if err := ds.Write(ctx, kbID, "doc-dead", 7, nil); err != nil { // tombstone
		t.Fatalf("ds.Write tombstone: %v", err)
	}
}

// TestChunkGarbageCollector_ChainReadFailureKeepsChunks: the liveness judgement
// depends on the control layer's answer. A read that fails is not "no live
// version" — treating it as one would reclaim the whole knowledge base's chunks
// (H2 of docs/code-review-2026-10-18.md, one level up).
func TestChunkGarbageCollector_ChainReadFailureKeepsChunks(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcScriptedChainRaftNode{
			gcTestRaftNode: &gcTestRaftNode{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}},
			answer: func(int) ([]types.VersionMeta, error) {
				return nil, errors.New("raft: no leader")
			},
		},
		ChunkDocMapper: cdm,
		DocStore:       ds,
		ChunkStore:     cs,
	})
	seedDeadChunk(t, cdm, ds, cs, "kb-1")

	if err := gc.Sweep(ctx); err == nil {
		t.Fatal("Sweep must report the chain-read failure instead of judging every chunk an orphan")
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-dead"); err != nil || !ok {
		t.Errorf("a chain-read failure reclaimed a chunk; exists=%v err=%v", ok, err)
	}
	if docs, err := cdm.ListDocIDs(ctx, "kb-1", "chunk-dead"); err != nil || len(docs) == 0 {
		t.Errorf("a chain-read failure dropped the mapping; docs=%v err=%v", docs, err)
	}
}

// TestChunkGarbageCollector_RecheckChainReadFailureKeepsChunk: the same rule
// inside the reclaim re-check, where the chain is re-read because the tail moved.
func TestChunkGarbageCollector_RecheckChainReadFailureKeepsChunk(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcScriptedChainRaftNode{
			gcTestRaftNode: &gcTestRaftNode{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}},
			// First call: the sweep snapshot. Second: the reclaim re-check.
			answer: func(call int) ([]types.VersionMeta, error) {
				if call == 1 {
					return []types.VersionMeta{{VersionID: 7, KBID: "kb-1"}}, nil
				}
				return nil, errors.New("raft: no leader")
			},
			tail: 8, // moved since the snapshot → the chain is re-read
		},
		ChunkDocMapper: cdm,
		DocStore:       ds,
		ChunkStore:     cs,
	})
	seedDeadChunk(t, cdm, ds, cs, "kb-1")

	if err := gc.Sweep(ctx); err == nil {
		t.Fatal("Sweep must report the re-check's chain-read failure")
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-dead"); err != nil || !ok {
		t.Errorf("a failed re-check read reclaimed a chunk; exists=%v err=%v", ok, err)
	}
}

// TestChunkGarbageCollector_EmptyChainAtRecheckKeepsChunk: a re-read that comes
// back empty means the knowledge base lost every version (a concurrent KB
// deletion). That data belongs to the KB-deletion path's full cleanup, not to
// this sweep.
func TestChunkGarbageCollector_EmptyChainAtRecheckKeepsChunk(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcScriptedChainRaftNode{
			gcTestRaftNode: &gcTestRaftNode{kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}}},
			answer: func(call int) ([]types.VersionMeta, error) {
				if call == 1 {
					return []types.VersionMeta{{VersionID: 7, KBID: "kb-1"}}, nil
				}
				return nil, nil
			},
			tail: 8,
		},
		ChunkDocMapper: cdm,
		DocStore:       ds,
		ChunkStore:     cs,
	})
	seedDeadChunk(t, cdm, ds, cs, "kb-1")

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-dead"); err != nil || !ok {
		t.Errorf("a reclaim run against an empty chain deleted the chunk; exists=%v err=%v", ok, err)
	}
}

// TestChunkGarbageCollector_ReclaimFailureIsReported: a chunk whose vector could
// not be deleted must surface, so the sweep's caller logs it and the next sweep
// retries instead of the failure passing silently.
func TestChunkGarbageCollector_ReclaimFailureIsReported(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := &gcFailingDeleteChunkStore{gcChunkStore: newGCChunkStore()}
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": {{VersionID: 7, KBID: "kb-1"}}},
		},
		ChunkDocMapper: cdm,
		DocStore:       ds,
		ChunkStore:     cs,
	})
	seedDeadChunk(t, cdm, ds, cs.gcChunkStore, "kb-1")

	if err := gc.Sweep(ctx); err == nil {
		t.Fatal("Sweep must report a failed vector delete")
	}
}

// TestChunkGarbageCollector_NoLiveVersionSkipsTheKB: a knowledge base whose
// chain is empty has nothing reachable, so the sweep does not even scan its
// chunks — their bytes belong to the KB-deletion path.
func TestChunkGarbageCollector_NoLiveVersionSkipsTheKB(t *testing.T) {
	ctx := context.Background()
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(t.TempDir())
	if err != nil {
		t.Fatalf("chunkdoc: %v", err)
	}
	defer cdm.Close()

	ds := newTestDocStore()
	cs := newGCChunkStore()
	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		RaftNode: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": nil},
		},
		ChunkDocMapper: cdm,
		DocStore:       ds,
		ChunkStore:     cs,
	})
	seedDeadChunk(t, cdm, ds, cs, "kb-1")

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ok, err := cs.Exists(ctx, "kb-1", "chunk-dead"); err != nil || !ok {
		t.Errorf("a KB with no live version had its chunks swept; exists=%v err=%v", ok, err)
	}
}
