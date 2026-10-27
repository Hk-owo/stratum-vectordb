package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// defaultGCSweepIntervalSec is used when the config leaves the interval unset or
// non-positive.
//
// An hour, not five minutes: the reclaim is event-driven — a version's deletion
// triggers ReclaimVersionChunks, which is when a chunk can actually BECOME
// orphaned — so the sweep only has to catch leftovers nothing names any more (a
// write path that crashed between the vector write and the mapping write). That
// job does not need a short interval, and every sweep costs one whole-chain read
// per knowledge base plus a scan of every chunk mapping.
const defaultGCSweepIntervalSec = 3600

// ChunkGarbageCollectorImpl is the real ChunkGarbageCollector.
type ChunkGarbageCollectorImpl struct {
	cfg    ChunkGarbageCollectorConfig
	logger *zap.Logger
}

// NewChunkGarbageCollectorImpl constructs a ChunkGarbageCollectorImpl,
// defaulting a non-positive sweep interval to 5 minutes. When cfg.Locks
// is nil a private set is allocated — sufficient for tests without
// concurrent writes; production wiring must inject the same instance that
// WriteCoordinatorConfig.Locks points to, or a sweep could erase data a
// concurrent write to the same knowledge base committed.
func NewChunkGarbageCollectorImpl(cfg ChunkGarbageCollectorConfig) *ChunkGarbageCollectorImpl {
	if cfg.SweepIntervalSec <= 0 {
		cfg.SweepIntervalSec = defaultGCSweepIntervalSec
	}
	if cfg.Locks == nil {
		cfg.Locks = NewKBLockSet()
	}
	return &ChunkGarbageCollectorImpl{cfg: cfg}
}

// SetLogger attaches a logger for sweep progress and errors. Optional:
// without it the collector runs silently.
func (g *ChunkGarbageCollectorImpl) SetLogger(l *zap.Logger) {
	g.logger = l
}

// Run implements ChunkGarbageCollector.
func (g *ChunkGarbageCollectorImpl) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(g.cfg.SweepIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := g.Sweep(ctx); err != nil && g.logger != nil {
				g.logger.Warn("chunk GC sweep failed", zap.Error(err))
			}
		}
	}
}

// Sweep implements ChunkGarbageCollector.
func (g *ChunkGarbageCollectorImpl) Sweep(ctx context.Context) error {
	kbs, err := g.cfg.RaftNode.ListKnowledgeBases(ctx)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListKnowledgeBases: %w", err)
	}

	// One knowledge base's failure must not stop the rest (M18 of
	// docs/code-review-2026-10-18.md). Returning at the first error meant a single KB
	// whose LastVersionID (or one of its chunk deletes) kept failing left every KB
	// after it in the list unreclaimed — for good, every sweep — while the log showed
	// one warning. Same policy as deleting_sweep.go: note it, keep going, report the
	// first failure at the end.
	var firstErr error
	for _, kb := range kbs {
		if err := ctx.Err(); err != nil {
			// Shutting down: the next sweep starts from the same list.
			return err
		}
		if err := g.sweepKB(ctx, kb.KBID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("chunk-gc: sweep %s: %w", kb.KBID, err)
			}
			if g.logger != nil {
				g.logger.Warn("chunk GC: could not finish this knowledge base; the next sweep retries it",
					zap.String("kb_id", kb.KBID), zap.Error(err))
			}
		}
	}
	return firstErr
}

// sweepKB reclaims every orphan chunk of a single knowledge base.
func (g *ChunkGarbageCollectorImpl) sweepKB(ctx context.Context, kbID string) error {
	// The LIVE CHAIN, not the newest version. A version stays queryable
	// (QueryRequest.version_id) until its metadata is removed, so a document
	// that the newest version no longer contains can still be part of an older
	// live version — and that version's index rebuild reads its chunk vectors
	// back. Judging by the tail alone reclaimed them
	// (TestChunkGC_KeepsChunkReferencedByOlderLiveVersion).
	//
	// The cost is one whole-chain read per knowledge base per sweep, on a node
	// whose RaftNode is remote. That is the price of the correct liveness
	// question; a cheap answer needs a control-layer read that returns the id
	// list (or a per-document "is this visible anywhere" judgement) instead of
	// full VersionMeta rows, which docs/known-gaps.md §F already asks for.
	// Keeping it to ONE read per KB is why the reclaim pass re-uses this
	// snapshot unless the tail moved (see reclaimOrphan).
	snapshot, err := g.liveVersionIDs(ctx, kbID, 0)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListVersions(%s): %w", kbID, err)
	}
	if len(snapshot) == 0 {
		// No live version (an empty KB, or every version is Deleting and its own
		// reclaim is running): nothing is reachable from a query, and the chunks
		// that can still be orphaned are reclaimed by the delete flow itself.
		return nil
	}

	// This snapshot judgement is only the first, lock-free pass that produces
	// reclaim CANDIDATES. The reclaim itself (reclaimOrphan) re-checks
	// orphanhood against the raft CURRENT version under the shared write
	// mutex — see its doc comment for the race rationale.

	chunkIDs, err := g.cfg.ChunkDocMapper.ListChunkIDs(ctx, kbID)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListChunkIDs(%s): %w", kbID, err)
	}

	for _, chunkID := range chunkIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		orphan, _, err := g.isOrphanChunk(ctx, kbID, chunkID, snapshot)
		if err != nil {
			return err
		}
		if !orphan {
			continue
		}

		// Candidate orphan under the snapshot; reclaim re-validates under
		// the current chain inside the write critical section.
		if err := g.reclaimOrphan(ctx, kbID, chunkID, snapshot); err != nil {
			return err
		}
	}
	return nil
}

// liveVersionIDs returns kbID's live version ids in ascending order — the set
// a query can still name, and therefore the set whose references keep a chunk
// alive.
//
// Two kinds of version are NOT live for this purpose:
//
//   - one marked Deleting (or named by exceptVersionID, the version whose own
//     reclamation is asking). It cannot be queried, cannot be a CreateVersion
//     parent, and its own bytes are being reclaimed right now, so counting it
//     would keep every chunk it references alive until its metadata row is
//     removed — i.e. the deletion would never free anything on its own pass;
//   - one the control layer no longer holds metadata for at all (absent from
//     ListVersions).
func (g *ChunkGarbageCollectorImpl) liveVersionIDs(ctx context.Context, kbID string, exceptVersionID int64) ([]int64, error) {
	versions, err := g.cfg.RaftNode.ListVersions(ctx, kbID)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(versions))
	for _, v := range versions {
		if v.Deleting || v.VersionID == exceptVersionID {
			continue
		}
		ids = append(ids, v.VersionID)
	}
	// Sorted defensively: the local state machine hands them out in allocation
	// order, but isOrphanChunk's read order (newest first) assumes ascending,
	// and a remote shape is not obliged to preserve it.
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// reclaimOrphan deletes one snapshot-candidate orphan chunk after
// re-validating orphanhood against the raft CURRENT version, holding the
// write lock shared with WriteCoordinatorImpl (ChunkGarbageCollectorConfig.Locks
// == WriteCoordinatorConfig.Locks), taken per knowledge base.
//
// This closes the stale-snapshot race: the first-pass judgement in
// sweepKB uses the version list taken at sweep start, and a newer
// version can commit (and finish its storage-layer writes) between that
// snapshot and the reclaim. Re-checking inside the write transaction's
// critical section makes the delete non-interleavable with concurrent
// writes:
//   - a write committed before the re-check is visible to it, so the
//     document reads alive and the chunk is kept;
//   - a write arriving after the reclaim simply re-creates the
//     content-addressed vector and the mapping (idempotent write path).
//
// The vector delete (a vecstore gRPC round-trip) intentionally stays
// inside the lock: the write path skips ChunkStore.Write when
// ChunkStore.Exists reports true, so deleting the vector outside the
// lock could race a concurrent write into a dangling mapping (mapping
// present, vector gone). The lock is taken per orphan chunk, so a
// concurrent CreateVersion is blocked only for this single chunk's
// re-check + local deletes + one RPC (milliseconds), never for the whole
// sweep.
//
// Why the re-check itself cannot leave the critical section (M9 of
// docs/code-review-2026-09-24.md suggests moving the confirmation stage out): the
// cheap-looking shortcut is to compare the current tail against the snapshot's and
// skip the document re-read when they match. That is wrong, and a test says so
// (TestGC_SharedWriteMu_ConcurrentWriteAndSweep_KeepsLiveChunk): a version's data
// writes land AFTER its id is allocated, so the tail can be unchanged while a
// document that keeps this chunk alive becomes readable. The document read IS the
// confirmation, so it stays inside the lock. What the tail comparison below is
// good for is narrower: deciding whether the SNAPSHOT's version set still covers
// every live version, i.e. whether the chain has to be re-read at all. What M9
// can remove from the lock is the SECOND scan of those documents — see the docIDs
// reuse below.
func (g *ChunkGarbageCollectorImpl) reclaimOrphan(ctx context.Context, kbID, chunkID string, snapshot []int64) error {
	unlock := g.cfg.Locks.Lock(kbID)
	defer unlock()

	// Re-check against the CURRENT raft chain, not the start-of-sweep snapshot.
	// The tail answers "did any version commit since the snapshot?" in one O(1)
	// call, which is the only way the chain can GAIN a version: only a new
	// allocation puts an id above the snapshot's highest one. A chain that merely
	// LOST versions (a deletion finished, its metadata row removed) is left on the
	// snapshot's set on purpose — a smaller live set only makes "orphan" easier to
	// satisfy, so judging against the larger set keeps a chunk one round longer
	// instead of ever reclaiming one too early.
	tail, err := g.cfg.RaftNode.LastVersionID(ctx, kbID)
	if err != nil {
		return fmt.Errorf("chunk-gc: reclaim re-check LastVersionID(%s): %w", kbID, err)
	}
	if tail == 0 {
		// No live version at reclaim time (concurrent KB deletion): leave
		// the data to the KB-deletion path's full cleanup.
		return nil
	}
	live := snapshot
	if tail != snapshot[len(snapshot)-1] {
		// A version committed after the snapshot: it may resurrect the
		// document, and the snapshot cannot see it, so re-read the chain.
		live, err = g.liveVersionIDs(ctx, kbID, 0)
		if err != nil {
			return fmt.Errorf("chunk-gc: reclaim re-check ListVersions(%s): %w", kbID, err)
		}
		if len(live) == 0 {
			return nil
		}
	}
	orphan, docIDs, err := g.isOrphanChunk(ctx, kbID, chunkID, live)
	if err != nil {
		return fmt.Errorf("chunk-gc: reclaim re-check (%s,%s): %w", kbID, chunkID, err)
	}
	if !orphan {
		// A newer version resurrected the document between the snapshot
		// judgement and now: keep the chunk.
		return nil
	}

	// Confirmed orphan: mappings first, vector second (idempotent; see
	// the interface doc comment for crash-safety rationale). Both inside
	// the write critical section — see the method doc comment.
	return g.deleteChunk(ctx, kbID, chunkID, docIDs, "reclaimed orphan chunk")
}

// ReclaimVersionChunks implements VersionChunkReclaimer: it reclaims the chunks
// that versionID was the last live referent of.
//
// This is the event-driven half, reached from the delete flow BEFORE the
// version's document list is dropped (that list names the candidate chunks):
// every document of the deleted version is turned into chunk ids through the
// mapping's reverse index, and each of those chunks is judged against the
// versions that remain live. A chunk that no live version can read is reclaimed
// on the spot, so its bytes go the moment the last version referencing it is
// deleted — instead of waiting for a periodic sweep to rediscover the fact.
//
// Costs: two prefix scans (the version's doc set, its chunks) plus one ReadAt
// per (chunk, document) that is dead at the newest live version. Documents
// shared with a surviving version answer on the first read and keep their
// chunk, which is the common case.
//
// Idempotent, and a no-op when this node holds no document list for the version
// (a replica that never received it) — the delete flow can call it any number of
// times, from a retry or from the tombstone's own retry loop.
func (g *ChunkGarbageCollectorImpl) ReclaimVersionChunks(ctx context.Context, kbID string, versionID int64) error {
	if versionID <= 0 {
		return fmt.Errorf("chunk-gc: reclaim version chunks %s: invalid version id %d", kbID, versionID)
	}
	if g.cfg.VersionDocList == nil {
		return nil // no local document list: nothing of this version lives here
	}

	// The candidate set is read before the delete flow drops the version's
	// document list; it is a superset of what can actually be reclaimed, and the
	// liveness judgement below narrows it. Out of the lock: these are two prefix
	// scans, and a stale candidate set is harmless — every candidate is re-judged
	// under the lock.
	docIDs, err := g.cfg.VersionDocList.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("chunk-gc: version doc set %s v%d: %w", kbID, versionID, err)
	}
	if len(docIDs) == 0 {
		return nil
	}
	chunkIDs, err := g.cfg.ChunkDocMapper.ListChunkIDsByDocs(ctx, kbID, docIDs)
	if err != nil {
		return fmt.Errorf("chunk-gc: chunks of %s v%d: %w", kbID, versionID, err)
	}
	if len(chunkIDs) == 0 {
		return nil
	}

	unlock := g.cfg.Locks.Lock(kbID)
	defer unlock()

	// Judged INSIDE the lock, like reclaimOrphan: a version can commit between
	// the candidate scan and now, resurrect one of these documents, and the write
	// path's storage writes land after its id is allocated — so the read is the
	// confirmation.
	live, err := g.liveVersionIDs(ctx, kbID, versionID)
	if err != nil {
		return fmt.Errorf("chunk-gc: live versions of %s while reclaiming v%d: %w", kbID, versionID, err)
	}

	for _, chunkID := range chunkIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		orphan, chunkDocIDs, err := g.isOrphanChunk(ctx, kbID, chunkID, live)
		if err != nil {
			return err
		}
		if !orphan {
			continue
		}
		if err := g.deleteChunk(ctx, kbID, chunkID, chunkDocIDs, "reclaimed chunk of a deleted version"); err != nil {
			return err
		}
	}
	return nil
}

// deleteChunk removes one confirmed-orphan chunk: its mappings first, its vector
// second.
//
// docIDs is the list isOrphanChunk already scanned, reused rather than re-scanned
// (M9 of docs/code-review-2026-09-24.md). DeleteMapping, not DeleteByDoc, is the
// delete (H3 of docs/code-review-2026-10-18.md): DeleteByDoc removes the
// document's mapping to every chunk it has, so the document's OTHER chunks would
// vanish from the sweep's candidate set (found by scanning the forward keyspace)
// while their vectors stayed in the store forever. Reclaiming one chunk may only
// unpin that chunk; the siblings stay visible and get their own pass.
//
// Both deletions are idempotent and the order is crash-safe (see the interface
// doc comment): a crash between them leaves a vector no mapping names, which no
// caller scans again.
func (g *ChunkGarbageCollectorImpl) deleteChunk(ctx context.Context, kbID, chunkID string, docIDs []string, reason string) error {
	for _, docID := range docIDs {
		if err := g.cfg.ChunkDocMapper.DeleteMapping(ctx, kbID, chunkID, docID); err != nil {
			return fmt.Errorf("chunk-gc: DeleteMapping(%s,%s,%s): %w", kbID, chunkID, docID, err)
		}
	}
	if err := g.cfg.ChunkStore.Delete(ctx, kbID, chunkID); err != nil {
		return fmt.Errorf("chunk-gc: ChunkStore.Delete(%s,%s): %w", kbID, chunkID, err)
	}
	if g.logger != nil {
		g.logger.Info("chunk GC "+reason,
			zap.String("kb_id", kbID), zap.String("chunk_id", chunkID))
	}
	return nil
}

// isOrphanChunk reports whether no document mapped to chunkID is readable from
// ANY of live's versions — i.e. every referencing document is tombstoned or
// absent in each live version — and returns the document list it consulted so
// the caller can reuse it instead of scanning the mapping again (M9 of
// docs/code-review-2026-09-24.md).
func (g *ChunkGarbageCollectorImpl) isOrphanChunk(ctx context.Context, kbID, chunkID string, live []int64) (bool, []string, error) {
	docIDs, err := g.cfg.ChunkDocMapper.ListDocIDs(ctx, kbID, chunkID)
	if err != nil {
		return false, nil, fmt.Errorf("chunk-gc: ListDocIDs(%s,%s): %w", kbID, chunkID, err)
	}
	if len(docIDs) == 0 {
		return false, nil, nil // no mapping entries at all: nothing to reclaim here
	}
	for _, docID := range docIDs {
		alive, err := g.docReadableFromAny(ctx, kbID, docID, live)
		if err != nil {
			return false, nil, err
		}
		if alive {
			// At least one live version still reads this document, so the
			// chunk is reachable: keep it (and its mapping).
			return false, nil, nil
		}
	}
	return true, docIDs, nil
}

// docReadableFromAny reports whether docID still resolves to content in at
// least one of live's versions.
//
// Newest first, because that is the cheap answer: for the overwhelming majority
// of mappings the document is alive at the chain's tail, so the common case
// costs ONE read. Only references that are already dead at the newest version
// walk down the chain — and those are exactly the ones a sweep is about to
// reclaim, so that walk happens once per dead document, not once per sweep.
//
// A storage failure is reported, never read as a deletion: anything other than
// "this document has no entry at or before this version" is the store being
// unable to answer, and treating that as "the document is gone" reclaims a LIVE
// document's chunks on the strength of a read failure (H2 of
// docs/code-review-2026-10-18.md).
func (g *ChunkGarbageCollectorImpl) docReadableFromAny(ctx context.Context, kbID, docID string, live []int64) (bool, error) {
	for i := len(live) - 1; i >= 0; i-- {
		v := live[i]
		content, err := g.cfg.DocStore.ReadAt(ctx, kbID, docID, v)
		if err != nil {
			if !errors.Is(err, stratumerrors.ErrVersionNotFound) {
				return false, fmt.Errorf(
					"chunk-gc: read %s/%s at live version %d: %w", kbID, docID, v, err)
			}
			continue
		}
		if len(content) > 0 {
			return true, nil
		}
		// An empty non-error value is a tombstone at v: the document is gone
		// as of that version, so keep walking down to the older live ones.
	}
	return false, nil
}

var (
	_ ChunkGarbageCollector = (*ChunkGarbageCollectorImpl)(nil)
	_ VersionChunkReclaimer = (*ChunkGarbageCollectorImpl)(nil)
)
