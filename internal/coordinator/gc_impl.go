package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// defaultGCSweepIntervalSec is used when the config leaves the interval
// unset or non-positive.
const defaultGCSweepIntervalSec = 300

// ChunkGarbageCollectorImpl is the real ChunkGarbageCollector.
type ChunkGarbageCollectorImpl struct {
	cfg    ChunkGarbageCollectorConfig
	logger *zap.Logger
}

// NewChunkGarbageCollectorImpl constructs a ChunkGarbageCollectorImpl,
// defaulting a non-positive sweep interval to 5 minutes. When
// cfg.WriteMu is nil a private lock is allocated — sufficient for tests
// without concurrent writes; production wiring must inject the same
// mutex that WriteCoordinatorConfig.WriteMu points to.
func NewChunkGarbageCollectorImpl(cfg ChunkGarbageCollectorConfig) *ChunkGarbageCollectorImpl {
	if cfg.SweepIntervalSec <= 0 {
		cfg.SweepIntervalSec = defaultGCSweepIntervalSec
	}
	if cfg.WriteMu == nil {
		cfg.WriteMu = &sync.Mutex{}
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

	for _, kb := range kbs {
		if err := g.sweepKB(ctx, kb.KBID); err != nil {
			return err
		}
	}
	return nil
}

// sweepKB reclaims every orphan chunk of a single knowledge base.
func (g *ChunkGarbageCollectorImpl) sweepKB(ctx context.Context, kbID string) error {
	versions, err := g.cfg.RaftNode.ListVersions(ctx, kbID)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListVersions(%s): %w", kbID, err)
	}
	if len(versions) == 0 {
		return nil // no live version: nothing reachable, nothing to scan
	}

	// The newest version is the definitive "current" view of the KB: a
	// document absent (deleted or tombstoned) at the newest version can
	// never be returned by a query of any live version.
	// NOTE: this snapshot judgement is only the first, lock-free pass that
	// produces reclaim CANDIDATES. The reclaim itself (reclaimOrphan)
	// re-checks orphanhood against the raft CURRENT version under the
	// shared write mutex — see its doc comment for the race rationale.
	snapshotMaxVersion := maxVersionID(versions)

	chunkIDs, err := g.cfg.ChunkDocMapper.ListChunkIDs(ctx, kbID)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListChunkIDs(%s): %w", kbID, err)
	}

	for _, chunkID := range chunkIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		orphan, err := g.isOrphanChunk(ctx, kbID, chunkID, snapshotMaxVersion)
		if err != nil {
			return err
		}
		if !orphan {
			continue
		}

		// Candidate orphan under the snapshot; reclaim re-validates under
		// the current version inside the write critical section.
		if err := g.reclaimOrphan(ctx, kbID, chunkID); err != nil {
			return err
		}
	}
	return nil
}

// reclaimOrphan deletes one snapshot-candidate orphan chunk after
// re-validating orphanhood against the raft CURRENT version, holding the
// write mutex shared with WriteCoordinatorImpl (WriteMu == txnMu).
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
func (g *ChunkGarbageCollectorImpl) reclaimOrphan(ctx context.Context, kbID, chunkID string) error {
	g.cfg.WriteMu.Lock()
	defer g.cfg.WriteMu.Unlock()

	// Re-check against the CURRENT raft version list, not the
	// start-of-sweep snapshot.
	versions, err := g.cfg.RaftNode.ListVersions(ctx, kbID)
	if err != nil {
		return fmt.Errorf("chunk-gc: reclaim re-check ListVersions(%s): %w", kbID, err)
	}
	if len(versions) == 0 {
		// No live version at reclaim time (concurrent KB deletion): leave
		// the data to the KB-deletion path's full cleanup.
		return nil
	}
	orphan, err := g.isOrphanChunk(ctx, kbID, chunkID, maxVersionID(versions))
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
	docIDs, err := g.cfg.ChunkDocMapper.ListDocIDs(ctx, kbID, chunkID)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListDocIDs(%s,%s): %w", kbID, chunkID, err)
	}
	for _, docID := range docIDs {
		if err := g.cfg.ChunkDocMapper.DeleteByDoc(ctx, kbID, docID); err != nil {
			return fmt.Errorf("chunk-gc: DeleteByDoc(%s,%s): %w", kbID, docID, err)
		}
	}
	if err := g.cfg.ChunkStore.Delete(ctx, kbID, chunkID); err != nil {
		return fmt.Errorf("chunk-gc: ChunkStore.Delete(%s,%s): %w", kbID, chunkID, err)
	}
	if g.logger != nil {
		g.logger.Info("chunk GC reclaimed orphan chunk",
			zap.String("kb_id", kbID), zap.String("chunk_id", chunkID))
	}
	return nil
}

// maxVersionID returns the largest VersionID in versions. versions must
// be non-empty.
func maxVersionID(versions []types.VersionMeta) int64 {
	var max int64
	for _, v := range versions {
		if v.VersionID > max {
			max = v.VersionID
		}
	}
	return max
}

// isOrphanChunk reports whether every document mapped to chunkID is
// deleted or tombstoned at the KB's newest version.
func (g *ChunkGarbageCollectorImpl) isOrphanChunk(ctx context.Context, kbID, chunkID string, maxVersion int64) (bool, error) {
	docIDs, err := g.cfg.ChunkDocMapper.ListDocIDs(ctx, kbID, chunkID)
	if err != nil {
		return false, fmt.Errorf("chunk-gc: ListDocIDs(%s,%s): %w", kbID, chunkID, err)
	}
	if len(docIDs) == 0 {
		return false, nil // no mapping entries at all: nothing to reclaim here
	}
	for _, docID := range docIDs {
		content, err := g.cfg.DocStore.ReadAt(ctx, kbID, docID, maxVersion)
		if err != nil {
			// Not found or tombstone at the newest version: the document
			// is gone, so this reference cannot keep the chunk alive.
			continue
		}
		if len(content) > 0 {
			// At least one live document still references the chunk.
			return false, nil
		}
		// An empty non-error value is a tombstone at maxVersion.
	}
	return true, nil
}

var _ ChunkGarbageCollector = (*ChunkGarbageCollectorImpl)(nil)
