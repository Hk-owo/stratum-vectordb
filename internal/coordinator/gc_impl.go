package coordinator

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
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
// defaulting a non-positive sweep interval to 5 minutes.
func NewChunkGarbageCollectorImpl(cfg ChunkGarbageCollectorConfig) *ChunkGarbageCollectorImpl {
	if cfg.SweepIntervalSec <= 0 {
		cfg.SweepIntervalSec = defaultGCSweepIntervalSec
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
	var maxVersion int64
	for _, v := range versions {
		if v.VersionID > maxVersion {
			maxVersion = v.VersionID
		}
	}

	chunkIDs, err := g.cfg.ChunkDocMapper.ListChunkIDs(ctx, kbID)
	if err != nil {
		return fmt.Errorf("chunk-gc: ListChunkIDs(%s): %w", kbID, err)
	}

	for _, chunkID := range chunkIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		orphan, err := g.isOrphanChunk(ctx, kbID, chunkID, maxVersion)
		if err != nil {
			return err
		}
		if !orphan {
			continue
		}

		// Reclaim: mappings first, vector second (idempotent; see the
		// interface doc comment for crash-safety rationale).
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
	}
	return nil
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
