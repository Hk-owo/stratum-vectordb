package coordinator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
)

// WriteCoordinatorConfig bundles all dependencies and configuration for
// WriteCoordinatorImpl, following the constructor-injection convention.
type WriteCoordinatorConfig struct {
	MaxRetries          int
	RetryBaseIntervalMS int

	WAL      wal.WAL
	RaftNode raft.RaftNode

	Splitter    splitter.ChunkSplitter
	EmbedClient embed.EmbedClient

	// ChunkBloom is the per-KB chunk-existence bloom filter. Used on the
	// write path to skip ChunkStore.Exists round-trips for new chunks.
	ChunkBloom bloom.BloomFilter

	// VersionBloom persists each version's document bloom filter (one per
	// version). May be nil in tests that do not exercise filter
	// persistence; the write path then skips step 5 and the read path
	// rebuilds filters lazily from VersionDocList.
	VersionBloom *bloom.VersionBloomStore

	ChunkStore     chunkstore.ChunkStore
	ChunkDocMapper chunkdoc.ChunkDocMapper
	DocStore       docstore.DocStore
	VersionDocList versiondoc.VersionDocList
	IndexManager   index.IndexManager
}

// WriteCoordinatorImpl is the real WriteCoordinator implementation,
// orchestrating the full CreateVersion write path (steps 1-7) as documented
// in Stratum_接口设计v9.md "CreateVersion" and Stratum_设计文档v10.md "写路径".
type WriteCoordinatorImpl struct {
	cfg WriteCoordinatorConfig

	// txnMu serializes CreateVersion transactions end to end (BEGIN through
	// COMMIT) so the WAL record order is BEGIN -> VERSION_ID per
	// transaction with no interleaved BEGIN from a concurrent transaction —
	// the property FileWAL.rebuildIndex relies on to bind each VERSION_ID
	// to the correct transaction's replay input (see internal/wal/file.go).
	txnMu sync.Mutex
}

// NewWriteCoordinatorImpl constructs a WriteCoordinatorImpl.
func NewWriteCoordinatorImpl(cfg WriteCoordinatorConfig) *WriteCoordinatorImpl {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBaseIntervalMS <= 0 {
		cfg.RetryBaseIntervalMS = 100
	}
	return &WriteCoordinatorImpl{cfg: cfg}
}

// Execute implements WriteCoordinator.
func (c *WriteCoordinatorImpl) Execute(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) (int64, error) {
	// Serialize the whole transaction (BEGIN through COMMIT) so the WAL's
	// BEGIN -> VERSION_ID binding per version stays unambiguous (see
	// txnMu's doc comment).
	c.txnMu.Lock()
	defer c.txnMu.Unlock()

	// Step 1: WAL.WriteBegin, persisting the transaction's replay input
	// (kbID, parentVersionID, changes) so crash recovery can replay the
	// storage writes if this process dies before COMMIT.
	if err := c.cfg.WAL.WriteBegin(ctx, kbID, parentVersionID, changes); err != nil {
		return 0, fmt.Errorf("coordinator: WAL.WriteBegin: %w", err)
	}

	// Step 2: RaftNode.ProposeCreateVersion
	// (Raft apply phase internally calls WAL.WriteVersionID before
	// allocating the version in the state machine.)
	versionID, err := c.cfg.RaftNode.ProposeCreateVersion(ctx, kbID, parentVersionID)
	if err != nil {
		return 0, fmt.Errorf("coordinator: Raft propose: %w", err)
	}

	// Get KB metadata for chunking and embed config.
	kbMeta, err := c.cfg.RaftNode.GetKB(ctx, kbID)
	if err != nil {
		return 0, fmt.Errorf("coordinator: GetKB: %w", err)
	}

	// Steps 3-6: storage-layer writes + WAL COMMIT.
	docIDs, err := c.writeVersionStorage(ctx, kbID, parentVersionID, versionID, changes, kbMeta)
	if err != nil {
		return 0, err
	}

	// Step 6.5: commit the version's document-ID set hash so followers can
	// verify their DataSync pulls are complete (see sync.VerifyDocIDSet and
	// VersionMeta.DocIDSetHash). Non-fatal, like TriggerBuild below: a
	// failed/missed propose leaves the version without a digest and
	// followers fall back to best-effort pulls.
	if err := c.cfg.RaftNode.ProposeUpdateVersionSummary(ctx, versionID, stratinternalsync.ComputeDocIDSetHash(docIDs)); err != nil {
		_ = err // logged upstream; digest is an optimization for follower verification
	}

	// Step 7: IndexManager.TriggerBuild (asynchronous).
	if err := c.cfg.IndexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
		// TriggerBuild failure is not fatal — the version exists, storage
		// is durably written, and the build can be retried later.
		// Log and continue.
		_ = err
	}

	return versionID, nil
}

// ReplayVersionStorageWrites implements WriteCoordinator: replays steps
// 3-6 (plus summary + async build) for an already-committed version after
// a crash, without writing a BEGIN record or proposing a new version.
func (c *WriteCoordinatorImpl) ReplayVersionStorageWrites(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) error {
	c.txnMu.Lock()
	defer c.txnMu.Unlock()

	kbMeta, err := c.cfg.RaftNode.GetKB(ctx, kbID)
	if err != nil {
		return fmt.Errorf("coordinator: replay GetKB: %w", err)
	}

	docIDs, err := c.writeVersionStorage(ctx, kbID, parentVersionID, versionID, changes, kbMeta)
	if err != nil {
		return fmt.Errorf("coordinator: replay version %d storage writes: %w", versionID, err)
	}

	if err := c.cfg.RaftNode.ProposeUpdateVersionSummary(ctx, versionID, stratinternalsync.ComputeDocIDSetHash(docIDs)); err != nil {
		_ = err
	}
	if err := c.cfg.IndexManager.TriggerBuild(ctx, kbID, versionID); err != nil {
		_ = err
	}
	return nil
}

// writeVersionStorage executes the synchronous storage-layer steps of the
// write path (3-6) for (kbID, versionID): per-change split/embed/write,
// the version's full document-ID set, the version-document bloom filter,
// and the WAL COMMIT. Shared by Execute and the crash-recovery replay;
// every write is idempotent, so re-running it for an already-partially-
// written version is always safe. Returns the version's sorted docIDs.
func (c *WriteCoordinatorImpl) writeVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta) ([]string, error) {
	// Step 3: Per changed document: split -> embed -> per chunk: bloom
	// test -> exists confirm -> write -> chunk-doc map -> doc store.
	for _, change := range changes {
		switch change.Op {
		case types.ChangeOpAdd, types.ChangeOpUpdate:
			if err := c.writeDocument(ctx, kbID, versionID, change, kbMeta); err != nil {
				return nil, err
			}
		case types.ChangeOpDelete:
			// Write a tombstone for the deleted document.
			if err := c.retry(ctx, func() error {
				return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, nil)
			}); err != nil {
				return nil, fmt.Errorf("coordinator: write tombstone for %s: %w", change.DocID, err)
			}
		}
	}

	// Step 4: VersionDocList.Write — compute the full document set for
	// the new version from the parent's set + this version's changes.
	docIDs, err := c.writeVersionDocList(ctx, kbID, parentVersionID, versionID, changes)
	if err != nil {
		return nil, err
	}

	// Step 5: version-document bloom filter, built from the full docID
	// set and persisted to disk. Non-fatal: a failed persist leaves the
	// filter absent, and the read path rebuilds it lazily from
	// VersionDocList on demand.
	if c.cfg.VersionBloom != nil {
		if _, err := c.cfg.VersionBloom.BuildAndPersist(kbID, versionID, docIDs); err != nil {
			_ = err // non-fatal; read path rebuilds lazily
		}
	}

	// Step 6: WAL.WriteCommit
	if err := c.cfg.WAL.WriteCommit(ctx, versionID); err != nil {
		return nil, fmt.Errorf("coordinator: WAL.WriteCommit: %w", err)
	}

	return docIDs, nil
}

// writeDocument handles a single ADD or UPDATE document change: split,
// embed, write chunks + mappings + doc content.
func (c *WriteCoordinatorImpl) writeDocument(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta) error {
	// Split
	chunks := c.cfg.Splitter.Split(change.Content, kbMeta.ChunkWindowSize, kbMeta.ChunkOverlapSize, kbMeta.EmbedConfig.ModelID)

	if len(chunks) == 0 {
		// No chunks produced (e.g. empty content): just write the document
		// content (or tombstone for empty content).
		return c.retry(ctx, func() error {
			return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
		})
	}

	// Embed
	var vectors map[string][]float32
	err := c.retry(ctx, func() error {
		var inner error
		vectors, inner = c.cfg.EmbedClient.Embed(ctx, chunks)
		return inner
	})
	if err != nil {
		return fmt.Errorf("coordinator: embed chunks for doc %s: %w", change.DocID, err)
	}

	// Per chunk: bloom check -> exists confirm (if needed) -> write + bloom add -> chunk-doc map
	for _, chunk := range chunks {
		vector, ok := vectors[chunk.ChunkID]
		if !ok {
			return fmt.Errorf("coordinator: embed did not return vector for chunk %s", chunk.ChunkID)
		}

		if err := c.writeChunk(ctx, kbID, chunk, vector); err != nil {
			return err
		}

		// Write chunk-doc mapping (idempotent).
		if err := c.retry(ctx, func() error {
			return c.cfg.ChunkDocMapper.Write(ctx, kbID, chunk.ChunkID, change.DocID)
		}); err != nil {
			return fmt.Errorf("coordinator: chunk-doc map write: %w", err)
		}
	}

	// Write document content.
	if err := c.retry(ctx, func() error {
		return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
	}); err != nil {
		return fmt.Errorf("coordinator: doc store write: %w", err)
	}

	return nil
}

// writeChunk handles a single chunk: bloom check -> authoritative exists
// confirm -> write + bloom add (if truly new).
func (c *WriteCoordinatorImpl) writeChunk(ctx context.Context, kbID string, chunk types.Chunk, vector []float32) error {
	// Bloom filter test.
	if c.cfg.ChunkBloom.Test(chunk.ChunkID) {
		// Bloom says "maybe exists" — confirm against the authoritative store.
		exists, err := c.cfg.ChunkStore.Exists(ctx, kbID, chunk.ChunkID)
		if err != nil {
			return fmt.Errorf("coordinator: ChunkStore.Exists for %s: %w", chunk.ChunkID, err)
		}
		if exists {
			return nil // already stored; nothing to do
		}
		// False positive: write it now.
	}

	// Write chunk to vecstore.
	if err := c.retry(ctx, func() error {
		return c.cfg.ChunkStore.Write(ctx, kbID, chunk.ChunkID, vector)
	}); err != nil {
		return fmt.Errorf("coordinator: ChunkStore.Write for %s: %w", chunk.ChunkID, err)
	}

	// Add to bloom filter.
	c.cfg.ChunkBloom.Add(chunk.ChunkID)
	return nil
}

// writeVersionDocList computes the new version's full document ID set by
// taking the parent version's set and applying this version's changes.
// It writes the set into VersionDocList and returns the sorted docIDs so
// the caller can compute the version's document-ID set digest.
func (c *WriteCoordinatorImpl) writeVersionDocList(ctx context.Context, kbID string, parentVersionID, newVersionID int64, changes []types.DocChange) ([]string, error) {
	// Get parent version's full doc set.
	parentDocs := make(map[string]bool)
	if parentVersionID != 0 {
		docIDs, err := c.cfg.VersionDocList.ListDocIDs(ctx, kbID, parentVersionID)
		if err != nil {
			return nil, fmt.Errorf("coordinator: list parent version %d docs: %w", parentVersionID, err)
		}
		for _, id := range docIDs {
			parentDocs[id] = true
		}
	}

	// Apply changes.
	for _, ch := range changes {
		switch ch.Op {
		case types.ChangeOpAdd, types.ChangeOpUpdate:
			parentDocs[ch.DocID] = true
		case types.ChangeOpDelete:
			delete(parentDocs, ch.DocID)
		}
	}

	// Write each doc ID to the new version.
	docIDs := make([]string, 0, len(parentDocs))
	for docID := range parentDocs {
		docIDs = append(docIDs, docID)
		if err := c.retry(ctx, func() error {
			return c.cfg.VersionDocList.Write(ctx, kbID, newVersionID, docID)
		}); err != nil {
			return nil, fmt.Errorf("coordinator: version doc list write: %w", err)
		}
	}
	sort.Strings(docIDs)

	return docIDs, nil
}

// retry executes fn with exponential backoff up to MaxRetries times.
func (c *WriteCoordinatorImpl) retry(ctx context.Context, fn func() error) error {
	base := time.Duration(c.cfg.RetryBaseIntervalMS) * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < c.cfg.MaxRetries {
			backoff := base * time.Duration(int64(math.Pow(2, float64(attempt))))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", c.cfg.MaxRetries+1, lastErr)
}

var _ WriteCoordinator = (*WriteCoordinatorImpl)(nil)
