package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	stratumync "stratum/internal/sync"
	"stratum/internal/types"
)

// WriteVersionStorage is the storage layer's data-write step for one version
// (steps 3-5): it resolves the KB metadata itself and performs the per-change
// split/embed/writes plus the version document set and bloom filter. It does
// NOT frame the WAL transaction (BEGIN/COMMIT) — the caller owns that framing,
// which is what lets the storage layer's DataPlane run the transaction as its
// own (docs/参考/历史/Stratum_设计文档v13.md §7.12).
// docStages accumulates the per-phase cost of the per-document step across its
// concurrent workers.
//
// The values are SUMS, not wall clock, and that is deliberate: with
// maxConcurrentDocumentWrites in flight they can exceed documents_us, and a sum is
// what answers "where did the work go" — a question a wall-clock number per
// document cannot answer when eight of them overlap.
//
// Why it matters: this step is where an embedder round trip sits, and the write
// path's own notes measure it at ~10 ms of the ~14.6 ms it takes to write one
// document. embed_us is what makes that visible from the outside, and
// embed_calls is its denominator.
type docStages struct {
	splitUs    atomic.Int64
	embedUs    atomic.Int64
	storeUs    atomic.Int64
	embedCalls atomic.Int64

	// The inside of store_us, which the write path had as one number covering
	// three stores with very different costs: the vecstore gRPC round trip per
	// NEW chunk (writeChunkUs), the chunk-existence checks per chunk
	// (chunkCheckUs — a bloom hit costs nothing, a miss costs a gRPC Exists), and
	// the document body (docWriteUs). Which of them dominates decides what a
	// batch API would have to batch; guessing at it would mean rebuilding the C++
	// vecstore for nothing.
	writeChunkUs atomic.Int64
	writeChunkN  atomic.Int64
	chunkCheckUs atomic.Int64
	docWriteUs   atomic.Int64
}

// writeStorageStages is where one version's storage writes went, split out of
// plane's `write: stage timings` storage_us.
//
// Why: storage_us is the largest piece of a write transaction's local half and
// was one opaque number covering four unrelated steps — a control-plane metadata
// read, the per-document split/embed/store, the version's full document-ID set,
// and its bloom filter. Which dominates decides what to optimise. flush is the
// vecstore WAL fsync that has to happen before the caller's COMMIT, counted
// apart because it is the one stage whose cost does not scale with the write.
type writeStorageStages struct {
	getKB     time.Duration
	documents time.Duration
	docList   time.Duration
	bloom     time.Duration
	flush     time.Duration
	docs      docStages
}

// WriteVersionStorage implements plane.VersionWriteExecutor: the storage-layer
// steps of one write (steps 3-5, plus the vecstore flush that has to precede the
// caller's WAL COMMIT): it resolves the KB metadata itself and performs the
// per-change split/embed/writes plus the version document set and bloom filter.
// It does
// NOT frame the WAL transaction (BEGIN/COMMIT) — the caller owns that framing,
// which is what lets the storage layer's DataPlane run the transaction as its
// own (docs/参考/历史/Stratum_设计文档v13.md §7.12).
func (c *WriteCoordinatorImpl) WriteVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) ([]string, error) {
	// Per-phase timings of one version's storage writes, at debug level.
	//
	// Why: this call IS plane's `write: stage timings` storage_us, the largest
	// sub-stage of local_us — itself the second largest stage of a write
	// transaction (p50 1.34 s for a 2,000-document batch, 2.68 s for 8,000). The
	// numbers here say which of its four steps that time belongs to, and, for the
	// per-document step, which phase of a document dominated it.
	storageStart := time.Now()
	var st writeStorageStages
	defer func() {
		c.logger().Debug("write: storage timings",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("changes", len(changes)),
			zap.Int64("getkb_us", st.getKB.Microseconds()),
			zap.Int64("documents_us", st.documents.Microseconds()),
			zap.Int64("doclist_us", st.docList.Microseconds()),
			zap.Int64("bloom_us", st.bloom.Microseconds()),
			zap.Int64("flush_us", st.flush.Microseconds()),
			zap.Int64("split_us", st.docs.splitUs.Load()),
			zap.Int64("embed_us", st.docs.embedUs.Load()),
			zap.Int64("embed_calls", st.docs.embedCalls.Load()),
			zap.Int64("chunkcheck_us", st.docs.chunkCheckUs.Load()),
			zap.Int64("writechunk_us", st.docs.writeChunkUs.Load()),
			zap.Int64("writechunk_calls", st.docs.writeChunkN.Load()),
			zap.Int64("docwrite_us", st.docs.docWriteUs.Load()),
			zap.Int64("store_us", st.docs.storeUs.Load()),
			zap.Int64("total_us", time.Since(storageStart).Microseconds()))
	}()

	stepStart := time.Now()
	kbMeta, err := c.cfg.RaftNode.GetKB(ctx, kbID)
	st.getKB = time.Since(stepStart)
	if err != nil {
		return nil, fmt.Errorf("coordinator: GetKB: %w", err)
	}
	return c.writeVersionStorage(ctx, kbID, parentVersionID, versionID, changes, kbMeta, &st)
}

// writeVersionStorage executes the synchronous storage-layer steps of the
// write path (3-5) for (kbID, versionID): per-change split/embed/write,
// the version's full document-ID set, the version-document bloom filter,
// and the vecstore WAL flush that must precede the caller's WAL COMMIT.
// Shared by Execute and the crash-recovery replay;
// every write is idempotent, so re-running it for an already-partially-
// written version is always safe. Returns the version's sorted docIDs.
func (c *WriteCoordinatorImpl) writeVersionStorage(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta, st *writeStorageStages) ([]string, error) {
	// Step 3: Per changed document: split -> embed -> per chunk: bloom
	// test -> exists confirm -> write -> chunk-doc map -> doc store.
	stepStart := time.Now()
	err := c.writeDocumentsConcurrently(ctx, kbID, versionID, changes, kbMeta, &st.docs)
	st.documents = time.Since(stepStart)
	if err != nil {
		return nil, err
	}

	// Step 4: VersionDocList.Write — compute the full document set for
	// the new version from the parent's set + this version's changes.
	stepStart = time.Now()
	docIDs, err := c.writeVersionDocList(ctx, kbID, parentVersionID, versionID, changes)
	st.docList = time.Since(stepStart)
	if err != nil {
		return nil, err
	}

	// Last step before returning: the durability barrier for step 3's chunk
	// writes, which the caller's WAL COMMIT is about to vouch for.
	//
	// The ordering is the whole point. COMMIT is fsynced, and it is the record
	// that says "this version's data has landed" — recovery reads a present
	// COMMIT as proof that no replay is needed. The vecstore writes that step 3
	// issued are sync=false (one fsync per chunk would dominate the write), so
	// without this call a power loss can keep the COMMIT and drop the vectors it
	// vouches for. That loss is silent and permanent: chunk vectors are the only
	// copy (the Go side stores none) and nothing reconciles a mapping that points
	// at a vector that is no longer there. One FlushWAL covers the version's whole
	// chunk set, so the cost is a single fsync amortised over every chunk in it.
	//
	// Failing here fails the write before the COMMIT is written, which is the safe
	// direction: the version's VERSION_ID-without-COMMIT record makes it
	// replayable, and the replay rewrites the chunks idempotently. Reporting
	// success instead would be the same lie as committing unflushed data.
	if c.cfg.ChunkStore != nil {
		flushStart := time.Now()
		if err := c.cfg.ChunkStore.FlushWAL(ctx); err != nil {
			return nil, fmt.Errorf("coordinator: ChunkStore.FlushWAL for %s v%d: %w", kbID, versionID, err)
		}
		st.flush = time.Since(flushStart)
	}

	return docIDs, nil
}

// maxConcurrentDocumentWrites bounds how many documents step 3 processes at once.
//
// The per-document work is independent and dominated by an embed round trip, so
// the parallelism is real; the bound exists because the embedder is a shared
// external service, and unbounded fan-out here would turn one large write into
// everyone else's outage. Eight is deliberately modest: it already takes a
// 1000-document batch from ~14.6 s of serialised embedding to a couple of
// seconds, and the point is to stop queueing, not to saturate the embedder.
const maxConcurrentDocumentWrites = 8

// writeDocumentsConcurrently runs step 3 for every change with bounded
// concurrency and returns the first failure.
//
// Why: each change touches only its own content, chunks, chunk-document mapping
// and doc-store entry, and each one waits on an embed call — measured at ~14.6 ms
// per document for a 1000-document batch, 10 ms of which was the embedder round
// trip. Serially that is 14.6 s inside a single candidate's dispatch budget
// (15 s floor + 50 ms/document), so a large batch was spending its budget
// queueing rather than working.
//
// The ordering that matters is preserved: the version's document-ID set and
// bloom filter are still written by the caller AFTER every change has settled,
// so a partly-written version is never published as complete. Every individual
// write is idempotent (see writeVersionStorage), so a failure that cancels the
// rest leaves a version a later attempt can finish — not a corrupt one.
func (c *WriteCoordinatorImpl) writeDocumentsConcurrently(ctx context.Context, kbID string, versionID int64, changes []types.DocChange, kbMeta types.KnowledgeBaseMeta, st *docStages) error {
	if st == nil {
		st = &docStages{}
	}
	// One presence cache for the whole version. Its lifetime is exactly this call
	// — one version's documents — which is the window in which a chunk's presence
	// cannot change except through this write. See chunkPresenceCache.
	presence := &chunkPresenceCache{}
	if len(changes) <= 1 {
		// Nothing to overlap. Keeping the single-document case free of the
		// coordination below matters because it is the interactive-write common
		// case, and it is what every existing order-sensitive test exercises.
		for _, change := range changes {
			if err := c.writeOneChange(ctx, kbID, versionID, change, kbMeta, st, presence); err != nil {
				return err
			}
		}
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, c.cfg.MaxConcurrentDocumentWrites)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, change := range changes {
		if ctx.Err() != nil {
			break // an earlier failure already stopped this write
		}
		sem <- struct{}{} // bounds how many embeddings are in flight
		wg.Add(1)
		go func(change types.DocChange) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := c.writeOneChange(ctx, kbID, versionID, change, kbMeta, st, presence); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel() // stop the rest: this version is not going to publish
				}
				mu.Unlock()
			}
		}(change)
	}
	wg.Wait()
	return firstErr
}

// writeOneChange is step 3 for a single change: an add/update goes through the
// split -> embed -> write path, a delete writes a tombstone.
func (c *WriteCoordinatorImpl) writeOneChange(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta, st *docStages, presence *chunkPresenceCache) error {
	switch change.Op {
	case types.ChangeOpAdd, types.ChangeOpUpdate:
		return c.writeDocument(ctx, kbID, versionID, change, kbMeta, st, presence)
	case types.ChangeOpDelete:
		// Write a tombstone for the deleted document.
		if err := c.retry(ctx, func() error {
			return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, nil)
		}); err != nil {
			return fmt.Errorf("coordinator: write tombstone for %s: %w", change.DocID, err)
		}
	}
	return nil
}

// writeDocument handles a single ADD or UPDATE document change: split, embed
// what is new, write chunks + mappings + doc content. st, when non-nil,
// accumulates this document's split/embed/store cost; see docStages for why
// those are sums rather than wall clock.
func (c *WriteCoordinatorImpl) writeDocument(ctx context.Context, kbID string, versionID int64, change types.DocChange, kbMeta types.KnowledgeBaseMeta, st *docStages, presence *chunkPresenceCache) error {
	// Split, with the algorithm this knowledge base was created with: the
	// splitter is shared by every KB on the node, so the mode travels with the
	// call (docs/已实施/content-defined-chunking-plan.md §3.8).
	params := kbMeta.Chunking()
	splitStart := time.Now()
	chunks := c.cfg.Splitter.Split(change.Content, params, kbMeta.EmbedConfig.ModelID)
	if st != nil {
		st.splitUs.Add(time.Since(splitStart).Microseconds())
	}

	if len(chunks) == 0 {
		// No chunks produced (e.g. empty content): just write the document
		// content (or tombstone for empty content).
		storeStart := time.Now()
		err := c.retry(ctx, func() error {
			return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
		})
		if st != nil {
			st.storeUs.Add(time.Since(storeStart).Microseconds())
		}
		return err
	}

	// Embed, but only the chunks this KB does not already hold.
	//
	// The chunk store is content-addressed, so a chunk the KB already has needs
	// neither a fresh embedding nor a fresh vector — only its chunk-document
	// mapping. Asking the embed service for it anyway is what used to make a
	// higher reuse rate worth nothing in compute; filtering here is what turns
	// the reuse into skipped embed calls (docs/已实施/content-defined-chunking-plan.md
	// §4). The filter asks exactly the question writeChunk asks (chunkPresent),
	// so the two cannot disagree.
	//
	// This is a per-node optimisation, not global dedup: the bloom filter that
	// answers it is local, so a chunk another node stored is still embedded
	// here (§4.3).
	pending := make([]types.Chunk, 0, len(chunks))
	alreadyPresent := 0
	for _, chunk := range chunks {
		chunkCheckStart := time.Now()
		present, err := c.chunkPresent(ctx, kbID, chunk.ChunkID, presence)
		if st != nil {
			st.chunkCheckUs.Add(time.Since(chunkCheckStart).Microseconds())
		}
		if err != nil {
			return err
		}
		if present {
			alreadyPresent++
			continue
		}
		pending = append(pending, chunk)
	}

	var vectors map[string][]float32
	if len(pending) > 0 {
		embedStart := time.Now()
		err := c.retry(ctx, func() error {
			var inner error
			vectors, inner = c.cfg.EmbedClient.Embed(ctx, pending)
			return inner
		})
		if st != nil {
			st.embedUs.Add(time.Since(embedStart).Microseconds())
			st.embedCalls.Add(1)
		}
		if err != nil {
			return fmt.Errorf("coordinator: embed chunks for doc %s: %w", change.DocID, err)
		}
	}

	// Store the new vectors. writeChunk re-checks existence: a concurrent write
	// of the same chunk (another document, another version) can store it
	// between the filter above and here, and that check is what keeps the two
	// paths from storing one chunk twice.
	storeStart := time.Now()
	lateHits := 0
	for _, chunk := range pending {
		vector, ok := vectors[chunk.ChunkID]
		if !ok {
			return fmt.Errorf("coordinator: embed did not return vector for chunk %s", chunk.ChunkID)
		}
		writeChunkStart := time.Now()
		existed, err := c.writeChunk(ctx, kbID, chunk, vector, presence)
		if st != nil {
			st.writeChunkUs.Add(time.Since(writeChunkStart).Microseconds())
			st.writeChunkN.Add(1)
		}
		if err != nil {
			return err
		}
		if existed {
			lateHits++
		}
	}

	// Map every chunk — the reused ones included — to this document, in ONE
	// durable commit rather than one per chunk (idempotent either way).
	chunkIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkIDs = append(chunkIDs, chunk.ChunkID)
	}
	if err := c.retry(ctx, func() error {
		return c.cfg.ChunkDocMapper.WriteMany(ctx, kbID, change.DocID, chunkIDs)
	}); err != nil {
		return fmt.Errorf("coordinator: chunk-doc map write: %w", err)
	}

	// Write document content.
	docWriteStart := time.Now()
	docWriteErr := c.retry(ctx, func() error {
		return c.cfg.DocStore.Write(ctx, kbID, change.DocID, versionID, []byte(change.Content))
	})
	if st != nil {
		st.docWriteUs.Add(time.Since(docWriteStart).Microseconds())
	}
	if docWriteErr != nil {
		return fmt.Errorf("coordinator: doc store write: %w", docWriteErr)
	}
	if st != nil {
		st.storeUs.Add(time.Since(storeStart).Microseconds())
	}

	// Reuse observation (docs/已实施/content-defined-chunking-plan.md §5). The two
	// counts differ exactly when a concurrent writer stored a chunk after the
	// pre-embed filter: those were reused, but their embedding was paid for
	// already.
	c.logger().Info("write: document stored",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
		zap.String("doc_id", change.DocID),
		zap.String("chunk_mode", params.Mode.String()),
		zap.Int("chunks", len(chunks)),
		zap.Int("chunks_already_present", alreadyPresent+lateHits),
		zap.Int("embed_skipped", alreadyPresent))

	return nil
}

// chunkPresent reports whether kbID's chunk store already holds chunkID.
//
// It is the write path's single answer to that question — whether the caller is
// filtering before an embed call or confirming just before a write. The bloom
// filter only hints: it can answer "maybe" for a chunk that is not there, and it
// is per-node state that starts empty after a restart. So a hint is always
// confirmed against the authoritative store.
func (c *WriteCoordinatorImpl) chunkPresent(ctx context.Context, kbID, chunkID string, presence *chunkPresenceCache) (bool, error) {
	if presence != nil && presence.isPresent(chunkID) {
		return true, nil
	}
	if !c.cfg.ChunkBloom.Test(chunkID) {
		return false, nil
	}
	exists, err := c.cfg.ChunkStore.Exists(ctx, kbID, chunkID)
	if err != nil {
		return false, fmt.Errorf("coordinator: ChunkStore.Exists for %s: %w", chunkID, err)
	}
	if exists && presence != nil {
		presence.notePresent(chunkID)
	}
	return exists, nil
}

// chunkPresenceCache remembers, for the life of ONE version's write, which chunks
// a single authoritative check has already confirmed present.
//
// Why it exists: a version's documents share chunks — that is what content
// addressing means, and the more repetitive the corpus the more they share — yet
// the write path asked the vecstore about every (document, chunk) pair
// separately. Measured on the 3+3 cluster with a 1,000-document batch,
// chunkPresent was 51.4% of the per-document step while the chunk WRITES in that
// same step were 38 calls: the bulk of it was ChunkStore.Exists asking the same
// question about the same chunk hundreds of times.
//
// Only PRESENT is remembered, deliberately. A bloom miss already answers "not
// here" locally and for free, so caching it buys nothing — and it could go stale
// inside the same write: a chunk another document just stored has become
// present, and a remembered "absent" would make the next document embed it again.
//
// Concurrent workers can each miss on the same chunk and both ask the store; that
// costs a few duplicate round trips at the start of a batch and keeps the cache
// lock-free, which is the right trade at this scale.
type chunkPresenceCache struct {
	present sync.Map // chunkID -> struct{}
}

func (c *chunkPresenceCache) isPresent(chunkID string) bool {
	_, ok := c.present.Load(chunkID)
	return ok
}

func (c *chunkPresenceCache) notePresent(chunkID string) {
	c.present.Store(chunkID, struct{}{})
}

// writeChunk stores one chunk's vector unless the store already holds it, and
// reports whether it was already there (writeDocument counts those).
func (c *WriteCoordinatorImpl) writeChunk(ctx context.Context, kbID string, chunk types.Chunk, vector []float32, presence *chunkPresenceCache) (bool, error) {
	present, err := c.chunkPresent(ctx, kbID, chunk.ChunkID, presence)
	if err != nil {
		return false, err
	}
	if present {
		return true, nil // already stored; nothing to do
	}

	// Write chunk to vecstore.
	if err := c.retry(ctx, func() error {
		return c.cfg.ChunkStore.Write(ctx, kbID, chunk.ChunkID, vector)
	}); err != nil {
		return false, fmt.Errorf("coordinator: ChunkStore.Write for %s: %w", chunk.ChunkID, err)
	}

	// Add to bloom filter, and to the in-flight cache: the chunk is present now,
	// and the documents that share it should not ask the store again.
	c.cfg.ChunkBloom.Add(chunk.ChunkID)
	if presence != nil {
		presence.notePresent(chunk.ChunkID)
	}
	return false, nil
}

// verifyParentDocSet checks that the parent document set read from this node's
// store is the one the control layer committed for that version.
//
// The comparison is the ONLY thing that separates "I hold the parent version" from
// "I hold nothing for it": both read back from ListDocIDs as an empty list. A
// mismatch means the child's document set must not be derived here — the write has
// to be retried (the parent's data may still be landing) or handled by a replica
// that does hold it.
func (c *WriteCoordinatorImpl) verifyParentDocSet(ctx context.Context, kbID string, parentVersionID int64, docIDs []string) error {
	if c.cfg.RaftNode == nil {
		return nil // no metadata source wired (unit-test stacks): nothing to compare against
	}
	parent, err := c.cfg.RaftNode.GetVersion(ctx, kbID, parentVersionID)
	if err != nil {
		return fmt.Errorf("coordinator: read parent version %d metadata: %w", parentVersionID, err)
	}
	if parent.DocIDSetHash == "" {
		// No digest committed for the parent yet (its write is still in flight, or
		// the version predates the field). Unverifiable is not the same as wrong, so
		// the write proceeds rather than being blocked on a fact nobody has.
		return nil
	}
	got := stratumync.ComputeDocIDSetHash(docIDs)
	if got == parent.DocIDSetHash {
		return nil
	}
	return fmt.Errorf(
		"coordinator: parent version %d's local document set does not match its committed digest "+
			"(local %s, committed %s): this node does not hold the parent version's data, so deriving "+
			"the child's document set here would drop every inherited document: %w",
		parentVersionID, got, parent.DocIDSetHash, stratumerrors.ErrIndexNotReady)
}
