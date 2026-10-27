// Package coordinator — ChunkGarbageCollector interface.
package coordinator

import (
	"context"

	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/raft"
	"stratum/internal/versiondoc"
)

// ChunkGarbageCollectorConfig bundles the dependencies of the orphan-chunk
// garbage collector.
type ChunkGarbageCollectorConfig struct {
	// SweepInterval is the delay between two full sweeps.
	SweepIntervalSec int

	RaftNode       raft.RaftNode
	ChunkDocMapper chunkdoc.ChunkDocMapper
	DocStore       docstore.DocStore
	ChunkStore     chunkstore.ChunkStore

	// VersionDocList is how the event-driven half finds the chunks a deleted
	// version maps to: the version's document set is read here BEFORE the delete
	// flow drops it, then turned into chunk ids through the reverse mapping. Nil
	// disables that half (ReclaimVersionChunks becomes a no-op), which is what a
	// node holding no version data wants.
	VersionDocList versiondoc.VersionDocList

	// Locks is the per-knowledge-base write lock shared with WriteCoordinatorImpl:
	// the GC's reclaim phase takes this KNOWLEDGE BASE's lock, re-checks orphanhood
	// against the raft CURRENT version while holding it, then deletes the mapping and
	// the vector. Mutual exclusion with a write to the same KB closes the
	// stale-snapshot race (a sweep erasing data a newer version committed between the
	// snapshot and the delete), and taking it per KB keeps a sweep from stopping
	// writes to every other knowledge base (M7 of docs/code-review-2026-09-24.md).
	// When nil a private set is allocated — sufficient only for tests without
	// concurrent writes; production wiring MUST inject the same set as
	// WriteCoordinatorConfig.Locks.
	Locks *KBLockSet
}

// ChunkGarbageCollector reclaims orphan chunks — chunk vectors whose every
// referencing document has been deleted from the knowledge base (see
// Stratum_设计文档v10.md "chunk-doc 映射" 异步 GC). It has two halves:
//
//   - ReclaimVersionChunks (VersionChunkReclaimer) is the PRIMARY trigger. It
//     runs from the delete-version flow and reclaims the chunks of the version
//     being deleted that no live version references any more. A chunk can only
//     become orphaned when the version that referenced it is deleted, so the
//     deletion event is exactly the right moment — and delays nothing.
//   - Sweep is the periodic safety net. Nothing names the leftovers it hunts: a
//     write path that crashed between its vector write and its mapping write
//     leaves a mapping no version deletion will ever mention. That is rare, so
//     the interval is long (an hour by default), and every sweep costs one
//     whole-chain read per knowledge base plus a scan of every chunk mapping.
//
// A chunk is orphaned when NO live version of the knowledge base can still
// read one of the documents it maps to — every referencing document is
// tombstoned or absent in every version the control layer still holds metadata
// for, and a version marked Deleting never counts as live. It is the whole live
// chain, not the newest version: an older version stays queryable
// (QueryRequest.version_id) and is rebuilt from the same chunk vectors, so a
// document deleted at the newest version still pins its chunks until the
// version that contains it is deleted too. Such chunks can never be reached by
// a query of any live version, so both their ChunkDocMapper entries and their
// vecstore vectors are reclaimed: mappings first, vectors second, so a crash
// between the two leaves a state that a later run can safely repeat (both
// deletions are idempotent; a vector whose mapping was already removed is
// simply not scanned again).
type ChunkGarbageCollector interface {
	// Run launches the background sweep loop; it returns when ctx is
	// cancelled. Non-blocking startup: the first sweep runs after one
	// interval.
	Run(ctx context.Context)

	// Sweep performs a single full sweep over every knowledge base. Safe
	// to call concurrently with itself (a second sweep finds nothing
	// already collected). Exposed for tests.
	Sweep(ctx context.Context) error
}

// VersionChunkReclaimer is the event-driven half of the chunk GC: it reclaims
// the chunks whose last live referent was versionID.
//
// It hangs off the delete-version flow (coordinator.LocalVersionDropper and
// WriteCoordinatorImpl.DropVersionStorage) instead of off a scan, because a
// chunk can only BECOME an orphan when the version that referenced it is
// deleted — a document deleted at a newer version is still part of every older
// live version. Doing it there means the sweep does not have to scan the whole
// knowledge base every interval to notice.
//
// It is idempotent and cheap when there is nothing to do: a version this node
// holds no document list for (the usual case on a replica that never received
// it) is a no-op.
type VersionChunkReclaimer interface {
	// ReclaimVersionChunks reclaims the chunks of versionID that no live version
	// references any more. It must run BEFORE the delete flow drops the version's
	// document list — that list is what names the candidate chunks — and it
	// returns an error rather than swallowing one, so the delete flow's retries
	// (and the tombstone's PENDING entry) can run it again.
	ReclaimVersionChunks(ctx context.Context, kbID string, versionID int64) error
}
