// Package coordinator — ChunkGarbageCollector interface.
package coordinator

import (
	"context"

	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	"stratum/internal/raft"
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
}

// ChunkGarbageCollector periodically reclaims orphan chunks — chunk
// vectors whose every referencing document has been deleted from the
// knowledge base (see Stratum_设计文档v10.md "chunk-doc 映射" 异步 GC).
//
// A chunk is orphaned when every document it maps to is absent (deleted
// or tombstoned) at the knowledge base's newest version. Such chunks can
// never be reached by a query of any live version, so both their
// ChunkDocMapper entries and their vecstore vectors are reclaimed:
// mappings first, vectors second, so a crash between the two leaves a
// state that the next sweep can safely re-run (both deletions are
// idempotent; a vector whose mapping was already removed is simply not
// scanned again).
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
