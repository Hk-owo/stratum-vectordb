// Package chunkdoc defines the ChunkDocMapper interface: a bidirectional
// mapping between chunk IDs and document IDs, scoped per knowledge base.
//
// See Stratum_接口设计v9.md "ChunkDocMapper" and Stratum_设计文档v10.md
// "chunk-doc 映射" for the authoritative design. This file contains only
// the interface definition; the PebbleDB-backed implementation is built in
// Phase 1.
package chunkdoc

import "context"

// ChunkDocMapper maintains two directions of the chunk <-> document
// relationship, written together in a single Write call so they never
// diverge:
//
//   - forward:  kbID + chunkID + docID  (chunk -> documents), used by the
//     Query read path to find which documents a hit chunk belongs to.
//   - reverse:  kbID + docID + chunkID  (document -> chunks), used by
//     IndexManager's async build to batch-reverse-lookup all chunks for a
//     version's document set.
//
// All writes are idempotent: writing the same (kbID, chunkID, docID)
// triple more than once does not produce duplicate entries on read.
type ChunkDocMapper interface {
	// Write records that chunkID belongs to docID within kbID, updating
	// both the forward and reverse mappings atomically (from the caller's
	// perspective). Idempotent.
	Write(ctx context.Context, kbID, chunkID, docID string) error

	// ListDocIDs returns all document IDs associated with chunkID within
	// kbID, via a forward-prefix scan. Used by the Query read path.
	ListDocIDs(ctx context.Context, kbID, chunkID string) ([]string, error)

	// ListChunkIDsByDocs batch-reverse-looks-up all chunk IDs associated
	// with any of docIDs within kbID, merging and de-duplicating results
	// across documents. Used by IndexManager.TriggerBuild to gather the
	// full chunk set for a version.
	ListChunkIDsByDocs(ctx context.Context, kbID string, docIDs []string) ([]string, error)

	// DeleteByKB removes all forward and reverse entries for kbID. Used by
	// knowledge base deletion. Idempotent (prefix-scan based).
	DeleteByKB(ctx context.Context, kbID string) error

	// DeleteByDoc removes all forward and reverse entries that mention
	// docID within kbID. Used by the asynchronous orphan-chunk GC path
	// after a document DELETE: this clears the mapping entries for the
	// deleted document without touching the chunk vectors themselves
	// (orphan chunk vector deletion is a separate, later GC step).
	DeleteByDoc(ctx context.Context, kbID, docID string) error
}
