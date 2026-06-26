// Package chunkstore defines the ChunkStore interface — the Go-side client
// wrapper around the C++ vecstore's internal gRPC ChunkStorageService.
//
// The authoritative storage for chunk vectors lives in C++ vecstore's
// RocksDB (ChunkStorage); ChunkStore is purely a client wrapper. No vector
// data is duplicated in PebbleDB.
//
// See Stratum_接口设计v9.md "ChunkStore" and Stratum_设计文档v10.md
// "chunk store" for the authoritative design. This file contains only the
// interface definition; the gRPC-backed implementation (VecstoreChunkStore)
// is built in Phase 2 (2-B), against the real vecstore gRPC server from
// Phase 1 (1-B-3).
package chunkstore

import "context"

// ChunkStore stores chunk vectors, keyed by kbID + chunkID. Chunks are
// content-addressed (ChunkID = SHA-256(chunk text + embed config ID)), so
// writing the same key with the same vector is naturally idempotent.
type ChunkStore interface {
	// Write stores vector under kbID + chunkID.
	Write(ctx context.Context, kbID, chunkID string, vector []float32) error

	// Exists reports whether kbID + chunkID is already stored. Used on the
	// write path to confirm a BloomFilter positive (which may be a false
	// positive) before deciding whether a write is actually needed.
	Exists(ctx context.Context, kbID, chunkID string) (bool, error)

	// Delete removes a single chunk's vector. Used by the asynchronous
	// orphan-chunk GC path once no surviving document references the
	// chunk.
	Delete(ctx context.Context, kbID, chunkID string) error

	// DeleteByKB removes all chunk vectors for kbID. Used by knowledge
	// base deletion.
	DeleteByKB(ctx context.Context, kbID string) error
}
