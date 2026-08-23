// Package docstore defines the DocStore interface: MVCC storage for raw
// document text, keyed by knowledge base ID + document ID + version ID.
//
// See Stratum_接口设计v9.md "DocStore" and Stratum_设计文档v10.md "doc store"
// for the authoritative design. This file contains only the interface
// definition; the PebbleDB-backed implementation is built in Phase 1.
package docstore

import "context"

// DocStore stores the raw text of documents with MVCC semantics. The key is
// kbID + docID + versionID; the value is the full document text, or an
// empty value to represent a tombstone (DELETE).
//
// Write semantics: only documents that actually changed (ADD / UPDATE /
// DELETE) in a given version produce a new record; unchanged documents
// produce zero new entries for that version.
//
// Read semantics: ReadAt scans by kbID + docID prefix and returns the entry
// with the largest versionID <= maxVersionID. If that entry is a
// tombstone, the document is considered deleted as of that version and
// ReadAt returns ErrVersionNotFound (or an equivalent "not found" signal —
// see implementation-level documentation once Phase 1 lands).
type DocStore interface {
	// Write stores the document content (or a tombstone, via an empty
	// value) for kbID + docID at versionID. Idempotent: writing the same
	// (kbID, docID, versionID) key twice with the same value is a no-op
	// from the caller's perspective.
	Write(ctx context.Context, kbID, docID string, versionID int64, value []byte) error

	// ReadAt returns the document content visible at maxVersionID: the
	// value of the entry with the largest versionID <= maxVersionID. If no
	// such entry exists, or the entry found is a tombstone, ReadAt returns
	// an error indicating the document is not found at that version.
	ReadAt(ctx context.Context, kbID, docID string, maxVersionID int64) ([]byte, error)

	// DeleteByKB removes all entries for kbID (all documents, all
	// versions). Used by knowledge base deletion. Prefix-scan based;
	// idempotent.
	DeleteByKB(ctx context.Context, kbID string) error

	// DeleteByVersion removes every entry for (kbID, versionID) across all
	// documents. Used by the DeleteVersion cleanup to physically reclaim
	// the version's MVCC records. The version ID sits at the end of the
	// key, so this requires a full scan of kbID's keyspace (O(keys in the
	// knowledge base)); idempotent.
	DeleteByVersion(ctx context.Context, kbID string, versionID int64) error

	// DiskUsage returns the approximate on-disk size in bytes of the store.
	// Used by GetSystemStatus's resource-usage snapshot.
	DiskUsage(ctx context.Context) (uint64, error)
}
