// Package versiondoc defines the VersionDocList interface: the full set of
// document IDs belonging to a given version of a knowledge base.
//
// See Stratum_接口设计v9.md "VersionDocList" and Stratum_设计文档v10.md
// "版本文档列表" for the authoritative design. This file contains only the
// interface definition; the PebbleDB-backed implementation is built in
// Phase 1.
package versiondoc

import "context"

// VersionDocList records, for each (kbID, versionID), the complete set of
// document IDs that belong to that version. The key is
// kbID + versionID + docID with an empty value.
//
// Write semantics: when a new version is created, the writer first
// prefix-scans the parent version's VersionDocList to obtain its full
// document ID set, applies this version's changes (ADD appends, DELETE
// removes, UPDATE leaves the document ID in place), and writes the
// resulting full set under the new version ID. Writes are naturally
// idempotent because the key includes the document ID.
//
// Read semantics: ListDocIDs prefix-scans by kbID + versionID and returns
// every document ID belonging to that version.
type VersionDocList interface {
	// Write records that docID belongs to versionID within kbID.
	// Idempotent: writing the same (kbID, versionID, docID) triple more
	// than once does not produce duplicate entries on read.
	Write(ctx context.Context, kbID string, versionID int64, docID string) error

	// ListDocIDs returns the full set of document IDs belonging to
	// versionID within kbID, via a kbID+versionID prefix scan.
	ListDocIDs(ctx context.Context, kbID string, versionID int64) ([]string, error)

	// DeleteByVersion removes all entries for a single version. Used by GC
	// when retiring old versions.
	DeleteByVersion(ctx context.Context, kbID string, versionID int64) error

	// DeleteByKB removes all entries for kbID across all versions. Used by
	// knowledge base deletion. Idempotent (prefix-scan based).
	DeleteByKB(ctx context.Context, kbID string) error
}
