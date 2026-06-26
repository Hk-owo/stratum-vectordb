// Package bloom defines the BloomFilter interface used for two purposes in
// Stratum:
//
//   - chunk existence filtering: one filter per knowledge base, containing
//     the set of chunk IDs already written to the chunk store. Used on the
//     write path to skip redundant ChunkStore.Exists round-trips for chunks
//     that are almost certainly new.
//   - version document filtering: one filter per version, containing the
//     full document ID set of that version. Used on the read path to
//     quickly reject document IDs that do not belong to the target
//     version before falling back to VersionDocList for a definitive
//     answer.
//
// Both usages share the same interface; the distinction is purely in how
// callers instantiate and scope individual filter instances.
//
// See Stratum_接口设计v9.md "BloomFilter" and Stratum_设计文档v10.md
// "布隆过滤器" for the authoritative design. This file contains only the
// interface definition; the bits-and-blooms-backed implementation is built
// in Phase 1 (1-A-4).
package bloom

// BloomFilter is a probabilistic set-membership filter supporting
// persistence and rebuild-from-source recovery after a crash (bloom
// filters are in-memory only; they are never the source of truth).
//
// Test is probabilistic: it may return true for a key that was never
// Added (a false positive), but never returns false for a key that was
// Added. Callers that need a definitive answer must confirm a positive
// Test result against the authoritative store (ChunkStore.Exists or
// VersionDocList.ListDocIDs, depending on which filter is in play).
type BloomFilter interface {
	// Add inserts key into the filter.
	Add(key string)

	// Test reports whether key is possibly in the set. False positives are
	// possible (bounded by the configured false-positive rate); false
	// negatives are not.
	Test(key string) bool

	// Serialize encodes the filter's current state for persistence to disk.
	Serialize() ([]byte, error)

	// Deserialize replaces the filter's state with the state encoded in
	// data, as previously produced by Serialize. Used to restore a
	// persisted filter, or as part of rebuilding a filter from its
	// authoritative source after a crash.
	Deserialize(data []byte) error

	// Reset clears the filter back to empty, as if newly constructed.
	Reset()
}
