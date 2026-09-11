package docstore

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/pebbleutil"
)

// PebbleDocStore is the real, disk-persistent DocStore implementation,
// backed by PebbleDB. Key layout (see Stratum_设计文档v10.md "doc store"):
//
//	EncodeString(kbID) + EncodeString(docID) + EncodeVersionID(versionID)
//
// Value layout: a single tag byte followed by the raw content bytes.
//   - tag 0x00: tombstone (DELETE). No content bytes follow.
//   - tag 0x01: live content. Remaining bytes are the document's raw text.
//
// The tag byte exists so that a document whose real content is the empty
// string (Write(..., []byte{})) is stored and read back distinctly from a
// tombstone (Write(..., nil)) — relying on "empty value means tombstone"
// without a tag would conflate the two. Callers signal a tombstone by
// passing a nil value to Write, exactly as MockDocStore does, so the two
// implementations are interchangeable from a caller's perspective.
type PebbleDocStore struct {
	db *pebble.DB
}

const (
	tagTombstone byte = 0x00
	tagContent   byte = 0x01
)

// NewPebbleDocStore opens (creating if necessary) a PebbleDB database at
// path and wraps it as a DocStore. The caller owns the returned store's
// lifetime and should call Close when done.
func NewPebbleDocStore(path string) (*PebbleDocStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("docstore: open PebbleDB at %s: %w", path, err)
	}
	return &PebbleDocStore{db: db}, nil
}

// Close releases the underlying PebbleDB handle. Not part of the DocStore
// interface (the interface is storage-agnostic and mock implementations
// have nothing to close), but real callers (e.g. cmd/stratum/main.go) are
// expected to call this during shutdown.
func (s *PebbleDocStore) Close() error {
	return s.db.Close()
}

// DB returns the underlying *pebble.DB handle. Used by the data-sync
// leader handler to stream DocStore entries directly without going
// through the DocStore interface.
func (s *PebbleDocStore) DB() *pebble.DB { return s.db }

func encodeDocKey(kbID, docID string, versionID int64) []byte {
	key := pebbleutil.EncodeString(kbID)
	key = append(key, pebbleutil.EncodeString(docID)...)
	key = append(key, pebbleutil.EncodeVersionID(versionID)...)
	return key
}

func encodeDocPrefix(kbID, docID string) []byte {
	key := pebbleutil.EncodeString(kbID)
	key = append(key, pebbleutil.EncodeString(docID)...)
	return key
}

func encodeKBPrefix(kbID string) []byte {
	return pebbleutil.EncodeString(kbID)
}

func encodeValue(value []byte) []byte {
	if value == nil {
		return []byte{tagTombstone}
	}
	out := make([]byte, 1+len(value))
	out[0] = tagContent
	copy(out[1:], value)
	return out
}

// decodeValue returns (content, isTombstone). content is nil for a
// tombstone.
func decodeValue(raw []byte) ([]byte, bool, error) {
	if len(raw) == 0 {
		return nil, false, fmt.Errorf("docstore: corrupt value: empty (missing tag byte)")
	}
	switch raw[0] {
	case tagTombstone:
		return nil, true, nil
	case tagContent:
		content := make([]byte, len(raw)-1)
		copy(content, raw[1:])
		return content, false, nil
	default:
		return nil, false, fmt.Errorf("docstore: corrupt value: unknown tag byte 0x%02x", raw[0])
	}
}

// Write implements DocStore. Idempotent: writing the same (kbID, docID,
// versionID) key again simply overwrites the stored value (which, for the
// expected caller pattern of always writing the same content for a given
// already-allocated versionID, is a no-op in effect).
func (s *PebbleDocStore) Write(_ context.Context, kbID, docID string, versionID int64, value []byte) error {
	key := encodeDocKey(kbID, docID, versionID)
	if err := s.db.Set(key, encodeValue(value), pebble.Sync); err != nil {
		return fmt.Errorf("docstore: Write(%s,%s,%d): %w", kbID, docID, versionID, err)
	}
	return nil
}

// ReadAt implements DocStore: returns the content visible at maxVersionID,
// i.e. the value of the entry with the largest versionID <= maxVersionID
// within (kbID, docID). Uses an iterator bounded to
// [docPrefix, docPrefix+EncodeVersionID(maxVersionID+1)] and seeks to the
// last key in that range, which is exactly the entry being sought — O(log
// n) via PebbleDB's internal seek rather than a linear scan.
func (s *PebbleDocStore) ReadAt(_ context.Context, kbID, docID string, maxVersionID int64) ([]byte, error) {
	if maxVersionID < 0 {
		return nil, fmt.Errorf("docstore: ReadAt(%s,%s,%d): negative maxVersionID: %w", kbID, docID, maxVersionID, stratumerrors.ErrInvalidArgument)
	}

	prefix := encodeDocPrefix(kbID, docID)
	upperBound := boundedUpperBound(prefix, maxVersionID)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("docstore: ReadAt(%s,%s,%d): new iterator: %w", kbID, docID, maxVersionID, err)
	}
	defer iter.Close()

	if !iter.Last() {
		return nil, fmt.Errorf("docstore: %s/%s not found at version %d: %w", kbID, docID, maxVersionID, stratumerrors.ErrVersionNotFound)
	}

	content, isTombstone, err := decodeValue(iter.Value())
	if err != nil {
		return nil, fmt.Errorf("docstore: ReadAt(%s,%s,%d): %w", kbID, docID, maxVersionID, err)
	}
	if isTombstone {
		return nil, fmt.Errorf("docstore: %s/%s deleted as of version %d: %w", kbID, docID, maxVersionID, stratumerrors.ErrVersionNotFound)
	}
	return content, nil
}

// boundedUpperBound computes an exclusive upper bound for an iterator over
// keys with the given docPrefix and versionID <= maxVersionID. If
// maxVersionID is the maximum representable int64 (so maxVersionID+1
// would overflow), it falls back to PrefixSuccessor(prefix) — i.e. "no
// version-level upper bound, just stay within this document's prefix" —
// since no real version ID will ever reach that value in practice (it's
// allocated by a monotonic Raft counter starting at 1).
func boundedUpperBound(prefix []byte, maxVersionID int64) []byte {
	if maxVersionID == 1<<63-1 {
		return pebbleutil.PrefixSuccessor(prefix)
	}
	upper := append([]byte(nil), prefix...)
	upper = append(upper, pebbleutil.EncodeVersionID(maxVersionID+1)...)
	return upper
}

// DeleteByKB implements DocStore: removes every entry for kbID via a
// single efficient range delete rather than iterating and deleting one key
// at a time.
func (s *PebbleDocStore) DeleteByKB(_ context.Context, kbID string) error {
	prefix := encodeKBPrefix(kbID)
	upperBound := pebbleutil.PrefixSuccessor(prefix)
	if upperBound == nil {
		return fmt.Errorf("docstore: DeleteByKB(%s): prefix has no successor (unexpected)", kbID)
	}
	if err := s.db.DeleteRange(prefix, upperBound, pebble.Sync); err != nil {
		return fmt.Errorf("docstore: DeleteByKB(%s): %w", kbID, err)
	}
	return nil
}

// DeleteByVersion implements DocStore: removes every entry for (kbID,
// versionID) across all documents. versionID sits at the END of the key
// (kbID + docID + versionID), so unlike DeleteByKB this cannot be a single
// prefix range delete — it prefix-scans kbID and batches up the keys whose
// trailing versionID matches. O(keys in the knowledge base), acceptable for
// the DeleteVersion cleanup path.
func (s *PebbleDocStore) DeleteByVersion(_ context.Context, kbID string, versionID int64) error {
	prefix := encodeKBPrefix(kbID)
	upperBound := pebbleutil.PrefixSuccessor(prefix)
	if upperBound == nil {
		return fmt.Errorf("docstore: DeleteByVersion(%s,%d): prefix has no successor (unexpected)", kbID, versionID)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBound})
	if err != nil {
		return fmt.Errorf("docstore: DeleteByVersion(%s,%d): new iterator: %w", kbID, versionID, err)
	}
	defer iter.Close()

	// Collect matching keys first, then delete in a single batch: mutating
	// the keyspace while iterating is not safe in PebbleDB.
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) >= 8 && binary.BigEndian.Uint64(k[len(k)-8:]) == uint64(versionID) {
			keys = append(keys, append([]byte(nil), k...))
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("docstore: DeleteByVersion(%s,%d): iterator error: %w", kbID, versionID, err)
	}
	if len(keys) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, k := range keys {
		if err := batch.Delete(k, nil); err != nil {
			return fmt.Errorf("docstore: DeleteByVersion(%s,%d): batch delete: %w", kbID, versionID, err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("docstore: DeleteByVersion(%s,%d): commit: %w", kbID, versionID, err)
	}
	return nil
}

// DeleteByVersionExceptVisibleFrom implements DocStore: it reclaims the
// version's records except the ones a surviving later version still reads.
//
// A version's records are the MVCC source for every later version that did
// not rewrite the same document (WriteCoordinator writes only the documents
// a version actually changes, and ReadAt falls back to the newest entry at
// or before the queried version). So when a DeleteVersion keeps any later
// version alive — SINGLE re-parents the target's children, ANCESTORS keeps
// the target's whole subtree — deleting those records would silently make
// the survivor read an older value or nothing.
//
// The decision uses anchorVersionID, the smallest surviving version above
// versionID: an entry must be kept iff it is the one ReadAt would return at
// that anchor. A zero anchor means nothing survives, so every record is
// reclaimable.
func (s *PebbleDocStore) DeleteByVersionExceptVisibleFrom(ctx context.Context, kbID string, versionID, anchorVersionID int64) error {
	if anchorVersionID <= 0 {
		return s.DeleteByVersion(ctx, kbID, versionID)
	}

	kbPrefix := encodeKBPrefix(kbID)
	upperBound := pebbleutil.PrefixSuccessor(kbPrefix)
	if upperBound == nil {
		return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): prefix has no successor (unexpected)", kbID, versionID)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: kbPrefix, UpperBound: upperBound})
	if err != nil {
		return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): new iterator: %w", kbID, versionID, err)
	}
	defer iter.Close()

	// Collect matching keys first, then delete in a single batch: mutating
	// the keyspace while iterating is not safe in PebbleDB.
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		// A docstore key is kbID || docID || versionID, and docID itself
		// carries a 4-byte length prefix, so anything shorter cannot be a
		// record we can attribute to a document.
		if len(k) < len(kbPrefix)+4+8 {
			continue
		}
		if binary.BigEndian.Uint64(k[len(k)-8:]) != uint64(versionID) {
			continue
		}
		// Decode the docID segment by hand rather than via
		// pebbleutil.DecodeString: that helper maps both "empty string" and
		// "malformed encoding" to "", and an empty docID is a valid encoding
		// we must not mistake for corruption.
		docSeg := k[len(kbPrefix) : len(k)-8]
		if len(docSeg) < 4 || int(binary.BigEndian.Uint32(docSeg[:4])) != len(docSeg)-4 {
			return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): undecodable docID in key", kbID, versionID)
		}
		docID := string(docSeg[4:])
		visible, ok, err := s.visibleVersionOf(kbID, docID, anchorVersionID)
		if err != nil {
			return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): %w", kbID, versionID, err)
		}
		if ok && visible == versionID {
			continue // still the read source for a surviving later version
		}
		keys = append(keys, append([]byte(nil), k...))
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): iterator error: %w", kbID, versionID, err)
	}
	if len(keys) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, k := range keys {
		if err := batch.Delete(k, nil); err != nil {
			return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): batch delete: %w", kbID, versionID, err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("docstore: DeleteByVersionExceptVisibleFrom(%s,%d): commit: %w", kbID, versionID, err)
	}
	return nil
}

// visibleVersionOf reports the key-visible version ID for (kbID, docID) at
// maxVersionID — the largest versionID <= maxVersionID — and whether such an
// entry exists. The entry may be a tombstone: this is deliberately ReadAt's
// key-only counterpart, not its value semantics, because a surviving version
// reads a tombstone too (and dropping it would resurrect a deleted document).
func (s *PebbleDocStore) visibleVersionOf(kbID, docID string, maxVersionID int64) (int64, bool, error) {
	prefix := encodeDocPrefix(kbID, docID)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: boundedUpperBound(prefix, maxVersionID)})
	if err != nil {
		return 0, false, fmt.Errorf("visible entry for %s/%s@%d: new iterator: %w", kbID, docID, maxVersionID, err)
	}
	defer iter.Close()
	if !iter.Last() {
		// No entry at or before maxVersionID — distinct from an iteration
		// error, which Last() also reports as false. The caller deletes the
		// record when no entry is visible, so an error must not be mistaken
		// for "nothing visible".
		if err := iter.Error(); err != nil {
			return 0, false, fmt.Errorf("visible entry for %s/%s@%d: iterator error: %w", kbID, docID, maxVersionID, err)
		}
		return 0, false, nil
	}
	k := iter.Key()
	return int64(binary.BigEndian.Uint64(k[len(k)-8:])), true, nil
}

// DiskUsage implements DocStore: returns PebbleDB's estimated total disk
// usage (live data + internal structures) in bytes.
func (s *PebbleDocStore) DiskUsage(_ context.Context) (uint64, error) {
	return s.db.Metrics().DiskSpaceUsage(), nil
}

var _ DocStore = (*PebbleDocStore)(nil)
