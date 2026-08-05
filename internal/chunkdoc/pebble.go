package chunkdoc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"

	"stratum/internal/pebbleutil"
)

// PebbleChunkDocMapper is the real, disk-persistent ChunkDocMapper
// implementation, backed by PebbleDB. Key layout (see
// Stratum_设计文档v10.md "chunk-doc 映射"):
//
//	forward: 'F' + EncodeString(kbID) + EncodeString(chunkID) + EncodeString(docID)
//	reverse: 'R' + EncodeString(kbID) + EncodeString(docID) + EncodeString(chunkID)
//
// Both directions live in the same PebbleDB instance under a one-byte
// direction tag prefix, so a single Write call can update both with one
// batch. Values are empty (presence in the keyspace is the only
// information carried).
type PebbleChunkDocMapper struct {
	db *pebble.DB
}

const (
	dirForward byte = 'F'
	dirReverse byte = 'R'
)

// NewPebbleChunkDocMapper opens (creating if necessary) a PebbleDB
// database at path and wraps it as a ChunkDocMapper.
func NewPebbleChunkDocMapper(path string) (*PebbleChunkDocMapper, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("chunkdoc: open PebbleDB at %s: %w", path, err)
	}
	return &PebbleChunkDocMapper{db: db}, nil
}

// Close releases the underlying PebbleDB handle.
func (m *PebbleChunkDocMapper) Close() error {
	return m.db.Close()
}

// DB returns the underlying *pebble.DB handle. Used by the data-sync
// leader handler.
func (m *PebbleChunkDocMapper) DB() *pebble.DB { return m.db }

func encodeForwardKey(kbID, chunkID, docID string) []byte {
	key := []byte{dirForward}
	key = append(key, pebbleutil.EncodeString(kbID)...)
	key = append(key, pebbleutil.EncodeString(chunkID)...)
	key = append(key, pebbleutil.EncodeString(docID)...)
	return key
}

func encodeForwardPrefix(kbID, chunkID string) []byte {
	key := []byte{dirForward}
	key = append(key, pebbleutil.EncodeString(kbID)...)
	key = append(key, pebbleutil.EncodeString(chunkID)...)
	return key
}

func encodeReverseKey(kbID, docID, chunkID string) []byte {
	key := []byte{dirReverse}
	key = append(key, pebbleutil.EncodeString(kbID)...)
	key = append(key, pebbleutil.EncodeString(docID)...)
	key = append(key, pebbleutil.EncodeString(chunkID)...)
	return key
}

func encodeReversePrefix(kbID, docID string) []byte {
	key := []byte{dirReverse}
	key = append(key, pebbleutil.EncodeString(kbID)...)
	key = append(key, pebbleutil.EncodeString(docID)...)
	return key
}

// encodeKBPrefix returns the prefix matching every forward or reverse key
// for kbID under the given direction tag.
func encodeKBPrefix(dir byte, kbID string) []byte {
	key := []byte{dir}
	key = append(key, pebbleutil.EncodeString(kbID)...)
	return key
}

// Write implements ChunkDocMapper: writes the forward and reverse entries
// for (kbID, chunkID, docID) together in a single batch so they can never
// diverge. Idempotent: PebbleDB Set on an already-present key is a no-op
// in effect (same empty value).
func (m *PebbleChunkDocMapper) Write(_ context.Context, kbID, chunkID, docID string) error {
	batch := m.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(encodeForwardKey(kbID, chunkID, docID), nil, nil); err != nil {
		return fmt.Errorf("chunkdoc: Write(%s,%s,%s): forward Set: %w", kbID, chunkID, docID, err)
	}
	if err := batch.Set(encodeReverseKey(kbID, docID, chunkID), nil, nil); err != nil {
		return fmt.Errorf("chunkdoc: Write(%s,%s,%s): reverse Set: %w", kbID, chunkID, docID, err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("chunkdoc: Write(%s,%s,%s): commit: %w", kbID, chunkID, docID, err)
	}
	return nil
}

// ListDocIDs implements ChunkDocMapper: forward-prefix-scans for all
// document IDs associated with chunkID within kbID.
func (m *PebbleChunkDocMapper) ListDocIDs(_ context.Context, kbID, chunkID string) ([]string, error) {
	prefix := encodeForwardPrefix(kbID, chunkID)
	upperBound := pebbleutil.PrefixSuccessor(prefix)

	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("chunkdoc: ListDocIDs(%s,%s): new iterator: %w", kbID, chunkID, err)
	}
	defer iter.Close()

	var docIDs []string
	for iter.First(); iter.Valid(); iter.Next() {
		docID, err := decodeSuffixString(iter.Key(), prefix)
		if err != nil {
			return nil, fmt.Errorf("chunkdoc: ListDocIDs(%s,%s): %w", kbID, chunkID, err)
		}
		docIDs = append(docIDs, docID)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("chunkdoc: ListDocIDs(%s,%s): iterator error: %w", kbID, chunkID, err)
	}
	return docIDs, nil
}

// ListChunkIDsByDocs implements ChunkDocMapper: reverse-prefix-scans each
// docID in turn, merging and de-duplicating the resulting chunk IDs.
func (m *PebbleChunkDocMapper) ListChunkIDsByDocs(_ context.Context, kbID string, docIDs []string) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string

	for _, docID := range docIDs {
		prefix := encodeReversePrefix(kbID, docID)
		upperBound := pebbleutil.PrefixSuccessor(prefix)

		iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBound})
		if err != nil {
			return nil, fmt.Errorf("chunkdoc: ListChunkIDsByDocs(%s): new iterator for doc %s: %w", kbID, docID, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			chunkID, decErr := decodeSuffixString(iter.Key(), prefix)
			if decErr != nil {
				iter.Close()
				return nil, fmt.Errorf("chunkdoc: ListChunkIDsByDocs(%s): %w", kbID, decErr)
			}
			if _, ok := seen[chunkID]; !ok {
				seen[chunkID] = struct{}{}
				out = append(out, chunkID)
			}
		}
		iterErr := iter.Error()
		iter.Close()
		if iterErr != nil {
			return nil, fmt.Errorf("chunkdoc: ListChunkIDsByDocs(%s): iterator error for doc %s: %w", kbID, docID, iterErr)
		}
	}
	return out, nil
}

// DeleteByKB implements ChunkDocMapper: range-deletes every forward and
// reverse entry for kbID.
func (m *PebbleChunkDocMapper) DeleteByKB(_ context.Context, kbID string) error {
	for _, dir := range []byte{dirForward, dirReverse} {
		prefix := encodeKBPrefix(dir, kbID)
		upperBound := pebbleutil.PrefixSuccessor(prefix)
		if upperBound == nil {
			return fmt.Errorf("chunkdoc: DeleteByKB(%s): prefix has no successor (unexpected)", kbID)
		}
		if err := m.db.DeleteRange(prefix, upperBound, pebble.Sync); err != nil {
			return fmt.Errorf("chunkdoc: DeleteByKB(%s): %w", kbID, err)
		}
	}
	return nil
}

// DeleteByDoc implements ChunkDocMapper: removes every forward and reverse
// entry mentioning docID within kbID. Unlike DeleteByKB, this cannot be a
// single range delete (the forward entries for docID are scattered across
// many different chunkID prefixes), so it first reverse-scans to find
// every chunkID referencing docID, then deletes the corresponding forward
// entry for each, plus the entire reverse range for docID.
func (m *PebbleChunkDocMapper) DeleteByDoc(_ context.Context, kbID, docID string) error {
	reversePrefix := encodeReversePrefix(kbID, docID)
	reverseUpperBound := pebbleutil.PrefixSuccessor(reversePrefix)

	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: reversePrefix, UpperBound: reverseUpperBound})
	if err != nil {
		return fmt.Errorf("chunkdoc: DeleteByDoc(%s,%s): new iterator: %w", kbID, docID, err)
	}

	var chunkIDs []string
	for iter.First(); iter.Valid(); iter.Next() {
		chunkID, decErr := decodeSuffixString(iter.Key(), reversePrefix)
		if decErr != nil {
			iter.Close()
			return fmt.Errorf("chunkdoc: DeleteByDoc(%s,%s): %w", kbID, docID, decErr)
		}
		chunkIDs = append(chunkIDs, chunkID)
	}
	iterErr := iter.Error()
	iter.Close()
	if iterErr != nil {
		return fmt.Errorf("chunkdoc: DeleteByDoc(%s,%s): iterator error: %w", kbID, docID, iterErr)
	}

	batch := m.db.NewBatch()
	defer batch.Close()

	for _, chunkID := range chunkIDs {
		if err := batch.Delete(encodeForwardKey(kbID, chunkID, docID), nil); err != nil {
			return fmt.Errorf("chunkdoc: DeleteByDoc(%s,%s): delete forward entry for chunk %s: %w", kbID, docID, chunkID, err)
		}
	}
	if reverseUpperBound != nil {
		if err := batch.DeleteRange(reversePrefix, reverseUpperBound, nil); err != nil {
			return fmt.Errorf("chunkdoc: DeleteByDoc(%s,%s): delete reverse range: %w", kbID, docID, err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("chunkdoc: DeleteByDoc(%s,%s): commit: %w", kbID, docID, err)
	}
	return nil
}

// decodeSuffixString strips prefix from key and decodes the remaining
// bytes as a single EncodeString-encoded string (the final component of
// either a forward or reverse key).
func decodeSuffixString(key, prefix []byte) (string, error) {
	if len(key) < len(prefix) {
		return "", fmt.Errorf("chunkdoc: key shorter than expected prefix")
	}
	suffix := key[len(prefix):]
	if len(suffix) < 4 {
		return "", fmt.Errorf("chunkdoc: corrupt key: suffix too short for length prefix")
	}
	strLen := int(suffix[0])<<24 | int(suffix[1])<<16 | int(suffix[2])<<8 | int(suffix[3])
	if len(suffix) < 4+strLen {
		return "", fmt.Errorf("chunkdoc: corrupt key: declared string length exceeds available bytes")
	}
	return string(suffix[4 : 4+strLen]), nil
}

var _ ChunkDocMapper = (*PebbleChunkDocMapper)(nil)
