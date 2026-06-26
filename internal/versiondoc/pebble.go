package versiondoc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"

	"stratum/internal/pebbleutil"
)

// PebbleVersionDocList is the real, disk-persistent VersionDocList
// implementation, backed by PebbleDB. Key layout (see
// Stratum_设计文档v10.md "版本文档列表"):
//
//	EncodeString(kbID) + EncodeVersionID(versionID) + EncodeString(docID)
//
// Values are empty; presence in the keyspace is the only information
// carried.
type PebbleVersionDocList struct {
	db *pebble.DB
}

// NewPebbleVersionDocList opens (creating if necessary) a PebbleDB
// database at path and wraps it as a VersionDocList.
func NewPebbleVersionDocList(path string) (*PebbleVersionDocList, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("versiondoc: open PebbleDB at %s: %w", path, err)
	}
	return &PebbleVersionDocList{db: db}, nil
}

// Close releases the underlying PebbleDB handle.
func (v *PebbleVersionDocList) Close() error {
	return v.db.Close()
}

func encodeVersionDocKey(kbID string, versionID int64, docID string) []byte {
	key := pebbleutil.EncodeString(kbID)
	key = append(key, pebbleutil.EncodeVersionID(versionID)...)
	key = append(key, pebbleutil.EncodeString(docID)...)
	return key
}

func encodeVersionPrefix(kbID string, versionID int64) []byte {
	key := pebbleutil.EncodeString(kbID)
	key = append(key, pebbleutil.EncodeVersionID(versionID)...)
	return key
}

func encodeVDLKBPrefix(kbID string) []byte {
	return pebbleutil.EncodeString(kbID)
}

// Write implements VersionDocList. Idempotent: Set on an already-present
// key (same empty value) is a no-op in effect.
func (v *PebbleVersionDocList) Write(_ context.Context, kbID string, versionID int64, docID string) error {
	key := encodeVersionDocKey(kbID, versionID, docID)
	if err := v.db.Set(key, nil, pebble.Sync); err != nil {
		return fmt.Errorf("versiondoc: Write(%s,%d,%s): %w", kbID, versionID, docID, err)
	}
	return nil
}

// ListDocIDs implements VersionDocList: prefix-scans by kbID + versionID
// and returns every document ID belonging to that version.
func (v *PebbleVersionDocList) ListDocIDs(_ context.Context, kbID string, versionID int64) ([]string, error) {
	prefix := encodeVersionPrefix(kbID, versionID)
	upperBound := pebbleutil.PrefixSuccessor(prefix)

	iter, err := v.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBound})
	if err != nil {
		return nil, fmt.Errorf("versiondoc: ListDocIDs(%s,%d): new iterator: %w", kbID, versionID, err)
	}
	defer iter.Close()

	var docIDs []string
	for iter.First(); iter.Valid(); iter.Next() {
		docID, decErr := decodeVDLSuffixString(iter.Key(), prefix)
		if decErr != nil {
			return nil, fmt.Errorf("versiondoc: ListDocIDs(%s,%d): %w", kbID, versionID, decErr)
		}
		docIDs = append(docIDs, docID)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("versiondoc: ListDocIDs(%s,%d): iterator error: %w", kbID, versionID, err)
	}
	return docIDs, nil
}

// DeleteByVersion implements VersionDocList: range-deletes every entry for
// a single (kbID, versionID).
func (v *PebbleVersionDocList) DeleteByVersion(_ context.Context, kbID string, versionID int64) error {
	prefix := encodeVersionPrefix(kbID, versionID)
	upperBound := pebbleutil.PrefixSuccessor(prefix)
	if upperBound == nil {
		return fmt.Errorf("versiondoc: DeleteByVersion(%s,%d): prefix has no successor (unexpected)", kbID, versionID)
	}
	if err := v.db.DeleteRange(prefix, upperBound, pebble.Sync); err != nil {
		return fmt.Errorf("versiondoc: DeleteByVersion(%s,%d): %w", kbID, versionID, err)
	}
	return nil
}

// DeleteByKB implements VersionDocList: range-deletes every entry for
// kbID across all versions.
func (v *PebbleVersionDocList) DeleteByKB(_ context.Context, kbID string) error {
	prefix := encodeVDLKBPrefix(kbID)
	upperBound := pebbleutil.PrefixSuccessor(prefix)
	if upperBound == nil {
		return fmt.Errorf("versiondoc: DeleteByKB(%s): prefix has no successor (unexpected)", kbID)
	}
	if err := v.db.DeleteRange(prefix, upperBound, pebble.Sync); err != nil {
		return fmt.Errorf("versiondoc: DeleteByKB(%s): %w", kbID, err)
	}
	return nil
}

// decodeVDLSuffixString strips prefix from key and decodes the remaining
// bytes as a single EncodeString-encoded string (the docID component of a
// version-doc key).
func decodeVDLSuffixString(key, prefix []byte) (string, error) {
	if len(key) < len(prefix) {
		return "", fmt.Errorf("versiondoc: key shorter than expected prefix")
	}
	suffix := key[len(prefix):]
	if len(suffix) < 4 {
		return "", fmt.Errorf("versiondoc: corrupt key: suffix too short for length prefix")
	}
	strLen := int(suffix[0])<<24 | int(suffix[1])<<16 | int(suffix[2])<<8 | int(suffix[3])
	if len(suffix) < 4+strLen {
		return "", fmt.Errorf("versiondoc: corrupt key: declared string length exceeds available bytes")
	}
	return string(suffix[4 : 4+strLen]), nil
}

var _ VersionDocList = (*PebbleVersionDocList)(nil)
