// Package pebbleutil provides low-level key-encoding primitives shared by
// the three PebbleDB-backed storage modules (docstore, chunkdoc,
// versiondoc). These are pure byte-encoding helpers with no PebbleDB
// dependency of their own; each module composes them into its own key
// schema as documented in Stratum_设计文档v10.md "存储层设计".
//
// Encoding rules:
//   - Variable-length string components (knowledge base ID, document ID,
//     chunk ID) are length-prefixed with a 4-byte big-endian length, so
//     that concatenating two encoded strings can never produce an
//     ambiguous byte boundary (e.g. kbID="ab"+docID="c" cannot collide
//     with kbID="a"+docID="bc").
//   - Fixed-width numeric components (version IDs) are encoded as 8-byte
//     big-endian, so that lexicographic byte comparison (which is what
//     PebbleDB uses to order keys) matches numeric comparison. Version IDs
//     are always non-negative (allocated by a monotonic Raft-backed
//     counter starting at 1), so no sign-bit handling is needed.
package pebbleutil

import (
	"encoding/binary"
	"fmt"
)

// EncodeString length-prefix-encodes s: a 4-byte big-endian length
// followed by the raw bytes of s. Appending the result of EncodeString to
// a key never introduces ambiguity about where s ends, regardless of s's
// content.
func EncodeString(s string) []byte {
	buf := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(s)))
	copy(buf[4:], s)
	return buf
}

// EncodeUint64 encodes v as 8 bytes, big-endian, so that lexicographic
// byte ordering matches numeric ordering.
func EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// EncodeVersionID encodes a version ID (always non-negative in Stratum's
// design) using EncodeUint64. Panics if versionID is negative, since a
// negative version ID indicates a programming error upstream (the Raft
// version counter never produces negative values).
func EncodeVersionID(versionID int64) []byte {
	if versionID < 0 {
		panic(fmt.Sprintf("pebbleutil: EncodeVersionID called with negative versionID %d", versionID))
	}
	return EncodeUint64(uint64(versionID))
}

// DecodeString reverses EncodeString: reads a 4-byte big-endian length
// prefix and returns the string that follows. b may be longer than the
// encoded string (e.g. b is a full compound key and the caller has
// already sliced off the prefix components); DecodeString returns only the
// decoded string, not the remaining suffix.
func DecodeString(b []byte) string {
	if len(b) < 4 {
		return ""
	}
	n := int(binary.BigEndian.Uint32(b[:4]))
	if n == 0 || len(b) < 4+n {
		return ""
	}
	return string(b[4 : 4+n])
}

// PrefixSuccessor returns the smallest byte slice that is strictly greater
// than every key with the given prefix, suitable as an exclusive upper
// bound for a prefix scan or range delete covering exactly the keys
// starting with prefix. Returns nil if prefix consists entirely of 0xFF
// bytes (or is empty), meaning there is no finite successor — callers
// should treat a nil result as "no upper bound" (scan to the end of the
// keyspace).
func PrefixSuccessor(prefix []byte) []byte {
	succ := make([]byte, len(prefix))
	copy(succ, prefix)
	for i := len(succ) - 1; i >= 0; i-- {
		if succ[i] < 0xFF {
			succ[i]++
			return succ[:i+1]
		}
	}
	return nil // prefix was all 0xFF bytes (or empty): no finite successor
}
