// Package versionview provides an immutable per-version read view combining a
// version's verified document-ID set and its compact chunk -> []docIdx
// inverted index, backed by a three-tier cache: L0 (in-memory LRU), L1
// (checksummed local .vview sidecars), and L2 (authoritative PebbleDB lookup).
package versionview

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// HitSource identifies which cache tier satisfied a Lookup call.
type HitSource string

const (
	HitNone HitSource = "none"
	HitL0   HitSource = "l0"
	HitL1   HitSource = "l1"
	HitL2   HitSource = "l2"
)

// Stats reports current residency and cumulative hit/eviction counts of a Store.
type Stats struct {
	Entries   int
	MemBytes  int64
	L0Hits    uint64
	L1Hits    uint64
	L2Builds  uint64
	Evictions uint64
}

// View is an immutable read snapshot of one version's document set and
// inverted chunk-to-document index. Callers must not mutate its fields.
type View struct {
	DocIDSetHash string
	DocIDs       []string
	ChunkToDocs  map[string][]uint32
	MemBytes     int64
}

// ComputeDocIDSetHash computes the canonical SHA-256 digest of docIDs: byte-wise
// sorted, '\n'-joined, hex-encoded. Matches internal/sync.ComputeDocIDSetHash.
func ComputeDocIDSetHash(docIDs []string) string {
	sorted := docIDs
	if !sort.StringsAreSorted(docIDs) {
		sorted = make([]string, len(docIDs))
		copy(sorted, docIDs)
		sort.Strings(sorted)
	}

	h := sha256.New()
	for i, id := range sorted {
		if i > 0 {
			h.Write([]byte{'\n'})
		}
		h.Write([]byte(id))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func newView(digest string, sortedDocIDs []string, chunkToDocs map[string][]uint32) *View {
	docsCopy := make([]string, len(sortedDocIDs))
	copy(docsCopy, sortedDocIDs)

	mapCopy := make(map[string][]uint32, len(chunkToDocs))
	var mem int64 = 64
	for _, id := range docsCopy {
		mem += int64(len(id) + 16)
	}
	for cid, list := range chunkToDocs {
		cp := make([]uint32, len(list))
		copy(cp, list)
		mapCopy[cid] = cp
		mem += int64(len(cid) + 32 + 4*len(cp))
	}

	return &View{
		DocIDSetHash: digest,
		DocIDs:       docsCopy,
		ChunkToDocs:  mapCopy,
		MemBytes:     mem,
	}
}
