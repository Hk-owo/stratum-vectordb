package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"stratum/internal/versiondoc"
)

// ComputeDocIDSetHash computes the canonical SHA-256 digest of a version's
// full document-ID set: the docIDs are sorted byte-wise and concatenated
// with '\n' separators, then hashed (empty set → SHA-256 of empty input).
//
// Sorting makes the digest independent of insertion order, so the leader
// (write path) and a follower (after a DataSync pull) compute identical
// values for the same set. The digest is committed into the version
// metadata (VersionMeta.DocIDSetHash) by the leader only after its
// storage writes finish; followers recompute it from their local
// VersionDocList and retry the pull until the two match — closing the
// "follower pulled before the leader's writes landed" race without moving
// any data into the Raft log.
func ComputeDocIDSetHash(docIDs []string) string {
	sorted := make([]string, len(docIDs))
	copy(sorted, docIDs)
	sort.Strings(sorted)

	h := sha256.New()
	for i, id := range sorted {
		if i > 0 {
			h.Write([]byte{'\n'})
		}
		h.Write([]byte(id))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// VerifyDocIDSet checks whether a follower's local VersionDocList now
// holds the full document set for (kbID, versionID): it recomputes the
// digest from the local store and compares it against expected (the
// digest the leader committed into the version metadata).
//
// An empty expected digest means the leader has not committed one yet
// (initial/empty version, or a missed propose) — the pull is then
// unverifiable and this returns (false, "", nil) so the caller can fall
// back to its own heuristic (typically "local data non-empty").
func VerifyDocIDSet(ctx context.Context, vdl versiondoc.VersionDocList, kbID string, versionID int64, expected string) (verified bool, got string, err error) {
	if expected == "" {
		return false, "", nil // unverifiable: no digest committed by the leader
	}
	docIDs, err := vdl.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return false, "", err
	}
	got = ComputeDocIDSetHash(docIDs)
	return got == expected, got, nil
}
