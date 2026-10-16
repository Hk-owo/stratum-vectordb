// Package owner computes which replicas should hold a version's INDEX — as a
// pure function of (kbID, versionID) and the replica set, with no state, no
// coordination and no allocation table.
//
// The design decision this package exists to encode is that an owner set
// PRESCRIBES where an index should live; it does not describe where one is. That
// distinction is what keeps the whole scheme stateless: because nobody has to
// know who is currently holding what, there is no index-location table to
// maintain, replicate, or invalidate (docs/index-owner-sharding-plan.md §1).
//
// Rendezvous (highest-random-weight) hashing is chosen for one property above
// the others: the top-k sets are NESTED. Adding a member to the set, or raising
// k, only ever appends to the end of an owner set — it does not reshuffle who
// owns what. With consistent hashing the same change would move a contiguous
// range of versions; with rendezvous, a member joining takes over roughly 1/N of
// the versions and leaves the other (N-1)/N alone.
package owner

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// Rank is the rendezvous score of one member for one version. Higher wins.
//
// SHA-256 rather than a cheap mixing function, for the same reason the project
// uses it for chunk identity: it is already a dependency, and a hash with poor
// avalanche behaviour would cluster owner sets (the same few members winning
// every version) without anything failing loudly.
// Members are identified by ADDRESS, not by node id, and that is the whole
// reason this scheme needs no coordination at all: every participant already
// reads the same address list from its own configuration, whereas node ids would
// have to be carried between the station and the storage nodes before the two
// could agree on who "member 1" is. The address is also what both sides actually
// use — a station forwards to an address, and a replica compares against its own.
func Rank(kbID string, versionID int64, member string) uint64 {
	var buf [8]byte
	h := sha256.New()
	binary.BigEndian.PutUint64(buf[:], uint64(versionID))
	h.Write(buf[:])
	h.Write([]byte{0})
	h.Write([]byte(member))
	h.Write([]byte{0})
	h.Write([]byte(kbID))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

// Owners returns the members that should hold versionID's index, in descending
// rank order — so element 0 is the member that BUILDS and the rest are the ones
// that INSTALL the artifact (§8.4).
//
// k <= 0 or k > len(members) means "every member": that is the active-version
// case and the k-unset default, and it is also what a caller falls back to when
// it cannot trust the owner decision. Returning everyone is the conservative
// direction — it reproduces the pre-sharding behaviour exactly.
//
// The order is a TOTAL order: ties on rank break by id. Without that, two
// callers could disagree about which member is the head, and the head is the one
// that builds.
func Owners(kbID string, versionID int64, members []string, k int) []string {
	if len(members) == 0 {
		return nil
	}
	if k <= 0 || k > len(members) {
		k = len(members)
	}

	type scored struct {
		member string
		rank   uint64
	}
	ranked := make([]scored, 0, len(members))
	for _, m := range members {
		ranked = append(ranked, scored{member: m, rank: Rank(kbID, versionID, m)})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].rank != ranked[j].rank {
			return ranked[i].rank > ranked[j].rank
		}
		return ranked[i].member < ranked[j].member
	})

	out := make([]string, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, ranked[i].member)
	}
	return out
}

// IsHead reports whether nodeID builds for (kbID, versionID) — the first member
// in the owner order.
//
