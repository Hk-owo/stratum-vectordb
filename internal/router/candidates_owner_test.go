package router

import (
	"reflect"
	"sort"
	"testing"

	"stratum/internal/owner"
)

// shardedRouter builds a Router over three storage nodes with a routing table
// that says all three hold kb-1, and the knowledge base serves version 5.
func shardedRouter(inactiveK int) *Router {
	addrs := []string{"node-1:7000", "node-2:7000", "node-3:7000"}
	rt := &RouteTable{
		ready: true,
		snapshot: routeSnapshot{
			servable: map[string]map[int]bool{
				"kb-1": {0: true, 1: true, 2: true},
			},
			expected: map[string]int64{"kb-1": 5},
		},
	}
	return &Router{
		controlAddrs:       append([]string(nil), addrs...),
		storageAddrs:       append([]string(nil), addrs...),
		storageIndexByAddr: map[string]int{"node-1:7000": 0, "node-2:7000": 1, "node-3:7000": 2},
		routes:             rt,
		ownerInactiveK:     inactiveK,
	}
}

func sortedInts(in []int) []int {
	out := append([]int(nil), in...)
	sort.Ints(out)
	return out
}

// TestCandidates_NarrowsToOwnersThatAlsoHold is the whole point of the change
// that R3 asks for: a read goes to the version's index OWNERS, not to every
// holder the round-robin happens to reach.
func TestCandidates_NarrowsToOwnersThatAlsoHold(t *testing.T) {
	r := shardedRouter(2)

	got := sortedInts(r.candidates("kb-1", 7, 3))

	// The expected set, computed the same way the router computes it: the k=2
	// owner set for v7, restricted to the holders.
	want := []int{}
	for _, addr := range owner.Owners("kb-1", 7, r.storageAddrs, 2) {
		if idx, ok := r.storageIndexByAddr[addr]; ok {
			want = append(want, idx)
		}
	}
	sort.Ints(want)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates = %v, want %v (the k=2 owners for v7)", got, want)
	}
	if len(got) != 2 {
		t.Fatalf("candidates = %v, want exactly the 2 owners — a shrinking candidate set is the point", got)
	}
}

// TestCandidates_ActiveVersionKeepsEveryHolder: the version the knowledge base
// serves may be asked of any replica, so its owner set is everyone. That is not a
// special rule in the intersection — it is k = everyone for that one version.
func TestCandidates_ActiveVersionKeepsEveryHolder(t *testing.T) {
	r := shardedRouter(2)

	// Version 5 is the expected (active) version per the routing table.
	got := sortedInts(r.candidates("kb-1", 5, 3))
	want := []int{0, 1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates for the ACTIVE version = %v, want %v — every holder must stay eligible", got, want)
	}
}

// TestCandidates_ShardingOffIsUnchanged: with no sharding configured, the
// candidate set is exactly what it was before — every holder. A deployment that
// has not opted in must not be able to tell this code exists.
func TestCandidates_ShardingOffIsUnchanged(t *testing.T) {
	r := shardedRouter(0)

	got := sortedInts(r.candidates("kb-1", 7, 3))
	want := []int{0, 1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates with sharding off = %v, want %v", got, want)
	}
}

// TestCandidates_FallsBackToHoldersWhenNoOwnerHoldsIt: an owner set that no
// holder belongs to means the owner view and the holder view disagree — a
// topology change, or a stale aggregate. Serving from a holder is still correct
// (the freshness credential is what guarantees the answer), and refusing would
// turn a topology change into an outage.
func TestCandidates_FallsBackToHoldersWhenNoOwnerHoldsIt(t *testing.T) {
	r := shardedRouter(2)
	// Only node-1 holds kb-1, and make it the one node that is NOT an owner for
	// v7 by shrinking the holder set until it excludes the owner set.
	owners := owner.Owners("kb-1", 7, r.storageAddrs, 2)
	holder := 0
	for i, addr := range r.storageAddrs {
		if addr != owners[0] && addr != owners[1] {
			holder = i
		}
	}
	r.routes.snapshot.servable["kb-1"] = map[int]bool{holder: true}

	got := r.candidates("kb-1", 7, 3)
	if !reflect.DeepEqual(got, []int{holder}) {
		t.Fatalf("candidates = %v, want [%d] — a holder that is not an owner must still serve rather than nothing", got, holder)
	}
}

// TestOwnersOf_ActiveVersionIsNotSharded pins the k choice itself, separately
// from the intersection: the active version gets k = everyone, everything else
// gets OwnerInactiveK.
func TestOwnersOf_ActiveVersionIsNotSharded(t *testing.T) {
	r := shardedRouter(2)

	if got := len(r.ownersOf("kb-1", 5)); got != 3 {
		t.Errorf("owners of the ACTIVE version = %d, want 3 (every replica)", got)
	}
	if got := len(r.ownersOf("kb-1", 7)); got != 2 {
		t.Errorf("owners of a non-active version = %d, want 2 (OwnerInactiveK)", got)
	}
}

// TestCandidates_PinnedVersionUsesItsOwnOwnerSet is R1: the candidates follow the
// version the CALLER named. Narrowing a pinned read by the active version would
// select exactly the nodes least likely to still hold the one that was asked for.
func TestCandidates_PinnedVersionUsesItsOwnOwnerSet(t *testing.T) {
	r := shardedRouter(2)

	// Without a pin the credential would be the active version (5) and every
	// holder would qualify; with a pin it must be the pinned version's owners.
	unpinned := sortedInts(r.candidates("kb-1", 5, 3))
	pinned := sortedInts(r.candidates("kb-1", 7, 3))

	if reflect.DeepEqual(unpinned, pinned) {
		t.Fatalf("pinned and unpinned candidate sets are identical (%v); this test cannot "+
			"distinguish them, so it proves nothing", pinned)
	}
	if len(pinned) != 2 {
		t.Fatalf("pinned candidates = %v, want the pinned version's 2 owners", pinned)
	}
}
