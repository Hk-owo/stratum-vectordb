package owner

import (
	"fmt"
	"testing"
)

// members builds an address list the way a deployment's own configuration would.
func members(ids ...int) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, fmt.Sprintf("node-%d:7000", id))
	}
	return out
}

// TestOwners_IsDeterministic: the whole scheme rests on two processes computing
// the same owner set from the same inputs, with nothing in between.
func TestOwners_IsDeterministic(t *testing.T) {
	m := members(1, 2, 3, 4, 5)

	first := Owners("kb-1", 7, m, 3)
	for i := 0; i < 5; i++ {
		again := Owners("kb-1", 7, m, 3)
		if len(again) != len(first) {
			t.Fatalf("call %d returned %d owners, want %d", i, len(again), len(first))
		}
		for j := range first {
			if again[j] != first[j] {
				t.Fatalf("call %d = %v, want %v (including order: element 0 is the builder)", i, again, first)
			}
		}
	}

	// And the order a caller happens to list its members in must not matter —
	// the station and the replicas read their own configuration files, and the
	// two need not list the addresses in the same order.
	shuffled := members(4, 1, 5, 3, 2)
	got := Owners("kb-1", 7, shuffled, 3)
	if len(got) != len(first) {
		t.Fatalf("shuffled members = %v, want the same set as %v", got, first)
	}
	for j := range first {
		if got[j] != first[j] {
			t.Fatalf("shuffled members = %v, want %v — rank must not depend on input order", got, first)
		}
	}
}

// TestOwners_TopKIsNested is the property rendezvous hashing was chosen for:
// raising k appends owners, it never reshuffles them. This is what makes
// "inactive versions use a smaller k than active ones" cheap — the extra owners
// are additions, not a migration.
func TestOwners_TopKIsNested(t *testing.T) {
	m := members(1, 2, 3, 4, 5, 6)

	for version := int64(1); version <= 200; version++ {
		one := Owners("kb-1", version, m, 1)
		three := Owners("kb-1", version, m, 3)
		five := Owners("kb-1", version, m, 5)

		if len(one) != 1 || len(three) != 3 || len(five) != 5 {
			t.Fatalf("v%d: sizes = %d/%d/%d, want 1/3/5", version, len(one), len(three), len(five))
		}
		if three[0] != one[0] {
			t.Fatalf("v%d: k=1 head %s != k=3 head %s — top-1 must be a prefix of top-3", version, one[0], three[0])
		}
		for i := 0; i < 3; i++ {
			if three[i] != five[i] {
				t.Fatalf("v%d: top-3 %v is not a prefix of top-5 %v", version, three, five)
			}
		}
	}
}

// TestOwners_AddingAMemberMovesAboutOneOverN is the migration cost, measured
// rather than asserted by hand-waving: a new replica takes over roughly 1/(N+1)
// of the versions, and the rest stay where they were.
func TestOwners_AddingAMemberMovesAboutOneOverN(t *testing.T) {
	const versions = 20000
	before := members(1, 2, 3, 4)
	after := members(1, 2, 3, 4, 5)

	moved := 0
	for v := int64(1); v <= versions; v++ {
		if Owners("kb-1", v, before, 1)[0] != Owners("kb-1", v, after, 1)[0] {
			moved++
		}
	}

	got := float64(moved) / versions
	want := 1.0 / 5.0 // one member added to four
	t.Logf("adding a 5th member moved %.2f%% of heads (want ~%.2f%%)", got*100, want*100)
	if got < want-0.03 || got > want+0.03 {
		t.Fatalf("migration = %.4f, want ~%.4f (a member joining must take ~1/N, not reshuffle)", got, want)
	}
}

// TestOwners_RankIsSpreadEvenly: a hash with poor avalanche would let the same
// few members win nearly every version. Nothing would fail loudly — the scheme
// would just quietly stop spreading load.
func TestOwners_RankIsSpreadEvenly(t *testing.T) {
	const (
		versions = 20000
		nodes    = 5
	)
	m := members(1, 2, 3, 4, 5)

	wins := make(map[string]int, nodes)
	for v := int64(1); v <= versions; v++ {
		wins[Owners("kb-1", v, m, 1)[0]]++
	}

	want := versions / nodes
	for _, addr := range m {
		got := wins[addr]
		if got < want*95/100 || got > want*105/100 {
			t.Errorf("%s won %d of %d heads, want ~%d (±5%%)", addr, got, versions, want)
		}
	}
}

// TestOwners_Boundaries pins what k means at its edges. "Everyone" is the
// deliberate fallback for k<=0 and k>N: it reproduces the pre-sharding
// behaviour, which is the safe direction when an owner decision cannot be
// trusted.
func TestOwners_Boundaries(t *testing.T) {
	m := members(1, 2, 3)

	for _, tc := range []struct {
		name string
		k    int
		want int
	}{
		{"k=0 means everyone", 0, 3},
		{"negative k means everyone", -1, 3},
		{"k=N", 3, 3},
		{"k>N means everyone", 9, 3},
		{"k=1", 1, 1},
	} {
		if got := Owners("kb-1", 1, m, tc.k); len(got) != tc.want {
			t.Errorf("%s: got %v (%d owners), want %d", tc.name, got, len(got), tc.want)
		}
	}

	if got := Owners("kb-1", 1, nil, 2); got != nil {
		t.Errorf("empty member set = %v, want nil", got)
	}
}
