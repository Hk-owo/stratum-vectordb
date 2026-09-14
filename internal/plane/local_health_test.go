package plane

import (
	"context"
	"strings"
	"testing"
	"time"
)

func sameOrder(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

// TestLocalHealthView_IsStableWithoutObservations: the view has nothing to say
// until real traffic says something. Until then the caller's own order — a
// sorted list that makes a failing sequence reproducible in logs — must survive
// untouched.
func TestLocalHealthView_IsStableWithoutObservations(t *testing.T) {
	v := NewLocalHealthView(HealthViewConfig{})
	in := []string{"a:7000", "b:7000", "c:7000"}
	if got := v.Rank(in); !sameOrder(got, in) {
		t.Fatalf("Rank without observations = %v, want %v", got, in)
	}
}

// TestLocalHealthView_DemotesAfterConsecutiveFailures: one failure is noise, a
// run of them is a pattern. The demoted peer goes to the BACK, and the peers the
// view knows nothing about keep their relative order.
func TestLocalHealthView_DemotesAfterConsecutiveFailures(t *testing.T) {
	v := NewLocalHealthView(HealthViewConfig{})
	in := []string{"a:7000", "b:7000", "c:7000"}

	// Two failures are below the threshold: still first.
	v.Observe("a:7000", false)
	v.Observe("a:7000", false)
	if got := v.Rank(in); !sameOrder(got, in) {
		t.Fatalf("Rank after two failures = %v, want %v (below the threshold)", got, in)
	}

	v.Observe("a:7000", false) // the third one is what §7.13.5 acts on
	want := []string{"b:7000", "c:7000", "a:7000"}
	if got := v.Rank(in); !sameOrder(got, want) {
		t.Fatalf("Rank after three failures = %v, want %v", got, want)
	}
}

// TestLocalHealthView_SuccessClearsTheHistory: a success closes the window the
// failures describe. Keeping a partial count would demote a peer for two old
// failures plus one new one — punishing it for the past rather than describing it.
func TestLocalHealthView_SuccessClearsTheHistory(t *testing.T) {
	v := NewLocalHealthView(HealthViewConfig{})
	in := []string{"a:7000", "b:7000"}

	v.Observe("a:7000", false)
	v.Observe("a:7000", false)
	v.Observe("a:7000", false) // demoted
	v.Observe("a:7000", true)  // reached after all
	if got := v.Rank(in); !sameOrder(got, in) {
		t.Fatalf("Rank after a success = %v, want %v", got, in)
	}

	// And the counter really is zero: two more failures must not demote it.
	v.Observe("a:7000", false)
	v.Observe("a:7000", false)
	if got := v.Rank(in); !sameOrder(got, in) {
		t.Fatalf("Rank after success + two failures = %v, want %v (the count restarted)", got, in)
	}
}

// TestLocalHealthView_DemotionExpires: a demotion is a bounded hint, not a
// verdict. Without a bound, one bad minute would demote a peer for the life of
// the process — and a peer that is never called again would stay penalized for a
// failure nobody revisited.
func TestLocalHealthView_DemotionExpires(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	v := NewLocalHealthView(HealthViewConfig{
		DemoteAfter: 1,
		DemoteFor:   30 * time.Second,
		Now:         func() time.Time { return now },
	})
	in := []string{"a:7000", "b:7000"}

	v.Observe("a:7000", false)
	want := []string{"b:7000", "a:7000"}
	if got := v.Rank(in); !sameOrder(got, want) {
		t.Fatalf("Rank right after a failure = %v, want %v", got, want)
	}

	now = now.Add(29 * time.Second)
	if got := v.Rank(in); !sameOrder(got, want) {
		t.Fatalf("Rank inside the demotion window = %v, want %v", got, want)
	}

	now = now.Add(2 * time.Second) // past DemoteFor
	if got := v.Rank(in); !sameOrder(got, in) {
		t.Fatalf("Rank after the demotion expired = %v, want %v (back in place)", got, in)
	}
}

// TestLocalHealthView_NeverDropsAnAddress: the view reorders, it never removes.
// That is the whole reason a wrong verdict is affordable — the worst case is one
// wasted attempt, not a skipped replica.
func TestLocalHealthView_NeverDropsAnAddress(t *testing.T) {
	v := NewLocalHealthView(HealthViewConfig{DemoteAfter: 1})
	in := []string{"a:7000", "b:7000", "c:7000", "d:7000"}
	for _, addr := range in {
		v.Observe(addr, false)
	}
	got := v.Rank(in)
	if len(got) != len(in) {
		t.Fatalf("Rank returned %d addresses, want %d (it must never drop one)", len(got), len(in))
	}
	seen := map[string]bool{}
	for _, addr := range got {
		seen[addr] = true
	}
	for _, addr := range in {
		if !seen[addr] {
			t.Fatalf("Rank dropped %s: %v", addr, got)
		}
	}
}

// TestCoordinatorDispatcher_HealthViewMovesUnreachablePeersBack is the §7.13.5
// wiring: the dispatcher's own attempts are the observations (no probe loop), and
// a peer that keeps refusing ends up last in the next dispatch's candidate order.
func TestCoordinatorDispatcher_HealthViewMovesUnreachablePeersBack(t *testing.T) {
	ctx := context.Background()
	failing := startFakeDataSyncServer(t, &fakeDataSyncServer{fail: true})
	healthy := startFakeDataSyncServer(t, &fakeDataSyncServer{})

	d := NewCoordinatorDispatcher(CoordinatorDispatcherConfig{
		Replicas: func(context.Context) ([]string, error) { return []string{failing, healthy}, nil },
	})

	order := func() []string {
		got, err := d.candidates(ctx)
		if err != nil {
			t.Fatalf("candidates: %v", err)
		}
		return got
	}
	if before := order(); len(before) != 2 {
		t.Fatalf("candidates = %v, want both replicas", before)
	}

	// Each dispatch tries the failing replica first (sorted order) and lands on
	// the healthy one; after three of them the view has a pattern to act on.
	for i := 1; i <= 3; i++ {
		addr, err := d.Dispatch(ctx, "kb-1", int64(i), 0, dispatchChanges())
		if err != nil {
			t.Fatalf("Dispatch #%d: %v", i, err)
		}
		if addr != healthy {
			t.Fatalf("Dispatch #%d landed on %s, want the healthy replica", i, addr)
		}
	}

	after := order()
	if len(after) != 2 || after[len(after)-1] != failing {
		t.Fatalf("candidates after repeated failures = %v, want %s last", after, failing)
	}
}
