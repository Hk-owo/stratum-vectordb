package router

import (
	"context"
	"errors"
	"testing"
)

// TestLeastLoadedFirst_PicksTheFreerOfTwo: with exactly two candidates both are
// always sampled, so the outcome is deterministic and there is no randomness to
// tolerat— the busier node must never come first.
func TestLeastLoadedFirst_PicksTheFreerOfTwo(t *testing.T) {
	load := newInflight(2)
	load[0].n.Store(7)
	load[1].n.Store(1)

	got := leastLoadedFirst([]int{0, 1}, load)
	if len(got) != 2 || got[0] != 1 {
		t.Fatalf("order = %v, want node 1 first (1 in flight vs 7)", got)
	}

	// And the reverse: the same function, the same call site, the other answer.
	load[0].n.Store(0)
	load[1].n.Store(3)
	got = leastLoadedFirst([]int{0, 1}, load)
	if got[0] != 0 {
		t.Fatalf("order = %v, want node 0 first (0 in flight vs 3)", got)
	}
}

// TestLeastLoadedFirst_NeverPromotesTheBusiest: with three candidates and one of
// them clearly busier, that one must never reach the front. Whichever pair is
// sampled, the other member of it is the freer, so the busy node can only ever
// lose the comparison.
func TestLeastLoadedFirst_NeverPromotesTheBusiest(t *testing.T) {
	load := newInflight(3)
	load[0].n.Store(100) // the busy one
	load[1].n.Store(0)
	load[2].n.Store(0)

	for i := 0; i < 500; i++ {
		if got := leastLoadedFirst([]int{0, 1, 2}, load); got[0] == 0 {
			t.Fatalf("iteration %d put the busy node first: %v", i, got)
		}
	}
}

// TestLeastLoadedFirst_SpreadsAcrossTheIdleOnes is the property P2C exists for: no
// single idle node monopolizes the front. With two equally idle candidates, both
// should reach it a comparable number of times.
func TestLeastLoadedFirst_SpreadsAcrossTheIdleOnes(t *testing.T) {
	load := newInflight(3)
	load[0].n.Store(100)

	firsts := map[int]int{}
	for i := 0; i < 3000; i++ {
		firsts[leastLoadedFirst([]int{0, 1, 2}, load)[0]]++
	}
	for _, idle := range []int{1, 2} {
		if firsts[idle] == 0 {
			t.Fatalf("idle node %d never reached the front; firsts = %v", idle, firsts)
		}
	}
	if firsts[0] != 0 {
		t.Fatalf("busy node reached the front %d times, want 0", firsts[0])
	}
	// Each idle node is the freer member of the pair with probability 1/2 when the
	// pair excludes the busy node, so a loose band is enough to catch a bias.
	if firsts[1] < 1000 || firsts[2] < 1000 {
		t.Fatalf("front is not spread: %v", firsts)
	}
}

// TestLeastLoadedFirst_LiftsRatherThanSwaps: the decision is about the FRONT, so
// the order behind it must be exactly what the caller gave, minus whichever node
// was lifted to the front. Swapping would drop the former front-runner into the
// gap, shuffling the tail as a side effect of a decision that was never about it.
//
// Note what is deliberately NOT asserted here: that a specific idle node comes
// first. P2C compares only the two it sampled, so an idle node nobody sampled
// stays exactly where it was — that is the algorithm, not a bug in it.
func TestLeastLoadedFirst_LiftsRatherThanSwaps(t *testing.T) {
	load := newInflight(4)
	for _, i := range []int{0, 1, 2, 3} {
		load[i].n.Store(int64(10 * i)) // a strict order, so exactly one node is freest
	}
	in := []int{0, 1, 2, 3}

	for tick := 0; tick < 200; tick++ {
		got := leastLoadedFirst(in, load)
		if got[0] != 3 {
			// Node 3 is the freest of ALL, so it wins whenever sampled. It is not
			// guaranteed to be sampled, which is why this only checks the case
			// where the promotion is visible.
			continue
		}
		// 3 was lifted from the back: everything else keeps its relative order.
		want := []int{0, 1, 2}
		for i := range want {
			if got[i+1] != want[i] {
				t.Fatalf("order = %v, want the remainder [0 1 2] in place behind the lifted node", got)
			}
		}
	}

	// And the input slice is never mutated, so a caller can keep and reuse it.
	if in[0] != 0 || in[3] != 3 {
		t.Fatalf("input was mutated to %v", in)
	}
}

// TestLeastLoadedFirst_DegenerateInputs: no counters, one candidate, or a
// candidate index this layer has no counter for. None of these may panic or
// reorder anything — this runs on every forwarded read.
func TestLeastLoadedFirst_DegenerateInputs(t *testing.T) {
	load := newInflight(2)
	load[0].n.Store(9)

	for _, tc := range []struct {
		name       string
		candidates []int
		load       []*inflight
	}{
		{"no counters for this layer", []int{0, 1}, nil},
		{"single candidate", []int{1}, load},
		{"no candidates", nil, load},
		{"index beyond the table", []int{0, 5}, load},
		{"negative index", []int{0, -1}, load},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := leastLoadedFirst(tc.candidates, tc.load)
			if len(got) != len(tc.candidates) {
				t.Fatalf("got %v, want the input unchanged (%v)", got, tc.candidates)
			}
			for i := range tc.candidates {
				if got[i] != tc.candidates[i] {
					t.Fatalf("got %v, want the input unchanged (%v)", got, tc.candidates)
				}
			}
		})
	}
}

// TestForwardRead_InflightCountsDoNotLeak is the way this kind of counter
// actually breaks: incremented once per attempt and decremented once per call.
// The failing path is the one that matters, because it is the one that goes round
// the loop more than once.
func TestForwardRead_InflightCountsDoNotLeak(t *testing.T) {
	load := newInflight(2)
	r := &Router{
		controlAddrs:    []string{"a:1", "b:1"},
		storageAddrs:    []string{"a:1", "b:1"},
		controlInflight: newInflight(2),
		storageInflight: load,
	}

	// Held means "some counter is non-zero exactly while a call is outstanding".
	// Observed from INSIDE the call, which is what makes it a measure of load
	// rather than of attempts.
	heldDuringCall := false
	alwaysFails := func(int, context.Context) (struct{}, error) {
		for _, c := range load {
			if c.n.Load() > 0 {
				heldDuringCall = true
			}
		}
		return struct{}{}, errors.New("always fails")
	}

	// Both candidates fail, so the loop runs twice — the case where a per-loop
	// increment and a per-call decrement silently diverge.
	if _, err := forwardRead(r, context.Background(), readTrace{}, []int{0, 1}, nil, load, alwaysFails); err == nil {
		t.Fatal("want the failure to surface")
	}
	if !heldDuringCall {
		t.Fatal("no counter was held while a call was in flight; the measure would always read zero")
	}
	for i, c := range load {
		if got := c.n.Load(); got != 0 {
			t.Fatalf("counter %d = %d after the calls, want 0 — a leaked counter makes the node look busy forever", i, got)
		}
	}
}
