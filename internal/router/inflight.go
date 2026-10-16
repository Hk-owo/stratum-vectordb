package router

import (
	"math/rand/v2"
	"sync/atomic"
)

// inflight counts the calls a station currently has outstanding at one node.
//
// It exists for power-of-two-choices, and what it buys is specifically the case a
// round-robin cannot see: a node that is merely SLOWER than its peers — a
// compaction running, a colder page cache, a smaller machine — collects the same
// share of calls as everyone else and lets them queue, and nothing in the station
// notices. Two random samples per call are enough to notice (the classic result),
// and they need no shared state: each station balances the traffic IT sends,
// which is the only traffic it can see anyway.
type inflight struct {
	n atomic.Int64
}

// newInflight returns one counter per node, or nil for a layer with no nodes.
//
// Pointers rather than a []atomic.Int64: that type carries noCopy, so a slice of
// it cannot be grown or copied, and a lazy allocation somewhere later would be a
// vet failure waiting to happen.
func newInflight(nodes int) []*inflight {
	if nodes <= 0 {
		return nil
	}
	out := make([]*inflight, nodes)
	for i := range out {
		out[i] = &inflight{}
	}
	return out
}

// leastLoadedFirst reorders candidates so the less busy of two RANDOMLY chosen
// ones comes first, leaving the rest of the order alone.
//
// Three properties matter, and each rules out a simpler version:
//
//   - It REORDERS, it never removes. A node that is wedged keeps a high counter
//     forever, and "skip the busiest" would stop ever trying it — the same
//     permanent avoidance the circuit breaker exists to bound, and the reason a
//     wedged node's recovery would go unnoticed. Randomness is what guarantees it
//     still gets sampled eventually.
//   - The two samples are RANDOM, not "the two least loaded". Picking the actual
//     minimum needs a full scan and produces a fixed preference order; picking
//     two at random is O(1) and spreads load in expectation.
//   - Only the front of the order changes. forwardRead walks the list and stops at
//     the first success, so what decides the cost of a call is the first few
//     entries — reordering the tail would be work nobody reads.
//
// A nil or short load table means "no counters for this layer", and the
// candidates come back untouched.
func leastLoadedFirst(candidates []int, load []*inflight) []int {
	if len(candidates) < 2 || len(load) == 0 {
		return candidates
	}

	i := rand.IntN(len(candidates))
	j := rand.IntN(len(candidates) - 1)
	if j >= i {
		j++ // two DISTINCT indices, drawn uniformly
	}
	ci, cj := candidates[i], candidates[j]
	if ci < 0 || cj < 0 || ci >= len(load) || cj >= len(load) {
		return candidates // an index this layer has no counter for; do not guess
	}

	lessBusy := i
	if load[cj].n.Load() < load[ci].n.Load() {
		lessBusy = j
	}
	if lessBusy == 0 {
		return candidates
	}
	// LIFT, do not swap. Swapping would drop the node that was first into the
	// position the promoted one left, so the order behind the front would shuffle
	// as a side effect of a decision that is only about the front. Lifting keeps
	// everything else in the order the caller gave.
	out := make([]int, 0, len(candidates))
	out = append(out, candidates[lessBusy])
	for i, idx := range candidates {
		if i != lessBusy {
			out = append(out, idx)
		}
	}
	return out
}
