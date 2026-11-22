package router

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// budgetSlice bounds one attempt's share of the client's remaining deadline
// (docs/参考/历史/Stratum_设计文档v13.md §9.5).
//
// The client's deadline is passed through unchanged — the station adds no
// timeout of its own — but an attempt that is allowed to run until that
// deadline would leave nothing for the retries §9.3(4) promises: one slow node
// would consume the entire budget and the request would fail with candidates
// still untried. So each attempt gets a fraction of what is left, and the
// count of remaining candidates sizes that fraction.
//
// No deadline means the caller set none; the station then imposes none either,
// because inventing one would break a caller that legitimately wants to wait.
func budgetSlice(ctx context.Context, attemptsLeft int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		// Already expired: let the call fail fast rather than adding a fresh
		// timeout that would mask why.
		return context.WithCancel(ctx)
	}
	if attemptsLeft < 1 {
		attemptsLeft = 1
	}
	share := remaining / time.Duration(attemptsLeft+1)
	if share < minAttemptBudget {
		share = minAttemptBudget
	}
	if share > remaining {
		// The client's deadline is a hard bound: a floor may widen an attempt
		// inside it, never beyond it. When what is left is smaller than the
		// floor, the remaining budget IS the attempt's budget — the call will
		// end at the client's deadline, which is the caller's own decision.
		share = remaining
	}
	return context.WithTimeout(ctx, share)
}

// minAttemptBudget keeps the per-attempt share from collapsing to nothing when
// the remaining budget is small or the candidate list is long: an attempt that
// is given microseconds cannot even fail informatively.
const minAttemptBudget = 200 * time.Millisecond

// readTrace carries what forwardRead reports about the station's own share of a
// read. Grouped rather than passed as three loose parameters so the tests that
// call forwardRead directly can pass the zero value.
type readTrace struct {
	kbID      string
	layer     string // "storage" or "control"
	admission time.Duration
}

// forwardRead runs fn across n nodes of one layer in round-robin order,
// skipping to the next node when one is unreachable. The first successful
// response wins.
//
// n is the layer's node count rather than a length read off the Router, because
// the same helper serves both layers and their client slices are indexed by
// their own node lists.
func forwardRead[T any](r *Router, ctx context.Context, trace readTrace, candidates []int, breakers []*breaker, load []*inflight, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	return forwardTiers(r, ctx, trace, [][]int{candidates}, breakers, load, fn)
}

// forwardTiers is forwardRead over tiers of candidates: a later tier is only asked once
// every node of the earlier ones has failed in a retryable way.
func forwardTiers[T any](r *Router, ctx context.Context, trace readTrace, tiers [][]int, breakers []*breaker, load []*inflight, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T

	// Per-stage timings of one forwarded read, at debug level — the read-side
	// counterpart of "router: write forward timings", for the same reason: from
	// the client's side, a station hop and a slow node look identical, so only
	// the station can say what it added. admission_us is what the station decided
	// on its own before asking anyone (credential check + §4.3(1)'s storage gate);
	// attempt_us is a forwarded RPC and contains the storage node's whole handling
	// (service/query.go's "query: stage timings", index/impl.go's
	// "index: search timings") plus the wire. layer says which tier was asked, and
	// node_index which member of it answered — a read that retried elsewhere is
	// invisible everywhere but here.
	start := time.Now()
	var tAttempt time.Duration
	attempts, nodeIdx := 0, -1
	defer func() {
		r.loggerOrNop().Debug("router: read forward timings",
			zap.String("kb_id", trace.kbID),
			zap.String("layer", trace.layer),
			zap.Int("node_index", nodeIdx),
			zap.Int("attempts", attempts),
			zap.Int64("admission_us", trace.admission.Microseconds()),
			zap.Int64("attempt_us", tAttempt.Microseconds()),
			zap.Int64("total_us", time.Since(start).Microseconds()))
	}()

	total := 0
	for _, tier := range tiers {
		total += len(tier)
	}
	if total == 0 {
		return zero, unavailableFor(trace.layer, "no node can serve this request")
	}
	rr := int(r.rr.Add(1) - 1)
	now := time.Now()
	admitted := 0
	for t, tier := range tiers {
		n := len(tier)
		for i := 0; i < n; i++ {
			idx := tier[(rr%n+i)%n]
			if idx < len(breakers) && !breakers[idx].allow(now) {
				continue // sick node: skip it instead of paying its timeout
			}
			admitted++
			// A share of the budget for each node left in this tier, plus one for all
			// the later tiers together: the fallbacks must not leave the nodes that
			// should answer too little time to do so, nor be left with nothing.
			shares := n - i
			if t < len(tiers)-1 {
				shares++
			}
			attemptCtx, cancel := budgetSlice(ctx, shares)
			attemptStart := time.Now()
			// P2C's other half: the counter is held for exactly as long as the call is
			// outstanding, and only for a call that was actually made. A node skipped
			// by the breaker above has nothing in flight, and counting it would make
			// the healthy nodes look busier than the sick one.
			//
			// The defer lives inside the closure so the decrement happens on every
			// exit — including a panic on the way out. Incrementing in the loop and
			// decrementing in the function's own defer is the version of this that
			// leaks a count per interval, forever.
			call := func() (T, error) {
				if idx < len(load) && load[idx] != nil {
					load[idx].n.Add(1)
					defer load[idx].n.Add(-1)
				}
				return fn(idx, attemptCtx)
			}
			resp, err := call()
			tAttempt += time.Since(attemptStart)
			cancel()
			attempts++
			nodeIdx = idx
			if idx < len(breakers) {
				breakers[idx].observeFor(ctx, err)
			}
			if err == nil {
				return resp, nil
			}
			// Reads keep version_pending retryable: another replica may already be
			// serving the version this one is still writing (see isRetryableErr).
			if !isRetryableErr(err, false) {
				return zero, err
			}
		}
	}
	if admitted == 0 {
		return zero, unavailableFor(trace.layer, "every node for this layer is circuit-broken")
	}
	return zero, unavailableFor(trace.layer, "all nodes unavailable")
}

// unavailableFor names the "nobody could take this call" family on the wire, so the
// caller's retry decision survives the station boundary.
//
// Why the sentinel is required rather than nice to have: these three exits used to be
// plain Go errors, so the interceptor had no name to restore and every one of them crossed
// the boundary as codes.Unknown with no ErrorInfo. A client that decides on the reason (as
// this codebase's own callers do) then treats "the storage layer is briefly unreachable"
// as terminal — the same failure cf8a250 fixed for the forwarded-node case, left in place
// on the fallback path.
//
// Which sentinel depends on which layer failed: the storage layer's absence is
// ErrStorageUnavailable, the control layer's is ErrNoLeaderYet (an election, or no leader
// known). Both are codes.Unavailable and both are in retryableReasons, which is the point.
func unavailableFor(layer, detail string) error {
	sentinel := stratumerrors.ErrStorageUnavailable
	if layer == "control" {
		sentinel = stratumerrors.ErrNoLeaderYet
	}
	return fmt.Errorf("%w: %s", sentinel, detail)
}
