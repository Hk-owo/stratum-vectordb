package router

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
)

// Forward routes a unary call to the right layer: storage-resident methods
// round-robin across the storage nodes, control writes go to the leader, and
// control reads round-robin across the control nodes. fn performs the actual
// call against index idx of that layer's clients; the concrete request/response
// types stay static in the caller.
//
// kbID is the knowledge base the call concerns, or "" for calls that are not
// about one (listing, health). It is what the authorization checkpoint checks a
// grant against, and it is taken from the request rather than from the caller:
// the tenant comes from the credential lookup, the knowledge base from the
// request, and neither from an assertion the client makes about itself.
//
// Every forwarded call also carries the station's internal trust mark, so a
// storage node can tell "the station sent this" from "something reached me
// directly". The caller's credential is deliberately not among what is
// forwarded.
func Forward[T any](r *Router, ctx context.Context, fullMethod, kbID string, fn func(idx int, ctx context.Context) (T, error), opts ...ForwardOption) (T, error) {
	var zero T
	var fo forwardOpts
	for _, opt := range opts {
		opt(&fo)
	}
	// The start of this station's own share of the call, so the write path can
	// report what the station added on top of the node it forwards to. Nothing
	// else on this path can measure that: from the client's side a station hop
	// and a slow node look identical.
	stationStart := time.Now()
	if r.auth != nil {
		if _, err := r.auth.Authorize(ctx, kbID, isWriteMethod(fullMethod)); err != nil {
			return zero, err
		}
	}
	// The mark is stamped unconditionally (given a key), and that is not the same
	// decision as whether the station authenticates anyone. It says "this call
	// arrived through a station"; a node requiring it is asking for exactly that
	// and nothing more. Stamping it only when a token table is configured would
	// deadlock the cluster this feature exists to protect: nodes requiring the
	// mark would reject the station's own forwarding the moment an operator ran
	// without a token table.
	//
	// What changed with H4 (docs/参考/code-review-2026-09-24.md): the stamp is an HMAC
	// over a timestamp under a key shared with the nodes, not the constant "1".
	// A node can now actually tell whether the station sent the call, which is
	// what the gate above always claimed.
	ctx = r.mark.Stamp(ctx)
	if isStorageMethod(fullMethod) {
		if r.requireDirectory && kbID == "" && r.storagePool != nil {
			return forwardDirectoryNodes(r, ctx, fn, stationStart)
		}
		if r.requireDirectory && kbID != "" {
			return forwardDirectory(r, ctx, fullMethod, kbID, fn, fo, stationStart)
		}
		// No separate storage layer means the all-in-one shape, where the control
		// nodes are the storage nodes too — the same fallback NewRouter applies
		// when it dials them. Keeping the two consistent matters because a Router
		// built directly (the tests do) skips that constructor.
		n := len(r.storageAddrs)
		if n == 0 {
			n = len(r.controlAddrs)
		}
		// The version this call must be answered from decides who is asked first. A
		// knowledge base the table has not seen yields no expected version, and then
		// every node qualifies — routing degrades to round-robin rather than to refusal.
		//
		// R1 of docs/code-review-2026-10-14.md: when the caller PINNED a version, that
		// version is the credential — not the active one. Routing a pinned read by the
		// active version would pick the owners of the wrong version.
		expected, _ := r.ExpectedVersion(kbID)
		if fo.pinnedVersion != 0 {
			expected = fo.pinnedVersion
		}
		// Sampling happens HERE, on the read being served, rather than on a ticker:
		// the versions worth counting are exactly the ones somebody asks for, and a
		// table a timer walked would keep decaying entries nobody is looking at
		// (docs/已实施/index-owner-sharding-plan.md §4).
		if r.priority != nil && expected != 0 {
			r.priority.Observe(kbID, expected)
		}
		// P2C within each tier: send the call to the less busy of two randomly chosen
		// nodes. It reorders only — a node that is merely slow is still tried, just not
		// first.
		tiers := r.routeTiers(kbID, expected, n, time.Now())
		for i, tier := range tiers {
			tiers[i] = leastLoadedFirst(tier, r.storageInflight)
		}
		return forwardTiers(r, ctx, readTrace{kbID: kbID, layer: "storage", admission: time.Since(stationStart)},
			tiers, r.storageBreakers, r.storageInflight, func(idx int, ctx context.Context) (T, error) {
				resp, err := fn(idx, ctx)
				if cursor, ok := stratumerrors.ReplicaCursorOf(err); ok {
					r.lag.note(kbID, idx, cursor, time.Now())
				}
				return resp, err
			})
	}
	if isWriteMethod(fullMethod) {
		// §4.3(1): refuse a write this station already knows cannot reach quorum,
		// rather than paying a control-layer round trip to be told the same thing.
		if err := r.writeGate(kbID); err != nil {
			return zero, err
		}
		// admission is everything the station decided for itself before asking
		// anyone: the authorization check above plus the storage gate just now.
		// forwardWrite reports it alongside its own stages.
		return forwardWrite(r, ctx, kbID, time.Since(stationStart), fn)
	}
	return forwardRead(r, ctx, readTrace{kbID: kbID, layer: "control", admission: time.Since(stationStart)},
		leastLoadedFirst(allIndexes(len(r.controlAddrs)), r.controlInflight),
		r.controlBreakers, r.controlInflight, fn)
}

// ForwardOption tunes how one forwarded call picks its candidates.
type ForwardOption func(*forwardOpts)

type forwardOpts struct {
	// pinnedVersion is the version the CALLER named, when it named one. Zero
	// means the caller did not pin, and the routing table's expected version is
	// the credential.
	pinnedVersion int64
}

// WithPinnedVersion tells the router the caller asked for one SPECIFIC version,
// so that version — not the knowledge base's active one — is what the candidates
// are narrowed by (R1 of docs/code-review-2026-10-14.md).
//
// It is an option rather than a parameter because most forwarded calls have no
// version to pin: metadata reads and writes are about the knowledge base.
func WithPinnedVersion(versionID int64) ForwardOption {
	return func(o *forwardOpts) { o.pinnedVersion = versionID }
}

// forwardWrite runs fn on the current leader. The leader is re-polled on
// every call (LeaderNow, no caching) so a stale leader can never keep
// writes pinned to a dead or demoted node. When the leader rejects the
// call (not leader, or the node went down), the call is retried on the
// re-polled leader. With no known leader it falls back to trying every
// node once. Non-retryable errors (validation, index failures, ...) are
// returned as-is.
func forwardWrite[T any](r *Router, ctx context.Context, kbID string, admission time.Duration, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T

	// Per-stage timings of one forwarded write, at debug level.
	//
	// Why it exists: the station is the first of the three processes a write
	// passes through, and its share of the cost is the only part of the path
	// that nothing behind it can measure — from the client's side, a station hop
	// and a slow node look identical. admission_us is what this station decided
	// on its own (credential check + §4.3(1)'s storage gate), leader_lookup_us is
	// finding the leader, and attempt_us is the forwarded RPC, which contains the
	// control node's whole handling (service: create version timings,
	// coordinator: write execute timings) plus the wire. kb_id + version_id join
	// this line to those; version_id is read out of the response, so it is
	// present exactly when the write got as far as allocating a version.
	//
	// attempts is why this line is not just a sum: §9.3(4) retries a retryable
	// failure on another node, so a write that took 4 s here may have been two
	// attempts, and the first one is invisible everywhere else. leader_index
	// says which node the leader poll landed on — negative means it found none
	// and fell back to trying every node once.
	start := time.Now()
	var versionID int64
	var tLeaderLookup, tAttempt time.Duration
	attempts, leaderIdx := 0, -1
	// lastErr keeps the last forwarded failure's cause.
	//
	// Why it exists: every error the loop sees is triaged by isRetryableErr, but
	// the triage's INPUT was dropped, so every retryable failure converged on one
	// flat "no leader available" — which names neither the node nor the reason.
	// That message is what a caller sees when a write fails, and it sent us
	// looking at the cluster when the leader was healthy the whole time
	// (GetClusterStatus answered has_leader=true, and leader_index was 0).
	var lastErr error
	defer func() {
		r.loggerOrNop().Debug("router: write forward timings",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Int("attempts", attempts),
			zap.Int("leader_index", leaderIdx),
			zap.Int64("admission_us", admission.Microseconds()),
			zap.Int64("leader_lookup_us", tLeaderLookup.Microseconds()),
			zap.Int64("attempt_us", tAttempt.Microseconds()),
			zap.Int64("total_us", time.Since(start).Microseconds()),
			zap.Error(lastErr))
	}()

	// noteResponse picks up the version id when the response carries one, which
	// is what makes this line joinable to the control and storage nodes' lines.
	// Not every forwarded call has an id (and a failed one has none), so this is
	// best-effort rather than a required part of the contract.
	noteResponse := func(resp T) T {
		if v, ok := any(resp).(versionIdentified); ok {
			versionID = v.GetVersionId()
		}
		return resp
	}

	leaderBroken := false
	for attempt := 0; attempt <= len(r.controlAddrs); attempt++ {
		lookupStart := time.Now()
		idx, ok := r.discoverer.LeaderNow(ctx)
		tLeaderLookup += time.Since(lookupStart)
		if !ok {
			// Election in progress or discovery failed: try every node
			// once — the real leader accepts the write if reachable.
			attemptStart := time.Now()
			resp, err := tryAll(r, ctx, fn)
			tAttempt += time.Since(attemptStart)
			attempts++
			return noteResponse(resp), err
		}
		leaderIdx = idx
		if !r.allowControl(idx) {
			// The discovered leader is circuit-broken: trying it again is what
			// the breaker exists to stop. Another attempt re-discovers, which is
			// also how a write finds the new leader after a failover.
			leaderBroken = true
			continue
		}
		attemptCtx, cancel := budgetSlice(ctx, len(r.controlAddrs)-attempt)
		attemptStart := time.Now()
		resp, err := fn(idx, attemptCtx)
		cancel()
		tAttempt += time.Since(attemptStart)
		attempts++
		r.observeControl(ctx, idx, err)
		if err == nil {
			return noteResponse(resp), nil
		}
		lastErr = err
		if !isRetryableErr(err, true) {
			return zero, err
		}
	}
	if lastErr != nil {
		// Wrap rather than replace: the cause stays inspectable with errors.Is,
		// and the message now says what actually went wrong. "no leader
		// available" alone is wrong anyway — the leader was found and the write
		// reached it.
		return zero, noLeaderAfter(fmt.Sprintf("router: no leader available after %d attempt(s)", attempts), lastErr)
	}
	if leaderBroken {
		// A leader was found and never asked: the write was not sent at all.
		return zero, fmt.Errorf("%w: the leader (node %d) is circuit-broken and the write was not sent",
			stratumerrors.ErrControlCircuitOpen, leaderIdx)
	}
	// Named, not a bare errors.New: "the cluster is electing" is transient, and without a
	// sentinel the caller sees `code = Unknown` and cannot know to retry — measured as a
	// whole T4 suite failing in setup while the election finished.
	return zero, fmt.Errorf("router: no leader available: %w", stratumerrors.ErrNoLeaderYet)
}

// versionIdentified is implemented by the responses that name the version a
// write produced. It is the station's half of the join key that ties one write's
// lines together across the three processes: kb_id + version_id appear on the
// station's line (router: write forward timings), the control node's two
// (service: create version timings, coordinator: write execute timings), the
// storage node's own staged transaction (write: stage timings) and its index
// build (index: build timings).
type versionIdentified interface {
	GetVersionId() int64
}

// allowControl / recordControl guard the breaker slices: a Router built without
// NewRouter (the tests) has none, and a nil slice must mean "no breaker", not a
// panic.
func (r *Router) allowControl(idx int) bool {
	if idx < 0 || idx >= len(r.controlBreakers) {
		return true
	}
	return r.controlBreakers[idx].allow(time.Now())
}

func (r *Router) observeControl(ctx context.Context, idx int, err error) {
	if idx < 0 || idx >= len(r.controlBreakers) {
		return
	}
	r.controlBreakers[idx].observeFor(ctx, err)
}

// tryAll runs fn against every node until one succeeds. Used as the
// fallback when no leader is known.
func tryAll[T any](r *Router, ctx context.Context, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	now := time.Now()
	admitted := 0
	// Same reason as forwardWrite's lastErr: the triage input must survive, or
	// every retryable failure flattens into one reasonless message.
	var lastErr error
	for idx := range r.controlAddrs {
		if idx < len(r.controlBreakers) && !r.controlBreakers[idx].allow(now) {
			continue
		}
		admitted++
		attemptCtx, cancel := budgetSlice(ctx, len(r.controlAddrs)-idx)
		resp, err := fn(idx, attemptCtx)
		cancel()
		r.observeControl(ctx, idx, err)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isRetryableErr(err, true) {
			return zero, err
		}
	}
	if admitted == 0 {
		return zero, unavailableFor("control", "every control node is circuit-broken")
	}
	if lastErr != nil {
		return zero, noLeaderAfter("router: no leader available (tried every node)", lastErr)
	}
	return zero, fmt.Errorf("router: no leader available: %w", stratumerrors.ErrNoLeaderYet)
}

// noLeaderAfter is the error for "every attempt failed in a retryable way".
//
// When the last attempt's error already carries a reason (storage_unavailable, say),
// that reason IS the diagnosis and is kept as the identity. When it carries none — a
// bare Unavailable from a node that is down, or "not leader" mid-election — the
// identity is ErrNoLeaderYet, because that is what the caller has to act on: the
// cluster may be electing, ask again. The last error stays wrapped either way, so the
// message and errors.Is still reach it.
func noLeaderAfter(msg string, lastErr error) error {
	if stratumerrors.ReasonOf(lastErr) != "" {
		return fmt.Errorf("%s: %w", msg, lastErr)
	}
	return fmt.Errorf("%s: %w (last error: %w)", msg, stratumerrors.ErrNoLeaderYet, lastErr)
}

// isRetryableErr reports whether a forwarded call should retry on another node.
//
// The decision is made on the error's IDENTITY first, and only then on its
// status code. The code alone cannot answer the question: FailedPrecondition is
// shared by failures that mean opposite things — "this replica has not built the
// index / has taken it out of service, another one may be fine" versus "the
// version is being deleted, do not retry anywhere". A code-only rule therefore
// either retries the terminal ones or gives up on the recoverable ones, and a
// message-matching rule (what the "not leader" special case below does) breaks
// the moment anyone rewords an error. Named sentinels travel as a standard
// ErrorInfo detail (errors.ReasonOf), which is what makes the distinction
// available on this side of the wire at all.
//
// forWrite picks between the two paths, and it is not cosmetic: the same
// sentinel means opposite things to each.
//
//   - version_pending, to a READ, means this replica's storage-layer write for
//     the version is still in progress — another replica may already be serving
//     it, so asking someone else is worth the hop.
//   - version_pending, to a WRITE, means the version's data has not landed yet.
//     A write goes through Raft, so every replica shares one state machine:
//     another candidate refuses for exactly the same reason. Retrying buys
//     nothing and costs something — it replaces a terminal, self-describing
//     FailedPrecondition ("version 95 is PENDING") with a flat "no leader
//     available", which names neither the version nor its state and reads like
//     an infrastructure fault. That message sent us auditing a healthy cluster
//     while GetClusterStatus was answering has_leader=true.
//
// The code-based fallback covers errors that carry no reason:
//   - codes.Unavailable: node down / connection failed (leader failover or a
//     stopped container) — worth another candidate.
//   - codes.Internal + "not leader": a forwarded kvraft error, which has no
//     named sentinel of its own yet.
//
// Everything else (validation errors, terminal version states, ...) is terminal.
func isRetryableErr(err error, forWrite bool) bool {
	if reason := stratumerrors.ReasonOf(err); reason != "" {
		if forWrite && writeTerminalReasons[reason] {
			return false
		}
		return retryableReasons[reason]
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() == codes.Unavailable {
		return true
	}
	return st.Code() == codes.Internal && strings.Contains(st.Message(), "not leader")
}

// writeTerminalReasons names the sentinels that are worth retrying to a reader
// but terminal to a writer.
//
// Expressed as an exception list rather than a second table so the two paths
// cannot drift: a new retryable reason is retryable on both unless it is named
// here, which is the safe direction — the failure mode of an unlisted exception
// is the extra hop this file's forWrite exists to avoid, not a lost write.
var writeTerminalReasons = map[string]bool{
	// Raft-replicated state, so no other candidate can disagree about whether
	// the version's data has landed.
	"version_pending": true,
}

// retryableReasons names the sentinels that mean "another candidate may
// succeed". Keyed by the WIRE name (part of the node-to-node protocol), so a
// reworded message cannot change the answer, and a sentinel that is not listed
// here — e.g. version_deleting or knowledge_base_deleted, which share
// FailedPrecondition but are terminal — is never retried by accident.
var retryableReasons = map[string]bool{
	"migration_in_progress":  true,
	"migration_busy":         true,
	"kb_write_paused":        true,
	"storage_service_paused": true,
	"placement_unknown":      true,
	// This replica has not built the version's index (yet); another one may have.
	"index_not_ready": true,
	// This replica IS building the version's index, and the caller waited out
	// load_wait_timeout_ms for it. The same family as index_not_ready — local and
	// transient, and another candidate may already be serving the version — with
	// an even stronger case: a build that has been running longer than the wait
	// is positive evidence that this replica is busy. Leaving it out made ONE busy
	// candidate a terminal error for the caller (docs/参考/stress-test-report.md §E.4).
	"index_load_timeout": true,
	// This replica took the version's index out of service on purpose, for the
	// §8.6(d) rolling cleanup; another one is serving it.
	"index_maintenance": true,
	// This replica's storage-layer write for the version is still in progress.
	// The work is running (or about to be scheduled), so another replica may
	// already be serving the version — and the query path now asks for the build
	// instead of only refusing, which is what makes the retry worth making.
	"version_pending": true,
	// No control node would take the write because the cluster has no leader yet — an
	// election in flight. Retrying is the whole answer.
	"no_leader_yet": true,
	// parent_not_durable is deliberately NOT here. It is retryable for the CALLER
	// (wait a moment, then send the same request), but another control node would
	// give the same answer — the parent's state is replicated. Listing it would
	// spend the candidate budget re-asking nodes that are all correct.

}
