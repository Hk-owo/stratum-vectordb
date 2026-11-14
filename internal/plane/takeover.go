// Coordinator-crash takeover (§4.5, §7.3).
//
// A replica that received a version watches for the announcement that would say
// the write reached quorum. When it does not arrive — the coordinator died after
// fanning out but before reporting — the replicas check among themselves and one
// of them reports on the coordinator's behalf. Only the "discover and report"
// step is taken over; no data is re-sent.

package plane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// takeoverTimeout is how long a replica waits for the coordinator's "quorum
// reached" signal before checking for itself (Stratum_设计文档v13.md §7.3).
// Short by design: replicas sit on the same LAN as the coordinator, and the
// cost of waiting too long is a version stranded in PENDING. Like the other
// §10.4 numbers it is a placeholder.
//
// A var so tests can shorten it; the default for
// LocalDataPlaneConfig.TakeoverTimeout.
var takeoverTimeout = 200 * time.Millisecond

// takeoverCheckTimeout bounds the whole quorum check attemptTakeover runs once
// the timer fires.
//
// It is deliberately NOT derived from takeoverTimeout. That one is a test-tunable
// knob, and this bound is read from the timer's own goroutine — deriving one from
// the other would make a test that shortens the knob race with a previous test's
// still-running check on the same variable (measured with -race). It is also the
// honest number: the check is a handful of small RPCs, not something whose budget
// should shrink to tens of milliseconds because a test wanted a prompt timer.
//
// The default for LocalDataPlaneConfig.TakeoverCheckTimeout.
const takeoverCheckTimeout = 2 * time.Second

// takeoverWatch is one pending §7.3 timer.
type takeoverWatch struct {
	timer *time.Timer
	// attempts is how many quorum checks this watch has already spent. A single
	// check is not enough evidence: the replica that is missing is usually the
	// one still copying the data, and that changes with time.
	attempts int
	// gen identifies this watch. A timer that fires after its watch was replaced
	// must not act on behalf of the watch that replaced it (see armTakeover).
	gen uint64
	// delay is this watch's re-check interval, captured when it was armed. It is
	// NOT read from takeoverTimeout on each retry: that variable is a test knob,
	// and a timer goroutine reading it while a test restores it is a data race
	// (measured with -race).
	delay time.Duration
}

// maxTakeoverAttempts bounds how many quorum checks one watch may spend.
//
// Why more than one: the usual reason a check finds no quorum is that the
// remaining replica has not finished its own pull yet — measured on the 3+3
// cluster, the third replica landed the version one catch-up tick later, and the
// single-shot check had already given up. Why not unbounded: each check costs a
// resolve plus one presence RPC per peer, and a version that no quorum holds
// deserves §10.1's verdict rather than an endless poll. With the backoff below
// the whole watch lives ~20 s, which covers a catch-up tick on either side.
//
// The default for LocalDataPlaneConfig.TakeoverMaxAttempts.
const maxTakeoverAttempts = 8

// maxTakeoverRetryDelay caps the exponential backoff between checks, so a long
// outage is still noticed promptly once the peers come back.
//
// The default for LocalDataPlaneConfig.TakeoverMaxRetryDelay.
const maxTakeoverRetryDelay = 5 * time.Second

// WatchVersionWrite starts the §7.3 takeover timer for a version this node has
// just received via fan-out or a pull.
//
// This is the replica's half of coordinator-crash recovery: the data is already
// here, so the only thing that can go missing is the announcement that a quorum
// holds it. If that announcement never arrives, the replica checks for itself
// rather than letting the version rot in PENDING.
//
// Every replica that ends up holding the data arms this — that is what makes
// "the third replica caught up later" self-healing: the late replica announces
// the version itself, so no single node has to poll forever.
func (d *LocalDataPlane) WatchVersionWrite(kbID string, versionID int64) {
	if d.control == nil || d.resolveReplicas == nil || d.presence == nil || d.digest == nil {
		return // takeover not wired; the client-retry path still covers this
	}
	d.armTakeover(kbID, versionID, 0, d.takeoverTimeout)
}

// watchForDurability arms the §7.3 check after data landed here through a path
// that will never receive a coordinator confirmation: a pull.
//
// This is deliberately NOT "report the digest here". One replica holding the
// data is not a quorum, and reporting from this point would claim durability on
// a single copy — the exact claim the control layer's parent gate then trusts,
// so a write chain built on it can stall behind a version no quorum ever
// confirmed. The announcement has to come from the same quorum check every
// other replica runs (attemptTakeover), which is what this arms.
func (d *LocalDataPlane) watchForDurability(ctx context.Context, kbID string, versionID int64) {
	if d.control == nil || d.resolveReplicas == nil || d.presence == nil || d.digest == nil {
		return
	}
	// Cheap gate first: a version already announced needs no timer at all, and
	// this runs on the pull path of every version this node fetches.
	//
	// The answer is only trusted when the local metadata still knows the
	// version. "Not found" is not "settled" — it means this node cannot judge,
	// which falls through to the announcement (harmless, and the safe
	// direction).
	if d.dataStatus != nil {
		// Bounded on its own: on a storage node this is an RPC to the control tier, and a
		// slow answer must not hold up a pull that has already finished.
		statusCtx, cancel := context.WithTimeout(ctx, d.takeoverCheckTimeout)
		st, ok := d.dataStatus.DataStatusOf(statusCtx, kbID, versionID)
		cancel()
		if ok && st != types.DataStatusPending {
			return
		}
	}
	d.WatchVersionWrite(kbID, versionID)
}

// armTakeover places (or replaces) the timer for one version.
//
// The timer learns its own identity as a GENERATION number handed out under the
// lock, not as a pointer to itself. A pointer would have to be written after
// AfterFunc returns and read by the callback: two goroutines racing on one
// variable (measured with -race). A generation is a value fixed before the
// closure exists, and the callback only ever reads it.
func (d *LocalDataPlane) armTakeover(kbID string, versionID int64, attempts int, delay time.Duration) {
	key := failureKey(kbID, versionID)

	d.takeoverMu.Lock()
	d.takeoverGen++
	gen := d.takeoverGen
	d.takeoverMu.Unlock()

	timer := time.AfterFunc(delay, func() { d.attemptTakeover(kbID, versionID, gen, attempts) })

	d.takeoverMu.Lock()
	if old, ok := d.pendingTakeovers[key]; ok {
		old.timer.Stop()
	}
	d.pendingTakeovers[key] = &takeoverWatch{timer: timer, attempts: attempts, gen: gen, delay: delay}
	d.takeoverMu.Unlock()
}

// ConfirmVersionWrite cancels the takeover timer: the coordinator reported that
// the version reached quorum, so there is nothing left for a replica to
// announce (Stratum_设计文档v13.md §7.3).
func (d *LocalDataPlane) ConfirmVersionWrite(kbID string, versionID int64) {
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	if w, ok := d.pendingTakeovers[key]; ok {
		w.timer.Stop()
		delete(d.pendingTakeovers, key)
	}
	d.takeoverMu.Unlock()
}

// attemptTakeover is §7.3's "no coordinator signal arrived" path: count how
// many replicas hold the version and, if that is a quorum, announce it.
//
// It replaces only the *announcement*, never the transfer: the data is already
// on disk here and on whichever peers answer. Announcing is safe to repeat
// across replicas — the proposal is idempotent, the state machine accepts a
// repeated digest for a DURABLE version, and it drops reports for versions that
// are retired (§10.6) — which is exactly why no replica has to be elected to do
// it.
//
// A check that finds no quorum, or cannot finish, is retried a bounded number of
// times (maxTakeoverAttempts): the missing replica is usually still copying, the
// failed report usually a leader election, and both are facts that change. Beyond
// that the version is left to its retry budget.
func (d *LocalDataPlane) attemptTakeover(kbID string, versionID int64, gen uint64, attempts int) {
	key := failureKey(kbID, versionID)
	d.takeoverMu.Lock()
	w, stillPending := d.pendingTakeovers[key]
	// gen guards against a stale timer: WatchVersionWrite may have replaced this
	// watch while the timer was already starting, and the retired timer must not
	// announce on behalf of the new one.
	if !stillPending || w.gen != gen {
		d.takeoverMu.Unlock()
		return // confirmed in the meantime, or superseded by a newer watch
	}
	// Read while the watch is still in hand: the retry interval belongs to this
	// watch, not to the package variable a test may be restoring right now.
	delay := w.delay
	delete(d.pendingTakeovers, key)
	d.takeoverMu.Unlock()

	// A fresh context: the request that delivered the data is long gone by
	// now, and the announcement is this node's own business.
	ctx, cancel := context.WithTimeout(context.Background(), d.takeoverCheckTimeout)
	defer cancel()

	// Already announced? Then there is nothing to say. This is the check that
	// makes arming on every pull affordable: without it every fetch of an
	// already-durable version would pay a Raft proposal plus one presence RPC
	// per peer to re-state a settled fact.
	//
	// Only a readable, non-PENDING verdict short-circuits. "Cannot read" is not
	// "settled", and falling through to the announcement is the safe direction.
	if d.dataStatus != nil {
		if st, ok := d.dataStatus.DataStatusOf(ctx, kbID, versionID); ok && st != types.DataStatusPending {
			d.logger.Debug("plane: takeover: data side already settled, nothing to announce",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
				zap.String("data_status", st.String()))
			return
		}
	}

	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		d.logger.Warn("plane: takeover: resolve replicas",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		d.retryTakeover(kbID, versionID, attempts, delay)
		return
	}

	// This node holds the data (it was pushed, pulled, or replayed here), so
	// its own copy counts — the same "self is one acknowledgement" rule fan-out
	// uses.
	acks := 1
	for _, peer := range peers {
		holds, err := d.presence.HasVersion(ctx, peer, kbID, versionID)
		if err != nil || !holds {
			// Unreachable and not-holding both go uncounted: an unreachable
			// peer is not evidence either way, and the quorum test below is
			// what decides whether the remaining answers suffice.
			continue
		}
		acks++
	}
	quorum := QuorumSize(len(peers) + 1)
	if acks < quorum {
		// The missing replica is usually the one still copying.
		if d.retryTakeover(kbID, versionID, attempts, delay) {
			return
		}
		// Out of attempts: better to let the version run out its retry budget
		// and reach §10.1's terminal state than to claim durability on a
		// minority.
		d.logger.Info("plane: takeover: quorum not reached, leaving the version to its retry budget",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("acks", acks), zap.Int("quorum", quorum), zap.Int("attempts", attempts+1))
		return
	}

	digest, err := d.digest.DigestOf(ctx, kbID, versionID)
	if err != nil {
		d.logger.Warn("plane: takeover: compute digest",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		d.retryTakeover(kbID, versionID, attempts, delay)
		return
	}
	if err := d.control.ReportDataDurable(ctx, kbID, versionID, digest); err != nil {
		if errors.Is(err, stratumerrors.ErrVersionNotFound) {
			// Discarded or deleted while this check was running. The same race the
			// write path already absorbs (see reportAndSchedule): "acted as if it
			// never existed" is an answer, not a failure, and nothing is left to
			// announce.
			d.logger.Debug("plane: takeover: the version is gone (discarded or deleted); nothing to announce",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
			return
		}
		// Usually a leader election. The quorum is proven and this node is no longer
		// behind, so nothing else would ever arm this check again.
		d.logger.Warn("plane: takeover: report durable",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		d.retryTakeover(kbID, versionID, attempts, delay)
		return
	}
	d.logger.Info("plane: takeover: announced a version the coordinator never announced",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("acks", acks))
}

// retryTakeover schedules the next quorum check with a doubling interval, so a long
// outage costs few RPCs while a catch-up tick is still noticed promptly. false means this
// watch has spent its attempts.
func (d *LocalDataPlane) retryTakeover(kbID string, versionID int64, attempts int, delay time.Duration) bool {
	if attempts+1 >= d.takeoverMaxAttempts {
		return false
	}
	next := delay * 2
	if next <= 0 || next > d.takeoverMaxRetryDelay {
		next = d.takeoverMaxRetryDelay
	}
	d.logger.Debug("plane: takeover: re-checking",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
		zap.Int("attempt", attempts+1), zap.Int("max_attempts", d.takeoverMaxAttempts),
		zap.Duration("next_check_in", next))
	d.armTakeover(kbID, versionID, attempts+1, next)
	return true
}

// SafeDurableVersion reports the highest version this node may claim durable
// for kbID after a restart: the largest version a quorum's worth of cursors
// still reaches (Stratum_设计文档v13.md §7.8).
//
// The contiguous cursor lives in memory, so a restarted node starts at 0 and
// cannot tell how far behind it is. Asking the peers is the only way to find
// out, and requiring a quorum's worth of answers is what keeps the claim safe:
// the answer can never overstate progress, because a version only counts if
// enough replicas still report holding it.
//
// ok=false means "there is no replica set to ask" (single-node deployment, or
// nothing wired), in which case the caller falls back to its local view —
// there is nobody to disagree with. An error means a quorum could not be
// assembled: "no answer" must never be mistaken for "no data".
func (d *LocalDataPlane) SafeDurableVersion(ctx context.Context, kbID string) (int64, bool, error) {
	if d.cursorQuerier == nil || d.resolveReplicas == nil {
		return 0, false, nil
	}
	peers, err := d.resolveReplicas(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("plane: SafeDurableVersion: resolve replicas: %w", err)
	}
	if len(peers) == 0 {
		return 0, false, nil
	}

	// This node always counts, even at cursor 0: its own (possibly lost)
	// progress is part of the picture, not an absence of one.
	reports := []int64{d.localVersionOf(kbID)}
	for _, peer := range peers {
		// A SHORT deadline per peer, not the caller's. This runs during STARTUP, and
		// the peers it asks may be inside this very call: every storage node reconciles
		// concurrently, and none of them reaches grpcServer.Serve until its own
		// reconcile has returned. Unbounded here, the cluster deadlocks at boot — each
		// node waits for the others while none of them is serving. (Measured: a storage
		// node holding historical data never logged "Stratum gRPC server listening",
		// and the control layer got "connection refused" from every storage address.)
		//
		// An unreachable peer is not evidence of anything and is skipped either way, so
		// the deadline only decides how long establishing "unreachable" takes — which
		// during startup is the difference between a node that boots and one that hangs.
		peerCtx, cancel := context.WithTimeout(ctx, d.peerCursorTimeout)
		cursor, err := d.cursorQuerier.LocalVersionOf(peerCtx, peer, kbID)
		cancel()
		if err != nil {
			// An unreachable peer is not evidence of anything. It is skipped,
			// and the quorum requirement below decides whether what is left
			// suffices.
			continue
		}
		reports = append(reports, cursor)
	}

	quorum := QuorumSize(len(peers) + 1)
	if len(reports) < quorum {
		return 0, false, fmt.Errorf("plane: SafeDurableVersion: %s: only %d of a required %d cursors arrived",
			kbID, len(reports), quorum)
	}
	// The largest version a quorum's worth of reports still reaches: sorted
	// descending, that is the quorum-th report.
	sort.Slice(reports, func(i, j int) bool { return reports[i] > reports[j] })
	return reports[quorum-1], true, nil
}
