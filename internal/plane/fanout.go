// Replication of one version's data (§7.1, §7.2) and the confirmation broadcast
// that closes it (§7.3, §4.5).
//
// The split of responsibility with write_tx.go is the design's: the transaction
// writes THIS node's copy, the fan-out pushes the same changes to the other
// replicas with a per-replica budget, and the confirmation tells them the quorum
// was reached so the takeover watch can stand down.

package plane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
)

// The confirmation broadcast's budget. All placeholder values in the §10.4 sense:
// what matters is the shape — a per-attempt deadline, a bounded number of
// attempts, and a whole-broadcast ceiling with room for every peer to spend its
// own retries.
const (
	// confirmAttempts is how many times one candidate is told the version reached
	// quorum. A confirmation is idempotent and the receiver only stands down a
	// timer, so a repeat costs nothing.
	confirmAttempts = 3
)

// The timing values are variables rather than constants so a test can shrink them
// (the takeoverTimeout precedent): the case worth pinning down is a peer that
// hangs for its whole attempt deadline, and no test should spend seconds per
// attempt to produce it. Unexported, and written nowhere outside tests.
//
// They are also the defaults behind LocalDataPlaneConfig.Confirm*: a configured
// value wins, these apply when none was given.
var (
	// confirmAttemptTimeout bounds ONE attempt. Same order as peerCursorTimeout:
	// this is a small RPC between peers, and one unreachable peer must not hold
	// the broadcast.
	confirmAttemptTimeout = 2 * time.Second
	// confirmRetryBackoff is the pause between attempts — short enough that a
	// peer's retries fit the broadcast budget, long enough to let a peer that is
	// still coming up become reachable.
	confirmRetryBackoff = 500 * time.Millisecond
	// confirmBroadcastBudget bounds the whole broadcast, resolving the peers
	// included: confirmAttempts × (confirmAttemptTimeout + confirmRetryBackoff)
	// per peer, for a replica set of a few peers, with room to spare.
	confirmBroadcastBudget = 60 * time.Second
)

// broadcastConfirmation tells the candidate replicas that the version reached
// quorum, so any §7.3 takeover timer they started can stand down.
//
// Best effort and asynchronous: the write is already durable and reported, and
// a replica that misses this message only checks for itself later — an extra
// announcement, never a missing one. That asymmetry is why the caller is not
// made to wait on it, and why everything below runs on its own budget.
//
// It goes to *every* candidate rather than only the replicas that acknowledged,
// for the same reason the cleanup broadcast does (§10.6): the coordinator does
// not track which push succeeded, and a stray confirmation to a replica that
// never received the data is harmless.
//
// Each peer gets its OWN context and its own bounded retry budget. Sharing one
// context across the loop was a real defect, and its failure mode is invisible in
// the logs it produces: the first peer to hang — an offline replica is the normal
// case, since the write that triggered this confirmation has just failed to push
// to it — consumed the whole window, and every peer after it then failed instantly
// with "context deadline exceeded" without ever being dialled. Measured with one
// storage node down: the confirmation reached NEITHER of the other two, so no
// quorum ever formed, the version's digest was never committed, and a returning
// node could not tell that version from an empty one — it advances over an empty
// version without fetching anything, so the miss reads as a successful catch-up.
func (d *LocalDataPlane) broadcastConfirmation(kbID string, versionID int64) {
	if d.confirmer == nil || d.resolveReplicas == nil {
		return
	}
	go func() {
		// A fresh context: the request that ran the write is over by now. This
		// bounds the whole broadcast — including resolving the peers — so a slow
		// peer cannot starve the ones behind it.
		ctx, cancel := context.WithTimeout(context.Background(), d.confirmBudget)
		defer cancel()
		peers, err := d.resolveReplicas(ctx)
		if err != nil {
			return
		}
		for _, peer := range peers {
			d.confirmPeer(ctx, peer, kbID, versionID)
		}
	}()
}

// confirmPeer tells one peer that the version reached quorum, retrying a bounded
// number of times with a deadline of its own for each attempt.
//
// A context per attempt (rather than one for the whole peer) is what makes the
// retry mean anything: a single unreachable address should cost one attempt's
// deadline, not the peer's entire budget.
func (d *LocalDataPlane) confirmPeer(ctx context.Context, peer, kbID string, versionID int64) {
	var lastErr error
	for attempt := 1; attempt <= confirmAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, d.confirmAttemptTimeout)
		// The confirmation doubles as the §8.5 announcement: it carries this
		// node's own address so the peer records where the version's data is
		// (see DataSourceRegistry). An empty address means "announce nothing".
		err := d.confirmer.ConfirmVersionWrite(attemptCtx, peer, kbID, versionID, d.selfDataSyncAddr)
		cancel()
		if err == nil {
			if attempt > 1 {
				// Worth a line: it says the earlier attempt was lost to something
				// transient, which is the whole reason the retry exists.
				d.logger.Info("plane: confirm version write landed on a retry",
					zap.String("peer", peer), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID), zap.Int("attempt", attempt))
			}
			return
		}
		lastErr = err
		if attempt < confirmAttempts {
			select {
			case <-ctx.Done():
			case <-time.After(d.confirmRetryBackoff):
			}
		}
	}
	// Still best effort, so this stays a warning: failing the write because a
	// confirmation did not land would turn lost gossip into data loss. The
	// attempts count is what separates "the peer was unreachable" from "we only
	// tried once".
	d.logger.Warn("plane: confirm version write",
		zap.String("peer", peer), zap.String("kb_id", kbID),
		zap.Int64("version_id", versionID), zap.Int("attempts", confirmAttempts), zap.Error(lastErr))
}

// reportFailure tells the control layer that an attempt at making the version
// durable failed. A report error is logged rather than returned: the caller is
// already handling a failure, and losing a report costs retries — not
// correctness, since the verdict is only ever "stop retrying automatically".
func (d *LocalDataPlane) reportFailure(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string) {
	if d.control == nil {
		return
	}
	terminal, err := d.control.ReportVersionFailure(ctx, kbID, versionID, types.FailureSideData, class, detail)
	if err != nil {
		d.logger.Warn("plane: report version failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if !terminal {
		return
	}
	// The verdict just landed: reclaim whatever physical data made it to disk HERE —
	// no broadcast.
	//
	// §10.6 broadcast because the control layer never knew which replicas had received
	// the version. That is no longer the situation: every replica is told by its own
	// apply of the verdict (see NoteTerminalVersion), so broadcasting again would turn
	// N nodes into N×N calls and mostly re-reach replicas already doing it. What this
	// path adds is SPEED — the node that detected the failure cleans up now, instead of
	// when its apply loop reaches the entry.
	if err := d.reclaimTerminalVersionLocally(kbID, versionID); err != nil {
		d.logger.Warn("plane: cleanup after a permanent failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}

// fanoutStages is where one fan-out's time went, split out of `write: stage
// timings` fanout_us: resolving the replica set, then waiting for quorum.
//
// targets is the number of replicas asked (this node's own copy excluded), which
// is what turns "fan-out was slow" into "fan-out waited on N replicas".
type fanoutStages struct {
	resolve time.Duration
	push    time.Duration
	targets int
}

// replicaPushTimeout bounds ONE replica's push during fan-out.
//
// Why not the caller's budget: a push inherits the candidate's dispatch budget
// (plane.CoordinatorDispatcher.candidateBudget = 15 s + 50 ms per document,
// capped at 10 min) when nothing bounds it here, and fan-out waits for every
// target — so ONE unreachable replica costs the whole budget. Measured on the
// 3+3 cluster with one storage replica killed: fanout_us = 15026509 (exactly the
// 15 s budget) inside a dispatch whose own budget is the same 15 s, which made a
// write that HAD reached quorum look like a candidate failure.
//
// It is a floor, not the whole story: PushVersion sends the version's identity,
// and the replica it reaches then pulls and writes the data itself (§7.5), so
// the time a push legitimately needs scales with the batch. A 1000-document
// batch measured pushBudget == the timeout exactly on all three replicas —
// every push timed out while the peers were busy committing the very version
// being pushed — which is what pushTimeoutPerDoc pays for.
//
// The default for LocalDataPlaneConfig.ReplicaPushTimeout.
const replicaPushTimeout = 3 * time.Second

// pushTimeoutPerDoc is what one document's worth of remote work adds to a push's
// budget: the peer pulls the documents and runs its own local transaction
// (measured at ~14.6 ms/document for a 1000-document batch, split/embed/write).
// Without it the budget is a constant while the work is not, and a large batch
// times out on a replica that is doing exactly what it was asked to.
//
// The default for LocalDataPlaneConfig.PushTimeoutPerDoc.
const pushTimeoutPerDoc = 20 * time.Millisecond

func (d *LocalDataPlane) fanOut(ctx context.Context, kbID string, versionID int64, docs int, st *fanoutStages) error {
	if d.pusher == nil || d.resolveReplicas == nil {
		return nil // replication not configured: the local write is the quorum
	}
	resolveStart := time.Now()
	targets, err := d.resolveReplicas(ctx)
	if st != nil {
		st.resolve = time.Since(resolveStart)
		st.targets = len(targets)
	}
	if err != nil {
		return fmt.Errorf("plane: fan-out for version %d: resolve replicas: %w", versionID, err)
	}
	if len(targets) == 0 {
		// 解析不出任何副本。"没有副本"在 quorum 判定上与"副本就是我自己"是同一
		// 件事——于是这一版被当成已复制完成,控制层据此推进。这里从前静默返回成
		// 功,而沉默有代价:多副本部署里它意味着其余副本永远拿不到这一版。
		//
		// 但空列表有两种来源,只有一种是故障。单副本部署里它**结构上就是空的**
		// (ResolveReplicas 列的是"除我之外的副本",而我之外没有副本),那种情况
		// 下本地写就是整个 quorum,是正常结果。ReplicaCount 用来区分二者:不区分
		// 的话每次写都会告警,而真正的故障会淹在里头。
		fields := []zap.Field{
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
			zap.Int("replica_count", d.replicaCount),
		}
		if d.replicaCount <= 1 {
			d.logger.Debug("plane: fan-out has no replica targets; this node's own copy is the whole quorum",
				fields...)
			return nil
		}
		d.logger.Warn("plane: fan-out found no replica targets on a multi-replica deployment; "+
			"treating this node's own copy as the quorum, and the other replicas will not receive this version",
			fields...)
		return nil
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		acked  = 1 // this node's own local write
		needed = QuorumSize(1 + len(targets))
		total  = 1 + len(targets)

		// reached fires the moment quorum is satisfied; allDone fires when every
		// push has settled. Waiting only for the latter is what made a dead
		// replica cost the entire budget: quorum is the contract, and the
		// replicas beyond it are free to be slow.
		reached = make(chan struct{})
		once    sync.Once
		allDone = make(chan struct{})
	)
	// Bound every push by its own budget, and never by more than the caller has
	// left: a push that inherits the dispatch budget would spend it in full on a
	// replica that is simply gone.
	//
	// The budget scales with the batch because the peer's work does: PushVersion
	// hands over the version's identity and the peer pulls and writes those
	// documents itself (§7.5). A constant budget against scaling work times out on
	// a peer doing exactly what it was asked to — measured on a 1000-document
	// batch, where pushBudget was the timeout to the microsecond on all three
	// replicas while each was busy committing the version being pushed.
	pushBudget := d.replicaPushTimeout + time.Duration(docs)*d.pushTimeoutPerDoc
	if deadline, ok := ctx.Deadline(); ok {
		if left := time.Until(deadline); left < pushBudget {
			pushBudget = left
		}
	}
	pushStart := time.Now()
	for _, target := range targets {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			pushCtx, cancel := context.WithTimeout(ctx, pushBudget)
			defer cancel()
			if _, err := d.pusher.PushVersion(pushCtx, target, kbID, versionID); err != nil {
				d.logger.Warn("plane: replica push failed",
					zap.String("replica", target), zap.String("kb_id", kbID),
					zap.Int64("version_id", versionID), zap.Error(err))
				return
			}
			mu.Lock()
			acked++
			satisfied := acked >= needed
			mu.Unlock()
			if satisfied {
				once.Do(func() { close(reached) })
			}
		}(target)
	}
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case <-reached:
		// Quorum is in hand; the remaining pushes keep running in the background
		// with their own budget.
	case <-allDone:
	case <-ctx.Done():
		// The caller's own bound expired: report what we have below.
	}
	if st != nil {
		st.push = time.Since(pushStart)
	}

	mu.Lock()
	final := acked
	mu.Unlock()
	if final < needed {
		return fmt.Errorf(
			"plane: version %d of %s reached %d/%d acknowledgements, below the quorum of %d",
			versionID, kbID, final, total, needed)
	}
	return nil
}

// finishStages is where a completed write's tail went: the fan-out (itself split
// into resolve/push by fanoutStages), the report, and the confirmation broadcast.
type finishStages struct {
	fanTotal time.Duration
	fan      fanoutStages
	report   time.Duration
	confirm  time.Duration
}

// finishVersionWrite is the tail of every write whose local transaction has
// already COMMITted: replicate to the candidate replicas, require a quorum, and
// only then report the version durable.
//
// Both the normal path (WriteVersionData) and the crash-recovery path
// (ResumeVersionWrite) end here. They used to implement this tail separately, and
// the recovery copy skipped the fan-out — going straight to the durable report for
// replicas that were never written to. That is the exact claim the failure branch
// below refuses to make without a quorum ("making it without quorum would be a lie
// the control layer acts on"), so the two paths had ended up disagreeing about what
// durability means. Sharing one function is what keeps them from drifting apart
// again: a step added here reaches both.
func (d *LocalDataPlane) finishVersionWrite(ctx context.Context, kbID string, versionID int64, docs int, docIDs []string, st *finishStages) error {
	fanStart := time.Now()
	var fan fanoutStages
	fanErr := d.fanOut(ctx, kbID, versionID, docs, &fan)
	if st != nil {
		st.fanTotal = time.Since(fanStart)
		st.fan = fan
	}
	if fanErr != nil {
		// The local transaction above already committed: this node holds the
		// version's documents, its document-ID set and its WAL commit record. What
		// fan-out adds is durability on OTHER replicas — which §7.5 restores by
		// backfill — not the version's existence, and not its ability to be served
		// from here.
		//
		// This path used to schedule an index build here as well, so that a version
		// whose replication fell short still had an index. That is no longer
		// needed and no longer happens: §8.6(b) builds a version when something
		// asks for it, and a query for a version with no index builds one on the
		// spot (service/query.go) — including a version the control layer has left
		// PENDING, which is what that workaround was really about. Leaving the
		// build here would mean every write builds eagerly, which is the cost
		// lazy construction exists to avoid.
		//
		// Note what is deliberately NOT done: no durable report either — see below.
		// Replication failed, and it is still transient (peers come back), so it is
		// reported and returned — the caller's Saga owns the retry decision (§10.1).
		// Note what is deliberately NOT done: no durable report. Durability is
		// precisely the claim quorum was supposed to establish, and making it
		// without quorum would be a lie the control layer acts on.
		d.reportFailure(ctx, kbID, versionID, types.FailureTransient, fmt.Sprintf("replication failed: %v", fanErr))
		return fanErr
	}

	reportStart := time.Now()
	d.reportAndSchedule(ctx, kbID, versionID, docIDs)
	if st != nil {
		st.report = time.Since(reportStart)
	}

	// len(docIDs) == 0 is the whole reason the announcement carries a flag: a
	// version with no documents is never fanned out (fanOut above sends nothing),
	// so replicas that did NOT coordinate it have no other way to learn it exists —
	// and their cursors would stay behind it, which the station reads as "stale"
	// (§9.3(2)).
	confirmStart := time.Now()
	d.broadcastConfirmation(kbID, versionID)
	if st != nil {
		st.confirm = time.Since(confirmStart)
	}
	return nil
}

// reportAndSchedule finishes a completed write transaction: it reports the
// version's document-set digest up (v1 §4.2 ReportDataDurable — followers use
// it to verify their data pulls) and schedules the index build. Both stay
// best-effort, exactly as they were inside the coordinator: a missed digest
// only costs followers a best-effort pull, and a failed build can be retried.
func (d *LocalDataPlane) reportAndSchedule(ctx context.Context, kbID string, versionID int64, docIDs []string) {
	if d.control != nil {
		digest := stratinternalsync.ComputeDocIDSetHash(docIDs)
		if err := d.control.ReportDataDurable(ctx, kbID, versionID, digest); err != nil {
			if errors.Is(err, stratumerrors.ErrVersionNotFound) {
				// The version is gone — discarded by its caller, or deleted — while
				// its write path was still finishing. Discarding means "act as if
				// this never existed", so a later digest report for it has nowhere
				// to land: expected, not a failure. The index build's callback
				// reaches the same conclusion the same way (see internal/index's
				// invokeCallback).
				//
				// Telling this apart matters because the warning below is the one
				// worth reading — a version with no durable digest never leaves
				// PENDING — and firing it on every discard is what buries that.
				d.logger.Debug("plane: the version is gone (discarded or deleted); no digest to report",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
			} else {
				// This used to be discarded (`_ =`), and the silence is expensive: with no
				// digest the version carries no evidence of having been durably
				// replicated, so cursor recovery reads it as "not held here"
				// (holdsVersionLocally) and the data side never leaves PENDING.
				// Measured on a 3-node cluster: not one version reached DATA_DURABLE, while
				// fan-out itself never failed — so whatever goes wrong, it goes wrong
				// here.
				d.logger.Warn("plane: reporting the version's digest failed; it keeps no durable digest",
					zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
			}
		} else {
			d.logger.Debug("plane: reported the version's digest",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("docs", len(docIDs)))
		}
	} else {
		d.logger.Debug("plane: no control plane wired; the version's digest is not reported",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
	}
}
