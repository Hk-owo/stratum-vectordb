package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// DefaultDispatchTimeout bounds one background dispatch when the config does not
// say otherwise.
//
// It must stay LARGER than plane's per-candidate budget (15 s floor +
// 50 ms/document, capped at 10 min — see plane.CoordinatorDispatcher): this is
// the OUTER bound, and two budgets of the same size would let the inner one
// expire first, leaving the outer one nothing to report and the caller with a
// write that looks dispatched but is not.
const DefaultDispatchTimeout = 5 * time.Minute

// dispatchClaimWindow is how long a background dispatch's claim explains a later
// miss by the apply hook.
//
// Generous next to the gap it measures (Execute hands off right after the
// proposal returns; the apply loop is a goroutine away) because the cost of
// being too large is only that a genuine "nobody holds these changes" gets
// reported as noise — while the cost of being too small is a false warning on
// every write, which is the state this replaced.
// The default for WriteCoordinatorConfig.DispatchClaimWindow.
const dispatchClaimWindow = 2 * time.Minute

// dispatchKey identifies one in-flight write's pending dispatch.
type dispatchKey struct {
	kbID            string
	clientRequestID string
}

// pendingDispatchEntry is what the dispatcher needs but the Raft command cannot
// carry: the changes to apply.
type pendingDispatchEntry struct {
	parentVersionID int64
	changes         []types.DocChange
}

// RegisterPendingDispatch remembers a write's changes so the apply-time
// dispatcher (§7.13.2) can hand them to the coordinator it picks. Registered
// before the proposal, because the apply may run before Execute has its version
// ID back.
func (c *WriteCoordinatorImpl) RegisterPendingDispatch(kbID, clientRequestID string, parentVersionID int64, changes []types.DocChange) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	if c.pendingDispatch == nil {
		c.pendingDispatch = make(map[dispatchKey]pendingDispatchEntry)
	}
	c.pendingDispatch[dispatchKey{kbID: kbID, clientRequestID: clientRequestID}] = pendingDispatchEntry{
		parentVersionID: parentVersionID,
		changes:         changes,
	}
}

// TakePendingDispatch removes and returns the changes registered for
// (kbID, clientRequestID). Taking rather than reading makes a repeated dispatch
// attempt a no-op instead of a second write of the same version.
func (c *WriteCoordinatorImpl) TakePendingDispatch(kbID, clientRequestID string) (int64, []types.DocChange, bool) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	key := dispatchKey{kbID: kbID, clientRequestID: clientRequestID}
	entry, ok := c.pendingDispatch[key]
	if !ok {
		return 0, nil, false
	}
	delete(c.pendingDispatch, key)
	return entry.parentVersionID, entry.changes, true
}

// claimDispatch records that the proposer's own background dispatch took this
// registration, so a later miss by the apply hook can be attributed to the race
// instead of to the version having no owner. See claimedDispatch.
func (c *WriteCoordinatorImpl) claimDispatch(kbID, clientRequestID string) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	if c.claimedDispatch == nil {
		c.claimedDispatch = make(map[dispatchKey]time.Time)
	}
	now := time.Now()
	// Sweep as we go: claims only matter for dispatchClaimWindow, and this keeps
	// the map bounded without a background goroutine.
	for k, at := range c.claimedDispatch {
		if now.Sub(at) > c.cfg.DispatchClaimWindow {
			delete(c.claimedDispatch, k)
		}
	}
	c.claimedDispatch[dispatchKey{kbID: kbID, clientRequestID: clientRequestID}] = now
}

// DispatchClaimed reports whether the proposer's background dispatch took this
// registration recently — i.e. the write is being handled, not orphaned.
//
// False means no claim is on record, which is what the apply hook's miss needs
// to know before it calls the version ownerless.
func (c *WriteCoordinatorImpl) DispatchClaimed(kbID, clientRequestID string) bool {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	at, ok := c.claimedDispatch[dispatchKey{kbID: kbID, clientRequestID: clientRequestID}]
	return ok && time.Since(at) <= c.cfg.DispatchClaimWindow
}

// ForgetPendingDispatch drops a registration whose proposal never landed, so it
// cannot be dispatched later against a version that does not exist.
func (c *WriteCoordinatorImpl) ForgetPendingDispatch(kbID, clientRequestID string) {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	delete(c.pendingDispatch, dispatchKey{kbID: kbID, clientRequestID: clientRequestID})
}

// dispatchInBackground hands a committed version's write to the coordinator the
// control layer picks (§7.13.2). It runs off the request path: the caller's
// version already exists, and the write's progress is visible through the
// version's status and digest.
func (c *WriteCoordinatorImpl) dispatchInBackground(kbID string, versionID int64, dispatchID string) {
	parentVersionID, changes, ok := c.TakePendingDispatch(kbID, dispatchID)
	if !ok {
		return // the apply hook got there first
	}
	// Claim it so the apply hook, if it is still on its way, can see that this
	// dispatch is the one handling the write — which is the normal outcome here,
	// not an orphaned version.
	c.claimDispatch(kbID, dispatchID)
	c.dispatchWG.Add(1)
	go func() {
		defer c.dispatchWG.Done()

		// Bounded, and it has to be: this context starts here (the client's is
		// long gone), so without a deadline every layer below inherits an
		// unbounded wait — and a stuck dispatch then produces neither a log line
		// nor a state change. Silence is the symptom. The dispatcher bounds each
		// candidate's RPC (plane.CoordinatorDispatcher.candidateBudget), but not
		// candidate resolution, not the retry loop over candidates, and not this
		// hand-off; this is the outer bound, so it is deliberately larger than
		// the per-candidate budget.
		ctx, cancel := context.WithTimeout(context.Background(), c.dispatchTimeout())
		defer cancel()

		if err := c.cfg.Dispatch(ctx, kbID, versionID, parentVersionID, changes); err != nil {
			// Not just a log line: hand it to the same give-up path the apply
			// hook uses, so §10.1's retry budget owns the verdict instead of the
			// version sitting PENDING forever.
			c.AbandonDispatch(ctx, kbID, versionID, dispatchFailureClass(err),
				fmt.Sprintf("dispatch failed: %v", err))
		}
	}()
}

// dispatchTimeout is the outer bound on one background dispatch.
func (c *WriteCoordinatorImpl) dispatchTimeout() time.Duration {
	if c.cfg.DispatchTimeout > 0 {
		return c.cfg.DispatchTimeout
	}
	return DefaultDispatchTimeout
}

// abandonReportTimeout bounds the Raft round trip that records a give-up.
//
// Short, and independent of anything the caller holds: it exists so the give-up path
// can always record its verdict, not to bound work the caller asked for.
// The default for WriteCoordinatorConfig.AbandonReportTimeout.
const abandonReportTimeout = 30 * time.Second

// dispatchFailureClass maps a dispatch failure onto the class §10.1 should see.
//
// The call site used to hardcode FailureTransient, so a permanent refusal — the
// knowledge base is gone, the parent version is invalid, the write was rejected
// outright — spent the version's whole retry budget, keeping it PENDING the entire
// time, before reaching the same verdict it could have reached immediately.
func dispatchFailureClass(err error) types.FailureClass {
	switch {
	case errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound),
		errors.Is(err, stratumerrors.ErrKnowledgeBaseDeleted),
		errors.Is(err, stratumerrors.ErrVersionNotFound),
		errors.Is(err, stratumerrors.ErrInvalidArgument),
		errors.Is(err, stratumerrors.ErrInvalidParentVersion),
		errors.Is(err, stratumerrors.ErrEmptyChanges):
		return types.FailureFatalGlobal
	}
	return types.FailureTransient
}

// AbandonDispatch is the single give-up path of the write path: the ONE place a
// version whose data never landed is handed to §10.1's failure accounting.
//
// What it replaces: two independent code paths (the apply hook finding no
// pending dispatch, and a background dispatch that failed or timed out) each
// logged a warning and returned. Both left the version PENDING for good while
// Query answered FailedPrecondition "version is PENDING" — a message that means
// "try again shortly" about a version nothing was ever going to touch again.
// Whether §10.1 retries it or declares FAILED_PERMANENT is that layer's
// decision; all this does is make sure the decision gets made.
//
// Safe to call repeatedly: the control plane owns the budget.
func (c *WriteCoordinatorImpl) AbandonDispatch(ctx context.Context, kbID string, versionID int64, class types.FailureClass, detail string) {
	log := c.logger()
	if versionID == 0 {
		// Nothing to attribute the failure to: no version was ever allocated
		// (the proposal did not land), so there is no version state to settle.
		log.Warn("coordinator: abandoning a write that never allocated a version",
			zap.String("kb_id", kbID), zap.String("detail", detail))
		return
	}
	log.Warn("coordinator: abandoning a write's data path",
		zap.String("kb_id", kbID), zap.Int64("version_id", versionID),
		zap.String("class", class.String()), zap.String("detail", detail))

	if c.cfg.ControlPlane == nil {
		// No accounting wired (a test stack, or a node without a control plane):
		// the warning above is all there is. Say so rather than pretending.
		log.Warn("coordinator: no control plane wired; this failure is recorded only in the log",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID))
		return
	}
	// The caller's context is very often the one that just expired: a dispatch that ran
	// out its budget is the most common reason to be here at all. Reporting runs a Raft
	// round trip, so inheriting that context guaranteed the report failed too — the one
	// give-up path unable to record the giving-up, leaving the version PENDING with its
	// retry budget already spent (H10 of docs/参考/code-review-2026-10-18.md).
	// WithoutCancel keeps the context's values (trace ids, deadlines that were not the
	// problem) and drops the cancellation, so a fresh bound decides instead.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.AbandonReportTimeout)
	defer cancel()
	terminal, err := c.cfg.ControlPlane.ReportVersionFailure(reportCtx, kbID, versionID, types.FailureSideData, class, detail)
	if err != nil {
		log.Warn("coordinator: reporting a version's failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		return
	}
	if !terminal {
		return // still inside the retry budget: §10.1 will try again
	}
	// The terminal verdict just landed: reclaim whatever physical data made it to disk
	// HERE.
	//
	// No broadcast. §10.6 broadcast because the control layer never knew which replicas
	// had received the version; every replica now learns the verdict from its own apply
	// and reclaims locally (plane.LocalDataPlane.NoteTerminalVersion), so broadcasting
	// again would mainly re-reach replicas already doing it. What this path adds is
	// SPEED on the node that detected the failure.
	if c.cfg.DataPlane == nil {
		return
	}
	if err := c.cfg.DataPlane.ReclaimVersionDataLocally(ctx, kbID, versionID); err != nil {
		log.Warn("coordinator: cleanup after a permanent failure",
			zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
	}
}
