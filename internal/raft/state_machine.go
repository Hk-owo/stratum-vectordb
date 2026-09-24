package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// applyResult is what a single command application produces: an error if
// the command was rejected by a deterministic validation check (e.g. an
// invalid parent version), or, for cmdCreateVersion specifically, the
// newly assigned VersionID.
type applyResult struct {
	VersionID int64
	// DeletedVersionIDs is set by cmdMarkVersionDeleting: every version
	// that command marked Deleting, so the caller can report the exact
	// impact set (for ANCESTORS that is every swept-up 前置版本).
	// Empty for every other command.
	DeletedVersionIDs []int64
	Err               error
}

// stateMachine holds the in-memory knowledge base and version metadata
// replicated via Raft. It is mutated exclusively by stateMachine.apply,
// called from RaftNodeImpl's single apply-dispatch loop — so apply itself
// needs no internal synchronization against concurrent writers, but reads
// (GetKB, ListVersions) can happen concurrently from other goroutines and
// so still need the mutex.
type stateMachine struct {
	mu            sync.RWMutex
	kbs           map[string]types.KnowledgeBaseMeta
	versions      map[int64]types.VersionMeta
	versionsByKB  map[string][]int64
	nextVersionID int64
	// versionsByRequest maps a client idempotency key (see requestKey) to the
	// version ID the first apply of that request allocated. It is what lets a
	// retried CreateVersion reuse its version instead of allocating another
	// one, so a client can re-send the changes for a version whose data never
	// landed (Stratum_设计文档v13.md §7.12). Entries live exactly as long as
	// the version metadata they point at.
	versionsByRequest map[string]int64
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		kbs:               make(map[string]types.KnowledgeBaseMeta),
		versions:          make(map[int64]types.VersionMeta),
		versionsByKB:      make(map[string][]int64),
		nextVersionID:     1,
		versionsByRequest: make(map[string]int64),
	}
}

// apply deterministically applies cmd to the state machine. Called from
// the single apply-dispatch loop, identically (same input, same code
// path) on every node that applies this log entry — followers included,
// not just whichever node originally proposed it. w is the Stratum WAL
// (used only by cmdCreateVersion, for the WAL-before-state-machine
// ordering described on RaftNodeImpl).
func (sm *stateMachine) apply(ctx context.Context, cmd command, w wal.WAL, logger *zap.Logger) applyResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch cmd.Type {
	case cmdCreateKB:
		kb := *cmd.KB
		if kb.Status == types.KBStatusActive || kb.Status == 0 {
			kb.Status = types.KBStatusActive
		}
		sm.kbs[kb.KBID] = kb
		return applyResult{}

	case cmdMarkKBDeleting:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
		}
		kb.Status = types.KBStatusDeleting
		sm.kbs[cmd.KBID] = kb
		return applyResult{}

	case cmdMarkKBDeleteFailed:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
		}
		kb.Status = types.KBStatusDeleteFailed
		sm.kbs[cmd.KBID] = kb
		return applyResult{}

	case cmdRemoveKBMeta:
		// Idempotent: deleting an already-absent key is a no-op, not an
		// error, so the delete flow's crash-recovery path can safely
		// re-propose this any number of times.
		delete(sm.kbs, cmd.KBID)
		for _, versionID := range sm.versionsByKB[cmd.KBID] {
			delete(sm.versions, versionID)
		}
		delete(sm.versionsByKB, cmd.KBID)
		sm.dropRequestMappings(cmd.KBID, 0)
		return applyResult{}

	case cmdCreateVersion:
		return sm.applyCreateVersion(ctx, cmd, w, logger)

	case cmdUpdateVersionStatus:
		v, ok := sm.versions[cmd.VersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}
		}
		v.IndexStatus = cmd.Status
		// §8.6(d)/§1.2: a replica reporting its own index READY is recorded by
		// IDENTITY. The status answers "is this serviceable somewhere"; the
		// identities answer the question a node asks right before it takes itself
		// out of service to reclaim an artifact — "how many are still serving?" —
		// and it needs them because it has to exclude itself from the count.
		//
		// nodeID == 0 means the control layer is setting a status itself (a
		// reconcile promotion, an availability verdict) rather than relaying a
		// replica's fact, so nothing is recorded: inventing a node there would
		// make the count lie in the direction that permits unsafe cleanup.
		if cmd.NodeID != 0 && cmd.Status == types.IndexStatusReady {
			v.IndexReadyNodes = withIndexReadyNode(v.IndexReadyNodes, cmd.NodeID)
		}
		sm.versions[cmd.VersionID] = v
		return applyResult{}

	case cmdUpdateVersionSummary:
		v, ok := sm.versions[cmd.VersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}
		}
		// A late confirmation must not resurrect a settled version
		// (Stratum_设计文档v13.md §10.6): the client may already have given up
		// on this write and run its compensation path, so letting the data
		// suddenly become queryable would be worse than the original failure.
		// The check belongs here because the state machine knows the status
		// deterministically, while a reporter only knows its own timing.
		//
		// "Settled" is judged on the DATA side only. IndexStatusReady must NOT be
		// consulted here, for the same reason the two sides are separate states at
		// all: an index reaches READY on every replica that builds it — with a small
		// knowledge base that takes milliseconds, and fan-out has just handed the
		// documents over — while the writer's digest proposal is still travelling
		// through Raft. Reading "index is READY" as "this version is settled, drop
		// the digest" therefore discards the very confirmation that would have made
		// the data durable, on a version that is perfectly healthy. Measured on a
		// 3-node cluster: fan-out never failed and every digest proposal was
		// accepted, yet not one version reached DATA_DURABLE — the confirmation was
		// dropped here every time.
		//
		// (IndexStatusFailedPermanent stays: that verdict retires the whole version,
		// not merely its index.)
		if v.DataStatus == types.DataStatusFailedPermanent ||
			v.IndexStatus == types.IndexStatusFailedPermanent {
			logger.Debug("raft: digest dropped, the version is retired",
				zap.Int64("version_id", cmd.VersionID),
				zap.String("data_status", v.DataStatus.String()),
				zap.String("index_status", v.IndexStatus.String()))
			return applyResult{}
		}
		v.DocIDSetHash = cmd.DocIDSetHash
		// The writer commits this digest only after its own storage writes
		// finished and a quorum confirmed them (ControlPlane.ReportDataDurable),
		// so its arrival is exactly what makes the DATA side durable
		// (Stratum_设计文档v13.md §10.1b).
		v.DataStatus = types.DataStatusDurable
		sm.versions[cmd.VersionID] = v
		return applyResult{}

	case cmdMarkDataDurable:
		return sm.applyMarkDataDurable(cmd)

	case cmdMarkVersionFailedPermanent:
		return sm.applyMarkVersionFailedPermanent(cmd)

	case cmdRollback:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
		}
		v, ok := sm.versions[cmd.TargetVersionID]
		if !ok || v.KBID != cmd.KBID {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}
		}
		if v.Deleting {
			return applyResult{Err: fmt.Errorf("target version %d is being deleted: %w", cmd.TargetVersionID, stratumerrors.ErrVersionDeleting)}
		}
		kb.ActiveVersionID = cmd.TargetVersionID
		sm.kbs[cmd.KBID] = kb
		return applyResult{}

	case cmdRetryVersion:
		return sm.applyRetryVersion(cmd)

	case cmdMarkVersionDeleting:
		return sm.applyMarkVersionDeleting(cmd)

	case cmdRemoveVersionMeta:
		return sm.applyRemoveVersionMeta(cmd)

	case cmdDiscardVersion:
		return sm.applyDiscardVersion(cmd)

	default:
		return applyResult{Err: fmt.Errorf("raft: unknown command type %q", cmd.Type)}
	}
}

// applyMarkDataDurable records the control layer's own promotion of a version's
// DATA side to durable (Stratum_设计文档v13.md §10.1b).
//
// It is the startup reconcile's path: the storage layer reports a contiguous
// cursor (§7.8's quorum minimum) and §7.9 turns it into replicated state. The
// writer's own path is cmdUpdateVersionSummary, which commits the digest at the
// same time; this one carries no digest because none was reported.
//
// Only a PENDING side moves. A version already DURABLE stays where it is, and a
// version carrying a terminal data verdict is left alone — which is what keeps a
// reconcile snapshot, by construction older than that verdict, from undoing it.
func (sm *stateMachine) applyMarkDataDurable(cmd command) applyResult {
	v, ok := sm.versions[cmd.VersionID]
	if !ok {
		return applyResult{Err: stratumerrors.ErrVersionNotFound}
	}
	if v.DataStatus != types.DataStatusPending {
		return applyResult{}
	}
	v.DataStatus = types.DataStatusDurable
	sm.versions[cmd.VersionID] = v
	return applyResult{}
}

// applyMarkVersionFailedPermanent records the terminal state for ONE SIDE of a
// version: the control layer has decided that side's Saga will not be retried
// automatically (Stratum_设计文档v13.md §10.1, §10.1b). The storage layer only
// reports failures — deciding when the budget is spent belongs to the control
// layer, and keeping the verdict here (rather than in a node's memory) is what
// makes it deterministic across replicas.
//
// The two sides have separate states and so separate verdicts: cmd.FailureSide
// picks which one this command settles. Leaving the other side untouched is the
// point — a version whose data never landed is not thereby a version whose
// index failed, and an operator reading the metadata must be able to tell.
//
// Idempotent: re-applying overwrites the recorded reason and count, so a
// replayed log entry converges instead of failing.
func (sm *stateMachine) applyMarkVersionFailedPermanent(cmd command) applyResult {
	v, ok := sm.versions[cmd.VersionID]
	if !ok {
		return applyResult{Err: stratumerrors.ErrVersionNotFound}
	}
	if v.KBID != cmd.KBID {
		return applyResult{Err: fmt.Errorf("version %d belongs to a different knowledge base: %w", cmd.VersionID, stratumerrors.ErrVersionNotFound)}
	}
	switch cmd.FailureSide {
	case types.FailureSideIndex:
		v.IndexStatus = types.IndexStatusFailedPermanent
	default:
		v.DataStatus = types.DataStatusFailedPermanent
	}
	v.FailureReason = cmd.FailureReason
	v.FailureCount = cmd.FailureCount
	// Recorded, not inferred: a version can end up terminal on BOTH sides, and
	// then only this field says which one the cause chain above describes
	// (Stratum_设计文档v13.md §10.1b).
	v.FailureSide = cmd.FailureSide
	sm.versions[cmd.VersionID] = v
	return applyResult{}
}

// applyRetryVersion handles cmdRetryVersion: an operator revoking the control
// layer's terminal verdict for ONE side of a version (Stratum_设计文档v13.md §10.1).
//
// It is deliberately narrow in three ways, each of which encodes a decision rather
// than an omission:
//
//   - It accepts only a side that IS terminal. Anything else is refused with
//     ErrVersionNotFailedPermanent rather than quietly accepted: a version that is
//     merely FAILED is already retried by the ordinary paths, one that is PENDING or
//     READY has nothing to retry, and reporting success for either would tell the
//     operator a state changed when nothing did.
//
//   - It refuses the DATA side outright. An index-side verdict is a statement about a
//     BUILD, and a build can be revoked by rebuilding; the data side's verdict says
//     the version's data will never arrive, so there is no write to re-attempt from
//     here. The operator's answer to that verdict is to abandon the version
//     (ForceAbandonVersion → DeleteVersion), and this refusal says so instead of
//     pretending to retry.
//
//   - It drops the recorded cause chain only once NEITHER side is terminal. The chain
//     describes the failure an operator is looking at; clearing it while the other
//     side is still dead would erase the diagnosis of a verdict still in force.
//
// Re-applying is refused by the first rule (the side is PENDING by then) rather than
// silently succeeding — the honest answer for a replay, since the state is already
// what the caller asked for. A retry that fails again goes through
// ReportVersionFailure and lands back in the terminal state with a fresh count.
func (sm *stateMachine) applyRetryVersion(cmd command) applyResult {
	v, ok := sm.versions[cmd.VersionID]
	if !ok {
		return applyResult{Err: stratumerrors.ErrVersionNotFound}
	}
	if v.KBID != cmd.KBID {
		return applyResult{Err: fmt.Errorf("version %d belongs to a different knowledge base: %w", cmd.VersionID, stratumerrors.ErrVersionNotFound)}
	}
	if v.Deleting {
		// A version already on its way out has no state left to revive: its data is
		// being reclaimed and its metadata is about to be removed.
		return applyResult{Err: fmt.Errorf("version %d is being deleted: %w", cmd.VersionID, stratumerrors.ErrVersionDeleting)}
	}
	switch cmd.FailureSide {
	case types.FailureSideIndex:
		if v.IndexStatus != types.IndexStatusFailedPermanent {
			return applyResult{Err: fmt.Errorf("version %d's index side is %s, not FAILED_PERMANENT: %w",
				cmd.VersionID, v.IndexStatus.String(), stratumerrors.ErrVersionNotFailedPermanent)}
		}
		v.IndexStatus = types.IndexStatusPending
	default:
		// Two different refusals in one branch, and the message has to say which:
		// either the caller asked to retry the DATA side, which is never retryable,
		// or the data side was not terminal to begin with.
		if v.DataStatus == types.DataStatusFailedPermanent {
			return applyResult{Err: fmt.Errorf("version %d's data side is FAILED_PERMANENT: a data-side verdict cannot be retried, abandon the version instead: %w",
				cmd.VersionID, stratumerrors.ErrVersionNotFailedPermanent)}
		}
		return applyResult{Err: fmt.Errorf("version %d's data side is %s, not FAILED_PERMANENT: %w",
			cmd.VersionID, v.DataStatus.String(), stratumerrors.ErrVersionNotFailedPermanent)}
	}
	if v.IndexStatus != types.IndexStatusFailedPermanent && v.DataStatus != types.DataStatusFailedPermanent {
		v.FailureReason = ""
		v.FailureCount = 0
		v.FailureSide = types.FailureSideData
	}
	sm.versions[cmd.VersionID] = v
	return applyResult{}
}

// applyMarkVersionDeleting handles cmdMarkVersionDeleting: validates the
// DeleteVersion constraints for the exact version set selected by cmd.Mode,
// applies the structural rewiring that mode implies, then marks that set as
// Deleting.
//
// The three modes (types.VersionDeleteMode):
//
// The chain is strictly linear (a parent has at most one child, enforced by
// applyCreateVersion), so these degrade to prefix/suffix trims:
//
//   - SUBTREE: versionID plus every descendant — i.e. the tail from
//     versionID onwards.
//   - SINGLE: versionID only; its single direct child is re-parented onto
//     versionID's parent (a linked-list splice), which is what lets an
//     arbitrary "middle" version be dropped without losing the chain below.
//   - ANCESTORS: every ancestor (前置版本) of versionID — i.e. the prefix
//     up to versionID; versionID becomes the new base (root) of the
//     knowledge base by having its ParentVersionID cleared.
//
// Constraints are evaluated against the state machine snapshot at apply
// time, so the descendant/ancestor set is deterministic across nodes
// regardless of which node proposed the command. The whole set is
// validated before anything is mutated: a partial mark followed by a
// rejection would leave the tree half-deleting. Idempotent: re-proposing
// for an already-Deleting set passes validation and re-marks it (a no-op),
// which is what lets the async cleanup flow resume safely after a crash.
func (sm *stateMachine) applyMarkVersionDeleting(cmd command) applyResult {
	if _, ok := sm.kbs[cmd.KBID]; !ok {
		return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
	}
	root, ok := sm.versions[cmd.VersionID]
	if !ok {
		return applyResult{Err: stratumerrors.ErrVersionNotFound}
	}
	if root.KBID != cmd.KBID {
		return applyResult{Err: fmt.Errorf("version %d belongs to a different knowledge base: %w", cmd.VersionID, stratumerrors.ErrVersionNotFound)}
	}

	targets, err := sm.versionDeleteTargets(cmd.KBID, cmd.VersionID, cmd.Mode)
	if err != nil {
		return applyResult{Err: err}
	}

	// Validate the entire set before marking anything: a partial mark
	// followed by a rejection would leave the tree half-deleting.
	for _, versionID := range targets {
		v := sm.versions[versionID]
		if sm.kbs[cmd.KBID].ActiveVersionID == versionID {
			return applyResult{Err: fmt.Errorf("version %d is the active version of %s: %w", versionID, cmd.KBID, stratumerrors.ErrVersionIsActive)}
		}
		if deleteBlockedByPending(v) {
			return applyResult{Err: fmt.Errorf("version %d is PENDING: %w", versionID, stratumerrors.ErrVersionPending)}
		}
	}
	if err := sm.validateSurvivorsNotPending(cmd.KBID, targets); err != nil {
		return applyResult{Err: err}
	}

	// Structural rewiring, only after validation succeeded. An ANCESTORS
	// delete with the parent pointer already clear is the "already the
	// base" no-op — the condition checks the pointer itself, not the
	// target set, so a broken chain (parent metadata already gone) is
	// healed into a proper root as well. Both cases report an empty
	// DeletedVersionIDs; the broken-chain case still clears the stale
	// pointer, which is the only side effect of an otherwise empty delete.
	switch cmd.Mode {
	case types.VersionDeleteSingle:
		sm.reparentChildren(cmd.KBID, cmd.VersionID, sm.spliceParent(cmd.KBID, root.ParentVersionID))
	case types.VersionDeleteAncestors:
		if root.ParentVersionID != 0 {
			root.ParentVersionID = 0
			sm.versions[cmd.VersionID] = root
		}
	}

	for _, versionID := range targets {
		v := sm.versions[versionID]
		v.Deleting = true
		sm.versions[versionID] = v
	}
	return applyResult{DeletedVersionIDs: targets}
}

// validateSurvivorsNotPending rejects a delete that would strand a still
// PENDING survivor: a version that stays alive but whose parent is in removed
// still needs that parent's VersionDocList to finish its storage writes
// (WriteCoordinatorImpl.writeVersionDocList reads the parent set) and to be
// rebuilt after a crash (cmd/stratum's replay uses the recorded
// ParentVersionID). Once the async cleanup drops that list, the survivor's
// doc set silently loses every document it inherited.
//
// The SUBTREE rule "no PENDING version inside the removed set" covered this
// before rewiring existed, because every descendant was removed too; SINGLE
// and ANCESTORS deliberately keep descendants alive, so the survivors get
// their own check.
//
// "Still PENDING" is a conservative proxy for "storage writes not finished": a
// version reaches READY only when its index build callback fires, which is
// strictly later than WriteCoordinator writing its full doc set. The guard
// therefore rejects a superset of the truly unsafe window, never a subset of
// it; FAILED versions are not a problem either, since their writes completed
// before the build was even triggered. Under SUBTREE the check is a no-op —
// that set is closed downwards, so no survivor's parent is in it.
//
// Which statuses count as PENDING is decided by deleteBlockedByPending, shared
// with the removed-set rule above so the two cannot drift apart: a data-side
// terminal verdict is not "still writing" on either side of the check.
func (sm *stateMachine) validateSurvivorsNotPending(kbID string, removed []int64) error {
	if len(removed) == 0 {
		return nil
	}
	removedSet := make(map[int64]bool, len(removed))
	for _, id := range removed {
		removedSet[id] = true
	}
	for _, id := range sm.versionsByKB[kbID] {
		if removedSet[id] {
			continue
		}
		v := sm.versions[id]
		if !removedSet[v.ParentVersionID] {
			continue
		}
		if deleteBlockedByPending(v) {
			return fmt.Errorf("version %d is PENDING and still depends on its to-be-deleted parent %d: %w", id, v.ParentVersionID, stratumerrors.ErrVersionPending)
		}
	}
	return nil
}

// deleteBlockedByPending reports whether v still counts as PENDING for
// DeleteVersion's admission rules — the question those rules are really asking is
// "may this version's storage writes still be in flight?" (see
// validateSurvivorsNotPending for why that matters).
//
// The rules were written when ONE status field carried both halves of a version's
// Saga, and they read that field as a proxy for "writes not finished"
// (Stratum_设计文档v13.md §10.1b). Now that the halves are separate, the proxy has to be
// read off the DATA side, and the one status that must NOT be read as "still
// writing" is the data side's terminal verdict: a version whose data will never
// arrive has no write left to wait for, and §10.6 has already reclaimed whatever had
// landed on some replica. Reading it as PENDING is not a conservative default, it is
// a dead end — neither DeleteVersion (this rule) nor DiscardVersion (which admits
// only a version whose data side is PENDING) will take such a version, so a verdict
// on the data side would strand it in the chain forever.
//
// IndexStatus still decides the rest, deliberately: PENDING with durable data means
// the index build is what is outstanding, and that window is exactly when dropping
// the version's VersionDocList would hurt a survivor. The same reasoning explains
// what is NOT changed: applyCreateVersion's parent check keeps refusing a PENDING
// parent even when that PENDING is a data-side verdict, because a new version
// derives its document set from the parent's, and a parent whose data will never
// arrive cannot contribute one — "you cannot build on it" is the right answer there,
// not an oversight.
func deleteBlockedByPending(v types.VersionMeta) bool {
	return v.IndexStatus == types.IndexStatusPending && v.DataStatus != types.DataStatusFailedPermanent
}

// versionDeleteTargets computes, deterministically from the current state
// machine contents, the exact set of versions cmd.Mode removes for
// versionID.
func (sm *stateMachine) versionDeleteTargets(kbID string, versionID int64, mode types.VersionDeleteMode) ([]int64, error) {
	switch mode {
	case types.VersionDeleteSubtree:
		return sm.collectVersionSubtree(kbID, versionID), nil

	case types.VersionDeleteSingle:
		return []int64{versionID}, nil

	case types.VersionDeleteAncestors:
		ancestors := sm.collectVersionAncestors(kbID, versionID)
		if len(ancestors) == 0 {
			return nil, nil // already the base: nothing ahead of it
		}
		// Keep versionID and everything after it. Anything reachable from an
		// ancestor but not from versionID — the ancestors themselves — is
		// removed with it. The chain is strictly linear, so there are no
		// sibling branches to consider.
		keep := make(map[int64]bool)
		for _, id := range sm.collectVersionSubtree(kbID, versionID) {
			keep[id] = true
		}
		seen := make(map[int64]bool)
		var out []int64
		for _, ancestor := range ancestors {
			for _, id := range sm.collectVersionSubtree(kbID, ancestor) {
				if keep[id] || seen[id] {
					continue
				}
				seen[id] = true
				out = append(out, id)
			}
		}
		return out, nil

	default:
		return nil, fmt.Errorf("unknown version delete mode %d: %w", mode, stratumerrors.ErrInvalidArgument)
	}
}

// collectVersionAncestors returns every ancestor (前置版本) of versionID
// within kbID, walking ParentVersionID edges upward. Deterministic given
// the state machine contents.
func (sm *stateMachine) collectVersionAncestors(kbID string, versionID int64) []int64 {
	var out []int64
	seen := map[int64]bool{versionID: true}
	cur := versionID
	for {
		v, ok := sm.versions[cur]
		if !ok {
			return out
		}
		parentID := v.ParentVersionID
		if parentID == 0 || seen[parentID] {
			return out
		}
		parent, ok := sm.versions[parentID]
		if !ok || parent.KBID != kbID {
			return out
		}
		seen[parentID] = true
		out = append(out, parentID)
		cur = parentID
	}
}

// reparentChildren rewires the child of fromVersionID within kbID onto
// newParentVersionID (0 meaning "becomes a root"). The chain is strictly
// linear, so there is at most one such child — this is a linked-list splice,
// and everything below it moves with it.
func (sm *stateMachine) reparentChildren(kbID string, fromVersionID, newParentVersionID int64) {
	for _, id := range sm.versionsByKB[kbID] {
		if id == fromVersionID {
			continue
		}
		v, ok := sm.versions[id]
		if !ok || v.ParentVersionID != fromVersionID {
			continue
		}
		v.ParentVersionID = newParentVersionID
		sm.versions[id] = v
	}
}

// spliceParent returns the version a removed version's children should be
// re-attached to: its parent, or 0 (becoming roots) when that parent is
// missing or is itself being deleted — re-attaching onto a version whose
// metadata is about to be removed would leave the children with a dangling
// ParentVersionID.
func (sm *stateMachine) spliceParent(kbID string, parentVersionID int64) int64 {
	if parentVersionID == 0 {
		return 0
	}
	parent, ok := sm.versions[parentVersionID]
	if !ok || parent.KBID != kbID || parent.Deleting {
		return 0
	}
	return parentVersionID
}

// requestKey is the idempotency-map key for a client request: the knowledge
// base is part of it, so two KBs may reuse the same client-generated id.
func requestKey(kbID, clientRequestID string) string {
	return kbID + "\x00" + clientRequestID
}

// dropRequestMappings removes idempotency entries belonging to kbID (all of
// them), or — when versionID > 0 — only the ones pointing at that version.
// Called whenever the metadata they refer to goes away, so a reused key can
// never resolve to a version that no longer exists.
func (sm *stateMachine) dropRequestMappings(kbID string, versionID int64) {
	for key, id := range sm.versionsByRequest {
		if versionID > 0 {
			if id == versionID {
				delete(sm.versionsByRequest, key)
			}
			continue
		}
		if strings.HasPrefix(key, kbID+"\x00") {
			delete(sm.versionsByRequest, key)
		}
	}
}

// applyRemoveVersionMeta handles cmdRemoveVersionMeta: removes a single
// version's metadata from the state machine. Idempotent: deleting an
// already-absent version succeeds, mirroring cmdRemoveKBMeta's
// crash-recovery semantics.
func (sm *stateMachine) applyRemoveVersionMeta(cmd command) applyResult {
	v, ok := sm.versions[cmd.VersionID]
	if !ok {
		return applyResult{} // idempotent
	}
	if v.KBID != cmd.KBID {
		return applyResult{Err: fmt.Errorf("version %d belongs to a different knowledge base: %w", cmd.VersionID, stratumerrors.ErrVersionNotFound)}
	}
	delete(sm.versions, cmd.VersionID)
	list := sm.versionsByKB[cmd.KBID]
	for i, id := range list {
		if id == cmd.VersionID {
			sm.versionsByKB[cmd.KBID] = append(list[:i], list[i+1:]...)
			break
		}
	}
	sm.dropRequestMappings(cmd.KBID, cmd.VersionID)
	return applyResult{}
}

// applyDiscardVersion handles cmdDiscardVersion: the CALLER's declaration that
// it is abandoning a version whose write never landed
// (docs/await-version-plan.md §7 Step 6).
//
// It is not DeleteVersion under another name, and the two refuse each other's
// work on purpose:
//
//   - DeleteVersion requires a version that HAS state to reclaim (it refuses
//     PENDING outright, and its flow rewires children, checks the active version
//     and drives asynchronous cleanup);
//   - discarding is about a version that never acquired any: no data, no index,
//     nothing to reclaim. Removing the metadata is the whole operation.
//
// The admission check is a compare-and-set inside the apply, not a read followed
// by a decision in the caller: the version must still be PENDING *at apply time*.
// A caller deciding to discard from a data_missing observation may race a write
// that just landed, and this is what keeps that race from deleting durable data
// (§5 contract 7).
func (sm *stateMachine) applyDiscardVersion(cmd command) applyResult {
	kb, ok := sm.kbs[cmd.KBID]
	if !ok {
		return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
	}
	v, ok := sm.versions[cmd.VersionID]
	if !ok || v.KBID != cmd.KBID {
		return applyResult{Err: stratumerrors.ErrVersionNotFound}
	}
	if kb.ActiveVersionID == cmd.VersionID {
		return applyResult{Err: fmt.Errorf("version %d is the active version of %s: %w", cmd.VersionID, cmd.KBID, stratumerrors.ErrVersionIsActive)}
	}
	if v.DataStatus != types.DataStatusPending || v.Deleting {
		return applyResult{Err: fmt.Errorf("version %d is not PENDING (data_status=%s, deleting=%v): %w",
			cmd.VersionID, v.DataStatus.String(), v.Deleting, stratumerrors.ErrVersionNotPending)}
	}
	// A PENDING version cannot have children (applyCreateVersion refuses a PENDING
	// parent), so this guards an invariant rather than handling a case that arises
	// today: discarding a parent would leave its child inheriting from a version
	// that no longer exists.
	for _, id := range sm.versionsByKB[cmd.KBID] {
		if child, ok := sm.versions[id]; ok && child.ParentVersionID == cmd.VersionID {
			return applyResult{Err: fmt.Errorf("version %d has child version %d: %w", cmd.VersionID, id, stratumerrors.ErrInvalidParentVersion)}
		}
	}
	delete(sm.versions, cmd.VersionID)
	list := sm.versionsByKB[cmd.KBID]
	for i, id := range list {
		if id == cmd.VersionID {
			sm.versionsByKB[cmd.KBID] = append(list[:i], list[i+1:]...)
			break
		}
	}
	// The idempotency mapping goes with it: the caller is starting over, and
	// leaving the mapping behind would make a re-send under the same key resolve
	// to a version that no longer exists (§7 Step 6).
	sm.dropRequestMappings(cmd.KBID, cmd.VersionID)
	return applyResult{}
}

// collectVersionSubtree returns rootID plus every descendant of rootID
// within kbID (following ParentVersionID edges), in an unspecified order.
// Deterministic given the state machine contents; used by both the
// mark-deleting and metadata-removal paths so leader and followers agree on
// the deleted set.
func (sm *stateMachine) collectVersionSubtree(kbID string, rootID int64) []int64 {
	visited := make(map[int64]bool)
	queue := []int64{rootID}
	var out []int64
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if visited[id] {
			continue
		}
		visited[id] = true
		out = append(out, id)
		for _, candidate := range sm.versionsByKB[kbID] {
			if candidate == id {
				continue
			}
			child, ok := sm.versions[candidate]
			if ok && child.ParentVersionID == id && !visited[candidate] {
				queue = append(queue, candidate)
			}
		}
	}
	return out
}

// applyCreateVersion handles cmdCreateVersion: validates the parent-version
// constraints, deterministically assigns the next version ID from the
// replicated counter, durably records the pending write in the local WAL
// (per the critical WAL-before-state-machine ordering — see the
// RaftNodeImpl doc comment), and finally writes the new version into the
// state machine. Must be called with sm.mu held.
func (sm *stateMachine) applyCreateVersion(ctx context.Context, cmd command, w wal.WAL, logger *zap.Logger) applyResult {
	if _, ok := sm.kbs[cmd.KBID]; !ok {
		return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}
	}

	// Idempotent retry: a request that already allocated a version returns
	// that same version instead of allocating another one. Checked before the
	// parent validation, because the parent's state may legitimately have
	// moved on (e.g. it is READY now) since the first attempt.
	if cmd.ClientRequestID != "" {
		if id, ok := sm.versionsByRequest[requestKey(cmd.KBID, cmd.ClientRequestID)]; ok {
			return applyResult{VersionID: id}
		}
	}

	if cmd.ParentVersionID != 0 {
		parent, ok := sm.versions[cmd.ParentVersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrInvalidParentVersion}
		}
		if parent.KBID != cmd.KBID {
			return applyResult{Err: fmt.Errorf("parent version %d belongs to a different knowledge base: %w", cmd.ParentVersionID, stratumerrors.ErrInvalidParentVersion)}
		}
		if parent.IndexStatus == types.IndexStatusPending {
			return applyResult{Err: fmt.Errorf("parent version %d is PENDING: %w", cmd.ParentVersionID, stratumerrors.ErrInvalidParentVersion)}
		}
		if parent.Deleting {
			return applyResult{Err: fmt.Errorf("parent version %d is being deleted: %w", cmd.ParentVersionID, stratumerrors.ErrInvalidParentVersion)}
		}
		// The version chain is strictly linear: a parent may have at most
		// one child. docstore.ReadAt resolves a document by numeric
		// "version <= maxVersionID", which is only equivalent to walking
		// the ancestor chain when there are no forks — with two children, a
		// query on one branch can read the sibling branch's write. Checked
		// here (in apply, not at propose time) so every replica reaches the
		// same verdict deterministically.
		for _, id := range sm.versionsByKB[cmd.KBID] {
			if v, ok := sm.versions[id]; ok && v.ParentVersionID == cmd.ParentVersionID {
				return applyResult{Err: fmt.Errorf("parent version %d already has child version %d: %w", cmd.ParentVersionID, id, stratumerrors.ErrInvalidParentVersion)}
			}
		}
	}

	versionID := sm.nextVersionID
	sm.nextVersionID++

	// Critical ordering: WAL.WriteVersionID before the version is written
	// into the state machine. This runs identically on every node that
	// applies this entry — not just the leader — because any node could
	// become leader after a restart and would then need its OWN WAL to
	// correctly detect and resume an incomplete write (see
	// Stratum_设计文档v10.md "关键时序约束").
	//
	// A WAL write failure here is treated as a local durability problem,
	// not a reason to diverge this node's replicated state from its
	// peers: the state machine update proceeds regardless, with the
	// failure logged loudly for operator attention. Aborting the state
	// machine update on a local WAL failure would make this node's
	// applied state disagree with every other node that succeeded,
	// breaking Raft's core replication invariant.
	if err := w.WriteVersionID(ctx, cmd.KBID, versionID); err != nil {
		logger.Error("WAL.WriteVersionID failed during apply; continuing to avoid replicated-state divergence, but this node's crash recovery for this version is now at risk and needs operator attention",
			zap.Int64("version_id", versionID), zap.String("kb_id", cmd.KBID), zap.Error(err))
	}

	meta := types.VersionMeta{
		VersionID:       versionID,
		ParentVersionID: cmd.ParentVersionID,
		KBID:            cmd.KBID,
		CreatedAt:       time.Now().Unix(),
		IndexStatus:     types.IndexStatusPending,
	}
	sm.versions[versionID] = meta
	sm.versionsByKB[cmd.KBID] = append(sm.versionsByKB[cmd.KBID], versionID)
	if cmd.ClientRequestID != "" {
		sm.versionsByRequest[requestKey(cmd.KBID, cmd.ClientRequestID)] = versionID
	}

	return applyResult{VersionID: versionID}
}

// snapshotState is the gob-serializable form of the full state machine,
// used by serialize/restore for Raft log compaction.
type snapshotState struct {
	KBs               map[string]types.KnowledgeBaseMeta
	Versions          map[int64]types.VersionMeta
	VersionsByKB      map[string][]int64
	NextVersionID     int64
	VersionsByRequest map[string]int64
}

// deepCopy returns a stable copy of the current state machine under RLock.
// It is the fast, lock-bounded step of the async snapshot path: the copy
// is cheap (KB/version metadata only), so apply is blocked for a moment;
// the slow gob encoding and disk write happen later on the copy without
// holding any lock, so a slow snapshot can never stall the apply loop.
func (sm *stateMachine) deepCopy() snapshotState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	kbs := make(map[string]types.KnowledgeBaseMeta, len(sm.kbs))
	for k, v := range sm.kbs {
		kbs[k] = v
	}
	versions := make(map[int64]types.VersionMeta, len(sm.versions))
	for k, v := range sm.versions {
		versions[k] = v
	}
	versionsByKB := make(map[string][]int64, len(sm.versionsByKB))
	for k, v := range sm.versionsByKB {
		versionsByKB[k] = append([]int64(nil), v...)
	}
	versionsByRequest := make(map[string]int64, len(sm.versionsByRequest))
	for k, v := range sm.versionsByRequest {
		versionsByRequest[k] = v
	}
	return snapshotState{
		KBs:               kbs,
		Versions:          versions,
		VersionsByKB:      versionsByKB,
		NextVersionID:     sm.nextVersionID,
		VersionsByRequest: versionsByRequest,
	}
}

// serialize encodes the full current state machine for a Raft snapshot.
func (sm *stateMachine) serialize() ([]byte, error) {
	return encodeSnapshot(sm.deepCopy())
}

// encodeSnapshot gob-encodes a (copied) snapshotState without touching the
// live state machine, so it can run off the state-machine lock.
func encodeSnapshot(snap snapshotState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return nil, fmt.Errorf("raft: serialize state machine snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// restore replaces the state machine's contents with a previously
// serialized snapshot.
func (sm *stateMachine) restore(data []byte) error {
	var snap snapshotState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return fmt.Errorf("raft: restore state machine snapshot: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.kbs = snap.KBs
	sm.versions = snap.Versions
	sm.versionsByKB = snap.VersionsByKB
	sm.nextVersionID = snap.NextVersionID
	sm.versionsByRequest = snap.VersionsByRequest
	if sm.kbs == nil {
		sm.kbs = make(map[string]types.KnowledgeBaseMeta)
	}
	if sm.versions == nil {
		sm.versions = make(map[int64]types.VersionMeta)
	}
	if sm.versionsByKB == nil {
		sm.versionsByKB = make(map[string][]int64)
	}
	// A snapshot written before the idempotency map existed decodes to nil.
	if sm.versionsByRequest == nil {
		sm.versionsByRequest = make(map[string]int64)
	}
	return nil
}

// listAllVersions returns a snapshot of every version that still needs
// storage-layer data sync: PENDING versions (their writes have not
// completed and no digest is committed yet) and Deleting versions (their
// data is being cleaned up) are excluded.
//
// Used after installing a leader-pushed snapshot: the snapshot covers log
// entries that are never applied individually on this node, so the normal
// per-version onVersionCreated hook would never fire for them — the caller
// triggers it explicitly for every version returned here.
func (sm *stateMachine) listAllVersions() []types.VersionMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]types.VersionMeta, 0, len(sm.versions))
	for _, v := range sm.versions {
		if v.IndexStatus == types.IndexStatusPending || v.Deleting {
			continue
		}
		out = append(out, v)
	}
	return out
}

// withIndexReadyNode returns nodes with id present, kept sorted and free of
// duplicates.
//
// The ordering is not decorative. Determinism is what makes the field safe to
// put in a state machine: two nodes applying the same sequence of reports must
// end up with byte-identical state, or a snapshot comparison between them would
// differ for no reason. Sorted insertion also makes a repeat report (the same
// replica reporting twice, which happens on retries and after a restart) a
// no-op — the report is a set membership fact, not a counter.
func withIndexReadyNode(nodes []int64, id int64) []int64 {
	i := sort.Search(len(nodes), func(i int) bool { return nodes[i] >= id })
	if i < len(nodes) && nodes[i] == id {
		return nodes
	}
	nodes = append(nodes, 0)
	copy(nodes[i+1:], nodes[i:])
	nodes[i] = id
	return nodes
}
