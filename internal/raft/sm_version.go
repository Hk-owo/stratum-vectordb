package raft

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// applyVersion applies version-chain lifecycle, status, durability, rollback,
// and deletion commands. The caller holds sm.mu.
func (sm *stateMachine) applyVersion(ctx context.Context, cmd command, w wal.WAL, logger *zap.Logger) (applyResult, bool) {
	switch cmd.Type {
	case cmdCreateVersion:
		return sm.applyCreateVersion(ctx, cmd, w, logger), true

	case cmdUpdateVersionStatus:
		if v, ok := sm.versions[cmd.VersionID]; ok && cmd.NodeID > 0 {
			if p, exists := sm.directory.Placements[v.KBID]; exists && !p.Contains(cmd.NodeID) {
				return applyResult{Err: stratumerrors.ErrNotKBReplica}, true
			}
		}
		if _, blocked := sm.directory.ServiceBlocks[cmd.NodeID]; cmd.NodeID > 0 && blocked {
			return applyResult{Err: stratumerrors.ErrStorageServicePaused}, true
		}
		v, ok := sm.versions[cmd.VersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}, true
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
		return applyResult{}, true

	case cmdUpdateVersionSummary:
		v, ok := sm.versions[cmd.VersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}, true
		}
		// A late confirmation must not resurrect a settled version
		// (docs/参考/历史/Stratum_设计文档v13.md §10.6): the client may already have given up
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
			return applyResult{}, true
		}
		if migration, active := sm.directory.ActiveMigration(v.KBID); active && migration.BlocksDataWrites() && v.DocIDSetHash != cmd.DocIDSetHash {
			return applyResult{Err: stratumerrors.ErrKBWritePaused}, true
		}
		v.DocIDSetHash = cmd.DocIDSetHash
		// The writer commits this digest only after its own storage writes
		// finished and a quorum confirmed them (ControlPlane.ReportDataDurable),
		// so its arrival is exactly what makes the DATA side durable
		// (docs/参考/历史/Stratum_设计文档v13.md §10.1b).
		v.DataStatus = types.DataStatusDurable
		sm.versions[cmd.VersionID] = v
		return applyResult{}, true

	case cmdMarkDataDurable:
		return sm.applyMarkDataDurable(cmd), true

	case cmdMarkVersionFailedPermanent:
		return sm.applyMarkVersionFailedPermanent(cmd), true

	case cmdRollback:
		kb, ok := sm.kbs[cmd.KBID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrKnowledgeBaseNotFound}, true
		}
		v, ok := sm.versions[cmd.TargetVersionID]
		if !ok || v.KBID != cmd.KBID {
			return applyResult{Err: stratumerrors.ErrVersionNotFound}, true
		}
		if v.Deleting {
			return applyResult{Err: fmt.Errorf("target version %d is being deleted: %w", cmd.TargetVersionID, stratumerrors.ErrVersionDeleting)}, true
		}
		kb.ActiveVersionID = cmd.TargetVersionID
		sm.kbs[cmd.KBID] = kb
		return applyResult{}, true

	case cmdRetryVersion:
		return sm.applyRetryVersion(cmd), true

	case cmdMarkVersionDeleting:
		return sm.applyMarkVersionDeleting(cmd), true

	case cmdRemoveVersionMeta:
		return sm.applyRemoveVersionMeta(cmd), true

	case cmdDiscardVersion:
		return sm.applyDiscardVersion(cmd), true

	default:
		return applyResult{}, false
	}
}

// applyMarkDataDurable records the control layer's own promotion of a version's
// DATA side to durable (docs/参考/历史/Stratum_设计文档v13.md §10.1b).
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
// automatically (docs/参考/历史/Stratum_设计文档v13.md §10.1, §10.1b). The storage layer only
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
		// A version whose DATA side already reached DURABLE cannot be declared
		// data-terminal. The two claims contradict, and the durable one wins: the
		// data is on a quorum, so this report describes a transient the system has
		// since resolved (a push that timed out while a lagging replica was already
		// catching up). Overwriting would mark a healthy version terminal, and every
		// replica's apply would then reclaim data a CHILD version already depends on
		// — data loss no later write can repair (docs/未实施/known-gaps.md §K).
		//
		// Returning an error (rather than silently ignoring) is what keeps the
		// reporter honest: ReportVersionFailure maps it to "already settled" and the
		// writer stops. It also keeps the apply-level success hooks from firing, so
		// no replica starts reclaiming.
		if v.DataStatus == types.DataStatusDurable {
			return applyResult{Err: fmt.Errorf(
				"version %d is already durable on the data side; refusing the terminal verdict: %w",
				cmd.VersionID, stratumerrors.ErrVersionDataSettled)}
		}
		v.DataStatus = types.DataStatusFailedPermanent
	}
	v.FailureReason = cmd.FailureReason
	v.FailureCount = cmd.FailureCount
	// Recorded, not inferred: a version can end up terminal on BOTH sides, and
	// then only this field says which one the cause chain above describes
	// (docs/参考/历史/Stratum_设计文档v13.md §10.1b).
	v.FailureSide = cmd.FailureSide
	sm.versions[cmd.VersionID] = v
	return applyResult{}
}

// applyRetryVersion handles cmdRetryVersion: an operator revoking the control
// layer's terminal verdict for ONE side of a version (docs/参考/历史/Stratum_设计文档v13.md §10.1).
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
// (docs/参考/历史/Stratum_设计文档v13.md §10.1b). Now that the halves are separate, the proxy has to be
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
// the version's VersionDocList would hurt a survivor.
//
// This is the ONE admission rule that still reads the index side. applyCreateVersion's
// parent check used to as well, and no longer does: a new version derives its document
// set from the parent's DATA, and the index is a read-side product built on demand
// wherever queries land (§8.6(b)) — so "the parent's index is not built yet" was never
// a real reason to refuse a write, only a reason to wait for something nothing was
// going to do. The two rules reading different sides is the point, not an
// inconsistency.
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
			sm.chainSeq++
			break
		}
	}
	sm.dropRequestMappings(cmd.KBID, cmd.VersionID)
	// The one moment a version stops existing: recorded so a storage node can learn
	// about it incrementally (see DeletionsSince).
	sm.recordDeletion(cmd.KBID, cmd.VersionID)
	return applyResult{}
}

// applyDiscardVersion handles cmdDiscardVersion: the CALLER's declaration that
// it is abandoning a version whose write never landed
// (docs/已实施/await-version-plan.md §7 Step 6).
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
			sm.chainSeq++
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
	if _, active := sm.directory.ActiveMigration(cmd.KBID); active {
		return applyResult{Err: stratumerrors.ErrMigrationInProgress}
	}

	if cmd.ParentVersionID != 0 {
		parent, ok := sm.versions[cmd.ParentVersionID]
		if !ok {
			return applyResult{Err: stratumerrors.ErrInvalidParentVersion}
		}
		if parent.KBID != cmd.KBID {
			return applyResult{Err: fmt.Errorf("parent version %d belongs to a different knowledge base: %w", cmd.ParentVersionID, stratumerrors.ErrInvalidParentVersion)}
		}
		if parent.DataStatus != types.DataStatusDurable {
			// 门禁看**数据侧**：数据落盘（VersionDocList 已写）就足以派生下一版的
			// 文档集，这也是唯一真实的依赖。
			//
			// 索引不参与，理由不是"省事"：它是读侧产物，构建与放置由查询频率动态
			// 决定（owner 集 + 按需构建，§8.4/§8.6(b)）。写路径既不知道它什么时候
			// 建、也不该知道——以前这里看 IndexStatus，等于要求"父版本的索引已建好"
			// 才能写下一版；而索引没人查就不建，于是数据早已落盘的库照样被拒在
			// invalid_parent_version 上，链就卡在那里。
			//
			// 两个非 durable 的原因必须分开：PENDING 是"还没到"（可重试），终态是
			// "永远不到"（重试无用）。混成一个是这条链在 20,000 档反复断掉的原因
			// （docs/未实施/known-gaps.md §K）——追赶副本建好父版本的索引在先、写方 digest
			// 绕 Raft 回来在后，调用方等的是索引 READY，于是必然撞上这个窗口。
			if parent.DataStatus == types.DataStatusPending {
				return applyResult{Err: fmt.Errorf("parent version %d has no durable data (data_status=%s): %w",
					cmd.ParentVersionID, parent.DataStatus.String(), stratumerrors.ErrParentNotDurable)}
			}
			return applyResult{Err: fmt.Errorf("parent version %d has no durable data (data_status=%s): %w",
				cmd.ParentVersionID, parent.DataStatus.String(), stratumerrors.ErrInvalidParentVersion)}
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
	// docs/参考/历史/Stratum_设计文档v10.md "关键时序约束").
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
	sm.chainSeq++
	if cmd.ClientRequestID != "" {
		sm.versionsByRequest[requestKey(cmd.KBID, cmd.ClientRequestID)] = versionID
	}

	return applyResult{VersionID: versionID}
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
