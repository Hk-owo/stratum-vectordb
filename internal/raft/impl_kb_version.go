package raft

import (
	"context"
	"sort"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

func (impl *RaftNodeImpl) ProposeCreateKB(ctx context.Context, kb types.KnowledgeBaseMeta) error {
	res, err := impl.proposeAndWait(ctx, newCreateKBCommand(kb))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeMarkKBDeleting(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, newMarkKBDeletingCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, newMarkKBDeleteFailedCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeRemoveKBMeta(ctx context.Context, kbID string) error {
	res, err := impl.proposeAndWait(ctx, newRemoveKBMetaCommand(kbID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64, opts ...ProposeOption) (int64, error) {
	o := resolveProposeOptions(opts)
	res, err := impl.proposeAndWait(ctx, newCreateVersionCommand(kbID, parentVersionID, o.clientRequestID))
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		return 0, res.Err
	}
	return res.VersionID, nil
}

// ProposeMarkVersionFailedPermanent implements RaftNode: the terminal verdict
// for one side of a version whose Saga has spent its retry budget
// (docs/参考/历史/Stratum_设计文档v13.md §10.1, §10.1b).
func (impl *RaftNodeImpl) ProposeMarkVersionFailedPermanent(ctx context.Context, kbID string, versionID int64, side types.FailureSide, reason string, count int32) error {
	res, err := impl.proposeAndWait(ctx, newMarkVersionFailedPermanentCommand(kbID, versionID, side, reason, count))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeRetryVersion implements RaftNode: an operator's revocation of one side's
// terminal verdict (docs/参考/历史/Stratum_设计文档v13.md §10.1). The narrow rules (index side only,
// terminal side only, cause chain dropped only once nothing is terminal any more)
// live in the state machine's apply, where every replica reaches the same verdict,
// rather than here: a proposer that decided them would let two callers disagree.
func (impl *RaftNodeImpl) ProposeRetryVersion(ctx context.Context, kbID string, versionID int64, side types.FailureSide) error {
	res, err := impl.proposeAndWait(ctx, newRetryVersionCommand(kbID, versionID, side))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status types.IndexStatus, nodeID int64) error {
	res, err := impl.proposeAndWait(ctx, newUpdateVersionStatusCommand(versionID, status, nodeID))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeUpdateVersionSummary implements RaftNode.
func (impl *RaftNodeImpl) ProposeUpdateVersionSummary(ctx context.Context, versionID int64, docIDSetHash string) error {
	res, err := impl.proposeAndWait(ctx, newUpdateVersionSummaryCommand(versionID, docIDSetHash))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeMarkVersionDataDurable implements RaftNode: the control layer's own
// promotion of a version's data side (§10.1b).
func (impl *RaftNodeImpl) ProposeMarkVersionDataDurable(ctx context.Context, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, newMarkDataDurableCommand(versionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error {
	res, err := impl.proposeAndWait(ctx, newRollbackCommand(kbID, targetVersionID))
	if err != nil {
		return err
	}
	return res.Err
}

// ProposeMarkVersionDeleting implements RaftNode.
func (impl *RaftNodeImpl) ProposeMarkVersionDeleting(ctx context.Context, kbID string, versionID int64, mode types.VersionDeleteMode) ([]int64, error) {
	res, err := impl.proposeAndWait(ctx, newMarkVersionDeletingCommand(kbID, versionID, mode))
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return nil, res.Err
	}
	return res.DeletedVersionIDs, nil
}

// ProposeRemoveVersionMeta implements RaftNode.
func (impl *RaftNodeImpl) ProposeRemoveVersionMeta(ctx context.Context, kbID string, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, newRemoveVersionMetaCommand(kbID, versionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) ProposeDiscardVersion(ctx context.Context, kbID string, versionID int64) error {
	res, err := impl.proposeAndWait(ctx, newDiscardVersionCommand(kbID, versionID))
	if err != nil {
		return err
	}
	return res.Err
}

func (impl *RaftNodeImpl) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	kb, ok := impl.sm.kbs[kbID]
	if !ok {
		return types.KnowledgeBaseMeta{}, stratumerrors.ErrKnowledgeBaseNotFound
	}
	return kb, nil
}

// ListVersionsInRange implements RaftNode: the same set, narrowed.
//
// A linear walk with two comparisons per version, and that is deliberate — the cost
// worth saving here is the WIRE (89 B per version), not this. The version set is a
// slice in allocation order that deletions splice, so a binary search would be buying
// microseconds with a new invariant to keep.
func (impl *RaftNodeImpl) ListVersionsInRange(ctx context.Context, kbID string, fromExclusive, toInclusive *int64) ([]types.VersionMeta, error) {
	all, err := impl.ListVersions(ctx, kbID)
	if err != nil {
		return nil, err
	}
	out := make([]types.VersionMeta, 0, len(all))
	for _, v := range all {
		if fromExclusive != nil && v.VersionID <= *fromExclusive {
			continue
		}
		if toInclusive != nil && v.VersionID > *toInclusive {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

func (impl *RaftNodeImpl) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return nil, stratumerrors.ErrKnowledgeBaseNotFound
	}
	ids := impl.sm.versionsByKB[kbID]
	out := make([]types.VersionMeta, 0, len(ids))
	for _, id := range ids {
		out = append(out, impl.sm.versions[id])
	}
	return out, nil
}

// GetVersion returns one version's metadata, read straight out of the state
// machine under the same RLock ListVersions takes. It is the O(1) sibling the
// await path polls (docs/已实施/await-version-plan.md §7 Step 2).
func (impl *RaftNodeImpl) GetVersion(_ context.Context, kbID string, versionID int64) (types.VersionMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return types.VersionMeta{}, stratumerrors.ErrKnowledgeBaseNotFound
	}
	v, ok := impl.sm.versions[versionID]
	if !ok || v.KBID != kbID {
		// One answer for "no such version" and for "that version belongs to a
		// different knowledge base": the caller asked about (kbID, versionID)
		// and from its side both mean "not in this KB".
		return types.VersionMeta{}, stratumerrors.ErrVersionNotFound
	}
	return v, nil
}

// LastVersionID implements RaftNode: O(1), because the version set is kept in
// allocation order (see the interface comment).
func (impl *RaftNodeImpl) LastVersionID(_ context.Context, kbID string) (int64, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return 0, stratumerrors.ErrKnowledgeBaseNotFound
	}
	ids := impl.sm.versionsByKB[kbID]
	if len(ids) == 0 {
		return 0, nil
	}
	// Append-ordered, and deletions splice the slice in place, so the last element
	// IS the highest surviving id — no scan, and no second invariant to keep in
	// step with anything else.
	return ids[len(ids)-1], nil
}

// ChainGeneration names the current state of the chain tails: it changes whenever one
// may have, and a new leader's term keeps its count apart from its predecessor's.
//
// Not on the RaftNode interface: only a leader's answer means anything, and every other
// shape would have to invent one.
func (impl *RaftNodeImpl) ChainGeneration() (term, seq uint64) {
	// Read apart: the apply path takes the state-machine lock while Raft is working, so
	// holding it across a call into Raft is an ordering nobody needs to reason about.
	term = impl.raft.Term()
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	return term, impl.sm.chainSeq
}

// VersionLiveness answers "which of these versions are still alive, and how far has
// allocation got" for one knowledge base.
//
// Not on the RaftNode interface: its consumers are the readers that judge leftovers,
// and widening the interface would oblige
// every shape (including a storage node that has no state machine) to answer a question
// only a state machine can.
//
// Both facts come from ONE lock hold, because they must agree: "id <= lastAllocated" is
// what proves this view has already applied that id's allocation, so only then does "and
// it is not alive" mean removed rather than "not seen yet".
func (impl *RaftNodeImpl) VersionLiveness(_ context.Context, kbID string, fromExclusive, toInclusive *int64) ([]int64, int64, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return nil, 0, stratumerrors.ErrKnowledgeBaseNotFound
	}
	// versionsByKB is in allocation order, so the range can be walked once and cut short.
	ids := impl.sm.versionsByKB[kbID]
	alive := make([]int64, 0, len(ids))
	for _, id := range ids {
		if fromExclusive != nil && id <= *fromExclusive {
			continue
		}
		if toInclusive != nil && id > *toInclusive {
			break
		}
		alive = append(alive, id)
	}
	return alive, impl.sm.nextVersionID - 1, nil
}

// DeletionsSince answers a consumer's incremental read of the bounded deletion log:
// every entry after afterSeq, up to limit of them, plus the three facts the caller
// needs to interpret what it got.
//
// latest is the log's current sequence number, oldest the sequence of its oldest
// retained entry (0 when the log is empty). A caller whose afterSeq is older than
// oldest cannot be served incrementally — entries it has not seen are already gone —
// and that is what truncated means at the consumer's end. The three are returned
// together, from one lock hold, because they must agree: reading "latest" and "the
// entries" separately could hand back a sequence number newer than the entries it
// describes, and a consumer would then advance its watermark past entries it never
// received.
//
// epoch identifies the state machine that minted these sequence numbers. A rebuilt
// cluster mints a new one, and a consumer that sees a different epoch must treat its
// watermark as void rather than as "I am ahead" — the two look identical on the
// sequence alone.
//
// Not on the RaftNode interface, for the same reason VersionLiveness is not: its
// consumer is the storage layer's reconciliation, and a shape without a state
// machine answers it through RemoteRaftNode.
func (impl *RaftNodeImpl) DeletionsSince(_ context.Context, afterSeq int64, limit int) ([]types.DeletedVersion, int64, int64, string, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()

	latest := impl.sm.deletedSeq
	epoch := impl.sm.deletedEpoch
	var oldest int64
	if len(impl.sm.deletedLog) > 0 {
		oldest = impl.sm.deletedLog[0].Seq
	}

	if afterSeq >= latest || len(impl.sm.deletedLog) == 0 {
		return nil, latest, oldest, epoch, nil
	}
	out := make([]types.DeletedVersion, 0, limit)
	for _, entry := range impl.sm.deletedLog {
		if entry.Seq <= afterSeq {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		out = append(out, entry)
	}
	return out, latest, oldest, epoch, nil
}

// DeletingVersionIDs answers "which of this knowledge base's versions are still marked
// Deleting", returning ids only.
//
// It exists because that is all its consumers want: the sweep runs every minute and only
// needs to know whether there is anything to resume, and DeleteVersionCoordinator.Execute
// re-scans a knowledge base the same way. ListVersions answers the same question at the
// cost of a VersionMeta value copy per version (~112 bytes) — measured at 42 µs per 1000
// versions on this machine, against 17 µs for this shape. The O(n) that remains is one
// map lookup per version, which no narrower return type can remove.
//
// Not on the RaftNode interface, for the reason VersionLiveness is not: its consumers are
// the sweeps, and widening the interface would oblige every shape — including a storage
// node, which has no state machine to scan — to answer it.
func (impl *RaftNodeImpl) DeletingVersionIDs(_ context.Context, kbID string) ([]int64, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	if _, ok := impl.sm.kbs[kbID]; !ok {
		return nil, stratumerrors.ErrKnowledgeBaseNotFound
	}
	var out []int64
	for _, id := range impl.sm.versionsByKB[kbID] {
		if v, ok := impl.sm.versions[id]; ok && v.Deleting {
			out = append(out, id)
		}
	}
	return out, nil
}

// DeletingKBIDs answers the knowledge-base half of the delete sweep: which
// knowledge bases are still marked Deleting. Straight out of the in-memory state
// machine and ids only — the same shape as DeletingVersionIDs, for the same
// reason, and what lets the sweep run without walking the metadata of every KB a
// node happens to hold.
//
// It exists because a KB deletion has the same crash window a version deletion
// has, and no equivalent repair until now (M3 of
// docs/参考/code-review-2026-10-18.md): DeleteKnowledgeBase proposes the Deleting
// status first (durable, replicated, the moment the API answers the caller) and
// only then starts the cleanup goroutine, which is what writes the WAL's delete
// mark. A process that dies in between leaves the KB in DELETING with neither a
// WAL record to resume from nor anything else that would notice.
func (impl *RaftNodeImpl) DeletingKBIDs(_ context.Context) ([]string, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	var out []string
	for kbID, kb := range impl.sm.kbs {
		if kb.Status == types.KBStatusDeleting {
			out = append(out, kbID)
		}
	}
	// Sorted so the sweep's order — and any log line or test asserting on it — does
	// not depend on Go's map iteration order.
	sort.Strings(out)
	return out, nil
}

// MarkDeletingDirty records that kbID has had a version marked Deleting. Called from the
// apply path, so it does no I/O and holds its own lock for the length of one map write.
// Exported because the sweep's DirtyDeletingTracker puts a knowledge base back with it
// when a pass could not finish.
func (impl *RaftNodeImpl) MarkDeletingDirty(kbID string) {
	if kbID == "" {
		return
	}
	impl.deletingDirtyMu.Lock()
	defer impl.deletingDirtyMu.Unlock()
	if impl.deletingDirty == nil {
		impl.deletingDirty = make(map[string]bool)
	}
	impl.deletingDirty[kbID] = true
}

// MarkAllDeletingDirty treats every knowledge base in the state machine as dirty.
//
// Two moments need it, and both are moments when this node may have MISSED a deletion
// mark rather than received one:
//
//   - start-up: the set is memory, so a restart begins with nothing dirty;
//   - a snapshot install: the entries a node never applied one by one are exactly the
//     deletions it was never told about.
//
// Both are the conservative direction — the next sweep asks about every knowledge base,
// which is what the sweep did before this set existed.
func (impl *RaftNodeImpl) MarkAllDeletingDirty() {
	impl.sm.mu.RLock()
	kbs := make([]string, 0, len(impl.sm.kbs))
	for kbID := range impl.sm.kbs {
		kbs = append(kbs, kbID)
	}
	impl.sm.mu.RUnlock()

	impl.deletingDirtyMu.Lock()
	defer impl.deletingDirtyMu.Unlock()
	if impl.deletingDirty == nil {
		impl.deletingDirty = make(map[string]bool)
	}
	for _, kbID := range kbs {
		impl.deletingDirty[kbID] = true
	}
}

// TakeDirtyDeletingKBs consumes the set: the caller gets the knowledge bases that have
// changed since the last call, and the set is cleared in the same hold, so two sweeps
// cannot both act on one change (harmless, since the action is idempotent, but it would
// double the work).
func (impl *RaftNodeImpl) TakeDirtyDeletingKBs() []string {
	impl.deletingDirtyMu.Lock()
	defer impl.deletingDirtyMu.Unlock()
	if len(impl.deletingDirty) == 0 {
		return nil
	}
	out := make([]string, 0, len(impl.deletingDirty))
	for kbID := range impl.deletingDirty {
		out = append(out, kbID)
	}
	impl.deletingDirty = make(map[string]bool)
	return out
}

// ListKnowledgeBases returns metadata for every knowledge base in the
// state machine. Order is not specified (map iteration).
func (impl *RaftNodeImpl) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	impl.sm.mu.RLock()
	defer impl.sm.mu.RUnlock()
	out := make([]types.KnowledgeBaseMeta, 0, len(impl.sm.kbs))
	for _, kb := range impl.sm.kbs {
		out = append(out, kb)
	}
	// map 遍历顺序随机，按 KBID 字典序排序，保证列表顺序稳定可预期。
	sort.Slice(out, func(i, j int) bool { return out[i].KBID < out[j].KBID })
	return out, nil
}
