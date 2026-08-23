package raft

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// MockRaftNode is a single-process, in-memory RaftNode for use in unit
// tests of modules that depend on RaftNode (WriteCoordinator,
// DeleteCoordinator, QueryService, etc.). It implements real single-node
// semantics — version ID monotonicity, parent-version validation, and the
// WAL.WriteVersionID-before-state-machine-write ordering — rather than
// being a thin recorder, because those semantics are exactly what
// downstream modules need to exercise in isolation before a real
// multi-node Raft implementation exists (Phase 3).
//
// It is not a substitute for the real kvserver-Raft-backed implementation's
// own tests, which additionally cover multi-node consensus, leader
// election, and network partition behavior (see T3-* / T4-* in
// Stratum_测试顺序.md).
type MockRaftNode struct {
	mu sync.Mutex

	wal wal.WAL // used by ProposeCreateVersion to enforce the WAL-before-state-machine ordering

	kbs           map[string]types.KnowledgeBaseMeta
	versions      map[int64]types.VersionMeta
	versionsByKB  map[string][]int64
	nextVersionID int64
}

// NewMockRaftNode constructs an empty MockRaftNode. w is the WAL instance
// ProposeCreateVersion uses to enforce the documented apply-phase ordering
// (WriteVersionID before allocating the version into the state machine);
// pass a *wal.MockWAL in tests, consistent with how the real
// RaftNodeImpl would be wired to the real WAL implementation.
func NewMockRaftNode(w wal.WAL) *MockRaftNode {
	return &MockRaftNode{
		wal:           w,
		kbs:           make(map[string]types.KnowledgeBaseMeta),
		versions:      make(map[int64]types.VersionMeta),
		versionsByKB:  make(map[string][]int64),
		nextVersionID: 1,
	}
}

func (r *MockRaftNode) ProposeCreateKB(_ context.Context, kb types.KnowledgeBaseMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if kb.Status == 0 {
		kb.Status = types.KBStatusActive
	}
	r.kbs[kb.KBID] = kb
	return nil
}

func (r *MockRaftNode) ProposeMarkKBDeleting(_ context.Context, kbID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	kb, ok := r.kbs[kbID]
	if !ok {
		return stratumerrors.ErrKnowledgeBaseNotFound
	}
	kb.Status = types.KBStatusDeleting
	r.kbs[kbID] = kb
	return nil
}

func (r *MockRaftNode) ProposeMarkKBDeleteFailed(_ context.Context, kbID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	kb, ok := r.kbs[kbID]
	if !ok {
		return stratumerrors.ErrKnowledgeBaseNotFound
	}
	kb.Status = types.KBStatusDeleteFailed
	r.kbs[kbID] = kb
	return nil
}

// ProposeRemoveKBMeta removes kbID's metadata. Idempotent: if the metadata
// is already gone, returns nil rather than ErrKnowledgeBaseNotFound, per
// the documented contract that lets the delete flow's crash-recovery path
// safely re-execute this step any number of times.
func (r *MockRaftNode) ProposeRemoveKBMeta(_ context.Context, kbID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.kbs, kbID)
	for _, vID := range r.versionsByKB[kbID] {
		delete(r.versions, vID)
	}
	delete(r.versionsByKB, kbID)
	return nil
}

// ProposeCreateVersion allocates a new version for kbID with the given
// parent, enforcing: (1) WAL.WriteVersionID is called before the version
// is written into the in-memory state machine, mirroring the real apply-
// phase ordering documented for RaftNodeImpl; (2) the parent version must
// belong to the same knowledge base; (3) the parent version must not be
// PENDING; (4) forking (multiple children of one parent) is allowed.
func (r *MockRaftNode) ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.kbs[kbID]; !ok {
		return 0, stratumerrors.ErrKnowledgeBaseNotFound
	}

	// parentVersionID == 0 is treated as "no parent" (initial version of a
	// freshly created knowledge base). Any other value must reference an
	// existing version satisfying the constraints below.
	if parentVersionID != 0 {
		parent, ok := r.versions[parentVersionID]
		if !ok {
			return 0, stratumerrors.ErrInvalidParentVersion
		}
		if parent.KBID != kbID {
			return 0, fmt.Errorf("parent version %d belongs to a different knowledge base: %w", parentVersionID, stratumerrors.ErrInvalidParentVersion)
		}
		if parent.IndexStatus == types.IndexStatusPending {
			return 0, fmt.Errorf("parent version %d is PENDING: %w", parentVersionID, stratumerrors.ErrInvalidParentVersion)
		}
	}

	versionID := r.nextVersionID

	// Critical ordering: WAL.WriteVersionID must succeed before the
	// version is allocated into the state machine. This is what
	// eliminates orphan versions on crash recovery — see the WAL package
	// doc comment and Stratum_设计文档v10.md "关键时序约束".
	if r.wal != nil {
		if err := r.wal.WriteVersionID(ctx, versionID); err != nil {
			return 0, fmt.Errorf("raft: WAL.WriteVersionID failed during apply: %w", err)
		}
	}

	r.nextVersionID++
	r.versions[versionID] = types.VersionMeta{
		VersionID:       versionID,
		ParentVersionID: parentVersionID,
		KBID:            kbID,
		CreatedAt:       time.Now().Unix(),
		IndexStatus:     types.IndexStatusPending,
	}
	r.versionsByKB[kbID] = append(r.versionsByKB[kbID], versionID)

	return versionID, nil
}

func (r *MockRaftNode) ProposeUpdateVersionStatus(_ context.Context, versionID int64, status types.IndexStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.versions[versionID]
	if !ok {
		return stratumerrors.ErrVersionNotFound
	}
	v.IndexStatus = status
	r.versions[versionID] = v
	return nil
}

// ProposeUpdateVersionSummary records the version's document-ID set hash.
func (r *MockRaftNode) ProposeUpdateVersionSummary(_ context.Context, versionID int64, docIDSetHash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.versions[versionID]
	if !ok {
		return stratumerrors.ErrVersionNotFound
	}
	v.DocIDSetHash = docIDSetHash
	r.versions[versionID] = v
	return nil
}

// ProposeRollback switches kbID's active version to targetVersionID.
// Callers are expected to have already validated targetVersionID is READY
// via GetKB/ListVersions, per the documented RollbackVersion logical flow
// (RaftNode.GetKB -> RaftNode.ProposeRollback); this method itself only
// enforces that the target version exists and belongs to kbID.
func (r *MockRaftNode) ProposeRollback(_ context.Context, kbID string, targetVersionID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	kb, ok := r.kbs[kbID]
	if !ok {
		return stratumerrors.ErrKnowledgeBaseNotFound
	}
	v, ok := r.versions[targetVersionID]
	if !ok || v.KBID != kbID {
		return stratumerrors.ErrVersionNotFound
	}
	if v.Deleting {
		return stratumerrors.ErrVersionDeleting
	}
	kb.ActiveVersionID = targetVersionID
	r.kbs[kbID] = kb
	return nil
}

// ProposeMarkVersionDeleting implements RaftNode, mirroring the real state
// machine's constraint checks: the version must exist and belong to kbID,
// and the whole recursive subtree (version + descendants) must not contain
// the active version or any PENDING version. Idempotent for an already
// Deleting subtree.
func (r *MockRaftNode) ProposeMarkVersionDeleting(_ context.Context, kbID string, versionID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.kbs[kbID]; !ok {
		return stratumerrors.ErrKnowledgeBaseNotFound
	}
	root, ok := r.versions[versionID]
	if !ok {
		return stratumerrors.ErrVersionNotFound
	}
	if root.KBID != kbID {
		return stratumerrors.ErrVersionNotFound
	}
	subtree := r.mockVersionSubtree(kbID, versionID)
	for _, id := range subtree {
		if r.kbs[kbID].ActiveVersionID == id {
			return stratumerrors.ErrVersionIsActive
		}
		if r.versions[id].IndexStatus == types.IndexStatusPending {
			return stratumerrors.ErrVersionPending
		}
	}
	for _, id := range subtree {
		v := r.versions[id]
		v.Deleting = true
		r.versions[id] = v
	}
	return nil
}

// ProposeRemoveVersionMeta implements RaftNode: removes a single version's
// metadata. Idempotent — an already-absent version succeeds, mirroring the
// real apply path so the delete flow's crash-recovery re-execution is safe.
func (r *MockRaftNode) ProposeRemoveVersionMeta(_ context.Context, kbID string, versionID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.versions[versionID]
	if !ok {
		return nil
	}
	if v.KBID != kbID {
		return stratumerrors.ErrVersionNotFound
	}
	delete(r.versions, versionID)
	list := r.versionsByKB[kbID]
	for i, id := range list {
		if id == versionID {
			r.versionsByKB[kbID] = append(list[:i], list[i+1:]...)
			break
		}
	}
	return nil
}

// mockVersionSubtree returns rootID plus every descendant of rootID within
// kbID (following ParentVersionID edges), mirroring the state machine's
// collectVersionSubtree.
func (r *MockRaftNode) mockVersionSubtree(kbID string, rootID int64) []int64 {
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
		for _, candidate := range r.versionsByKB[kbID] {
			if candidate == id {
				continue
			}
			child, ok := r.versions[candidate]
			if ok && child.ParentVersionID == id && !visited[candidate] {
				queue = append(queue, candidate)
			}
		}
	}
	return out
}

func (r *MockRaftNode) GetKB(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kb, ok := r.kbs[kbID]
	if !ok {
		return types.KnowledgeBaseMeta{}, stratumerrors.ErrKnowledgeBaseNotFound
	}
	return kb, nil
}

func (r *MockRaftNode) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.kbs[kbID]; !ok {
		return nil, stratumerrors.ErrKnowledgeBaseNotFound
	}
	ids := r.versionsByKB[kbID]
	out := make([]types.VersionMeta, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.versions[id])
	}
	return out, nil
}

// ListKnowledgeBases returns metadata for every knowledge base in the
// mock. Order is not specified (map iteration).
func (r *MockRaftNode) ListKnowledgeBases(_ context.Context) ([]types.KnowledgeBaseMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]types.KnowledgeBaseMeta, 0, len(r.kbs))
	for _, kb := range r.kbs {
		out = append(out, kb)
	}
	// 与 RaftNodeImpl 保持一致：按 KBID 字典序排序。
	sort.Slice(out, func(i, j int) bool { return out[i].KBID < out[j].KBID })
	return out, nil
}

// GetClusterStatus reports a trivial always-healthy single-node status,
// since MockRaftNode does not model multi-node consensus.
func (r *MockRaftNode) GetClusterStatus(_ context.Context) (types.ClusterStatus, error) {
	return types.ClusterStatus{HasLeader: true, MemberCount: 1, LeaderID: 1}, nil
}

// GetVersion is a test convenience helper (not part of the RaftNode
// interface) for tests that want to inspect a single version's metadata
// directly rather than scanning ListVersions.
func (r *MockRaftNode) GetVersion(versionID int64) (types.VersionMeta, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.versions[versionID]
	return v, ok
}

// Reset clears all stored state. Convenience for tests; not part of the
// RaftNode interface.
func (r *MockRaftNode) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kbs = make(map[string]types.KnowledgeBaseMeta)
	r.versions = make(map[int64]types.VersionMeta)
	r.versionsByKB = make(map[string][]int64)
	r.nextVersionID = 1
}

var _ RaftNode = (*MockRaftNode)(nil)
