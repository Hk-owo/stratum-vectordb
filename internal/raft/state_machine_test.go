package raft

import (
	"context"
	"errors"
	"sort"
	"testing"

	"go.uber.org/zap"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// newTestSM builds a state machine with a real WAL (to verify the
// WAL-before-state-machine ordering) and a no-op logger.
func newTestSM(t *testing.T) (*stateMachine, *wal.MockWAL) {
	t.Helper()
	w := wal.NewMockWAL()
	return newStateMachine(), w
}

func testKB(kbID string) types.KnowledgeBaseMeta {
	return types.KnowledgeBaseMeta{
		KBID:             kbID,
		Name:             kbID,
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig:      types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
		Status:           types.KBStatusActive,
	}
}

func TestStateMachine_Apply_CreateKB_DefaultsActive(t *testing.T) {
	sm, w := newTestSM(t)
	kb := testKB("kb-1")
	kb.Status = 0 // zero value must normalize to ACTIVE

	res := sm.apply(context.Background(), command{Type: cmdCreateKB, KB: &kb}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("apply(cmdCreateKB): %v", res.Err)
	}
	got, ok := sm.kbs["kb-1"]
	if !ok {
		t.Fatal("KB not stored")
	}
	if got.Status != types.KBStatusActive {
		t.Errorf("status = %v, want ACTIVE", got.Status)
	}
}

func TestStateMachine_Apply_MarkKBDeleting(t *testing.T) {
	sm, w := newTestSM(t)
	sm.apply(context.Background(), command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	res := sm.apply(context.Background(), command{Type: cmdMarkKBDeleting, KBID: "kb-1"}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkKBDeleting: %v", res.Err)
	}
	if sm.kbs["kb-1"].Status != types.KBStatusDeleting {
		t.Errorf("status = %v, want DELETING", sm.kbs["kb-1"].Status)
	}

	// Unknown KB → ErrKnowledgeBaseNotFound.
	res = sm.apply(context.Background(), command{Type: cmdMarkKBDeleting, KBID: "nope"}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("expected ErrKnowledgeBaseNotFound, got %v", res.Err)
	}
}

func TestStateMachine_Apply_MarkKBDeleteFailed(t *testing.T) {
	sm, w := newTestSM(t)
	sm.apply(context.Background(), command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	res := sm.apply(context.Background(), command{Type: cmdMarkKBDeleteFailed, KBID: "kb-1"}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkKBDeleteFailed: %v", res.Err)
	}
	if sm.kbs["kb-1"].Status != types.KBStatusDeleteFailed {
		t.Errorf("status = %v, want DELETE_FAILED", sm.kbs["kb-1"].Status)
	}

	res = sm.apply(context.Background(), command{Type: cmdMarkKBDeleteFailed, KBID: "nope"}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("expected ErrKnowledgeBaseNotFound, got %v", res.Err)
	}
}

func TestStateMachine_Apply_RemoveKBMeta(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: 0}, w, zap.NewNop())
	if v1.Err != nil {
		t.Fatalf("create v1: %v", v1.Err)
	}

	res := sm.apply(ctx, command{Type: cmdRemoveKBMeta, KBID: "kb-1"}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("RemoveKBMeta: %v", res.Err)
	}
	if _, ok := sm.kbs["kb-1"]; ok {
		t.Error("KB still present after RemoveKBMeta")
	}
	if _, ok := sm.versions[v1.VersionID]; ok {
		t.Error("version still present after RemoveKBMeta")
	}

	// Idempotent on an already-absent KB.
	res = sm.apply(ctx, command{Type: cmdRemoveKBMeta, KBID: "kb-1"}, w, zap.NewNop())
	if res.Err != nil {
		t.Errorf("second RemoveKBMeta must be idempotent, got %v", res.Err)
	}
}

func TestStateMachine_Apply_CreateVersion_Constraints(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()

	// KB must exist.
	res := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "nope"}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("expected ErrKnowledgeBaseNotFound, got %v", res.Err)
	}

	// Parent must exist.
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	res = sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: 99}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrInvalidParentVersion) {
		t.Errorf("expected ErrInvalidParentVersion for missing parent, got %v", res.Err)
	}

	// Parent must belong to the same KB.
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	other := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2"}, w, zap.NewNop())
	res = sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: other.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrInvalidParentVersion) {
		t.Errorf("expected ErrInvalidParentVersion for cross-KB parent, got %v", res.Err)
	}

	// Parent must not be PENDING.
	pending := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	res = sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: pending.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrInvalidParentVersion) {
		t.Errorf("expected ErrInvalidParentVersion for PENDING parent, got %v", res.Err)
	}
}

func TestStateMachine_Apply_CreateVersion_AllocatesAndWritesWAL(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// Mark parent READY so a child can be forked.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	// Fork two children from v1: distinct monotonic IDs.
	c1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	c2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	if c1.Err != nil || c2.Err != nil {
		t.Fatalf("fork: %v, %v", c1.Err, c2.Err)
	}
	if c2.VersionID != c1.VersionID+1 {
		t.Errorf("version IDs not monotonic: %d then %d", c1.VersionID, c2.VersionID)
	}

	// WAL-before-state-machine: every allocated version has a WAL
	// VERSION_ID record (recoverable as pending until committed).
	records, err := w.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, r := range records {
		if r.Type == types.PendingRecordTypeVersionWrite {
			seen[r.VersionID] = true
		}
	}
	for _, vid := range []int64{v1.VersionID, c1.VersionID, c2.VersionID} {
		if !seen[vid] {
			t.Errorf("WAL missing VERSION_ID for version %d", vid)
		}
	}
}

func TestStateMachine_Apply_UpdateVersionStatus(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v.VersionID, Status: types.IndexStatusFailed}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("UpdateVersionStatus: %v", res.Err)
	}
	if sm.versions[v.VersionID].IndexStatus != types.IndexStatusFailed {
		t.Errorf("status = %v, want FAILED", sm.versions[v.VersionID].IndexStatus)
	}

	res = sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: 999}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("expected ErrVersionNotFound, got %v", res.Err)
	}
}

func TestStateMachine_Apply_Rollback(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("Rollback: %v", res.Err)
	}
	if sm.kbs["kb-1"].ActiveVersionID != v.VersionID {
		t.Errorf("active = %d, want %d", sm.kbs["kb-1"].ActiveVersionID, v.VersionID)
	}

	// Unknown KB / unknown version.
	res = sm.apply(ctx, command{Type: cmdRollback, KBID: "nope", TargetVersionID: v.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("expected ErrKnowledgeBaseNotFound, got %v", res.Err)
	}
	// Version exists but belongs to a different KB.
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	res = sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-2", TargetVersionID: v.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("expected ErrVersionNotFound for cross-KB rollback, got %v", res.Err)
	}
}

func TestStateMachine_Apply_UnknownCommand(t *testing.T) {
	sm, w := newTestSM(t)
	res := sm.apply(context.Background(), command{Type: commandType("Bogus")}, w, zap.NewNop())
	if res.Err == nil {
		t.Fatal("expected error for unknown command type")
	}
}

func TestStateMachine_SerializeRestore_RoundTrip(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	data, err := sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("serialized snapshot is empty")
	}

	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if got := restored.kbs["kb-1"].Name; got != "kb-1" {
		t.Errorf("restored KB name = %q", got)
	}
	got := restored.versions[v.VersionID]
	if got.IndexStatus != types.IndexStatusReady || got.KBID != "kb-1" {
		t.Errorf("restored version = %+v", got)
	}
	if restored.nextVersionID != sm.nextVersionID {
		t.Errorf("nextVersionID = %d, want %d", restored.nextVersionID, sm.nextVersionID)
	}

	// The restored machine keeps working (new version continues the counter).
	after := restored.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if after.Err != nil || after.VersionID != sm.nextVersionID {
		t.Errorf("post-restore create = (v%d, %v), want (v%d, nil)", after.VersionID, after.Err, sm.nextVersionID)
	}
}

func TestStateMachine_Restore_CorruptData(t *testing.T) {
	sm := newStateMachine()
	if err := sm.restore([]byte("not a gob stream")); err == nil {
		t.Fatal("expected restore of corrupt data to error")
	}
}

func TestStateMachine_Restore_EmptyData(t *testing.T) {
	sm := newStateMachine()
	sm.kbs["kb-1"] = testKB("kb-1")
	if err := sm.restore([]byte{}); err == nil {
		// An empty snapshot is still valid gob input? It is not; gob
		// requires at least the stream header. Either way the maps must
		// remain non-nil afterwards (restore guards against nil maps).
		t.Log("restore of empty data did not error (implementation detail)")
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.kbs == nil || sm.versions == nil || sm.versionsByKB == nil {
		t.Fatal("maps must never be nil after restore")
	}
}

// TestCommandEncodeDecode_RoundTrip verifies the JSON wire format for the
// control-plane command stream survives a full round trip.
func TestCommandEncodeDecode_RoundTrip(t *testing.T) {
	kb := testKB("kb-1")
	cases := []command{
		{Type: cmdCreateKB, KB: &kb},
		{Type: cmdMarkKBDeleting, KBID: "kb-1"},
		{Type: cmdMarkKBDeleteFailed, KBID: "kb-1"},
		{Type: cmdRemoveKBMeta, KBID: "kb-1"},
		{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: 3},
		{Type: cmdUpdateVersionStatus, VersionID: 5, Status: types.IndexStatusFailed},
		{Type: cmdRollback, KBID: "kb-1", TargetVersionID: 2},
		{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: 4},
		{Type: cmdRemoveVersionMeta, KBID: "kb-1", VersionID: 4},
	}
	for _, c := range cases {
		data, err := encodeCommand(c)
		if err != nil {
			t.Fatalf("encode %+v: %v", c, err)
		}
		got, err := decodeCommand(data)
		if err != nil {
			t.Fatalf("decode %+v: %v", c, err)
		}
		if got.Type != c.Type || got.KBID != c.KBID || got.VersionID != c.VersionID ||
			got.ParentVersionID != c.ParentVersionID || got.TargetVersionID != c.TargetVersionID ||
			got.Status != c.Status {
			t.Errorf("round trip mismatch: got %+v want %+v", got, c)
		}
	}
}

func TestCommandDecode_Corrupt(t *testing.T) {
	if _, err := decodeCommand([]byte("{not json")); err == nil {
		t.Fatal("expected decode of corrupt command to error")
	}
}

func kbPtr(kb types.KnowledgeBaseMeta) *types.KnowledgeBaseMeta {
	return &kb
}

// TestStateMachine_Apply_UpdateVersionSummary covers committing the
// version's document-ID set digest.
func TestStateMachine_Apply_UpdateVersionSummary(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdUpdateVersionSummary, VersionID: v.VersionID, DocIDSetHash: "deadbeef"}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("UpdateVersionSummary: %v", res.Err)
	}
	if sm.versions[v.VersionID].DocIDSetHash != "deadbeef" {
		t.Errorf("DocIDSetHash = %q, want deadbeef", sm.versions[v.VersionID].DocIDSetHash)
	}

	// Unknown version → ErrVersionNotFound.
	res = sm.apply(ctx, command{Type: cmdUpdateVersionSummary, VersionID: 999, DocIDSetHash: "x"}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("expected ErrVersionNotFound, got %v", res.Err)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting covers the basic DeleteVersion
// constraint checks: active versions and PENDING versions (in the whole
// recursive subtree) are rejected.
func TestStateMachine_Apply_MarkVersionDeleting(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// v1: active (set via Rollback below), READY.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())
	// v2: child of v1, READY.
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	// Active version must be rejected.
	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v1.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionIsActive) {
		t.Errorf("expected ErrVersionIsActive for active v%d, got %v", v1.VersionID, res.Err)
	}
	if sm.versions[v1.VersionID].Deleting {
		t.Error("active version must not be marked Deleting")
	}

	// Non-active version marks fine; v1 stays untouched.
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkVersionDeleting(v%d): %v", v2.VersionID, res.Err)
	}
	if !sm.versions[v2.VersionID].Deleting {
		t.Error("v2 should be marked Deleting")
	}
	if sm.versions[v1.VersionID].Deleting {
		t.Error("v1 must not be marked Deleting")
	}

	// Idempotent re-mark succeeds.
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Errorf("re-mark of Deleting version should be idempotent, got %v", res.Err)
	}

	// Unknown version / cross-KB version.
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: 999}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("expected ErrVersionNotFound, got %v", res.Err)
	}
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	other := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2"}, w, zap.NewNop())
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: other.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("expected ErrVersionNotFound for cross-KB version, got %v", res.Err)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_Recursive covers the recursive
// subtree semantics: deleting a version marks all descendants too, and a
// PENDING or active version anywhere in the subtree rejects the whole
// deletion.
func TestStateMachine_Apply_MarkVersionDeleting_Recursive(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// Chain v1 -> v2 -> v3, all READY; v1 active.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())

	// Delete v2: v2 and v3 both marked; v1 untouched.
	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("MarkVersionDeleting(v%d): %v", v2.VersionID, res.Err)
	}
	if !sm.versions[v2.VersionID].Deleting || !sm.versions[v3.VersionID].Deleting {
		t.Errorf("expected v%d and v%d Deleting, got v2=%v v3=%v", v2.VersionID, v3.VersionID, sm.versions[v2.VersionID].Deleting, sm.versions[v3.VersionID].Deleting)
	}
	if sm.versions[v1.VersionID].Deleting {
		t.Error("v1 must not be marked Deleting")
	}

	// New tree where the subtree contains the active version: rejected
	// wholesale. v4 -> v5, v5 active.
	v4 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v4.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v5 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v4.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v5.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v5.VersionID}, w, zap.NewNop())

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v4.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionIsActive) {
		t.Errorf("expected ErrVersionIsActive for subtree containing active v%d, got %v", v5.VersionID, res.Err)
	}
	if sm.versions[v4.VersionID].Deleting || sm.versions[v5.VersionID].Deleting {
		t.Error("rejected deletion must not mark any version")
	}

	// PENDING version anywhere in the subtree rejects the whole deletion.
	// v6 -> v7 (v7 stays PENDING), v1 active again.
	v6 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v6.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v7 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v6.VersionID}, w, zap.NewNop()) // stays PENDING
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v6.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionPending) {
		t.Errorf("expected ErrVersionPending for subtree containing pending v%d, got %v", v7.VersionID, res.Err)
	}
	if sm.versions[v6.VersionID].Deleting || sm.versions[v7.VersionID].Deleting {
		t.Error("rejected deletion must not mark any version")
	}
}

// TestStateMachine_Apply_RemoveVersionMeta covers single-version metadata
// removal and its idempotency.
func TestStateMachine_Apply_RemoveVersionMeta(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdRemoveVersionMeta, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("RemoveVersionMeta(v%d): %v", v2.VersionID, res.Err)
	}
	if _, ok := sm.versions[v2.VersionID]; ok {
		t.Error("v2 metadata should be gone")
	}
	if len(sm.versionsByKB["kb-1"]) != 1 || sm.versionsByKB["kb-1"][0] != v1.VersionID {
		t.Errorf("versionsByKB = %v, want [v%d]", sm.versionsByKB["kb-1"], v1.VersionID)
	}

	// Idempotent: removing an already-removed version succeeds.
	res = sm.apply(ctx, command{Type: cmdRemoveVersionMeta, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())
	if res.Err != nil {
		t.Errorf("re-remove should be idempotent, got %v", res.Err)
	}

	// Unknown version succeeds (idempotent); cross-KB is rejected.
	res = sm.apply(ctx, command{Type: cmdRemoveVersionMeta, KBID: "kb-1", VersionID: 999}, w, zap.NewNop())
	if res.Err != nil {
		t.Errorf("remove of absent version should be idempotent, got %v", res.Err)
	}
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	other := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2"}, w, zap.NewNop())
	res = sm.apply(ctx, command{Type: cmdRemoveVersionMeta, KBID: "kb-1", VersionID: other.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("expected ErrVersionNotFound for cross-KB version, got %v", res.Err)
	}
}

// TestStateMachine_Apply_Rollback_RejectsDeleting ensures a Deleting version
// cannot become active again.
func TestStateMachine_Apply_Rollback_RejectsDeleting(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v2.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionDeleting) {
		t.Errorf("expected ErrVersionDeleting, got %v", res.Err)
	}
	if sm.kbs["kb-1"].ActiveVersionID != v1.VersionID {
		t.Errorf("active version must stay v%d, got %d", v1.VersionID, sm.kbs["kb-1"].ActiveVersionID)
	}
}

// TestStateMachine_Apply_CreateVersion_RejectsDeletingParent ensures a
// Deleting version cannot become a parent.
func TestStateMachine_Apply_CreateVersion_RejectsDeletingParent(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrInvalidParentVersion) {
		t.Errorf("expected ErrInvalidParentVersion for Deleting parent, got %v", res.Err)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_AncestorOfActive pins down the
// ancestor protection: the active version's parent (and any ancestor) can
// never be deleted, because its recursive subtree contains the active
// version. Chain v1 -> v2 -> v3 with v3 active.
func TestStateMachine_Apply_MarkVersionDeleting_AncestorOfActive(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v3.VersionID}, w, zap.NewNop())

	// Parent of the active version: rejected.
	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionIsActive) {
		t.Errorf("delete parent of active: expected ErrVersionIsActive, got %v", res.Err)
	}

	// Grandparent of the active version: rejected too.
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v1.VersionID}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionIsActive) {
		t.Errorf("delete grandparent of active: expected ErrVersionIsActive, got %v", res.Err)
	}

	// Nothing may have been marked Deleting by the rejected attempts.
	for _, vid := range []int64{v1.VersionID, v2.VersionID, v3.VersionID} {
		if sm.versions[vid].Deleting {
			t.Errorf("v%d must not be marked Deleting after rejected attempts", vid)
		}
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_SingleSplicesChildren covers
// the SINGLE mode: only the target version is removed, and its direct
// children are re-parented onto its parent, so an arbitrary middle version
// can be dropped without losing the branch structure below it.
func TestStateMachine_Apply_MarkVersionDeleting_SingleSplicesChildren(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// v1 -> v2, then v2 forks into v3 and v4. v1 is active.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v1.VersionID}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v4 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v4.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID, Mode: types.VersionDeleteSingle}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("SINGLE delete of v%d: %v", v2.VersionID, res.Err)
	}
	if len(res.DeletedVersionIDs) != 1 || res.DeletedVersionIDs[0] != v2.VersionID {
		t.Errorf("deleted set = %v, want [%d]", res.DeletedVersionIDs, v2.VersionID)
	}
	if !sm.versions[v2.VersionID].Deleting {
		t.Error("v2 should be marked Deleting")
	}
	for _, child := range []int64{v3.VersionID, v4.VersionID} {
		if got := sm.versions[child].ParentVersionID; got != v1.VersionID {
			t.Errorf("child v%d parent = %d, want %d (spliced onto v1)", child, got, v1.VersionID)
		}
		if sm.versions[child].Deleting {
			t.Errorf("child v%d must not be marked Deleting", child)
		}
	}
	if sm.versions[v1.VersionID].Deleting {
		t.Error("v1 must not be marked Deleting")
	}

	// Idempotent re-mark of the already-Deleting target succeeds.
	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID, Mode: types.VersionDeleteSingle}, w, zap.NewNop())
	if res.Err != nil {
		t.Errorf("re-mark of Deleting version should be idempotent, got %v", res.Err)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_Ancestors covers the ANCESTORS
// mode: every preceding version is removed — including sibling branches
// hanging off those ancestors — and the target becomes the new base by
// having its parent pointer cleared. The target, its subtree and the active
// version of the KB survive.
func TestStateMachine_Apply_MarkVersionDeleting_Ancestors(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// v1 -> v2 -> v3, plus a sibling branch v1 -> v5. v3 is active.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v5 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v5.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v3.VersionID}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v3.VersionID, Mode: types.VersionDeleteAncestors}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("ANCESTORS delete of v%d: %v", v3.VersionID, res.Err)
	}
	want := map[int64]bool{v1.VersionID: true, v2.VersionID: true, v5.VersionID: true}
	if len(res.DeletedVersionIDs) != len(want) {
		t.Errorf("deleted set = %v, want the %d ancestors + siblings", res.DeletedVersionIDs, len(want))
	}
	for _, id := range res.DeletedVersionIDs {
		if !want[id] {
			t.Errorf("unexpected deleted version v%d", id)
		}
		delete(want, id)
	}
	for id := range want {
		t.Errorf("v%d missing from the deleted set", id)
	}
	if sm.versions[v3.VersionID].Deleting {
		t.Error("the new base v3 must not be marked Deleting")
	}
	if got := sm.versions[v3.VersionID].ParentVersionID; got != 0 {
		t.Errorf("new base v3 parent = %d, want 0", got)
	}

	// Active-version protection still applies to the removed ancestor set:
	// in a fresh KB with v1 active, ANCESTORS on v3 must be rejected and
	// must not touch v3's parent pointer.
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	o1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: o1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-2", TargetVersionID: o1.VersionID}, w, zap.NewNop())
	o2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2", ParentVersionID: o1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: o2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-2", VersionID: o2.VersionID, Mode: types.VersionDeleteAncestors}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionIsActive) {
		t.Errorf("ANCESTORS over an active ancestor: expected ErrVersionIsActive, got %v", res.Err)
	}
	if sm.versions[o1.VersionID].Deleting || sm.versions[o2.VersionID].Deleting {
		t.Error("rejected deletion must not mark any version")
	}
	if got := sm.versions[o2.VersionID].ParentVersionID; got != o1.VersionID {
		t.Errorf("rejected deletion must not rewire o2's parent, got %d", got)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_AncestorsIsNoopOnBase pins the
// idempotency of "set as base" on a version that already is the base, and
// rejects an unknown delete mode.
func TestStateMachine_Apply_MarkVersionDeleting_AncestorsIsNoopOnBase(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v1.VersionID, Mode: types.VersionDeleteAncestors}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("ANCESTORS on the base v%d: %v", v1.VersionID, res.Err)
	}
	if len(res.DeletedVersionIDs) != 0 {
		t.Errorf("deleted set = %v, want empty (already the base)", res.DeletedVersionIDs)
	}
	if sm.versions[v1.VersionID].Deleting || sm.versions[v2.VersionID].Deleting {
		t.Error("a no-op ANCESTORS call must not mark anything Deleting")
	}
	if got := sm.versions[v1.VersionID].ParentVersionID; got != 0 {
		t.Errorf("base v1 parent = %d, want 0", got)
	}

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID, Mode: types.VersionDeleteMode(99)}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrInvalidArgument) {
		t.Errorf("unknown mode: expected ErrInvalidArgument, got %v", res.Err)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_AncestorsReplayAndPending covers
// re-applying an ANCESTORS command (log replay / operator retry) and the
// PENDING rejection when a swept-up sibling branch is still building.
func TestStateMachine_Apply_MarkVersionDeleting_AncestorsReplayAndPending(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// v1 -> v2 -> v3, all READY; v3 active.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v3.VersionID}, w, zap.NewNop())

	ancestors := command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v3.VersionID, Mode: types.VersionDeleteAncestors}
	if res := sm.apply(ctx, ancestors, w, zap.NewNop()); res.Err != nil {
		t.Fatalf("ANCESTORS(v%d): %v", v3.VersionID, res.Err)
	}
	if !sm.versions[v1.VersionID].Deleting || !sm.versions[v2.VersionID].Deleting {
		t.Fatal("v1/v2 should be Deleting after the first apply")
	}

	// Replay: no ancestors remain, so nothing changes and the set is empty.
	res := sm.apply(ctx, ancestors, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("replayed ANCESTORS(v%d): %v", v3.VersionID, res.Err)
	}
	if len(res.DeletedVersionIDs) != 0 {
		t.Errorf("replayed deleted set = %v, want empty", res.DeletedVersionIDs)
	}
	if sm.versions[v3.VersionID].ParentVersionID != 0 || sm.versions[v3.VersionID].Deleting {
		t.Error("replay must leave the new base untouched")
	}

	// A PENDING version on a swept-up sibling branch rejects the whole call.
	// kb-2: o1 -> o2 -> o3, plus sibling o1 -> o4 (left PENDING); o3 active.
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	o1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: o1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	o2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2", ParentVersionID: o1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: o2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	o3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2", ParentVersionID: o2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: o3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	o4 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2", ParentVersionID: o1.VersionID}, w, zap.NewNop()) // stays PENDING
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-2", TargetVersionID: o3.VersionID}, w, zap.NewNop())

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-2", VersionID: o3.VersionID, Mode: types.VersionDeleteAncestors}, w, zap.NewNop())
	if !errors.Is(res.Err, stratumerrors.ErrVersionPending) {
		t.Errorf("ANCESTORS over a PENDING sibling v%d: expected ErrVersionPending, got %v", o4.VersionID, res.Err)
	}
	for _, vid := range []int64{o1.VersionID, o2.VersionID, o3.VersionID, o4.VersionID} {
		if sm.versions[vid].Deleting {
			t.Errorf("rejected deletion must not mark v%d Deleting", vid)
		}
	}
	if sm.versions[o3.VersionID].ParentVersionID != o2.VersionID {
		t.Error("rejected deletion must not clear the target's parent pointer")
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_SingleSpliceEdgeCases covers the
// two SINGLE-mode rewiring edge cases: the target is already the base (its
// children become roots), and the target's parent is itself being deleted
// (children become roots instead of dangling off a vanishing version).
func TestStateMachine_Apply_MarkVersionDeleting_SingleSpliceEdgeCases(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	// v1 is the base; v2 is active so v1 can be removed.
	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v2.VersionID}, w, zap.NewNop())

	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v1.VersionID, Mode: types.VersionDeleteSingle}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("SINGLE delete of base v%d: %v", v1.VersionID, res.Err)
	}
	if got := sm.versions[v2.VersionID].ParentVersionID; got != 0 {
		t.Errorf("child of a removed base: parent = %d, want 0", got)
	}

	// Parent already Deleting: children become roots rather than re-attaching
	// to a version whose metadata is about to be removed.
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-2"))}, w, zap.NewNop())
	p1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: p1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	p2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2", ParentVersionID: p1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: p2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	p3 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-2", ParentVersionID: p2.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: p3.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-2", TargetVersionID: p3.VersionID}, w, zap.NewNop())
	// Simulate p1 caught mid-cleanup (a concurrent SUBTREE delete already
	// marked it, its metadata not yet removed by RemoveVersionMeta).
	p1Meta := sm.versions[p1.VersionID]
	p1Meta.Deleting = true
	sm.versions[p1.VersionID] = p1Meta

	res = sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-2", VersionID: p2.VersionID, Mode: types.VersionDeleteSingle}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("SINGLE delete of v%d under a deleting parent: %v", p2.VersionID, res.Err)
	}
	if got := sm.versions[p3.VersionID].ParentVersionID; got != 0 {
		t.Errorf("child under a deleting parent: parent = %d, want 0 (root)", got)
	}
}

// TestStateMachine_Apply_MarkVersionDeleting_AncestorsHealsBrokenChain covers
// the broken-chain case: the parent pointer still names a version whose
// metadata is already gone (its cleanup finished). ANCESTORS has nothing left
// to delete, yet must still clear the stale pointer so the version becomes a
// proper root instead of a permanent orphan.
func TestStateMachine_Apply_MarkVersionDeleting_AncestorsHealsBrokenChain(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	v1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v1.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	v2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: v1.VersionID}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: v2.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())
	sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v2.VersionID}, w, zap.NewNop())

	// v1's metadata is already gone while v2 still points at it.
	delete(sm.versions, v1.VersionID)
	sm.versionsByKB["kb-1"] = []int64{v2.VersionID}
	if got := sm.versions[v2.VersionID].ParentVersionID; got != v1.VersionID {
		t.Fatalf("fixture: v2 parent = %d, want the now-missing v%d", got, v1.VersionID)
	}

	res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: v2.VersionID, Mode: types.VersionDeleteAncestors}, w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("ANCESTORS over a broken chain: %v", res.Err)
	}
	if len(res.DeletedVersionIDs) != 0 {
		t.Errorf("deleted set = %v, want empty (nothing left to delete)", res.DeletedVersionIDs)
	}
	if got := sm.versions[v2.VersionID].ParentVersionID; got != 0 {
		t.Errorf("broken-chain heal: v2 parent = %d, want 0", got)
	}
	if sm.versions[v2.VersionID].Deleting {
		t.Error("the healed root must not be marked Deleting")
	}
}

// TestMockAndStateMachine_VersionDeleteModesAgree pins MockRaftNode and the
// real state machine to identical behavior for every DeleteVersion mode: the
// same deleted set, the same rewiring (ParentVersionID) and the same Deleting
// flags. MockRaftNode hand-mirrors the apply logic (it is a per-test double
// for the coordinator/service packages), so without this the two copies can
// drift apart silently.
func TestMockAndStateMachine_VersionDeleteModesAgree(t *testing.T) {
	ctx := context.Background()

	// build wires one identical fixture — v1 -> v2 -> v3 plus a sibling
	// subtree v1 -> v4, all READY (unless pendingLeaf keeps v3 PENDING),
	// v3 active — into a fresh state machine and a fresh MockRaftNode, and
	// returns the version IDs by name.
	build := func(t *testing.T, pendingLeaf bool) (*stateMachine, *wal.MockWAL, *MockRaftNode, map[string]int64) {
		t.Helper()
		sm, w := newTestSM(t)
		mock := NewMockRaftNode(w)
		kb := testKB("kb-1")

		if res := sm.apply(ctx, command{Type: cmdCreateKB, KB: &kb}, w, zap.NewNop()); res.Err != nil {
			t.Fatalf("sm create KB: %v", res.Err)
		}
		if err := mock.ProposeCreateKB(ctx, kb); err != nil {
			t.Fatalf("mock create KB: %v", err)
		}

		ids := map[string]int64{}
		create := func(name string, parentID int64, ready bool) int64 {
			res := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: parentID}, w, zap.NewNop())
			if res.Err != nil {
				t.Fatalf("sm create %s: %v", name, res.Err)
			}
			mockID, err := mock.ProposeCreateVersion(ctx, "kb-1", parentID)
			if err != nil {
				t.Fatalf("mock create %s: %v", name, err)
			}
			if res.VersionID != mockID {
				t.Fatalf("%s: sm version ID %d != mock version ID %d", name, res.VersionID, mockID)
			}
			if ready {
				sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: mockID, Status: types.IndexStatusReady}, w, zap.NewNop())
				if err := mock.ProposeUpdateVersionStatus(ctx, mockID, types.IndexStatusReady); err != nil {
					t.Fatalf("mock %s READY: %v", name, err)
				}
			}
			ids[name] = mockID
			return mockID
		}

		v1 := create("v1", 0, true)
		v2 := create("v2", v1, true)
		v3 := create("v3", v2, !pendingLeaf)
		create("v4", v1, true)

		sm.apply(ctx, command{Type: cmdRollback, KBID: "kb-1", TargetVersionID: v3}, w, zap.NewNop())
		if err := mock.ProposeRollback(ctx, "kb-1", v3); err != nil {
			t.Fatalf("mock rollback: %v", err)
		}
		return sm, w, mock, ids
	}

	cases := []struct {
		name   string
		target string
		mode   types.VersionDeleteMode
		// wantSentinel is the error both implementations must reject with;
		// nil means the call must succeed.
		wantSentinel error
		// pendingLeaf keeps the leaf v3 PENDING, which makes it a live
		// survivor that still depends on a to-be-deleted parent.
		pendingLeaf bool
	}{
		{"subtree-rejected-active-in-subtree", "v2", types.VersionDeleteSubtree, stratumerrors.ErrVersionIsActive, false},
		{"subtree-leaf", "v4", types.VersionDeleteSubtree, nil, false},
		{"single", "v2", types.VersionDeleteSingle, nil, false},
		{"single-pending-survivor", "v2", types.VersionDeleteSingle, stratumerrors.ErrVersionPending, true},
		{"ancestors", "v3", types.VersionDeleteAncestors, nil, false},
		{"ancestors-pending-survivor", "v3", types.VersionDeleteAncestors, stratumerrors.ErrVersionPending, true},
		{"ancestors-on-base", "v1", types.VersionDeleteAncestors, nil, false},
		{"invalid-mode", "v2", types.VersionDeleteMode(99), stratumerrors.ErrInvalidArgument, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sm, w, mock, ids := build(t, tc.pendingLeaf)
			target := ids[tc.target]

			// expectedParent is the fixture's version tree (v1 -> v2 -> v3
			// plus sibling v1 -> v4), used to pin that a rejected command
			// did not rewire anything.
			expectedParent := map[int64]int64{
				ids["v1"]: 0,
				ids["v2"]: ids["v1"],
				ids["v3"]: ids["v2"],
				ids["v4"]: ids["v1"],
			}
			// assertStatesAgree pins that MockRaftNode and the state machine
			// ended up with identical metadata for every fixture version.
			assertStatesAgree := func(t *testing.T) {
				t.Helper()
				for _, id := range ids {
					smMeta := sm.versions[id]
					mockMeta, ok := mock.GetVersion(id)
					if !ok {
						t.Fatalf("mock lost version %d", id)
					}
					if smMeta.ParentVersionID != mockMeta.ParentVersionID {
						t.Errorf("v%d ParentVersionID: sm=%d mock=%d", id, smMeta.ParentVersionID, mockMeta.ParentVersionID)
					}
					if smMeta.Deleting != mockMeta.Deleting {
						t.Errorf("v%d Deleting: sm=%v mock=%v", id, smMeta.Deleting, mockMeta.Deleting)
					}
				}
			}

			res := sm.apply(ctx, command{Type: cmdMarkVersionDeleting, KBID: "kb-1", VersionID: target, Mode: tc.mode}, w, zap.NewNop())
			mockIDs, mockErr := mock.ProposeMarkVersionDeleting(ctx, "kb-1", target, tc.mode)

			if tc.wantSentinel == nil {
				if res.Err != nil || mockErr != nil {
					t.Fatalf("expected success, got sm=%v mock=%v", res.Err, mockErr)
				}
			} else {
				if !errors.Is(res.Err, tc.wantSentinel) || !errors.Is(mockErr, tc.wantSentinel) {
					t.Fatalf("expected %v, got sm=%v mock=%v", tc.wantSentinel, res.Err, mockErr)
				}
				// A rejected delete must leave the tree untouched on both
				// sides: same metadata as each other, nothing marked
				// Deleting. Without this, an implementation that mutated
				// the tree before failing would slip through.
				assertStatesAgree(t)
				for _, id := range ids {
					if sm.versions[id].Deleting || sm.versions[id].ParentVersionID != expectedParent[id] {
						t.Errorf("rejected delete mutated v%d: parent=%d deleting=%v", id, sm.versions[id].ParentVersionID, sm.versions[id].Deleting)
					}
				}
				return
			}

			got := append([]int64(nil), res.DeletedVersionIDs...)
			want := append([]int64(nil), mockIDs...)
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
			if len(got) != len(want) {
				t.Fatalf("deleted set mismatch: sm=%v mock=%v", got, want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("deleted set mismatch: sm=%v mock=%v", got, want)
				}
			}

			assertStatesAgree(t)
		})
	}
}
