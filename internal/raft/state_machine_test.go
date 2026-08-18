package raft

import (
	"context"
	"errors"
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
