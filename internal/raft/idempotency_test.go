package raft

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// TestStateMachine_Apply_CreateVersion_IdempotentRetry pins the core of the
// client idempotency key: a retried CreateVersion carrying the same key
// returns the version the first attempt allocated instead of allocating
// another one. That is what lets a client re-send the changes for a version
// whose data never landed (Stratum_设计文档v13.md §7.12).
func TestStateMachine_Apply_CreateVersion_IdempotentRetry(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	first := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-1"}, w, zap.NewNop())
	if first.Err != nil {
		t.Fatalf("first create: %v", first.Err)
	}

	retry := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-1"}, w, zap.NewNop())
	if retry.Err != nil {
		t.Fatalf("retry: %v", retry.Err)
	}
	if retry.VersionID != first.VersionID {
		t.Fatalf("retry allocated v%d, want the original v%d", retry.VersionID, first.VersionID)
	}
	if got := len(sm.versions); got != 1 {
		t.Errorf("versions = %d, want 1 (a retry must not allocate)", got)
	}

	// A different key allocates its own version.
	other := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-2"}, w, zap.NewNop())
	if other.Err != nil {
		t.Fatalf("different key: %v", other.Err)
	}
	if other.VersionID == first.VersionID {
		t.Error("a different client key must allocate a new version")
	}

	// An empty key keeps the historical semantics: every call allocates.
	none1 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	none2 := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if none1.Err != nil || none2.Err != nil {
		t.Fatalf("keyless creates: %v, %v", none1.Err, none2.Err)
	}
	if none1.VersionID == none2.VersionID {
		t.Error("without a client key every call must allocate a new version")
	}
}

// A retry that arrives after the parent's own state moved on (e.g. it became
// READY, or gained a child) must still resolve to the original version: the
// idempotency check deliberately runs before the parent constraints.
func TestStateMachine_Apply_CreateVersion_IdempotentRetryBeatsParentChecks(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	root := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1"}, w, zap.NewNop())
	if root.Err != nil {
		t.Fatalf("root create: %v", root.Err)
	}
	sm.apply(ctx, command{Type: cmdUpdateVersionStatus, VersionID: root.VersionID, Status: types.IndexStatusReady}, w, zap.NewNop())

	first := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: root.VersionID, ClientRequestID: "req-1"}, w, zap.NewNop())
	if first.Err != nil {
		t.Fatalf("first child: %v", first.Err)
	}

	// The parent now has a child, so a *new* create against it would be
	// rejected by the linear-chain rule; the retry must not be.
	retry := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ParentVersionID: root.VersionID, ClientRequestID: "req-1"}, w, zap.NewNop())
	if retry.Err != nil {
		t.Fatalf("retry against a parent that moved on: %v", retry.Err)
	}
	if retry.VersionID != first.VersionID {
		t.Fatalf("retry allocated v%d, want the original v%d", retry.VersionID, first.VersionID)
	}
}

// The idempotency map must survive a snapshot round-trip, otherwise a retry
// after a restart would fork a second version.
func TestStateMachine_IdempotencyMapSurvivesSnapshot(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	first := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-1"}, w, zap.NewNop())
	if first.Err != nil {
		t.Fatalf("create: %v", first.Err)
	}

	data, err := sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}

	retry := restored.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-1"}, w, zap.NewNop())
	if retry.VersionID != first.VersionID {
		t.Fatalf("after a snapshot round-trip the retry allocated v%d, want v%d", retry.VersionID, first.VersionID)
	}
}

// The mapping is dropped together with the metadata it points at, so a reused
// key can never resolve to a version that no longer exists.
func TestStateMachine_RequestMappingDroppedWithVersion(t *testing.T) {
	sm, w := newTestSM(t)
	ctx := context.Background()
	sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB("kb-1"))}, w, zap.NewNop())

	first := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-1"}, w, zap.NewNop())
	if first.Err != nil {
		t.Fatalf("create: %v", first.Err)
	}

	sm.apply(ctx, command{Type: cmdRemoveVersionMeta, KBID: "kb-1", VersionID: first.VersionID}, w, zap.NewNop())
	if _, ok := sm.versionsByRequest[requestKey("kb-1", "req-1")]; ok {
		t.Error("the idempotency mapping must be dropped with the version metadata")
	}

	again := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: "kb-1", ClientRequestID: "req-1"}, w, zap.NewNop())
	if again.Err != nil {
		t.Fatalf("reused key after removal: %v", again.Err)
	}
	if again.VersionID == first.VersionID {
		t.Error("the key must be free to allocate a fresh version once the old one is gone")
	}
}

// TestMockRaftNode_CreateVersion_IdempotentRetry keeps the test double in step
// with the real state machine (the two must agree on retry semantics).
func TestMockRaftNode_CreateVersion_IdempotentRetry(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	first, err := r.ProposeCreateVersion(ctx, "kb1", 0, WithClientRequestID("req-1"))
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	retry, err := r.ProposeCreateVersion(ctx, "kb1", 0, WithClientRequestID("req-1"))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry != first {
		t.Fatalf("retry allocated v%d, want the original v%d", retry, first)
	}

	other, err := r.ProposeCreateVersion(ctx, "kb1", 0, WithClientRequestID("req-2"))
	if err != nil {
		t.Fatalf("different key: %v", err)
	}
	if other == first {
		t.Error("a different client key must allocate a new version")
	}
}
