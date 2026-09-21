package raft

import (
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// DiscardVersion is the caller's way out of a write that never landed
// (docs/await-version-plan.md §7 Step 6). Its admission rule is the interesting
// part: it is a compare-and-set on the state machine, so what these tests pin is
// who gets refused and why.

func TestDiscardVersion_RemovesAPendingVersion(t *testing.T) {
	ctx := proposeCtx(t)
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")
	v, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	if err := r.ProposeDiscardVersion(ctx, "kb1", v); err != nil {
		t.Fatalf("ProposeDiscardVersion: %v", err)
	}
	if _, ok := r.VersionByID(v); ok {
		t.Error("version metadata survived the discard")
	}
	versions, err := r.ListVersions(ctx, "kb1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("versions after discard = %d, want 0", len(versions))
	}

	// The state machine's answer for a second discard is "there is no such
	// version" — the service layer is what turns that into discarded=false, so
	// that a retry after a lost response is harmless rather than an error.
	if err := r.ProposeDiscardVersion(ctx, "kb1", v); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("second discard = %v, want ErrVersionNotFound", err)
	}
}

func TestDiscardVersion_RefusesASettledVersion(t *testing.T) {
	ctx := proposeCtx(t)
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")
	v, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if err := r.ProposeMarkVersionDataDurable(ctx, v); err != nil {
		t.Fatalf("ProposeMarkVersionDataDurable: %v", err)
	}

	// The data landed: this is DeleteVersion's business, and the refusal has to
	// say so rather than removing durable data a caller merely believed lost.
	err = r.ProposeDiscardVersion(ctx, "kb1", v)
	if !errors.Is(err, stratumerrors.ErrVersionNotPending) {
		t.Errorf("discarding a durable version = %v, want ErrVersionNotPending", err)
	}
	if _, ok := r.VersionByID(v); !ok {
		t.Error("a refused discard removed the version anyway")
	}
}

func TestDiscardVersion_RefusesTheActiveVersion(t *testing.T) {
	ctx := proposeCtx(t)
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")
	v, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	mustUpdateStatus(t, r, v, types.IndexStatusReady)
	if err := r.ProposeRollback(ctx, "kb1", v); err != nil {
		t.Fatalf("ProposeRollback: %v", err)
	}

	if err := r.ProposeDiscardVersion(ctx, "kb1", v); !errors.Is(err, stratumerrors.ErrVersionIsActive) {
		t.Errorf("discarding the active version = %v, want ErrVersionIsActive", err)
	}
}

func TestDiscardVersion_RefusesAVersionWithAChild(t *testing.T) {
	ctx := proposeCtx(t)
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")
	parent, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	// A PENDING version cannot acquire a child through the write path — that is
	// exactly the invariant applyCreateVersion enforces — so the child is
	// planted here to pin what happens if that invariant ever breaks: discarding
	// the parent would leave the child inheriting from a version that is gone.
	child := parent + 1000
	r.versions[child] = types.VersionMeta{
		VersionID:       child,
		KBID:            "kb1",
		ParentVersionID: parent,
		IndexStatus:     types.IndexStatusPending,
		DataStatus:      types.DataStatusPending,
	}
	r.versionsByKB["kb1"] = append(r.versionsByKB["kb1"], child)

	if err := r.ProposeDiscardVersion(ctx, "kb1", parent); !errors.Is(err, stratumerrors.ErrInvalidParentVersion) {
		t.Errorf("discarding a parent with a child = %v, want ErrInvalidParentVersion", err)
	}
	if _, ok := r.VersionByID(parent); !ok {
		t.Error("the refused discard removed the parent anyway")
	}
}

func TestDiscardVersion_ReportsAnUnknownVersion(t *testing.T) {
	ctx := proposeCtx(t)
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	if err := r.ProposeDiscardVersion(ctx, "kb1", 4242); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("unknown version = %v, want ErrVersionNotFound", err)
	}
	if err := r.ProposeDiscardVersion(ctx, "no-such-kb", 1); !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("unknown knowledge base = %v, want ErrKnowledgeBaseNotFound", err)
	}
}

func TestDiscardVersion_FreesTheIdempotencyKey(t *testing.T) {
	ctx := proposeCtx(t)
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0, WithClientRequestID("key-1"))
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if err := r.ProposeDiscardVersion(ctx, "kb1", v1); err != nil {
		t.Fatalf("ProposeDiscardVersion: %v", err)
	}

	// The point of discarding is starting over. If the idempotency mapping
	// survived, a caller re-sending under the same key would be handed back the
	// very version it just abandoned — a version that no longer exists.
	v2, err := r.ProposeCreateVersion(ctx, "kb1", 0, WithClientRequestID("key-1"))
	if err != nil {
		t.Fatalf("re-send under the discarded key: %v", err)
	}
	if v2 == v1 {
		t.Errorf("re-send reused discarded version %d; want a fresh version", v1)
	}
}
