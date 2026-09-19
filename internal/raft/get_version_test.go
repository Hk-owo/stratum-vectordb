package raft

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// GetVersion exists so the await path can read ONE version instead of the whole
// chain on every poll (docs/await-version-plan.md §7 Step 2). What matters for
// its callers is that it answers exactly what ListVersions would say about that
// version, and that "not in this knowledge base" is one answer rather than two.

func TestGetVersion_AgreesWithListVersions(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")

	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion(v1): %v", err)
	}
	mustUpdateStatus(t, r, v1, types.IndexStatusReady)
	v2, err := r.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion(v2): %v", err)
	}
	if err := r.ProposeUpdateVersionSummary(ctx, v2, "digest"); err != nil {
		t.Fatalf("ProposeUpdateVersionSummary(v2): %v", err)
	}

	versions, err := r.ListVersions(ctx, "kb1")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	for _, want := range versions {
		got, err := r.GetVersion(ctx, "kb1", want.VersionID)
		if err != nil {
			t.Fatalf("GetVersion(%d): %v", want.VersionID, err)
		}
		// The same version described two ways must not drift: the await path
		// reads with GetVersion while the console reads with ListVersions, and a
		// disagreement between them would be visible as "the console and the
		// caller see different states for one version".
		if got.VersionID != want.VersionID ||
			got.ParentVersionID != want.ParentVersionID ||
			got.IndexStatus != want.IndexStatus ||
			got.DataStatus != want.DataStatus ||
			got.DocIDSetHash != want.DocIDSetHash ||
			got.Deleting != want.Deleting {
			t.Errorf("GetVersion(%d) = %+v, want %+v (ListVersions' answer)", want.VersionID, got, want)
		}
	}
}

func TestGetVersion_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestRaftNode()
	mustCreateKB(t, r, "kb1")
	v1, err := r.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	if _, err := r.GetVersion(ctx, "kb1", v1+999); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("unknown version id = %v, want ErrVersionNotFound", err)
	}
	if _, err := r.GetVersion(ctx, "no-such-kb", v1); !errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
		t.Errorf("unknown knowledge base = %v, want ErrKnowledgeBaseNotFound", err)
	}

	// A version id is unique within a knowledge base, not globally: asking for
	// kb1's version under kb2 must not resolve it, because the caller would then
	// be watching the wrong version's progress.
	mustCreateKB(t, r, "kb2")
	if _, err := r.GetVersion(ctx, "kb2", v1); !errors.Is(err, stratumerrors.ErrVersionNotFound) {
		t.Errorf("version read through the wrong KB = %v, want ErrVersionNotFound", err)
	}
}
