package service

import (
	"context"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// TestGetSystemStatus_ReportsFailedPermanentVersions pins the operator-visible
// half of Stratum_设计文档v13.md §10.1: a version the control layer declared dead
// shows up with its cause chain, so someone can act on it.
func TestGetSystemStatus_ReportsFailedPermanentVersions(t *testing.T) {
	ctx := context.Background()

	h := newAdminHarness()
	if err := h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	versionID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", versionID,
		types.FailureSideData, "data unavailable on every replica", 4); err != nil {
		t.Fatalf("ProposeMarkVersionFailedPermanent: %v", err)
	}

	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}

	failed := resp.GetFailedPermanentVersions()
	if len(failed) != 1 {
		t.Fatalf("failed_permanent_versions = %v, want the one declared version", failed)
	}
	got := failed[0]
	if got.GetKbId() != "kb-1" || got.GetVersionId() != versionID {
		t.Errorf("reported %s/v%d, want kb-1/v%d", got.GetKbId(), got.GetVersionId(), versionID)
	}
	if got.GetReason() != "data unavailable on every replica" {
		t.Errorf("reason = %q, want the recorded cause", got.GetReason())
	}
	if got.GetFailureCount() != 4 {
		t.Errorf("failure_count = %d, want 4", got.GetFailureCount())
	}
	if got.GetSide() != pb.FailureSide_FAILURE_SIDE_DATA {
		t.Errorf("side = %v, want FAILURE_SIDE_DATA: the report named the data side (§10.1b)", got.GetSide())
	}

	// A permanent failure is a terminal verdict, not a version waiting to be
	// recovered: it must not be reported as merely data-missing.
	if n := len(resp.GetDataMissingVersions()); n != 0 {
		t.Errorf("data_missing_versions = %d, want 0 for a terminal verdict", n)
	}
}

// A recoverable FAILED version is not reported as permanently failed: the two
// states mean different things to an operator ("retry it" vs "it is dead").
func TestGetSystemStatus_RetryableFailureIsNotPermanent(t *testing.T) {
	ctx := context.Background()

	h := newAdminHarness()
	if err := h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	versionID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusFailed, 0); err != nil {
		t.Fatalf("ProposeUpdateVersionStatus: %v", err)
	}

	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	if n := len(resp.GetFailedPermanentVersions()); n != 0 {
		t.Errorf("failed_permanent_versions = %d, want 0 for a retryable failure", n)
	}
	if n := len(resp.GetStuckVersions()); n != 1 {
		t.Errorf("stuck_versions = %d, want the FAILED version to still be reported there", n)
	}
}

// The side travels with the verdict (Stratum_设计文档v13.md §10.1b) because the
// remedies differ: retrying a write and rebuilding an index are different actions
// on different layers, and a version can end up terminal on both.
func TestGetSystemStatus_ReportsWhichSideFailed(t *testing.T) {
	ctx := context.Background()

	h := newAdminHarness()
	if err := h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	versionID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", versionID,
		types.FailureSideIndex, "index build failed", 5); err != nil {
		t.Fatalf("ProposeMarkVersionFailedPermanent: %v", err)
	}

	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	failed := resp.GetFailedPermanentVersions()
	if len(failed) != 1 {
		t.Fatalf("failed_permanent_versions = %v, want the one declared version", failed)
	}
	if got := failed[0].GetSide(); got != pb.FailureSide_FAILURE_SIDE_INDEX {
		t.Errorf("side = %v, want FAILURE_SIDE_INDEX: the verdict named the index side", got)
	}
}
