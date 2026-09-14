package service

import (
	"context"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/types"
)

// TestGetSystemStatus_ReportsDataMissingVersions pins the wiring: a PENDING
// version that no candidate replica holds shows up in the status response
// (Stratum_设计文档v13.md §7.12 step ② — surface it instead of letting it look
// like a slow index build).
func TestGetSystemStatus_ReportsDataMissingVersions(t *testing.T) {
	ctx := context.Background()

	// Shorten the probe age so a freshly created version is judged now.
	oldAge := dataMissingMinAgeSec
	dataMissingMinAgeSec = 0
	defer func() { dataMissingMinAgeSec = oldAge }()

	h := newAdminHarness()
	if err := h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if _, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0); err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	// No replica holds the version's data.
	h.svc.replicas = func() []string { return []string{"n2", "n3"} }
	h.svc.presence = &stubPresence{holds: nil}

	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	if len(resp.GetDataMissingVersions()) != 1 {
		t.Fatalf("data_missing_versions = %v, want the one PENDING version", resp.GetDataMissingVersions())
	}
	if got := resp.GetDataMissingVersions()[0].GetKbId(); got != "kb-1" {
		t.Errorf("reported KB = %q, want kb-1", got)
	}
}

// A version that some replica holds is not reported: it is merely waiting for
// its index build.
func TestGetSystemStatus_HeldVersionIsNotDataMissing(t *testing.T) {
	ctx := context.Background()

	oldAge := dataMissingMinAgeSec
	dataMissingMinAgeSec = 0
	defer func() { dataMissingMinAgeSec = oldAge }()

	h := newAdminHarness()
	if err := h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	vID, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	h.svc.replicas = func() []string { return []string{"n2", "n3"} }
	h.svc.presence = &stubPresence{holds: map[string]map[int64]bool{"n3": {vID: true}}}

	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	if len(resp.GetDataMissingVersions()) != 0 {
		t.Errorf("data_missing_versions = %v, want none (a replica holds it)", resp.GetDataMissingVersions())
	}
}

// Without a replica set (single-node deployment) the check is a no-op rather
// than reporting every PENDING version as missing.
func TestGetSystemStatus_NoReplicaSetSkipsDataMissingCheck(t *testing.T) {
	ctx := context.Background()

	oldAge := dataMissingMinAgeSec
	dataMissingMinAgeSec = 0
	defer func() { dataMissingMinAgeSec = oldAge }()

	h := newAdminHarness()
	if err := h.raftNode.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
		KBID: "kb-1", Name: "kb-1", Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB: %v", err)
	}
	if _, err := h.raftNode.ProposeCreateVersion(ctx, "kb-1", 0); err != nil {
		t.Fatalf("ProposeCreateVersion: %v", err)
	}

	// newAdminHarness leaves replicas/presence nil.
	resp, err := h.svc.GetSystemStatus(ctx, &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	if len(resp.GetDataMissingVersions()) != 0 {
		t.Errorf("data_missing_versions = %v, want none without a replica set", resp.GetDataMissingVersions())
	}
}
