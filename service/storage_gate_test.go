package service

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
)

// stubStorageGate is a StorageDegradationSource under the test's control.
//
// The two tiers are set independently because they are independent answers on the
// wire; a test that wants "the storage layer is gone" sets unavailable, and one
// that wants "short a quorum" sets degraded.
type stubStorageGate struct {
	degraded    bool
	unavailable bool
	detail      string
	known       bool
}

func (g *stubStorageGate) StorageDegraded(string) (bool, string, bool) {
	return g.degraded, g.detail, g.known
}

func (g *stubStorageGate) StorageUnavailable(string) (bool, string, bool) {
	return g.unavailable, g.detail, g.known
}

func createVersionRequest(kbID string) *pb.CreateVersionRequest {
	return &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "hello"},
		},
	}
}

// The control-layer entry point is the authoritative gate
// (docs/storage-degradation-signal-plan.md §4.3): a write that cannot reach quorum
// is refused here, and refused BEFORE anything is allocated — no version number, no
// Raft entry, no retry budget spent on an attempt that could never have succeeded.
func TestKnowledgeBaseService_CreateVersionRefusesWhenStorageIsDegraded(t *testing.T) {
	h := newKBSvcTestHarness()
	h.svc.SetStorageDegradationSource(&stubStorageGate{
		degraded: true,
		detail:   "kb-1: 1 of 3 required replicas live, below the quorum of 2; silent: 2 (never reported)",
		known:    true,
	})

	_, err := h.svc.CreateVersion(context.Background(), createVersionRequest("kb-1"))
	if err == nil {
		t.Fatal("want a refusal while the storage layer is below quorum")
	}

	// Retryable, not terminal. The verdict is soft state, so a refusal the caller
	// cannot retry would be worse than the attempt it saved.
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("code = %s, want Unavailable", got)
	}
	if got := stratumerrors.ReasonOf(err); got != "kb_storage_degraded" {
		t.Errorf("reason = %q, want kb_storage_degraded", got)
	}
	if !strings.Contains(err.Error(), "below the quorum of 2") {
		t.Errorf("error = %q, want the diagnosis to reach the caller", err.Error())
	}

	// The point of checking before the coordinator: nothing was allocated.
	if calls := h.writeC.Calls(); len(calls) != 0 {
		t.Errorf("WriteCoordinator called %d times; a doomed write must not spend a version number", len(calls))
	}
}

// Unknown ALLOWS (fail-open, §3.3). An empty aggregate — what a leadership change
// looks like — must not become a write outage.
func TestKnowledgeBaseService_CreateVersionAllowsWhenTheVerdictIsUnknown(t *testing.T) {
	h := newKBSvcTestHarness()
	h.svc.SetStorageDegradationSource(&stubStorageGate{known: false})

	resp, err := h.svc.CreateVersion(context.Background(), createVersionRequest("kb-1"))
	if err != nil {
		t.Fatalf("an unknown verdict must allow the write: %v", err)
	}
	if resp.GetVersionId() == 0 {
		t.Error("the write went through, so it must have a version id")
	}
	if calls := h.writeC.Calls(); len(calls) != 1 {
		t.Errorf("WriteCoordinator calls = %d, want 1", len(calls))
	}
}

// The extreme tier gets the other sentinel: "the storage layer is gone" is a
// different sentence from "it is short a quorum", and the two names are what let a
// client tell them apart (docs/storage-degradation-signal-plan.md §4.1). The
// refusal itself is identical — retryable, and the write never reaches the
// coordinator either way.
func TestKnowledgeBaseService_CreateVersionNamesTheClusterTier(t *testing.T) {
	h := newKBSvcTestHarness()
	h.svc.SetStorageDegradationSource(&stubStorageGate{
		unavailable: true,
		detail:      "kb-1: no required replica is live (the quorum is 2); silent: 1 (never reported), 2 (never reported)",
		known:       true,
	})

	_, err := h.svc.CreateVersion(context.Background(), createVersionRequest("kb-1"))
	if err == nil {
		t.Fatal("want a refusal while not one required replica is live")
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("code = %s, want Unavailable (retryable either way)", got)
	}
	if got := stratumerrors.ReasonOf(err); got != "storage_unavailable" {
		t.Errorf("reason = %q, want storage_unavailable", got)
	}
	if !strings.Contains(err.Error(), "no required replica is live") {
		t.Errorf("error = %q, want the cluster-tier diagnosis", err.Error())
	}
	if calls := h.writeC.Calls(); len(calls) != 0 {
		t.Errorf("WriteCoordinator called %d times; the extreme tier must also fail before anything is allocated", len(calls))
	}
}

// Without a gate the service behaves exactly as it did before this existed: every
// existing deployment and test wires no source, and must see no change.
func TestKnowledgeBaseService_CreateVersionUnchangedWithoutAGate(t *testing.T) {
	h := newKBSvcTestHarness()

	if _, err := h.svc.CreateVersion(context.Background(), createVersionRequest("kb-1")); err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}
	if calls := h.writeC.Calls(); len(calls) != 1 {
		t.Errorf("WriteCoordinator calls = %d, want 1", len(calls))
	}
}

// GetSystemStatus inherits the verdict through health, which is where §4.5 puts it.
func TestAdminService_GetSystemStatusSurfacesStorageDegradation(t *testing.T) {
	h := newAdminHarness()
	h.svc.SetStorageDegradationSource(&stubStorageGate{
		degraded: true,
		detail:   "1 of 3 required replicas live, below the quorum of 2; silent: 2 (never reported)",
		known:    true,
	})

	resp, err := h.svc.GetSystemStatus(context.Background(), &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus failed: %v", err)
	}
	if !strings.Contains(resp.GetHealth().GetDetails(), "storage: 1 of 3 required replicas live") {
		t.Errorf("health details = %q, want the storage diagnosis", resp.GetHealth().GetDetails())
	}
}

// HealthCheck reports the verdict in Details and leaves the STATUS alone: a probe
// that turned this node unhealthy would pull traffic off a node whose reads are
// still being served, which is the trade-off §2 explicitly keeps (§4.5).
func TestAdminService_HealthCheckPutsStorageDegradationInDetailsOnly(t *testing.T) {
	h := newAdminHarness()
	h.svc.SetStorageDegradationSource(&stubStorageGate{
		degraded: true,
		detail:   "1 of 3 required replicas live, below the quorum of 2",
		known:    true,
	})

	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}
	if !strings.Contains(resp.GetDetails(), "storage: 1 of 3 required replicas live") {
		t.Errorf("details = %q, want the storage diagnosis", resp.GetDetails())
	}
	if resp.GetStatus() == pb.HealthStatus_HEALTH_STATUS_UNHEALTHY {
		t.Error("storage redundancy must not map to UNHEALTHY: this node still serves reads")
	}
}

// An unknown verdict says nothing, rather than vouching for a storage layer it
// cannot see.
func TestAdminService_HealthCheckSaysNothingWhenTheVerdictIsUnknown(t *testing.T) {
	h := newAdminHarness()
	h.svc.SetStorageDegradationSource(&stubStorageGate{known: false})

	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}
	if strings.Contains(resp.GetDetails(), "storage:") {
		t.Errorf("details = %q; an unknown verdict must report nothing", resp.GetDetails())
	}
}

// The station's write gate reads this RPC, so its three answers have to stay apart:
// healthy, each failure tier, and "this node cannot judge".
func TestKnowledgeBaseService_GetStorageDegradation(t *testing.T) {
	ask := func(t *testing.T, gate StorageDegradationSource) *pb.GetStorageDegradationResponse {
		t.Helper()
		h := newKBSvcTestHarness()
		if gate != nil {
			h.svc.SetStorageDegradationSource(gate)
		}
		resp, err := h.svc.GetStorageDegradation(context.Background(), &pb.GetStorageDegradationRequest{})
		if err != nil {
			t.Fatalf("GetStorageDegradation failed: %v", err)
		}
		return resp
	}

	t.Run("no gate wired is unknown", func(t *testing.T) {
		if ask(t, nil).GetKnown() {
			t.Error("a node with no gate cannot judge")
		}
	})
	t.Run("a node that cannot judge says so", func(t *testing.T) {
		resp := ask(t, &stubStorageGate{known: false, detail: "not the control leader"})
		if resp.GetKnown() || resp.GetDetail() != "" {
			t.Errorf("resp = %+v, want unknown with no detail", resp)
		}
	})
	t.Run("short of a quorum", func(t *testing.T) {
		resp := ask(t, &stubStorageGate{known: true, degraded: true, detail: "1 of 3"})
		if !resp.GetKnown() || !resp.GetDegraded() || resp.GetStorageUnavailable() || resp.GetDetail() != "1 of 3" {
			t.Errorf("resp = %+v, want the degraded tier", resp)
		}
	})
	t.Run("nobody answering", func(t *testing.T) {
		resp := ask(t, &stubStorageGate{known: true, unavailable: true, detail: "none live"})
		if !resp.GetKnown() || resp.GetDegraded() || !resp.GetStorageUnavailable() || resp.GetDetail() != "none live" {
			t.Errorf("resp = %+v, want the unavailable tier", resp)
		}
	})
}
