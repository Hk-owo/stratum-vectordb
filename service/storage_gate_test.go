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

// stubHolderSource is a VersionHolderSource under the test's control.
type stubHolderSource struct {
	holders []VersionHolder
	ok      bool
}

func (s stubHolderSource) DataVersionHolders(string, int64) ([]VersionHolder, bool) {
	return s.holders, s.ok
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

// The redundancy verdict rides the holders RPC because the station already polls it
// on every route refresh (§4.2) — the station's write gate reads it from there.
func TestKnowledgeBaseService_GetDataVersionHoldersCarriesTheVerdict(t *testing.T) {
	t.Run("a known verdict rides the response", func(t *testing.T) {
		h := newKBSvcTestHarness()
		h.svc.SetVersionHolderSource(stubHolderSource{
			ok:      true,
			holders: []VersionHolder{{NodeID: 1, Address: "10.0.0.1:7000"}},
		})
		h.svc.SetStorageDegradationSource(&stubStorageGate{
			degraded: true,
			detail:   "1 of 3 required replicas live, below the quorum of 2",
			known:    true,
		})

		resp, err := h.svc.GetDataVersionHolders(context.Background(),
			&pb.GetDataVersionHoldersRequest{KnowledgeBaseId: "kb-1", VersionId: 1})
		if err != nil {
			t.Fatalf("GetDataVersionHolders failed: %v", err)
		}
		if !resp.GetDegradationKnown() || !resp.GetDegraded() {
			t.Errorf("degradation = (degraded=%v, known=%v), want (true, true)",
				resp.GetDegraded(), resp.GetDegradationKnown())
		}
		if resp.GetDegradationDetail() != "1 of 3 required replicas live, below the quorum of 2" {
			t.Errorf("detail = %q, want the leader's diagnosis", resp.GetDegradationDetail())
		}
		// The holders half is unaffected: one response, two independent answers.
		if !resp.GetKnown() || len(resp.GetHolders()) != 1 {
			t.Errorf("holders = (%v, %v), want the single reported holder", resp.GetKnown(), resp.GetHolders())
		}
	})

	t.Run("an unknown verdict is not laundered into healthy", func(t *testing.T) {
		h := newKBSvcTestHarness()
		h.svc.SetStorageDegradationSource(&stubStorageGate{known: false})

		resp, err := h.svc.GetDataVersionHolders(context.Background(),
			&pb.GetDataVersionHoldersRequest{KnowledgeBaseId: "kb-1", VersionId: 1})
		if err != nil {
			t.Fatalf("GetDataVersionHolders failed: %v", err)
		}
		if resp.GetDegradationKnown() {
			t.Error("degradation_known must be false when the leader has no verdict")
		}
		if resp.GetDegraded() {
			t.Error("degraded must be false when the verdict is unknown, so a reader of the flag alone fails open")
		}
	})

	t.Run("no gate wired answers no verdict at all", func(t *testing.T) {
		h := newKBSvcTestHarness()

		resp, err := h.svc.GetDataVersionHolders(context.Background(),
			&pb.GetDataVersionHoldersRequest{KnowledgeBaseId: "kb-1", VersionId: 1})
		if err != nil {
			t.Fatalf("GetDataVersionHolders failed: %v", err)
		}
		if resp.GetDegradationKnown() || resp.GetDegraded() || resp.GetStorageUnavailable() {
			t.Error("an unwired gate has no information, which is exactly what degradation_known=false says")
		}
	})

	// The extreme tier travels on the same response so the station can name the
	// refusal the way the control layer does (§4.1) — and its detail has to be the
	// more specific one, or the station would report "1 of 3" for a cluster where
	// nobody answers.
	t.Run("the cluster tier rides along and wins the diagnosis", func(t *testing.T) {
		h := newKBSvcTestHarness()
		h.svc.SetStorageDegradationSource(&stubStorageGate{
			unavailable: true,
			detail:      "no required replica is live (the quorum is 2); silent: 1 (never reported), 2 (never reported)",
			known:       true,
		})

		resp, err := h.svc.GetDataVersionHolders(context.Background(),
			&pb.GetDataVersionHoldersRequest{KnowledgeBaseId: "kb-1", VersionId: 1})
		if err != nil {
			t.Fatalf("GetDataVersionHolders failed: %v", err)
		}
		if !resp.GetDegradationKnown() || !resp.GetStorageUnavailable() {
			t.Errorf("degradation = (known=%v, unavailable=%v), want (true, true)",
				resp.GetDegradationKnown(), resp.GetStorageUnavailable())
		}
		if resp.GetDegraded() {
			t.Error("degraded must stay false: the two tiers are exclusive on the wire")
		}
		if !strings.Contains(resp.GetDegradationDetail(), "no required replica is live") {
			t.Errorf("detail = %q, want the cluster-tier diagnosis", resp.GetDegradationDetail())
		}
	})

	t.Run("the verdict is reported even when the holders half cannot answer", func(t *testing.T) {
		h := newKBSvcTestHarness()
		h.svc.SetVersionHolderSource(stubHolderSource{ok: false}) // not the leader
		h.svc.SetStorageDegradationSource(&stubStorageGate{
			degraded: true,
			detail:   "2 of 3 required replicas live, below the quorum of 2",
			known:    true,
		})

		resp, err := h.svc.GetDataVersionHolders(context.Background(),
			&pb.GetDataVersionHoldersRequest{KnowledgeBaseId: "kb-1", VersionId: 1})
		if err != nil {
			t.Fatalf("GetDataVersionHolders failed: %v", err)
		}
		if resp.GetKnown() {
			t.Error("a non-leader names no holders")
		}
		if !resp.GetDegradationKnown() || !resp.GetDegraded() {
			t.Error("the two halves are independent: a missing holder list must not hide the verdict")
		}
	})
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
