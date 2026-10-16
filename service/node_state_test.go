package service

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
	"stratum/internal/nodestate"
)

// nodeStateStub stands in for the storage node's state machine, so the admin
// surface's mapping onto the wire can be pinned without standing up a machine.
// It is also the reason the service declares NodeStateSource instead of
// depending on the machine's construction.
type nodeStateStub struct {
	snap     nodestate.NodeSnapshot
	versions []nodestate.VersionEntry
}

func (s nodeStateStub) Snapshot() nodestate.NodeSnapshot { return s.snap }

func (s nodeStateStub) NonSteadyVersions() []nodestate.VersionEntry { return s.versions }

// TestAdmin_GetSystemStatus_SurfacesNodeState: the node's own state reaches the
// status response, with the version layer's not-at-rest entries alongside it.
func TestAdmin_GetSystemStatus_SurfacesNodeState(t *testing.T) {
	h := newAdminHarness()
	entered := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	h.svc.SetNodeStateSource(nodeStateStub{
		snap: nodestate.NodeSnapshot{
			State:            nodestate.NodeServing,
			Since:            entered,
			Redundancy:       nodestate.RedundancyDegraded,
			RedundancyDetail: "2 of 3 required replicas live",
			Actions:          []string{"building 1 index(es)"},
		},
		versions: []nodestate.VersionEntry{
			{
				KBID:      "kb-1",
				VersionID: 7,
				State:     nodestate.VersionBuildFailed,
				Since:     entered,
				Detail:    "vecstore: connection refused",
			},
		},
	})

	resp, err := h.svc.GetSystemStatus(context.Background(), &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}

	if resp.NodeState == nil {
		t.Fatal("node_state is absent, want the state machine's phase")
	}
	if resp.NodeState.State != "SERVING" {
		t.Errorf("state = %q, want SERVING", resp.NodeState.State)
	}
	if resp.NodeState.Since != entered.Format(time.RFC3339) {
		t.Errorf("since = %q, want %q", resp.NodeState.Since, entered.Format(time.RFC3339))
	}
	if resp.NodeState.Redundancy != "DEGRADED" {
		t.Errorf("redundancy = %q, want DEGRADED", resp.NodeState.Redundancy)
	}
	if resp.NodeState.RedundancyDetail != "2 of 3 required replicas live" {
		t.Errorf("redundancy_detail = %q, want the machine's cause", resp.NodeState.RedundancyDetail)
	}
	if len(resp.NodeState.Actions) != 1 || resp.NodeState.Actions[0] != "building 1 index(es)" {
		t.Errorf("actions = %v, want [building 1 index(es)]", resp.NodeState.Actions)
	}

	if len(resp.VersionStates) != 1 {
		t.Fatalf("version_states = %+v, want 1 entry", resp.VersionStates)
	}
	got := resp.VersionStates[0]
	if got.KbId != "kb-1" || got.VersionId != 7 || got.State != "BUILD_FAILED" {
		t.Errorf("version state = %+v, want kb-1 v7 BUILD_FAILED", got)
	}
	if got.Detail != "vecstore: connection refused" {
		t.Errorf("version state detail = %q, want the build's cause", got.Detail)
	}
	if got.Since != entered.Format(time.RFC3339) {
		t.Errorf("version state since = %q, want %q", got.Since, entered.Format(time.RFC3339))
	}
}

// TestAdmin_GetSystemStatus_OmitsNodeStateWhenUnwired: a control node keeps no
// storage and so no state machine. The field must be ABSENT rather than
// claiming a phase — and an empty version list is the normal answer, not a
// signal.
func TestAdmin_GetSystemStatus_OmitsNodeStateWhenUnwired(t *testing.T) {
	h := newAdminHarness()

	resp, err := h.svc.GetSystemStatus(context.Background(), &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	if resp.NodeState != nil {
		t.Errorf("node_state = %+v, want absent without a state machine", resp.NodeState)
	}
	if len(resp.VersionStates) != 0 {
		t.Errorf("version_states = %+v, want empty without a state machine", resp.VersionStates)
	}
}

// TestAdmin_HealthCheck_ReportsNodePhase: a node still reconciling says so, and
// the STATUS is untouched — the phase line is for whoever is diagnosing, while
// turning it into UNHEALTHY would have a load balancer pull traffic off a node
// that is answering reads.
func TestAdmin_HealthCheck_ReportsNodePhase(t *testing.T) {
	h := newAdminHarness()
	h.svc.SetNodeStateSource(nodeStateStub{
		snap: nodestate.NodeSnapshot{
			State:      nodestate.NodeReconciling,
			Redundancy: nodestate.RedundancyUnknown,
			Actions:    []string{"reconciling indexes"},
		},
	})

	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if resp.Status != pb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Errorf("status = %v, want HEALTHY: the phase is not a health verdict", resp.Status)
	}
	if !strings.Contains(resp.Details, "node: RECONCILING") {
		t.Errorf("details = %q, want it to name the RECONCILING phase", resp.Details)
	}
	if strings.Contains(resp.Details, "redundancy") {
		t.Errorf("details = %q, want no redundancy line for UNKNOWN", resp.Details)
	}
}

// TestAdmin_HealthCheck_RedundancyVerdictOnlyWhenItIsOne: DEGRADED is reported
// with its cause; UNKNOWN is not reported at all. Unknown is what every
// non-leader answers, and printing it would put a line on every one of them —
// which is exactly the noise that hides a real degradation.
func TestAdmin_HealthCheck_RedundancyVerdictOnlyWhenItIsOne(t *testing.T) {
	h := newAdminHarness()
	h.svc.SetNodeStateSource(nodeStateStub{
		snap: nodestate.NodeSnapshot{
			State:            nodestate.NodeServing,
			Redundancy:       nodestate.RedundancyDegraded,
			RedundancyDetail: "2 of 3 required replicas live",
		},
	})

	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if !strings.Contains(resp.Details, "node redundancy: DEGRADED") {
		t.Errorf("details = %q, want it to name the DEGRADED verdict", resp.Details)
	}
	if !strings.Contains(resp.Details, "2 of 3 required replicas live") {
		t.Errorf("details = %q, want the verdict's cause", resp.Details)
	}
	if strings.Contains(resp.Details, "node: ") {
		t.Errorf("details = %q, want no phase line while SERVING", resp.Details)
	}
}

// TestAdmin_HealthCheck_QuietWhenServingAndRedundancyUnknown: the ordinary
// non-leader, serving with no verdict to offer, must produce the same "ok" it
// did before the state machine existed.
func TestAdmin_HealthCheck_QuietWhenServingAndRedundancyUnknown(t *testing.T) {
	h := newAdminHarness()
	h.svc.SetNodeStateSource(nodeStateStub{
		snap: nodestate.NodeSnapshot{
			State:      nodestate.NodeServing,
			Redundancy: nodestate.RedundancyUnknown,
		},
	})

	resp, err := h.svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if resp.Details != "ok" {
		t.Errorf("details = %q, want %q", resp.Details, "ok")
	}
}
