package router

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stratumerrors "stratum/internal/errors"
)

// tableWithVerdicts takes one successful snapshot carrying the given verdicts,
// which is the state the background refresh leaves behind.
func tableWithVerdicts(verdicts map[string]degradationVerdict) *RouteTable {
	table := NewRouteTable(nil, time.Minute, nil)
	table.mu.Lock()
	table.snapshot = routeSnapshot{
		servable:    map[string]map[int]bool{},
		expected:    map[string]int64{},
		degradation: verdicts,
	}
	table.updated = time.Now()
	table.ready = true
	table.mu.Unlock()
	return table
}

// The station's half of the gate (§4.3): a write the snapshot already knows cannot
// reach quorum is refused before the control-layer round trip that would find out
// the same thing more slowly.
func TestRouter_WriteGateRefusesWhenTheSnapshotSaysDegraded(t *testing.T) {
	r := &Router{routes: tableWithVerdicts(map[string]degradationVerdict{
		"kb-1": {degraded: true, detail: "1 of 3 required replicas live, below the quorum of 2"},
	})}

	err := r.writeGate("kb-1")
	if err == nil {
		t.Fatal("want a refusal for a KB the snapshot reports below quorum")
	}
	// Retryable, because the snapshot lags reality in both directions.
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("code = %s, want Unavailable", got)
	}
	if got := stratumerrors.ReasonOf(err); got != "kb_storage_degraded" {
		t.Errorf("reason = %q, want kb_storage_degraded", got)
	}
	if !strings.Contains(err.Error(), "below the quorum of 2") {
		t.Errorf("error = %q, want the leader's diagnosis to reach the caller", err.Error())
	}
}

// The station names the extreme tier the same way the control layer would, so a
// client sees one error for one fault whichever gate caught it (§4.1).
func TestRouter_WriteGateNamesTheClusterTier(t *testing.T) {
	r := &Router{routes: tableWithVerdicts(map[string]degradationVerdict{
		"kb-1": {unavailable: true, detail: "no required replica is live (the quorum is 2)"},
	})}

	err := r.writeGate("kb-1")
	if err == nil {
		t.Fatal("want a refusal when not one required replica is live")
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("code = %s, want Unavailable", got)
	}
	if got := stratumerrors.ReasonOf(err); got != "storage_unavailable" {
		t.Errorf("reason = %q, want storage_unavailable — the same name the control layer gives this fault", got)
	}
	if !strings.Contains(err.Error(), "no required replica is live") {
		t.Errorf("error = %q, want the diagnosis", err.Error())
	}
}

// Everything the station cannot judge allows. The snapshot is periodic and carries
// soft state, so "cannot tell" must fall in the harmless direction or a failover
// becomes a write outage (§3.3).
func TestRouter_WriteGateAllowsEverythingItCannotJudge(t *testing.T) {
	table := tableWithVerdicts(map[string]degradationVerdict{
		"kb-1":  {degraded: true, detail: "1 of 3 required replicas live"},
		"kb-ok": {degraded: false, detail: "3 of 3 required replicas live, quorum is 2"},
	})

	cases := map[string]struct {
		router *Router
		kbID   string
	}{
		"a healthy verdict":           {&Router{routes: table}, "kb-ok"},
		"a KB the snapshot never saw": {&Router{routes: table}, "kb-never-seen"},
		"no route table at all":       {&Router{}, "kb-1"},
		"no knowledge base named":     {&Router{routes: table}, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := tc.router.writeGate(tc.kbID); err != nil {
				t.Errorf("writeGate(%q) = %v, want nil", tc.kbID, err)
			}
		})
	}
}

// A table that has not completed a refresh holds no verdicts, which is what a
// station has during the moment it takes to come up.
func TestRouter_WriteGateAllowsBeforeTheFirstRefresh(t *testing.T) {
	r := &Router{routes: NewRouteTable(nil, time.Minute, nil)}
	if err := r.writeGate("kb-1"); err != nil {
		t.Errorf("writeGate before the first refresh = %v, want nil", err)
	}
}

// The verdict reaches the gate through the snapshot, and only a KNOWN verdict is
// recorded: a missing entry is what keeps the gate open.
func TestRouteTable_Degradation(t *testing.T) {
	table := tableWithVerdicts(map[string]degradationVerdict{
		"kb-1": {degraded: true, detail: "1 of 3 required replicas live"},
	})

	verdict, known := table.Degradation("kb-1")
	if !known || !verdict.degraded || verdict.unavailable || verdict.detail != "1 of 3 required replicas live" {
		t.Errorf("Degradation(kb-1) = (%+v, %v), want the degraded verdict", verdict, known)
	}
	if !verdict.unhealthy() {
		t.Error("a KB short of quorum cannot be written")
	}

	if _, known := table.Degradation("kb-other"); known {
		t.Error("a KB the snapshot never saw has no verdict, and absence is not a clean bill of health")
	}
}

// The two tiers travel as two fields, and both are "cannot write" — the write gate
// is what turns the difference into a different sentinel.
func TestRouteTable_VerdictTiers(t *testing.T) {
	table := tableWithVerdicts(map[string]degradationVerdict{
		"kb-gone": {unavailable: true, detail: "no required replica is live (the quorum is 2)"},
	})

	verdict, known := table.Degradation("kb-gone")
	if !known {
		t.Fatal("the snapshot carries a verdict for this KB")
	}
	if verdict.degraded {
		t.Error("degraded must stay false when nobody at all is answering: the tiers are exclusive on the wire")
	}
	if !verdict.unavailable || !verdict.unhealthy() {
		t.Errorf("verdict = %+v, want the unavailable tier and unhealthy", verdict)
	}

	// The summary is the "is anything wrong" answer, so either tier has to satisfy
	// it — a read-only client asking about the storage layer does not care which.
	unhealthy, detail, known := table.DegradationSummary()
	if !known || !unhealthy || detail == "" {
		t.Errorf("DegradationSummary = (%v, %q, %v), want the unavailable tier reported as unhealthy", unhealthy, detail, known)
	}
}
