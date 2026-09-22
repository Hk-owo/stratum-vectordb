package service

import (
	"testing"
	"time"
)

// TestReclaimBlockedKB_MapsTheDiagnosis pins the payload an operator acts on: which
// knowledge base is stuck, WHICH required replica is holding it, why, and how long that
// replica has been quiet. The bare watermark answer ("unknown") carries none of it, which
// is the whole reason this field exists (§7.5).
func TestReclaimBlockedKB_MapsTheDiagnosis(t *testing.T) {
	reportedAgo := 42 * time.Second
	reportedAt := time.Now().Add(-reportedAgo)

	entry := reclaimBlockedKB("kb-1", []ReclaimBlockedReplica{
		{NodeID: 4, Reason: "never_reported"},
		{NodeID: 2, Reason: "stale", Reached: 9, ReportedAt: reportedAt},
	})
	if entry.GetKbId() != "kb-1" {
		t.Fatalf("kb_id = %q, want kb-1", entry.GetKbId())
	}
	blockers := entry.GetBlockers()
	if len(blockers) != 2 {
		t.Fatalf("blockers = %+v, want two entries", blockers)
	}
	// Order is the caller's (the plane reports in topology order), pinned so that a
	// re-sorting in the mapping layer is a visible change rather than a silent one.
	if blockers[0].GetNodeId() != 4 || blockers[0].GetReason() != "never_reported" {
		t.Errorf("blockers[0] = %+v, want node 4 never_reported", blockers[0])
	}
	if got := blockers[0].GetLastReportAgeMs(); got != -1 {
		t.Errorf("never-reported age = %d, want -1: there is no report to age", got)
	}
	if blockers[1].GetNodeId() != 2 || blockers[1].GetReachedVersion() != 9 {
		t.Errorf("blockers[1] = %+v, want node 2 at cursor 9", blockers[1])
	}
	if got, want := blockers[1].GetLastReportAgeMs(), reportedAgo.Milliseconds(); got < want {
		t.Errorf("stale age = %dms, want at least %dms: the age is what says how long the "+
			"knowledge base has been stuck", got, want)
	}
	// The reason strings are a wire contract: whatever reads them keys off these names.
	if got := blockers[1].GetReason(); got != "stale" {
		t.Errorf("reason = %q, want the stable wire name %q", got, "stale")
	}
}

// Nothing blocked maps to NO entry at all, so a healthy fleet does not carry a row per
// knowledge base — the list only ever names what is actually stuck.
func TestReclaimBlockedKB_NothingBlockedIsNoEntry(t *testing.T) {
	if entry := reclaimBlockedKB("kb-1", nil); entry != nil {
		t.Errorf("entry = %+v, want nil when nothing blocks the knowledge base", entry)
	}
	if entry := reclaimBlockedKB("kb-1", []ReclaimBlockedReplica{}); entry != nil {
		t.Errorf("entry = %+v, want nil for an empty (not nil) blocker list too", entry)
	}
}
