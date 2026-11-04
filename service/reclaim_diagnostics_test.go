package service

import (
	"testing"
)

// TestReclaimBlockedKB_MapsTheDiagnosis pins the payload an operator acts on: which
// knowledge base is stuck, WHICH replica is holding it, and why (§7.5).
func TestReclaimBlockedKB_MapsTheDiagnosis(t *testing.T) {
	entry := reclaimBlockedKB("kb-1", []ReclaimBlockedReplica{
		{Address: "10.0.0.4:7000", Reason: "unreachable"},
		{Address: "local", Reason: "no_cursor_for_kb"},
	})
	if entry.GetKbId() != "kb-1" {
		t.Fatalf("kb_id = %q, want kb-1", entry.GetKbId())
	}
	blockers := entry.GetBlockers()
	if len(blockers) != 2 {
		t.Fatalf("blockers = %+v, want two entries", blockers)
	}
	// Order is the caller's, pinned so that a re-sorting in the mapping layer is a
	// visible change rather than a silent one. The reason strings are a wire contract.
	if blockers[0].GetAddress() != "10.0.0.4:7000" || blockers[0].GetReason() != "unreachable" {
		t.Errorf("blockers[0] = %+v, want the unreachable replica", blockers[0])
	}
	if blockers[1].GetAddress() != "local" || blockers[1].GetReason() != "no_cursor_for_kb" {
		t.Errorf("blockers[1] = %+v, want this node with no cursor", blockers[1])
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
