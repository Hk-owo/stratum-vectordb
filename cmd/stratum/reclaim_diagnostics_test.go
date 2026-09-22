package main

import (
	"strings"
	"testing"
	"time"

	"stratum/internal/plane"
)

// TestDescribeReclaimBlockers_RendersWhatAnOperatorActsOn pins the warning line's shape.
//
// It matters more than a log format usually would: on a control-role node in a split
// deployment there is no GetSystemStatus to read, so this line is where the diagnosis
// reaches an operator at all. What it has to carry is therefore the actionable part —
// which node, and how long it has been quiet.
func TestDescribeReclaimBlockers_RendersWhatAnOperatorActsOn(t *testing.T) {
	got := describeReclaimBlockers("kb-1", []plane.ReclaimBlocker{
		{NodeID: 4, Reason: plane.ReclaimBlockerNeverReported},
		{
			NodeID: 2, Reason: plane.ReclaimBlockerStale, Reached: 9,
			ReportedAt: time.Now().Add(-90 * time.Second),
		},
		{NodeID: 3, Reason: plane.ReclaimBlockerNoCursor, ReportedAt: time.Now()},
	})
	for _, want := range []string{
		"kb-1",
		"node 4: has never reported",
		"node 2: last report 1m30s ago (cursor 9)",
		"node 3: reports, but never reported a cursor for this knowledge base",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("description %q does not contain %q", got, want)
		}
	}
}

// An unknown reason still says which node, rather than dropping the entry: a newer plane
// may learn a reason this binary has never heard of, and "node 5: <something new>" is
// still an operator's starting point.
func TestDescribeReclaimBlockers_KeepsAnUnknownReason(t *testing.T) {
	got := describeReclaimBlockers("kb-1", []plane.ReclaimBlocker{
		{NodeID: 5, Reason: "some_future_reason"},
	})
	if !strings.Contains(got, "node 5: some_future_reason") {
		t.Errorf("description %q dropped a reason it did not recognise", got)
	}
}
