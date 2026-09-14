package service

import (
	"testing"
	"time"

	"stratum/internal/index"
)

// stubGCPressure is a GCPressureReporter with a fixed answer.
type stubGCPressure struct {
	blocked []index.GCPressure
	asked   int
}

func (s *stubGCPressure) BlockedCollections() []index.GCPressure {
	s.asked++
	return s.blocked
}

// TestGcBlockedVersions_MapsTheReport pins the payload: an operator acting on this
// needs the two numbers the decision was made from (how many others are serving,
// how many are required) and the age of the blockage — "blocked since" is what
// separates a moment from a deployment that needs its replica count raised.
func TestGcBlockedVersions_MapsTheReport(t *testing.T) {
	since := time.Unix(1_700_000_000, 0)
	reporter := &stubGCPressure{blocked: []index.GCPressure{{
		KBID:            "kb-1",
		VersionID:       7,
		DeadShare:       0.42,
		OthersServing:   1,
		MinimumRequired: 2,
		Since:           since,
	}}}
	s := &AdminServiceImpl{}
	s.SetGCPressureReporter(reporter)

	got := s.gcBlockedVersions()
	if len(got) != 1 {
		t.Fatalf("gc_blocked_versions = %+v, want exactly one entry", got)
	}
	entry := got[0]
	if entry.GetKbId() != "kb-1" || entry.GetVersionId() != 7 {
		t.Fatalf("entry = %+v, want kb-1 v7", entry)
	}
	if entry.GetDeadShare() != 0.42 {
		t.Fatalf("dead_share = %v, want 0.42", entry.GetDeadShare())
	}
	if entry.GetOthersServing() != 1 || entry.GetMinimumRequired() != 2 {
		t.Fatalf("entry = %+v, want others=1 minimum=2 (that shortfall is the whole reason it is here)",
			entry)
	}
	if entry.GetBlockedSince() != since.Unix() {
		t.Fatalf("blocked_since = %d, want %d", entry.GetBlockedSince(), since.Unix())
	}
	if reporter.asked != 1 {
		t.Fatalf("the reporter was asked %d times, want 1", reporter.asked)
	}
}

// TestGcBlockedVersions_IsEmptyWithoutAReporterOrWithoutBlockedWork: both "no
// index manager on this node" and "nothing is stuck" are normal, and both are the
// same empty answer. Nothing here should be mistaken for a failure.
func TestGcBlockedVersions_IsEmptyWithoutAReporterOrWithoutBlockedWork(t *testing.T) {
	// No reporter: a control node keeps no index manager.
	s := &AdminServiceImpl{}
	if got := s.gcBlockedVersions(); got != nil {
		t.Fatalf("without a reporter the field must be empty, got %+v", got)
	}

	// A reporter with nothing blocked: the normal state of a deployment with slack.
	s.SetGCPressureReporter(&stubGCPressure{})
	if got := s.gcBlockedVersions(); got != nil {
		t.Fatalf("nothing blocked must mean no entries, got %+v", got)
	}
}
