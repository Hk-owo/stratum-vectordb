package types

import "testing"

// TestIndexStatus_String covers every defined IndexStatus plus an
// out-of-range value, verifying the human-readable names used in logs and
// system status output.
func TestIndexStatus_String(t *testing.T) {
	cases := []struct {
		status IndexStatus
		want   string
	}{
		{IndexStatusPending, "PENDING"},
		{IndexStatusReady, "READY"},
		{IndexStatusFailed, "FAILED"},
		{IndexStatus(42), "UNKNOWN"}, // out-of-range must not panic
		{IndexStatus(-1), "UNKNOWN"},
	}
	for _, tc := range cases {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("IndexStatus(%d).String() = %q, want %q", tc.status, got, tc.want)
		}
	}
}

// TestIndexStatus_Order verifies the iota ordering — PENDING is the zero
// value (default for a freshly allocated version) and READY < FAILED.
func TestIndexStatus_Order(t *testing.T) {
	if IndexStatusPending != 0 {
		t.Errorf("IndexStatusPending = %d, want 0 (default)", IndexStatusPending)
	}
	if !(IndexStatusPending < IndexStatusReady && IndexStatusReady < IndexStatusFailed) {
		t.Errorf("expected PENDING < READY < FAILED, got %d < %d < %d",
			IndexStatusPending, IndexStatusReady, IndexStatusFailed)
	}
}

// TestKBStatus_String covers every defined KBStatus plus out-of-range
// values.
func TestKBStatus_String(t *testing.T) {
	cases := []struct {
		status KBStatus
		want   string
	}{
		{KBStatusActive, "ACTIVE"},
		{KBStatusDeleting, "DELETING"},
		{KBStatusDeleteFailed, "DELETE_FAILED"},
		{KBStatus(99), "UNKNOWN"},
		{KBStatus(-1), "UNKNOWN"},
	}
	for _, tc := range cases {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("KBStatus(%d).String() = %q, want %q", tc.status, got, tc.want)
		}
	}
}

// TestKBStatus_Order verifies the iota ordering — a fresh KB is ACTIVE.
func TestKBStatus_Order(t *testing.T) {
	if KBStatusActive != 0 {
		t.Errorf("KBStatusActive = %d, want 0 (default)", KBStatusActive)
	}
	if !(KBStatusActive < KBStatusDeleting && KBStatusDeleting < KBStatusDeleteFailed) {
		t.Errorf("expected ACTIVE < DELETING < DELETE_FAILED")
	}
}

// TestChangeOp_String covers every defined ChangeOp plus out-of-range
// values.
func TestChangeOp_String(t *testing.T) {
	cases := []struct {
		op   ChangeOp
		want string
	}{
		{ChangeOpAdd, "ADD"},
		{ChangeOpDelete, "DELETE"},
		{ChangeOpUpdate, "UPDATE"},
		{ChangeOp(7), "UNKNOWN"},
		{ChangeOp(-1), "UNKNOWN"},
	}
	for _, tc := range cases {
		if got := tc.op.String(); got != tc.want {
			t.Errorf("ChangeOp(%d).String() = %q, want %q", tc.op, got, tc.want)
		}
	}
}

// TestChangeOp_Order verifies the iota ordering — ADD is the default.
func TestChangeOp_Order(t *testing.T) {
	if ChangeOpAdd != 0 {
		t.Errorf("ChangeOpAdd = %d, want 0 (default)", ChangeOpAdd)
	}
}

// TestAggregationMethod_Order verifies that MEDIAN is the default (0) so
// that an unset proto field falls through to median aggregation.
func TestAggregationMethod_Order(t *testing.T) {
	if AggregationMethodMedian != 0 {
		t.Errorf("AggregationMethodMedian = %d, want 0 (default)", AggregationMethodMedian)
	}
	if !(AggregationMethodMedian < AggregationMethodMax && AggregationMethodMax < AggregationMethodMean) {
		t.Errorf("expected MEDIAN < MAX < MEAN")
	}
}

// TestPendingRecord_Fields verifies PendingRecord carries exactly the
// fields each PendingRecordType needs and that constructing records for
// both recovery scenarios yields the documented values.
func TestPendingRecord_Fields(t *testing.T) {
	verRec := PendingRecord{Type: PendingRecordTypeVersionWrite, VersionID: 5}
	if verRec.KBID != "" {
		t.Errorf("VersionWrite record should not carry KBID, got %q", verRec.KBID)
	}

	delRec := PendingRecord{Type: PendingRecordTypeDeleteMark, KBID: "kb-1"}
	if delRec.VersionID != 0 {
		t.Errorf("DeleteMark record should not carry VersionID, got %d", delRec.VersionID)
	}
}

// TestPendingRecordType_Order verifies DeleteMark is the zero value so
// zero-initialized records read as delete marks (backward compatible with
// the original design-doc enum that only had DeleteMark).
func TestPendingRecordType_Order(t *testing.T) {
	if PendingRecordTypeDeleteMark != 0 {
		t.Errorf("PendingRecordTypeDeleteMark = %d, want 0", PendingRecordTypeDeleteMark)
	}
}

// TestClusterStatus verifies the fields used by GetClusterStatus.
func TestClusterStatus(t *testing.T) {
	cs := ClusterStatus{HasLeader: true, MemberCount: 3, LeaderID: 2}
	if !cs.HasLeader || cs.MemberCount != 3 || cs.LeaderID != 2 {
		t.Errorf("ClusterStatus round-trip failed: %+v", cs)
	}
}

// TestReplayCounter verifies the WAL replay-failure bookkeeping record.
func TestReplayCounter(t *testing.T) {
	rec := PendingRecord{Type: PendingRecordTypeDeleteMark, KBID: "kb"}
	rc := ReplayCounter{Record: rec, RetryCount: 3}
	if rc.Record.KBID != "kb" || rc.RetryCount != 3 {
		t.Errorf("ReplayCounter round-trip failed: %+v", rc)
	}
}

// TestEmbedConfig_ModelIDIsString ensures the field used in chunk-ID
// computation is a plain string (SHA-256(chunk text + ModelID)), not an
// enum that could change meaning across config versions.
func TestEmbedConfig_ModelIDIsString(t *testing.T) {
	cfg := EmbedConfig{ServiceAddr: "http://embed:8080", ModelID: "m1"}
	if cfg.ModelID != "m1" || cfg.ServiceAddr != "http://embed:8080" {
		t.Errorf("EmbedConfig round-trip failed: %+v", cfg)
	}
}
