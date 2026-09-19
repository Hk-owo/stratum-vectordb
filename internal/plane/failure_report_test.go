package plane

import (
	"context"
	"errors"
	"strings"
	"testing"

	"stratum/internal/types"
)

// stubControl is a ControlPlane that records the reports it receives.
type stubControl struct {
	failures []failureReport

	// terminal is what ReportVersionFailure answers, so a test can drive the
	// §10.6 cleanup trigger.
	terminal bool

	// durableErr is what ReportDataDurable answers; nil (the default) means the
	// report succeeded, which is what every other test wants.
	durableErr error
}

// ReclaimableChangesThrough defaults to "unknown": a stub must keep the data,
// because that is the only safe answer when the judgement cannot be made.
func (c *stubControl) ReclaimableChangesThrough(string) (int64, bool) { return 0, false }

type failureReport struct {
	kbID      string
	versionID int64
	reason    string
	class     types.FailureClass
}

func (c *stubControl) ReportDataDurable(context.Context, string, int64, string) error {
	return c.durableErr
}
func (c *stubControl) ReportIndexReady(context.Context, string, int64) error          { return nil }
func (c *stubControl) ReportEpoch(context.Context, uint64, map[string]int64, map[string][]int64) error {
	return nil
}
func (c *stubControl) ReportAvailability(context.Context, string, int64, Availability) error {
	return nil
}

func (c *stubControl) SetFailureBudget(_ context.Context, _ string, _ int) error { return nil }

func (c *stubControl) ReportVersionFailure(_ context.Context, kbID string, versionID int64, side types.FailureSide, class types.FailureClass, detail string) (bool, error) {
	c.failures = append(c.failures, failureReport{kbID: kbID, versionID: versionID, reason: detail, class: class})
	return c.terminal, nil
}

var _ ControlPlane = (*stubControl)(nil)

// newReportingPlane builds a LocalDataPlane whose writes fail, to observe what
// it reports.
func newReportingPlane(control ControlPlane) (*LocalDataPlane, *stubWAL, *stubExecutor) {
	tr := &tracer{}
	w := &stubWAL{t: tr}
	e := &stubExecutor{t: tr}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          w,
		Executor:     e,
		Control:      control,
	}), w, e
}

// A failed local write must be reported: the control layer cannot count
// attempts it never hears about, and without a count it can never reach the
// terminal verdict (Stratum_设计文档v13.md §10.1).
func TestLocalDataPlane_LocalWriteFailureIsReported(t *testing.T) {
	control := &stubControl{}
	dp, w, _ := newReportingPlane(control)
	w.beginErr = errors.New("disk full")

	err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil)
	if err == nil {
		t.Fatal("WriteVersionData succeeded despite a failing WAL")
	}
	if len(control.failures) != 1 {
		t.Fatalf("reports = %+v, want exactly one", control.failures)
	}
	got := control.failures[0]
	if got.kbID != "kb-1" || got.versionID != 7 {
		t.Errorf("report on %s/v%d, want kb-1/v7", got.kbID, got.versionID)
	}
	if !strings.Contains(got.reason, "local write failed") {
		t.Errorf("reason = %q, want it to name the failing stage", got.reason)
	}
}

// A replication failure is a different stage and must be reported as such: the
// cause chain is what tells "the disk is broken" from "the peers are down".
func TestLocalDataPlane_ReplicationFailureIsReported(t *testing.T) {
	control := &stubControl{}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr},
		Control:      control,
		Pusher:       &stubPusher{fail: map[string]bool{"peer-a": true, "peer-b": true}},
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer-a", "peer-b"}, nil
		},
	})

	err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil)
	if err == nil {
		t.Fatal("WriteVersionData succeeded despite a failing quorum")
	}
	if len(control.failures) != 1 {
		t.Fatalf("reports = %+v, want exactly one", control.failures)
	}
	if !strings.Contains(control.failures[0].reason, "replication failed") {
		t.Errorf("reason = %q, want it to name the replication stage", control.failures[0].reason)
	}
}

// Without a control plane the write still fails normally — reporting is a side
// channel, not a precondition.
func TestLocalDataPlane_WriteFailureWithoutControlPlane(t *testing.T) {
	dp, w, _ := newReportingPlane(nil)
	w.beginErr = errors.New("disk full")

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil); err == nil {
		t.Fatal("WriteVersionData succeeded despite a failing WAL")
	}
}
