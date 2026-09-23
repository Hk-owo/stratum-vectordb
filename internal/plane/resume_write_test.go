package plane

import (
	"context"
	"strings"
	"testing"
)

// newResumePlane builds a plane whose local transaction succeeds and whose
// replica set is targets, with a control plane that records what it reports.
func newResumePlane(targets []string, pusher VersionPusher, control ControlPlane) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Control:      control,
		Pusher:       pusher,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return targets, nil
		},
	})
}

// Crash recovery runs the same tail as a normal write: no quorum, no durable
// report.
//
// This path used to call reportAndSchedule directly, announcing a version
// durable for replicas that were never written to. Durability is precisely the
// claim the quorum establishes (see finishVersionWrite), and a version resumed
// after a crash must clear the same bar as one that never crashed.
func TestLocalDataPlane_ResumeVersionWriteRequiresQuorum(t *testing.T) {
	control := &stubControl{}
	pusher := &stubPusher{fail: map[string]bool{"peer-a": true, "peer-b": true}}
	dp := newResumePlane([]string{"peer-a", "peer-b"}, pusher, control)

	err := dp.ResumeVersionWrite(context.Background(), "kb-1", 7, 6, nil)
	if err == nil {
		t.Fatal("ResumeVersionWrite must fail when acknowledgements fall below quorum")
	}
	if len(control.durables) != 0 {
		t.Errorf("durable reports = %+v, want none below quorum", control.durables)
	}
	if !strings.Contains(err.Error(), "quorum") {
		t.Errorf("error = %v, want it to name the quorum shortfall", err)
	}
}

// With a full quorum the recovery path reports durable like any other write:
// this change is not "recovery never reports", it is "recovery reports against
// the same standard".
func TestLocalDataPlane_ResumeVersionWriteReportsDurableOnQuorum(t *testing.T) {
	control := &stubControl{}
	dp := newResumePlane([]string{"peer-a", "peer-b"}, &stubPusher{}, control)

	if err := dp.ResumeVersionWrite(context.Background(), "kb-1", 7, 6, nil); err != nil {
		t.Fatalf("ResumeVersionWrite with a full quorum: %v", err)
	}
	if len(control.durables) != 1 {
		t.Fatalf("durable reports = %+v, want exactly one", control.durables)
	}
	if got := control.durables[0]; got.kbID != "kb-1" || got.versionID != 7 {
		t.Errorf("durable report = %+v, want kb-1/7", got)
	}
}

// The two paths must answer the same way for the same scenario. That is the
// whole point of the shared tail: they used to implement it separately, and the
// recovery copy dropped the fan-out.
func TestLocalDataPlane_ResumeAndNormalWriteAgreeOnQuorum(t *testing.T) {
	cases := []struct {
		name        string
		fail        map[string]bool
		wantDurable bool
	}{
		{name: "quorum reached", wantDurable: true},
		{name: "below quorum", fail: map[string]bool{"peer-a": true, "peer-b": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			normal := &stubControl{}
			normalPlane := newResumePlane([]string{"peer-a", "peer-b"}, &stubPusher{fail: tc.fail}, normal)
			normalErr := normalPlane.WriteVersionData(context.Background(), "kb-1", 7, 6, nil)

			resumed := &stubControl{}
			resumePlane := newResumePlane([]string{"peer-a", "peer-b"}, &stubPusher{fail: tc.fail}, resumed)
			resumeErr := resumePlane.ResumeVersionWrite(context.Background(), "kb-1", 7, 6, nil)

			if got := len(normal.durables) == 1; got != tc.wantDurable {
				t.Errorf("normal path reported durable = %v, want %v", got, tc.wantDurable)
			}
			if got := len(resumed.durables) == 1; got != tc.wantDurable {
				t.Errorf("recovery path reported durable = %v, want %v", got, tc.wantDurable)
			}
			if (normalErr == nil) != (resumeErr == nil) {
				t.Errorf("paths disagree on failure: normal=%v resume=%v", normalErr, resumeErr)
			}
		})
	}
}
