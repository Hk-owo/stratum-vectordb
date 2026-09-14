package plane

import (
	"context"
	"errors"
	"testing"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// A globally fatal failure does not spend the retry budget: retrying cannot
// help, so waiting would only delay the verdict and keep the version PENDING
// meanwhile (Stratum_设计文档v13.md §10.1).
func TestLocalControlPlane_FatalFailureShortCircuitsTheBudget(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(5))

	terminal, err := cp.ReportVersionFailure(context.Background(), "kb-1", 7,
		types.FailureFatalGlobal, "knowledge base is deleted")
	if err != nil {
		t.Fatalf("ReportVersionFailure: %v", err)
	}
	if !terminal {
		t.Fatal("the first fatal report must produce the terminal verdict")
	}
	if len(meta.permanentCalls) != 1 {
		t.Fatalf("permanent calls = %+v, want exactly one", meta.permanentCalls)
	}
	if got := meta.permanentCalls[0]; got.count != 1 {
		t.Errorf("recorded count = %d, want 1 (the attempt that was fatal)", got.count)
	}
	if got := meta.permanentCalls[0].reason; got != "knowledge base is deleted" {
		t.Errorf("recorded reason = %q, want the reported detail", got)
	}
}

// Transient failures still have to earn the verdict: the short-circuit is for
// the unrecoverable case only.
func TestLocalControlPlane_TransientFailureStillCounts(t *testing.T) {
	meta := &stubMeta{}
	cp := NewLocalControlPlane(meta, WithFailureBudget(3))

	for i := 1; i <= 2; i++ {
		terminal, err := cp.ReportVersionFailure(context.Background(), "kb-1", 7,
			types.FailureTransient, "disk full")
		if err != nil {
			t.Fatal(err)
		}
		if terminal {
			t.Fatalf("transient report %d produced the terminal verdict early", i)
		}
	}
	if len(meta.permanentCalls) != 0 {
		t.Fatalf("verdicts = %+v, want none inside the budget", meta.permanentCalls)
	}
}

// The classification is the storage layer's job, and only unrecoverable causes
// may be called fatal — everything else deserves another attempt, possibly on
// another node.
func TestClassifyLocalWriteFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want types.FailureClass
	}{
		{"knowledge base gone", stratumerrors.ErrKnowledgeBaseNotFound, types.FailureFatalGlobal},
		{"knowledge base deleted", stratumerrors.ErrKnowledgeBaseDeleted, types.FailureFatalGlobal},
		{"rejected input", stratumerrors.ErrInvalidArgument, types.FailureFatalGlobal},
		{"invalid parent", stratumerrors.ErrInvalidParentVersion, types.FailureFatalGlobal},
		{"version vanished", stratumerrors.ErrVersionNotFound, types.FailureFatalGlobal},
		{"wrapped fatal", errors.Join(errors.New("ctx"), stratumerrors.ErrKnowledgeBaseDeleted), types.FailureFatalGlobal},
		{"disk full", errors.New("disk full"), types.FailureTransient},
		{"nil", nil, types.FailureTransient},
	}
	for _, tc := range cases {
		if got := classifyLocalWriteFailure(tc.err); got != tc.want {
			t.Errorf("%s: classify(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// The classification has to survive the trip to the control layer: a local
// write that failed because the knowledge base is gone must arrive as fatal,
// not as one more transient failure.
func TestLocalDataPlane_FatalLocalWriteIsReportedAsFatal(t *testing.T) {
	control := &stubControl{}
	dp, w, _ := newReportingPlane(control)
	w.beginErr = stratumerrors.ErrKnowledgeBaseDeleted

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil); err == nil {
		t.Fatal("expected the write to fail")
	}
	if len(control.failures) != 1 {
		t.Fatalf("reports = %+v, want exactly one", control.failures)
	}
	if got := control.failures[0].class; got != types.FailureFatalGlobal {
		t.Errorf("reported class = %v, want FATAL_GLOBAL", got)
	}
}

// A replication shortfall is the opposite case: peers come back, so it must
// stay transient and let the budget decide.
func TestLocalDataPlane_ReplicationFailureIsReportedAsTransient(t *testing.T) {
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

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil); err == nil {
		t.Fatal("expected the write to fail")
	}
	if len(control.failures) != 1 {
		t.Fatalf("reports = %+v, want exactly one", control.failures)
	}
	if got := control.failures[0].class; got != types.FailureTransient {
		t.Errorf("reported class = %v, want TRANSIENT", got)
	}
}
