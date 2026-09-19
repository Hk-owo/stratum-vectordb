package plane

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	stratumerrors "stratum/internal/errors"
)

// The version is gone — discarded by its caller, or deleted — while its write
// path was still finishing. Discarding means "act as if this never existed", so
// the digest report that follows has nowhere to land: expected, not a failure.
//
// It has to stay quiet because this happens on EVERY discard, and the warning in
// the sibling test — the one that says a version will never leave PENDING — is
// what gets buried when this line fires just as loudly.
func TestReportAndSchedule_VersionGoneIsNotAWarning(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	dp := &LocalDataPlane{
		control: &stubControl{durableErr: stratumerrors.ErrVersionNotFound},
		logger:  zap.New(core),
	}

	dp.reportAndSchedule(context.Background(), "kb-1", 7, []string{"doc-1"})

	if n := logs.FilterLevelExact(zap.WarnLevel).Len(); n != 0 {
		t.Errorf("reporting a vanished version produced %d warning(s), want 0 — this happens on every discard", n)
	}
}

// Any other failure still warns, and still names its cause: a version with no
// durable digest carries no evidence of having been replicated, so cursor
// recovery reads it as "not held here" and the data side never leaves PENDING.
// That is the whole reason this line exists.
func TestReportAndSchedule_OtherFailuresStillWarn(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	dp := &LocalDataPlane{
		control: &stubControl{durableErr: errors.New("control plane unreachable")},
		logger:  zap.New(core),
	}

	dp.reportAndSchedule(context.Background(), "kb-1", 7, []string{"doc-1"})

	warns := logs.FilterLevelExact(zap.WarnLevel).All()
	if len(warns) != 1 {
		t.Fatalf("produced %d warning(s), want exactly 1", len(warns))
	}
	if got := warns[0].ContextMap()["error"]; got != "control plane unreachable" {
		t.Errorf("the warning did not name its cause, error = %v", got)
	}
}

// fanOut's empty-target-list case has two meanings and the config carries which
// one applies. With nothing to replicate to (ReplicaCount 1), the local write IS
// the whole quorum — the documented single-node default — so the line must be
// quiet; on a multi-replica deployment the same empty list means the peer list
// failed to build and the other replicas will never receive the version, which
// is worth a warning.
func TestFanOut_EmptyTargetsDependsOnTheReplicaCount(t *testing.T) {
	noReplicas := func(context.Context) ([]string, error) { return nil, nil }

	for _, tc := range []struct {
		name         string
		replicaCount int
		wantWarns    int
	}{
		{name: "single replica (the default)", replicaCount: 1, wantWarns: 0},
		{name: "multi replica", replicaCount: 3, wantWarns: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.DebugLevel)
			dp := &LocalDataPlane{
				pusher:          &stubPusher{},
				resolveReplicas: noReplicas,
				replicaCount:    tc.replicaCount,
				logger:          zap.New(core),
			}

			if err := dp.fanOut(context.Background(), "kb-1", 7, 1); err != nil {
				t.Fatalf("fanOut: %v", err)
			}
			if n := logs.FilterLevelExact(zap.WarnLevel).Len(); n != tc.wantWarns {
				t.Errorf("produced %d warning(s), want %d", n, tc.wantWarns)
			}
		})
	}
}
