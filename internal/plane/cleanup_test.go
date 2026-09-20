package plane

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// stubDropper records local cleanups and can fail them.
type stubDropper struct {
	dropped []string
	err     error
}

func (d *stubDropper) DropVersionStorage(_ context.Context, kbID string, versionID int64) error {
	if d.err != nil {
		return d.err
	}
	d.dropped = append(d.dropped, fmt.Sprintf("%s/%d", kbID, versionID))
	return nil
}

var _ VersionDataDropper = (*stubDropper)(nil)

// stubCleaner records which peers were asked to clean up.
type stubCleaner struct {
	targets  []string
	versions []int64
	err      error
}

func (c *stubCleaner) DeleteVersionData(_ context.Context, peerAddr, _ string, versionID int64, _ string) error {
	if c.err != nil {
		return c.err
	}
	c.targets = append(c.targets, peerAddr)
	c.versions = append(c.versions, versionID)
	return nil
}

var _ VersionDataCleaner = (*stubCleaner)(nil)

func newCleanupPlane(dropper VersionDataDropper, cleaner VersionDataCleaner, peers []string) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:       &stubIndexStore{},
		WAL:                &stubWAL{t: tr},
		Executor:           &stubExecutor{t: tr},
		DataDropper:        dropper,
		CleanupBroadcaster: cleaner,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return peers, nil
		},
	})
}

// Cleanup must cover every candidate replica, not just the ones that
// acknowledged the write: the control layer does not know where the data
// actually landed (Stratum_设计文档v13.md §10.6).
func TestLocalDataPlane_DropVersionDataCoversEveryCandidate(t *testing.T) {
	dropper := &stubDropper{}
	cleaner := &stubCleaner{}
	dp := newCleanupPlane(dropper, cleaner, []string{"peer-a", "peer-b", "peer-c"})

	if err := dp.DropVersionData(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("DropVersionData: %v", err)
	}
	if len(cleaner.targets) != 3 {
		t.Fatalf("broadcast to %v, want all three candidates", cleaner.targets)
	}
	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/7" {
		t.Errorf("local drop = %v, want kb-1/7", dropper.dropped)
	}
	for i, v := range cleaner.versions {
		if v != 7 {
			t.Errorf("broadcast %d cleaned version %d, want 7", i, v)
		}
	}
}

// A replica that cannot be reached is reported, but must not stop the others:
// the cleanup is best effort, and the local copy still has to go.
func TestLocalDataPlane_DropVersionDataReportsBroadcastFailuresButKeepsGoing(t *testing.T) {
	dropper := &stubDropper{}
	cleaner := &stubCleaner{err: errors.New("unreachable")}
	dp := newCleanupPlane(dropper, cleaner, []string{"peer-a", "peer-b"})

	err := dp.DropVersionData(context.Background(), "kb-1", 7)
	if err == nil {
		t.Fatal("a failed broadcast must be surfaced so it can be logged")
	}
	if len(dropper.dropped) != 1 {
		t.Errorf("local drop = %v, want it to happen despite the broadcast failure", dropper.dropped)
	}
}

// The cursor advances past a permanently failed version: it will never have
// data, so the gap must not look like something a backfill should fetch.
func TestLocalDataPlane_DropVersionDataAdvancesTheCursor(t *testing.T) {
	dp := newCleanupPlane(&stubDropper{}, &stubCleaner{}, nil)
	dp.advanceLocalVersion("kb-1", 6)

	if err := dp.DropVersionData(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("DropVersionData: %v", err)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 7 {
		t.Errorf("cursor = %d, want 7 (past the dead version)", got)
	}
}

// Without wiring, the failure is explicit rather than a silent no-op: a
// cleanup that quietly does nothing is how orphan data survives.
func TestLocalDataPlane_DropVersionDataWithoutWiring(t *testing.T) {
	dp := newCleanupPlane(nil, nil, nil)

	err := dp.DropVersionData(context.Background(), "kb-1", 7)
	if err == nil || !strings.Contains(err.Error(), "not wired") {
		t.Fatalf("err = %v, want an explicit not-wired error", err)
	}
}

// The terminal verdict is what triggers the cleanup — not merely a failure.
func TestLocalDataPlane_TerminalVerdictTriggersCleanup(t *testing.T) {
	control := &stubControl{terminal: true}
	dropper := &stubDropper{}
	cleaner := &stubCleaner{}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:       &stubIndexStore{},
		WAL:                &stubWAL{t: tr, beginErr: errors.New("disk full")},
		Executor:           &stubExecutor{t: tr},
		Control:            control,
		DataDropper:        dropper,
		CleanupBroadcaster: cleaner,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return []string{"peer-a"}, nil
		},
	})

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil); err == nil {
		t.Fatal("expected the failing write to return an error")
	}
	if len(control.failures) != 1 {
		t.Fatalf("failure reports = %d, want 1", len(control.failures))
	}
	if len(dropper.dropped) != 1 {
		t.Fatalf("local drop = %v, want the cleanup to fire on the terminal verdict", dropper.dropped)
	}
	// No broadcast. §10.6 broadcast because the control layer knew nothing about which
	// replicas had the data; every replica now learns the verdict from its own apply and
	// reclaims locally (see NoteTerminalVersion). This path exists only to be FASTER on
	// the node that noticed the failure.
	if len(cleaner.targets) != 0 {
		t.Fatalf("broadcast = %v, want none: the verdict reaches every replica through its own apply", cleaner.targets)
	}
}

// Before the verdict nothing is reclaimed: the version is still retryable, and
// deleting its data would destroy a write that could still succeed.
func TestLocalDataPlane_NonTerminalFailureDoesNotCleanUp(t *testing.T) {
	control := &stubControl{terminal: false}
	dropper := &stubDropper{}
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr, beginErr: errors.New("disk full")},
		Executor:     &stubExecutor{t: tr},
		Control:      control,
		DataDropper:  dropper,
	})

	if err := dp.WriteVersionData(context.Background(), "kb-1", 7, 6, nil); err == nil {
		t.Fatal("expected the failing write to return an error")
	}
	if len(dropper.dropped) != 0 {
		t.Fatalf("local drop = %v, want none while the version is still retryable", dropper.dropped)
	}
}
