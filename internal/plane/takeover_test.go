package plane

import (
	"context"
	"fmt"
	"testing"
	"time"

	"stratum/internal/types"
)

// takeoverControl records the durable reports a takeover produces.
type takeoverControl struct {
	durable []durableReport
}

type durableReport struct {
	kbID      string
	versionID int64
	digest    string
}

func (c *takeoverControl) ReportDataDurable(_ context.Context, kbID string, versionID int64, digest string) error {
	c.durable = append(c.durable, durableReport{kbID: kbID, versionID: versionID, digest: digest})
	return nil
}

func (c *takeoverControl) SetFailureBudget(context.Context, string, int) error { return nil }

func (c *takeoverControl) ReportIndexReady(context.Context, string, int64) error { return nil }
func (c *takeoverControl) ReportEpoch(context.Context, uint64, map[string]int64, map[string][]int64) error {
	return nil
}
func (c *takeoverControl) ReportAvailability(context.Context, string, int64, Availability) error {
	return nil
}
func (c *takeoverControl) ReportVersionFailure(context.Context, string, int64, types.FailureSide, types.FailureClass, string) (bool, error) {
	return false, nil
}

// ReclaimableChangesThrough defaults to "unknown": every stub must keep the data,
// because that is the only safe answer when the judgement cannot be made.
func (c *takeoverControl) ReclaimableChangesThrough(string) (int64, bool) { return 0, false }

var _ ControlPlane = (*takeoverControl)(nil)

// stubPresence answers per-peer "do you hold it?".
type stubPresence struct {
	holds map[string]bool
	err   error
}

func (p *stubPresence) HasVersion(_ context.Context, peerAddr, _ string, _ int64) (bool, error) {
	if p.err != nil {
		return false, p.err
	}
	return p.holds[peerAddr], nil
}

var _ VersionPresenceQuerier = (*stubPresence)(nil)

// stubDigest answers with a fixed digest.
type stubDigest struct {
	digest string
	err    error
}

func (d *stubDigest) DigestOf(context.Context, string, int64) (string, error) {
	return d.digest, d.err
}

var _ VersionDigest = (*stubDigest)(nil)

func newTakeoverPlane(control ControlPlane, presence VersionPresenceQuerier, digest VersionDigest, peers []string) *LocalDataPlane {
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: &tracer{}},
		Executor:     &stubExecutor{t: &tracer{}},
		Control:      control,
		Presence:     presence,
		Digest:       digest,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return peers, nil
		},
	})
}

// shortenTakeover makes the takeover fire promptly, and restores the
// production value afterwards.
func shortenTakeover(t *testing.T, d time.Duration) {
	t.Helper()
	old := takeoverTimeout
	takeoverTimeout = d
	t.Cleanup(func() { takeoverTimeout = old })
}

// The §7.3 path: the coordinator went quiet, a quorum holds the version, so
// this replica announces it — with the digest it computed itself, because
// followers will verify their pulls against exactly that value.
func TestLocalDataPlane_TakeoverAnnouncesWhenAQuorumHoldsTheVersion(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control,
		&stubPresence{holds: map[string]bool{"peer-a": true, "peer-b": false}},
		&stubDigest{digest: "digest-from-this-node"},
		[]string{"peer-a", "peer-b"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	deadline := time.Now().Add(2 * time.Second)
	for len(control.durable) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if len(control.durable) != 1 {
		t.Fatalf("durable reports = %+v, want exactly one", control.durable)
	}
	got := control.durable[0]
	if got.kbID != "kb-1" || got.versionID != 7 {
		t.Errorf("announced %s v%d, want kb-1 v7", got.kbID, got.versionID)
	}
	if got.digest != "digest-from-this-node" {
		t.Errorf("digest = %q, want the locally computed one", got.digest)
	}
}

// A minority is not enough: claiming durability it cannot back is worse than
// saying nothing, so the version is left to its retry budget (§10.1).
func TestLocalDataPlane_TakeoverStaysSilentWithoutAQuorum(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control,
		&stubPresence{holds: map[string]bool{"peer-a": false, "peer-b": false}},
		&stubDigest{digest: "d"},
		[]string{"peer-a", "peer-b"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	time.Sleep(200 * time.Millisecond)

	if len(control.durable) != 0 {
		t.Fatalf("durable reports = %+v, want none on a minority", control.durable)
	}
}

// An unreachable peer is not evidence either way: it goes uncounted, and what
// remains can still be a quorum.
func TestLocalDataPlane_TakeoverIgnoresUnreachablePeers(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	// peer-a is unreachable, peer-b answers "yes". Self + peer-b = 2 of 3, a
	// quorum — the silence of peer-a says nothing about peer-b.
	dp := newTakeoverPlane(control,
		&partialPresence{down: map[string]bool{"peer-a": true}, holds: map[string]bool{"peer-b": true}},
		&stubDigest{digest: "d"},
		[]string{"peer-a", "peer-b"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	deadline := time.Now().Add(2 * time.Second)
	for len(control.durable) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if len(control.durable) != 1 {
		t.Fatalf("durable reports = %+v, want one: peer-b's answer is enough for a quorum", control.durable)
	}
}

// partialPresence fails for selected peers and answers for the rest.
type partialPresence struct {
	down  map[string]bool
	holds map[string]bool
}

func (p *partialPresence) HasVersion(_ context.Context, peerAddr, _ string, _ int64) (bool, error) {
	if p.down[peerAddr] {
		return false, fmt.Errorf("peer %s unreachable", peerAddr)
	}
	return p.holds[peerAddr], nil
}

// The coordinator's confirmation is what makes the timer unnecessary: once it
// arrives, nothing should be announced.
func TestLocalDataPlane_ConfirmStandsDownTheTakeover(t *testing.T) {
	shortenTakeover(t, 50*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control,
		&stubPresence{holds: map[string]bool{"peer-a": true}},
		&stubDigest{digest: "d"},
		[]string{"peer-a"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	dp.ConfirmVersionWrite("kb-1", 7)
	time.Sleep(300 * time.Millisecond)

	if len(control.durable) != 0 {
		t.Fatalf("durable reports = %+v, want none after the confirmation", control.durable)
	}
}

// Confirming a version nobody is watching is a no-op, not a panic or a stray
// announcement: the coordinator broadcasts to every candidate, not just the
// replicas that acknowledged (§10.6 reasoning).
func TestLocalDataPlane_ConfirmWithoutWatchIsHarmless(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control, &stubPresence{}, &stubDigest{}, []string{"peer-a"})

	dp.ConfirmVersionWrite("kb-1", 7)
	time.Sleep(50 * time.Millisecond)

	if len(control.durable) != 0 {
		t.Fatalf("durable reports = %+v, want none", control.durable)
	}
}

// Without the takeover wiring there is no timer at all: the node behaves
// exactly as before, relying on the client-retry path.
func TestLocalDataPlane_WatchWithoutWiringIsANoOp(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: &tracer{}},
		Executor:     &stubExecutor{t: &tracer{}},
	})

	dp.WatchVersionWrite("kb-1", 7)
	dp.takeoverMu.Lock()
	n := len(dp.pendingTakeovers)
	dp.takeoverMu.Unlock()
	if n != 0 {
		t.Errorf("pending takeovers = %d, want 0 without the wiring", n)
	}
}

// A second delivery of the same version replaces the timer instead of stacking
// one: taking delivery twice must not produce two announcements.
func TestLocalDataPlane_WatchIsIdempotentPerVersion(t *testing.T) {
	shortenTakeover(t, 30*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control,
		&stubPresence{holds: map[string]bool{"peer-a": true}},
		&stubDigest{digest: "d"},
		[]string{"peer-a"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	dp.WatchVersionWrite("kb-1", 7)
	time.Sleep(300 * time.Millisecond)

	if len(control.durable) != 1 {
		t.Fatalf("durable reports = %+v, want exactly one announcement", control.durable)
	}
}
