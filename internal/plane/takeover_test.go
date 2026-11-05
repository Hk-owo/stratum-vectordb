package plane

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// takeoverControl records the durable reports a takeover produces.
//
// The lock is not decoration: the reports arrive on the takeover timer's own
// goroutine (LocalDataPlane.WatchVersionWrite's timer), while the tests read
// them from the test goroutine. Without it every case below is a data race on
// the slice — measured with -race on the tests that poll for an announcement.
type takeoverControl struct {
	mu      sync.Mutex
	durable []durableReport
	// durableErr makes every report fail, so the takeover's handling of a refusal
	// (a version the caller discarded meanwhile) can be asserted.
	durableErr error
}

type durableReport struct {
	kbID      string
	versionID int64
	digest    string
}

func (c *takeoverControl) ReportDataDurable(_ context.Context, kbID string, versionID int64, digest string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.durable = append(c.durable, durableReport{kbID: kbID, versionID: versionID, digest: digest})
	return c.durableErr
}

// count is how many announcements have landed so far — the polling condition
// the tests wait on.
func (c *takeoverControl) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.durable)
}

// reports returns a copy of the announcements, so a test can assert on a stable
// snapshot rather than on a slice the timer may still be appending to.
func (c *takeoverControl) reports() []durableReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]durableReport(nil), c.durable...)
}

func (c *takeoverControl) SetFailureBudget(context.Context, string, int) error { return nil }

func (c *takeoverControl) ReportIndexReady(context.Context, string, int64) error { return nil }
func (c *takeoverControl) ReportEpoch(context.Context, uint64, map[string]int64, map[string][]int64) error {
	return nil
}
func (c *takeoverControl) ReportVersionFailure(context.Context, string, int64, types.FailureSide, types.FailureClass, string) (bool, error) {
	return false, nil
}

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

// waitForAnnouncements blocks until want announcements have landed, so a case
// asserts on a snapshot instead of polling the control directly.
func waitForAnnouncements(t *testing.T, c *takeoverControl, want int) []durableReport {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.count() < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return c.reports()
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
	reports := waitForAnnouncements(t, control, 1)

	if len(reports) != 1 {
		t.Fatalf("durable reports = %+v, want exactly one", reports)
	}
	got := reports[0]
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

	if reports := control.reports(); len(reports) != 0 {
		t.Fatalf("durable reports = %+v, want none on a minority", reports)
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
	reports := waitForAnnouncements(t, control, 1)

	if len(reports) != 1 {
		t.Fatalf("durable reports = %+v, want one: peer-b's answer is enough for a quorum", reports)
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

	if reports := control.reports(); len(reports) != 0 {
		t.Fatalf("durable reports = %+v, want none after the confirmation", reports)
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

	if reports := control.reports(); len(reports) != 0 {
		t.Fatalf("durable reports = %+v, want none", reports)
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
	// The replaced timer fires at 30ms; waiting well past that gives it every
	// chance to also announce, so "exactly one" is a real assertion rather than
	// a race won by the fast path.
	time.Sleep(300 * time.Millisecond)

	if reports := control.reports(); len(reports) != 1 {
		t.Fatalf("durable reports = %+v, want exactly one announcement", reports)
	}
}

// --- the catch-up path arms the same check (docs/known-gaps.md §K) ----------

// stubDataStatus answers the local metadata read that lets a takeover skip a
// version whose data side is already settled.
type stubDataStatus struct {
	status map[int64]types.DataStatus
	known  bool
}

func (s *stubDataStatus) DataStatusOf(_ context.Context, _ string, versionID int64) (types.DataStatus, bool) {
	if !s.known {
		return types.DataStatusPending, false
	}
	st, ok := s.status[versionID]
	return st, ok
}

var _ VersionDataStatusQuerier = (*stubDataStatus)(nil)

// newTakeoverPlaneWithStatus is newTakeoverPlane plus the optional local
// metadata read.
func newTakeoverPlaneWithStatus(control ControlPlane, presence VersionPresenceQuerier, digest VersionDigest,
	status VersionDataStatusQuerier, peers []string,
) *LocalDataPlane {
	dp := newTakeoverPlane(control, presence, digest, peers)
	dp.dataStatus = status
	return dp
}

// A version whose data side is already settled must not be announced again. The
// report would be harmless — the state machine accepts a repeated digest for a
// DURABLE version — but pointless: a Raft proposal plus one presence RPC per
// peer to re-state a settled fact, on a path that runs for every version this
// node ever pulls.
func TestLocalDataPlane_TakeoverLeavesASettledVersionAlone(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlaneWithStatus(control,
		&stubPresence{holds: map[string]bool{"peer-a": true, "peer-b": true}},
		&stubDigest{digest: "d"},
		&stubDataStatus{known: true, status: map[int64]types.DataStatus{7: types.DataStatusDurable}},
		[]string{"peer-a", "peer-b"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	time.Sleep(200 * time.Millisecond)

	if reports := control.reports(); len(reports) != 0 {
		t.Fatalf("durable reports = %+v, want none: the data side is already durable", reports)
	}
}

// "Cannot read the status" is not "settled": the announcement still happens.
// Forgetting one strands the version in PENDING, while an unnecessary one costs
// a single idempotent proposal.
func TestLocalDataPlane_TakeoverAnnouncesWhenTheStatusCannotBeRead(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlaneWithStatus(control,
		&stubPresence{holds: map[string]bool{"peer-a": true}},
		&stubDigest{digest: "d"},
		&stubDataStatus{known: false},
		[]string{"peer-a"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	reports := waitForAnnouncements(t, control, 1)

	if len(reports) != 1 {
		t.Fatalf("durable reports = %+v, want one: an unreadable status must not silence the announcement", reports)
	}
}

// flipPresence answers "not held" for its first `left` calls, then `holds`.
//
// It stands in for the case the single-shot check got wrong: the replica that
// has to answer is still running its own pull, so the same question has
// different answers a moment apart.
type flipPresence struct {
	mu    sync.Mutex
	left  int
	holds bool
}

func (p *flipPresence) HasVersion(context.Context, string, string, int64) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.left > 0 {
		p.left--
		return false, nil
	}
	return p.holds, nil
}

var _ VersionPresenceQuerier = (*flipPresence)(nil)

// No quorum on the first look, a quorum on the next: the check must not give up
// after one attempt (the third replica lands the data one catch-up tick later),
// and it must still announce exactly once.
func TestLocalDataPlane_TakeoverRetriesUntilAQuorumAppears(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control,
		&flipPresence{left: 1, holds: true},
		&stubDigest{digest: "d"},
		[]string{"peer-a"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	reports := waitForAnnouncements(t, control, 1)
	if len(reports) != 1 {
		t.Fatalf("durable reports = %+v, want exactly one", reports)
	}
	// Past the remaining attempts: a version that was announced must not keep
	// announcing.
	time.Sleep(200 * time.Millisecond)
	if n := control.count(); n != 1 {
		t.Fatalf("durable reports = %d, want 1: a successful announcement ends the watch", n)
	}
}

// The caller may discard the version while the check is running. Whichever lands
// first, the two paths agree; this covers the one the takeover sees: the discard
// wins, the report comes back VersionNotFound, and that must read as "nothing to
// announce" —
//
//   - not as a failure to retry (the version is not coming back),
//   - not as a reason to keep the watch alive (it would poll a discarded id),
//   - and not as a warning, which is what buries this signal in a busy cluster
//     (docs/known-gaps.md §K).
//
// The other order — the announcement lands first — is the service layer's: the
// discard is then refused because the version already has durable data. Both
// outcomes leave the same state: no version, or a durable one.
func TestLocalDataPlane_TakeoverAcceptsADiscardedVersion(t *testing.T) {
	shortenTakeover(t, 10*time.Millisecond)
	control := &takeoverControl{durableErr: stratumerrors.ErrVersionNotFound}
	dp := newTakeoverPlane(control,
		&stubPresence{holds: map[string]bool{"peer-a": true}},
		&stubDigest{digest: "d"},
		[]string{"peer-a"},
	)

	dp.WatchVersionWrite("kb-1", 7)
	time.Sleep(150 * time.Millisecond)

	if n := control.count(); n == 0 {
		t.Fatal("the check never reported, so the refusal path was not exercised")
	}
	dp.takeoverMu.Lock()
	pending := len(dp.pendingTakeovers)
	dp.takeoverMu.Unlock()
	if pending != 0 {
		t.Fatalf("pending takeovers = %d, want 0: a discarded version must not be polled", pending)
	}
}
