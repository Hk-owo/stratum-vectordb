package plane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"stratum/internal/types"
)

// countingPresence answers "nobody holds it" and counts how often it was asked.
type countingPresence struct {
	asked atomic.Int64
}

func (p *countingPresence) HasVersion(context.Context, string, string, int64) (bool, error) {
	p.asked.Add(1)
	return false, nil
}

func (d *LocalDataPlane) takeoverWatchFor(kbID string, versionID int64) (takeoverWatch, bool) {
	d.takeoverMu.Lock()
	defer d.takeoverMu.Unlock()
	w, ok := d.pendingTakeovers[failureKey(kbID, versionID)]
	if !ok {
		return takeoverWatch{}, false
	}
	return *w, true
}

// waitForNoTakeover blocks until no watch is left for the version.
func waitForNoTakeover(t *testing.T, dp *LocalDataPlane, kbID string, versionID int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := dp.takeoverWatchFor(kbID, versionID); !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("a takeover watch for %s v%d is still pending", kbID, versionID)
}

// The retries are bounded: a version no quorum holds belongs to §10.1's verdict, not to
// an endless poll. Every attempt asks every peer, and nothing is announced.
func TestLocalDataPlane_TakeoverGivesUpAfterMaxAttempts(t *testing.T) {
	shortenTakeover(t, time.Millisecond)
	control := &takeoverControl{}
	presence := &countingPresence{}
	dp := newTakeoverPlane(control, presence, &stubDigest{digest: "d"}, []string{"peer-a", "peer-b"})

	dp.WatchVersionWrite("kb-1", 7)
	// Spent attempts leave the last timer behind only until it fires, so wait on the
	// count first: the map is briefly empty between two attempts.
	want := int64(maxTakeoverAttempts * 2)
	deadline := time.Now().Add(10 * time.Second)
	for presence.asked.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	waitForNoTakeover(t, dp, "kb-1", 7)
	time.Sleep(20 * time.Millisecond)

	if got := presence.asked.Load(); got != want {
		t.Errorf("presence asked %d times, want %d (%d attempts × 2 peers)", got, want, maxTakeoverAttempts)
	}
	if got := control.count(); got != 0 {
		t.Errorf("announced %d times without a quorum, want 0", got)
	}
}

// The re-check interval doubles and stops at the cap, and each re-check spends one attempt.
func TestLocalDataPlane_TakeoverRetryBacksOffUpToTheCap(t *testing.T) {
	cases := []struct {
		name string
		from time.Duration
		want time.Duration
	}{
		{"doubles", 200 * time.Millisecond, 400 * time.Millisecond},
		{"stops at the cap", 4 * time.Second, maxTakeoverRetryDelay},
		{"stays at the cap", maxTakeoverRetryDelay, maxTakeoverRetryDelay},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dp := newTakeoverPlane(&takeoverControl{}, &countingPresence{}, &stubDigest{digest: "d"}, []string{"peer-a"})
			dp.armTakeover("kb-1", 7, 2, tc.from)
			w, ok := dp.takeoverWatchFor("kb-1", 7)
			if !ok {
				t.Fatal("no watch armed")
			}
			w.timer.Stop()

			dp.attemptTakeover("kb-1", 7, w.gen, w.attempts)

			next, ok := dp.takeoverWatchFor("kb-1", 7)
			if !ok {
				t.Fatal("no quorum and attempts left: the version must be re-checked")
			}
			next.timer.Stop()
			if next.delay != tc.want {
				t.Errorf("next delay = %v, want %v", next.delay, tc.want)
			}
			if next.attempts != 3 {
				t.Errorf("attempts = %d, want 3", next.attempts)
			}
		})
	}
}

// The coordinator's confirmation has to stop a watch that is already retrying, or the
// replica keeps polling peers about a version nobody is waiting on.
func TestLocalDataPlane_ConfirmationCancelsARetryingTakeover(t *testing.T) {
	control := &takeoverControl{}
	presence := &countingPresence{}
	dp := newTakeoverPlane(control, presence, &stubDigest{digest: "d"}, []string{"peer-a"})

	dp.armTakeover("kb-1", 7, 0, time.Hour)
	w, _ := dp.takeoverWatchFor("kb-1", 7)
	w.timer.Stop()
	dp.attemptTakeover("kb-1", 7, w.gen, w.attempts)
	if _, ok := dp.takeoverWatchFor("kb-1", 7); !ok {
		t.Fatal("the first attempt found no quorum, so a retry must be armed")
	}

	dp.ConfirmVersionWrite("kb-1", 7)

	if _, ok := dp.takeoverWatchFor("kb-1", 7); ok {
		t.Fatal("the confirmation must cancel the retry")
	}
	asked := presence.asked.Load()
	time.Sleep(20 * time.Millisecond)
	if got := presence.asked.Load(); got != asked {
		t.Errorf("presence asked again after the confirmation (%d → %d)", asked, got)
	}
}

// A timer that was already firing when its watch got replaced must not act for the
// watch that replaced it: it would spend the newer watch and announce on its behalf.
func TestLocalDataPlane_SupersededTakeoverTimerDoesNothing(t *testing.T) {
	control := &takeoverControl{}
	dp := newTakeoverPlane(control, &stubPresence{holds: map[string]bool{"peer-a": true}}, &stubDigest{digest: "d"}, []string{"peer-a"})

	dp.armTakeover("kb-1", 7, 0, time.Hour)
	old, _ := dp.takeoverWatchFor("kb-1", 7)
	dp.armTakeover("kb-1", 7, 0, time.Hour)
	current, _ := dp.takeoverWatchFor("kb-1", 7)
	t.Cleanup(func() { current.timer.Stop() })
	if old.gen == current.gen {
		t.Fatal("re-arming must hand out a new identity")
	}

	dp.attemptTakeover("kb-1", 7, old.gen, 0)

	if got := control.count(); got != 0 {
		t.Errorf("the superseded timer announced %d times, want 0", got)
	}
	if w, ok := dp.takeoverWatchFor("kb-1", 7); !ok || w.gen != current.gen {
		t.Error("the superseded timer must leave the current watch in place")
	}
}

// Arming twice replaces the watch, so one version never has two timers running.
func TestLocalDataPlane_RearmingKeepsOneWatchPerVersion(t *testing.T) {
	shortenTakeover(t, 20*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control, &stubPresence{holds: map[string]bool{"peer-a": true}}, &stubDigest{digest: "d"}, []string{"peer-a"})

	for i := 0; i < 5; i++ {
		dp.WatchVersionWrite("kb-1", 7)
	}
	dp.takeoverMu.Lock()
	watches := len(dp.pendingTakeovers)
	dp.takeoverMu.Unlock()
	if watches != 1 {
		t.Errorf("watches = %d, want 1", watches)
	}

	waitForAnnouncements(t, control, 1)
	time.Sleep(60 * time.Millisecond)
	if got := control.count(); got != 1 {
		t.Errorf("announced %d times, want once", got)
	}
}

// A plane with nothing to check quorum with arms nothing, instead of timers that would
// panic or report without evidence.
func TestLocalDataPlane_TakeoverNotWiredArmsNothing(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{WAL: &stubWAL{t: tr}, Executor: &stubExecutor{t: tr}})

	dp.WatchVersionWrite("kb-1", 7)
	dp.watchForDurability(context.Background(), "kb-1", 7)

	if _, ok := dp.takeoverWatchFor("kb-1", 7); ok {
		t.Fatal("no presence, digest or control is wired, so nothing can be armed")
	}
}

// watchForDurability's gate: only a readable, settled verdict skips the check. "Cannot
// read" falls through, because forgetting the announcement strands the version.
func TestLocalDataPlane_WatchForDurabilityGate(t *testing.T) {
	cases := []struct {
		name  string
		known bool
		st    types.DataStatus
		armed bool
	}{
		{"pending is armed", true, types.DataStatusPending, true},
		{"durable has nothing to announce", true, types.DataStatusDurable, false},
		{"terminal has nothing to announce", true, types.DataStatusFailedPermanent, false},
		{"unreadable is armed", false, types.DataStatusPending, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dp := newTakeoverPlaneWithStatus(&takeoverControl{}, &countingPresence{}, &stubDigest{digest: "d"},
				&stubDataStatus{known: tc.known, status: map[int64]types.DataStatus{7: tc.st}}, []string{"peer-a"})

			dp.watchForDurability(context.Background(), "kb-1", 7)

			w, armed := dp.takeoverWatchFor("kb-1", 7)
			if armed {
				w.timer.Stop()
			}
			if armed != tc.armed {
				t.Errorf("armed = %v, want %v", armed, tc.armed)
			}
		})
	}
}

// A replay that did not land holds nothing, so it must not arm a check that would count
// this node's copy as an acknowledgement.
func TestLocalDataPlane_FailedBackfillArmsNothing(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:             &stubWAL{t: tr},
		Executor:        &stubExecutor{t: tr, err: errors.New("disk full")},
		Control:         &takeoverControl{},
		Presence:        &stubPresence{holds: map[string]bool{"peer-a": true}},
		Digest:          &stubDigest{digest: "d"},
		ResolveReplicas: func(context.Context) ([]string, error) { return []string{"peer-a"}, nil },
	})

	err := dp.ApplyBackfillChanges(context.Background(), "kb-1", 7, 6, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1"}})
	if err == nil {
		t.Fatal("the replay failed, so the call must say so")
	}
	if w, ok := dp.takeoverWatchFor("kb-1", 7); ok {
		w.timer.Stop()
		t.Fatal("a failed replay must not arm the quorum check")
	}
}

// landingPuller fails its pull while the node's own write lands meanwhile, which is the
// case the top of EnsureIndex's loop exists for.
type landingPuller struct {
	dp *LocalDataPlane
}

func (p *landingPuller) PullVersionData(_ context.Context, _, kbID string, versionID int64) error {
	p.dp.dataLanded(kbID, versionID)
	return errors.New("the source has nothing yet")
}

// The cursor reaching the version mid-loop arms the check for a version still PENDING,
// and skips it for one already settled.
func TestLocalDataPlane_EnsureIndexArmsWhenTheCursorArrivesMidLoop(t *testing.T) {
	for _, tc := range []struct {
		name  string
		st    types.DataStatus
		armed bool
	}{
		{"pending", types.DataStatusPending, true},
		{"durable", types.DataStatusDurable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dp := newTakeoverPlaneWithStatus(&takeoverControl{}, &countingPresence{}, &stubDigest{digest: "d"},
				&stubDataStatus{known: true, status: map[int64]types.DataStatus{7: tc.st}}, []string{"peer-a"})
			dp.puller = &landingPuller{dp: dp}
			dp.resolve = func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil }

			if err := dp.EnsureIndex(context.Background(), "kb-1", 7); err != nil {
				t.Fatalf("EnsureIndex: %v", err)
			}

			w, armed := dp.takeoverWatchFor("kb-1", 7)
			if armed {
				w.timer.Stop()
			}
			if armed != tc.armed {
				t.Errorf("armed = %v, want %v", armed, tc.armed)
			}
		})
	}
}
