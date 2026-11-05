package plane

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The ordinary catch-up of a PENDING version ends here: the pull succeeds, verification
// accepts it (a PENDING version has no digest yet, so verifyVersionPull accepts any pull
// that produced documents), and EnsureIndex returns without passing the loop-top check
// where the quorum check is armed. A PENDING version is always the chain tail, so this is
// the catch-up target rather than a gap version, and ApplyBackfillChanges never sees it.
func TestRepro_PulledPendingVersionArmsTheDurabilityCheck(t *testing.T) {
	shortenTakeover(t, 5*time.Millisecond)
	control := &takeoverControl{}
	dp := newTakeoverPlane(control, &stubPresence{holds: map[string]bool{"peer-a": true}}, &stubDigest{digest: "d"}, []string{"peer-a"})
	dp.puller = &recordingPuller{}
	dp.verify = func(context.Context, string, int64) bool { return true }
	dp.resolve = func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil }

	if err := dp.EnsureIndex(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if got := control.count(); got != 1 {
		t.Fatalf("announced %d times, want 1: this node and peer-a are a quorum of 2, and nobody else will say so", got)
	}
}

// Only "no quorum yet" is retried. A report that fails, say during a leader election, drops
// the watch with the quorum already proven, and every replica's watch can hit the same
// election. Nothing arms it again, because the node is no longer behind.
func TestRepro_FailedAnnouncementIsRetried(t *testing.T) {
	shortenTakeover(t, time.Millisecond)
	control := &takeoverControl{durableErr: errors.New("no leader yet")}
	dp := newTakeoverPlane(control, &stubPresence{holds: map[string]bool{"peer-a": true}}, &stubDigest{digest: "d"}, []string{"peer-a"})

	dp.WatchVersionWrite("kb-1", 7)

	time.Sleep(100 * time.Millisecond)
	if got := control.count(); got < 2 {
		t.Fatalf("attempted the announcement %d times, want a retry", got)
	}
}
