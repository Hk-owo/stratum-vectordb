package plane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stubConfirmRecorder records one attempt per peer and can be told how to behave:
// fail every attempt, fail the first N attempts for a peer, or hang until that
// attempt's context expires — the shape an unreachable peer actually has.
type stubConfirmRecorder struct {
	mu       sync.Mutex
	attempts map[string]int

	alwaysFail bool            // every attempt fails (an unreachable peer that answers fast)
	failFirst  map[string]int  // peer → how many leading attempts fail
	hang       map[string]bool // peer → never answers, the attempt deadline decides
	err        error           // what a failed attempt reports; default is a dial error
}

func newStubConfirmRecorder() *stubConfirmRecorder {
	return &stubConfirmRecorder{
		attempts:  make(map[string]int),
		failFirst: make(map[string]int),
		hang:      make(map[string]bool),
	}
}

func (c *stubConfirmRecorder) ConfirmVersionWrite(ctx context.Context, peerAddr, _ string, _ int64, _ string) error {
	c.mu.Lock()
	c.attempts[peerAddr]++
	n := c.attempts[peerAddr]
	failThrough := c.failFirst[peerAddr]
	hangs := c.hang[peerAddr]
	alwaysFail := c.alwaysFail
	err := c.err
	c.mu.Unlock()

	if hangs {
		<-ctx.Done()
		return ctx.Err()
	}
	if alwaysFail || n <= failThrough {
		if err == nil {
			err = errors.New("dial peer: connection refused")
		}
		return err
	}
	return nil
}

func (c *stubConfirmRecorder) attemptsFor(peer string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts[peer]
}

var _ WriteConfirmer = (*stubConfirmRecorder)(nil)

// confirmPlane builds a plane whose only wired parts are the confirmation's.
func confirmPlane(confirmer WriteConfirmer, peers ...string) *LocalDataPlane {
	return NewLocalDataPlane(LocalDataPlaneConfig{
		Confirmer: confirmer,
		ResolveReplicas: func(context.Context) ([]string, error) {
			return peers, nil
		},
	})
}

// shrinkConfirmTimings makes the broadcast's injectable timings small enough for a
// test to reach the interesting case quickly, and restores them afterwards.
func shrinkConfirmTimings(t *testing.T, attempt, backoff time.Duration) {
	t.Helper()
	oldAttempt, oldBackoff, oldBudget := confirmAttemptTimeout, confirmRetryBackoff, confirmBroadcastBudget
	confirmAttemptTimeout, confirmRetryBackoff, confirmBroadcastBudget = attempt, backoff, 5*time.Second
	t.Cleanup(func() {
		confirmAttemptTimeout, confirmRetryBackoff, confirmBroadcastBudget = oldAttempt, oldBackoff, oldBudget
	})
}

// awaitCondition polls until cond holds or the deadline passes.
func awaitCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

// A confirmation that does not land is retried, a bounded number of times. Before
// this, each peer was told exactly once, and an offline replica — the normal case,
// since the write that triggered the confirmation has just failed to push to it —
// meant no confirmation at all.
func TestConfirmBroadcastRetriesEachPeer(t *testing.T) {
	shrinkConfirmTimings(t, 100*time.Millisecond, 10*time.Millisecond)

	confirmer := newStubConfirmRecorder()
	confirmer.alwaysFail = true
	plane := confirmPlane(confirmer, "peer-a:7000")

	plane.broadcastConfirmation("kb-1", 7)

	awaitCondition(t, func() bool { return confirmer.attemptsFor("peer-a:7000") == confirmAttempts })
}

// The regression this exists for: every peer shared ONE context, so the first peer
// to hang consumed the whole window and every peer after it failed instantly with
// "context deadline exceeded" without ever being dialled. Measured in the cluster:
// with one storage node down, the confirmation reached NEITHER of the other two,
// no quorum ever formed, and the version's digest was never committed.
//
// With a budget per peer, the peer behind a hung one still gets its own attempts —
// and its retry still succeeds.
func TestConfirmBroadcastDoesNotLetAHungPeerStarveTheRest(t *testing.T) {
	shrinkConfirmTimings(t, 100*time.Millisecond, 10*time.Millisecond)

	confirmer := newStubConfirmRecorder()
	confirmer.hang["peer-a:7000"] = true   // unreachable: spends each attempt's deadline
	confirmer.failFirst["peer-b:7000"] = 1 // transient: succeeds on the retry
	plane := confirmPlane(confirmer, "peer-a:7000", "peer-b:7000")

	plane.broadcastConfirmation("kb-1", 7)

	awaitCondition(t, func() bool { return confirmer.attemptsFor("peer-b:7000") >= 2 })

	if got := confirmer.attemptsFor("peer-a:7000"); got != confirmAttempts {
		t.Fatalf("hung peer attempts = %d, want %d", got, confirmAttempts)
	}
	// Exactly two: one failure and one success. A spent shared budget would show
	// up here as a peer that was never retried, or never dialled at all.
	if got := confirmer.attemptsFor("peer-b:7000"); got != 2 {
		t.Fatalf("peer behind the hung one: attempts = %d, want 2 (fail then succeed)", got)
	}
}

// A reachable peer is told once: the retry is for peers that did not answer, not a
// tax on the ones that did.
func TestConfirmBroadcastDoesNotRetryASuccessfulPeer(t *testing.T) {
	shrinkConfirmTimings(t, 100*time.Millisecond, 10*time.Millisecond)

	confirmer := newStubConfirmRecorder()
	plane := confirmPlane(confirmer, "peer-a:7000")

	plane.broadcastConfirmation("kb-1", 7)

	awaitCondition(t, func() bool { return confirmer.attemptsFor("peer-a:7000") >= 1 })
	time.Sleep(50 * time.Millisecond) // room for a second attempt, if one were coming
	if got := confirmer.attemptsFor("peer-a:7000"); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 for a peer that answered", got)
	}
}
