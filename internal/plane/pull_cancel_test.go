package plane

import (
	"context"
	"errors"
	"testing"
	"time"
)

// M10 of docs/code-review-2026-09-24.md: the pull loop treated every failure as
// "try again" and never looked at the caller's context, so a cancelled pull kept
// going — to its 30 s idle timeout or its 10 min ceiling — while the goroutine
// that asked for it had already returned. Cancellation is an answer.
func TestEnsureIndexStopsWhenTheContextIsCancelled(t *testing.T) {
	puller := &sequencedPuller{err: errors.New("peer is down"), delay: 20 * time.Millisecond}
	dp := newSourcePlane(puller, []string{"peer-a"},
		&stubQuerier{cursors: map[string]int64{"peer-a": 9}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dp.EnsureIndex(ctx, "kb-1", 3) }()

	// Let it fail at least once, so the test is about the retrying loop and not
	// about a call that never started.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && puller.calls == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if puller.calls == 0 {
		t.Fatal("the pull never started")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want it to carry context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a cancelled pull must return promptly instead of retrying to its idle timeout")
	}

	// And it must actually stop: no further attempts after the return.
	settled := puller.calls
	time.Sleep(300 * time.Millisecond)
	if got := puller.calls; got != settled {
		t.Fatalf("the loop kept pulling after cancellation: %d → %d attempts", settled, got)
	}
}

// A context that is already cancelled must not even start: the caller is gone and
// every RPC the loop would send is one nobody is waiting for.
func TestEnsureIndexRefusesAnAlreadyCancelledContext(t *testing.T) {
	puller := &sequencedPuller{}
	dp := newSourcePlane(puller, []string{"peer-a"},
		&stubQuerier{cursors: map[string]int64{"peer-a": 9}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := dp.EnsureIndex(ctx, "kb-1", 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if puller.calls != 0 {
		t.Fatalf("a cancelled context started %d pulls", puller.calls)
	}
}
