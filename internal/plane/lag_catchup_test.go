package plane

import (
	"context"
	"sync"
	"testing"
	"time"
)

type ensureCall struct {
	kbID   string
	target int64
}

// A knowledge base whose own cursor is behind the tail is picked up; one that is
// level with it is not. The threshold is what draws the line, and 1 makes "cursor 7,
// tail 7" not a lag while "cursor 3, tail 9" is.
func TestLagCatchup_CatchesUpWhatIsBehind(t *testing.T) {
	var mu sync.Mutex
	var calls []ensureCall
	done := make(chan struct{}, 4)

	l := NewLagCatchup(LagCatchupConfig{
		MinLagVersions: 1,
		Ensure: func(_ context.Context, kbID string, versionID int64) error {
			mu.Lock()
			calls = append(calls, ensureCall{kbID, versionID})
			mu.Unlock()
			done <- struct{}{}
			return nil
		},
		Cursor: func() map[string]int64 {
			return map[string]int64{"kb-behind": 3, "kb-level": 7}
		},
	})

	l.SetChainTails(map[string]int64{"kb-behind": 9, "kb-level": 7})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a knowledge base six versions behind was never picked up")
	}
	// Give a wrongly-started second catch-up a chance to show up before asserting.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 || calls[0].kbID != "kb-behind" || calls[0].target != 9 {
		t.Fatalf("calls = %v, want exactly one: kb-behind → 9", calls)
	}
}

// MinLagVersions raises the bar: at 2, a one-version gap is left alone.
func TestLagCatchup_MinLagVersionsRaisesTheBar(t *testing.T) {
	var mu sync.Mutex
	var calls []ensureCall

	l := NewLagCatchup(LagCatchupConfig{
		MinLagVersions: 2,
		Ensure: func(_ context.Context, kbID string, versionID int64) error {
			mu.Lock()
			calls = append(calls, ensureCall{kbID, versionID})
			mu.Unlock()
			return nil
		},
		Cursor: func() map[string]int64 {
			return map[string]int64{"kb-one-behind": 8, "kb-two-behind": 7}
		},
	})

	l.SetChainTails(map[string]int64{"kb-one-behind": 9, "kb-two-behind": 9})
	// Wait for whichever catch-up starts; then check only the two-behind one did.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(calls)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 || calls[0].kbID != "kb-two-behind" {
		t.Fatalf("calls = %v, want only kb-two-behind (the other is just one behind)", calls)
	}
}

// Nothing wired, nothing done: a node that never assembled the catch-up must absorb a
// signal without starting anything (and without panicking). The component is
// unconditional now, so "not wired" is the only remaining absent state.
func TestLagCatchup_UnwiredDoesNothing(t *testing.T) {
	l := NewLagCatchup(LagCatchupConfig{})

	// No Ensure and no Cursor: this must be a no-op, not a panic.
	l.SetChainTails(map[string]int64{"kb": 99})
	time.Sleep(50 * time.Millisecond)
}

// A knowledge base already catching up is not started again: the signal repeats every
// report interval, so without this every interval would begin another pass over the
// same gap.
func TestLagCatchup_DoesNotStartTheSameKnowledgeBaseTwice(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})

	l := NewLagCatchup(LagCatchupConfig{
		Ensure: func(ctx context.Context, _ string, _ int64) error {
			started <- struct{}{}
			<-release
			return nil
		},
		Cursor: func() map[string]int64 { return map[string]int64{"kb": 1} },
	})

	l.SetChainTails(map[string]int64{"kb": 99})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the first catch-up never started")
	}

	// Three more reports while it is still running: all must be ignored.
	l.SetChainTails(map[string]int64{"kb": 99})
	l.SetChainTails(map[string]int64{"kb": 99})
	l.SetChainTails(map[string]int64{"kb": 99})
	close(release)

	// Drain: exactly one start, and after it finishes the slot is free again.
	time.Sleep(100 * time.Millisecond)
	if n := len(started); n != 0 {
		t.Fatalf("%d further catch-ups started while one was already running", n)
	}
}

// Jitter alone is not an upper bound once more knowledge bases are behind than anyone
// expected, so the concurrency bound is what actually caps the burst.
func TestLagCatchup_BoundsConcurrentCatchUps(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})

	l := NewLagCatchup(LagCatchupConfig{
		MaxConcurrentKBs: 1,
		Ensure: func(ctx context.Context, _ string, _ int64) error {
			started <- struct{}{}
			<-release
			return nil
		},
		Cursor: func() map[string]int64 {
			return map[string]int64{"kb-a": 1, "kb-b": 1, "kb-c": 1}
		},
	})

	l.SetChainTails(map[string]int64{"kb-a": 99, "kb-b": 99, "kb-c": 99})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("nothing started")
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(started); n != 0 {
		t.Fatalf("%d catch-ups started past the bound of 1", n)
	}
	close(release)
}

// The delay is real, bounded by Jitter, and derived from the knowledge base id — two
// nodes behind on the same knowledge base should not land in the same slot.
func TestLagCatchup_JitterIsBoundedAndDelaysTheStart(t *testing.T) {
	var mu sync.Mutex
	var slept []time.Duration
	done := make(chan struct{}, 1)

	cfg := LagCatchupConfig{
		Jitter: 500 * time.Millisecond,
		Ensure: func(_ context.Context, _ string, _ int64) error {
			done <- struct{}{}
			return nil
		},
		Cursor: func() map[string]int64 { return map[string]int64{"kb": 1} },
	}
	cfg.sleep = func(d time.Duration) {
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
	}
	l := NewLagCatchup(cfg)

	l.SetChainTails(map[string]int64{"kb": 99})
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the catch-up never ran")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(slept) != 1 {
		t.Fatalf("slept %v, want exactly one delay", slept)
	}
	if slept[0] < 0 || slept[0] >= 500*time.Millisecond {
		t.Fatalf("jitter = %v, want it inside [0, 500ms)", slept[0])
	}
}

// TestLagCatchup_ChainTailsKeepsTheMirrorWithoutCatchUpWiring: §8.6(d)'s tombstone scan
// reads the tail mirror, and a node that cannot catch up (no Ensure/Cursor wired) is
// still a node whose scan wants to know where the chain ends. The returned map is a
// copy — the caller must not be able to mutate the mirror the next report replaces.
func TestLagCatchup_ChainTailsKeepsTheMirrorWithoutCatchUpWiring(t *testing.T) {
	lc := NewLagCatchup(LagCatchupConfig{})
	lc.SetChainTails(map[string]int64{"kb-1": 7})

	got := lc.ChainTails()
	if len(got) != 1 || got["kb-1"] != 7 {
		t.Fatalf("ChainTails = %v, want kb-1→7 kept even without catch-up wiring", got)
	}

	got["kb-1"] = 99
	got["kb-2"] = 1
	again := lc.ChainTails()
	if again["kb-1"] != 7 || len(again) != 1 {
		t.Fatalf("ChainTails handed out the live mirror: %v", again)
	}

	// "Not told yet" must read as nil rather than as an empty set: the difference
	// matters to callers that treat absence as a fact about a knowledge base.
	if empty := NewLagCatchup(LagCatchupConfig{}).ChainTails(); empty != nil {
		t.Errorf("an unwired mirror = %v, want nil", empty)
	}
}
