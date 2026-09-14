package plane

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
)

// stubReclaimer answers ReclaimChanges from a fixed table and counts the passes.
type stubReclaimer struct {
	used  map[string]int64
	err   error
	calls int
}

func (s *stubReclaimer) ReclaimChanges(context.Context) (map[string]int64, error) {
	s.calls++
	return s.used, s.err
}

// One pass hands exactly what the storage layer reported back out, and a failure is
// surfaced rather than swallowed: an operator must be able to tell "could not shrink"
// from "nothing to shrink".
func TestWALReclaimer_ReclaimOnceSurfacesFailures(t *testing.T) {
	boom := errors.New("disk full")
	r := NewWALReclaimer(WALReclaimerConfig{
		Reclaimer: &stubReclaimer{err: boom},
		Logger:    zap.NewNop(),
	})
	if err := r.ReclaimOnce(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("ReclaimOnce = %v, want the reclaimer's error", err)
	}

	ok := &stubReclaimer{used: map[string]int64{"kb-1": 9}}
	r = NewWALReclaimer(WALReclaimerConfig{Reclaimer: ok, Logger: zap.NewNop()})
	if err := r.ReclaimOnce(context.Background()); err != nil {
		t.Fatalf("ReclaimOnce: %v", err)
	}
	if !reflect.DeepEqual(ok.used, map[string]int64{"kb-1": 9}) {
		t.Errorf("used = %v", ok.used)
	}
}

// With no reclaimer wired the loop is inert rather than a nil-pointer panic: a node
// assembled without reclaim must still start, and Run returns at once instead of
// spinning a ticker over nothing.
func TestWALReclaimer_WithoutAReclaimerIsInert(t *testing.T) {
	r := NewWALReclaimer(WALReclaimerConfig{})
	if err := r.ReclaimOnce(context.Background()); err != nil {
		t.Fatalf("ReclaimOnce: %v", err)
	}
	done := make(chan struct{})
	go func() {
		r.Run(context.Background())
		close(done)
	}()
	select {
	case <-done:
		// Expected: there is nothing to reclaim.
	case <-time.After(2 * time.Second):
		t.Fatal("Run blocked with no reclaimer wired; it should return immediately")
	}
}

// The loop keeps going after a failed pass and stops promptly on cancellation.
func TestWALReclaimer_RunKeepsGoingAndStopsOnCancel(t *testing.T) {
	stub := &stubReclaimer{err: errors.New("transient")}
	r := NewWALReclaimer(WALReclaimerConfig{
		Reclaimer: stub,
		Interval:  5 * time.Millisecond,
		Logger:    zap.NewNop(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	// Let a few failing passes happen: a failure must not end the loop.
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if stub.calls < 2 {
		t.Errorf("passes = %d, want at least 2 (a failure must not stop the loop)", stub.calls)
	}
}
