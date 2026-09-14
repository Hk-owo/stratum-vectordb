package plane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Up to the limit, Acquire must not block: ordinary write concurrency has to
// survive the backstop.
func TestWriteLimiter_AdmitsUpToTheLimit(t *testing.T) {
	l := newWriteLimiter(3)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := l.Acquire(ctx, "kb-1"); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
	}
	if got := l.InFlight("kb-1"); got != 3 {
		t.Fatalf("in flight = %d, want 3", got)
	}
}

// The write past the limit queues rather than failing: it is a legitimate
// write that merely has to wait for a slot (Stratum_设计文档v13.md §7.7).
func TestWriteLimiter_QueuesPastTheLimit(t *testing.T) {
	l := newWriteLimiter(1)
	ctx := context.Background()

	if err := l.Acquire(ctx, "kb-1"); err != nil {
		t.Fatal(err)
	}

	admitted := make(chan error, 1)
	go func() { admitted <- l.Acquire(ctx, "kb-1") }()

	select {
	case err := <-admitted:
		t.Fatalf("the second acquire returned %v immediately, want it queued", err)
	case <-time.After(50 * time.Millisecond):
		// Still waiting, as it should be.
	}

	l.Release("kb-1")
	select {
	case err := <-admitted:
		if err != nil {
			t.Fatalf("queued acquire: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("releasing a slot did not wake the queued write")
	}
}

// A caller that gives up must stop waiting — and must not leave a phantom
// slot behind.
func TestWriteLimiter_AcquireHonoursContext(t *testing.T) {
	l := newWriteLimiter(1)
	if err := l.Acquire(context.Background(), "kb-1"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := l.Acquire(ctx, "kb-1")
	if err == nil {
		t.Fatal("want an error when the context ends while queued")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap the context error", err)
	}
	if got := l.InFlight("kb-1"); got != 1 {
		t.Errorf("in flight = %d, want 1: a cancelled waiter must not take a slot", got)
	}
}

// Limits are per knowledge base: one KB's backlog must not stall another's.
func TestWriteLimiter_LimitsArePerKnowledgeBase(t *testing.T) {
	l := newWriteLimiter(1)
	ctx := context.Background()

	if err := l.Acquire(ctx, "kb-1"); err != nil {
		t.Fatal(err)
	}
	// kb-2 has its own gate, so this must not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := l.Acquire(ctx, "kb-2"); err != nil {
			t.Errorf("kb-2 acquire: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("kb-2 was blocked by kb-1's backlog")
	}
}

// A declared per-KB cap replaces the default, and clearing it restores the
// default without stranding the count.
func TestWriteLimiter_SetLimitOverridesPerKnowledgeBase(t *testing.T) {
	l := newWriteLimiter(3)
	l.SetLimit("kb-1", 1)

	ctx := context.Background()
	if err := l.Acquire(ctx, "kb-1"); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		_ = l.Acquire(ctx, "kb-1")
	}()
	select {
	case <-blocked:
		t.Fatal("the override (1) was not enforced")
	case <-time.After(50 * time.Millisecond):
	}

	l.Release("kb-1")
	<-blocked
	// The woken goroutine took the slot and never gave it back; hand it over
	// before probing the restored default.
	l.Release("kb-1")

	// Clearing the override returns kb-1 to the default of 3.
	l.SetLimit("kb-1", 0)
	for i := 0; i < 3; i++ {
		if err := l.Acquire(ctx, "kb-1"); err != nil {
			t.Fatalf("acquire under the restored default: %v", err)
		}
	}
}

// Releasing without a slot in hand must not drive the count negative and
// silently widen the real limit.
func TestWriteLimiter_ReleaseWithoutAcquireIsIgnored(t *testing.T) {
	l := newWriteLimiter(1)
	l.Release("kb-1")
	if got := l.InFlight("kb-1"); got != 0 {
		t.Fatalf("in flight = %d, want 0", got)
	}
	if err := l.Acquire(context.Background(), "kb-1"); err != nil {
		t.Fatalf("acquire after a stray release: %v", err)
	}
}

// The write path is where the queue actually happens: with every slot taken,
// WriteVersionData has to wait instead of running.
func TestLocalDataPlane_WriteVersionDataQueuesBehindTheLimit(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:      &stubIndexStore{},
		WAL:               &stubWAL{t: tr},
		Executor:          &stubExecutor{t: tr},
		MaxInFlightWrites: 1,
	})
	ctx := context.Background()

	// Occupy the only slot.
	if err := dp.limiter.Acquire(ctx, "kb-1"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	released := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(released)
		_ = dp.WriteVersionData(ctx, "kb-1", 7, 6, nil)
	}()

	select {
	case <-released:
		t.Fatal("the write ran despite the limit being reached")
	case <-time.After(60 * time.Millisecond):
	}

	dp.limiter.Release("kb-1")

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("the write did not proceed after a slot was released")
	}
	wg.Wait()
}

// A caller whose context ends while queued gets an error, not a write that
// silently proceeds later.
func TestLocalDataPlane_WriteVersionDataFailsWhenQueuedAndCancelled(t *testing.T) {
	tr := &tracer{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager:      &stubIndexStore{},
		WAL:               &stubWAL{t: tr},
		Executor:          &stubExecutor{t: tr},
		MaxInFlightWrites: 1,
	})
	if err := dp.limiter.Acquire(context.Background(), "kb-1"); err != nil {
		t.Fatal(err)
	}
	defer dp.limiter.Release("kb-1")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := dp.WriteVersionData(ctx, "kb-1", 7, 6, nil); err == nil {
		t.Fatal("want an error when the caller gives up while queued")
	}
}
