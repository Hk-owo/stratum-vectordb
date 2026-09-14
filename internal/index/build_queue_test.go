package index

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// The pool's two jobs are to BOUND how many builds run at once and to decide WHICH
// one runs next. Both are tested here without a vecstore, because the pool is
// deliberately decoupled from doBuild: it takes the work as a plain function, so a
// test can hand it a counter instead of a real build.
//
// Why bound it: triggerBuild used to spawn a goroutine per request with no ceiling,
// so a restart over a populated volume — every historical artifact missing — put
// hundreds of builds on the CPU, disk and vecstore at once, and a fresh write that
// arrived during the sweep waited 601s for READY.
//
// Why prioritise: bounding alone turns the storm into a queue, and a queue is only
// acceptable if the work someone is WAITING for goes first. A reconcile backfill can
// wait; a query cannot.

// poolHarness drives a pool and records what it observed.
type poolHarness struct {
	mu      sync.Mutex
	running int
	peak    int
	order   []int64
	started chan int64
	release chan struct{}
	gate    bool // when true, fn blocks until release is closed
}

func newPoolHarness(gate bool) *poolHarness {
	return &poolHarness{
		started: make(chan int64, 1024),
		release: make(chan struct{}),
		gate:    gate,
	}
}

func (h *poolHarness) fn(req buildRequest) {
	h.mu.Lock()
	h.running++
	if h.running > h.peak {
		h.peak = h.running
	}
	h.order = append(h.order, req.versionID)
	h.mu.Unlock()

	h.started <- req.versionID

	if h.gate {
		<-h.release
	}

	h.mu.Lock()
	h.running--
	h.mu.Unlock()
}

func (h *poolHarness) snapshot() (peak int, order []int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peak, append([]int64(nil), h.order...)
}

// awaitPeak waits until the harness has seen `want` builds running at once.
func (h *poolHarness) awaitPeak(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		got := h.peak
		h.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("harness never saw %d concurrent builds", want)
}

// TestBuildPool_BoundsConcurrency is the failure the pool exists to prevent: with a
// ceiling of 4, fifty queued builds must never run more than four at a time.
func TestBuildPool_BoundsConcurrency(t *testing.T) {
	const (
		size  = 4
		tasks = 50
	)
	h := newPoolHarness(true)
	pool := newBuildPool(size, h.fn, zap.NewNop())

	for i := 0; i < tasks; i++ {
		if !pool.Submit(buildRequest{kbID: "kb", versionID: int64(i), priority: BuildPriorityBackfill}) {
			t.Fatalf("Submit(%d) refused while the pool is open", i)
		}
	}

	// Wait until the ceiling is actually reached, so a pool that silently serialises
	// everything would fail here rather than pass by doing nothing.
	h.awaitPeak(t, size)

	if peak, _ := h.snapshot(); peak > size {
		t.Errorf("peak concurrent builds = %d, want <= %d", peak, size)
	}

	close(h.release)
	pool.Close()

	peak, order := h.snapshot()
	if peak > size {
		t.Errorf("peak concurrent builds = %d, want <= %d", peak, size)
	}
	if len(order) != tasks {
		t.Errorf("ran %d builds, want all %d", len(order), tasks)
	}
}

// TestBuildPool_PrefersInteractiveOverBackfill is the starvation fix: a build that
// somebody is waiting for must not queue behind a sweep's worth of backfill work.
//
// The pool is intentionally size 1 so the ORDER of execution is observable: the
// first backfill occupies the worker, three more backfills queue up, and then an
// interactive request arrives. When the worker frees up it must pick the interactive
// one, not the older backfills.
func TestBuildPool_PrefersInteractiveOverBackfill(t *testing.T) {
	h := newPoolHarness(true)
	pool := newBuildPool(1, h.fn, zap.NewNop())

	// The tail of the backfill queue is what the interactive build must jump over.
	if !pool.Submit(buildRequest{kbID: "kb", versionID: 1, priority: BuildPriorityBackfill}) {
		t.Fatal("Submit(1) refused")
	}
	// Wait for it to actually start, so the queued ones really are queued.
	select {
	case <-h.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the first backfill never started")
	}
	for id := int64(2); id <= 4; id++ {
		if !pool.Submit(buildRequest{kbID: "kb", versionID: id, priority: BuildPriorityBackfill}) {
			t.Fatalf("Submit(%d) refused", id)
		}
	}
	if !pool.Submit(buildRequest{kbID: "kb", versionID: 99, priority: BuildPriorityInteractive}) {
		t.Fatal("Submit(interactive) refused")
	}

	close(h.release)
	pool.Close()

	_, order := h.snapshot()
	if len(order) != 5 {
		t.Fatalf("ran %d builds (%v), want all 5", len(order), order)
	}
	// order[0] is the backfill that was already running. The interactive build must
	// be the very next one — ahead of the still-queued backfills.
	if order[0] != 1 {
		t.Fatalf("order = %v, want the already-running backfill v1 first", order)
	}
	if order[1] != 99 {
		t.Errorf("order = %v, want the interactive v99 second — it must not queue behind "+
			"the backfill sweep", order)
	}
}

// TestBuildPool_DrainsOnClose: work already accepted must still run. A pool that
// drops queued builds on shutdown would silently leave versions unbuilt, which is
// exactly the failure mode that is hardest to notice.
func TestBuildPool_DrainsOnClose(t *testing.T) {
	const tasks = 20
	h := newPoolHarness(false) // no gating: each build completes immediately
	pool := newBuildPool(3, h.fn, zap.NewNop())

	for i := 0; i < tasks; i++ {
		if !pool.Submit(buildRequest{kbID: "kb", versionID: int64(i), priority: BuildPriorityBackfill}) {
			t.Fatalf("Submit(%d) refused while the pool is open", i)
		}
	}
	pool.Close()

	if _, order := h.snapshot(); len(order) != tasks {
		t.Errorf("ran %d builds, want all %d to drain before Close returns", len(order), tasks)
	}
}

// TestBuildPool_RefusesAfterClose: a closed pool must say so rather than accept work
// it will never run, so the caller can fall back (or log) instead of believing a
// build was scheduled.
func TestBuildPool_RefusesAfterClose(t *testing.T) {
	h := newPoolHarness(false)
	pool := newBuildPool(2, h.fn, zap.NewNop())
	pool.Close()

	if pool.Submit(buildRequest{kbID: "kb", versionID: 7, priority: BuildPriorityInteractive}) {
		t.Error("Submit after Close returned true; a refused build must be visible to the caller")
	}
}
