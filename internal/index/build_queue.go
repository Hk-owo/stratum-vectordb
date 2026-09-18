package index

import (
	"runtime"
	"sync"

	"go.uber.org/zap"
)

// BuildPriority says WHY a build is being scheduled. The distinction exists because
// the two kinds cannot share a queue fairly: a reconcile sweep can enqueue hundreds
// of builds that nobody is waiting for, and a build somebody IS waiting for must not
// sit behind them.
type BuildPriority int

const (
	// BuildPriorityInteractive is a build someone is waiting on: EnsureIndex needs
	// the index to answer a query or a confirmation, or a writer is waiting for its
	// version to reach READY.
	BuildPriorityInteractive BuildPriority = iota
	// BuildPriorityBackfill is a head start for a version nobody has asked for yet
	// (reconcile after a restart). Useful, never urgent.
	BuildPriorityBackfill
)

// buildRequest is one queued build.
type buildRequest struct {
	kbID      string
	versionID int64
	graphFree bool
	priority  BuildPriority
}

// defaultBuildConcurrency is how many builds may run at once when nothing is
// configured.
//
// Derived from the CPU count rather than picked out of the air, because a build is
// CPU- and IO-bound and running more of them than there are cores buys nothing but
// contention. Like the other budget numbers it is a starting point (§10.4's
// placeholder convention), to be replaced once there is a real measurement.
func defaultBuildConcurrency() int {
	n := runtime.NumCPU()
	if n < 1 {
		return 1
	}
	return n
}

// buildPool runs index builds with a bounded number in flight, and always drains the
// interactive queue before the backfill queue.
//
// Why bound it: triggerBuild used to spawn a goroutine per request with no ceiling.
// Back then a restart over a populated volume rebuilt every historical version's
// missing artifact, which could put hundreds of builds on the CPU, disk and vecstore
// at once — a fresh write arriving during such a sweep waited 601s for READY
// because its build was competing with hundreds of others instead of running.
// Reconcile no longer schedules those rebuilds at all (see ReconcileIndexes), but the
// bound still earns its keep: a batch of version switches, a burst of cold rebuilds or
// a catch-up sweep can each queue more builds than the machine should run at once.
//
// Why prioritise: bounding alone converts the storm into a long queue, and a queue is
// only acceptable if the work someone is WAITING for goes first. A backfill item can
// wait; a query cannot.
//
// The pool is deliberately decoupled from doBuild — it takes the work as a plain
// function — so its two properties can be tested without a vecstore.
type buildPool struct {
	fn     func(buildRequest)
	logger *zap.Logger

	mu          sync.Mutex
	cond        *sync.Cond
	interactive []buildRequest
	backfill    []buildRequest
	closed      bool
	inFlight    int
	wg          sync.WaitGroup
}

// newBuildPool starts size workers. size <= 0 takes defaultBuildConcurrency.
func newBuildPool(size int, fn func(buildRequest), logger *zap.Logger) *buildPool {
	if size <= 0 {
		size = defaultBuildConcurrency()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	p := &buildPool{fn: fn, logger: logger}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(size)
	for i := 0; i < size; i++ {
		go p.worker()
	}
	return p
}

// nextLocked pops the highest-priority queued request. Interactive first, always:
// a backfill item can wait, a query cannot. Callers must hold p.mu.
func (p *buildPool) nextLocked() (buildRequest, bool) {
	if len(p.interactive) > 0 {
		req := p.interactive[0]
		p.interactive = p.interactive[1:]
		return req, true
	}
	if len(p.backfill) > 0 {
		req := p.backfill[0]
		p.backfill = p.backfill[1:]
		return req, true
	}
	return buildRequest{}, false
}

func (p *buildPool) worker() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for len(p.interactive) == 0 && len(p.backfill) == 0 && !p.closed {
			p.cond.Wait()
		}
		if len(p.interactive) == 0 && len(p.backfill) == 0 {
			// Closed with an empty queue: nothing left to drain.
			p.mu.Unlock()
			return
		}
		req, _ := p.nextLocked()
		p.inFlight++
		p.mu.Unlock()

		p.fn(req)

		p.mu.Lock()
		p.inFlight--
		p.mu.Unlock()
	}
}

// Submit queues a build and never blocks. A reconcile sweep can enqueue hundreds of
// requests, and blocking there would stall the sweep instead of merely queueing work.
//
// It returns false only when the pool is closed, so a refused build stays visible to
// the caller rather than looking like a scheduled one.
func (p *buildPool) Submit(req buildRequest) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	if req.priority == BuildPriorityInteractive {
		p.interactive = append(p.interactive, req)
	} else {
		p.backfill = append(p.backfill, req)
	}
	p.cond.Signal()
	return true
}

// Close stops accepting new work and waits for everything already queued to finish.
//
// Draining rather than dropping is deliberate: a queued build is a version that was
// promised an artifact. Abandoning those at shutdown would leave them unbuilt with
// nothing left to notice it — the same silent failure the pool exists to remove.
func (p *buildPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
}
