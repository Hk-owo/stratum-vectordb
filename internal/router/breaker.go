package router

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

// breaker is the §9.5 three-state circuit breaker for one upstream node.
//
// Why it exists (Stratum_设计文档v13.md §9.3): the routing layer's only defence
// against a sick node was "forward, fail, try the next one" — so every client
// paid the failing node's timeout, every time, until an operator noticed. A
// breaker moves that cost to where it belongs: the node that is failing.
//
// It is per-instance, unsynchronised local state, which §9.5 calls out as a
// requirement rather than a limitation: a service station scales horizontally
// precisely because instances share nothing, and a shared breaker table would
// make every instance's view depend on the others'. Each station therefore
// learns a node's health on its own and converges by observing the same node.
//
// The slow-request half of §9.5 ("慢请求比例或错误率超阈值") is left to the error
// rate: a slow request that completes is a latency-budget concern, and
// conflating the two would make a node that is merely under load look dead.
// Timeout-shaped failures still count as failures here, so a hung node does
// trip the breaker — it just trips on the timeout, not on a latency histogram.
type breaker struct {
	cfg breakerConfig

	mu     sync.Mutex
	state  breakerState
	opened time.Time

	// window holds the recent outcomes the ratio is computed over, timestamped
	// so the window is a real duration rather than a sample count: a node that
	// failed five times an hour ago is not failing now.
	window  []outcome
	probes  int // half-open admissions consumed
	probing bool
}

// outcome is one call's result as the breaker sees it.
type outcome struct {
	at      time.Time
	success bool
}

// breakerConfig tunes one breaker. The defaults are starting points: §9.5 says
// the numbers are to be calibrated against a real deployment, so they are
// fields rather than constants.
type breakerConfig struct {
	// failureRatio is the failure share that trips the breaker, once the window
	// holds at least minSamples outcomes.
	failureRatio float64
	// minSamples keeps a single unlucky call from tripping a healthy node.
	minSamples int
	// window is how far back outcomes are counted.
	window time.Duration
	// cooldown is how long an open breaker refuses traffic before it lets a
	// probe through.
	cooldown time.Duration
	// halfOpenProbes is how many consecutive successes close it again. One
	// failure during probing reopens it.
	halfOpenProbes int
}

var defaultBreakerConfig = breakerConfig{
	failureRatio:   0.5,
	minSamples:     5,
	window:         10 * time.Second,
	cooldown:       5 * time.Second,
	halfOpenProbes: 2,
}

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

func (s breakerState) String() string {
	switch s {
	case breakerClosed:
		return "closed"
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

func newBreaker(cfg breakerConfig) *breaker {
	return &breaker{cfg: cfg, state: breakerClosed}
}

// allow reports whether a call may be sent to this node right now.
//
// An open breaker lets nothing through until its cooldown elapses, at which
// point it becomes half-open and admits a bounded number of probes. A
// half-open breaker stops admitting once its probe budget is spent, so a node
// that is still sick is not re-tested by every in-flight request at once.
func (b *breaker) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if now.Sub(b.opened) < b.cfg.cooldown {
			return false
		}
		b.state = breakerHalfOpen
		b.probes = 0
		b.probing = true
	case breakerHalfOpen:
	}

	if b.probes >= b.cfg.halfOpenProbes {
		return false
	}
	b.probes++
	return true
}

// observe feeds one call's outcome to the breaker.
//
// The distinction it draws is the whole point: a breaker asks "can this node
// serve?", not "did this request succeed?". A node answering
// FailedPrecondition ("this version is still building its index") or NotFound
// ("no such knowledge base") is working correctly and telling the truth —
// counting those as failures means a client polling for readiness blacklists
// every healthy replica after minSamples attempts, which is a spectacular way
// for a health mechanism to cause the outage it exists to prevent.
func (b *breaker) observe(err error) {
	if err != nil && !nodeLevelFailure(err) {
		return
	}
	b.record(time.Now(), err == nil)
}

// nodeLevelFailure reports whether err says something about the NODE rather than
// about the request.
//
// Unavailable and DeadlineExceeded are the node failing to hold up its end:
// unreachable, or accepting work and not answering. ResourceExhausted is a node
// shedding load, which is also a capacity fact.
//
// Everything else is an answer. Internal is deliberately excluded even though
// "not leader" travels as Internal: that is a routing mistake the station
// corrects by asking someone else, not a mark against the node that said it.
func nodeLevelFailure(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// record feeds one outcome back. A failure during half-open reopens the
// breaker immediately: the node was given its chance and did not take it.
func (b *breaker) record(now time.Time, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == breakerHalfOpen && !success {
		b.tripLocked(now)
		return
	}

	b.window = append(b.window, outcome{at: now, success: success})
	b.pruneLocked(now)

	if b.state == breakerHalfOpen {
		// The probe budget doubling as the success count is the point: N
		// consecutive admissions that all succeeded close the breaker.
		if b.probes >= b.cfg.halfOpenProbes {
			b.state = breakerClosed
			b.probing = false
			b.window = nil
		}
		return
	}

	if b.state == breakerClosed {
		if failures, total := b.countsLocked(); total >= b.cfg.minSamples &&
			float64(failures)/float64(total) >= b.cfg.failureRatio {
			b.tripLocked(now)
		}
	}
}

// stateNow reports the breaker's current state, advancing an elapsed cooldown
// to half-open. Exposed for tests and diagnostics.
func (b *breaker) stateNow(now time.Time) breakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == breakerOpen && now.Sub(b.opened) >= b.cfg.cooldown {
		b.state = breakerHalfOpen
		b.probes = 0
	}
	return b.state
}

func (b *breaker) tripLocked(now time.Time) {
	b.state = breakerOpen
	b.opened = now
	b.window = nil
	b.probes = 0
	b.probing = false
}

// pruneLocked drops outcomes that fell out of the window.
func (b *breaker) pruneLocked(now time.Time) {
	cutoff := now.Add(-b.cfg.window)
	drop := 0
	for drop < len(b.window) && b.window[drop].at.Before(cutoff) {
		drop++
	}
	if drop > 0 {
		b.window = b.window[drop:]
	}
}

func (b *breaker) countsLocked() (failures, total int) {
	for _, o := range b.window {
		if !o.success {
			failures++
		}
	}
	return failures, len(b.window)
}
