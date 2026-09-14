package plane

import (
	"context"
	"fmt"
	"sync"
)

// DefaultMaxInFlightWrites caps how many writes may be in flight for one
// knowledge base at once (Stratum_设计文档v13.md §7.7). It is a §10.4 placeholder:
// low enough that a stalled version cannot pile up an unbounded backlog on the
// storage nodes, high enough to leave ordinary write concurrency intact.
const DefaultMaxInFlightWrites = 8

// perKBGate counts the writes currently in flight for one knowledge base.
//
// A release closes `wait` and installs a fresh one, which is how a waiter is
// woken without broadcasting: only one slot opens per release, so only one
// waiter needs to learn about it. It also keeps the limit adjustable — a
// buffered channel would freeze the limit at creation time.
type perKBGate struct {
	n    int
	wait chan struct{}
}

// writeLimiter bounds in-flight writes per knowledge base: the control layer
// may dispatch versions concurrently, but the storage layer is where a stuck
// version would otherwise let the rest pile up behind it
// (Stratum_设计文档v13.md §7.7).
//
// Reaching the limit queues the newcomer rather than failing it. The write is
// legitimate; it just has to wait for a slot. A version that is genuinely stuck
// is the retry budget's and the terminal verdict's problem (§10.1), not a
// reason to start refusing new work.
type writeLimiter struct {
	mu           sync.Mutex
	gates        map[string]*perKBGate
	defaultLimit int
	kbLimits     map[string]int
}

func newWriteLimiter(defaultLimit int) *writeLimiter {
	if defaultLimit <= 0 {
		defaultLimit = DefaultMaxInFlightWrites
	}
	return &writeLimiter{
		gates:        make(map[string]*perKBGate),
		defaultLimit: defaultLimit,
		kbLimits:     make(map[string]int),
	}
}

// limitFor returns the effective limit for kbID. Callers hold l.mu.
func (l *writeLimiter) limitFor(kbID string) int {
	if n, ok := l.kbLimits[kbID]; ok {
		return n
	}
	return l.defaultLimit
}

// SetLimit overrides the in-flight limit for one knowledge base. A non-positive
// value clears the override, returning that KB to the default.
func (l *writeLimiter) SetLimit(kbID string, n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n <= 0 {
		delete(l.kbLimits, kbID)
		return
	}
	l.kbLimits[kbID] = n
}

// Acquire takes a slot for kbID, blocking until one is free or ctx ends.
//
// Blocking is the point: the caller is a write with every right to proceed, and
// failing fast would push the backpressure out to clients as spurious errors
// rather than keeping it where the backlog actually forms.
func (l *writeLimiter) Acquire(ctx context.Context, kbID string) error {
	for {
		l.mu.Lock()
		gate, ok := l.gates[kbID]
		if !ok {
			gate = &perKBGate{wait: make(chan struct{})}
			l.gates[kbID] = gate
		}
		if gate.n < l.limitFor(kbID) {
			gate.n++
			l.mu.Unlock()
			return nil
		}
		wait := gate.wait
		l.mu.Unlock()

		select {
		case <-wait:
			// A slot opened. Loop rather than proceeding: another waiter may
			// have taken it first.
		case <-ctx.Done():
			return fmt.Errorf("plane: waiting for a write slot for %s: %w", kbID, ctx.Err())
		}
	}
}

// Release frees a slot and wakes one waiter. Releasing without a slot in hand
// is ignored rather than allowed to push the count negative.
func (l *writeLimiter) Release(kbID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	gate, ok := l.gates[kbID]
	if !ok || gate.n == 0 {
		return
	}
	gate.n--
	close(gate.wait)
	gate.wait = make(chan struct{})
}

// InFlight reports how many writes are currently in flight for kbID.
func (l *writeLimiter) InFlight(kbID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gate, ok := l.gates[kbID]; ok {
		return gate.n
	}
	return 0
}
