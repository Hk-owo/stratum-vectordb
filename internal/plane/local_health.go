package plane

import (
	"sync"
	"time"
)

// LocalHealthView is one node's own record of which peers it has recently been
// able to reach (Stratum_设计文档v13.md §7.13.5).
//
// It is deliberately small, and every property below comes straight from that
// section:
//
//   - No authority. Every node keeps its own view; nothing is elected, and a
//     node that dies takes only its own observations with it. A shared view
//     would need a maintainer, and a maintainer needs election and failover —
//     the exact consensus problem this avoids.
//   - Fed by real traffic. An observation is a call the node was making
//     anyway (a dispatch attempt against a candidate). There is no heartbeat,
//     no ping loop, no probe subsystem to schedule or tune, which is what §7.13.5
//     rules out in as many words.
//   - Optimization only, never correctness. It REORDERS candidates; it never
//     removes one. Acting on a wrong verdict costs one extra attempt, not a
//     failed write — which is what makes a purely local, unsynchronized view
//     safe to act on.
//   - It does not leave the node. Not in ReportEpoch, not reported to the
//     control layer, not shared with the station (which runs its own health
//     checks, with its own thresholds and cadence — two views disagreeing is
//     expected, not a bug to reconcile).
type LocalHealthView struct {
	// demoteAfter is how many consecutive failures push a peer to the back of
	// the candidate order.
	demoteAfter int

	// demoteFor is how long that lasts. Without a bound, one bad minute would
	// demote a peer for the life of the process; with it, the view forgets on
	// its own instead of needing the peer to succeed before it is trusted
	// again — and a peer that is merely slow can still be reached, because a
	// demoted candidate is still tried (last).
	demoteFor time.Duration

	// now is injectable so tests do not sleep.
	now func() time.Time

	mu      sync.Mutex
	entries map[string]*peerHealth
}

// peerHealth is what the view remembers about one peer.
type peerHealth struct {
	consecutiveFailures int
	// demotedAt is when the current demotion started; zero means not demoted.
	demotedAt time.Time
}

// HealthViewConfig tunes a LocalHealthView. The zero value takes the defaults,
// which is what the node assembly passes: these numbers are heuristic and
// deliberately not exposed as knobs until a deployment has a reason.
type HealthViewConfig struct {
	// DemoteAfter is the consecutive-failure threshold. <= 0 takes the default.
	DemoteAfter int
	// DemoteFor is how long a demotion lasts. <= 0 takes the default.
	DemoteFor time.Duration

	// Now is injectable for tests.
	Now func() time.Time
}

const (
	// defaultDemoteAfter: one failure is noise (a restarting peer, a dropped
	// packet). Three in a row is a pattern worth acting on — and acting on it
	// wrongly costs one attempt, so the threshold can be low.
	defaultDemoteAfter = 3

	// defaultDemoteFor bounds the demotion so the view cannot go stale
	// forever. It is short enough that a recovered peer is tried again within
	// the same write's retry budget rather than on the next write.
	defaultDemoteFor = 30 * time.Second
)

// NewLocalHealthView returns a view with cfg's thresholds, or the defaults.
func NewLocalHealthView(cfg HealthViewConfig) *LocalHealthView {
	demoteAfter := cfg.DemoteAfter
	if demoteAfter <= 0 {
		demoteAfter = defaultDemoteAfter
	}
	demoteFor := cfg.DemoteFor
	if demoteFor <= 0 {
		demoteFor = defaultDemoteFor
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &LocalHealthView{
		demoteAfter: demoteAfter,
		demoteFor:   demoteFor,
		now:         now,
		entries:     make(map[string]*peerHealth),
	}
}

// Observe records the outcome of one real call to addr.
//
// A success clears the peer's history outright: the failures that preceded it
// describe a window that has closed, and keeping a partial count would demote
// a peer for two old failures plus one new one.
func (v *LocalHealthView) Observe(addr string, ok bool) {
	if v == nil || addr == "" {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	entry, exists := v.entries[addr]
	if !exists {
		entry = &peerHealth{}
		v.entries[addr] = entry
	}
	if ok {
		entry.consecutiveFailures = 0
		entry.demotedAt = time.Time{}
		return
	}
	entry.consecutiveFailures++
	if entry.consecutiveFailures >= v.demoteAfter && entry.demotedAt.IsZero() {
		entry.demotedAt = v.now()
	}
}

// Rank returns addrs with recently-unreachable peers moved to the back.
//
// Two properties matter more than the ordering itself:
//
//   - It is stable within each group, so the caller's own order (a sorted list
//     that makes a failing sequence reproducible in logs) survives wherever the
//     view has nothing to say.
//   - It never drops an address. A demoted candidate is still tried, just last,
//     so a wrong verdict costs one attempt rather than a write.
func (v *LocalHealthView) Rank(addrs []string) []string {
	if v == nil || len(addrs) < 2 {
		return addrs
	}
	v.mu.Lock()
	now := v.now()
	demoted := make(map[string]bool, len(v.entries))
	for addr, entry := range v.entries {
		if entry.demotedAt.IsZero() {
			continue
		}
		if now.Sub(entry.demotedAt) >= v.demoteFor {
			// The demotion has expired. Clearing it here (rather than only on
			// the next Observe) is what lets a peer that never gets called
			// again stop being penalized for an old failure.
			entry.demotedAt = time.Time{}
			continue
		}
		demoted[addr] = true
	}
	v.mu.Unlock()

	if len(demoted) == 0 {
		return addrs
	}
	out := make([]string, 0, len(addrs))
	var poor []string
	for _, addr := range addrs {
		if demoted[addr] {
			poor = append(poor, addr)
			continue
		}
		out = append(out, addr)
	}
	return append(out, poor...)
}
