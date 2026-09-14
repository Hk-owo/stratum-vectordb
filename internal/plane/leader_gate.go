package plane

import "sync"

// LeaderGate derives "am I the control leader" from a cheap local check and runs a
// takeover hook when this node BECOMES the leader — the one transition that
// invalidates leadership-scoped soft state.
//
// Why a hook rather than exposing the raw check: §7.13.4's aggregate is per-term.
// A node that keeps a predecessor's reports would answer "who holds version V"
// from evidence it never received, and that answer decides where data is fetched
// from. Clearing on the false→true edge is the narrowest correct point — while a
// node is a follower its aggregate is not being read at all.
//
// The check is polled rather than pushed, because the Raft layer exposes no
// leadership callback. The consequence is that the clear happens on the first
// observation of the new term, which is always before the aggregate is consulted:
// every read path asks IsLeader first.
type LeaderGate struct {
	mu         sync.Mutex
	wasLeader  bool
	isLeader   func() bool
	onTakeover func()
}

// NewLeaderGate wraps isLeader; onTakeover runs once each time IsLeader observes
// this node going from follower to leader. A nil isLeader means "never the
// leader" — the single-node and test default.
func NewLeaderGate(isLeader func() bool, onTakeover func()) *LeaderGate {
	return &LeaderGate{isLeader: isLeader, onTakeover: onTakeover}
}

// IsLeader reports whether this node currently leads, running the takeover hook
// when the answer flips from false to true.
func (g *LeaderGate) IsLeader() bool {
	leader := g.isLeader != nil && g.isLeader()

	g.mu.Lock()
	tookOver := leader && !g.wasLeader
	g.wasLeader = leader
	g.mu.Unlock()

	if tookOver && g.onTakeover != nil {
		g.onTakeover()
	}
	return leader
}
