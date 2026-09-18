package router

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// RouteTable is the service station's routing cache (§9.3(1)): which storage
// nodes can serve which knowledge base at which version.
//
// It is filled from the CONTROL layer, not by probing storage nodes: the leader
// already folds every node's periodic §7.13.4 cursor report into an in-memory
// aggregate, and answers "which nodes reported holding version V" from it
// (GetDataVersionHolders, §3.1). Reading that is what §4.3(1) asks for —
// "refreshed asynchronously from the control layer, not asking it on every
// query".
//
// The alternative — each station probing every storage node for its cursor, per
// knowledge base, every interval — is the duplication §2.2 keeps out of this
// design: a second view of where data is, maintained on its own schedule, free
// to disagree with the leader's. The station would be answering "who has V"
// differently from the one component whose job that is.
//
// The same refresh also collects the version each KB should currently be served
// at, which the station stamps on a forwarded query as the freshness credential
// (§9.3(2)).
//
// Refreshing is periodic and asynchronous by design: §9.3(1) says the table is
// refreshed in the background rather than consulted per query, so a query costs
// a map read and the control layer is not on the read path.
type RouteTable struct {
	// refresh pulls one snapshot of the world. Supplied by the Router, which
	// owns the connections.
	refresh func(context.Context) (routeSnapshot, error)

	// interval is how often refresh runs.
	interval time.Duration

	logger *zap.Logger

	mu       sync.RWMutex
	snapshot routeSnapshot
	updated  time.Time
	// ready is false until the first successful refresh: an empty table is not
	// the same answer as "nothing is servable".
	ready bool
}

// routeSnapshot is one observation of the cluster.
type routeSnapshot struct {
	// servable[kbID] is the set of storage-node indexes the control leader
	// reported holding that knowledge base at or past its expected version.
	// Indexed by this station's own connection order, resolved from the address
	// each holder reported.
	//
	// An absent or empty entry means the leader named no holder — which is
	// "nobody I have heard from", never "nobody has it" (the aggregate is soft
	// state). Servable treats it as "no narrowing", not as "nothing can serve".
	servable map[string]map[int]bool

	// expected[kbID] is the version that knowledge base should currently be
	// served at — the control layer's answer, which becomes the freshness
	// credential on a forwarded query.
	expected map[string]int64

	// degradation[kbID] is the control leader's redundancy verdict for that
	// knowledge base (docs/storage-degradation-signal-plan.md §4.3): whether a
	// write for it can still reach a quorum of the replicas that must hold it.
	//
	// It rides the same refresh as `servable` because the leader answers both
	// from one aggregate, on the RPC this station already polls (§4.2) — no new
	// channel and no second collection.
	//
	// An absent entry means the leader reported no verdict for this KB, which is
	// "no information": never "healthy", never "unavailable". The write gate lets
	// the write through on absence — see Degradation.
	degradation map[string]degradationVerdict
}

// degradationVerdict is one knowledge base's redundancy verdict as the leader
// reported it.
type degradationVerdict struct {
	degraded bool
	detail   string
}

// Degradation reports whether the last snapshot says a write for kbID cannot
// reach quorum, with the leader's diagnosis.
//
// known=false means this station holds no verdict for the KB: no successful
// refresh yet, the KB was absent from the last one, or the leader answered no
// verdict for it (a follower, an unwired replica topology). A caller MUST allow
// the write then. The verdict is soft state — a leadership change empties the
// leader's aggregate — so "cannot tell" has to fall in the harmless direction,
// or a failover becomes a write outage (§3.3, §4.3).
func (t *RouteTable) Degradation(kbID string) (degraded bool, detail string, known bool) {
	if t == nil {
		return false, "", false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready {
		return false, "", false
	}
	verdict, ok := t.snapshot.degradation[kbID]
	if !ok {
		return false, "", false
	}
	return verdict.degraded, verdict.detail, true
}

// NewRouteTable returns a table refreshed by refresh every interval.
func NewRouteTable(refresh func(context.Context) (routeSnapshot, error), interval time.Duration, logger *zap.Logger) *RouteTable {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &RouteTable{refresh: refresh, interval: interval, logger: logger}
}

// refreshTimeout bounds one snapshot.
//
// Without it, a single unresponsive storage node hangs the refresh forever: the
// cursor query is per node per knowledge base, and a call that connects but never
// answers has nothing above it to cut it off. That would not merely stale the
// table — Start performs the first refresh before returning, so the station
// itself would fail to come up.
const refreshTimeout = 10 * time.Second

// Start refreshes the table periodically until ctx ends.
//
// The first refresh is asynchronous on purpose: whether a snapshot can be taken
// is not a reason for the station to exist or not. Before the first successful
// refresh the table answers "every node", which is the pre-§9 behaviour — so a
// station that cannot reach the control layer yet still serves, rather than
// refusing to start.
func (t *RouteTable) Start(ctx context.Context) {
	if t == nil || t.refresh == nil {
		return
	}
	go t.Refresh(ctx)
	go func() {
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.Refresh(ctx)
			}
		}
	}()
}

// Refresh takes one snapshot. Failures are logged and the previous snapshot is
// kept: stale routing information is worth more than none, and the freshness
// credential is what protects the caller from a stale answer in the meantime.
func (t *RouteTable) Refresh(ctx context.Context) {
	if t == nil || t.refresh == nil {
		return
	}
	// Bounded, because the work is per storage node per knowledge base and one
	// node that never answers must not hold the snapshot open indefinitely.
	rctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	snap, err := t.refresh(rctx)
	if err != nil {
		t.logger.Warn("route table refresh failed; keeping the previous snapshot", zap.Error(err))
		return
	}
	t.mu.Lock()
	t.snapshot = snap
	t.updated = time.Now()
	t.ready = true
	t.mu.Unlock()
}

// Servable returns the storage node indexes the leader reported holding kbID at
// or past minVersion, in ascending order.
//
// A minVersion of 0 means "no credential": every node qualifies, because there
// is nothing to be fresh about.
//
// Before the first successful refresh it also returns every node, and it does the
// same whenever the leader named no holder for the KB. Both are deliberate and
// for the same reason: the aggregate is soft state, so an empty answer means "not
// known" rather than "nothing can serve". Refusing queries on that basis would
// turn a warm-up — or a KB whose reports have not arrived yet — into an outage.
// What protects the caller from a stale answer is not this list but the freshness
// credential on the request: a node that does not hold the version cannot answer
// it at all.
func (t *RouteTable) Servable(kbID string, minVersion int64, nodeCount int) []int {
	if t == nil || minVersion <= 0 {
		return allIndexes(nodeCount)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready {
		return allIndexes(nodeCount)
	}

	set := t.snapshot.servable[kbID]
	if len(set) == 0 {
		return allIndexes(nodeCount)
	}
	out := make([]int, 0, len(set))
	for idx := range set {
		if idx >= 0 && idx < nodeCount {
			out = append(out, idx)
		}
	}
	if len(out) == 0 {
		// Every holder the leader named is one this station has no connection to.
		// Fall back rather than fail: the credential still gates correctness, and
		// "the station's node list and the leader's differ" is a deployment fact,
		// not a reason to refuse reads.
		return allIndexes(nodeCount)
	}
	sort.Ints(out)
	return out
}

// ExpectedVersion returns the version the control layer says kbID should be
// served at, and whether that is known.
//
// Unknown means "no credential to attach": the station forwards the query
// without one, which is the pre-§9 behaviour for any KB the table has not seen —
// a KB created a moment ago, for instance.
func (t *RouteTable) ExpectedVersion(kbID string) (int64, bool) {
	if t == nil {
		return 0, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready {
		return 0, false
	}
	v, ok := t.snapshot.expected[kbID]
	return v, ok
}

// Updated reports when the table was last refreshed successfully.
func (t *RouteTable) Updated() time.Time {
	if t == nil {
		return time.Time{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.updated
}

func allIndexes(n int) []int {
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, i)
	}
	return out
}
