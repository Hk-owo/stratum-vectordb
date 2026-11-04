package router

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// RouteTable is the service station's cache of what the control layer knows and a
// forwarded call needs: the version each knowledge base should be served at, which the
// station stamps on a query as the freshness credential (§9.3(2)), and the storage
// layer's redundancy verdict for the write gate.
//
// WHERE a query goes is not in here. The owner set is a pure function of the version and
// the member list, and a node that cannot serve the version refuses by its own cursor,
// so the station computes the first and learns the second from refusals (Router.routeTiers).
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
	// ready is false until the first successful refresh: an empty table is not
	// the same answer as "nothing is servable".
	ready bool
}

// routeSnapshot is one observation of the cluster.
type routeSnapshot struct {
	// expected[kbID] is the version that knowledge base should currently be
	// served at — the control layer's answer, which becomes the freshness
	// credential on a forwarded query.
	expected map[string]int64

	// degradation[kbID] is the control leader's redundancy verdict for that
	// knowledge base (docs/storage-degradation-signal-plan.md §4.3): whether a
	// write for it can still reach a quorum of the replicas that must hold it.
	//
	// An absent entry means the leader reported no verdict for this KB, which is
	// "no information": never "healthy", never "unavailable". The write gate lets
	// the write through on absence — see Degradation.
	degradation map[string]degradationVerdict
}

// degradationVerdict is one knowledge base's redundancy verdict as the leader
// reported it.
//
// degraded and unavailable are mutually exclusive on the wire — they are the two
// failure tiers of the same judgement (see GetDataVersionHoldersResponse in
// knowledgebase.proto) — and are kept as two fields rather than one tri-state
// because that is what the leader sends.
type degradationVerdict struct {
	degraded bool
	// unavailable means NOT ONE required replica is live: the station names this
	// refusal storage_unavailable rather than kb_storage_degraded, matching what
	// the control layer would have answered directly (§4.1).
	unavailable bool
	detail      string
}

// unhealthy reports whether this verdict says a write cannot land, whichever tier
// it is. Anything asking "can a write succeed here" wants this; anything asking
// "which failure is it" wants the two fields.
func (v degradationVerdict) unhealthy() bool {
	return v.degraded || v.unavailable
}

// Degradation reports whether the last snapshot says a write for kbID cannot
// reach quorum, which failure tier it is, and the leader's diagnosis.
//
// known=false means this station holds no verdict for the KB: no successful
// refresh yet, the KB was absent from the last one, or the leader answered no
// verdict for it (a follower, an unwired replica topology). A caller MUST allow
// the write then. The verdict is soft state — a leadership change empties the
// leader's aggregate — so "cannot tell" has to fall in the harmless direction,
// or a failover becomes a write outage (§3.3, §4.3).
func (t *RouteTable) Degradation(kbID string) (verdict degradationVerdict, known bool) {
	if t == nil {
		return degradationVerdict{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready {
		return degradationVerdict{}, false
	}
	verdict, ok := t.snapshot.degradation[kbID]
	if !ok {
		return degradationVerdict{}, false
	}
	return verdict, true
}

// DegradationSummary reports whether ANY knowledge base in the last snapshot
// cannot be written — either failure tier — with that verdict's diagnosis.
//
// It answers the station's cluster-level questions: the `storage_degraded` flag on
// a read response, and the storage line HealthCheck appends. Both ask "is the
// storage layer unhealthy", where BOTH tiers are a yes — the distinction between
// "short a quorum" and "nobody answering" is worth naming on a refusal (§4.1), but
// not worth hiding from a caller asking whether things are well.
//
// With a single cluster-wide replica topology every KB shares the verdict
// (docs/storage-degradation-signal-plan.md §3.1), so "some KB is unhealthy" and
// "the storage layer is unhealthy" are the same statement; per-KB placement
// (§10.2) is what would make the difference meaningful, and this method is where
// it would have to be drawn.
//
// known=false means the snapshot holds no verdict at all — no successful refresh
// yet, or the leader reported none — and callers must read it as "report
// nothing", never as "healthy".
func (t *RouteTable) DegradationSummary() (unhealthy bool, detail string, known bool) {
	if t == nil {
		return false, "", false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready || len(t.snapshot.degradation) == 0 {
		return false, "", false
	}
	// Sorted so the answer does not depend on map iteration order: a station
	// answering two identical requests with two different diagnoses would be a
	// debugging trap.
	kbs := make([]string, 0, len(t.snapshot.degradation))
	for kbID := range t.snapshot.degradation {
		kbs = append(kbs, kbID)
	}
	sort.Strings(kbs)
	for _, kbID := range kbs {
		if verdict := t.snapshot.degradation[kbID]; verdict.unhealthy() {
			return true, verdict.detail, true
		}
	}
	return false, "", true
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
	// Bounded, because one control node that never answers must not hold the
	// snapshot open indefinitely.
	rctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	snap, err := t.refresh(rctx)
	if err != nil {
		t.logger.Warn("route table refresh failed; keeping the previous snapshot", zap.Error(err))
		return
	}
	t.mu.Lock()
	t.snapshot = snap
	t.ready = true
	t.mu.Unlock()
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

func allIndexes(n int) []int {
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, i)
	}
	return out
}
