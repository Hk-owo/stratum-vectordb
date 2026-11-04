package router

import (
	"context"
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

	// degradation is the control leader's redundancy verdict
	// (docs/storage-degradation-signal-plan.md §4.3): whether a write can still reach a
	// quorum of the replicas that must hold it. There is one replica topology for the
	// whole cluster, so there is one verdict rather than one per knowledge base.
	//
	// degradationKnown=false is "no information": never "healthy", never "unavailable".
	// The write gate lets the write through then — see Degradation.
	degradation      degradationVerdict
	degradationKnown bool
}

// degradationVerdict is the redundancy verdict as the leader reported it.
//
// degraded and unavailable are mutually exclusive on the wire — they are the two
// failure tiers of the same judgement (see GetStorageDegradationResponse in
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

// Degradation reports the last snapshot's verdict on whether a write can reach quorum,
// which failure tier it is, and the leader's diagnosis.
//
// known=false means this station holds no verdict: no successful refresh yet, or the
// leader answered none (a follower, an unwired replica topology). A caller MUST allow the
// write then. The verdict is soft state — a leadership change empties the leader's view —
// so "cannot tell" has to fall in the harmless direction, or a failover becomes a write
// outage (§3.3, §4.3).
func (t *RouteTable) Degradation() (verdict degradationVerdict, known bool) {
	if t == nil {
		return degradationVerdict{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready || !t.snapshot.degradationKnown {
		return degradationVerdict{}, false
	}
	return t.snapshot.degradation, true
}

// DegradationSummary answers the station's cluster-level questions: the
// `storage_degraded` flag on a read response, and the storage line HealthCheck appends.
// Both ask "is the storage layer unhealthy", where BOTH tiers are a yes — the distinction
// between "short a quorum" and "nobody answering" is worth naming on a refusal (§4.1), but
// not worth hiding from a caller asking whether things are well.
//
// known=false means the caller must report nothing, never "healthy".
func (t *RouteTable) DegradationSummary() (unhealthy bool, detail string, known bool) {
	verdict, known := t.Degradation()
	if !known {
		return false, "", false
	}
	if !verdict.unhealthy() {
		return false, "", true
	}
	return true, verdict.detail, true
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
