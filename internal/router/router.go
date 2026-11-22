package router

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"go.uber.org/zap"
	pb "stratum/api/proto/stratum"

	"stratum/internal/authmeta"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
	"stratum/internal/wire"
)

// Config configures a Router.
//
// The two address lists exist because the two layers no longer answer the same
// services (docs/参考/历史/Stratum_设计文档v13.md §11 阶段 ④). QueryService and AdminService take
// the index manager and the local stores in their constructors, so only a node
// that holds data can serve them; KnowledgeBaseService is metadata and belongs
// to the control layer. A router that treated every node as interchangeable
// would send a query to a node that has no indices to search — which is not a
// hypothetical, it is what it did.
type Config struct {
	// Required for per-KB placement. The executable enables it unconditionally;
	// false retains compatibility for legacy embedded/test router stacks.
	RequireDirectory bool
	// Addrs is the control-layer nodes: metadata reads and writes. On the
	// all-in-one topology these are also the storage nodes.
	Addrs []string

	// StorageAddrs is the storage-layer nodes: the ones that serve queries and
	// admin operations, because those need local data.
	//
	// Empty means "the same nodes as Addrs" — the all-in-one topology, where
	// every node is both. That is the behaviour the router has always had, so
	// an unmodified deployment keeps working.
	StorageAddrs []string

	// OwnerInactiveK is how many replicas should hold a NON-active version's
	// index (docs/已实施/index-owner-sharding-plan.md §3). <= 0 disables sharding: every
	// node qualifies for every version, which is the pre-sharding behaviour.
	//
	// The active version always keeps its whole replica set, because any node may
	// be asked for it. That is not a special case in the owner math — it is just
	// k = everyone for that one version.
	OwnerInactiveK int

	// OwnerPriorityHalfLife is how long it takes a version's query score to decay
	// by half, and it is also the switch: <= 0 leaves every non-active version at
	// exactly OwnerInactiveK, which is the behaviour before priorities existed.
	// DefaultPriorityHalfLife is what the station's flag defaults to, so a
	// deployment that says nothing gets the priority ladder; 0 turns it off.
	//
	// The ladder itself is DefaultPriorityTable, deliberately not configurable
	// yet: the thresholds are a placeholder to be calibrated against real traffic,
	// and exposing them now would invite tuning against a number nobody has
	// measured.
	OwnerPriorityHalfLife time.Duration

	// Auth is the station's authentication checkpoint (§9.3(5)). nil means
	// "deployment controls access by isolation" — the pre-§9 arrangement —
	// which is why it is optional rather than required: a cluster that has not
	// adopted a station keeps working, and one that has gets the checkpoint for
	// free on every forwarded call.
	Auth *Authenticator

	// Mark is the shared secret forwarded calls are stamped with, so a node can
	// tell "the station sent this" from "something reached my port directly"
	// (internal/authmeta.Signer). nil, or a signer whose key is empty, stamps
	// nothing — fine for a cluster whose nodes do not require the mark, fatal
	// for one that does: a node requiring it rejects an unmarked forward, and
	// the station has no other way to reach it. The two ends are configured
	// together, from the same secret.
	Mark *authmeta.Signer

	// RouteRefreshInterval is how often the routing cache re-observes the
	// storage layer's cursors and the control layer's expected versions
	// (§9.3(1)). Zero means the default (5s). Negative disables the table,
	// which makes the station route every query to every storage node — the
	// pre-§9 behaviour.
	RouteRefreshInterval time.Duration

	// Logger receives the station's own share of a write's cost (the per-stage
	// timings of forwardWrite) and the routing refreshes' failures. Optional:
	// nil means a no-op logger, which is what every station had before there
	// were any timings. Debug level is what those timings are emitted at, so a
	// station left at info logs nothing extra.
	Logger *zap.Logger
}

// Router is the routing layer core: it holds connections to both layers,
// discovers the current leader, and routes unary calls to the layer that can
// actually serve them.
//
// The index space of fn's idx argument follows the route: a control-routed call
// indexes the control clients (kbs), a storage-routed one indexes the storage
// clients (querys, admins).
type Router struct {
	requireDirectory bool
	storagePool      *storagePool
	directories      []pb.InternalServiceClient
	// dirPreferred is the index in directories of the control node that answered
	// the last directory read.
	//
	// Why it exists: only the control LEADER answers GetStorageDirectory (§13.3), and
	// the refresh used to walk the list from index 0 every time. With three control
	// nodes and a leader that is not the first, that is one guaranteed wasted round
	// trip per refresh — measured on the 5-storage-node cluster during a performance
	// run: 1,414 `Unavailable/GetStorageDirectory` calls on control1 (the non-leader
	// at the head of the list) against 1,380 successful ones, i.e. half of the
	// control plane's directory traffic answered nothing.
	//
	// Starting from the last node that worked turns "one wasted hop per refresh" into
	// "one wasted hop per leadership change". A leadership change costs exactly one
	// failed probe, because the next success overwrites this.
	dirPreferred atomic.Int32
	controlAddrs []string
	storageAddrs []string
	conns        []*grpc.ClientConn
	kbs          []pb.KnowledgeBaseServiceClient // control layer

	// logger records the background refreshes' failures. A refresh failure is
	// expected and survivable (the previous snapshot stands), so it must be
	// visible without being alarming — hence Debug for a KB that could not be
	// narrowed, and the table's own Warn for a refresh that failed outright.
	logger     *zap.Logger
	querys     []pb.QueryServiceClient // storage layer
	admins     []pb.AdminServiceClient // storage layer
	discoverer leaderResolver
	rr         atomic.Uint64

	// One breaker per node per layer (§9.5). Kept per instance and never
	// shared: a service station scales horizontally because instances hold
	// nothing in common, and a shared table would tie each one's view of a node
	// to the others'.
	controlBreakers []*breaker
	storageBreakers []*breaker

	// controlInflight / storageInflight count this station's outstanding calls per
	// node, per layer — the same split the breakers use, and for the same reason:
	// a slow metadata read and a slow query are different observations.
	//
	// Only this station's own calls are counted. Another station's load on the
	// same node is invisible here, which is the trade P2C makes for needing no
	// shared state at all.
	controlInflight []*inflight
	storageInflight []*inflight

	auth *Authenticator

	// mark stamps each forwarded call so a node can tell it came through this
	// station (see Config.Mark). Never nil after NewRouter — an unconfigured
	// deployment gets a disabled signer, which stamps nothing.
	mark *authmeta.Signer

	// syncs are the storage layer's data-plane clients: the station asks them whether a
	// circuit-broken node is back (probeOpenBreakers).
	syncs []pb.DataSyncServiceClient

	// routes is the station's local cache of each knowledge base's expected version
	// and the storage layer's redundancy verdict. nil when disabled.
	routes *RouteTable

	// lag is what this station learned from refusals: which nodes were behind on which
	// knowledge base. nil means nothing is remembered, and every node stays in line.
	lag *lagRecords

	// ownerInactiveK is Config.OwnerInactiveK: the BASE owner count for a version
	// that is not active — the floor the priority ladder raises from.
	ownerInactiveK int

	// priority raises k for versions the cluster keeps querying, and lets it fall
	// back for versions it stopped querying. Nil means "every non-active version
	// gets exactly OwnerInactiveK", which is the pre-priority behaviour.
	priority *indexPriority
}

// NewRouter dials every node address (non-blocking, like the HTTP
// gateway) and builds the leader discoverer.
func NewRouter(cfg Config) (*Router, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("router: no node addresses configured")
	}
	storage := cfg.StorageAddrs
	if len(storage) == 0 {
		// All-in-one: every node serves both layers.
		storage = cfg.Addrs
	}
	r := &Router{
		requireDirectory: cfg.RequireDirectory,
		controlAddrs:     append([]string(nil), cfg.Addrs...),
		storageAddrs:     append([]string(nil), storage...),
		auth:             cfg.Auth,
		mark:             cfg.Mark,
		logger:           cfg.Logger,
		ownerInactiveK:   cfg.OwnerInactiveK,
		priority:         newIndexPriority(cfg.OwnerPriorityHalfLife, nil),
		controlInflight:  newInflight(len(cfg.Addrs)),
		storageInflight:  newInflight(len(storage)),
	}
	if cfg.RequireDirectory {
		r.storagePool = newStoragePool()
	}
	if r.logger == nil {
		r.logger = zap.NewNop()
	}
	dial := func(addrs []string) error {
		d := rpcpool.DefaultDialer()
		for _, a := range addrs {
			conn, err := d.Dial(a)
			if err != nil {
				return err
			}
			r.conns = append(r.conns, conn)
		}
		return nil
	}
	if err := dial(cfg.Addrs); err != nil {
		r.Close()
		return nil, err
	}
	if !cfg.RequireDirectory {
		if err := dial(storage); err != nil {
			r.Close()
			return nil, err
		}
	}
	// Connections are appended in the same order the client slices are built
	// below, so the two stay aligned.
	//
	// discoveryAdmins is built here, beside kbs, because it is the CONTROL
	// layer's AdminService — not the same list as r.admins below it.
	discoveryAdmins := make([]statusClient, 0, len(cfg.Addrs))
	for i := 0; i < len(cfg.Addrs); i++ {
		r.directories = append(r.directories, pb.NewInternalServiceClient(r.conns[i]))
		r.kbs = append(r.kbs, pb.NewKnowledgeBaseServiceClient(r.conns[i]))
		discoveryAdmins = append(discoveryAdmins, pb.NewAdminServiceClient(r.conns[i]))
	}
	for i := len(cfg.Addrs); i < len(r.conns); i++ {
		r.querys = append(r.querys, pb.NewQueryServiceClient(r.conns[i]))
		r.admins = append(r.admins, pb.NewAdminServiceClient(r.conns[i]))
		r.syncs = append(r.syncs, pb.NewDataSyncServiceClient(r.conns[i]))
	}
	// Leader discovery asks the CONTROL nodes, and which node answers decides
	// what the answer means.
	//
	// GetClusterStatus reports {node_id: the answering node's own ID, leader_id:
	// the ID it believes leads}, and LeaderNow resolves leader_id to a position
	// in the list it was handed. Both IDs are therefore read in the ID space of
	// whoever answers, and the two layers do not share one: a control node
	// reports 1..N, a storage node in the split topology reports 11..1N
	// (docs/参考/历史/Stratum_设计文档v13.md §11). Pointing this at the storage layer — which the
	// station did, under a comment claiming a control node builds no
	// AdminService, untrue since cmd/stratum/control_admin.go taught it to answer
	// the Raft view — keyed nodeByID on storage IDs while the votes named control
	// IDs. No vote could ever resolve: ok was false for every write.
	//
	// The cost was not "discovery is a no-op". Every write fell through to
	// tryAll, which broadcasts to every control node; the two that do not lead
	// forward the proposal to whoever does, and when that node is the one that
	// just died they come back Unavailable. The breaker counts that against THEM
	// — the healthy nodes — so killing one node tripped all three breakers, and
	// the station then refused for the rest of its retry budget with "every
	// control node is circuit-broken". That is the shape CI kept failing
	// TestT4_MinorityFaultTolerance in.
	r.discoverer = NewLeaderDiscoverer(discoveryAdmins)

	for range r.controlAddrs {
		r.controlBreakers = append(r.controlBreakers, newBreaker(defaultBreakerConfig))
	}
	for range r.storageAddrs {
		r.storageBreakers = append(r.storageBreakers, newBreaker(defaultBreakerConfig))
	}

	if cfg.RouteRefreshInterval >= 0 {
		r.routes = NewRouteTable(r.refreshRouteSnapshot, cfg.RouteRefreshInterval, nil)
		r.lag = newLagRecords(defaultLagRecordTTL)
	}
	return r, nil
}

// Start begins the background refreshes the routing table needs (§9.3(1)). It
// is separate from NewRouter because a caller may want the station serving
// before the first refresh lands — the table's pre-refresh answer is "every
// node", not "none".
func (r *Router) Start(ctx context.Context) {
	if r.routes != nil {
		r.routes.Start(ctx)
	}
	if r.storagePool != nil {
		r.storagePool.enableProbes()
		go r.probeOpenBreakers(ctx, breakerProbeInterval)
	} else if len(r.syncs) > 0 && len(r.syncs) == len(r.storageBreakers) {
		for _, b := range r.storageBreakers {
			b.useExternalProbe()
		}
		go r.probeOpenBreakers(ctx, breakerProbeInterval)
	}
}

const (
	// breakerProbeInterval is how often the prober looks for an open breaker that has
	// waited out its cooldown. A healthy cluster costs nothing: no breaker is open, so
	// nothing is sent.
	breakerProbeInterval = time.Second
	// breakerProbeTimeout bounds one probe, so a hung node cannot stall the others.
	breakerProbeTimeout = 2 * time.Second
)

// probeOpenBreakers finds out when a circuit-broken storage node is back, so no client
// request has to fail over to learn the same thing.
//
// It asks LocalVersion rather than HealthCheck. A storage node answers HealthCheck by
// reading the cluster status from the control tier, so during a control outage a healthy
// storage node would keep failing its probe, and reads it could serve would stay blocked.
// LocalVersion is answered from the node's own memory, which is the question here.
func (r *Router) probeOpenBreakers(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.probeOnce(ctx)
		}
	}
}

// probeOnce probes every storage node whose breaker has waited out its cooldown.
func (r *Router) probeOnce(ctx context.Context) {
	if r.storagePool != nil {
		r.storagePool.probe(r.mark.Stamp(ctx))
	}
	for idx, b := range r.storageBreakers {
		if idx >= len(r.syncs) || !b.needsProbe(time.Now()) {
			continue
		}
		probeCtx, cancel := context.WithTimeout(r.mark.Stamp(ctx), breakerProbeTimeout)
		_, err := r.syncs[idx].LocalVersion(probeCtx, &pb.LocalVersionRequest{})
		cancel()
		// Only a node-level failure keeps it open: an answer of any kind means the node
		// is up and handling requests.
		b.probeResult(time.Now(), err == nil || !nodeLevelFailure(err))
	}
}

// refreshRouteSnapshot observes the cluster once: which knowledge bases exist and what
// version each should be served at, and the storage layer's redundancy verdict. One RPC
// for each, however many knowledge bases there are.
// dirOrder is the order in which control nodes are asked for the directory:
// preferred first, then the rest in their original order.
//
// Pure and separate from the caller so the wrap-around and the out-of-range cases
// are testable without a gRPC server — preferred comes from a badged atomic that a
// previous refresh wrote, and a stale index (fewer control nodes than before) must
// not become an out-of-range panic on the first call after a configuration change.
func dirOrder(preferred, n int) []int {
	if n <= 0 {
		return nil
	}
	if preferred < 0 || preferred >= n {
		preferred = 0
	}
	order := make([]int, 0, n)
	for i := 0; i < n; i++ {
		order = append(order, (preferred+i)%n)
	}
	return order
}

func (r *Router) refreshRouteSnapshot(ctx context.Context) (routeSnapshot, error) {
	// Everything the refresh sends to the control layer carries the internal mark.
	//
	// These are client-facing RPCs (service.UnaryAuthGate), so a cluster that
	// enforces authentication rejects them like any unauthenticated caller — and
	// the failure is invisible: Refresh logs it and keeps the previous snapshot, so
	// the table simply never narrows and every query still "works" by going
	// everywhere. That is exactly the kind of silent degradation this design keeps
	// trying to avoid, and it was live until this mark was added.
	ctx = r.mark.Stamp(ctx)

	snap := routeSnapshot{expected: map[string]int64{}}
	if r.requireDirectory {
		var lastErr error
		for _, idx := range dirOrder(int(r.dirPreferred.Load()), len(r.directories)) {
			client := r.directories[idx]
			resp, err := client.GetStorageDirectory(ctx, &pb.GetStorageDirectoryRequest{})
			if err != nil {
				lastErr = err
				continue
			}
			r.dirPreferred.Store(int32(idx))
			dir := wire.StorageDirectoryFromProto(resp)
			if r.storagePool != nil {
				r.storagePool.reconcile(dir)
			}
			snap.directory = &dir
			snap.expected = dir.ActiveVersions
			snap.health = make(map[string]storage.Health, len(resp.GetHealth()))
			for kb, h := range resp.GetHealth() {
				snap.health[kb] = storage.Health{Known: h.GetKnown(), Degraded: h.GetDegraded(), Unavailable: h.GetUnavailable(), Detail: h.GetDetail()}
			}
			return snap, nil
		}
		if lastErr == nil {
			lastErr = stratumerrors.ErrPlacementUnknown
		}
		return routeSnapshot{}, lastErr
	}

	// The control layer is the authority on what exists and what version should
	// be visible; any control node can answer, they share the log.
	var kbResp *pb.ListKnowledgeBasesResponse
	var lastErr error
	for i := range r.kbs {
		resp, err := r.kbs[i].ListKnowledgeBases(ctx, &pb.ListKnowledgeBasesRequest{})
		if err != nil {
			lastErr = err
			continue
		}
		kbResp = resp
		break
	}
	if kbResp == nil {
		return routeSnapshot{}, lastErr
	}
	for _, kb := range kbResp.GetKnowledgeBases() {
		if kb.GetActiveVersionId() == 0 {
			// A knowledge base with no active version — nothing written yet. It is
			// left out of the table rather than recorded as version 0: "who holds
			// version 0" has no answer, so asking would narrow the KB to nothing by
			// accident, and the freshness credential below would carry a version
			// that exists nowhere (docs/已实施/cursor-persistence-plan.md §5.3).
			continue
		}
		snap.expected[kb.GetKnowledgeBaseId()] = kb.GetActiveVersionId()
	}

	// The redundancy verdict is one question per refresh, not one per knowledge base:
	// with a single cluster-wide replica topology every knowledge base shares it.
	//
	// A failed query, or an answer from a node that cannot judge, leaves the verdict
	// unknown, and the write gate reads unknown as allow. The freshness credential still
	// rides the forwarded query, so nothing about correctness depends on this succeeding.
	snap.degradation, snap.degradationKnown = r.storageDegradation(ctx)
	return snap, nil
}

// Close closes every node connection.
func (r *Router) Close() {
	if r.storagePool != nil {
		r.storagePool.close()
	}
	for _, c := range r.conns {
		_ = c.Close()
	}
}

// loggerOrNop returns the configured logger, or a no-op one.
//
// NewRouter guarantees a non-nil logger, but a Router built directly — which the
// tests do, to avoid dialling anything — has none, and a nil *zap.Logger is not a
// usable no-op: its methods dereference the receiver and panic. The accessor is
// what keeps a missing logger from being a crash on a path that only logs.
func (r *Router) loggerOrNop() *zap.Logger {
	if r.logger == nil {
		return zap.NewNop()
	}
	return r.logger
}
