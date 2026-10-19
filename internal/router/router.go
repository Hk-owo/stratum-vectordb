package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"go.uber.org/zap"
	pb "stratum/api/proto/stratum"

	"stratum/internal/authmeta"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/owner"
)

// Config configures a Router.
//
// The two address lists exist because the two layers no longer answer the same
// services (Stratum_设计文档v13.md §11 阶段 ④). QueryService and AdminService take
// the index manager and the local stores in their constructors, so only a node
// that holds data can serve them; KnowledgeBaseService is metadata and belongs
// to the control layer. A router that treated every node as interchangeable
// would send a query to a node that has no indices to search — which is not a
// hypothetical, it is what it did.
type Config struct {
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
	// index (docs/index-owner-sharding-plan.md §3). <= 0 disables sharding: every
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
	controlAddrs []string
	storageAddrs []string
	conns        []*grpc.ClientConn
	kbs          []pb.KnowledgeBaseServiceClient // control layer

	// storageIndexByAddr maps a storage node's reported address to this station's
	// connection index for it. The leader's aggregate identifies holders by the
	// address each node reported for itself; turning that into a candidate means
	// matching it against the addresses this station dials, which are the same
	// list (storage.nodes in the config, -storage-nodes here).
	storageIndexByAddr map[string]int

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

	// syncs are the storage layer's data-plane clients: the station asks them
	// for cursors to build the routing table (§9.3(1)), using the §7.6 query
	// rather than a new interface.
	syncs []pb.DataSyncServiceClient

	// routes is the station's local cache of "which node can serve which KB at
	// which version". nil when disabled.
	routes *RouteTable

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
		controlAddrs:       append([]string(nil), cfg.Addrs...),
		storageAddrs:       append([]string(nil), storage...),
		storageIndexByAddr: make(map[string]int, len(storage)),
		auth:               cfg.Auth,
		mark:               cfg.Mark,
		logger:             cfg.Logger,
		ownerInactiveK:     cfg.OwnerInactiveK,
		priority:           newIndexPriority(cfg.OwnerPriorityHalfLife, nil),
		controlInflight:    newInflight(len(cfg.Addrs)),
		storageInflight:    newInflight(len(storage)),
	}
	if r.logger == nil {
		r.logger = zap.NewNop()
	}
	// Indexed by position in storage, which is the order the storage clients are
	// built in below — so an address the leader names resolves to the same node
	// this station would dial for it.
	for idx, addr := range storage {
		r.storageIndexByAddr[addr] = idx
	}
	dial := func(addrs []string) error {
		for _, a := range addrs {
			conn, err := grpc.Dial(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	if err := dial(storage); err != nil {
		r.Close()
		return nil, err
	}
	// Connections are appended in the same order the client slices are built
	// below, so the two stay aligned.
	//
	// discoveryAdmins is built here, beside kbs, because it is the CONTROL
	// layer's AdminService — not the same list as r.admins below it.
	discoveryAdmins := make([]statusClient, 0, len(cfg.Addrs))
	for i := 0; i < len(cfg.Addrs); i++ {
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
	// (Stratum_设计文档v13.md §11). Pointing this at the storage layer — which the
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
}

// refreshRouteSnapshot observes the cluster once, by asking the control layer
// both halves of the question: which knowledge bases exist and what version each
// should be served at, and which storage nodes the leader's §7.13.4 aggregate
// says hold that version (§3.1, §4.3(1)).
//
// The leader that can answer either question is whichever one is leading — any
// control node holds the log, but only the leader folds the cursor reports, so
// the holders query is retried across the control set like any other read.
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

	snap := routeSnapshot{
		servable:    map[string]map[int]bool{},
		expected:    map[string]int64{},
		degradation: map[string]degradationVerdict{},
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
			// that exists nowhere (docs/cursor-persistence-plan.md §5.3).
			continue
		}
		snap.expected[kb.GetKnowledgeBaseId()] = kb.GetActiveVersionId()
	}

	// Per knowledge base, ask which nodes reported holding its expected version,
	// and whether a write for it can still reach quorum.
	//
	// A KB whose query fails, or whose answer comes from a non-leader, is simply
	// left out: Servable then does not narrow for it, which is the same answer it
	// gives before the first refresh, and the write gate reads a missing verdict
	// as unknown — which allows. The freshness credential still rides the
	// forwarded query, so nothing about correctness depends on this succeeding.
	for kbID, version := range snap.expected {
		answer, err := r.versionHolders(ctx, kbID, version)
		if err != nil {
			r.loggerOrNop().Debug("route table: holders query failed; leaving the KB unnarrowed",
				zap.String("kb_id", kbID), zap.Int64("version", version), zap.Error(err))
			continue
		}
		set := make(map[int]bool, len(answer.addrs))
		for _, addr := range answer.addrs {
			if idx, ok := r.storageIndexByAddr[addr]; ok {
				set[idx] = true
			}
		}
		snap.servable[kbID] = set
		// Only a KNOWN verdict is recorded. An absent entry is what makes the
		// write gate allow, so storing a zero-valued "healthy" for an unknown
		// answer would launder missing information into a clean bill of health.
		if answer.degradationKnown {
			snap.degradation[kbID] = degradationVerdict{
				degraded:    answer.degraded,
				unavailable: answer.unavailable,
				detail:      answer.detail,
			}
		}
	}
	return snap, nil
}

// holdersAnswer is one control node's answer to the holders question: the storage
// addresses that reported holding the version, plus the leader's redundancy
// verdict for the knowledge base.
//
// Both ride one response because both come from one aggregate
// (docs/storage-degradation-signal-plan.md §4.2), and because the station needs
// them on one schedule: the write gate reads the verdict, the route table reads
// the addresses, and a single refresh keeps the two consistent with each other.
type holdersAnswer struct {
	// addrs are the storage addresses the leader named for this version.
	addrs []string

	// degraded / unavailable / detail carry the redundancy verdict: the two
	// failure tiers and the diagnosis, straight off the wire. They become a
	// degradationVerdict in the snapshot, which is what the write gate reads.
	degraded    bool
	unavailable bool
	detail      string

	// degradationKnown is false when the leader reported no verdict: an older
	// peer that predates the field, or a leader whose replica topology is not
	// wired. That is emphatically not "not degraded" — see
	// RouteTable.Degradation.
	degradationKnown bool
}

// versionHolders asks the control layer which nodes hold kbID at version, and
// returns them together with the leader's redundancy verdict for the knowledge
// base.
//
// ok=false from the leader is not an error: it means the node we reached is not
// the leader and has folded no reports, so the empty answer carries no
// information. Returning it as a failure is what makes the caller leave the KB
// unnarrowed instead of recording "no holders", which Servable would otherwise
// have to interpret.
//
// A non-leader's verdict is skipped along with its holder list, for the same
// reason it is skipped for the list: an empty aggregate has no verdict to give,
// and the caller moves on to the node that has one.
func (r *Router) versionHolders(ctx context.Context, kbID string, version int64) (holdersAnswer, error) {
	var lastErr error
	for i := range r.kbs {
		resp, err := r.kbs[i].GetDataVersionHolders(ctx, &pb.GetDataVersionHoldersRequest{
			KnowledgeBaseId: kbID,
			VersionId:       version,
		})
		if err != nil {
			lastErr = err
			continue
		}
		if !resp.GetKnown() {
			lastErr = errors.New("router: holders answer came from a non-leader")
			continue
		}
		answer := holdersAnswer{
			addrs:            make([]string, 0, len(resp.GetHolders())),
			degraded:         resp.GetDegraded(),
			unavailable:      resp.GetStorageUnavailable(),
			detail:           resp.GetDegradationDetail(),
			degradationKnown: resp.GetDegradationKnown(),
		}
		for _, h := range resp.GetHolders() {
			if h.GetAddress() != "" {
				answer.addrs = append(answer.addrs, h.GetAddress())
			}
		}
		return answer, nil
	}
	if lastErr == nil {
		lastErr = errors.New("router: no control node answered the holders query")
	}
	return holdersAnswer{}, lastErr
}

// candidates narrows a storage-layer call to the nodes that should serve it:
// the version's INDEX OWNERS that also HOLD its data.
//
// versionID does double duty, and that is not a coincidence — it is the same
// value in both roles. It is the freshness credential §9.3(1) needs (a node whose
// cursor has not reached it cannot answer without returning a stale-but-complete
// result), and it is the key the owner set is computed from.
//
// The two sets are combined by INTERSECTION, and neither alone would do:
//
//   - holders alone is what this used to be, and it is why every node converged
//     on the same working set (R3 of docs/code-review-2026-10-14.md): a query
//     landed on whichever holder the round-robin reached, so every node's LRU
//     filled with the same versions and the cluster held N copies of one working
//     set.
//   - owners alone would send reads to replicas that may not hold the data yet.
//
// An empty intersection falls back to the holders, deliberately: an owner set
// that no holder belongs to means the owner view is stale or the topology has
// just changed, and "the node that has the data" is the answer that still works.
func (r *Router) candidates(kbID string, versionID int64, n int) []int {
	if r.routes == nil {
		return allIndexes(n)
	}
	holders := r.routes.Servable(kbID, versionID, n)
	if r.ownerInactiveK <= 0 || len(r.storageAddrs) == 0 {
		return holders // sharding off: every holder qualifies
	}

	owners := r.ownersOf(kbID, versionID)
	if len(owners) == 0 {
		return holders
	}
	narrowed := make([]int, 0, len(holders))
	for _, idx := range holders {
		if idx >= 0 && idx < len(r.storageAddrs) && owners[r.storageAddrs[idx]] {
			narrowed = append(narrowed, idx)
		}
	}
	if len(narrowed) == 0 {
		// Nobody in the owner set holds the version yet. Serving from a holder is
		// still correct (the freshness credential is what guarantees the answer),
		// and refusing would turn a topology change into an outage.
		return holders
	}
	return narrowed
}

// ownerKFor decides how many replicas should hold this version's index.
//
// Two rules, in order:
//
//   - The ACTIVE version — the one the routing table says this knowledge base
//     serves — keeps the whole replica set (k = 0, "every member"). Any node may
//     be asked for it, so all of them are expected to hold its index.
//   - Everything else starts at OwnerInactiveK and climbs one rung per priority
//     tier, capped at the replica count. A version the cluster keeps querying earns
//     more owners; one it stops querying lets them go. With no half-life
//     configured the ladder is not consulted at all and k is simply the base.
func (r *Router) ownerKFor(kbID string, versionID int64) int {
	if expected, ok := r.ExpectedVersion(kbID); ok && expected == versionID {
		return 0
	}
	k := r.ownerInactiveK
	if r.priority != nil {
		k += r.priority.Tier(kbID, versionID)
		if k > len(r.storageAddrs) {
			k = len(r.storageAddrs)
		}
	}
	return k
}

// ownersOf computes the index owner set for one version, as the set of addresses
// that should hold it.
func (r *Router) ownersOf(kbID string, versionID int64) map[string]bool {
	owners := owner.Owners(kbID, versionID, r.storageAddrs, r.ownerKFor(kbID, versionID))
	if len(owners) == 0 {
		return nil
	}
	set := make(map[string]bool, len(owners))
	for _, addr := range owners {
		set[addr] = true
	}
	return set
}

// ExpectedVersion exposes the control layer's answer for kbID, so a caller can
// stamp it on the request it forwards (§9.3(2)).
func (r *Router) ExpectedVersion(kbID string) (int64, bool) {
	if r.routes == nil {
		return 0, false
	}
	return r.routes.ExpectedVersion(kbID)
}

// Close closes every node connection.
func (r *Router) Close() {
	for _, c := range r.conns {
		_ = c.Close()
	}
}

// Forward routes a unary call to the right layer: storage-resident methods
// round-robin across the storage nodes, control writes go to the leader, and
// control reads round-robin across the control nodes. fn performs the actual
// call against index idx of that layer's clients; the concrete request/response
// types stay static in the caller.
//
// kbID is the knowledge base the call concerns, or "" for calls that are not
// about one (listing, health). It is what the authorization checkpoint checks a
// grant against, and it is taken from the request rather than from the caller:
// the tenant comes from the credential lookup, the knowledge base from the
// request, and neither from an assertion the client makes about itself.
//
// Every forwarded call also carries the station's internal trust mark, so a
// storage node can tell "the station sent this" from "something reached me
// directly". The caller's credential is deliberately not among what is
// forwarded.
func Forward[T any](r *Router, ctx context.Context, fullMethod, kbID string, fn func(idx int, ctx context.Context) (T, error), opts ...ForwardOption) (T, error) {
	var zero T
	var fo forwardOpts
	for _, opt := range opts {
		opt(&fo)
	}
	// The start of this station's own share of the call, so the write path can
	// report what the station added on top of the node it forwards to. Nothing
	// else on this path can measure that: from the client's side a station hop
	// and a slow node look identical.
	stationStart := time.Now()
	if r.auth != nil {
		if _, err := r.auth.Authorize(ctx, kbID, isWriteMethod(fullMethod)); err != nil {
			return zero, err
		}
	}
	// The mark is stamped unconditionally (given a key), and that is not the same
	// decision as whether the station authenticates anyone. It says "this call
	// arrived through a station"; a node requiring it is asking for exactly that
	// and nothing more. Stamping it only when a token table is configured would
	// deadlock the cluster this feature exists to protect: nodes requiring the
	// mark would reject the station's own forwarding the moment an operator ran
	// without a token table.
	//
	// What changed with H4 (docs/code-review-2026-09-24.md): the stamp is an HMAC
	// over a timestamp under a key shared with the nodes, not the constant "1".
	// A node can now actually tell whether the station sent the call, which is
	// what the gate above always claimed.
	ctx = r.mark.Stamp(ctx)
	if isStorageMethod(fullMethod) {
		// No separate storage layer means the all-in-one shape, where the control
		// nodes are the storage nodes too — the same fallback NewRouter applies
		// when it dials them. Keeping the two consistent matters because a Router
		// built directly (the tests do) skips that constructor.
		n := len(r.storageAddrs)
		if n == 0 {
			n = len(r.controlAddrs)
		}
		// §9.3(1): the routing table narrows the candidates to the nodes whose
		// cursor has reached the version this query should be answering from. A
		// knowledge base the table has not seen yields no expected version, and
		// then every node qualifies — routing degrades to round-robin rather
		// than to refusal.
		//
		// R1 of docs/code-review-2026-10-14.md: when the caller PINNED a
		// version, that version is the credential — not the active one. Narrowing
		// a pinned read by "fresh for the active version" selects exactly the
		// nodes least likely to still hold the old one, since the whole point of
		// the active pointer is that older versions stop being the working set.
		expected, _ := r.ExpectedVersion(kbID)
		if fo.pinnedVersion != 0 {
			expected = fo.pinnedVersion
		}
		// Sampling happens HERE, on the read being served, rather than on a ticker:
		// the versions worth counting are exactly the ones somebody asks for, and a
		// table a timer walked would keep decaying entries nobody is looking at
		// (docs/index-owner-sharding-plan.md §4).
		if r.priority != nil && expected != 0 {
			r.priority.Observe(kbID, expected)
		}
		// P2C over the candidates the owner intersection produced: send the call to
		// the less busy of two randomly chosen nodes. It reorders only — a node
		// that is merely slow is still tried, just not first.
		cands := leastLoadedFirst(r.candidates(kbID, expected, n), r.storageInflight)
		return forwardRead(r, ctx, readTrace{kbID: kbID, layer: "storage", admission: time.Since(stationStart)},
			cands, r.storageBreakers, r.storageInflight, fn)
	}
	if isWriteMethod(fullMethod) {
		// §4.3(1): refuse a write this station already knows cannot reach quorum,
		// rather than paying a control-layer round trip to be told the same thing.
		if err := r.writeGate(kbID); err != nil {
			return zero, err
		}
		// admission is everything the station decided for itself before asking
		// anyone: the authorization check above plus the storage gate just now.
		// forwardWrite reports it alongside its own stages.
		return forwardWrite(r, ctx, kbID, time.Since(stationStart), fn)
	}
	return forwardRead(r, ctx, readTrace{kbID: kbID, layer: "control", admission: time.Since(stationStart)},
		leastLoadedFirst(allIndexes(len(r.controlAddrs)), r.controlInflight),
		r.controlBreakers, r.controlInflight, fn)
}

// ForwardOption tunes how one forwarded call picks its candidates.
type ForwardOption func(*forwardOpts)

type forwardOpts struct {
	// pinnedVersion is the version the CALLER named, when it named one. Zero
	// means the caller did not pin, and the routing table's expected version is
	// the credential.
	pinnedVersion int64
}

// WithPinnedVersion tells the router the caller asked for one SPECIFIC version,
// so that version — not the knowledge base's active one — is what the candidates
// are narrowed by (R1 of docs/code-review-2026-10-14.md).
//
// It is an option rather than a parameter because most forwarded calls have no
// version to pin: metadata reads and writes are about the knowledge base.
func WithPinnedVersion(versionID int64) ForwardOption {
	return func(o *forwardOpts) { o.pinnedVersion = versionID }
}

// writeGate refuses a write the routing cache already knows cannot reach quorum
// (docs/storage-degradation-signal-plan.md §4.3, first line).
//
// This is the station's half of the gate and deliberately the cheap half: it
// answers from the snapshot the background refresh already took, so the common
// case costs a map lookup and the doomed control-layer round trip never happens.
// The control layer checks again regardless, because a request can reach it
// without passing a station at all.
//
// Unknown ALWAYS allows: no verdict for the KB, an unwired table, or a KB the
// snapshot never saw. The snapshot is periodic and what it carries is soft state,
// so it can be wrong in either direction — which is why the refusal is retryable
// (both sentinels are codes.Unavailable) and why this signal never decides
// correctness, only fails earlier and with a reason the caller can read.
//
// Which sentinel it carries follows the leader's verdict: the two tiers get the
// two names, so a client sees the same error whether the station caught it or the
// control layer did (§4.1).
func (r *Router) writeGate(kbID string) error {
	if r.routes == nil || kbID == "" {
		return nil
	}
	verdict, known := r.routes.Degradation(kbID)
	if !known || !verdict.unhealthy() {
		return nil
	}
	sentinel := stratumerrors.ErrKBStorageDegraded
	if verdict.unavailable {
		sentinel = stratumerrors.ErrStorageUnavailable
	}
	return stratumerrors.ToGRPCStatus(fmt.Errorf("%w: %s", sentinel, verdict.detail))
}

// budgetSlice bounds one attempt's share of the client's remaining deadline
// (Stratum_设计文档v13.md §9.5).
//
// The client's deadline is passed through unchanged — the station adds no
// timeout of its own — but an attempt that is allowed to run until that
// deadline would leave nothing for the retries §9.3(4) promises: one slow node
// would consume the entire budget and the request would fail with candidates
// still untried. So each attempt gets a fraction of what is left, and the
// count of remaining candidates sizes that fraction.
//
// No deadline means the caller set none; the station then imposes none either,
// because inventing one would break a caller that legitimately wants to wait.
func budgetSlice(ctx context.Context, attemptsLeft int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		// Already expired: let the call fail fast rather than adding a fresh
		// timeout that would mask why.
		return context.WithCancel(ctx)
	}
	if attemptsLeft < 1 {
		attemptsLeft = 1
	}
	share := remaining / time.Duration(attemptsLeft+1)
	if share < minAttemptBudget {
		share = minAttemptBudget
	}
	if share > remaining {
		// The client's deadline is a hard bound: a floor may widen an attempt
		// inside it, never beyond it. When what is left is smaller than the
		// floor, the remaining budget IS the attempt's budget — the call will
		// end at the client's deadline, which is the caller's own decision.
		share = remaining
	}
	return context.WithTimeout(ctx, share)
}

// minAttemptBudget keeps the per-attempt share from collapsing to nothing when
// the remaining budget is small or the candidate list is long: an attempt that
// is given microseconds cannot even fail informatively.
const minAttemptBudget = 200 * time.Millisecond

// forwardWrite runs fn on the current leader. The leader is re-polled on
// every call (LeaderNow, no caching) so a stale leader can never keep
// writes pinned to a dead or demoted node. When the leader rejects the
// call (not leader, or the node went down), the call is retried on the
// re-polled leader. With no known leader it falls back to trying every
// node once. Non-retryable errors (validation, index failures, ...) are
// returned as-is.
func forwardWrite[T any](r *Router, ctx context.Context, kbID string, admission time.Duration, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T

	// Per-stage timings of one forwarded write, at debug level.
	//
	// Why it exists: the station is the first of the three processes a write
	// passes through, and its share of the cost is the only part of the path
	// that nothing behind it can measure — from the client's side, a station hop
	// and a slow node look identical. admission_us is what this station decided
	// on its own (credential check + §4.3(1)'s storage gate), leader_lookup_us is
	// finding the leader, and attempt_us is the forwarded RPC, which contains the
	// control node's whole handling (service: create version timings,
	// coordinator: write execute timings) plus the wire. kb_id + version_id join
	// this line to those; version_id is read out of the response, so it is
	// present exactly when the write got as far as allocating a version.
	//
	// attempts is why this line is not just a sum: §9.3(4) retries a retryable
	// failure on another node, so a write that took 4 s here may have been two
	// attempts, and the first one is invisible everywhere else. leader_index
	// says which node the leader poll landed on — negative means it found none
	// and fell back to trying every node once.
	start := time.Now()
	var versionID int64
	var tLeaderLookup, tAttempt time.Duration
	attempts, leaderIdx := 0, -1
	// lastErr keeps the last forwarded failure's cause.
	//
	// Why it exists: every error the loop sees is triaged by isRetryableErr, but
	// the triage's INPUT was dropped, so every retryable failure converged on one
	// flat "no leader available" — which names neither the node nor the reason.
	// That message is what a caller sees when a write fails, and it sent us
	// looking at the cluster when the leader was healthy the whole time
	// (GetClusterStatus answered has_leader=true, and leader_index was 0).
	var lastErr error
	defer func() {
		r.loggerOrNop().Debug("router: write forward timings",
			zap.String("kb_id", kbID),
			zap.Int64("version_id", versionID),
			zap.Int("attempts", attempts),
			zap.Int("leader_index", leaderIdx),
			zap.Int64("admission_us", admission.Microseconds()),
			zap.Int64("leader_lookup_us", tLeaderLookup.Microseconds()),
			zap.Int64("attempt_us", tAttempt.Microseconds()),
			zap.Int64("total_us", time.Since(start).Microseconds()),
			zap.Error(lastErr))
	}()

	// noteResponse picks up the version id when the response carries one, which
	// is what makes this line joinable to the control and storage nodes' lines.
	// Not every forwarded call has an id (and a failed one has none), so this is
	// best-effort rather than a required part of the contract.
	noteResponse := func(resp T) T {
		if v, ok := any(resp).(versionIdentified); ok {
			versionID = v.GetVersionId()
		}
		return resp
	}

	for attempt := 0; attempt <= len(r.controlAddrs); attempt++ {
		lookupStart := time.Now()
		idx, ok := r.discoverer.LeaderNow(ctx)
		tLeaderLookup += time.Since(lookupStart)
		if !ok {
			// Election in progress or discovery failed: try every node
			// once — the real leader accepts the write if reachable.
			attemptStart := time.Now()
			resp, err := tryAll(r, ctx, fn)
			tAttempt += time.Since(attemptStart)
			attempts++
			return noteResponse(resp), err
		}
		leaderIdx = idx
		if !r.allowControl(idx) {
			// The discovered leader is circuit-broken: trying it again is what
			// the breaker exists to stop. Another attempt re-discovers, which is
			// also how a write finds the new leader after a failover.
			continue
		}
		attemptCtx, cancel := budgetSlice(ctx, len(r.controlAddrs)-attempt)
		attemptStart := time.Now()
		resp, err := fn(idx, attemptCtx)
		cancel()
		tAttempt += time.Since(attemptStart)
		attempts++
		r.observeControl(idx, err)
		if err == nil {
			return noteResponse(resp), nil
		}
		lastErr = err
		if !isRetryableErr(err, true) {
			return zero, err
		}
	}
	if lastErr != nil {
		// Wrap rather than replace: the cause stays inspectable with errors.Is,
		// and the message now says what actually went wrong. "no leader
		// available" alone is wrong anyway — the leader was found and the write
		// reached it.
		return zero, noLeaderAfter(fmt.Sprintf("router: no leader available after %d attempt(s)", attempts), lastErr)
	}
	// Named, not a bare errors.New: "the cluster is electing" is transient, and without a
	// sentinel the caller sees `code = Unknown` and cannot know to retry — measured as a
	// whole T4 suite failing in setup while the election finished.
	return zero, fmt.Errorf("router: no leader available: %w", stratumerrors.ErrNoLeaderYet)
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

// versionIdentified is implemented by the responses that name the version a
// write produced. It is the station's half of the join key that ties one write's
// lines together across the three processes: kb_id + version_id appear on the
// station's line (router: write forward timings), the control node's two
// (service: create version timings, coordinator: write execute timings), the
// storage node's own staged transaction (write: stage timings) and its index
// build (index: build timings).
type versionIdentified interface {
	GetVersionId() int64
}

// allowControl / recordControl guard the breaker slices: a Router built without
// NewRouter (the tests) has none, and a nil slice must mean "no breaker", not a
// panic.
func (r *Router) allowControl(idx int) bool {
	if idx < 0 || idx >= len(r.controlBreakers) {
		return true
	}
	return r.controlBreakers[idx].allow(time.Now())
}

func (r *Router) observeControl(idx int, err error) {
	if idx < 0 || idx >= len(r.controlBreakers) {
		return
	}
	r.controlBreakers[idx].observe(err)
}

// readTrace carries what forwardRead reports about the station's own share of a
// read. Grouped rather than passed as three loose parameters so the tests that
// call forwardRead directly can pass the zero value.
type readTrace struct {
	kbID      string
	layer     string // "storage" or "control"
	admission time.Duration
}

// forwardRead runs fn across n nodes of one layer in round-robin order,
// skipping to the next node when one is unreachable. The first successful
// response wins.
//
// n is the layer's node count rather than a length read off the Router, because
// the same helper serves both layers and their client slices are indexed by
// their own node lists.
func forwardRead[T any](r *Router, ctx context.Context, trace readTrace, candidates []int, breakers []*breaker, load []*inflight, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T

	// Per-stage timings of one forwarded read, at debug level — the read-side
	// counterpart of "router: write forward timings", for the same reason: from
	// the client's side, a station hop and a slow node look identical, so only
	// the station can say what it added. admission_us is what the station decided
	// on its own before asking anyone (credential check + §4.3(1)'s storage gate);
	// attempt_us is a forwarded RPC and contains the storage node's whole handling
	// (service/query.go's "query: stage timings", index/impl.go's
	// "index: search timings") plus the wire. layer says which tier was asked, and
	// node_index which member of it answered — a read that retried elsewhere is
	// invisible everywhere but here.
	start := time.Now()
	var tAttempt time.Duration
	attempts, nodeIdx := 0, -1
	defer func() {
		r.loggerOrNop().Debug("router: read forward timings",
			zap.String("kb_id", trace.kbID),
			zap.String("layer", trace.layer),
			zap.Int("node_index", nodeIdx),
			zap.Int("attempts", attempts),
			zap.Int64("admission_us", trace.admission.Microseconds()),
			zap.Int64("attempt_us", tAttempt.Microseconds()),
			zap.Int64("total_us", time.Since(start).Microseconds()))
	}()

	if len(candidates) == 0 {
		return zero, errors.New("router: no node can serve this request")
	}
	n := len(candidates)
	rrStart := int(r.rr.Add(1)-1) % n
	now := time.Now()
	admitted := 0
	for i := 0; i < n; i++ {
		idx := candidates[(rrStart+i)%n]
		if idx < len(breakers) && !breakers[idx].allow(now) {
			continue // sick node: skip it instead of paying its timeout
		}
		admitted++
		attemptCtx, cancel := budgetSlice(ctx, n-i)
		attemptStart := time.Now()
		// P2C's other half: the counter is held for exactly as long as the call is
		// outstanding, and only for a call that was actually made. A node skipped
		// by the breaker above has nothing in flight, and counting it would make
		// the healthy nodes look busier than the sick one.
		//
		// The defer lives inside the closure so the decrement happens on every
		// exit — including a panic on the way out. Incrementing in the loop and
		// decrementing in the function's own defer is the version of this that
		// leaks a count per interval, forever.
		call := func() (T, error) {
			if idx < len(load) && load[idx] != nil {
				load[idx].n.Add(1)
				defer load[idx].n.Add(-1)
			}
			return fn(idx, attemptCtx)
		}
		resp, err := call()
		tAttempt += time.Since(attemptStart)
		cancel()
		attempts++
		nodeIdx = idx
		if idx < len(breakers) {
			breakers[idx].observe(err)
		}
		if err == nil {
			return resp, nil
		}
		// Reads keep version_pending retryable: another replica may already be
		// serving the version this one is still writing (see isRetryableErr).
		if !isRetryableErr(err, false) {
			return zero, err
		}
	}
	if admitted == 0 {
		return zero, errors.New("router: every node for this layer is circuit-broken")
	}
	return zero, errors.New("router: all nodes unavailable")
}

// tryAll runs fn against every node until one succeeds. Used as the
// fallback when no leader is known.
func tryAll[T any](r *Router, ctx context.Context, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	now := time.Now()
	admitted := 0
	// Same reason as forwardWrite's lastErr: the triage input must survive, or
	// every retryable failure flattens into one reasonless message.
	var lastErr error
	for idx := range r.controlAddrs {
		if idx < len(r.controlBreakers) && !r.controlBreakers[idx].allow(now) {
			continue
		}
		admitted++
		attemptCtx, cancel := budgetSlice(ctx, len(r.controlAddrs)-idx)
		resp, err := fn(idx, attemptCtx)
		cancel()
		r.observeControl(idx, err)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isRetryableErr(err, true) {
			return zero, err
		}
	}
	if admitted == 0 {
		return zero, errors.New("router: every control node is circuit-broken")
	}
	if lastErr != nil {
		return zero, noLeaderAfter("router: no leader available (tried every node)", lastErr)
	}
	return zero, fmt.Errorf("router: no leader available: %w", stratumerrors.ErrNoLeaderYet)
}

// noLeaderAfter is the error for "every attempt failed in a retryable way".
//
// When the last attempt's error already carries a reason (storage_unavailable, say),
// that reason IS the diagnosis and is kept as the identity. When it carries none — a
// bare Unavailable from a node that is down, or "not leader" mid-election — the
// identity is ErrNoLeaderYet, because that is what the caller has to act on: the
// cluster may be electing, ask again. The last error stays wrapped either way, so the
// message and errors.Is still reach it.
func noLeaderAfter(msg string, lastErr error) error {
	if stratumerrors.ReasonOf(lastErr) != "" {
		return fmt.Errorf("%s: %w", msg, lastErr)
	}
	return fmt.Errorf("%s: %w (last error: %w)", msg, stratumerrors.ErrNoLeaderYet, lastErr)
}

// isRetryableErr reports whether a forwarded call should retry on another node.
//
// The decision is made on the error's IDENTITY first, and only then on its
// status code. The code alone cannot answer the question: FailedPrecondition is
// shared by failures that mean opposite things — "this replica has not built the
// index / has taken it out of service, another one may be fine" versus "the
// version is being deleted, do not retry anywhere". A code-only rule therefore
// either retries the terminal ones or gives up on the recoverable ones, and a
// message-matching rule (what the "not leader" special case below does) breaks
// the moment anyone rewords an error. Named sentinels travel as a standard
// ErrorInfo detail (errors.ReasonOf), which is what makes the distinction
// available on this side of the wire at all.
//
// forWrite picks between the two paths, and it is not cosmetic: the same
// sentinel means opposite things to each.
//
//   - version_pending, to a READ, means this replica's storage-layer write for
//     the version is still in progress — another replica may already be serving
//     it, so asking someone else is worth the hop.
//   - version_pending, to a WRITE, means the version's data has not landed yet.
//     A write goes through Raft, so every replica shares one state machine:
//     another candidate refuses for exactly the same reason. Retrying buys
//     nothing and costs something — it replaces a terminal, self-describing
//     FailedPrecondition ("version 95 is PENDING") with a flat "no leader
//     available", which names neither the version nor its state and reads like
//     an infrastructure fault. That message sent us auditing a healthy cluster
//     while GetClusterStatus was answering has_leader=true.
//
// The code-based fallback covers errors that carry no reason:
//   - codes.Unavailable: node down / connection failed (leader failover or a
//     stopped container) — worth another candidate.
//   - codes.Internal + "not leader": a forwarded kvraft error, which has no
//     named sentinel of its own yet.
//
// Everything else (validation errors, terminal version states, ...) is terminal.
func isRetryableErr(err error, forWrite bool) bool {
	if reason := stratumerrors.ReasonOf(err); reason != "" {
		if forWrite && writeTerminalReasons[reason] {
			return false
		}
		return retryableReasons[reason]
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() == codes.Unavailable {
		return true
	}
	return st.Code() == codes.Internal && strings.Contains(st.Message(), "not leader")
}

// writeTerminalReasons names the sentinels that are worth retrying to a reader
// but terminal to a writer.
//
// Expressed as an exception list rather than a second table so the two paths
// cannot drift: a new retryable reason is retryable on both unless it is named
// here, which is the safe direction — the failure mode of an unlisted exception
// is the extra hop this file's forWrite exists to avoid, not a lost write.
var writeTerminalReasons = map[string]bool{
	// Raft-replicated state, so no other candidate can disagree about whether
	// the version's data has landed.
	"version_pending": true,
}

// retryableReasons names the sentinels that mean "another candidate may
// succeed". Keyed by the WIRE name (part of the node-to-node protocol), so a
// reworded message cannot change the answer, and a sentinel that is not listed
// here — e.g. version_deleting or knowledge_base_deleted, which share
// FailedPrecondition but are terminal — is never retried by accident.
var retryableReasons = map[string]bool{
	// This replica has not built the version's index (yet); another one may have.
	"index_not_ready": true,
	// This replica IS building the version's index, and the caller waited out
	// load_wait_timeout_ms for it. The same family as index_not_ready — local and
	// transient, and another candidate may already be serving the version — with
	// an even stronger case: a build that has been running longer than the wait
	// is positive evidence that this replica is busy. Leaving it out made ONE busy
	// candidate a terminal error for the caller (docs/stress-test-report.md §E.4).
	"index_load_timeout": true,
	// This replica took the version's index out of service on purpose, for the
	// §8.6(d) rolling cleanup; another one is serving it.
	"index_maintenance": true,
	// This replica's storage-layer write for the version is still in progress.
	// The work is running (or about to be scheduled), so another replica may
	// already be serving the version — and the query path now asks for the build
	// instead of only refusing, which is what makes the retry worth making.
	"version_pending": true,
	// No control node would take the write because the cluster has no leader yet — an
	// election in flight. Retrying is the whole answer.
	"no_leader_yet": true,
}
