package router

import (
	"context"
	"errors"
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

	// Auth is the station's authentication checkpoint (§9.3(5)). nil means
	// "deployment controls access by isolation" — the pre-§9 arrangement —
	// which is why it is optional rather than required: a cluster that has not
	// adopted a station keeps working, and one that has gets the checkpoint for
	// free on every forwarded call.
	Auth *Authenticator

	// RouteRefreshInterval is how often the routing cache re-observes the
	// storage layer's cursors and the control layer's expected versions
	// (§9.3(1)). Zero means the default (5s). Negative disables the table,
	// which makes the station route every query to every storage node — the
	// pre-§9 behaviour.
	RouteRefreshInterval time.Duration
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

	auth *Authenticator

	// syncs are the storage layer's data-plane clients: the station asks them
	// for cursors to build the routing table (§9.3(1)), using the §7.6 query
	// rather than a new interface.
	syncs []pb.DataSyncServiceClient

	// routes is the station's local cache of "which node can serve which KB at
	// which version". nil when disabled.
	routes *RouteTable
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
	for i := 0; i < len(cfg.Addrs); i++ {
		r.kbs = append(r.kbs, pb.NewKnowledgeBaseServiceClient(r.conns[i]))
	}
	for i := len(cfg.Addrs); i < len(r.conns); i++ {
		r.querys = append(r.querys, pb.NewQueryServiceClient(r.conns[i]))
		r.admins = append(r.admins, pb.NewAdminServiceClient(r.conns[i]))
		r.syncs = append(r.syncs, pb.NewDataSyncServiceClient(r.conns[i]))
	}
	// Leader discovery goes through the storage layer's AdminService: a control
	// node builds no AdminService (it has no stores to report on), while a
	// storage node answers GetClusterStatus by asking the control cluster
	// through its remote metadata channel — one extra hop, and the only place
	// the answer exists.
	admins := make([]statusClient, len(r.admins))
	for i, a := range r.admins {
		admins[i] = a
	}
	r.discoverer = NewLeaderDiscoverer(admins)

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
	ctx = authmeta.WithVerifiedMark(ctx)

	snap := routeSnapshot{servable: map[string]map[int]bool{}, expected: map[string]int64{}}

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
		snap.expected[kb.GetKnowledgeBaseId()] = kb.GetActiveVersionId()
	}

	// Per knowledge base, ask which nodes reported holding its expected version.
	//
	// A KB whose query fails, or whose answer comes from a non-leader, is simply
	// left out: Servable then does not narrow for it, which is the same answer it
	// gives before the first refresh. The freshness credential still rides the
	// forwarded query, so nothing about correctness depends on this succeeding.
	for kbID, version := range snap.expected {
		holders, err := r.versionHolders(ctx, kbID, version)
		if err != nil {
			r.logger.Debug("route table: holders query failed; leaving the KB unnarrowed",
				zap.String("kb_id", kbID), zap.Int64("version", version), zap.Error(err))
			continue
		}
		set := make(map[int]bool, len(holders))
		for _, addr := range holders {
			if idx, ok := r.storageIndexByAddr[addr]; ok {
				set[idx] = true
			}
		}
		snap.servable[kbID] = set
	}
	return snap, nil
}

// versionHolders asks the control layer which nodes hold kbID at version, and
// returns their addresses.
//
// ok=false from the leader is not an error: it means the node we reached is not
// the leader and has folded no reports, so the empty answer carries no
// information. Returning it as a failure is what makes the caller leave the KB
// unnarrowed instead of recording "no holders", which Servable would otherwise
// have to interpret.
func (r *Router) versionHolders(ctx context.Context, kbID string, version int64) ([]string, error) {
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
		addrs := make([]string, 0, len(resp.GetHolders()))
		for _, h := range resp.GetHolders() {
			if h.GetAddress() != "" {
				addrs = append(addrs, h.GetAddress())
			}
		}
		return addrs, nil
	}
	if lastErr == nil {
		lastErr = errors.New("router: no control node answered the holders query")
	}
	return nil, lastErr
}

// candidates narrows a storage-layer call to the nodes the routing table says
// can serve it (§9.3(1)).
//
// expectedVersion is the freshness credential the caller will attach: a node
// whose cursor has not reached it cannot answer without returning a stale-but-
// complete result, which is the failure §9.1 risk 1 describes. When there is no
// credential, every node qualifies and this is the plain round-robin it used to
// be.
func (r *Router) candidates(kbID string, expectedVersion int64, n int) []int {
	if r.routes == nil {
		return allIndexes(n)
	}
	return r.routes.Servable(kbID, expectedVersion, n)
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
func Forward[T any](r *Router, ctx context.Context, fullMethod, kbID string, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	if r.auth != nil {
		if _, err := r.auth.Authorize(ctx, kbID, isWriteMethod(fullMethod)); err != nil {
			return zero, err
		}
	}
	// The mark is stamped unconditionally, and that is not the same decision as
	// whether the station authenticates anyone. It says "this call arrived
	// through a station"; a node requiring it is asking for exactly that and
	// nothing more. Stamping it only when a token table is configured would
	// deadlock the cluster this feature exists to protect: nodes requiring the
	// mark would reject the station's own forwarding the moment an operator ran
	// without a token table.
	ctx = authmeta.WithVerifiedMark(ctx)
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
		expected, _ := r.ExpectedVersion(kbID)
		return forwardRead(r, ctx, r.candidates(kbID, expected, n), r.storageBreakers, fn)
	}
	if isWriteMethod(fullMethod) {
		return forwardWrite(r, ctx, fn)
	}
	return forwardRead(r, ctx, allIndexes(len(r.controlAddrs)), r.controlBreakers, fn)
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
func forwardWrite[T any](r *Router, ctx context.Context, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt <= len(r.controlAddrs); attempt++ {
		idx, ok := r.discoverer.LeaderNow(ctx)
		if !ok {
			// Election in progress or discovery failed: try every node
			// once — the real leader accepts the write if reachable.
			return tryAll(r, ctx, fn)
		}
		if !r.allowControl(idx) {
			// The discovered leader is circuit-broken: trying it again is what
			// the breaker exists to stop. Another attempt re-discovers, which is
			// also how a write finds the new leader after a failover.
			continue
		}
		attemptCtx, cancel := budgetSlice(ctx, len(r.controlAddrs)-attempt)
		resp, err := fn(idx, attemptCtx)
		cancel()
		r.observeControl(idx, err)
		if err == nil {
			return resp, nil
		}
		if !isRetryableErr(err) {
			return zero, err
		}
	}
	return zero, errors.New("router: no leader available")
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

// forwardRead runs fn across n nodes of one layer in round-robin order,
// skipping to the next node when one is unreachable. The first successful
// response wins.
//
// n is the layer's node count rather than a length read off the Router, because
// the same helper serves both layers and their client slices are indexed by
// their own node lists.
func forwardRead[T any](r *Router, ctx context.Context, candidates []int, breakers []*breaker, fn func(idx int, ctx context.Context) (T, error)) (T, error) {
	var zero T
	if len(candidates) == 0 {
		return zero, errors.New("router: no node can serve this request")
	}
	n := len(candidates)
	start := int(r.rr.Add(1)-1) % n
	now := time.Now()
	admitted := 0
	for i := 0; i < n; i++ {
		idx := candidates[(start+i)%n]
		if idx < len(breakers) && !breakers[idx].allow(now) {
			continue // sick node: skip it instead of paying its timeout
		}
		admitted++
		attemptCtx, cancel := budgetSlice(ctx, n-i)
		resp, err := fn(idx, attemptCtx)
		cancel()
		if idx < len(breakers) {
			breakers[idx].observe(err)
		}
		if err == nil {
			return resp, nil
		}
		if !isRetryableErr(err) {
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
		if !isRetryableErr(err) {
			return zero, err
		}
	}
	if admitted == 0 {
		return zero, errors.New("router: every control node is circuit-broken")
	}
	return zero, errors.New("router: no leader available")
}

// isRetryableErr reports whether a forwarded call should retry on another
// node:
//   - kvraft.ErrNotLeader (codes.Internal + "not leader"): this node is
//     not the Raft leader, the router must re-discover and retry.
//   - codes.Unavailable: node down / connection failed (leader failover
//     or a stopped container).
//
// Everything else (validation errors, index failures, ...) is terminal.
func isRetryableErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() == codes.Unavailable {
		return true
	}
	return st.Code() == codes.Internal && strings.Contains(st.Message(), "not leader")
}
