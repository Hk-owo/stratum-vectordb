package router

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/metadata"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

func notLeaderErr() error {
	return status.Error(codes.Internal, "kvraft: not leader")
}

func unavailableErr() error {
	return status.Error(codes.Unavailable, "connection unavailable")
}

// fakeResolver is a scriptable leaderResolver: each LeaderNow call pops
// the next index from order (the last one repeats).
type fakeResolver struct {
	mu       sync.Mutex
	order    []int
	ok       bool
	nowCalls int
}

// LeaderNow pops the next scripted leader index. nowCalls counts
// invocations so tests can assert that writes never reuse a cached
// leader.
func (f *fakeResolver) LeaderNow(context.Context) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nowCalls++
	if len(f.order) == 0 {
		return 0, f.ok
	}
	idx := f.order[0]
	if len(f.order) > 1 {
		f.order = f.order[1:]
	}
	return idx, f.ok
}

func TestForwardWrite_LeaderSuccess(t *testing.T) {
	r := &Router{
		controlAddrs: []string{"a", "b", "c"},
		discoverer:   &fakeResolver{order: []int{1}, ok: true},
	}
	got, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		if idx != 1 {
			t.Errorf("fn called with idx %d, want 1 (leader)", idx)
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
}

// TestForwardWrite_AlwaysRediscover pins the fix for stale-leader writes:
// every write must re-poll the cluster (LeaderNow) instead of serving the
// TTL cache, so a leader change can never leave writes pinned to an old
// leader for up to a second.
func TestForwardWrite_AlwaysRediscover(t *testing.T) {
	fr := &fakeResolver{order: []int{0, 1}, ok: true}
	r := &Router{controlAddrs: []string{"a", "b", "c"}, discoverer: fr}

	// Two back-to-back writes. With TTL caching the second would hit the
	// cached leader without re-polling; with LeaderNow it must re-poll.
	for i := 0; i < 2; i++ {
		if _, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
			return "ok", nil
		}); err != nil {
			t.Fatalf("write %d: forwardWrite: %v", i, err)
		}
	}
	if fr.nowCalls != 2 {
		t.Errorf("LeaderNow calls = %d, want 2 (every write re-discovers)", fr.nowCalls)
	}
}

func TestForwardWrite_NotLeaderRediscover(t *testing.T) {
	fr := &fakeResolver{order: []int{1, 2}, ok: true}
	r := &Router{controlAddrs: []string{"a", "b", "c"}, discoverer: fr}

	got, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		if idx == 1 {
			return "", notLeaderErr() // stale leader: redirect
		}
		if idx == 2 {
			return "ok", nil
		}
		t.Errorf("fn called with unexpected idx %d", idx)
		return "", errors.New("unexpected")
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
	if fr.nowCalls != 2 {
		t.Errorf("LeaderNow calls = %d, want 2 (retry re-discovers)", fr.nowCalls)
	}
}

func TestForwardWrite_UnavailableRediscover(t *testing.T) {
	fr := &fakeResolver{order: []int{2, 1}, ok: true}
	r := &Router{controlAddrs: []string{"a", "b", "c"}, discoverer: fr}

	got, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		if idx == 2 {
			return "", unavailableErr() // stale leader went down
		}
		if idx == 1 {
			return "ok", nil // re-discovered leader accepts
		}
		t.Errorf("fn called with unexpected idx %d", idx)
		return "", errors.New("unexpected")
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
	if fr.nowCalls != 2 {
		t.Errorf("LeaderNow calls = %d, want 2 (retry re-discovers)", fr.nowCalls)
	}
}

func TestForwardWrite_NoLeaderTryAll(t *testing.T) {
	r := &Router{
		controlAddrs: []string{"a", "b", "c"},
		discoverer:   &fakeResolver{ok: false}, // no leader known
	}
	got, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		if idx == 1 {
			return "ok", nil // node 1 happens to accept the write
		}
		return "", notLeaderErr()
	})
	if err != nil {
		t.Fatalf("forwardWrite: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
}

func TestForwardWrite_NoLeaderAllFail(t *testing.T) {
	r := &Router{
		controlAddrs: []string{"a", "b", "c"},
		discoverer:   &fakeResolver{ok: false},
	}
	_, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		return "", notLeaderErr()
	})
	if err == nil || err.Error() != "router: no leader available" {
		t.Errorf("err = %v, want 'router: no leader available'", err)
	}
}

func TestForwardWrite_NonRetryable(t *testing.T) {
	fr := &fakeResolver{order: []int{1}, ok: true}
	r := &Router{controlAddrs: []string{"a", "b", "c"}, discoverer: fr}

	want := status.Error(codes.InvalidArgument, "bad request")
	_, err := forwardWrite(r, context.Background(), "", 0, func(idx int, ctx context.Context) (string, error) {
		return "", want
	})
	if err != want {
		t.Errorf("err = %v, want the original non-retryable error", err)
	}
	if fr.nowCalls != 1 {
		t.Errorf("LeaderNow calls = %d, want 1 (no retry)", fr.nowCalls)
	}
}

func TestForwardRead_RoundRobin(t *testing.T) {
	r := &Router{controlAddrs: []string{"a", "b", "c"}}
	var hits [3]int
	for i := 0; i < 6; i++ {
		_, err := forwardRead(r, context.Background(), allIndexes(3), nil, func(idx int, ctx context.Context) (string, error) {
			hits[idx]++
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("forwardRead: %v", err)
		}
	}
	if hits != [3]int{2, 2, 2} {
		t.Errorf("hits = %v, want even distribution [2 2 2]", hits)
	}
}

func TestForwardRead_Failover(t *testing.T) {
	r := &Router{controlAddrs: []string{"a", "b", "c"}}
	got, err := forwardRead(r, context.Background(), allIndexes(3), nil, func(idx int, ctx context.Context) (string, error) {
		if idx == 0 {
			return "", unavailableErr()
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("forwardRead: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q, want ok", got)
	}
}

func TestForwardRead_AllDown(t *testing.T) {
	r := &Router{controlAddrs: []string{"a", "b", "c"}}
	_, err := forwardRead(r, context.Background(), allIndexes(3), nil, func(idx int, ctx context.Context) (string, error) {
		return "", unavailableErr()
	})
	if err == nil || err.Error() != "router: all nodes unavailable" {
		t.Errorf("err = %v, want 'router: all nodes unavailable'", err)
	}
}

func TestForwardRead_NonRetryable(t *testing.T) {
	r := &Router{controlAddrs: []string{"a", "b", "c"}}
	want := status.Error(codes.NotFound, "not found")
	_, err := forwardRead(r, context.Background(), allIndexes(3), nil, func(idx int, ctx context.Context) (string, error) {
		return "", want
	})
	if err != want {
		t.Errorf("err = %v, want the original non-retryable error", err)
	}
}

func TestForward_SwitchesByMethod(t *testing.T) {
	// Write methods route via the resolver; read methods round-robin.
	fr := &fakeResolver{order: []int{2}, ok: true}
	r := &Router{controlAddrs: []string{"a", "b", "c"}, discoverer: fr}
	ctx := context.Background()

	writeIdx := -1
	if _, err := Forward(r, ctx, pb.KnowledgeBaseService_RollbackVersion_FullMethodName, "", func(idx int, ctx context.Context) (string, error) {
		writeIdx = idx
		return "ok", nil
	}); err != nil {
		t.Fatalf("Forward(write): %v", err)
	}
	if writeIdx != 2 {
		t.Errorf("write routed to idx %d, want 2 (leader)", writeIdx)
	}

	readIdx := -1
	if _, err := Forward(r, ctx, pb.QueryService_Query_FullMethodName, "", func(idx int, ctx context.Context) (string, error) {
		readIdx = idx
		return "ok", nil
	}); err != nil {
		t.Fatalf("Forward(read): %v", err)
	}
	if readIdx != 0 {
		t.Errorf("read routed to idx %d, want 0 (round-robin start)", readIdx)
	}

	// CreateVersion is leader-bound again (§7.13.2): the control layer picks the
	// coordinator from the KB's replica topology, so the entry is redirected to
	// the leader rather than run on whichever node accepted it.
	coordinatorIdx := -1
	if _, err := Forward(r, ctx, pb.KnowledgeBaseService_CreateVersion_FullMethodName, "", func(idx int, ctx context.Context) (string, error) {
		coordinatorIdx = idx
		return "ok", nil
	}); err != nil {
		t.Fatalf("Forward(create-version): %v", err)
	}
	if coordinatorIdx != 2 {
		t.Errorf("CreateVersion routed to idx %d, want 2 (the leader picks the coordinator)", coordinatorIdx)
	}
}

// TestForward_RoutesMethodsToTheLayerThatCanServeThem pins the split.
//
// Getting this wrong is not a performance detail: routing a query to a control
// node answers "unknown service stratum.QueryService", and the retry rule
// treats Unimplemented as terminal, so the query fails outright. That is what
// the router did before it knew there were two layers.
func TestForward_RoutesMethodsToTheLayerThatCanServeThem(t *testing.T) {
	r := &Router{
		controlAddrs: []string{"c1", "c2"},
		storageAddrs: []string{"s1", "s2", "s3"},
	}
	ctx := context.Background()
	probe := func(fullMethod string) int {
		t.Helper()
		seen := -1
		if _, err := Forward(r, ctx, fullMethod, "", func(idx int, _ context.Context) (int, error) {
			seen = idx
			return idx, nil
		}); err != nil {
			t.Fatalf("Forward(%s): %v", fullMethod, err)
		}
		return seen
	}

	// Storage-resident: their constructors need the local stores, so only a
	// node holding data can answer.
	for _, m := range []string{
		"/stratum.QueryService/Query",
		"/stratum.AdminService/GetSystemStatus",
		"/stratum.AdminService/RebuildIndex",
	} {
		if idx := probe(m); idx < 0 || idx >= len(r.storageAddrs) {
			t.Errorf("%s routed to index %d, outside the storage layer (%d nodes)",
				m, idx, len(r.storageAddrs))
		}
	}

	// Control-side metadata: present on every control node, leader or not.
	for _, m := range []string{
		"/stratum.KnowledgeBaseService/ListVersions",
		"/stratum.KnowledgeBaseService/GetKnowledgeBase",
	} {
		if idx := probe(m); idx < 0 || idx >= len(r.controlAddrs) {
			t.Errorf("%s routed to index %d, outside the control layer (%d nodes)",
				m, idx, len(r.controlAddrs))
		}
	}
}

// TestNewRouter_StorageDefaultsToControl keeps the all-in-one topology working:
// naming only one address list means every node serves both layers, which is the
// behaviour the router had before the split existed.
func TestNewRouter_StorageDefaultsToControl(t *testing.T) {
	cfg := Config{Addrs: []string{"127.0.0.1:1", "127.0.0.1:2"}}
	r, err := NewRouter(cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer r.Close()

	if len(r.storageAddrs) != len(cfg.Addrs) {
		t.Errorf("storage layer = %v, want the control nodes %v", r.storageAddrs, cfg.Addrs)
	}
	if len(r.querys) != len(cfg.Addrs) || len(r.kbs) != len(cfg.Addrs) {
		t.Errorf("clients = %d query / %d kb, want %d each",
			len(r.querys), len(r.kbs), len(cfg.Addrs))
	}
}

// TestBudgetSlice_GivesEachAttemptAPortionOfTheRemainingBudget pins §9.5: an
// attempt may not spend the whole remaining deadline, or a single slow node
// would leave nothing for the retries the station promises.
func TestBudgetSlice_GivesEachAttemptAPortionOfTheRemainingBudget(t *testing.T) {
	const total = 4 * time.Second
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(total))
	defer cancel()

	attemptCtx, cancelAttempt := budgetSlice(ctx, 3)
	defer cancelAttempt()

	d, ok := attemptCtx.Deadline()
	if !ok {
		t.Fatal("expected the attempt to carry a deadline")
	}
	got := time.Until(d)
	// 3 candidates left => remaining/4, so roughly a quarter of 4s.
	if got > 1500*time.Millisecond || got < 500*time.Millisecond {
		t.Errorf("attempt budget = %v, want roughly a quarter of the remaining %v", got, total)
	}
}

// TestBudgetSlice_WithoutAClientDeadlineAddsNone keeps the station from
// inventing a bound: a caller that set no deadline wants to wait.
func TestBudgetSlice_WithoutAClientDeadlineAddsNone(t *testing.T) {
	ctx, cancelAttempt := budgetSlice(context.Background(), 3)
	defer cancelAttempt()

	if _, ok := ctx.Deadline(); ok {
		t.Error("the station must not add a deadline the caller did not set")
	}
}

// TestBudgetSlice_NeverExceedsTheClientDeadline pins the floor's limit: the
// minimum attempt budget widens an attempt WITHIN the client's deadline, never
// past it. With less than the floor remaining, what is left is the budget —
// ending at the client's own deadline is the caller's decision, not the
// station's to override.
func TestBudgetSlice_NeverExceedsTheClientDeadline(t *testing.T) {
	const remaining = 10 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(remaining))
	defer cancel()

	attemptCtx, cancelAttempt := budgetSlice(ctx, 50)
	defer cancelAttempt()

	d, ok := attemptCtx.Deadline()
	if !ok {
		t.Fatal("expected the attempt to carry a deadline")
	}
	clientDeadline, _ := ctx.Deadline()
	if d.After(clientDeadline) {
		t.Errorf("attempt deadline %v is after the client's %v", d, clientDeadline)
	}
}

// TestBudgetSlice_PreservesAnExpiredDeadline is the honest failure: an already
// expired request must fail on its own deadline, not on a fresh timeout that
// hides why.
func TestBudgetSlice_PreservesAnExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	attemptCtx, cancelAttempt := budgetSlice(ctx, 3)
	defer cancelAttempt()

	if attemptCtx.Err() == nil {
		t.Error("an attempt on an expired context must already be done")
	}
}

// --- §9.3(5) authentication -------------------------------------------------

// testTenantTable is a stand-in for the deployment's token table: the station
// looks credentials up, it does not parse them.
func testTenantTable() func(string) (Principal, bool) {
	return func(token string) (Principal, bool) {
		switch token {
		case "key-alice":
			return Principal{TenantID: "alice", Grants: map[string]Grant{
				"kb-1": {Read: true, Write: true},
			}}, true
		case "key-bob":
			return Principal{TenantID: "bob", Grants: map[string]Grant{
				"kb-1": {Read: true},
			}}, true
		}
		return Principal{}, false
	}
}

func ctxWithToken(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(CredentialMetadataKey, "Bearer "+token))
}

// TestAuthorize_TenantComesFromTheCredentialNotTheRequest pins the anti-pattern
// the design calls out: a client that could name its own tenant would make the
// credential check decorative.
func TestAuthorize_TenantComesFromTheCredentialNotTheRequest(t *testing.T) {
	a := NewAuthenticator(testTenantTable())

	p, err := a.Authorize(ctxWithToken("key-alice"), "kb-1", false)
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if p.TenantID != "alice" {
		t.Errorf("tenant = %q, want the one the credential resolves to (alice)", p.TenantID)
	}
}

// TestAuthorize_GrantIsPerKnowledgeBaseAndPerVerb keeps the granularity where
// the design put it: KB level, read/write.
func TestAuthorize_GrantIsPerKnowledgeBaseAndPerVerb(t *testing.T) {
	a := NewAuthenticator(testTenantTable())

	// bob may read kb-1 but not write it...
	if _, err := a.Authorize(ctxWithToken("key-bob"), "kb-1", true); status.Code(err) != codes.PermissionDenied {
		t.Errorf("bob writing kb-1: err = %v, want PermissionDenied", err)
	}
	// ...and has no access at all to another KB.
	if _, err := a.Authorize(ctxWithToken("key-bob"), "kb-2", false); status.Code(err) != codes.PermissionDenied {
		t.Errorf("bob reading kb-2: err = %v, want PermissionDenied", err)
	}
	// alice may do both.
	if _, err := a.Authorize(ctxWithToken("key-alice"), "kb-1", true); err != nil {
		t.Errorf("alice writing kb-1: %v", err)
	}
}

// TestAuthorize_MissingOrUnknownCredentialIsUnauthenticated distinguishes "who
// are you" from "you may not": the two lead callers to different actions.
func TestAuthorize_MissingOrUnknownCredentialIsUnauthenticated(t *testing.T) {
	a := NewAuthenticator(testTenantTable())

	if _, err := a.Authorize(context.Background(), "kb-1", false); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no credential: err = %v, want Unauthenticated", err)
	}
	if _, err := a.Authorize(ctxWithToken("key-nobody"), "kb-1", false); status.Code(err) != codes.Unauthenticated {
		t.Errorf("unknown credential: err = %v, want Unauthenticated", err)
	}
}

// TestForward_RefusesAnUnauthorizedCallBeforeReachingANode keeps the checkpoint
// where it belongs: nothing is forwarded, so no node sees an unauthorized
// request at all.
func TestForward_RefusesAnUnauthorizedCallBeforeReachingANode(t *testing.T) {
	r := &Router{
		storageAddrs: []string{"s1"},
		auth:         NewAuthenticator(testTenantTable()),
	}

	called := false
	_, err := Forward(r, ctxWithToken("key-bob"), "/stratum.QueryService/Query", "kb-2",
		func(idx int, _ context.Context) (int, error) { called = true; return idx, nil })

	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("err = %v, want PermissionDenied", err)
	}
	if called {
		t.Error("an unauthorized call must not reach a node")
	}
}

// TestForward_StampsTheInternalMarkWithoutTheCredential pins §9.3(5)\u2019s split:
// the node learns "the station vouched for this", never who the caller is.
func TestForward_StampsTheInternalMarkWithoutTheCredential(t *testing.T) {
	r := &Router{
		storageAddrs: []string{"s1"},
		auth:         NewAuthenticator(testTenantTable()),
	}

	var sawMark, sawCredential bool
	if _, err := Forward(r, ctxWithToken("key-alice"), "/stratum.QueryService/Query", "kb-1",
		func(_ int, ctx context.Context) (int, error) {
			md, _ := metadata.FromOutgoingContext(ctx)
			sawMark = len(md.Get(VerifiedMetadataKey)) > 0
			sawCredential = len(md.Get(CredentialMetadataKey)) > 0
			return 0, nil
		}); err != nil {
		t.Fatalf("Forward: %v", err)
	}

	if !sawMark {
		t.Error("the forwarded call must carry the station's trust mark")
	}
	if sawCredential {
		t.Error("the caller's credential must not be forwarded downstream")
	}
}
