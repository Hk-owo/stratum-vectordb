package router

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/owner"
)

// shardedRouter builds a Router over three storage nodes whose knowledge base serves
// version 5.
func shardedRouter(inactiveK int) *Router {
	addrs := []string{"node-1:7000", "node-2:7000", "node-3:7000"}
	rt := &RouteTable{
		ready:    true,
		snapshot: routeSnapshot{expected: map[string]int64{"kb-1": 5}},
	}
	return &Router{
		controlAddrs:    append([]string(nil), addrs...),
		storageAddrs:    append([]string(nil), addrs...),
		routes:          rt,
		lag:             newLagRecords(time.Minute),
		ownerInactiveK:  inactiveK,
		storageBreakers: []*breaker{newBreaker(defaultBreakerConfig), newBreaker(defaultBreakerConfig), newBreaker(defaultBreakerConfig)},
	}
}

func sortedInts(in []int) []int {
	out := append([]int(nil), in...)
	sort.Ints(out)
	return out
}

// ownerIndexes computes the k=2 owners of v7 the way the router does.
func ownerIndexes(r *Router, versionID int64, k int) (owners []int, other int) {
	inSet := map[string]bool{}
	for _, addr := range owner.Owners("kb-1", versionID, r.storageAddrs, k) {
		inSet[addr] = true
	}
	for i, addr := range r.storageAddrs {
		if inSet[addr] {
			owners = append(owners, i)
		} else {
			other = i
		}
	}
	return owners, other
}

// TestRouteTiers_OwnersFirstThenEveryoneElse: a read goes to the version's index owners,
// which is what keeps every node's LRU from holding the same working set. The other
// replicas stay reachable behind them, because an owner can be behind while they hold
// the data.
func TestRouteTiers_OwnersFirstThenEveryoneElse(t *testing.T) {
	r := shardedRouter(2)
	owners, other := ownerIndexes(r, 7, 2)

	tiers := r.routeTiers("kb-1", 7, 3, time.Now())

	if len(tiers) != 2 {
		t.Fatalf("tiers = %v, want owners then the rest", tiers)
	}
	if got := sortedInts(tiers[0]); !reflect.DeepEqual(got, owners) {
		t.Errorf("first tier = %v, want the k=2 owners %v", got, owners)
	}
	if !reflect.DeepEqual(tiers[1], []int{other}) {
		t.Errorf("second tier = %v, want the non-owner [%d]", tiers[1], other)
	}
}

// TestRouteTiers_ActiveVersionAsksEveryReplica: any replica may be asked for the version
// the knowledge base serves, so its owner set is everyone and there is no fallback tier.
func TestRouteTiers_ActiveVersionAsksEveryReplica(t *testing.T) {
	r := shardedRouter(2)

	tiers := r.routeTiers("kb-1", 5, 3, time.Now())

	if len(tiers) != 1 || !reflect.DeepEqual(sortedInts(tiers[0]), []int{0, 1, 2}) {
		t.Fatalf("tiers for the ACTIVE version = %v, want one tier of every replica", tiers)
	}
}

// TestRouteTiers_ShardingOffIsOneTier: a deployment that has not opted in to sharding
// sees plain round-robin over every node.
func TestRouteTiers_ShardingOffIsOneTier(t *testing.T) {
	r := shardedRouter(0)

	tiers := r.routeTiers("kb-1", 7, 3, time.Now())

	if len(tiers) != 1 || !reflect.DeepEqual(sortedInts(tiers[0]), []int{0, 1, 2}) {
		t.Fatalf("tiers with sharding off = %v, want one tier of every node", tiers)
	}
}

// TestRouteTiers_NoRouteTableIsOneTier: a station with routing disabled keeps round-robin.
func TestRouteTiers_NoRouteTableIsOneTier(t *testing.T) {
	r := shardedRouter(2)
	r.routes = nil

	tiers := r.routeTiers("kb-1", 7, 3, time.Now())

	if len(tiers) != 1 || len(tiers[0]) != 3 {
		t.Fatalf("tiers with no route table = %v, want one tier of every node", tiers)
	}
}

// TestOwnersOf_ActiveVersionIsNotSharded pins the k choice itself: the active version
// gets k = everyone, everything else gets OwnerInactiveK.
func TestOwnersOf_ActiveVersionIsNotSharded(t *testing.T) {
	r := shardedRouter(2)

	if got := len(r.ownersOf("kb-1", 5)); got != 3 {
		t.Errorf("owners of the ACTIVE version = %d, want 3 (every replica)", got)
	}
	if got := len(r.ownersOf("kb-1", 7)); got != 2 {
		t.Errorf("owners of a non-active version = %d, want 2 (OwnerInactiveK)", got)
	}
}

// TestRouteTiers_BehindNodeGoesLast: a recent refusal moves a node behind everyone else,
// but only for versions above its recorded cursor, and it is never ruled out: the record
// is a hint, and the node may have caught up since.
func TestRouteTiers_BehindNodeGoesLast(t *testing.T) {
	r := shardedRouter(0)
	now := time.Now()
	r.lag.note("kb-1", 1, 6, now)

	tiers := r.routeTiers("kb-1", 7, 3, now)
	if len(tiers) != 2 || !reflect.DeepEqual(tiers[1], []int{1}) {
		t.Fatalf("tiers = %v, want node 1 in a last tier of its own", tiers)
	}

	// Version 6 is within what the node holds, so the record says nothing about it.
	if tiers := r.routeTiers("kb-1", 6, 3, now); len(tiers) != 1 || len(tiers[0]) != 3 {
		t.Errorf("tiers for a version the node holds = %v, want every node in one tier", tiers)
	}
	// Another knowledge base is not affected.
	if tiers := r.routeTiers("kb-2", 7, 3, now); len(tiers) != 1 || len(tiers[0]) != 3 {
		t.Errorf("tiers for another knowledge base = %v, want every node in one tier", tiers)
	}
}

// TestRouteTiers_ExpiredRecordRestoresTheRoute: after the record expires the node is
// back on its normal route, which is how a node that caught up returns to service.
func TestRouteTiers_ExpiredRecordRestoresTheRoute(t *testing.T) {
	r := shardedRouter(0)
	now := time.Now()
	r.lag.note("kb-1", 1, 6, now)

	tiers := r.routeTiers("kb-1", 7, 3, now.Add(2*time.Minute))
	if len(tiers) != 1 || len(tiers[0]) != 3 {
		t.Fatalf("tiers after the record expired = %v, want every node in one tier", tiers)
	}
}

// TestRouteTiers_EveryNodeBehindStillAsksThem: when every node has a record, the query
// is still sent. A hint must never turn into a refusal.
func TestRouteTiers_EveryNodeBehindStillAsksThem(t *testing.T) {
	r := shardedRouter(0)
	now := time.Now()
	for idx := 0; idx < 3; idx++ {
		r.lag.note("kb-1", idx, 1, now)
	}

	tiers := r.routeTiers("kb-1", 7, 3, now)
	if len(tiers) != 1 || len(tiers[0]) != 3 {
		t.Fatalf("tiers with every node behind = %v, want every node still asked", tiers)
	}
}

// refusal is what a replica behind the required version sends over the wire.
func refusal(have, want int64) error {
	return stratumerrors.ToGRPCStatus(&stratumerrors.ReplicaBehindError{KBID: "kb-1", Have: have, Want: want})
}

// TestForward_RefusalFallsThroughToANonOwnerAndIsRemembered is the whole flow: every
// owner is behind, a non-owner has the data, and the next query does not repeat the
// wasted round trips.
func TestForward_RefusalFallsThroughToANonOwnerAndIsRemembered(t *testing.T) {
	r := shardedRouter(2)
	owners, other := ownerIndexes(r, 7, 2)
	isOwner := map[int]bool{owners[0]: true, owners[1]: true}

	var asked []int
	query := func(idx int, _ context.Context) (int, error) {
		asked = append(asked, idx)
		if isOwner[idx] {
			return 0, refusal(3, 7)
		}
		return idx, nil
	}

	got, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "kb-1", query, WithPinnedVersion(7))
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if got != other {
		t.Fatalf("answered by node %d, want the non-owner %d", got, other)
	}
	if len(asked) != 3 || asked[2] != other {
		t.Fatalf("asked %v, want both owners and then the non-owner", asked)
	}

	asked = nil
	if _, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "kb-1", query, WithPinnedVersion(7)); err != nil {
		t.Fatalf("second Forward: %v", err)
	}
	if !reflect.DeepEqual(asked, []int{other}) {
		t.Errorf("second query asked %v, want only node %d: the refusals were not remembered", asked, other)
	}
}

// TestForward_AnswerFromABehindNodeIsStillAnAnswer: when nobody else can answer, the
// nodes with records get asked, and one that caught up in the meantime serves.
func TestForward_AnswerFromABehindNodeIsStillAnAnswer(t *testing.T) {
	r := shardedRouter(0)
	now := time.Now()
	for idx := 0; idx < 3; idx++ {
		r.lag.note("kb-1", idx, 1, now)
	}

	got, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "kb-1",
		func(idx int, _ context.Context) (string, error) { return "ok", nil }, WithPinnedVersion(7))
	if err != nil || got != "ok" {
		t.Fatalf("Forward = %q, %v; want the behind node's answer", got, err)
	}
}

// TestForward_NonRetryableErrorStopsTheTiers: a terminal answer is the answer. Falling
// through to the next tier would ask everyone the same doomed question.
func TestForward_NonRetryableErrorStopsTheTiers(t *testing.T) {
	r := shardedRouter(2)
	calls := 0

	_, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "kb-1",
		func(int, context.Context) (int, error) {
			calls++
			return 0, stratumerrors.ToGRPCStatus(stratumerrors.ErrKnowledgeBaseNotFound)
		}, WithPinnedVersion(7))

	if err == nil || calls != 1 {
		t.Fatalf("calls = %d, err = %v; want one call and its error", calls, err)
	}
}

// TestForward_OtherFailuresAreNotRecordedAsLag: only a refusal carrying a cursor says a
// node is behind. A connection failure belongs to the breaker.
func TestForward_OtherFailuresAreNotRecordedAsLag(t *testing.T) {
	r := shardedRouter(0)

	_, _ = Forward(r, context.Background(), "/stratum.QueryService/Query", "kb-1",
		func(int, context.Context) (int, error) {
			return 0, stratumerrors.ToGRPCStatus(stratumerrors.ErrIndexNotReady)
		}, WithPinnedVersion(7))

	if tiers := r.routeTiers("kb-1", 7, 3, time.Now()); len(tiers) != 1 || len(tiers[0]) != 3 {
		t.Fatalf("tiers = %v, want no node marked behind", tiers)
	}
}
