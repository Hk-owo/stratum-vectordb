package router

import (
	"context"
	"errors"
	"testing"
	"time"
)

// tableWith builds a route table holding one fixed snapshot.
func tableWith(t *testing.T, servable map[string]map[int]bool, expected map[string]int64) *RouteTable {
	t.Helper()
	snap := routeSnapshot{servable: servable, expected: expected}
	tbl := NewRouteTable(func(context.Context) (routeSnapshot, error) { return snap, nil }, time.Minute, nil)
	tbl.Refresh(context.Background())
	return tbl
}

// TestRouteTable_NarrowsCandidatesToReportedHolders is §9.3(1)'s core: the
// leader's aggregate says which nodes hold the version, and a node it does not
// name is not a candidate — it could answer completely and still be wrong.
func TestRouteTable_NarrowsCandidatesToReportedHolders(t *testing.T) {
	tbl := tableWith(t,
		map[string]map[int]bool{"kb-1": {0: true, 2: true}},
		map[string]int64{"kb-1": 8},
	)

	got := tbl.Servable("kb-1", 8, 3)
	want := []int{0, 2}
	if len(got) != len(want) {
		t.Fatalf("Servable = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Servable = %v, want %v", got, want)
		}
	}
}

// TestRouteTable_NoCredentialMeansNoFiltering keeps the pre-§9 behaviour
// available: with nothing to be fresh about, every node qualifies.
func TestRouteTable_NoCredentialMeansNoFiltering(t *testing.T) {
	tbl := tableWith(t,
		map[string]map[int]bool{"kb-1": {1: true}},
		map[string]int64{"kb-1": 1},
	)

	if got := tbl.Servable("kb-1", 0, 3); len(got) != 3 {
		t.Errorf("Servable with no credential = %v, want all 3 nodes", got)
	}
}

// TestRouteTable_NoReportedHoldersDoesNotRefuseTheQuery pins the decision that
// changed with the data source.
//
// "The leader named no holder" is a statement about what the leader has HEARD
// (the aggregate is soft state, §7.13.4), not about where data lives — so it
// must not narrow the candidates to nothing. The freshness credential on the
// forwarded request is what keeps a stale answer impossible: a node that does
// not hold the version cannot answer it at all.
//
// The previous implementation probed cursors directly and treated "every cursor
// behind" as an error. That answer is not available from this data source and,
// more to the point, was never the right one: silence is not evidence of
// absence.
func TestRouteTable_NoReportedHoldersDoesNotRefuseTheQuery(t *testing.T) {
	tbl := tableWith(t,
		map[string]map[int]bool{"kb-1": {}},
		map[string]int64{"kb-1": 9},
	)

	if got := tbl.Servable("kb-1", 9, 3); len(got) != 3 {
		t.Errorf("Servable with no reported holders = %v, want all 3 nodes (the credential does the gating)", got)
	}
}

// TestRouteTable_HoldersTheStationCannotDialFallBack: if every holder the leader
// named is a node this station has no connection to, fall back rather than fail.
// The credential still gates correctness, and a station whose node list differs
// from the leader's is a deployment fact, not a reason to refuse reads.
func TestRouteTable_HoldersTheStationCannotDialFallBack(t *testing.T) {
	tbl := tableWith(t,
		map[string]map[int]bool{"kb-1": {7: true}}, // index beyond this station's 2
		map[string]int64{"kb-1": 9},
	)

	if got := tbl.Servable("kb-1", 9, 2); len(got) != 2 {
		t.Errorf("Servable with only undialable holders = %v, want all 2 nodes", got)
	}
}

// TestRouteTable_BeforeTheFirstRefreshServesEverything pins the warm-up
// decision: an empty cache means "not known yet", not "nothing can serve".
// Refusing every query until the first refresh lands would turn a warm-up into
// an outage.
func TestRouteTable_BeforeTheFirstRefreshServesEverything(t *testing.T) {
	tbl := NewRouteTable(func(context.Context) (routeSnapshot, error) {
		return routeSnapshot{}, errors.New("not yet")
	}, time.Minute, nil)

	if got := tbl.Servable("kb-1", 8, 3); len(got) != 3 {
		t.Errorf("Servable before any refresh = %v, want all 3 nodes", got)
	}
	if _, ok := tbl.ExpectedVersion("kb-1"); ok {
		t.Error("no expected version before the first refresh")
	}
}

// TestRouteTable_KeepsItsSnapshotWhenARefreshFails: stale routing information
// beats none, and the freshness credential is what protects the caller from a
// stale ANSWER in the meantime.
func TestRouteTable_KeepsItsSnapshotWhenARefreshFails(t *testing.T) {
	fail := false
	snap := routeSnapshot{
		servable: map[string]map[int]bool{"kb-1": {0: true}},
		expected: map[string]int64{"kb-1": 9},
	}
	tbl := NewRouteTable(func(context.Context) (routeSnapshot, error) {
		if fail {
			return routeSnapshot{}, errors.New("control layer unreachable")
		}
		return snap, nil
	}, time.Minute, nil)
	tbl.Refresh(context.Background())

	fail = true
	tbl.Refresh(context.Background())

	if got := tbl.Servable("kb-1", 9, 2); len(got) != 1 || got[0] != 0 {
		t.Errorf("after a failed refresh, Servable = %v, want the previous snapshot's [0]", got)
	}
	if v, ok := tbl.ExpectedVersion("kb-1"); !ok || v != 9 {
		t.Errorf("expected version = %d/%v after a failed refresh, want 9/true", v, ok)
	}
}

// TestRouteTable_UnreportedNodeDropsOut: a node the leader never named is not a
// candidate — the aggregate only speaks for nodes that have reported.
func TestRouteTable_UnreportedNodeDropsOut(t *testing.T) {
	tbl := tableWith(t,
		map[string]map[int]bool{"kb-1": {0: true}},
		map[string]int64{"kb-1": 1},
	)

	got := tbl.Servable("kb-1", 1, 2)
	if len(got) != 1 || got[0] != 0 {
		t.Errorf("Servable = %v, want only the node the leader named", got)
	}
}

// TestForward_UsesTheRoutingTableToSkipANodeTheLeaderDidNotName is the
// integration that matters: a query must not land on a node the leader did not
// report holding the version, even though that node is up and would answer.
func TestForward_UsesTheRoutingTableToSkipANodeTheLeaderDidNotName(t *testing.T) {
	tbl := tableWith(t,
		map[string]map[int]bool{"kb-1": {1: true}},
		map[string]int64{"kb-1": 9},
	)
	r := &Router{
		storageAddrs:    []string{"s1", "s2"},
		storageBreakers: []*breaker{newBreaker(defaultBreakerConfig), newBreaker(defaultBreakerConfig)},
		routes:          tbl,
	}

	asked := map[int]int{}
	for i := 0; i < 4; i++ {
		if _, err := Forward(r, context.Background(), "/stratum.QueryService/Query", "kb-1",
			func(idx int, _ context.Context) (int, error) {
				asked[idx]++
				return idx, nil
			}); err != nil {
			t.Fatalf("Forward: %v", err)
		}
	}

	if asked[0] != 0 {
		t.Errorf("node 0 was asked %d times, want 0 (asked: %v) — the leader did not name it", asked[0], asked)
	}
	if asked[1] == 0 {
		t.Error("the named node must serve the query")
	}
}
