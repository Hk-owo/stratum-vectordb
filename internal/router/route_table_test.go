package router

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRouteTable_BeforeTheFirstRefreshKnowsNothing: an empty cache means "not known
// yet", so a query carries no credential and every node stays in line.
func TestRouteTable_BeforeTheFirstRefreshKnowsNothing(t *testing.T) {
	tbl := NewRouteTable(func(context.Context) (routeSnapshot, error) {
		return routeSnapshot{}, errors.New("not yet")
	}, time.Minute, nil)

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

	if v, ok := tbl.ExpectedVersion("kb-1"); !ok || v != 9 {
		t.Errorf("expected version = %d/%v after a failed refresh, want 9/true", v, ok)
	}
}
