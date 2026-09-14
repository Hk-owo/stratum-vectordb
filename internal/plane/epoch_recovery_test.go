package plane

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func safeVersionPlane(local int64, peers []string, cursors map[string]int64, down map[string]bool) *LocalDataPlane {
	dp := newCleanupPlane(nil, nil, peers)
	dp.cursorQuerier = &stubQuerier{cursors: cursors, down: down}
	if local > 0 {
		dp.advanceLocalVersion("kb-1", local)
	}
	return dp
}

// After a restart the local cursor is 0, so the claim must come from the peers
// — and only as far as a quorum still reaches (Stratum_设计文档v13.md §7.8).
func TestLocalDataPlane_SafeDurableVersionTakesAQuorumMinimum(t *testing.T) {
	// Three peers + self = 4 nodes, quorum 3. Reports: 0(self), 9, 7, 2.
	// The largest version three of the four still reach is 2.
	dp := safeVersionPlane(0,
		[]string{"peer-a", "peer-b", "peer-c"},
		map[string]int64{"peer-a": 9, "peer-b": 7, "peer-c": 2},
		nil)

	got, ok, err := dp.SafeDurableVersion(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("SafeDurableVersion: %v", err)
	}
	if !ok {
		t.Fatal("ok = false, want a replica-set answer")
	}
	if got != 2 {
		t.Errorf("safe durable version = %d, want 2 (the third-highest of four reports)", got)
	}
}

// An unreachable peer cannot pad the report set: without a quorum of answers
// the node must not claim anything, because silence is not agreement.
func TestLocalDataPlane_SafeDurableVersionNeedsAQuorumOfAnswers(t *testing.T) {
	dp := safeVersionPlane(0,
		[]string{"peer-a", "peer-b", "peer-c"},
		map[string]int64{"peer-a": 9},
		map[string]bool{"peer-b": true, "peer-c": true})

	_, ok, err := dp.SafeDurableVersion(context.Background(), "kb-1")
	if err == nil {
		t.Fatal("want an error when a quorum of cursors is missing")
	}
	if ok {
		t.Error("ok = true, want false when the answer is unreliable")
	}
	if !strings.Contains(err.Error(), "cursors arrived") {
		t.Errorf("err = %v, want it to name the missing reports", err)
	}
}

// A single-node deployment has nobody to ask, so the caller keeps its local
// view: ok=false rather than a claim of 0, which would look like "no data".
func TestLocalDataPlane_SafeDurableVersionWithoutPeers(t *testing.T) {
	dp := safeVersionPlane(6, nil, nil, nil)

	got, ok, err := dp.SafeDurableVersion(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("SafeDurableVersion: %v", err)
	}
	if ok {
		t.Errorf("ok = true (version %d), want false: there is no replica set", got)
	}
}

// No cursor querier wired is the same situation as no peers.
func TestLocalDataPlane_SafeDurableVersionWithoutQuerier(t *testing.T) {
	dp := newCleanupPlane(nil, nil, []string{"peer-a"})

	if _, ok, err := dp.SafeDurableVersion(context.Background(), "kb-1"); err != nil || ok {
		t.Fatalf("(ok=%v, err=%v), want (false, nil) without a querier", ok, err)
	}
}

// The local report always participates: a node that lost its cursor (0) drags
// the answer down, which is the safe direction.
func TestLocalDataPlane_SafeDurableVersionCountsTheLocalReport(t *testing.T) {
	// One peer + self = 2 nodes, quorum 2. Reports: 0(self), 5(peer).
	dp := safeVersionPlane(0, []string{"peer-a"}, map[string]int64{"peer-a": 5}, nil)

	got, ok, err := dp.SafeDurableVersion(context.Background(), "kb-1")
	if err != nil || !ok {
		t.Fatalf("(ok=%v, err=%v), want a quorum answer", ok, err)
	}
	if got != 0 {
		t.Errorf("safe durable version = %d, want 0: this node's own report counts too", got)
	}
}

// A resolve failure is surfaced rather than silently downgraded to "no peers".
func TestLocalDataPlane_SafeDurableVersionSurfacesResolveErrors(t *testing.T) {
	dp := newCleanupPlane(nil, nil, nil)
	dp.resolveReplicas = func(context.Context) ([]string, error) {
		return nil, errors.New("raft unavailable")
	}
	dp.cursorQuerier = &stubQuerier{}

	if _, _, err := dp.SafeDurableVersion(context.Background(), "kb-1"); err == nil {
		t.Fatal("want the resolve error surfaced")
	}
}
