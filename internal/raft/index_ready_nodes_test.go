package raft

import (
	"reflect"
	"testing"

	"go.uber.org/zap"

	"stratum/internal/types"
	"stratum/internal/wal"
)

// seedOneVersion creates a knowledge base and one version, returning the version
// id the state machine allocated.
func seedOneVersion(t *testing.T, sm *stateMachine, w *wal.MockWAL, kbID string) int64 {
	t.Helper()
	ctx := proposeCtx(t)
	if res := sm.apply(ctx, command{Type: cmdCreateKB, KB: kbPtr(testKB(kbID))}, w, zap.NewNop()); res.Err != nil {
		t.Fatalf("create KB: %v", res.Err)
	}
	if res := sm.apply(ctx, command{Type: cmdCreateVersion, KBID: kbID, ParentVersionID: 0}, w, zap.NewNop()); res.Err != nil {
		t.Fatalf("create version: %v", res.Err)
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for id := range sm.versions {
		return id
	}
	t.Fatal("no version was created")
	return 0
}

// indexReadyNodesOf reads a version's recorded ready-node set.
func indexReadyNodesOf(t *testing.T, sm *stateMachine, versionID int64) []int64 {
	t.Helper()
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.versions[versionID]
	if !ok {
		t.Fatalf("version %d is not in the state machine", versionID)
	}
	return v.IndexReadyNodes
}

// TestStateMachine_RecordsWhichReplicasReportedIndexReady pins the §8.6(d)
// addition: the control layer has to know WHICH replicas are serving a version,
// because the rolling cleanup asks "if I step out for a moment, how many remain?"
// — a question a bare status ("READY somewhere") cannot answer.
func TestStateMachine_RecordsWhichReplicasReportedIndexReady(t *testing.T) {
	ctx := proposeCtx(t)
	sm, w := newTestSM(t)
	versionID := seedOneVersion(t, sm, w, "kb-1")

	// Two replicas report, out of order on purpose: the recorded set must be
	// sorted regardless, so two nodes applying the same reports end up with
	// byte-identical state.
	for _, node := range []int64{7, 3} {
		res := sm.apply(ctx, newUpdateVersionStatusCommand(versionID, types.IndexStatusReady, node), w, zap.NewNop())
		if res.Err != nil {
			t.Fatalf("report from node %d: %v", node, res.Err)
		}
	}
	if got, want := indexReadyNodesOf(t, sm, versionID), []int64{3, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IndexReadyNodes = %v, want %v (sorted)", got, want)
	}

	// A repeat report is a set-membership fact, not a counter: retries and
	// restarts re-report, and the set must not grow or reorder.
	res := sm.apply(ctx, newUpdateVersionStatusCommand(versionID, types.IndexStatusReady, 7), w, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("repeat report: %v", res.Err)
	}
	if got, want := indexReadyNodesOf(t, sm, versionID), []int64{3, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IndexReadyNodes after a repeated report = %v, want %v", got, want)
	}
}

// TestStateMachine_ControlLayerVerdictsRecordNoReplica: nodeID 0 means the
// control layer set the status itself — a reconcile promotion, an availability
// verdict. Recording a node from those would invent a serving replica, and the
// count that decides whether a replica may step out would then be a lie in the
// direction that permits unsafe cleanup.
func TestStateMachine_ControlLayerVerdictsRecordNoReplica(t *testing.T) {
	ctx := proposeCtx(t)
	sm, w := newTestSM(t)
	versionID := seedOneVersion(t, sm, w, "kb-1")

	if res := sm.apply(ctx, newUpdateVersionStatusCommand(versionID, types.IndexStatusReady, 0), w, zap.NewNop()); res.Err != nil {
		t.Fatalf("control-layer promotion: %v", res.Err)
	}
	if got := indexReadyNodesOf(t, sm, versionID); got != nil {
		t.Fatalf("IndexReadyNodes = %v, want none for a control-layer verdict", got)
	}

	// A FAILED report from an identified node is not a serving replica either.
	if res := sm.apply(ctx, newUpdateVersionStatusCommand(versionID, types.IndexStatusFailed, 5), w, zap.NewNop()); res.Err != nil {
		t.Fatalf("failure report: %v", res.Err)
	}
	if got := indexReadyNodesOf(t, sm, versionID); got != nil {
		t.Fatalf("IndexReadyNodes = %v, want none after a non-READY report", got)
	}
}

// TestStateMachine_IndexReadyNodesSurviveADeepCopy is the snapshot half of the
// risk: the field is new in a state machine that is gob-serialized, so it has to
// travel with a snapshot. (gob carries exported fields automatically; this pins
// that the field is exported and copied, which is what the round trip needs.)
func TestStateMachine_IndexReadyNodesSurviveADeepCopy(t *testing.T) {
	ctx := proposeCtx(t)
	sm, w := newTestSM(t)
	versionID := seedOneVersion(t, sm, w, "kb-1")

	if res := sm.apply(ctx, newUpdateVersionStatusCommand(versionID, types.IndexStatusReady, 4), w, zap.NewNop()); res.Err != nil {
		t.Fatalf("report: %v", res.Err)
	}
	snap := sm.deepCopy()
	if got, want := snap.Versions[versionID].IndexReadyNodes, []int64{4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot IndexReadyNodes = %v, want %v", got, want)
	}
}

// TestWithIndexReadyNode_KeepsItSortedAndUnique pins the helper's contract, which
// is what makes the field safe inside a deterministic state machine.
func TestWithIndexReadyNode_KeepsItSortedAndUnique(t *testing.T) {
	cases := []struct {
		name  string
		nodes []int64
		id    int64
		want  []int64
	}{
		{"into empty", nil, 5, []int64{5}},
		{"at the front", []int64{3, 7}, 1, []int64{1, 3, 7}},
		{"in the middle", []int64{3, 7}, 5, []int64{3, 5, 7}},
		{"at the end", []int64{3, 7}, 9, []int64{3, 7, 9}},
		{"already present", []int64{3, 7}, 3, []int64{3, 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := withIndexReadyNode(append([]int64(nil), tc.nodes...), tc.id)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("withIndexReadyNode(%v, %d) = %v, want %v", tc.nodes, tc.id, got, tc.want)
			}
		})
	}
}
