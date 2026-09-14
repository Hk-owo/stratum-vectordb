package plane

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// recordingCompactorWAL is a WAL that both writes and records what compaction was
// asked to do — the seam ReclaimChanges talks to.
type recordingCompactorWAL struct {
	stubWAL
	compactions []map[string]int64
	compactErr  error
}

func (w *recordingCompactorWAL) Compact(_ context.Context, keepThrough map[string]int64) error {
	if w.compactErr != nil {
		return w.compactErr
	}
	kept := make(map[string]int64, len(keepThrough))
	for kbID, v := range keepThrough {
		kept[kbID] = v
	}
	w.compactions = append(w.compactions, kept)
	return nil
}

// watermarkControl answers ReclaimableChangesThrough from a fixed table.
type watermarkControl struct {
	stubControl
	watermarks map[string]int64
}

func (c *watermarkControl) ReclaimableChangesThrough(kbID string) (int64, bool) {
	v, ok := c.watermarks[kbID]
	return v, ok
}

func reclaimPlane(t *testing.T, wal TransactionWAL, control ControlPlane, kbs ...string) *LocalDataPlane {
	t.Helper()
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:      wal,
		Executor: &stubExecutor{t: &tracer{}},
		Control:  control,
	})
	for _, kbID := range kbs {
		dp.advanceLocalVersion(kbID, 1)
	}
	return dp
}

// The storage layer asks its control layer for the watermark and hands exactly that
// to the WAL: the reclaim decision is never made locally.
func TestLocalDataPlane_ReclaimChangesUsesTheControlWatermark(t *testing.T) {
	wal := &recordingCompactorWAL{}
	control := &watermarkControl{watermarks: map[string]int64{"kb-1": 7}}
	dp := reclaimPlane(t, wal, control, "kb-1")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if !reflect.DeepEqual(used, map[string]int64{"kb-1": 7}) {
		t.Errorf("watermarks used = %v, want kb-1:7", used)
	}
	if len(wal.compactions) != 1 || !reflect.DeepEqual(wal.compactions[0], map[string]int64{"kb-1": 7}) {
		t.Errorf("compactions = %v, want one call with kb-1:7", wal.compactions)
	}
}

// An unknown watermark is not an error and not a default: that knowledge base keeps
// everything, while one with a known watermark is still reclaimed. The two are
// independent, so a partial answer is used as far as it goes.
func TestLocalDataPlane_ReclaimChangesSkipsKnowledgeBasesWithNoWatermark(t *testing.T) {
	wal := &recordingCompactorWAL{}
	control := &watermarkControl{watermarks: map[string]int64{"kb-1": 3}}
	dp := reclaimPlane(t, wal, control, "kb-1", "kb-2")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if !reflect.DeepEqual(used, map[string]int64{"kb-1": 3}) {
		t.Errorf("watermarks used = %v, want only kb-1 (kb-2's is unknown)", used)
	}
	if _, present := wal.compactions[0]["kb-2"]; present {
		t.Error("kb-2 was given a watermark it does not have; an unknown must not become a default")
	}
}

// Nothing to reclaim is not a failure: no watermark at all means no compaction call.
func TestLocalDataPlane_ReclaimChangesDoesNothingWithoutWatermarks(t *testing.T) {
	wal := &recordingCompactorWAL{}
	dp := reclaimPlane(t, wal, &watermarkControl{}, "kb-1")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(used) != 0 {
		t.Errorf("used = %v, want empty", used)
	}
	if len(wal.compactions) != 0 {
		t.Errorf("compactions = %v, want none", wal.compactions)
	}
}

// A WAL that cannot compact is not an error either — the in-memory one just grows.
func TestLocalDataPlane_ReclaimChangesIsQuietWhenTheWALCannotCompact(t *testing.T) {
	control := &watermarkControl{watermarks: map[string]int64{"kb-1": 7}}
	dp := reclaimPlane(t, &stubWAL{t: &tracer{}}, control, "kb-1")

	used, err := dp.ReclaimChanges(context.Background())
	if err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(used) != 0 {
		t.Errorf("used = %v, want empty", used)
	}
}

// No control layer means no judgement, so nothing is reclaimed.
func TestLocalDataPlane_ReclaimChangesNeedsAControlLayer(t *testing.T) {
	wal := &recordingCompactorWAL{}
	dp := reclaimPlane(t, wal, nil, "kb-1")

	if _, err := dp.ReclaimChanges(context.Background()); err != nil {
		t.Fatalf("ReclaimChanges: %v", err)
	}
	if len(wal.compactions) != 0 {
		t.Errorf("compactions = %v, want none without a control layer", wal.compactions)
	}
}

// A compaction failure must surface: silently believing the WAL shrank would leave
// the operator with an unbounded log and no signal.
func TestLocalDataPlane_ReclaimChangesReportsCompactionFailure(t *testing.T) {
	wal := &recordingCompactorWAL{compactErr: errors.New("disk full")}
	control := &watermarkControl{watermarks: map[string]int64{"kb-1": 7}}
	dp := reclaimPlane(t, wal, control, "kb-1")

	if _, err := dp.ReclaimChanges(context.Background()); err == nil {
		t.Fatal("a failed compaction must be reported")
	}
}
