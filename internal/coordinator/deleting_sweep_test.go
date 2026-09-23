package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"stratum/internal/types"
)

// sweepMeta is the two-call metadata read the sweep depends on — narrow on purpose,
// so the sweep has nothing else to reach for.
type sweepMeta struct {
	kbs        []types.KnowledgeBaseMeta
	versions   map[string][]types.VersionMeta
	versionErr map[string]error
	listErr    error
}

func (m sweepMeta) ListKnowledgeBases(context.Context) ([]types.KnowledgeBaseMeta, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.kbs, nil
}

func (m sweepMeta) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	if err := m.versionErr[kbID]; err != nil {
		return nil, err
	}
	return m.versions[kbID], nil
}

func deletingVersion(kbID string, versionID int64) types.VersionMeta {
	return types.VersionMeta{VersionID: versionID, KBID: kbID, Deleting: true}
}

// Only a knowledge base that actually has a version stuck at Deleting is touched: the
// pass must not re-run cleanup for every knowledge base it can see.
func TestDeletingVersionSweeper_ResumesOnlyKnowledgeBasesWithDeletingVersions(t *testing.T) {
	meta := sweepMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-stuck"}, {KBID: "kb-healthy"}},
		versions: map[string][]types.VersionMeta{
			"kb-stuck":   {deletingVersion("kb-stuck", 1)},
			"kb-healthy": {{VersionID: 2, KBID: "kb-healthy"}},
		},
	}
	coord := NewMockDeleteVersionCoordinator()

	sweeper := NewDeletingVersionSweeper(meta, coord, time.Minute, nil)
	resumed, err := sweeper.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1", resumed)
	}
	if calls := coord.Calls(); len(calls) != 1 || calls[0] != "kb-stuck" {
		t.Errorf("Execute calls = %v, want [kb-stuck]", calls)
	}
}

// One Execute per knowledge base is enough — it re-scans that knowledge base's Deleting
// versions itself — and the count reported is knowledge bases, not versions.
func TestDeletingVersionSweeper_OneExecutePerKnowledgeBase(t *testing.T) {
	meta := sweepMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-stuck"}},
		versions: map[string][]types.VersionMeta{
			"kb-stuck": {
				deletingVersion("kb-stuck", 1),
				deletingVersion("kb-stuck", 2),
				deletingVersion("kb-stuck", 3),
			},
		},
	}
	coord := NewMockDeleteVersionCoordinator()

	resumed, err := NewDeletingVersionSweeper(meta, coord, time.Minute, nil).SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 knowledge base", resumed)
	}
	if calls := coord.Calls(); len(calls) != 1 {
		t.Errorf("Execute calls = %v, want exactly one (Execute re-scans the whole KB)", calls)
	}
}

// "I cannot enumerate this knowledge base" is not "there is nothing to do there": the
// failure is reported and named, and the readable knowledge bases still run.
func TestDeletingVersionSweeper_UnreadableKnowledgeBaseIsReportedNotSkipped(t *testing.T) {
	meta := sweepMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-unreadable"}, {KBID: "kb-stuck"}},
		versions: map[string][]types.VersionMeta{
			"kb-stuck": {deletingVersion("kb-stuck", 1)},
		},
		versionErr: map[string]error{"kb-unreadable": errors.New("metadata away")},
	}
	coord := NewMockDeleteVersionCoordinator()

	resumed, err := NewDeletingVersionSweeper(meta, coord, time.Minute, nil).SweepOnce(context.Background())
	if err == nil {
		t.Error("an unreadable knowledge base must be surfaced, not silently treated as empty")
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 (the readable knowledge base still runs)", resumed)
	}
	if calls := coord.Calls(); len(calls) != 1 || calls[0] != "kb-stuck" {
		t.Errorf("Execute calls = %v, want [kb-stuck]", calls)
	}
}

// A cleanup that fails must not stop the pass: the state it would have repaired is
// still there for the next tick.
func TestDeletingVersionSweeper_ExecuteFailureDoesNotStopThePass(t *testing.T) {
	meta := sweepMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-a"}, {KBID: "kb-b"}},
		versions: map[string][]types.VersionMeta{
			"kb-a": {deletingVersion("kb-a", 1)},
			"kb-b": {deletingVersion("kb-b", 2)},
		},
	}
	coord := NewMockDeleteVersionCoordinator()
	coord.SetExecuteFunc(func(_ context.Context, kbID string) error {
		if kbID == "kb-a" {
			return errors.New("broadcast failed")
		}
		return nil
	})

	resumed, err := NewDeletingVersionSweeper(meta, coord, time.Minute, nil).SweepOnce(context.Background())
	if err == nil {
		t.Error("a failed cleanup must be reported")
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 (only kb-b finished)", resumed)
	}
	if calls := coord.Calls(); len(calls) != 2 {
		t.Errorf("Execute calls = %v, want both knowledge bases attempted", calls)
	}
}

// A metadata list that fails outright is reported, and nothing is attempted.
func TestDeletingVersionSweeper_ListFailureAttemptsNothing(t *testing.T) {
	meta := sweepMeta{listErr: errors.New("metadata away")}
	coord := NewMockDeleteVersionCoordinator()

	resumed, err := NewDeletingVersionSweeper(meta, coord, time.Minute, nil).SweepOnce(context.Background())
	if err == nil {
		t.Error("a failed list must be reported")
	}
	if resumed != 0 || len(coord.Calls()) != 0 {
		t.Errorf("resumed = %d calls = %v, want nothing attempted", resumed, coord.Calls())
	}
}
