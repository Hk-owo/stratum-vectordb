package raft

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"stratum/internal/types"
)

// The deletion log is what a storage node consumes incrementally, so its two
// load-bearing properties are ordering (seq is what a watermark is expressed in) and
// boundedness (it travels in every snapshot).
func TestStateMachine_DeletionLogIsSequencedAndBounded(t *testing.T) {
	sm := newStateMachine()
	if sm.deletedEpoch == "" {
		t.Fatal("a fresh state machine must mint an epoch: consumers compare it to tell " +
			"\"another cluster\" apart from \"a lagging replica\"")
	}

	for i := 1; i <= 3; i++ {
		sm.recordDeletion("kb-1", int64(i))
	}
	if sm.deletedSeq != 3 {
		t.Fatalf("deletedSeq = %d, want 3", sm.deletedSeq)
	}
	if len(sm.deletedLog) != 3 || sm.deletedLog[0].Seq != 1 || sm.deletedLog[2].VersionID != 3 {
		t.Fatalf("deletedLog = %+v, want three ordered entries", sm.deletedLog)
	}

	// Over the cap: the OLDEST entries go, and what remains is still contiguous.
	limit := sm.deletedLogLimitOrDefault()
	for i := 4; i <= limit+5; i++ {
		sm.recordDeletion("kb-1", int64(i))
	}
	if got := len(sm.deletedLog); got != limit {
		t.Fatalf("len(deletedLog) = %d, want the cap %d", got, limit)
	}
	if first := sm.deletedLog[0].Seq; first != sm.deletedSeq-int64(limit)+1 {
		t.Fatalf("oldest retained seq = %d, want %d — trimming must drop the head, never the tail",
			first, sm.deletedSeq-int64(limit)+1)
	}
}

// The cap is a deployment decision (it trades "how far can a node fall behind" against
// snapshot size), so a configured value has to win over the default.
func TestStateMachine_DeletedLogLimitIsConfigurable(t *testing.T) {
	sm := newStateMachineWithDeletedLogLimit(2)
	for i := 1; i <= 5; i++ {
		sm.recordDeletion("kb-1", int64(i))
	}
	if len(sm.deletedLog) != 2 {
		t.Fatalf("len(deletedLog) = %d, want the configured 2", len(sm.deletedLog))
	}
	if sm.deletedLog[0].VersionID != 4 || sm.deletedLog[1].VersionID != 5 {
		t.Fatalf("retained = %+v, want the two newest", sm.deletedLog)
	}
}

// A KB deletion is ONE entry naming the KB itself. Per-version entries would flush the
// bounded log in a single apply on a large knowledge base, taking everything a consumer
// had not read yet with it.
func TestStateMachine_WholeKBDeletionIsOneEntry(t *testing.T) {
	ctx := func() context.Context { return context.Background() }
	sm := newStateMachine()
	sm.kbs["kb-1"] = types.KnowledgeBaseMeta{KBID: "kb-1"}
	sm.versionsByKB["kb-1"] = []int64{1, 2, 3}
	for _, id := range []int64{1, 2, 3} {
		sm.versions[id] = types.VersionMeta{VersionID: id, KBID: "kb-1"}
	}

	res := sm.apply(ctx(), command{Type: cmdRemoveKBMeta, KBID: "kb-1"}, nil, zap.NewNop())
	if res.Err != nil {
		t.Fatalf("apply(cmdRemoveKBMeta): %v", res.Err)
	}
	if len(sm.deletedLog) != 1 {
		t.Fatalf("deletedLog = %+v, want exactly one KB-level entry", sm.deletedLog)
	}
	if entry := sm.deletedLog[0]; entry.KBID != "kb-1" || entry.VersionID != 0 {
		t.Fatalf("entry = %+v, want kb-1 with version 0 (the KB itself)", entry)
	}
}

// The log rides in the snapshot, so a node restored from one can still serve a
// consumer's incremental read.
func TestStateMachine_DeletionLogSurvivesASnapshot(t *testing.T) {
	sm := newStateMachine()
	sm.recordDeletion("kb-1", 7)
	sm.recordDeletion("kb-1", 0)
	epoch, seq := sm.deletedEpoch, sm.deletedSeq

	data, err := sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	restored := newStateMachine()
	if err := restored.restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.deletedEpoch != epoch || restored.deletedSeq != seq {
		t.Fatalf("restored (epoch %q, seq %d), want (%q, %d)", restored.deletedEpoch, restored.deletedSeq, epoch, seq)
	}
	if len(restored.deletedLog) != 2 || restored.deletedLog[1].VersionID != 0 {
		t.Fatalf("restored log = %+v, want both entries", restored.deletedLog)
	}
}

// DeletionsSince is a paged read, and its three facts come from one lock hold so they
// cannot describe different states.
func TestRaftNodeImpl_DeletionsSincePagesAndReportsTheWindow(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	for _, v := range []int64{1, 2, 3} {
		impl.sm.mu.Lock()
		impl.sm.recordDeletion("kb-1", v)
		impl.sm.mu.Unlock()
	}

	entries, latest, oldest, epoch, err := impl.DeletionsSince(context.Background(), 0, 2)
	if err != nil {
		t.Fatalf("DeletionsSince: %v", err)
	}
	if latest != 3 || oldest != 1 {
		t.Fatalf("latest/oldest = %d/%d, want 3/1", latest, oldest)
	}
	if epoch == "" {
		t.Fatal("epoch must be reported: a consumer compares it with its watermark")
	}
	if len(entries) != 2 || entries[0].VersionID != 1 || entries[1].VersionID != 2 {
		t.Fatalf("entries = %+v, want the first page (v1, v2)", entries)
	}

	next, _, _, _, err := impl.DeletionsSince(context.Background(), entries[len(entries)-1].Seq, 2)
	if err != nil {
		t.Fatalf("DeletionsSince (page 2): %v", err)
	}
	if len(next) != 1 || next[0].VersionID != 3 {
		t.Fatalf("page 2 = %+v, want just v3", next)
	}

	done, latest2, _, _, err := impl.DeletionsSince(context.Background(), latest, 2)
	if err != nil {
		t.Fatalf("DeletionsSince (caught up): %v", err)
	}
	if len(done) != 0 || latest2 != latest {
		t.Fatalf("caught-up read = %+v (latest %d), want nothing new", done, latest2)
	}
}
