package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"stratum/internal/kvraft"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// TestRaftNodeImpl_RestartAfterSnapshotRestoresStateMachine verifies that
// a node restarted after its log has been compacted (locally triggered
// snapshot) restores its state machine from the persisted snapshot:
// every pre-compaction KB/version must be back, and the version counter
// must continue without collisions. Before the startup restore was added,
// a restart booted an empty state machine whose lastApplied bookkeeping
// claimed the compacted entries were applied — all pre-snapshot state was
// silently lost.
func TestRaftNodeImpl_RestartAfterSnapshotRestoresStateMachine(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	addr := freeLoopbackAddrForTest(t)
	w := wal.NewMockWAL()

	newImpl := func() *RaftNodeImpl {
		impl, err := NewRaftNodeImpl(Config{
			NodeID:             1,
			DataDir:            dir,
			RaftAddr:           addr,
			WAL:                w,
			MaxLogLength:       4,
			ElectionTimeoutMin: 30 * time.Millisecond,
			ElectionTimeoutMax: 60 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaftNodeImpl: %v", err)
		}
		if !waitForCond(t, 2*time.Second, impl.raft.IsLeader) {
			t.Fatalf("never became leader")
		}
		return impl
	}

	// First incarnation: build state, then force a local snapshot by
	// letting the log grow past MaxLogLength.
	impl1 := newImpl()
	mustCreateKBImpl(t, impl1, "kb1")
	lastID := int64(0)
	for i := 0; i < 3; i++ {
		v, err := impl1.ProposeCreateVersion(ctx, "kb1", 0)
		if err != nil {
			t.Fatalf("ProposeCreateVersion #%d: %v", i, err)
		}
		lastID = v
	}
	if !waitForCond(t, 5*time.Second, func() bool {
		base, _ := impl1.raft.LogBase()
		return base > 0
	}) {
		t.Fatalf("local snapshot never compacted the log")
	}
	data, snapIdx, err := impl1.persister.LoadSnapshot()
	if err != nil || data == nil || snapIdx == 0 {
		t.Fatalf("persisted snapshot missing after local compaction (len=%d idx=%d err=%v)", len(data), snapIdx, err)
	}
	impl1.Stop()

	// Restart on the same DataDir: the log is compacted, so only the
	// persisted snapshot can restore the state machine.
	impl2 := newImpl()
	t.Cleanup(impl2.Stop)

	kb, err := impl2.GetKB(ctx, "kb1")
	if err != nil {
		t.Fatalf("GetKB after snapshot restart: %v", err)
	}
	if kb.KBID != "kb1" {
		t.Fatalf("KB metadata lost across snapshot restart: %+v", kb)
	}
	v, err := impl2.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion after snapshot restart: %v", err)
	}
	if v <= lastID {
		t.Fatalf("version ID after snapshot restart = %d, want > %d (counter must survive)", v, lastID)
	}
}

// TestRaftNodeImpl_InstalledSnapshotIsPersisted verifies the
// InstallSnapshot side of the state-machine layer: when a leader pushes a
// snapshot (this node had fallen too far behind), the node must both
// replace its state machine AND persist the snapshot data, because
// InstallDone trims every covered log entry — without the persisted data
// a restart would find a trimmed log but no matching state-machine state.
func TestRaftNodeImpl_InstalledSnapshotIsPersisted(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	// Build a snapshot from the live state machine and install it as if
	// it had been pushed by a leader.
	data, err := impl.sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	idx := impl.raft.LastApplied()
	if !waitForCond(t, 2*time.Second, func() bool { return impl.raft.LastApplied() > 0 }) {
		t.Fatal("expected at least the no-op election entry to be applied")
	}
	idx = impl.raft.LastApplied()
	term := impl.raft.Term()
	impl.handleSnapshotMsg(kvraft.ApplyMsg{
		IsSnapshot:    true,
		SnapshotData:  data,
		SnapshotIndex: idx,
		SnapshotTerm:  term,
	})

	got, gotIdx, err := impl.persister.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if gotIdx != idx || string(got) != string(data) {
		t.Fatalf("installed snapshot not persisted: got (idx=%d len=%d), want (idx=%d len=%d)",
			gotIdx, len(got), idx, len(data))
	}

	// Bookkeeping must match the installed snapshot: log trimmed to the
	// snapshot index, lastApplied advanced.
	base, _ := impl.raft.LogBase()
	if base != idx {
		t.Errorf("log base after install = %d, want %d", base, idx)
	}
	if impl.raft.LastApplied() < idx {
		t.Errorf("LastApplied after install = %d, want >= %d", impl.raft.LastApplied(), idx)
	}
}

// TestRaftNodeImpl_InstalledSnapshotTriggersVersionSync verifies that
// installing a leader-pushed snapshot fires the onVersionCreated data-sync
// hook for every version the snapshot restores — and only for those. A
// snapshot covers log entries that are never applied individually on the
// receiving node, so without this explicit trigger the follower would
// never pull storage-layer data (docstore/vecstore) or build indexes for
// snapshot-covered versions. PENDING versions (writes not yet completed)
// and Deleting versions must be skipped: syncing them could prematurely
// READY-mark a version whose data is still being written.
func TestRaftNodeImpl_InstalledSnapshotTriggersVersionSync(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	ctx := context.Background()

	// Build a KB with two READY versions plus one PENDING version (whose
	// storage write has not completed) — the PENDING one must NOT be
	// synced.
	mustCreateKBImpl(t, impl, "kb1")
	v1, err := impl.ProposeCreateVersion(ctx, "kb1", 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion v1: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v1, types.IndexStatusReady)
	v2, err := impl.ProposeCreateVersion(ctx, "kb1", v1)
	if err != nil {
		t.Fatalf("ProposeCreateVersion v2: %v", err)
	}
	mustUpdateStatusImpl(t, impl, v2, types.IndexStatusReady)
	v3, err := impl.ProposeCreateVersion(ctx, "kb1", v2)
	if err != nil {
		t.Fatalf("ProposeCreateVersion v3: %v", err)
	}
	_ = v3 // stays PENDING: no READY status update

	// Register the hook only now, so it observes only the snapshot-install
	// trigger — not the normal apply path of the versions above.
	synced := make(chan string, 4)
	impl.SetOnVersionCreated(func(kbID string, versionID int64) {
		synced <- fmt.Sprintf("%s/%d", kbID, versionID)
	})

	data, err := impl.sm.serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if !waitForCond(t, 2*time.Second, func() bool { return impl.raft.LastApplied() > 0 }) {
		t.Fatal("expected at least the no-op election entry to be applied")
	}
	idx := impl.raft.LastApplied()
	term := impl.raft.Term()
	impl.handleSnapshotMsg(kvraft.ApplyMsg{
		IsSnapshot:    true,
		SnapshotData:  data,
		SnapshotIndex: idx,
		SnapshotTerm:  term,
	})

	// v1 and v2 must be synced; v3 (PENDING) must not.
	want := map[string]bool{"kb1/1": true, "kb1/2": true}
	for len(want) > 0 {
		select {
		case got := <-synced:
			delete(want, got)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for snapshot-version sync hooks; remaining: %v", want)
		}
	}
	select {
	case got := <-synced:
		t.Fatalf("unexpected sync hook for %s (PENDING version must be skipped)", got)
	case <-time.After(200 * time.Millisecond):
	}
}
