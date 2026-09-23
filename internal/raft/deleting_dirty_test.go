package raft

import (
	"context"
	"testing"

	"stratum/internal/kvraft"
	"stratum/internal/types"
)

// The narrow read: ids only, and only the versions actually marked Deleting.
func TestRaftNodeImpl_DeletingVersionIDs(t *testing.T) {
	impl := &RaftNodeImpl{sm: newStateMachine()}
	impl.sm.kbs["kb-1"] = types.KnowledgeBaseMeta{KBID: "kb-1"}
	impl.sm.versionsByKB["kb-1"] = []int64{1, 2, 3}
	impl.sm.versions[1] = types.VersionMeta{VersionID: 1, KBID: "kb-1", Deleting: true}
	impl.sm.versions[2] = types.VersionMeta{VersionID: 2, KBID: "kb-1"}
	impl.sm.versions[3] = types.VersionMeta{VersionID: 3, KBID: "kb-1", Deleting: true}

	ids, err := impl.DeletingVersionIDs(context.Background(), "kb-1")
	if err != nil {
		t.Fatalf("DeletingVersionIDs: %v", err)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Errorf("ids = %v, want [1 3]", ids)
	}

	// A knowledge base that does not exist is NotFound, not an empty answer: the caller
	// distinguishes "nothing to resume" from "I cannot tell".
	if _, err := impl.DeletingVersionIDs(context.Background(), "kb-none"); err == nil {
		t.Error("a missing knowledge base must report NotFound rather than an empty set")
	}
}

// The dirty set: repeated marks collapse, Take both returns and clears it.
func TestRaftNodeImpl_DeletingDirtySetCollapsesAndClears(t *testing.T) {
	impl := &RaftNodeImpl{}

	if got := impl.TakeDirtyDeletingKBs(); got != nil {
		t.Errorf("a fresh node has nothing dirty, got %v", got)
	}

	impl.MarkDeletingDirty("kb-1")
	impl.MarkDeletingDirty("kb-1") // the same knowledge base twice: one entry
	impl.MarkDeletingDirty("kb-2")
	impl.MarkDeletingDirty("") // an empty id is not a knowledge base

	got := impl.TakeDirtyDeletingKBs()
	if len(got) != 2 {
		t.Fatalf("dirty = %v, want 2 knowledge bases", got)
	}
	seen := map[string]bool{}
	for _, kbID := range got {
		seen[kbID] = true
	}
	if !seen["kb-1"] || !seen["kb-2"] {
		t.Errorf("dirty = %v, want kb-1 and kb-2", got)
	}

	if again := impl.TakeDirtyDeletingKBs(); again != nil {
		t.Errorf("Take must clear the set, second call got %v", again)
	}
}

// MarkAllDeletingDirty is the conservative start-up / snapshot-install direction: every
// knowledge base in the state machine becomes dirty.
func TestRaftNodeImpl_MarkAllDeletingDirty(t *testing.T) {
	impl := &RaftNodeImpl{sm: newStateMachine()}
	impl.sm.kbs["kb-1"] = types.KnowledgeBaseMeta{KBID: "kb-1"}
	impl.sm.kbs["kb-2"] = types.KnowledgeBaseMeta{KBID: "kb-2"}
	impl.MarkDeletingDirty("kb-already")

	impl.MarkAllDeletingDirty()

	got := impl.TakeDirtyDeletingKBs()
	if len(got) != 3 {
		t.Fatalf("dirty = %v, want all three knowledge bases", got)
	}
}

// The apply path is the dirty set's ONLY source, so this is the test that keeps the sweep
// from going blind: a successful cmdMarkVersionDeleting must reach it.
func TestRaftNodeImpl_AppliedMarkVersionDeletingMarksTheKnowledgeBaseDirty(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)
	// READY, because the mark's admission refuses a PENDING version.
	impl.sm.mu.Lock()
	impl.sm.kbs["kb-1"] = types.KnowledgeBaseMeta{KBID: "kb-1"}
	impl.sm.versionsByKB["kb-1"] = []int64{1}
	impl.sm.versions[1] = types.VersionMeta{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady}
	impl.sm.mu.Unlock()

	raw, err := encodeCommand(newMarkVersionDeletingCommand("kb-1", 1, types.VersionDeleteSubtree))
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	impl.handleEntryMsg(kvraft.ApplyMsg{Index: 7, Term: 3, Command: raw})

	got := impl.TakeDirtyDeletingKBs()
	if len(got) != 1 || got[0] != "kb-1" {
		t.Errorf("dirty = %v, want [kb-1] — an applied mark must reach the sweep", got)
	}
}

// A REJECTED mark leaves nothing to resume (the version was active, pending, or missing),
// so it must not wake the sweep for that knowledge base.
func TestRaftNodeImpl_RejectedMarkDoesNotMarkDirty(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	raw, err := encodeCommand(newMarkVersionDeletingCommand("kb-missing", 1, types.VersionDeleteSubtree))
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	impl.handleEntryMsg(kvraft.ApplyMsg{Index: 8, Term: 3, Command: raw})

	if got := impl.TakeDirtyDeletingKBs(); got != nil {
		t.Errorf("a rejected mark must not mark anything dirty, got %v", got)
	}
}
