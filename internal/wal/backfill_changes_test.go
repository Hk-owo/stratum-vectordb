package wal

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// TestFileWAL_ChangesFor_AnswersBackfillFromTheSameRecordCrashRecoveryReplays
// pins §7.5's reuse: the BEGIN record that crash recovery replays is also what
// answers a lagging peer's "what changed between version N−1 and N" request.
// The record must survive a restart, because the peer asking may arrive long
// after the writer's process is gone.
func TestFileWAL_ChangesFor_AnswersBackfillFromTheSameRecordCrashRecoveryReplays(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/wal.log"

	changes := []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "alpha"},
		{Op: types.ChangeOpDelete, DocID: "doc-2"},
	}

	w := mustOpenFileWAL(t, path)
	if err := w.WriteBegin(ctx, "kb-1", 1, changes); err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := w.WriteVersionID(ctx, 7); err != nil {
		t.Fatalf("WriteVersionID: %v", err)
	}
	if err := w.WriteCommit(ctx, 7); err != nil {
		t.Fatalf("WriteCommit: %v", err)
	}

	got, ok, err := w.ChangesFor(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("ChangesFor: %v", err)
	}
	if !ok {
		t.Fatal("ChangesFor must find the record a committed version's writer wrote")
	}
	if len(got) != 2 ||
		got[0].Op != types.ChangeOpAdd || got[0].DocID != "doc-1" || got[0].Content != "alpha" ||
		got[1].Op != types.ChangeOpDelete || got[1].DocID != "doc-2" {
		t.Fatalf("ChangesFor = %+v, want the recorded changes %+v", got, changes)
	}

	// The reader may be a peer arriving after this node restarted: the record
	// has to come back from disk, not from the process that wrote it.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w2 := mustOpenFileWAL(t, path)
	got2, ok2, err := w2.ChangesFor(ctx, "kb-1", 7)
	if err != nil {
		t.Fatalf("ChangesFor after restart: %v", err)
	}
	if !ok2 {
		t.Fatal("the BEGIN record must be rebuilt from the file on Open, so a peer can still be answered after a restart")
	}
	if len(got2) != 2 || got2[0].Content != "alpha" || got2[1].DocID != "doc-2" {
		t.Fatalf("ChangesFor after restart = %+v, want %+v", got2, changes)
	}
}

// A version this node never wrote must answer "not here" rather than an empty
// change set — a follower that merely applied the leader's log writes no BEGIN
// record, and an empty set would look like a legitimate no-op version.
func TestFileWAL_ChangesFor_UnknownVersionIsAbsent(t *testing.T) {
	ctx := context.Background()
	w := mustOpenFileWAL(t, t.TempDir()+"/wal.log")

	if _, ok, err := w.ChangesFor(ctx, "kb-1", 42); err != nil || ok {
		t.Fatalf("ChangesFor(unknown version) = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
}

// A version ID is only unique within a knowledge base, so the KB must match.
func TestFileWAL_ChangesFor_OtherKnowledgeBaseIsAbsent(t *testing.T) {
	ctx := context.Background()
	w := mustOpenFileWAL(t, t.TempDir()+"/wal.log")

	if err := w.WriteBegin(ctx, "kb-1", 1, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d", Content: "c"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteVersionID(ctx, 7); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := w.ChangesFor(ctx, "kb-2", 7); err != nil || ok {
		t.Fatalf("ChangesFor(another KB) = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	if _, ok, _ := w.ChangesFor(ctx, "kb-1", 7); !ok {
		t.Error("the same version under its own knowledge base must still be found")
	}
}

// A lagging peer catches up by walking the interval (localVersion, V]: one
// ChangesFor call per step, in ascending version order.
func TestFileWAL_ChangesFor_WalksAnInterval(t *testing.T) {
	ctx := context.Background()
	w := mustOpenFileWAL(t, t.TempDir()+"/wal.log")

	contents := map[int64]string{2: "second", 3: "third", 4: "fourth"}
	for v := int64(2); v <= 4; v++ {
		if err := w.WriteBegin(ctx, "kb-1", v-1, []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc", Content: contents[v]}}); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteVersionID(ctx, v); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteCommit(ctx, v); err != nil {
			t.Fatal(err)
		}
	}

	// The peer already holds version 1 and needs (1, 3].
	var walked []string
	for v := int64(2); v <= 3; v++ {
		got, ok, err := w.ChangesFor(ctx, "kb-1", v)
		if err != nil || !ok || len(got) != 1 {
			t.Fatalf("ChangesFor(v%d) = (%+v, ok=%v, err=%v)", v, got, ok, err)
		}
		walked = append(walked, got[0].Content)
	}
	if len(walked) != 2 || walked[0] != "second" || walked[1] != "third" {
		t.Fatalf("interval walk = %v, want [second third] (v2 then v3, ascending)", walked)
	}
}
