package kvraft

import (
	"os"
	"path/filepath"
	"testing"

	kvraftpb "stratum/api/proto/kvraft"
	"stratum/internal/kvstorage"
)

// TestSnapshot_KeepsLogWhenSnapshotWriteFails pins H5 of
// docs/code-review-2026-10-18.md.
//
// A snapshot whose write fails must not trim the log. Trimming is irreversible:
// the discarded entries are in neither the snapshot nor the log, so a restart
// replays neither and the state machine comes back from an older snapshot with
// the metadata in between simply gone. The log is what makes the failure
// survivable, so it has to stay until the snapshot is safely on disk.
func TestSnapshot_KeepsLogWhenSnapshotWriteFails(t *testing.T) {
	base := filepath.Join(t.TempDir(), "raft")
	persister := kvstorage.NewPersister(base)

	log := []*kvraftpb.Entry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	if err := persister.SaveState(1, -1, log); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	rf, err := NewRaft(1, NewTransport(nil), make(chan ApplyMsg, 16), persister)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(rf.Stop)

	// Make the snapshot write fail: the rename target exists and is a non-empty
	// directory, so atomicWriteGob's rename cannot replace it.
	blocked := base + ".snapshot"
	if err := os.MkdirAll(filepath.Join(blocked, "keep"), 0o755); err != nil {
		t.Fatalf("mkdir blocking snapshot path: %v", err)
	}

	rf.Snapshot(2, []byte("snapshot-data"))

	rf.mu.Lock()
	gotBase := rf.log[0].Index
	rf.mu.Unlock()
	if gotBase != 0 {
		t.Errorf("log base = %d after a failed snapshot write, want 0 — trimming without a persisted snapshot loses the [1,2] entries for good", gotBase)
	}

	// Sanity: the same call with a writable path does trim, so the assertion
	// above is about the failure and not about Snapshot being a no-op.
	if err := os.RemoveAll(blocked); err != nil {
		t.Fatalf("unblock snapshot path: %v", err)
	}
	rf.Snapshot(2, []byte("snapshot-data"))
	rf.mu.Lock()
	gotBase = rf.log[0].Index
	rf.mu.Unlock()
	if gotBase != 2 {
		t.Errorf("log base = %d after a successful snapshot, want 2", gotBase)
	}
}
