package kvraft

import (
	"os"
	"path/filepath"
	"testing"

	"stratum/internal/kvstorage"
)

// TestNewRaft_RefusesToStartOnUnreadableHardState pins H4 of
// docs/code-review-2026-10-18.md at the entry point: a hard-state file that
// cannot be decoded must stop the node, not silently restart it in term 0 with an
// empty log (free to vote twice in one term, and forgetful of committed entries).
func TestNewRaft_RefusesToStartOnUnreadableHardState(t *testing.T) {
	base := filepath.Join(t.TempDir(), "raft")
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Neither a valid gob stream nor an absent file.
	if err := os.WriteFile(base, []byte("this is not a gob-encoded hard state"), 0o644); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}

	if _, err := NewRaft(1, NewTransport(nil), make(chan ApplyMsg, 16), kvstorage.NewPersister(base)); err == nil {
		t.Fatal("NewRaft must refuse to start on an unreadable hard state, not boot from zero")
	}
}

// TestNewRaft_RefusesToStartOnUnreadableSnapshot is the same rule for the
// snapshot: an empty state machine whose lastApplied still claims the compacted
// range is applied loses every pre-snapshot KB and version.
func TestNewRaft_RefusesToStartOnUnreadableSnapshot(t *testing.T) {
	base := filepath.Join(t.TempDir(), "raft")
	if err := os.WriteFile(base+".snapshot", []byte("not a gob stream either"), 0o644); err != nil {
		t.Fatalf("write corrupt snapshot: %v", err)
	}

	if _, err := NewRaft(1, NewTransport(nil), make(chan ApplyMsg, 16), kvstorage.NewPersister(base)); err == nil {
		t.Fatal("NewRaft must refuse to start on an unreadable snapshot")
	}
}
