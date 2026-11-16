package main

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"stratum/internal/raft"
	"stratum/internal/tombstone"
	"stratum/internal/wal"
)

// The logging level is configuration, and a typo in it must not take the node down:
// the whole point of the field is observability, so an unknown level keeps the
// current logger and says so.
func TestLoggerAtLevel(t *testing.T) {
	base, logs := observer.New(zapcore.DebugLevel)
	l := zap.New(base)

	if loggerAtLevel(l, "") != l {
		t.Error("an empty level rebuilt the logger")
	}
	if got := loggerAtLevel(l, "debug"); got == l {
		t.Error("a known level did not rebuild the logger")
	}
	if got := loggerAtLevel(l, "nonsense"); got != l {
		t.Error("an unknown level replaced the logger")
	}
	if n := logs.FilterMessage("unknown logging level; keeping the current one").Len(); n != 1 {
		t.Errorf("warnings = %d, want the typo reported once", n)
	}
}

// The control address table is what a storage node dials, so it must carry exactly
// the peers that advertised an address — a peer without one cannot be reached and
// must not appear as an empty string.
func TestControlAddrsFromPeers(t *testing.T) {
	got := controlAddrsFromPeers([]raft.PeerConfig{
		{ID: 1, ServiceAddr: "s1"},
		{ID: 2},
		{ID: 3, ServiceAddr: "s3"},
	})
	if len(got) != 2 || got[1] != "s1" || got[3] != "s3" {
		t.Errorf("addresses = %v, want only the peers that advertised one", got)
	}
	if _, ok := got[2]; ok {
		t.Error("a peer without an address was given an entry")
	}
	if len(controlAddrsFromPeers(nil)) != 0 {
		t.Error("no peers produced entries")
	}
}

// A node without a tombstone table must hand the coordinator an untyped nil: a nil
// *PebbleStore inside a non-nil interface would be dereferenced on the first call.
func TestTombstoneHelpersKeepNilUntyped(t *testing.T) {
	if rec := tombstoneRecorder(nil); rec != nil {
		t.Errorf("tombstoneRecorder(nil) = %v, want an untyped nil", rec)
	}
	if opt := tombstoneOption(nil); opt != nil {
		t.Error("tombstoneOption(nil) returned an option, want nil so the caller can skip it")
	}

	store, err := tombstone.NewPebbleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if tombstoneRecorder(store) == nil {
		t.Error("a real store was dropped")
	}
	if tombstoneOption(store) == nil {
		t.Error("a real store produced no option")
	}
}

// The deletion log is optional: a node whose control shape cannot answer the
// incremental read reports nil, and the sweep stays on the live-chain path.
func TestDeletionLogSource(t *testing.T) {
	// A control node answers the incremental read; this only asserts the type
	// assertion, so the node is never called.
	var answering raft.RaftNode = (*raft.RaftNodeImpl)(nil)
	if deletionLogSource(answering) == nil {
		t.Error("a node that answers DeletionsSince was not picked up")
	}
	var plain raft.RaftNode = raft.NewMockRaftNode(wal.NewMockWAL())
	if src := deletionLogSource(plain); src != nil {
		t.Error("a node without the deletion log produced a source")
	}
	if src := deletionLogSource(nil); src != nil {
		t.Error("a nil node produced a source")
	}
}
