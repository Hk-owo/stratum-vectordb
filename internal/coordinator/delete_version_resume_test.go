package coordinator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"stratum/internal/raft"
	"stratum/internal/tombstone"
	"stratum/internal/types"
	"stratum/internal/wal"
)

// orderedTombstones records the two calls the resume makes, so a test can pin "the
// tombstone is written BEFORE any byte is touched" — the order is the point, not the
// individual calls.
type orderedTombstones struct {
	*tombstone.MemoryStore
	events *[]string
}

func (o orderedTombstones) Note(ctx context.Context, kbID string, versionID int64, at time.Time) error {
	*o.events = append(*o.events, "note")
	return o.MemoryStore.Note(ctx, kbID, versionID, at)
}

func (o orderedTombstones) MarkDone(ctx context.Context, kbID string, versionID int64, at time.Time) error {
	*o.events = append(*o.events, "mark-done")
	return o.MemoryStore.MarkDone(ctx, kbID, versionID, at)
}

// recordingDropper records the reclaim and can fail, standing in for a candidate replica
// that cannot be reached.
type recordingDropper struct {
	events *[]string
	err    error
}

func (d recordingDropper) DropVersion(context.Context, string, int64) error {
	*d.events = append(*d.events, "drop")
	return d.err
}

func resumeConfig(w wal.WAL, rn raft.RaftNode, events *[]string, dropErr error, blame *tombstone.MemoryStore) DeleteVersionCoordinatorConfig {
	return DeleteVersionCoordinatorConfig{
		MaxRetries:          1,
		RetryBaseIntervalMS: 1,
		WAL:                 w,
		RaftNode:            rn,
		// Nothing is wired below the dropper: the resume must reach for the dropper, not
		// for the stores.
		Dropper:    recordingDropper{events: events, err: dropErr},
		Tombstones: orderedTombstones{MemoryStore: blame, events: events},
	}
}

// The record Execute cannot pay: the crash landed after ProposeRemoveVersionMeta, so the
// version is no longer in the metadata and a "list the Deleting versions" scan finds
// nothing. ResumeVersionDelete is named by the record itself, so it finishes the job —
// including writing the WAL completion, which is what stops Recover from handing the
// record back on every restart.
func TestDeleteVersionCoordinator_ResumeCompletesARecordWhoseVersionIsGone(t *testing.T) {
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w) // no KB, no versions: the metadata is already gone
	var events []string
	blame := tombstone.NewMemoryStore()

	coord := NewDeleteVersionCoordinatorImpl(resumeConfig(w, rn, &events, nil, blame))
	if err := coord.ResumeVersionDelete(ctx, "kb-1", 7); err != nil {
		t.Fatalf("ResumeVersionDelete: %v", err)
	}

	if got := strings.Join(events, ","); got != "note,drop,mark-done" {
		t.Fatalf("events = %q, want note,drop,mark-done — the tombstone has to be written "+
			"before any byte is touched, and marked done once the reclaim succeeded", got)
	}
	gone, err := blame.Contains(ctx, "kb-1", 7)
	if err != nil || !gone {
		t.Fatalf("Contains(kb-1, v7) = (%v, %v), want the tombstone recorded", gone, err)
	}
	if pending, err := blame.Pending(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("Pending = (%+v, %v), want nothing owed after a successful reclaim", pending, err)
	}

	records, err := w.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for _, rec := range records {
		if rec.Type == types.PendingRecordTypeVersionDelete {
			t.Fatalf("Recover still reports %+v: the completion record is what ends the replay", rec)
		}
	}
}

// A reclaim that reaches nobody must not keep the record owed — same policy as the live
// flow (deleteOne), for the same reason: removing the metadata is what tells every
// storage node the version is gone. What is left behind is the tombstone entry, in
// PENDING, which is the state that says "still owed" to the 30 s retry.
func TestDeleteVersionCoordinator_ResumeCompletesWhenTheReclaimFails(t *testing.T) {
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	var events []string
	blame := tombstone.NewMemoryStore()
	reclaimErr := errors.New("replica unreachable")

	coord := NewDeleteVersionCoordinatorImpl(resumeConfig(w, rn, &events, reclaimErr, blame))
	err := coord.ResumeVersionDelete(ctx, "kb-1", 9)
	if err == nil || !strings.Contains(err.Error(), "not reclaimed everywhere") {
		t.Fatalf("ResumeVersionDelete = %v, want the unreclaimed-bytes debt reported", err)
	}
	if !errors.Is(err, reclaimErr) {
		t.Fatalf("ResumeVersionDelete = %v, want it to wrap the reclaim failure", err)
	}

	// Written first, and left owed: the entry is what the retry loop drains.
	gone, cerr := blame.Contains(ctx, "kb-1", 9)
	if cerr != nil || !gone {
		t.Fatalf("Contains(kb-1, v9) = (%v, %v), want the tombstone recorded", gone, cerr)
	}
	pending, perr := blame.Pending(ctx)
	if perr != nil || len(pending) != 1 || pending[0].VersionID != 9 {
		t.Fatalf("Pending = (%+v, %v), want one entry still owed for v9", pending, perr)
	}
	// The reclaim is retried (MaxRetries), so "drop" may appear more than once; what
	// matters is that it was attempted and never marked done.
	if got := strings.Join(events, ","); !strings.HasPrefix(got, "note,drop") || strings.Contains(got, "mark-done") {
		t.Fatalf("events = %q, want note first, then the reclaim, and no mark-done", got)
	}

	records, rerr := w.Recover(ctx)
	if rerr != nil {
		t.Fatalf("Recover: %v", rerr)
	}
	for _, rec := range records {
		if rec.Type == types.PendingRecordTypeVersionDelete {
			t.Fatalf("Recover still reports %+v: the bytes are owed by the tombstone, not by the WAL", rec)
		}
	}
}

// A tombstone that cannot be written is reported but does not stop the deletion: refusing
// to reclaim would leave bytes for a version nothing names afterwards.
func TestDeleteVersionCoordinator_ResumeReportsAFailedTombstoneButStillReclaims(t *testing.T) {
	ctx := context.Background()
	w := wal.NewMockWAL()
	rn := raft.NewMockRaftNode(w)
	var events []string
	blame := tombstone.NewMemoryStore()
	noteErr := errors.New("tombstone store is read-only")

	cfg := resumeConfig(w, rn, &events, nil, blame)
	cfg.Tombstones = failingNoteTombstones{VersionTombstoneRecorder: blame, err: noteErr}
	coord := NewDeleteVersionCoordinatorImpl(cfg)

	err := coord.ResumeVersionDelete(ctx, "kb-1", 4)
	if err == nil || !errors.Is(err, noteErr) {
		t.Fatalf("ResumeVersionDelete = %v, want the tombstone failure reported", err)
	}
	if !strings.HasPrefix(strings.Join(events, ","), "drop") {
		t.Fatalf("events = %q, want the reclaim to run anyway", events)
	}
	records, rerr := w.Recover(ctx)
	if rerr != nil {
		t.Fatalf("Recover: %v", rerr)
	}
	for _, rec := range records {
		if rec.Type == types.PendingRecordTypeVersionDelete {
			t.Fatalf("Recover still reports %+v: the record must still complete", rec)
		}
	}
}

// failingNoteTombstones refuses the note and delegates everything else, so the
// mark-done step stays observable after a failed note.
type failingNoteTombstones struct {
	VersionTombstoneRecorder
	err error
}

func (f failingNoteTombstones) Note(context.Context, string, int64, time.Time) error { return f.err }

// An id that cannot name a version is refused before anything is written.
func TestDeleteVersionCoordinator_ResumeRefusesAVersionlessRecord(t *testing.T) {
	var events []string
	coord := NewDeleteVersionCoordinatorImpl(resumeConfig(wal.NewMockWAL(), raft.NewMockRaftNode(wal.NewMockWAL()), &events, nil, tombstone.NewMemoryStore()))

	if err := coord.ResumeVersionDelete(context.Background(), "kb-1", 0); err == nil {
		t.Fatal("ResumeVersionDelete(v0) = nil, want a refusal")
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want nothing written for an invalid id", events)
	}
}
