package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	stratumerrors "stratum/internal/errors"
)

// stubDeletingKBReader answers the sweep's question from a fixed list.
type stubDeletingKBReader struct {
	kbIDs []string
	err   error
}

func (r stubDeletingKBReader) DeletingKBIDs(context.Context) ([]string, error) {
	return r.kbIDs, r.err
}

// recordingDeleteCoordinator records which knowledge bases the sweep resumed and
// can be told to fail specific ones.
type recordingDeleteCoordinator struct {
	mu     sync.Mutex
	called []string
	errs   map[string]error
}

func newRecordingDeleteCoordinator() *recordingDeleteCoordinator {
	return &recordingDeleteCoordinator{errs: make(map[string]error)}
}

func (c *recordingDeleteCoordinator) Execute(_ context.Context, kbID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.called = append(c.called, kbID)
	return c.errs[kbID]
}

func (c *recordingDeleteCoordinator) calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.called))
	copy(out, c.called)
	return out
}

// TestDeletingKBSweeper_ResumesEveryKBMarkedDeleting pins M3 of
// docs/code-review-2026-10-18.md: a knowledge base marked DELETING whose cleanup
// never ran (the process died between the Raft proposal and the goroutine that
// writes the WAL delete mark) has no other repair — the WAL has no record and no
// pass looks for a KB still present in the metadata. The sweep is that repair.
func TestDeletingKBSweeper_ResumesEveryKBMarkedDeleting(t *testing.T) {
	cleanup := newRecordingDeleteCoordinator()
	sw := NewDeletingKBSweeper(stubDeletingKBReader{kbIDs: []string{"kb-1", "kb-2"}}, cleanup, 0, nil)

	resumed, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if resumed != 2 {
		t.Errorf("resumed = %d, want 2", resumed)
	}
	got := cleanup.calls()
	if len(got) != 2 || got[0] != "kb-1" || got[1] != "kb-2" {
		t.Errorf("cleanup calls = %v, want [kb-1 kb-2]", got)
	}
}

// TestDeletingKBSweeper_OneFailureDoesNotStopTheRest keeps the pass's promise: a KB
// that cannot be finished is reported, and the others are still resumed.
func TestDeletingKBSweeper_OneFailureDoesNotStopTheRest(t *testing.T) {
	cleanup := newRecordingDeleteCoordinator()
	cleanup.errs["kb-1"] = fmt.Errorf("storage group unreachable")
	sw := NewDeletingKBSweeper(stubDeletingKBReader{kbIDs: []string{"kb-1", "kb-2"}}, cleanup, 0, nil)

	resumed, err := sw.SweepOnce(context.Background())
	if err == nil {
		t.Error("a failed resume has to be reported, not swallowed")
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 (only kb-2 finished)", resumed)
	}
	if got := cleanup.calls(); len(got) != 2 {
		t.Errorf("cleanup calls = %v, want both attempted", got)
	}
}

// TestDeletingKBSweeper_FinishedDeletionIsNotAFailure: a KB that disappears between
// the read and the resume is a normal end, not an error to log every minute.
func TestDeletingKBSweeper_FinishedDeletionIsNotAFailure(t *testing.T) {
	cleanup := newRecordingDeleteCoordinator()
	cleanup.errs["kb-1"] = fmt.Errorf("read metadata: %w", stratumerrors.ErrKnowledgeBaseNotFound)
	sw := NewDeletingKBSweeper(stubDeletingKBReader{kbIDs: []string{"kb-1"}}, cleanup, 0, nil)

	resumed, err := sw.SweepOnce(context.Background())
	if err != nil {
		t.Errorf("a KB that finished on its own must not be reported as a failure: %v", err)
	}
	if resumed != 0 {
		t.Errorf("resumed = %d, want 0", resumed)
	}
	if _, ok := cleanup.errs["kb-1"]; !ok {
		t.Error("the attempt should still have been made")
	}
}

// TestDeletingKBSweeper_ReaderFailureIsReported: "I could not enumerate" is not
// "there is nothing to do".
func TestDeletingKBSweeper_ReaderFailureIsReported(t *testing.T) {
	sw := NewDeletingKBSweeper(
		stubDeletingKBReader{err: errors.New("state machine unreadable")},
		newRecordingDeleteCoordinator(), 0, nil)

	if _, err := sw.SweepOnce(context.Background()); err == nil {
		t.Error("an unreadable KB list must be reported")
	}
}
