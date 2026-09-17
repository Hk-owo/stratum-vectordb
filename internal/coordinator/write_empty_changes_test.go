package coordinator

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/wal"
)

// TestWriteCoordinator_RejectsEmptyChanges pins docs/cursor-persistence-plan.md §5:
// an empty changes list is REFUSED rather than turned into a version.
//
// A version's document set is inherited from its parent, so "no changes" means
// "unchanged" — which coincides with "the empty document set" only at the root of
// a chain. Accepting it produced a version whose document set could not be stated
// from the command alone, and both layers then had to special-case it (the state
// machine marked it durable at creation, the storage layer advanced cursors
// without fetching). The refusal is what lets those special cases go away.
func TestWriteCoordinator_RejectsEmptyChanges(t *testing.T) {
	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		WAL:      wal.NewMockWAL(),
		RaftNode: newTestRaftNode(),
	})

	versionID, err := coord.Execute(context.Background(), "kb-1", 0, nil, "")
	if err == nil {
		t.Fatalf("Execute with no changes returned version %d, want an error", versionID)
	}
	if !errors.Is(err, stratumerrors.ErrEmptyChanges) {
		t.Fatalf("error = %v, want it to wrap ErrEmptyChanges", err)
	}
	// The wire name is what a caller keys on, so the mapping is part of the
	// contract, not a detail of the message.
	if code := status.Code(stratumerrors.ToGRPCStatus(err)); code != codes.InvalidArgument {
		t.Errorf("wire code = %v, want InvalidArgument", code)
	}
	if name := stratumerrors.Name(err); name != "empty_changes" {
		t.Errorf("wire name = %q, want empty_changes", name)
	}
}

// The refusal must not swallow an idempotent replay: re-sending the SAME changes
// under the same (kbID, ClientRequestID) is the documented recovery for a write
// whose data never landed, and that request is not empty.
func TestWriteCoordinator_EmptyChangesIsNotAnIdempotentReplay(t *testing.T) {
	coord := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		WAL:      wal.NewMockWAL(),
		RaftNode: newTestRaftNode(),
	})

	if _, err := coord.Execute(context.Background(), "kb-1", 0, nil, "req-1"); err == nil {
		t.Fatal("an empty write is refused whether or not it carries an idempotency key")
	}
	// The registration taken on the way in must not be left behind for an
	// idempotency key that never became a version.
	if _, _, ok := coord.TakePendingDispatch("kb-1", "req-1"); ok {
		t.Error("a refused write left its pending dispatch registered")
	}
}
