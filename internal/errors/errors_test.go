package errors

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCStatus(t *testing.T) {
	tests := []struct {
		name    string
		input   error
		want    codes.Code
		wantNil bool
	}{
		{"ErrVersionNotFound", ErrVersionNotFound, codes.NotFound, false},
		{"ErrVersionPending", ErrVersionPending, codes.FailedPrecondition, false},
		{"ErrVersionFailed", ErrVersionFailed, codes.FailedPrecondition, false},
		{"ErrVersionDeleting", ErrVersionDeleting, codes.FailedPrecondition, false},
		{"ErrVersionIsActive", ErrVersionIsActive, codes.FailedPrecondition, false},
		{"ErrKnowledgeBaseNotFound", ErrKnowledgeBaseNotFound, codes.NotFound, false},
		{"ErrKnowledgeBaseDeleted", ErrKnowledgeBaseDeleted, codes.FailedPrecondition, false},
		{"ErrIndexNotReady", ErrIndexNotReady, codes.FailedPrecondition, false},
		{"ErrInvalidArgument", ErrInvalidArgument, codes.InvalidArgument, false},
		{"ErrIndexLoadTimeout", ErrIndexLoadTimeout, codes.DeadlineExceeded, false},
		{"ErrInvalidParentVersion", ErrInvalidParentVersion, codes.InvalidArgument, false},
		{"unknown error", errors.New("unknown"), codes.Internal, false},
		{"nil", nil, codes.OK, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToGRPCStatus(tt.input)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("ToGRPCStatus(nil) = %v, want nil", got)
				}
				return
			}
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("ToGRPCStatus(%v) did not return a gRPC status error: %v", tt.input, got)
			}
			if st.Code() != tt.want {
				t.Fatalf("ToGRPCStatus(%v) code = %v, want %v", tt.input, st.Code(), tt.want)
			}
		})
	}
}

func TestToGRPCStatus_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("wrap: %w", ErrVersionPending)
	got := ToGRPCStatus(wrapped)
	st, ok := status.FromError(got)
	if !ok {
		t.Fatalf("ToGRPCStatus(wrapped) did not return a gRPC status error: %v", got)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("ToGRPCStatus(wrapped) code = %v, want %v", st.Code(), codes.FailedPrecondition)
	}
}

func TestToGRPCStatus_DoublyWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", ErrInvalidParentVersion))
	got := ToGRPCStatus(wrapped)
	st, _ := status.FromError(got)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("doubly-wrapped error code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

// An error that is already a gRPC status keeps its own code. This is the regression
// guard for a real defect: the vector store classifies a search on a still-building
// index as FAILED_PRECONDITION, and the old fallback relabelled it Internal — the
// classification was not missing, it was overwritten on the way out.
func TestToGRPCStatus_PreservesAnIncomingStatusCode(t *testing.T) {
	// Exactly what a grpc-go client hands back from another service.
	incoming := status.Error(codes.FailedPrecondition, "hnsw_index: search: index is still building; not queryable yet")

	got := ToGRPCStatus(incoming)
	st, ok := status.FromError(got)
	if !ok {
		t.Fatalf("ToGRPCStatus(incoming status) = %v, want a gRPC status", got)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v — an incoming classification must not be relabelled", st.Code(), codes.FailedPrecondition)
	}
	if !strings.Contains(st.Message(), "index is still building") {
		t.Errorf("message = %q, want the original text preserved", st.Message())
	}
}

// Wrapping does not break it: the status may sit anywhere in the chain.
func TestToGRPCStatus_PreservesAnIncomingStatusThroughWrapping(t *testing.T) {
	incoming := status.Error(codes.FailedPrecondition, "index is still building")
	wrapped := fmt.Errorf("index: vector search (kb/2): %w", incoming)

	st, _ := status.FromError(ToGRPCStatus(wrapped))
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v through a wrap", st.Code(), codes.FailedPrecondition)
	}
}

// A local sentinel still wins over any incoming status: the named business errors are
// the contract this package defines, and they must keep mapping as before.
func TestToGRPCStatus_LocalSentinelWinsOverIncomingStatus(t *testing.T) {
	mixed := fmt.Errorf("outer: %w",
		status.Error(codes.FailedPrecondition, "from a peer"))

	st, _ := status.FromError(ToGRPCStatus(mixed))
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v", st.Code(), codes.FailedPrecondition)
	}

	// And a truthfully unrecognizable error is still Internal: the fallback must not
	// have become a blanket passthrough.
	st, _ = status.FromError(ToGRPCStatus(errors.New("something entirely unexpected")))
	if st.Code() != codes.Internal {
		t.Errorf("unrecognized error code = %v, want %v", st.Code(), codes.Internal)
	}
}
