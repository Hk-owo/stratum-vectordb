package errors

import (
	"errors"
	"fmt"
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
