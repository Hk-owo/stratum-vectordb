// Package errors defines Stratum's named business error sentinels and the
// mapping from those errors to gRPC status codes.
//
// Internal modules (DocStore, ChunkStore, Coordinator, etc.) only ever
// return these named errors (or errors wrapping them via fmt.Errorf with
// %w). They never construct gRPC status errors directly — that conversion
// happens exactly once, at the outermost layer of each gRPC method, via
// ToGRPCStatus.
package errors

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Named business errors. New error types are added here first; a single
// line is then added to grpcCodeMap to route them to the correct gRPC
// status code.
var (
	ErrVersionNotFound       = errors.New("version not found")
	ErrVersionPending        = errors.New("version is pending")   // storage-layer write for the version still in progress; not queryable
	ErrVersionFailed         = errors.New("version index failed") // index build failed; not queryable
	ErrKnowledgeBaseNotFound = errors.New("knowledge base not found")
	ErrKnowledgeBaseDeleted  = errors.New("knowledge base is deleted")
	ErrIndexNotReady         = errors.New("index not ready")
	ErrInvalidArgument       = errors.New("invalid argument")
	ErrIndexLoadTimeout      = errors.New("index load timeout")
	ErrInvalidParentVersion  = errors.New("invalid parent version")
)

// grpcCodeMap is the single source of truth for business-error -> gRPC
// status code translation. Errors not present here map to codes.Internal.
var grpcCodeMap = map[error]codes.Code{
	ErrVersionNotFound:       codes.NotFound,
	ErrVersionPending:        codes.FailedPrecondition,
	ErrVersionFailed:         codes.FailedPrecondition,
	ErrKnowledgeBaseNotFound: codes.NotFound,
	ErrKnowledgeBaseDeleted:  codes.FailedPrecondition,
	ErrIndexNotReady:         codes.FailedPrecondition,
	ErrInvalidArgument:       codes.InvalidArgument,
	ErrIndexLoadTimeout:      codes.DeadlineExceeded,
	ErrInvalidParentVersion:  codes.InvalidArgument,
}

// ToGRPCStatus converts a business error into a gRPC status error. It walks
// the error chain with errors.Is so wrapped errors (fmt.Errorf("...: %w",
// err)) are correctly matched against the named sentinels. nil maps to nil.
// Unrecognized errors map to codes.Internal — they should not normally
// reach this function uncategorized; treat repeated Internal mappings for
// the same error as a signal to add it to grpcCodeMap.
//
// Every gRPC method implementation calls ToGRPCStatus exactly once, at its
// outermost layer, on whatever error it is about to return.
func ToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	for sentinel, code := range grpcCodeMap {
		if errors.Is(err, sentinel) {
			return status.Error(code, err.Error())
		}
	}
	return status.Error(codes.Internal, err.Error())
}

// Wrap is a thin convenience wrapper around fmt.Errorf("...: %w", err) for
// call sites that want to attach context to a business error while
// preserving errors.Is matchability. It exists purely for readability at
// call sites; using fmt.Errorf directly is equally correct.
func Wrap(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}
