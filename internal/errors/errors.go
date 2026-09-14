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
	ErrVersionDeleting       = errors.New("version is being deleted")
	ErrVersionIsActive       = errors.New("version is the active version")
	ErrKnowledgeBaseNotFound = errors.New("knowledge base not found")
	ErrKnowledgeBaseDeleted  = errors.New("knowledge base is deleted")
	ErrIndexNotReady         = errors.New("index not ready")
	ErrInvalidArgument       = errors.New("invalid argument")
	ErrIndexLoadTimeout      = errors.New("index load timeout")
	ErrInvalidParentVersion  = errors.New("invalid parent version")
)

// sentinelNames gives every sentinel a stable wire name. A proposal forwarded
// to the leader crosses a process boundary, so its outcome — error included —
// has to travel as data and be rebuilt on the caller's side; carrying the name
// is what keeps errors.Is working after the forward.
//
// A slice, not a map: Name must be deterministic when an error wraps more than
// one sentinel.
//
// These names are part of the node-to-node protocol. Renaming one means a
// mixed-version cluster stops agreeing on what an error means.
var sentinelNames = []struct {
	name     string
	sentinel error
}{
	{"version_not_found", ErrVersionNotFound},
	{"version_pending", ErrVersionPending},
	{"version_failed", ErrVersionFailed},
	{"version_deleting", ErrVersionDeleting},
	{"version_is_active", ErrVersionIsActive},
	{"knowledge_base_not_found", ErrKnowledgeBaseNotFound},
	{"knowledge_base_deleted", ErrKnowledgeBaseDeleted},
	{"index_not_ready", ErrIndexNotReady},
	{"invalid_argument", ErrInvalidArgument},
	{"index_load_timeout", ErrIndexLoadTimeout},
	{"invalid_parent_version", ErrInvalidParentVersion},
}

// Name returns the stable wire name of the sentinel err wraps, or "" when err
// is not one of them (an unknown error travels as its message alone).
func Name(err error) string {
	if err == nil {
		return ""
	}
	for _, entry := range sentinelNames {
		if errors.Is(err, entry.sentinel) {
			return entry.name
		}
	}
	return ""
}

// ByName rebuilds a sentinel from its wire name, or nil when the name is
// unknown — a newer peer may know errors this build does not, and inventing one
// would be worse than reporting the message alone.
func ByName(name string) error {
	for _, entry := range sentinelNames {
		if entry.name == name {
			return entry.sentinel
		}
	}
	return nil
}

// grpcCodeMap is the single source of truth for business-error -> gRPC
// status code translation. Errors not present here map to codes.Internal.
var grpcCodeMap = map[error]codes.Code{
	ErrVersionNotFound:       codes.NotFound,
	ErrVersionPending:        codes.FailedPrecondition,
	ErrVersionFailed:         codes.FailedPrecondition,
	ErrVersionDeleting:       codes.FailedPrecondition,
	ErrVersionIsActive:       codes.FailedPrecondition,
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
