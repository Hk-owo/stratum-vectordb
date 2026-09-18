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

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Named business errors. New error types are added here first; a single
// line is then added to grpcCodeMap to route them to the correct gRPC
// status code, and one to sentinelNames so the identity survives a process
// boundary.
var (
	ErrVersionNotFound       = errors.New("version not found")
	ErrVersionPending        = errors.New("version is pending")   // storage-layer write for the version still in progress; not queryable
	ErrVersionFailed         = errors.New("version index failed") // index build failed; not queryable
	ErrVersionDeleting       = errors.New("version is being deleted")
	ErrVersionIsActive       = errors.New("version is the active version")
	ErrKnowledgeBaseNotFound = errors.New("knowledge base not found")
	ErrKnowledgeBaseDeleted  = errors.New("knowledge base is deleted")
	ErrIndexNotReady         = errors.New("index not ready")
	// ErrIndexMaintenance is this node deliberately taking a version's index out
	// of service: §8.6(d)'s rolling cleanup reopens a sealed artifact, which
	// leaves it in BUILDING until it is resealed. It is deliberately SEPARATE
	// from ErrIndexNotReady — "we took it down on purpose" and "it has not been
	// built yet" share a gRPC code (neither is fixable by retrying THIS node)
	// but are different root causes, and whoever reads a log wants to know which.
	ErrIndexMaintenance = errors.New("index under maintenance")
	ErrInvalidArgument  = errors.New("invalid argument")
	// ErrEmptyChanges rejects a CreateVersion whose changes list is empty. A
	// version's document set is INHERITED from its parent, so an empty changes
	// list does not mean "the empty set" — it means "the same set as my parent",
	// and only at the ROOT of a chain is there no parent to inherit from. The
	// control layer therefore refuses the write rather than creating a version
	// whose document set it cannot state (docs/cursor-persistence-plan.md §5).
	ErrEmptyChanges         = errors.New("empty changes")
	ErrIndexLoadTimeout     = errors.New("index load timeout")
	ErrInvalidParentVersion = errors.New("invalid parent version")
	// ErrKBStorageDegraded refuses a WRITE to a knowledge base whose live
	// replicas are below quorum (docs/storage-degradation-signal-plan.md §4.1).
	//
	// It is deliberately Unavailable — retryable — and not a terminal
	// FailedPrecondition: the judgement behind it is soft state (a periodic
	// aggregate over node reports), so it can be stale in the optimistic
	// direction too, and the caller has to be able to come back and get the
	// authoritative verdict. A refusal that a caller cannot act on is worse than
	// the retry it saves.
	//
	// Reads are NOT refused by it. Below quorum a replica that still has the data
	// can still serve, and this design keeps that trade-off (§2, non-goals).
	ErrKBStorageDegraded = errors.New("knowledge base storage degraded")
	// ErrStorageUnavailable is the cluster-wide counterpart: the storage layer as
	// a whole is below quorum, so no knowledge base can be written. Retryable for
	// the same reason as ErrKBStorageDegraded.
	ErrStorageUnavailable = errors.New("storage unavailable")
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
	{"index_maintenance", ErrIndexMaintenance},
	{"invalid_argument", ErrInvalidArgument},
	{"empty_changes", ErrEmptyChanges},
	{"index_load_timeout", ErrIndexLoadTimeout},
	{"invalid_parent_version", ErrInvalidParentVersion},
	{"kb_storage_degraded", ErrKBStorageDegraded},
	{"storage_unavailable", ErrStorageUnavailable},
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
	ErrIndexMaintenance:      codes.FailedPrecondition,
	ErrInvalidArgument:       codes.InvalidArgument,
	ErrEmptyChanges:          codes.InvalidArgument,
	ErrIndexLoadTimeout:      codes.DeadlineExceeded,
	ErrInvalidParentVersion:  codes.InvalidArgument,
	// Unavailable, not FailedPrecondition: below quorum the storage layer is
	// temporarily unable to accept this write — the storage layer is coming back
	// or a failover is in flight, and the caller should retry rather than treat
	// the version as rejected. See the sentinel's comment.
	ErrKBStorageDegraded:  codes.Unavailable,
	ErrStorageUnavailable: codes.Unavailable,
}

// ToGRPCStatus converts a business error into a gRPC status error. It walks
// the error chain with errors.Is so wrapped errors (fmt.Errorf("...: %w",
// err)) are correctly matched against the named sentinels. nil maps to nil.
//
// A NAMED sentinel carries its identity onto the wire, as a standard
// google.rpc.ErrorInfo detail (see ReasonOf). The status code alone cannot
// identify an error: FailedPrecondition covers both "retry elsewhere, another
// replica may be ready" (index_not_ready, index_maintenance) and "stop, this is
// terminal" (version_deleting, knowledge_base_deleted). Attaching the reason at
// this single conversion point means every named error travels with its
// identity — before this, only errors crossing the proposal-forwarding path
// carried one, so a caller across the network had nothing to match on but the
// message text.
//
// An error that is ALREADY a gRPC status (one that came back from another service, or
// from an internal boundary that returned one) is returned AS IS — code, message and
// details. That preservation was a real defect twice over: the vector store classifies
// a search on a still-building index as FAILED_PRECONDITION (its C++ side maps absl's
// FailedPrecondition straight to the gRPC code), and an earlier version of this function
// relabelled it Internal; rebuilding the status from (code, message) still threw away
// any details the producer attached.
//
// Only genuinely unrecognized errors map to codes.Internal — they should not normally
// reach this function uncategorized; treat repeated Internal mappings for the same
// error as a signal to add it to grpcCodeMap.
//
// Every gRPC method implementation calls ToGRPCStatus exactly once, at its
// outermost layer, on whatever error it is about to return.
func ToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	if reason := Name(err); reason != "" {
		return statusWithReason(grpcCodeOf(err), err.Error(), reason)
	}
	// Checked with errors.As against the GRPCStatus interface rather than
	// status.FromError: the latter reports success for ANY error (it yields
	// codes.Unknown for those carrying no status at all), which would silently turn
	// every unclassifiable error into Unknown.
	var withStatus interface{ GRPCStatus() *status.Status }
	if errors.As(err, &withStatus) {
		return withStatus.GRPCStatus().Err()
	}
	return status.Error(codes.Internal, err.Error())
}

// grpcCodeOf maps a business error to its status code, defaulting to Internal
// for anything not in grpcCodeMap.
func grpcCodeOf(err error) codes.Code {
	for sentinel, code := range grpcCodeMap {
		if errors.Is(err, sentinel) {
			return code
		}
	}
	return codes.Internal
}

// statusWithReason builds a status carrying a sentinel's wire name as a
// google.rpc.ErrorInfo detail. A status that refuses the detail is still a valid
// status, so failing to attach is not an error: the reason is an improvement,
// not a requirement.
func statusWithReason(code codes.Code, msg, reason string) error {
	st := status.New(code, msg)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason})
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}

// ReasonOf returns the sentinel name a gRPC status carries as ErrorInfo, or ""
// when it carries none.
//
// This is how a caller across a process boundary tells two same-code failures
// apart. Matching on the message text instead is what this replaces: the codebase
// used to read codes.Internal plus strings.Contains(msg, "not leader"), which
// breaks the moment anyone rewords an error and cannot promise it will not catch
// something else.
func ReasonOf(err error) string {
	if err == nil {
		return ""
	}
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// Wrap is a thin convenience wrapper around fmt.Errorf("...: %w", err) for
// call sites that want to attach context to a business error while
// preserving errors.Is matchability. It exists purely for readability at
// call sites; using fmt.Errorf directly is equally correct.
func Wrap(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}
