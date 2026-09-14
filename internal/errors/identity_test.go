package errors

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestToGRPCStatus_AttachesTheSentinelIdentity: a named error crossing a process
// boundary has to carry WHICH error it is, not just a status code. The code alone
// cannot identify it — FailedPrecondition covers both "another replica may
// succeed" and "stop, this is terminal" — so the identity travels as a standard
// google.rpc.ErrorInfo detail.
func TestToGRPCStatus_AttachesTheSentinelIdentity(t *testing.T) {
	st := ToGRPCStatus(ErrIndexMaintenance)

	got, ok := status.FromError(st)
	if !ok {
		t.Fatalf("ToGRPCStatus produced a non-status error: %v", st)
	}
	if got.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v", got.Code(), codes.FailedPrecondition)
	}
	if reason := ReasonOf(st); reason != "index_maintenance" {
		t.Fatalf("ReasonOf = %q, want %q", reason, "index_maintenance")
	}
}

// TestToGRPCStatus_KeepsTheReasonThroughWrapping: business errors are wrapped
// with %w on the way up, so the attachment has to survive that.
func TestToGRPCStatus_KeepsTheReasonThroughWrapping(t *testing.T) {
	wrapped := fmt.Errorf("query: kb-1: %w", ErrIndexNotReady)

	if reason := ReasonOf(ToGRPCStatus(wrapped)); reason != "index_not_ready" {
		t.Fatalf("ReasonOf = %q, want %q", reason, "index_not_ready")
	}
}

// TestToGRPCStatus_LeavesAnUnnamedErrorWithoutAReason: the reason is reserved for
// errors the codebase names. An unclassified error keeps mapping to Internal and
// carries no identity — a caller must not read "no reason" as "safe to retry".
func TestToGRPCStatus_LeavesAnUnnamedErrorWithoutAReason(t *testing.T) {
	st := ToGRPCStatus(errors.New("something broke"))

	got, ok := status.FromError(st)
	if !ok || got.Code() != codes.Internal {
		t.Fatalf("unclassified error mapped to %v, want %v", st, codes.Internal)
	}
	if reason := ReasonOf(st); reason != "" {
		t.Fatalf("ReasonOf = %q, want empty for an unnamed error", reason)
	}
}

// TestToGRPCStatus_PreservesAnIncomingStatus: an error that is ALREADY a gRPC
// status keeps its code AND its details. Rebuilding it from (code, message) —
// what this function used to do — dropped whatever the producer attached, so a
// reason from a peer would have been lost on the second hop.
func TestToGRPCStatus_PreservesAnIncomingStatus(t *testing.T) {
	incoming := ToGRPCStatus(ErrIndexMaintenance) // as a peer would have sent it

	again := ToGRPCStatus(incoming)
	if reason := ReasonOf(again); reason != "index_maintenance" {
		t.Fatalf("ReasonOf after a second conversion = %q, want it preserved", reason)
	}
	if st, _ := status.FromError(again); st.Code() != codes.FailedPrecondition {
		t.Fatalf("code after a second conversion = %v, want it preserved", st.Code())
	}
}

// TestReasonOf_OnPlainErrors: a non-status error has no identity to read, and
// asking must not panic or invent one.
func TestReasonOf_OnPlainErrors(t *testing.T) {
	if got := ReasonOf(nil); got != "" {
		t.Errorf("ReasonOf(nil) = %q, want empty", got)
	}
	if got := ReasonOf(errors.New("plain")); got != "" {
		t.Errorf("ReasonOf(plain error) = %q, want empty", got)
	}
	if got := ReasonOf(ByName("index_maintenance")); got != "" {
		t.Errorf("ReasonOf(a raw sentinel) = %q, want empty (it is not a status yet)", got)
	}
}

// TestByName_RoundTripsEveryNamedSentinel: the wire names are the protocol; a
// sentinel that cannot be rebuilt from its name cannot survive a forward.
func TestByName_RoundTripsEveryNamedSentinel(t *testing.T) {
	for _, entry := range sentinelNames {
		got := ByName(entry.name)
		if got == nil {
			t.Errorf("ByName(%q) = nil, want the sentinel back", entry.name)
			continue
		}
		if !errors.Is(got, entry.sentinel) {
			t.Errorf("ByName(%q) did not rebuild the same sentinel", entry.name)
		}
		if name := Name(got); name != entry.name {
			t.Errorf("Name(ByName(%q)) = %q, want the same name", entry.name, name)
		}
	}
	if got := ByName("no_such_reason"); got != nil {
		t.Errorf("ByName of an unknown name = %v, want nil", got)
	}
}

// TestIndexMaintenanceIsItsOwnSentinel pins the deliberate split: "we took it out
// of service" and "it was never built" share a status code but are different root
// causes, and an operator reading a log needs to tell them apart.
func TestIndexMaintenanceIsItsOwnSentinel(t *testing.T) {
	if errors.Is(ErrIndexMaintenance, ErrIndexNotReady) {
		t.Fatal("ErrIndexMaintenance must not be the same sentinel as ErrIndexNotReady")
	}
	if Name(ErrIndexMaintenance) == Name(ErrIndexNotReady) {
		t.Fatal("the two must have different wire names")
	}
	if codeOf(ErrIndexMaintenance) != codeOf(ErrIndexNotReady) {
		t.Fatal("they are expected to share a status code; the identity is what differs")
	}
}

// codeOf reads the mapped status code for a sentinel.
func codeOf(err error) codes.Code { return grpcCodeOf(err) }
