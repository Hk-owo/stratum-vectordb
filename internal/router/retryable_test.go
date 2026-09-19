package router

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stratumerrors "stratum/internal/errors"
)

// TestIsRetryableErr_DecidesOnTheSentinelNotTheCode is the point of carrying an
// identity on the wire: FailedPrecondition covers both "another replica may
// succeed" and "stop, this is terminal", so a code-only rule gets one of the two
// classes wrong.
func TestIsRetryableErr_DecidesOnTheSentinelNotTheCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			// This replica has not built the index (yet); another may have.
			name: "index not ready",
			err:  stratumerrors.ToGRPCStatus(stratumerrors.ErrIndexNotReady),
			want: true,
		},
		{
			// This replica took the index out of service for the §8.6(d) rolling
			// cleanup; another one is serving it.
			name: "index under maintenance",
			err:  stratumerrors.ToGRPCStatus(stratumerrors.ErrIndexMaintenance),
			want: true,
		},
		{
			// Terminal: the version is going away. Retrying anywhere is wrong.
			name: "version deleting",
			err:  stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionDeleting),
			want: false,
		},
		{
			// Terminal: the knowledge base is gone.
			name: "knowledge base deleted",
			err:  stratumerrors.ToGRPCStatus(stratumerrors.ErrKnowledgeBaseDeleted),
			want: false,
		},
		{
			// Terminal: the index build failed for good.
			name: "version failed",
			err:  stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionFailed),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// These verdicts are the same for a read and a write: index_* means
			// "another replica may have it", and the terminal ones mean "do not
			// retry anywhere". version_pending is the one reason that differs by
			// path, and it has its own case below.
			for _, forWrite := range []bool{false, true} {
				if got := isRetryableErr(tc.err, forWrite); got != tc.want {
					t.Fatalf("isRetryableErr(%v, forWrite=%v) = %v, want %v", tc.err, forWrite, got, tc.want)
				}
			}
		})
	}
}

// TestIsRetryableErr_VersionPendingDependsOnThePath pins the one reason whose
// verdict is opposite on the two paths — the whole point of forWrite.
//
// Read: this replica's storage-layer write for the version is still in progress,
// so another replica may already be serving it; asking someone else is worth the
// hop.
//
// Write: a write goes through Raft, so every replica shares one state machine and
// another candidate refuses for exactly this reason. Retrying buys nothing and
// costs something — it swaps a terminal, self-describing FailedPrecondition
// ("version 95 is PENDING") for a flat "no leader available", which names neither
// the version nor its state and reads like an infrastructure fault. That message
// sent us auditing a healthy cluster while GetClusterStatus answered
// has_leader=true and the timing line showed leader_index=0.
func TestIsRetryableErr_VersionPendingDependsOnThePath(t *testing.T) {
	err := stratumerrors.ToGRPCStatus(stratumerrors.ErrVersionPending)

	if !isRetryableErr(err, false) {
		t.Error("version_pending must stay retryable on a read: another replica may be serving it")
	}
	if isRetryableErr(err, true) {
		t.Error("version_pending must be terminal on a write: Raft state agrees everywhere")
	}
}

// TestIsRetryableErr_UnnamedFailedPreconditionIsTerminal: a FailedPrecondition
// with no identity (a vecstore "still building", a freshness refusal from the
// station path) keeps the pre-existing verdict — terminal. The identity rule
// narrows which failures retry; it must not widen into "any FailedPrecondition
// may be retried".
func TestIsRetryableErr_UnnamedFailedPreconditionIsTerminal(t *testing.T) {
	unnamed := status.Error(codes.FailedPrecondition, "index is still building")

	for _, forWrite := range []bool{false, true} {
		if isRetryableErr(unnamed, forWrite) {
			t.Fatal("an unnamed FailedPrecondition must stay terminal")
		}
	}
}

// TestIsRetryableErr_KeepsTheTransportAndNotLeaderRules: the code-based fallback
// still covers what has no sentinel — an unreachable node, and the forwarded
// kvraft "not leader" (which is matched by message because no sentinel exists for
// it yet).
func TestIsRetryableErr_KeepsTheTransportAndNotLeaderRules(t *testing.T) {
	if !isRetryableErr(status.Error(codes.Unavailable, "connection refused"), true) {
		t.Error("an unreachable node must be retried on another candidate")
	}
	if !isRetryableErr(status.Error(codes.Internal, "kvraft: not leader"), true) {
		t.Error("a forwarded not-leader must be retried on another candidate")
	}
	if isRetryableErr(errors.New("plain error"), true) {
		t.Error("a plain error carries no status and must not be retried")
	}
}

// TestIsRetryableErr_TheReasonSurvivesARegistryHop makes the cross-process claim
// concrete: a named error is converted to a status (as the node returns it),
// converted again (as the station would after receiving it), and still decides
// the same way. This is what a message-text rule cannot promise.
func TestIsRetryableErr_TheReasonSurvivesARegistryHop(t *testing.T) {
	// The node returns this.
	onTheWire := stratumerrors.ToGRPCStatus(stratumerrors.ErrIndexMaintenance)
	// The station hands the received status to the same rule.
	if !isRetryableErr(onTheWire, false) {
		t.Fatal("the maintenance identity must survive the hop")
	}
	// And the reason is readable as data, not as text.
	if reason := stratumerrors.ReasonOf(onTheWire); reason != "index_maintenance" {
		t.Fatalf("ReasonOf = %q, want index_maintenance", reason)
	}
	if strings.Contains(status.Convert(onTheWire).Message(), "maintenance") == false {
		t.Fatal("precondition: the message does mention maintenance (the rule just must not rely on it)")
	}
}
