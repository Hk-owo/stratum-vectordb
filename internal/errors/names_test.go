package errors

import (
	"errors"
	"fmt"
	"testing"
)

// Every sentinel must survive the round trip: a proposal forwarded to the
// leader crosses a process boundary, and the caller's errors.Is checks have to
// keep working afterwards (Stratum_设计文档v13.md §7.3).
func TestNameByNameRoundTripEverySentinel(t *testing.T) {
	for _, err := range []error{
		ErrVersionNotFound,
		ErrVersionPending,
		ErrVersionFailed,
		ErrVersionDeleting,
		ErrVersionIsActive,
		ErrKnowledgeBaseNotFound,
		ErrKnowledgeBaseDeleted,
		ErrIndexNotReady,
		ErrInvalidArgument,
		ErrIndexLoadTimeout,
		ErrInvalidParentVersion,
	} {
		name := Name(err)
		if name == "" {
			t.Errorf("Name(%v) = \"\", want a stable wire name", err)
			continue
		}
		got := ByName(name)
		if !errors.Is(got, err) {
			t.Errorf("ByName(%q) = %v, want %v", name, got, err)
		}
	}
}

// A wrapped sentinel is still identified: the forwarding path sees errors that
// travelled through fmt.Errorf, not bare sentinels.
func TestNameUnwrapsWrappedSentinels(t *testing.T) {
	wrapped := fmt.Errorf("plane: backfill kb-1: %w", ErrVersionNotFound)
	if got := Name(wrapped); got != "version_not_found" {
		t.Errorf("Name(wrapped) = %q, want version_not_found", got)
	}
}

// An error that is not a sentinel carries no name — it travels as its message
// alone rather than being forced into a category it does not belong to.
func TestNameOfNonSentinel(t *testing.T) {
	if got := Name(errors.New("disk on fire")); got != "" {
		t.Errorf("Name(non-sentinel) = %q, want \"\"", got)
	}
	if got := Name(nil); got != "" {
		t.Errorf("Name(nil) = %q, want \"\"", got)
	}
}

// An unknown name yields nil instead of inventing an error: a newer peer may
// know sentinels this build does not, and the message alone is more honest.
func TestByNameUnknown(t *testing.T) {
	if got := ByName("some_error_from_the_future"); got != nil {
		t.Errorf("ByName(unknown) = %v, want nil", got)
	}
	if got := ByName(""); got != nil {
		t.Errorf("ByName(\"\") = %v, want nil", got)
	}
}

// The names are wire protocol: duplicates would make Name ambiguous.
func TestSentinelNamesAreUnique(t *testing.T) {
	seen := make(map[string]bool, len(sentinelNames))
	for _, entry := range sentinelNames {
		if seen[entry.name] {
			t.Errorf("duplicate wire name %q", entry.name)
		}
		seen[entry.name] = true
	}
}
