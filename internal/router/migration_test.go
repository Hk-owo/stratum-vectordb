package router

import (
	serrors "stratum/internal/errors"
	"testing"
)

func TestMigrationPauseDoesNotTripNodeBreaker(t *testing.T) {
	for _, sentinel := range []error{serrors.ErrMigrationInProgress, serrors.ErrMigrationBusy, serrors.ErrKBWritePaused} {
		err := serrors.ToGRPCStatus(sentinel)
		if nodeLevelFailure(err) {
			t.Fatal("KB-local preparation was counted as node outage:", err)
		}
		if !isRetryableErr(err, true) {
			t.Fatal("preparation refusal must remain retryable:", err)
		}
	}
	if !nodeLevelFailure(serrors.ToGRPCStatus(serrors.ErrStorageUnavailable)) {
		t.Fatal("real node outage was hidden")
	}
}
