package plane

import (
	"context"
	"errors"
	"testing"
)

// failingCleaner always refuses, so cleanups stay pending.
type failingCleaner struct{ calls int }

func (c *failingCleaner) DeleteVersionData(context.Context, string, string, int64, string) error {
	c.calls++
	return errors.New("replica unreachable")
}

var _ VersionDataCleaner = (*failingCleaner)(nil)

// A cleanup that did not reach every replica is queued rather than forgotten:
// orphaned data should not hinge on someone reading a warn line (§10.6).
func TestLocalDataPlane_FailedCleanupIsQueuedForRetry(t *testing.T) {
	dp := newCleanupPlane(&stubDropper{}, &failingCleaner{}, []string{"peer-a"})

	if err := dp.DropVersionData(context.Background(), "kb-1", 7); err == nil {
		t.Fatal("want the broadcast failure surfaced")
	}
	if got := dp.PendingCleanups(); got != 1 {
		t.Fatalf("pending cleanups = %d, want 1", got)
	}
}

// A successful cleanup leaves nothing behind.
func TestLocalDataPlane_SuccessfulCleanupIsNotQueued(t *testing.T) {
	dp := newCleanupPlane(&stubDropper{}, &stubCleaner{}, []string{"peer-a"})

	if err := dp.DropVersionData(context.Background(), "kb-1", 7); err != nil {
		t.Fatalf("DropVersionData: %v", err)
	}
	if got := dp.PendingCleanups(); got != 0 {
		t.Fatalf("pending cleanups = %d, want 0", got)
	}
}

// A retry that succeeds drains the entry.
func TestLocalDataPlane_RetryCleanupsDrainsOnSuccess(t *testing.T) {
	cleaner := &failingCleaner{}
	dp := newCleanupPlane(&stubDropper{}, cleaner, []string{"peer-a"})
	ctx := context.Background()

	_ = dp.DropVersionData(ctx, "kb-1", 7)
	if got := dp.PendingCleanups(); got != 1 {
		t.Fatalf("pending cleanups = %d, want 1", got)
	}

	// The replica comes back.
	dp.cleaner = &stubCleaner{}
	dp.retryCleanups(ctx)

	if got := dp.PendingCleanups(); got != 0 {
		t.Fatalf("pending cleanups = %d, want 0 after a successful retry", got)
	}
	if cleaner.calls == 0 {
		t.Error("the first cleanup attempt never reached the cleaner")
	}
}

// A replica that keeps refusing is eventually given up on — an unbounded
// queue would quietly become its own leak.
func TestLocalDataPlane_RetryCleanupsGivesUpAfterTheLimit(t *testing.T) {
	dp := newCleanupPlane(&stubDropper{}, &failingCleaner{}, []string{"peer-a"})
	ctx := context.Background()

	_ = dp.DropVersionData(ctx, "kb-1", 7)
	for i := 0; i < cleanupRetryAttempts; i++ {
		dp.retryCleanups(ctx)
	}
	if got := dp.PendingCleanups(); got != 0 {
		t.Fatalf("pending cleanups = %d, want 0 after %d attempts", got, cleanupRetryAttempts)
	}
}

// Repeated failures for the same version replace the entry instead of stacking
// one per attempt.
func TestLocalDataPlane_CleanupQueueIsKeyedPerVersion(t *testing.T) {
	dp := newCleanupPlane(&stubDropper{}, &failingCleaner{}, []string{"peer-a"})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = dp.DropVersionData(ctx, "kb-1", 7)
	}
	if got := dp.PendingCleanups(); got != 1 {
		t.Fatalf("pending cleanups = %d, want 1 (keyed per version)", got)
	}

	_ = dp.DropVersionData(ctx, "kb-1", 8)
	if got := dp.PendingCleanups(); got != 2 {
		t.Fatalf("pending cleanups = %d, want 2 (one per version)", got)
	}
}
