package plane

import (
	"context"
	"errors"
	"testing"
)

// recordingPuller records the versions pulled (and can fail specific ones), so
// tests can pin the order the cursor forces.
type recordingPuller struct {
	pulled []int64
	fail   map[int64]bool
}

// PullVersionData satisfies VersionPuller.
func (p *recordingPuller) PullVersionData(ctx context.Context, sourceAddr, kbID string, versionID int64) error {
	return p.PullVersion(ctx, sourceAddr, kbID, versionID)
}

func (p *recordingPuller) PullVersion(_ context.Context, _ string, _ string, versionID int64) error {
	if p.fail[versionID] {
		return errors.New("pull failed")
	}
	p.pulled = append(p.pulled, versionID)
	return nil
}

var _ VersionPuller = (*recordingPuller)(nil)

func newCursorPlane(puller VersionPuller) *LocalDataPlane {
	tr := &tracer{}
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		WAL:          &stubWAL{t: tr},
		Executor:     &stubExecutor{t: tr, docIDs: []string{"doc-1"}},
		Puller:       puller,
		Verify:       func(context.Context, string, int64) bool { return true },
		Resolve:      func(context.Context, string, int64) (string, bool, error) { return "peer-a", true, nil },
	})
}

// The cursor records contiguous progress and never moves backwards: a
// re-applied older version (an idempotent retry, or backfill replay) must not
// pull it back.
func TestLocalDataPlane_LocalVersionCursorOnlyMovesForward(t *testing.T) {
	dp := newCursorPlane(&recordingPuller{})

	if got := dp.localVersionOf("kb-1"); got != 0 {
		t.Fatalf("a KB nothing has been applied for must report cursor 0, got %d", got)
	}
	dp.advanceLocalVersion("kb-1", 5)
	if got := dp.localVersionOf("kb-1"); got != 5 {
		t.Fatalf("cursor = %d, want 5", got)
	}
	dp.advanceLocalVersion("kb-1", 3)
	if got := dp.localVersionOf("kb-1"); got != 5 {
		t.Fatalf("cursor = %d, want 5 (an older version must not move it back)", got)
	}
}

// A node that knows it is behind fills the gap in order before applying the
// newer version (Stratum_设计文档v13.md §7.5).
func TestLocalDataPlane_BackfillClosesKnownGap(t *testing.T) {
	puller := &recordingPuller{}
	dp := newCursorPlane(puller)
	dp.advanceLocalVersion("kb-1", 3)

	if err := dp.EnsureIndex(context.Background(), "kb-1", 6); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	want := []int64{4, 5, 6} // the gap, then the version itself
	if len(puller.pulled) != len(want) {
		t.Fatalf("pulled = %v, want %v", puller.pulled, want)
	}
	for i, v := range want {
		if puller.pulled[i] != v {
			t.Fatalf("pulled = %v, want %v (in order)", puller.pulled, want)
		}
	}
	if got := dp.localVersionOf("kb-1"); got != 6 {
		t.Errorf("cursor = %d, want 6 after applying the version", got)
	}
}

// With no cursor yet there is no *known* gap: a node learning about a version
// pulls that version whole rather than speculating about the ones before it.
func TestLocalDataPlane_NoBackfillWithoutCursor(t *testing.T) {
	puller := &recordingPuller{}
	dp := newCursorPlane(puller)

	if err := dp.EnsureIndex(context.Background(), "kb-1", 6); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if len(puller.pulled) != 1 || puller.pulled[0] != 6 {
		t.Errorf("pulled = %v, want just the version it learned about", puller.pulled)
	}
}

// A gap that cannot be closed must abort before the newer version is applied:
// applying it onto incomplete data is exactly what the invariant prevents.
func TestLocalDataPlane_BackfillFailureAbortsApply(t *testing.T) {
	puller := &recordingPuller{fail: map[int64]bool{4: true}}
	dp := newCursorPlane(puller)
	dp.advanceLocalVersion("kb-1", 3)

	if err := dp.EnsureIndex(context.Background(), "kb-1", 6); err == nil {
		t.Fatal("an unclosable gap must abort the apply")
	}
	if len(puller.pulled) != 0 {
		t.Errorf("pulled = %v, want nothing after the failing version", puller.pulled)
	}
}
