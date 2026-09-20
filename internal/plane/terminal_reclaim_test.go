package plane

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

// The apply-driven reclaim: its own queue, its own cadence. The broadcast queue it
// replaced is gone (see cleanupTask), so these two cases are what remains of this file.
// TestLocalDataPlane_NoteTerminalVersionReclaimsLocally: a terminal verdict reaches
// this node through its OWN apply, so the reclaim must not depend on §10.6's
// best-effort broadcast — and it must not re-broadcast either, because every replica
// is told by its own apply (telling them again would turn N nodes into N×N calls).
func TestLocalDataPlane_NoteTerminalVersionReclaimsLocally(t *testing.T) {
	dropper := &stubDropper{}
	cleaner := &stubCleaner{}
	dp := newCleanupPlane(dropper, cleaner, []string{"peer-1"})

	dp.NoteTerminalVersion("kb-1", 7)

	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/7" {
		t.Fatalf("local drops = %v, want [kb-1/7]", dropper.dropped)
	}
	if len(cleaner.targets) != 0 {
		t.Errorf("broadcast targets = %v, want none: every node learned this from its own apply", cleaner.targets)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 7 {
		t.Errorf("cursor = %d, want 7: a version whose data will never arrive must not hold the cursor back", got)
	}
	if got := dp.PendingTerminalReclaims(); got != 0 {
		t.Errorf("pending reclaims = %d, want 0", got)
	}
}

// TestLocalDataPlane_FailedTerminalReclaimIsRetriedOnItsOwnCadence: the retry is this
// node's own business (nothing else will do it, nothing is waiting on it), so a failed
// local delete waits in its own queue — and the cursor must NOT step past bytes that
// are still on disk (data first, cursor second).
func TestLocalDataPlane_FailedTerminalReclaimIsRetriedOnItsOwnCadence(t *testing.T) {
	dropper := &stubDropper{err: errors.New("disk full")}
	dp := newCleanupPlane(dropper, &stubCleaner{}, []string{"peer-1"})

	dp.NoteTerminalVersion("kb-1", 7)
	if got := dp.PendingTerminalReclaims(); got != 1 {
		t.Fatalf("pending reclaims = %d, want 1 after a failed drop", got)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 0 {
		t.Errorf("cursor = %d, want 0: it must not step past bytes that are still there", got)
	}

	dropper.err = nil
	dp.retryTerminalReclaims(context.Background())
	if got := dp.PendingTerminalReclaims(); got != 0 {
		t.Errorf("pending reclaims after a successful pass = %d, want 0", got)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 7 {
		t.Errorf("cursor = %d, want 7", got)
	}
}

// TestLocalDataPlane_ReclaimTerminalVersionsRebuildsTheListOnStart: a restart must not
// turn a terminal verdict into "nobody's business". The verdict's metadata survives in
// the snapshot, so sweeping the state machine once rebuilds exactly the list the apply
// hook would have built — and only for the DATA side, because an index-side verdict
// says a build failed, not that the data is gone.
func TestLocalDataPlane_ReclaimTerminalVersionsRebuildsTheListOnStart(t *testing.T) {
	meta := &stubMeta{
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
		versions: map[string][]types.VersionMeta{
			"kb-1": {
				{VersionID: 1, KBID: "kb-1", IndexStatus: types.IndexStatusReady, DataStatus: types.DataStatusDurable},
				// The one to reclaim: the data side is terminal, the index side was never touched.
				{VersionID: 2, KBID: "kb-1", IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusFailedPermanent},
				// Index-side verdict only: its data may be perfectly good.
				{VersionID: 3, KBID: "kb-1", IndexStatus: types.IndexStatusFailedPermanent, DataStatus: types.DataStatusDurable},
				{VersionID: 4, KBID: "kb-1", IndexStatus: types.IndexStatusPending, DataStatus: types.DataStatusPending},
			},
		},
	}
	dropper := &stubDropper{}
	cleaner := &stubCleaner{}
	dp := newCleanupPlane(dropper, cleaner, []string{"peer-1"})

	queued, err := dp.ReclaimTerminalVersions(context.Background(), meta)
	if err != nil {
		t.Fatalf("ReclaimTerminalVersions: %v", err)
	}
	if queued != 1 {
		t.Fatalf("queued = %d, want 1 (only the data-side verdict)", queued)
	}
	if len(dropper.dropped) != 1 || dropper.dropped[0] != "kb-1/2" {
		t.Errorf("local drops = %v, want [kb-1/2]", dropper.dropped)
	}
	if len(cleaner.targets) != 0 {
		t.Errorf("broadcast targets = %v, want none: the sweep is local", cleaner.targets)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 2 {
		t.Errorf("cursor = %d, want 2: the swept version must not hold the contiguous cursor back", got)
	}
}
