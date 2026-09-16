package plane

import "testing"

// The contiguous cursor's whole meaning is "this node's history is unbroken to
// here" (§7.5), and the station's freshness check (§9.3(2)) reads it that way.
// A plain maximum broke it: one lost push followed by a successful later push
// moved the cursor over a version the node does not hold, so the node claimed
// history it could not serve — and could then be picked as a backfill source
// (§7.6) for a version it would fail to provide.
//
// Announcing a version is how the control layer's own knowledge reaches the
// cursor: the version exists (the apply announced it), this node does not have
// it, therefore the cursor stops below it.
//
// These tests drive the cursor directly rather than through the write/pull
// paths; those paths are covered by the tests that own them, and what is new
// here is only how the cursor reacts to the two facts.

// TestLocalDataPlane_CursorStopsAtAnAnnouncedGap is the fix's core case: v4 is
// announced and never arrives, v5 does. The cursor must stay below v4 rather
// than step over the hole to 5.
func TestLocalDataPlane_CursorStopsAtAnAnnouncedGap(t *testing.T) {
	const kbID = "kb-gap"
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	dp.AnnounceVersion(kbID, 4)
	dp.advanceLocalVersion(kbID, 5)

	if got := dp.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor = %d, want 0 — v4 exists and this node does not hold it", got)
	}
}

// TestLocalDataPlane_CursorAdvancesWhenTheGapIsFilled shows the cursor is
// blocked, not broken: once the missing version's data lands, the run is
// unbroken again and the cursor catches up over everything behind it in one step.
func TestLocalDataPlane_CursorAdvancesWhenTheGapIsFilled(t *testing.T) {
	const kbID = "kb-fill"
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	dp.AnnounceVersion(kbID, 4)
	dp.advanceLocalVersion(kbID, 5)
	if got := dp.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor = %d, want 0 before the gap is filled", got)
	}

	dp.advanceLocalVersion(kbID, 4)
	if got := dp.LocalVersionOf(kbID); got != 5 {
		t.Fatalf("cursor = %d, want 5 — the run is unbroken again", got)
	}
}

// TestLocalDataPlane_CursorStepsOverVersionsThatWereNeverAnnounced pins the
// direction the fix must NOT change: with nothing announced there is no known
// gap, so the cursor still follows the versions this node has accounted for.
// Every storage path announces, making this the degenerate case — but it has to
// stay the old behaviour rather than freezing every cursor at 0.
func TestLocalDataPlane_CursorStepsOverVersionsThatWereNeverAnnounced(t *testing.T) {
	const kbID = "kb-silent"
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	dp.advanceLocalVersion(kbID, 9)
	if got := dp.LocalVersionOf(kbID); got != 9 {
		t.Fatalf("cursor = %d, want 9 — with no announcement there is no known gap", got)
	}
}

// TestLocalDataPlane_CursorWaitsForOnlyTheMissingVersion checks that a gap
// blocks exactly what it should: several later versions have already arrived,
// and the cursor jumps straight to the end of the run once the one missing
// version lands.
func TestLocalDataPlane_CursorWaitsForOnlyTheMissingVersion(t *testing.T) {
	const kbID = "kb-multi"
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	for _, v := range []int64{3, 4, 5} {
		dp.AnnounceVersion(kbID, v)
	}
	for _, v := range []int64{3, 5} {
		dp.advanceLocalVersion(kbID, v)
	}
	if got := dp.LocalVersionOf(kbID); got != 3 {
		t.Fatalf("cursor = %d, want 3 — v4 is the gap", got)
	}

	dp.advanceLocalVersion(kbID, 4)
	if got := dp.LocalVersionOf(kbID); got != 5 {
		t.Fatalf("cursor = %d, want 5 once v4 lands", got)
	}
}

// TestLocalDataPlane_AnnounceAfterTheCursorPassedDoesNotMoveItBack keeps the
// one-way property: an announcement that arrives late (the data landed first)
// must not pull the cursor back. The cursor is what peers and the station read
// as "unbroken to here" (§7.6, §9.3(2)); making it rewind is a bigger change
// than this one, and under-claiming is the safe direction anyway.
func TestLocalDataPlane_AnnounceAfterTheCursorPassedDoesNotMoveItBack(t *testing.T) {
	const kbID = "kb-late"
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	dp.advanceLocalVersion(kbID, 5)
	dp.AnnounceVersion(kbID, 4)

	if got := dp.LocalVersionOf(kbID); got != 5 {
		t.Fatalf("cursor = %d, want 5 — the cursor never moves back", got)
	}
}

// TestLocalDataPlane_FullStateTransferAccountsForTheSkippedRun pins §6.4's
// contract: the full-state transfer replaces a run of versions it cannot fetch
// one by one (one of them is deleted), so the cursor moves to the snapshot — but
// it does so as a run of ACCOUNTED versions, not as a bare jump, so that the
// invariant "nothing above the cursor is outstanding" survives.
func TestLocalDataPlane_FullStateTransferAccountsForTheSkippedRun(t *testing.T) {
	const kbID = "kb-full"
	dp := NewLocalDataPlane(LocalDataPlaneConfig{})

	// The versions in the skipped run were announced (they exist in the
	// metadata), and the snapshot itself replaces all of them.
	for _, v := range []int64{4, 5, 6, 7} {
		dp.AnnounceVersion(kbID, v)
	}
	dp.markVersionsHandled(kbID, 4, 7)

	if got := dp.LocalVersionOf(kbID); got != 7 {
		t.Fatalf("cursor = %d, want 7 — the snapshot accounts for the whole run", got)
	}
}
