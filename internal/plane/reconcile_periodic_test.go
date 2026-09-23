package plane

import "testing"

// The periodic pass exists because the startup one only sees what exists at boot; what
// it needs to know is whether the local set of held versions changed since. Every
// landing path runs through advanceLocalVersion, so that single call is where the answer
// has to be recorded.
func TestLocalDataPlane_AdvanceLocalVersionMarksReconcileDirty(t *testing.T) {
	dp := newCursorPlane(&recordingPuller{})

	if dp.takeDeletedReconcileDirty() {
		t.Fatal("nothing has landed, so there is nothing new to reconcile")
	}

	dp.advanceLocalVersion("kb-1", 5)
	if !dp.takeDeletedReconcileDirty() {
		t.Error("a landed version must mark the reconciliation dirty")
	}
	if dp.takeDeletedReconcileDirty() {
		t.Error("taking the flag must clear it — one change must not be acted on twice")
	}

	// The late-push shape (§B's source ③): a version landing UNDER a cursor that has
	// already moved past it takes advanceLocalVersion's early return, and that is
	// precisely the leftover this pass exists to find — so the early return has to mark
	// dirty too. Without that, the only case the periodic pass covers is the one the
	// startup pass already covered.
	dp.advanceLocalVersion("kb-1", 3)
	if !dp.takeDeletedReconcileDirty() {
		t.Error("a version landing below the cursor (the late-push case) must mark dirty too")
	}
}

// A node at rest pays nothing: with the flag clear, the pass ticks and does no work.
func TestLocalDataPlane_ReconcileFlagStaysClearWhenNothingLands(t *testing.T) {
	dp := newCursorPlane(&recordingPuller{})

	for i := 0; i < 3; i++ {
		if dp.takeDeletedReconcileDirty() {
			t.Fatal("the flag must not set itself — nothing has landed since the last take")
		}
	}
}
