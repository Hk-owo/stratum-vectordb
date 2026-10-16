package nodestate

import (
	"sort"
	"testing"
)

// TestMachine_CursorStopsAtAnAnnouncedGap is the invariant §7.5 rests on, and
// the reason the cursor is an account rather than a maximum: a version the
// control layer says exists but this node does not hold has to stop it, or a
// node that missed one push and received a later one would claim an unbroken
// history it does not have.
func TestMachine_CursorStopsAtAnAnnouncedGap(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	if got := m.Advance(kb, 1); got != 1 {
		t.Fatalf("after v1 landed, cursor = %d, want 1", got)
	}

	// v2 is known to exist and is not here, so v3 landing must not carry the
	// cursor over it.
	m.Announce(kb, 2)
	if got := m.Advance(kb, 3); got != 1 {
		t.Fatalf("after v3 landed behind an announced v2, cursor = %d, want 1", got)
	}

	// v2 landing closes the gap, and the cursor steps over BOTH in one move.
	if got := m.Advance(kb, 2); got != 3 {
		t.Fatalf("after v2 landed, cursor = %d, want 3", got)
	}
}

// TestMachine_CursorStepsOverWhatIsAccountedFor: with nothing announced, a
// version landing above the cursor moves it there. "Accounted for" is the
// operative phrase — the machine has been told this node holds it or that it
// no longer exists — and it is deliberately not the same as "held", which is
// why the reclaim path (cursorStepsPast) reports it the same way.
func TestMachine_CursorStepsOverWhatIsAccountedFor(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	if got := m.Advance(kb, 4); got != 4 {
		t.Fatalf("cursor = %d, want 4: nothing announced a gap below it", got)
	}
	if got := m.Advance(kb, 4); got != 4 {
		t.Fatalf("repeating an advance moved the cursor to %d, want it to stay at 4", got)
	}
	if got := m.Advance(kb, 2); got != 4 {
		t.Fatalf("an older version moved the cursor back to %d, want it to stay at 4", got)
	}
}

// TestMachine_MarkHandledAccountsForARangeInOneStep: §6.4's full-state transfer
// covers a whole range at once, and the cursor has to end up as far along as if
// every version in it had landed individually.
func TestMachine_MarkHandledAccountsForARangeInOneStep(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	m.Advance(kb, 1)
	if got := m.MarkHandled(kb, 2, 5); got != 5 {
		t.Fatalf("cursor after MarkHandled(2,5) = %d, want 5", got)
	}
}

// TestMachine_AnnounceAloneNeverMovesTheCursor: existence is not possession.
func TestMachine_AnnounceAloneNeverMovesTheCursor(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	m.Advance(kb, 1)
	m.Announce(kb, 2)
	m.Announce(kb, 3)

	if got := m.Cursor(kb); got != 1 {
		t.Fatalf("cursor = %d, want 1: announcing a version does not make this node hold it", got)
	}
}

// TestMachine_InstalledCursorIsMonotone: the recovery path reads a value written
// before a crash, while memory may already have moved past it. Moving back would
// forget versions this node holds.
func TestMachine_InstalledCursorIsMonotone(t *testing.T) {
	m := New(Options{})
	const kb = "kb-1"

	m.Advance(kb, 3)
	m.InstallCursor(kb, 1)
	if got := m.Cursor(kb); got != 3 {
		t.Fatalf("an older persisted record moved the cursor back to %d, want it to stay at 3", got)
	}

	m.InstallCursor(kb, 7)
	if got := m.Cursor(kb); got != 7 {
		t.Fatalf("a newer persisted record left the cursor at %d, want 7", got)
	}
}

// TestMachine_CursorsIsACopy: the caller encodes it, retries it, or holds it
// while a write lands — none of which may mutate what was already reported.
func TestMachine_CursorsIsACopy(t *testing.T) {
	m := New(Options{})
	m.Advance("kb-1", 1)
	m.Advance("kb-2", 2)

	snapshot := m.Cursors()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot = %v, want both knowledge bases", snapshot)
	}

	// Mutating the copy must not reach the machine, and a later advance must not
	// reach the copy.
	snapshot["kb-1"] = 99
	snapshot["kb-3"] = 3
	m.Advance("kb-1", 2)

	if got := m.Cursor("kb-1"); got != 2 {
		t.Fatalf("cursor = %d, want 2 (the snapshot edit must not have landed)", got)
	}
	if _, ok := snapshot["kb-2"]; !ok || snapshot["kb-1"] != 99 {
		t.Fatalf("snapshot = %v, want the caller's own copy left alone", snapshot)
	}

	// An empty machine reports nil rather than an empty map, so a receiver can
	// tell "never reported" from "reported nothing".
	if got := New(Options{}).Cursors(); got != nil {
		t.Fatalf("Cursors() with nothing = %v, want nil", got)
	}
}

// TestMachine_KnowledgeBasesIsTheCursorKeySet: a knowledge base with a cursor is
// one this node holds data for, which is exactly what the reclaim sweep needs.
func TestMachine_KnowledgeBasesIsTheCursorKeySet(t *testing.T) {
	m := New(Options{})
	m.Advance("kb-b", 1)
	m.Advance("kb-a", 1)
	m.Announce("kb-c", 1) // announced but not held: no data to reclaim

	got := m.KnowledgeBases()
	want := []string{"kb-a", "kb-b"}
	if len(got) != len(want) {
		t.Fatalf("KnowledgeBases = %v, want %v", got, want)
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("KnowledgeBases = %v, want a sorted result so a sweep is reproducible", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KnowledgeBases = %v, want %v", got, want)
		}
	}
}
