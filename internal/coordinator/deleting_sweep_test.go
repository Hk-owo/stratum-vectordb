package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	stratumerrors "stratum/internal/errors"
)

// fakeDeleting is the narrow read: ids per knowledge base, optionally failing, and it
// records which knowledge bases were asked about — which is how the tests below prove
// that an untouched knowledge base costs nothing.
type fakeDeleting struct {
	ids  map[string][]int64
	errs map[string]error
	read []string
}

func (f *fakeDeleting) DeletingVersionIDs(_ context.Context, kbID string) ([]int64, error) {
	f.read = append(f.read, kbID)
	if err := f.errs[kbID]; err != nil {
		return nil, err
	}
	return f.ids[kbID], nil
}

// fakeDirty is the tracker: a set that Take consumes, and a put-back for a pass that
// could not finish.
type fakeDirty struct {
	set map[string]bool
}

func (f *fakeDirty) TakeDirtyDeletingKBs() []string {
	if len(f.set) == 0 {
		return nil
	}
	out := make([]string, 0, len(f.set))
	for kbID := range f.set {
		out = append(out, kbID)
	}
	f.set = make(map[string]bool)
	return out
}

func (f *fakeDirty) MarkDeletingDirty(kbID string) {
	if f.set == nil {
		f.set = make(map[string]bool)
	}
	f.set[kbID] = true
}

func sweeping(versions *fakeDeleting, dirty *fakeDirty, cleanup *MockDeleteVersionCoordinator) *DeletingVersionSweeper {
	return NewDeletingVersionSweeper(versions, dirty, cleanup, time.Minute, nil)
}

// The whole point of the dirty set: a knowledge base nothing has happened to is never
// asked about. Without it the pass walks every knowledge base of the node each minute.
func TestDeletingVersionSweeper_ScansOnlyDirtyKnowledgeBases(t *testing.T) {
	versions := &fakeDeleting{ids: map[string][]int64{"kb-a": {1}, "kb-b": {2}}}
	cleanup := NewMockDeleteVersionCoordinator()

	// Nothing dirty: no read at all, and no Execute.
	resumed, err := sweeping(versions, &fakeDirty{}, cleanup).SweepOnce(context.Background())
	if err != nil || resumed != 0 {
		t.Fatalf("idle pass: resumed=%d err=%v, want 0 and nil", resumed, err)
	}
	if len(versions.read) != 0 {
		t.Errorf("an idle pass read %v; it must read nothing", versions.read)
	}
	if len(cleanup.Calls()) != 0 {
		t.Errorf("an idle pass executed %v", cleanup.Calls())
	}

	// One dirty knowledge base: exactly that one is read.
	resumed, err = sweeping(versions, &fakeDirty{set: map[string]bool{"kb-b": true}}, cleanup).SweepOnce(context.Background())
	if err != nil || resumed != 1 {
		t.Fatalf("resumed=%d err=%v, want 1 and nil", resumed, err)
	}
	if len(versions.read) != 1 || versions.read[0] != "kb-b" {
		t.Errorf("read %v, want exactly [kb-b]", versions.read)
	}
	if calls := cleanup.Calls(); len(calls) != 1 || calls[0] != "kb-b" {
		t.Errorf("Execute calls = %v, want [kb-b]", calls)
	}
}

// One Execute per knowledge base is enough — it re-scans that knowledge base's Deleting
// versions itself — and what is reported is knowledge bases, not versions.
func TestDeletingVersionSweeper_OneExecutePerKnowledgeBase(t *testing.T) {
	versions := &fakeDeleting{ids: map[string][]int64{"kb-1": {1, 2, 3}}}
	cleanup := NewMockDeleteVersionCoordinator()

	resumed, err := sweeping(versions, &fakeDirty{set: map[string]bool{"kb-1": true}}, cleanup).SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 knowledge base", resumed)
	}
	if calls := cleanup.Calls(); len(calls) != 1 {
		t.Errorf("Execute calls = %v, want exactly one", calls)
	}
}

// A knowledge base with nothing left to resume must not run the cleanup: between the mark
// and this pass, the deletion may have finished (or the version been discarded).
func TestDeletingVersionSweeper_NoDeletingVersionsDoesNotExecute(t *testing.T) {
	versions := &fakeDeleting{ids: map[string][]int64{"kb-1": nil}}
	cleanup := NewMockDeleteVersionCoordinator()

	resumed, err := sweeping(versions, &fakeDirty{set: map[string]bool{"kb-1": true}}, cleanup).SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if resumed != 0 || len(cleanup.Calls()) != 0 {
		t.Errorf("resumed=%d calls=%v, want nothing resumed", resumed, cleanup.Calls())
	}
}

// A pass that could not finish with a knowledge base puts it BACK: the next tick has to
// try again, and without that the deletion would be stranded exactly as it was before this
// sweep existed.
func TestDeletingVersionSweeper_FailedCleanupGoesBackIntoTheDirtySet(t *testing.T) {
	dirty := &fakeDirty{set: map[string]bool{"kb-a": true, "kb-b": true}}
	versions := &fakeDeleting{ids: map[string][]int64{"kb-a": {1}, "kb-b": {2}}}
	cleanup := NewMockDeleteVersionCoordinator()
	cleanup.SetExecuteFunc(func(_ context.Context, kbID string) error {
		if kbID == "kb-a" {
			return errors.New("broadcast failed")
		}
		return nil
	})
	sweeper := sweeping(versions, dirty, cleanup)

	resumed, err := sweeper.SweepOnce(context.Background())
	if err == nil {
		t.Error("a failed cleanup must be reported")
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 (only kb-b finished)", resumed)
	}
	if len(cleanup.Calls()) != 2 {
		t.Errorf("Execute calls = %v, want both attempted", cleanup.Calls())
	}
	if !dirty.set["kb-a"] {
		t.Error("kb-a must go back into the dirty set after a failed pass")
	}
	if dirty.set["kb-b"] {
		t.Error("kb-b finished and must not be re-queued")
	}

	// And the next pass picks it up again.
	cleanup.SetExecuteFunc(func(context.Context, string) error { return nil })
	if resumed, err := sweeper.SweepOnce(context.Background()); err != nil || resumed != 1 {
		t.Errorf("second pass: resumed=%d err=%v, want 1 and nil", resumed, err)
	}
}

// A read that fails is reported and re-queued: "I cannot enumerate" is not "there is
// nothing there", and it is not the knowledge base's fault either.
func TestDeletingVersionSweeper_UnreadableKnowledgeBaseIsReportedAndRequeued(t *testing.T) {
	dirty := &fakeDirty{set: map[string]bool{"kb-unreadable": true, "kb-stuck": true}}
	versions := &fakeDeleting{
		ids:  map[string][]int64{"kb-stuck": {1}},
		errs: map[string]error{"kb-unreadable": errors.New("metadata away")},
	}
	cleanup := NewMockDeleteVersionCoordinator()

	resumed, err := sweeping(versions, dirty, cleanup).SweepOnce(context.Background())
	if err == nil {
		t.Error("an unreadable knowledge base must be surfaced")
	}
	if resumed != 1 {
		t.Errorf("resumed = %d, want 1 (the readable one still runs)", resumed)
	}
	if !dirty.set["kb-unreadable"] {
		t.Error("the unreadable knowledge base must be re-queued")
	}
}

// A knowledge base that is simply GONE is a normal end, not a failure: a whole-KB delete
// finishes by removing it, so a dirty mark can outlive its knowledge base.
func TestDeletingVersionSweeper_GoneKnowledgeBaseIsQuietAndNotRequeued(t *testing.T) {
	dirty := &fakeDirty{set: map[string]bool{"kb-gone": true}}
	versions := &fakeDeleting{errs: map[string]error{"kb-gone": stratumerrors.ErrKnowledgeBaseNotFound}}
	cleanup := NewMockDeleteVersionCoordinator()

	resumed, err := sweeping(versions, dirty, cleanup).SweepOnce(context.Background())
	if err != nil {
		t.Errorf("a knowledge base that no longer exists is not an error, got %v", err)
	}
	if resumed != 0 {
		t.Errorf("resumed = %d, want 0", resumed)
	}
	if dirty.set["kb-gone"] {
		t.Error("a knowledge base that is gone must not be re-queued forever")
	}
}
