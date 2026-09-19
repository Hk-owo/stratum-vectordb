package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"stratum/internal/types"
)

// dispatchCommittedVersion is the §7.13.2 hand-off, and the case these tests
// are about is the one Step 0 changed: a version this node just committed but
// holds no changes for (docs/await-version-plan.md §4.3, §7 Step 0).

type stubPendingDispatch struct {
	parentID  int64
	changes   []types.DocChange
	found     bool
	abandoned []string
	// claimed answers DispatchClaimed: true models the proposer's own background
	// dispatch having taken the registration — the race the apply hook is
	// designed to lose sometimes.
	claimed bool
}

func (s *stubPendingDispatch) TakePendingDispatch(kbID, clientRequestID string) (int64, []types.DocChange, bool) {
	return s.parentID, s.changes, s.found
}

func (s *stubPendingDispatch) DispatchClaimed(kbID, clientRequestID string) bool {
	return s.claimed
}

func (s *stubPendingDispatch) AbandonDispatch(_ context.Context, kbID string, versionID int64, class types.FailureClass, detail string) {
	s.abandoned = append(s.abandoned, detail)
}

type stubDispatcher struct {
	calls      []stubDispatchCall
	changesGot []types.DocChange
	err        error
}

type stubDispatchCall struct {
	kbID            string
	versionID       int64
	parentVersionID int64
}

func (s *stubDispatcher) Dispatch(_ context.Context, kbID string, versionID, parentVersionID int64, changes []types.DocChange) (string, error) {
	s.calls = append(s.calls, stubDispatchCall{kbID: kbID, versionID: versionID, parentVersionID: parentVersionID})
	s.changesGot = changes
	return "node-1:7000", s.err
}

// TestDispatchCommittedVersion_LeavesTheVersionPendingWhenNothingIsRegistered
// is Step 0's regression: an in-memory miss on this node must NOT be turned
// into a failure report. It proves neither "the background dispatch already
// took the changes" nor "nobody has them", so the version stays PENDING and the
// caller decides — re-send or discard (docs/await-version-plan.md §4.3).
func TestDispatchCommittedVersion_LeavesTheVersionPendingWhenNothingIsRegistered(t *testing.T) {
	registry := &stubPendingDispatch{found: false}
	dispatcher := &stubDispatcher{}

	dispatchCommittedVersion(registry, dispatcher, zap.NewNop(), "kb-1", 7, 6, "key-1")

	if len(dispatcher.calls) != 0 {
		t.Errorf("dispatched %d times with nothing registered, want 0", len(dispatcher.calls))
	}
	if len(registry.abandoned) != 0 {
		t.Errorf("reported the version as failed (%v); an in-memory miss is not evidence, and the verdict is the caller's",
			registry.abandoned)
	}
}

func TestDispatchCommittedVersion_DispatchesWithTheRegisteredParent(t *testing.T) {
	registry := &stubPendingDispatch{
		found:    true,
		parentID: 42,
		changes:  []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc-1", Content: "x"}},
	}
	dispatcher := &stubDispatcher{}

	dispatchCommittedVersion(registry, dispatcher, zap.NewNop(), "kb-1", 7, 6, "key-1")

	if len(dispatcher.calls) != 1 {
		t.Fatalf("dispatch calls = %d, want 1", len(dispatcher.calls))
	}
	// The registered parent wins over the one on the command: a retry may
	// re-register a newer parent than the entry carried.
	if got := dispatcher.calls[0].parentVersionID; got != 42 {
		t.Errorf("parent = %d, want 42 (the registered one)", got)
	}
	if len(dispatcher.changesGot) != 1 {
		t.Errorf("changes = %d, want the registered ones", len(dispatcher.changesGot))
	}
}

// A failed dispatch is recorded, not turned into a verdict here: §10.1's retry
// budget owns that decision, and this callback is not where it gets made.
func TestDispatchCommittedVersion_AFailedDispatchIsNotAVerdict(t *testing.T) {
	registry := &stubPendingDispatch{found: true, changes: []types.DocChange{{Op: types.ChangeOpAdd, DocID: "d"}}}
	dispatcher := &stubDispatcher{err: errors.New("every candidate is gone")}

	dispatchCommittedVersion(registry, dispatcher, zap.NewNop(), "kb-1", 7, 0, "key-1")

	if len(dispatcher.calls) != 1 {
		t.Errorf("dispatch calls = %d, want 1 (the failure is the dispatcher's answer, not a reason to skip it)", len(dispatcher.calls))
	}
	if len(registry.abandoned) != 0 {
		t.Errorf("a failed dispatch reported the version as failed (%v); that verdict belongs to §10.1's budget", registry.abandoned)
	}
}

// TestDispatchCommittedVersion_LostRaceIsNotAWarning pins the distinction the
// miss used to blur. When the proposer's own background dispatch already took
// the registration, the write is in flight — the DESIGNED outcome of racing the
// two dispatch points, not a fault.
//
// It has to stay quiet because that path runs on every write: warning there is
// what made the sibling test's case unfindable in practice.
func TestDispatchCommittedVersion_LostRaceIsNotAWarning(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	registry := &stubPendingDispatch{found: false, claimed: true}
	dispatcher := &stubDispatcher{}

	dispatchCommittedVersion(registry, dispatcher, zap.New(core), "kb-1", 7, 6, "key-1")

	if n := logs.FilterLevelExact(zap.WarnLevel).Len(); n != 0 {
		t.Errorf("losing the race produced %d warning(s), want 0 — this happens on every write", n)
	}
	if len(dispatcher.calls) != 0 {
		t.Errorf("dispatched %d times with nothing registered, want 0", len(dispatcher.calls))
	}
}

// And the case the warning exists for still speaks up: no claim on record means
// nothing is in flight, because the process that proposed the changes is gone.
func TestDispatchCommittedVersion_OrphanedVersionStillWarns(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	registry := &stubPendingDispatch{found: false, claimed: false}
	dispatcher := &stubDispatcher{}

	dispatchCommittedVersion(registry, dispatcher, zap.New(core), "kb-1", 7, 6, "key-1")

	warns := logs.FilterLevelExact(zap.WarnLevel).All()
	if len(warns) != 1 {
		t.Fatalf("an orphaned version produced %d warning(s), want exactly 1", len(warns))
	}
	if msg := warns[0].Message; !strings.Contains(msg, "leaving it PENDING") {
		t.Errorf("warning = %q, want it to say the version stays PENDING", msg)
	}
	if len(registry.abandoned) != 0 {
		t.Errorf("an orphaned version was reported as failed (%v); that verdict is the caller's", registry.abandoned)
	}
}
