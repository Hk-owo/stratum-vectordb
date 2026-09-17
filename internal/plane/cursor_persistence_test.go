package plane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"stratum/internal/types"
	"stratum/internal/wal"
)

// countingIndexStore counts IndexExists calls. The whole point of persisting the
// cursor is that startup stops asking the disk what this node holds: an artifact
// is a cache the retention policy may delete, so the answer it gives is not a
// statement of fact (docs/cursor-persistence-plan.md §4).
type countingIndexStore struct {
	stubIndexStore
	existsCalls int
}

func (s *countingIndexStore) IndexExists(ctx context.Context, kbID string, versionID int64) (bool, error) {
	s.existsCalls++
	return s.stubIndexStore.IndexExists(ctx, kbID, versionID)
}

var _ IndexStore = (*countingIndexStore)(nil)

// failingCursorStore accepts no record: the shape of a crash that lands between
// the data arriving and the cursor being written.
type failingCursorStore struct {
	mu     sync.Mutex
	writes int
}

func (f *failingCursorStore) WriteCursor(context.Context, string, int64) error {
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	return errors.New("disk on fire")
}

func (f *failingCursorStore) RecoverCursors(context.Context) (map[string]int64, error) {
	return nil, nil
}

func (f *failingCursorStore) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

var _ CursorStore = (*failingCursorStore)(nil)

// versionChain builds the metadata of a contiguous chain [from, to] whose
// versions all carry a document set (and therefore no "empty set" shortcut).
func versionChain(kbID string, from, to int64) []types.VersionMeta {
	out := make([]types.VersionMeta, 0, to-from+1)
	for v := from; v <= to; v++ {
		out = append(out, types.VersionMeta{
			KBID:         kbID,
			VersionID:    v,
			IndexStatus:  types.IndexStatusReady,
			DocIDSetHash: fmt.Sprintf("digest-%d", v),
		})
	}
	return out
}

// newCursorTestPlane wires only what a cursor advance needs, so each case can
// drive one advance point and then look at the record it produced.
func newCursorTestPlane(store CursorStore) *LocalDataPlane {
	return NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       &stubPuller{},
		Verify:       func(context.Context, string, int64) bool { return true },
		Resolve: func(context.Context, string, int64) (string, bool, error) {
			return "peer:7000", true, nil
		},
		DataDropper: &stubDropper{},
		CursorWAL:   store,
	})
}

// waitForPersistedCursor polls until the record for kbID carries want. The write
// is asynchronous by design (the cursor's owner must never fsync under its own
// lock), so a read immediately after an advance would race it.
func waitForPersistedCursor(t *testing.T, store CursorStore, kbID string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var got map[string]int64
	for time.Now().Before(deadline) {
		var err error
		got, err = store.RecoverCursors(context.Background())
		if err != nil {
			t.Fatalf("RecoverCursors: %v", err)
		}
		if got[kbID] == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cursor record for %s = %d, want %d (records: %v)", kbID, got[kbID], want, got)
}

// TestLocalDataPlane_RecoverLocalCursors_PrefersTheRecord is the acceptance case
// of docs/cursor-persistence-plan.md §4.3: a node that holds v1..v10 but whose
// artifacts were ALL deleted by the retention policy must come back with a cursor
// of 10.
//
// Judging "what I hold" from an artifact is what used to understate that cursor —
// the policy deletes artifacts on purpose, and a node whose cursor stays low is
// kept out of the station's holder set. With a record, the question is not asked
// of the disk at all, which the IndexExists count asserts directly.
func TestLocalDataPlane_RecoverLocalCursors_PrefersTheRecord(t *testing.T) {
	const kbID = "kb-restart"
	ctx := context.Background()

	store := wal.NewMockWAL()
	if err := store.WriteCursor(ctx, kbID, 10); err != nil {
		t.Fatalf("WriteCursor: %v", err)
	}

	idx := &countingIndexStore{stubIndexStore: stubIndexStore{exists: map[int64]bool{}}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: idx, CursorWAL: store})
	meta := &stubMetadata{
		kbs:      []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: versionChain(kbID, 1, 10)},
	}

	if err := dp.RecoverLocalCursors(ctx, meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 10 {
		t.Fatalf("cursor = %d, want 10 — the record says 10 and no artifact exists, so inferring would report 0", got)
	}
	if idx.existsCalls != 0 {
		t.Errorf("IndexExists calls = %d, want 0: a knowledge base with a persisted cursor needs no inference", idx.existsCalls)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_FallbackInfersAndPersists covers §3.5's
// other half: a knowledge base with NO record (an older log) is still inferred
// from local facts — and the inference is written down, so the next startup reads
// it back instead of inferring again.
func TestLocalDataPlane_RecoverLocalCursors_FallbackInfersAndPersists(t *testing.T) {
	const kbID = "kb-old-wal"
	ctx := context.Background()

	store := wal.NewMockWAL() // no cursor record: an older log
	idx := &countingIndexStore{stubIndexStore: stubIndexStore{exists: map[int64]bool{1: true, 2: true}}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: idx, CursorWAL: store})
	meta := &stubMetadata{
		kbs:      []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: versionChain(kbID, 1, 3)},
	}

	if err := dp.RecoverLocalCursors(ctx, meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 2 {
		t.Fatalf("cursor = %d, want 2 (v3 has no artifact here, so the chain stops below it)", got)
	}
	if idx.existsCalls == 0 {
		t.Error("IndexExists was never called: without a record the fallback HAS to ask the disk")
	}
	waitForPersistedCursor(t, store, kbID, 2)
}

// TestLocalDataPlane_CrashBetweenDataAndCursorLeavesTheCursorLow pins the
// direction of the failure (docs/cursor-persistence-plan.md §3.3): the record may
// lag the data, never lead it.
//
// The crash is simulated by a store that accepts nothing. In memory the cursor
// moves — the data is here and this node serves it — while a fresh plane reading
// the log back sees no record and therefore reports 0. Low is the safe direction:
// a restart backfills a little more than it strictly must, instead of claiming a
// history it does not hold.
func TestLocalDataPlane_CrashBetweenDataAndCursorLeavesTheCursorLow(t *testing.T) {
	const kbID = "kb-crash"
	ctx := context.Background()

	store := &failingCursorStore{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: &stubIndexStore{}, CursorWAL: store})
	dp.advanceLocalVersion(kbID, 7) // the data landed; only the record did not
	if got := dp.LocalVersionOf(kbID); got != 7 {
		t.Fatalf("in-memory cursor = %d, want 7 — the crash is in the record, not in the running node", got)
	}
	// The write is attempted off the advance (the cursor must never be persisted
	// under the lock its readers take), so give it room to happen before asserting
	// that it did.
	deadline := time.Now().Add(2 * time.Second)
	for store.writeCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.writeCount() == 0 {
		t.Fatal("precondition: the node tried to persist the cursor (it failed, which is the point)")
	}

	// The restart: a fresh plane over a log that has no record for the KB.
	restarted := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &countingIndexStore{stubIndexStore: stubIndexStore{exists: map[int64]bool{}}},
		CursorWAL:    &failingCursorStore{},
	})
	meta := &stubMetadata{
		kbs:      []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: versionChain(kbID, 1, 7)},
	}
	if err := restarted.RecoverLocalCursors(ctx, meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := restarted.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor after the crash = %d, want 0: a missing record reads as 'behind', never as 'ahead'", got)
	}
}

// TestLocalDataPlane_EveryAdvancePointPersistsTheCursor drives each path that
// accounts for a version and asserts that the record followed it. The paths are
// what docs/cursor-persistence-plan.md §3.2 lists; they all go through
// advanceLocalVersion, and this is what keeps that from being a promise in a
// comment.
func TestLocalDataPlane_EveryAdvancePointPersistsTheCursor(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name  string
		kbID  string
		want  int64
		drive func(t *testing.T, dp *LocalDataPlane)
	}{
		{
			name: "EnsureIndex (pull)", kbID: "kb-ensure", want: 4,
			drive: func(t *testing.T, dp *LocalDataPlane) {
				if err := dp.EnsureIndex(ctx, "kb-ensure", 4); err != nil {
					t.Fatalf("EnsureIndex: %v", err)
				}
			},
		},
		{
			name: "FetchVersionData", kbID: "kb-fetch", want: 5,
			drive: func(t *testing.T, dp *LocalDataPlane) {
				if err := dp.FetchVersionData(ctx, "kb-fetch", 5); err != nil {
					t.Fatalf("FetchVersionData: %v", err)
				}
			},
		},
		{
			name: "DropVersionData", kbID: "kb-drop", want: 6,
			drive: func(t *testing.T, dp *LocalDataPlane) {
				if err := dp.DropVersionData(ctx, "kb-drop", 6); err != nil {
					t.Fatalf("DropVersionData: %v", err)
				}
			},
		},
		{
			name: "full-state transfer", kbID: "kb-snapshot", want: 9,
			drive: func(t *testing.T, dp *LocalDataPlane) {
				dp.markVersionsHandled("kb-snapshot", 8, 9)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := wal.NewMockWAL()
			dp := newCursorTestPlane(store)
			tc.drive(t, dp)
			if got := dp.LocalVersionOf(tc.kbID); got != tc.want {
				t.Fatalf("in-memory cursor = %d, want %d", got, tc.want)
			}
			waitForPersistedCursor(t, store, tc.kbID, tc.want)
		})
	}
}

// TestLocalDataPlane_RecoverLocalCursors_MixesTheRecordAndTheFallback covers the
// startup step's real shape (docs/cursor-persistence-plan.md §3.5): some knowledge
// bases have a persisted cursor and some do not, and the two are decided by
// DIFFERENT evidence in the SAME pass — the record where there is one, the disk
// where there is not.
//
// The second half is what makes the fallback a migration rather than a permanent
// cost: the inference is written down, so the next startup reads both knowledge
// bases back and asks the disk nothing at all.
func TestLocalDataPlane_RecoverLocalCursors_MixesTheRecordAndTheFallback(t *testing.T) {
	const recorded, inferred = "kb-recorded", "kb-inferred"
	ctx := context.Background()

	store := wal.NewMockWAL()
	if err := store.WriteCursor(ctx, recorded, 7); err != nil {
		t.Fatalf("WriteCursor: %v", err)
	}

	// Only kb-inferred has artifacts on this node (v1, v2), and it has no record:
	// the fallback is the only evidence available for it.
	idx := &countingIndexStore{stubIndexStore: stubIndexStore{exists: map[int64]bool{1: true, 2: true}}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: idx, CursorWAL: store})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: recorded}, {KBID: inferred}},
		versions: map[string][]types.VersionMeta{
			recorded: versionChain(recorded, 1, 9),
			inferred: versionChain(inferred, 1, 3),
		},
	}

	if err := dp.RecoverLocalCursors(ctx, meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(recorded); got != 7 {
		t.Errorf("cursor of the recorded knowledge base = %d, want 7", got)
	}
	if got := dp.LocalVersionOf(inferred); got != 2 {
		t.Errorf("cursor of the inferred knowledge base = %d, want 2 (v3 has no artifact here)", got)
	}
	if idx.existsCalls == 0 {
		t.Error("the knowledge base with no record was never inferred: the fallback did not run")
	}
	waitForPersistedCursor(t, store, inferred, 2)

	// Second startup, same log: both knowledge bases now have a record, so the disk
	// is not consulted at all.
	before := idx.existsCalls
	second := NewLocalDataPlane(LocalDataPlaneConfig{IndexManager: idx, CursorWAL: store})
	if err := second.RecoverLocalCursors(ctx, meta); err != nil {
		t.Fatalf("second RecoverLocalCursors: %v", err)
	}
	if idx.existsCalls != before {
		t.Errorf("IndexExists calls on the second startup = %d, want 0: once a knowledge base has a record, nothing may be inferred again",
			idx.existsCalls-before)
	}
	if got := second.LocalVersionOf(recorded); got != 7 {
		t.Errorf("cursor of the recorded knowledge base after a second startup = %d, want 7", got)
	}
	if got := second.LocalVersionOf(inferred); got != 2 {
		t.Errorf("cursor of the inferred knowledge base after a second startup = %d, want 2", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_LogsWhichEvidenceWasUsed pins the
// observable half of docs/cursor-persistence-plan.md §9: "read the cursor back"
// and "inferred it from the disk" have to be distinguishable in the log, because
// an operator looking at a low cursor needs to know which of the two happened —
// one is a fact the node recorded, the other is a judgement about files that may
// already have been deleted.
func TestLocalDataPlane_RecoverLocalCursors_LogsWhichEvidenceWasUsed(t *testing.T) {
	const recorded, inferred = "kb-recorded", "kb-inferred"
	ctx := context.Background()

	core, logs := observer.New(zapcore.InfoLevel)

	store := wal.NewMockWAL()
	if err := store.WriteCursor(ctx, recorded, 7); err != nil {
		t.Fatalf("WriteCursor: %v", err)
	}
	idx := &countingIndexStore{stubIndexStore: stubIndexStore{exists: map[int64]bool{1: true, 2: true}}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: idx,
		CursorWAL:    store,
		Logger:       zap.New(core),
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: recorded}, {KBID: inferred}},
		versions: map[string][]types.VersionMeta{
			recorded: versionChain(recorded, 1, 9),
			inferred: versionChain(inferred, 1, 3),
		},
	}
	if err := dp.RecoverLocalCursors(ctx, meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}

	assertLogged := func(message, kbID string) {
		t.Helper()
		entries := logs.FilterMessage(message).All()
		for _, e := range entries {
			if e.ContextMap()["kb_id"] == kbID {
				return
			}
		}
		t.Errorf("no %q line for %s (entries: %v)", message, kbID, entries)
	}
	assertLogged("plane: cursor recovery: read the persisted data cursor back", recorded)
	assertLogged("plane: cursor recovery: recovered the local data cursor from disk", inferred)
}
