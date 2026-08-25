package wal

import (
	"context"
	"sync"

	"stratum/internal/types"
)

// recordKind identifies the kind of WAL record in MockWAL's internal log.
// This is private to the mock — the real on-disk format (Phase 2-A) is not
// constrained by this representation.
type recordKind int

const (
	recordBegin recordKind = iota
	recordVersionID
	recordCommit
	recordDeleteMark
	recordDeleteComplete
	recordVersionDeleteMark
	recordVersionDeleteComplete
)

type record struct {
	kind      recordKind
	versionID int64
	kbID      string
	begin     beginData // populated for recordBegin
}

// MockWAL is an in-memory WAL for use in unit tests of modules that depend
// on WAL (RaftNode, WriteCoordinator, DeleteCoordinator). Unlike most other
// mocks in this codebase, MockWAL keeps an actual ordered record log (not
// just latest-state) because WAL.Recover's contract is fundamentally about
// reasoning over a sequence of records — tests need to be able to simulate
// "truncate after step N" by constructing a MockWAL with exactly the
// records that would exist at that point.
//
// It is not a substitute for the real on-disk implementation's own tests
// (see T1-6 in Stratum_测试顺序.md, which specifically test truncation —
// i.e. an incomplete on-disk log — which only makes sense against a real
// file-backed WAL).
type MockWAL struct {
	mu      sync.Mutex
	records []record

	// idempotency tracking
	versionIDsWritten   map[int64]bool
	committedVersions   map[int64]bool
	deleteMarked        map[string]bool
	deleteCompleted     map[string]bool
	versionDeleteMarked map[int64]string // versionID -> kbID
	versionDeleteDone   map[int64]bool

	// beginDataByVersion mirrors FileWAL: VERSION_ID records bind to the
	// replay input of the most recent unpaired BEGIN (see WriteBegin /
	// WriteVersionID).
	beginDataByVersion map[int64]beginData

	replayCounters map[replayKey]int
}

// NewMockWAL constructs an empty MockWAL.
func NewMockWAL() *MockWAL {
	return &MockWAL{
		versionIDsWritten:   make(map[int64]bool),
		committedVersions:   make(map[int64]bool),
		deleteMarked:        make(map[string]bool),
		deleteCompleted:     make(map[string]bool),
		versionDeleteMarked: make(map[int64]string),
		versionDeleteDone:   make(map[int64]bool),
		beginDataByVersion:  make(map[int64]beginData),
		replayCounters:      make(map[replayKey]int),
	}
}

func (w *MockWAL) WriteBegin(_ context.Context, kbID string, parentVersionID int64, changes []types.DocChange) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, record{kind: recordBegin, begin: beginData{kbID: kbID, parentVersionID: parentVersionID, changes: changes}})
	return nil
}

func (w *MockWAL) WriteVersionID(_ context.Context, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.versionIDsWritten[versionID] {
		return nil // idempotent
	}
	w.versionIDsWritten[versionID] = true
	w.records = append(w.records, record{kind: recordVersionID, versionID: versionID})
	// Bind the most recent unpaired BEGIN's replay input to this version,
	// mirroring FileWAL.rebuildIndex.
	for i := len(w.records) - 1; i >= 0; i-- {
		if w.records[i].kind == recordBegin {
			w.beginDataByVersion[versionID] = w.records[i].begin
			break
		}
	}
	return nil
}

func (w *MockWAL) WriteCommit(_ context.Context, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committedVersions[versionID] {
		return nil // idempotent
	}
	w.committedVersions[versionID] = true
	w.records = append(w.records, record{kind: recordCommit, versionID: versionID})
	return nil
}

func (w *MockWAL) WriteDeleteMark(_ context.Context, kbID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deleteMarked[kbID] {
		return nil
	}
	w.deleteMarked[kbID] = true
	w.records = append(w.records, record{kind: recordDeleteMark, kbID: kbID})
	return nil
}

func (w *MockWAL) WriteDeleteComplete(_ context.Context, kbID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deleteCompleted[kbID] {
		return nil
	}
	w.deleteCompleted[kbID] = true
	w.records = append(w.records, record{kind: recordDeleteComplete, kbID: kbID})
	return nil
}

// WriteVersionDeleteMark records the start of a DeleteVersion flow.
func (w *MockWAL) WriteVersionDeleteMark(_ context.Context, kbID string, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, done := w.versionDeleteMarked[versionID]; done {
		return nil // idempotent
	}
	w.versionDeleteMarked[versionID] = kbID
	w.records = append(w.records, record{kind: recordVersionDeleteMark, kbID: kbID, versionID: versionID})
	return nil
}

// WriteVersionDeleteComplete records the end of a DeleteVersion flow.
func (w *MockWAL) WriteVersionDeleteComplete(_ context.Context, kbID string, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.versionDeleteDone[versionID] {
		return nil // idempotent
	}
	w.versionDeleteDone[versionID] = true
	w.records = append(w.records, record{kind: recordVersionDeleteComplete, kbID: kbID, versionID: versionID})
	return nil
}

// IsDeleteMarked reports whether a DeleteKnowledgeBase marker was written
// for kbID. Test helper, not part of the WAL interface.
func (w *MockWAL) IsDeleteMarked(kbID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deleteMarked[kbID]
}

// Recover replays the in-memory record log and returns PendingRecords for
// any flow that began but did not reach its terminal record:
//   - a BEGIN with no following VERSION_ID for the same transaction slot
//     produces no PendingRecord (per the design doc: the state machine has
//     no corresponding version, so there is nothing to resume — the caller
//     just re-proposes from scratch on its own initiative);
//   - a VERSION_ID with no matching COMMIT produces a
//     PendingRecord{Type: VersionWrite, VersionID: versionID};
//   - a DELETE_MARK with no matching DELETE_COMPLETE produces a
//     PendingRecord{Type: DeleteMark, KBID: kbID}.
func (w *MockWAL) Recover(_ context.Context) ([]types.PendingRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var out []types.PendingRecord
	for versionID, written := range w.versionIDsWritten {
		if written && !w.committedVersions[versionID] {
			rec := types.PendingRecord{Type: types.PendingRecordTypeVersionWrite, VersionID: versionID}
			if bd, ok := w.beginDataByVersion[versionID]; ok {
				rec.KBID = bd.kbID
				rec.ParentVersionID = bd.parentVersionID
				rec.Changes = bd.changes
			}
			out = append(out, rec)
		}
	}
	for kbID := range w.deleteMarked {
		if !w.deleteCompleted[kbID] {
			out = append(out, types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: kbID})
		}
	}
	for versionID, kbID := range w.versionDeleteMarked {
		if !w.versionDeleteDone[versionID] {
			out = append(out, types.PendingRecord{Type: types.PendingRecordTypeVersionDelete, KBID: kbID, VersionID: versionID})
		}
	}
	return out, nil
}

// PendingVersionIDs returns every versionID that has a VERSION_ID record
// but no matching COMMIT record — i.e. versions whose storage writes need
// to be replayed from scratch on restart. This is the same information
// Recover() now surfaces as PendingRecordTypeVersionWrite entries;
// PendingVersionIDs remains as a convenience accessor for tests that only
// care about the version IDs themselves. Not part of the WAL interface.
func (w *MockWAL) PendingVersionIDs() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []int64
	for versionID, written := range w.versionIDsWritten {
		if written && !w.committedVersions[versionID] {
			out = append(out, versionID)
		}
	}
	return out
}

// RecordCount returns the number of records in the mock's ordered log.
// Test-only helper for asserting that an operation appended the expected
// number of records (e.g. that a replay wrote exactly one COMMIT and no
// new BEGIN/VERSION_ID).
func (w *MockWAL) RecordCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.records)
}

func (w *MockWAL) GetReplayCounters() []types.ReplayCounter {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]types.ReplayCounter, 0, len(w.replayCounters))
	for rec, count := range w.replayCounters {
		out = append(out, types.ReplayCounter{
			Record:     types.PendingRecord{Type: rec.typ, KBID: rec.kbID, VersionID: rec.versionID},
			RetryCount: count,
		})
	}
	return out
}

// IncrementReplayCounter is a test/internal helper letting callers (e.g. a
// future WriteCoordinator under test) simulate replay failures
// accumulating against a PendingRecord, mirroring the real WALImpl's
// in-memory (non-persisted) ReplayCounter behavior.
func (w *MockWAL) IncrementReplayCounter(rec types.PendingRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.replayCounters[makeReplayKey(rec)]++
}

// Truncate discards the last n records from the in-memory log, simulating
// a crash mid-write that left the on-disk WAL truncated. This is the
// primary mechanism MockWAL exposes for constructing the T1-6-style
// "truncate after step N" test scenarios at the mock level; the real T1-6
// suite exercises actual file truncation against WALImpl.
func (w *MockWAL) Truncate(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if n <= 0 || n > len(w.records) {
		return
	}
	removed := w.records[len(w.records)-n:]
	w.records = w.records[:len(w.records)-n]
	// Roll back idempotency tracking for removed records so Recover/
	// PendingVersionIDs reflect the truncated state.
	for _, r := range removed {
		switch r.kind {
		case recordVersionID:
			delete(w.versionIDsWritten, r.versionID)
		case recordCommit:
			delete(w.committedVersions, r.versionID)
		case recordDeleteMark:
			delete(w.deleteMarked, r.kbID)
		case recordDeleteComplete:
			delete(w.deleteCompleted, r.kbID)
		case recordVersionDeleteMark:
			delete(w.versionDeleteMarked, r.versionID)
		case recordVersionDeleteComplete:
			delete(w.versionDeleteDone, r.versionID)
		}
	}
}

// Reset clears all stored state. Convenience for tests; not part of the
// WAL interface.
func (w *MockWAL) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = nil
	w.versionIDsWritten = make(map[int64]bool)
	w.committedVersions = make(map[int64]bool)
	w.deleteMarked = make(map[string]bool)
	w.deleteCompleted = make(map[string]bool)
	w.versionDeleteMarked = make(map[int64]string)
	w.versionDeleteDone = make(map[int64]bool)
	w.replayCounters = make(map[replayKey]int)
}

var _ WAL = (*MockWAL)(nil)
