package plane

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
	"stratum/internal/wal"
)

// recordingChangesFetcher serves a fixed delta table and records the range it was
// asked for.
type recordingChangesFetcher struct {
	deltas  map[int64]wal.VersionDelta
	err     error
	calls   int
	gotFrom int64
	gotTo   int64
}

func (f *recordingChangesFetcher) ChangesInRange(_ context.Context, _, _ string, fromExclusive, toInclusive int64) (map[int64]wal.VersionDelta, error) {
	f.calls++
	f.gotFrom, f.gotTo = fromExclusive, toInclusive
	if f.err != nil {
		return nil, f.err
	}
	return f.deltas, nil
}

func deltaFor(versionID, parentID int64) wal.VersionDelta {
	return wal.VersionDelta{
		VersionID:       versionID,
		ParentVersionID: parentID,
		Changes:         []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc", Content: "v"}},
	}
}

// TestLocalDataPlane_BackfillUsesRecordedChangesWhenTheGapIsComplete pins the
// §7.5 fast path: when the peer has a record for every version in the gap, the
// backfill replays the deltas and never falls back to pulling full records.
func TestLocalDataPlane_BackfillUsesRecordedChangesWhenTheGapIsComplete(t *testing.T) {
	tr := &tracer{}
	puller := &stubPuller{}
	fetcher := &recordingChangesFetcher{deltas: map[int64]wal.VersionDelta{
		2: deltaFor(2, 1),
		3: deltaFor(3, 2),
	}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:            &stubWAL{t: tr},
		Executor:       &stubExecutor{t: tr},
		Puller:         puller,
		ChangesFetcher: fetcher,
	})
	dp.advanceLocalVersion("kb-1", 1) // this node holds up to v1; it needs (1,3]

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 4); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}

	if fetcher.calls != 1 || fetcher.gotFrom != 1 || fetcher.gotTo != 3 {
		t.Errorf("delta fetch = %d call(s) over (%d,%d], want 1 call over (1,3]", fetcher.calls, fetcher.gotFrom, fetcher.gotTo)
	}
	if puller.calls != 0 {
		t.Errorf("the full-record path ran %d time(s); a complete gap must be replayed from deltas", puller.calls)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 3 {
		t.Errorf("localVersion = %d, want 3 (every replayed version advances the cursor)", got)
	}
}

// A single missing delta must send the WHOLE gap down the full-record path: a
// partial replay would leave a hole in this node's history while moving its
// cursor past it.
func TestLocalDataPlane_BackfillFallsBackToFullRecordsWhenADeltaIsMissing(t *testing.T) {
	tr := &tracer{}
	puller := &stubPuller{}
	fetcher := &recordingChangesFetcher{deltas: map[int64]wal.VersionDelta{
		2: deltaFor(2, 1),
		// v3 deliberately absent: the peer has no record for it.
	}}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:            &stubWAL{t: tr},
		Executor:       &stubExecutor{t: tr},
		Puller:         puller,
		ChangesFetcher: fetcher,
	})
	dp.advanceLocalVersion("kb-1", 1)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 4); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}

	if fetcher.calls != 1 {
		t.Errorf("delta fetch calls = %d, want 1 (it is worth one look before giving up)", fetcher.calls)
	}
	if puller.calls != 2 {
		t.Errorf("full-record pulls = %d, want 2 (v2 and v3: the whole gap, not just the missing part)", puller.calls)
	}
	if got := dp.LocalVersionOf("kb-1"); got != 3 {
		t.Errorf("localVersion = %d, want 3", got)
	}
}

// A transport failure is not a verdict about the data: the backfill falls back to
// full records rather than reporting the gap as unfillable.
func TestLocalDataPlane_BackfillFallsBackToFullRecordsWhenTheDeltaFetchFails(t *testing.T) {
	tr := &tracer{}
	puller := &stubPuller{}
	fetcher := &recordingChangesFetcher{err: errors.New("peer unreachable")}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:            &stubWAL{t: tr},
		Executor:       &stubExecutor{t: tr},
		Puller:         puller,
		ChangesFetcher: fetcher,
	})
	dp.advanceLocalVersion("kb-1", 1)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 3); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}
	if puller.calls != 1 {
		t.Errorf("full-record pulls = %d, want 1 (v2) after the delta fetch failed", puller.calls)
	}
}

// With no fetcher configured the old path is untouched: this is what keeps every
// existing node (and test stack) working.
func TestLocalDataPlane_BackfillWithoutFetcherIsUnchanged(t *testing.T) {
	tr := &tracer{}
	puller := &stubPuller{}
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		WAL:      &stubWAL{t: tr},
		Executor: &stubExecutor{t: tr},
		Puller:   puller,
	})
	dp.advanceLocalVersion("kb-1", 1)

	if err := dp.backfillTo(context.Background(), "peer:7001", "kb-1", 3); err != nil {
		t.Fatalf("backfillTo: %v", err)
	}
	if puller.calls != 1 {
		t.Errorf("full-record pulls = %d, want 1 (v2)", puller.calls)
	}
}
