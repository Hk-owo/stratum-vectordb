package plane

import (
	"context"
	"testing"

	"stratum/internal/types"
)

// §7.9 keeps the two sides apart: data being durable does not mean the index is
// built, so a cursor alone must not make a version queryable.
func TestLocalControlPlane_ReportEpoch_DataCursorAloneDoesNotPromote(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {{KBID: "kb-1", VersionID: 3, IndexStatus: types.IndexStatusPending}},
	}}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportEpoch(context.Background(), 0, map[string]int64{"kb-1": 99}, nil); err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	if len(meta.statusCalls) != 0 {
		t.Errorf("status calls = %+v, want none: durable data does not build an index", meta.statusCalls)
	}
}

// The index side is what promotes, and it is an explicit set rather than a
// scalar: readiness is not monotonic in version order (§8.1), so a reported
// version set must be honoured as given.
func TestLocalControlPlane_ReportEpoch_IndexSetPromotesOnlyListedVersions(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{KBID: "kb-1", VersionID: 1, IndexStatus: types.IndexStatusPending},
			{KBID: "kb-1", VersionID: 2, IndexStatus: types.IndexStatusPending},
		},
	}}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportEpoch(context.Background(), 0, nil, map[string][]int64{"kb-1": {2}}); err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	if len(meta.statusCalls) != 1 {
		t.Fatalf("status calls = %+v, want exactly one promotion", meta.statusCalls)
	}
	if meta.statusCalls[0].versionID != 2 {
		t.Errorf("promoted v%d, want only v2: the set is explicit, not a range", meta.statusCalls[0].versionID)
	}
	if meta.statusCalls[0].status != types.IndexStatusReady {
		t.Errorf("status = %v, want READY", meta.statusCalls[0].status)
	}
}

// A knowledge base absent from the index-side report is left alone entirely,
// rather than promoted up to some implied boundary.
func TestLocalControlPlane_ReportEpoch_UnreportedKBIstUntouched(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {{KBID: "kb-1", VersionID: 7, IndexStatus: types.IndexStatusPending}},
		"kb-2": {{KBID: "kb-2", VersionID: 1, IndexStatus: types.IndexStatusPending}},
	}}
	cp := NewLocalControlPlane(meta)

	// Only kb-2 reports an index side; kb-1's cursor is far ahead but it is
	// silent, so its PENDING version must stay put.
	err := cp.ReportEpoch(context.Background(), 0,
		map[string]int64{"kb-1": 10, "kb-2": 1},
		map[string][]int64{"kb-2": {1}})
	if err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	if len(meta.statusCalls) != 1 {
		t.Fatalf("status calls = %+v, want exactly one promotion (kb-2's v1)", meta.statusCalls)
	}
	if meta.statusCalls[0].versionID != 1 {
		t.Errorf("promoted v%d, want v1 — kb-1 (v7) is absent from the index-side report",
			meta.statusCalls[0].versionID)
	}
}

// §10.1b/§7.9: the data half of the payload promotes the DATA side from the
// reported cursor. Reading a cursor as a range is safe because the chain is
// linear, and the storage layer reports the quorum MINIMUM of its replicas'
// cursors (§7.8) — so the failure mode is under-reporting, never a version
// promoted before its data really sits on a quorum.
//
// It must not touch IndexStatus: "the data is durable" is not "the index is
// queryable", which is exactly why §7.9 splits the payload.
func TestLocalControlPlane_ReportEpoch_DataCursorPromotesTheDataSide(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{KBID: "kb-1", VersionID: 1},
			{KBID: "kb-1", VersionID: 2},
			{KBID: "kb-1", VersionID: 3},
		},
	}}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportEpoch(context.Background(), 0, map[string]int64{"kb-1": 2}, nil); err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	if len(meta.dataDurableCalls) != 2 {
		t.Fatalf("data-side promotions = %v, want exactly v1 and v2 (the cursor reaches 2)", meta.dataDurableCalls)
	}
	for _, v := range meta.dataDurableCalls {
		if v > 2 {
			t.Errorf("promoted v%d, which is past the reported cursor", v)
		}
	}
	if len(meta.statusCalls) != 0 {
		t.Errorf("status calls = %+v, want none: a data cursor must never touch IndexStatus", meta.statusCalls)
	}
}

// A version being deleted is skipped even when the cursor covers it. A delete
// flow reclaims the data BEFORE it removes the metadata (§10.6), so inside that
// window a cursor can sit above a version whose bytes are already gone — calling
// that durable would tell the cleanup path a version is alive when it is not.
func TestLocalControlPlane_ReportEpoch_DeletingVersionIsNotPromoted(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{KBID: "kb-1", VersionID: 1},
			{KBID: "kb-1", VersionID: 2, Deleting: true},
			{KBID: "kb-1", VersionID: 3},
		},
	}}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportEpoch(context.Background(), 0, map[string]int64{"kb-1": 3}, nil); err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	for _, v := range meta.dataDurableCalls {
		if v == 2 {
			t.Fatal("v2 is being deleted: its data is (or is about to be) gone, so it must not be recorded durable")
		}
	}
	if len(meta.dataDurableCalls) != 2 {
		t.Errorf("data-side promotions = %v, want v1 and v3 only", meta.dataDurableCalls)
	}
}

// A version the control layer already settled on the data side is not reported
// again: the startup report is a snapshot, and a version already DURABLE — or
// carrying a terminal data verdict — has nothing to learn from it.
func TestLocalControlPlane_ReportEpoch_SettledDataSideIsLeftAlone(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{KBID: "kb-1", VersionID: 1, DataStatus: types.DataStatusDurable},
			{KBID: "kb-1", VersionID: 2, DataStatus: types.DataStatusFailedPermanent},
			{KBID: "kb-1", VersionID: 3}, // still PENDING: the only one to promote
		},
	}}
	cp := NewLocalControlPlane(meta)

	if err := cp.ReportEpoch(context.Background(), 0, map[string]int64{"kb-1": 3}, nil); err != nil {
		t.Fatalf("ReportEpoch: %v", err)
	}
	if len(meta.dataDurableCalls) != 1 || meta.dataDurableCalls[0] != 3 {
		t.Fatalf("data-side promotions = %v, want only v3: the others are already settled", meta.dataDurableCalls)
	}
}
