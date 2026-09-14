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
