package plane

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

// DataStatusOf is what lets a takeover skip a settled version, and "cannot answer" has
// to stay distinct from every answer: the caller announces on it.
func TestLocalControlPlane_DataStatusOf(t *testing.T) {
	meta := &stubMeta{versions: map[string][]types.VersionMeta{
		"kb-1": {
			{KBID: "kb-1", VersionID: 6, DataStatus: types.DataStatusDurable},
			{KBID: "kb-1", VersionID: 7, DataStatus: types.DataStatusPending},
			{KBID: "kb-1", VersionID: 9, DataStatus: types.DataStatusFailedPermanent},
		},
	}}
	cp := NewLocalControlPlane(meta)
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		kbID      string
		versionID int64
		want      types.DataStatus
		ok        bool
	}{
		{"durable", "kb-1", 6, types.DataStatusDurable, true},
		{"pending", "kb-1", 7, types.DataStatusPending, true},
		{"terminal", "kb-1", 9, types.DataStatusFailedPermanent, true},
		// Between two live neighbours: their statuses say nothing about this one.
		{"absent", "kb-1", 8, types.DataStatusPending, false},
		{"another knowledge base", "kb-2", 6, types.DataStatusPending, false},
		{"no such version id", "kb-1", 0, types.DataStatusPending, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := cp.DataStatusOf(ctx, tc.kbID, tc.versionID)
			if ok != tc.ok || got != tc.want {
				t.Errorf("DataStatusOf = (%v, %v), want (%v, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestLocalControlPlane_DataStatusOfCannotAnswer(t *testing.T) {
	if _, ok := NewLocalControlPlane(nil).DataStatusOf(context.Background(), "kb-1", 7); ok {
		t.Error("no metadata is wired, so there is no answer")
	}
	meta := &stubMeta{listErr: errors.New("no quorum")}
	if _, ok := NewLocalControlPlane(meta).DataStatusOf(context.Background(), "kb-1", 7); ok {
		t.Error("an unreadable list is not a verdict")
	}
}

// Only the settled refusal is absorbed. Anything else stays a report failure: swallowing
// it would leave the version PENDING with its budget spent and nobody told.
func TestLocalControlPlane_OtherVerdictFailuresStillSurface(t *testing.T) {
	meta := &stubMeta{permanentErr: errors.New("propose: lost leadership")}
	cp := NewLocalControlPlane(meta, WithFailureBudget(1))

	terminal, err := cp.ReportVersionFailure(context.Background(), "kb-1", 7, types.FailureSideData, types.FailureTransient, "x")
	if err == nil {
		t.Fatal("a failed proposal must be reported")
	}
	if terminal {
		t.Error("nothing was decided, so terminal must be false")
	}
}
