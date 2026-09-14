package service

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

// stubPresence answers per (replica, version) and can simulate unreachable
// replicas.
type stubPresence struct {
	holds map[string]map[int64]bool
	down  map[string]bool
}

func (s *stubPresence) HasVersion(_ context.Context, addr, _ string, versionID int64) (bool, error) {
	if s.down[addr] {
		return false, errors.New("replica unreachable")
	}
	return s.holds[addr][versionID], nil
}

func pendingVersion(versionID, createdAt int64) types.VersionMeta {
	return types.VersionMeta{
		VersionID:   versionID,
		KBID:        "kb-1",
		IndexStatus: types.IndexStatusPending,
		CreatedAt:   createdAt,
	}
}

// TestDataMissingVersions pins the distinction between "still being written /
// being built" and "the data never landed anywhere"
// (Stratum_设计文档v13.md §7.12 step ①).
func TestDataMissingVersions(t *testing.T) {
	ctx := context.Background()
	const (
		now   = int64(1000)
		minAg = int64(60)
	)
	replicas := []string{"n1", "n2", "n3"}

	cases := []struct {
		name     string
		versions []types.VersionMeta
		holds    map[string]map[int64]bool
		want     []int64
	}{
		{
			name:     "held by one replica is fine",
			versions: []types.VersionMeta{pendingVersion(5, 0)},
			holds:    map[string]map[int64]bool{"n2": {5: true}},
			want:     nil,
		},
		{
			name:     "no replica holds it is reported",
			versions: []types.VersionMeta{pendingVersion(5, 0)},
			holds:    nil,
			want:     []int64{5},
		},
		{
			name:     "a version too young to judge is skipped",
			versions: []types.VersionMeta{pendingVersion(5, now-5)},
			holds:    nil,
			want:     nil,
		},
		{
			name: "only PENDING versions are probed",
			versions: []types.VersionMeta{{
				VersionID: 5, KBID: "kb-1", IndexStatus: types.IndexStatusReady, CreatedAt: 0,
			}},
			holds: nil,
			want:  nil,
		},
		{
			name: "a deleting version is not probed",
			versions: []types.VersionMeta{func() types.VersionMeta {
				v := pendingVersion(5, 0)
				v.Deleting = true
				return v
			}()},
			holds: nil,
			want:  nil,
		},
		{
			name:     "only the missing versions of several are reported",
			versions: []types.VersionMeta{pendingVersion(5, 0), pendingVersion(6, 0), pendingVersion(7, 0)},
			holds:    map[string]map[int64]bool{"n1": {5: true}, "n3": {7: true}},
			want:     []int64{6},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dataMissingVersions(ctx, tc.versions, replicas, &stubPresence{holds: tc.holds}, now, minAg)
			if len(got) != len(tc.want) {
				t.Fatalf("dataMissingVersions = %v, want versions %v", got, tc.want)
			}
			for i, want := range tc.want {
				if got[i].VersionID != want {
					t.Errorf("result[%d] = v%d, want v%d", i, got[i].VersionID, want)
				}
			}
		})
	}
}

// An unreachable replica must count as "unknown", not as "does not hold it":
// otherwise a single down node would make every PENDING version look
// data-missing.
func TestDataMissingVersions_UnreachableReplicasAreUnknown(t *testing.T) {
	ctx := context.Background()
	replicas := []string{"n1", "n2", "n3"}
	versions := []types.VersionMeta{pendingVersion(5, 0)}

	allDown := &stubPresence{down: map[string]bool{"n1": true, "n2": true, "n3": true}}
	if got := dataMissingVersions(ctx, versions, replicas, allDown, 1000, 60); len(got) != 0 {
		t.Errorf("with every replica unreachable nothing can be concluded, got %v", got)
	}

	// One replica answers "no" while the others are down: that is still not
	// enough to conclude the data is missing everywhere.
	oneAnswering := &stubPresence{
		down:  map[string]bool{"n2": true, "n3": true},
		holds: nil, // n1 answers: does not hold it
	}
	if got := dataMissingVersions(ctx, versions, replicas, oneAnswering, 1000, 60); len(got) != 1 {
		t.Errorf("with an answering replica that does not hold it, the version must be reported; got %v", got)
	}
}

// Without replicas (or without a checker) there is nothing to probe.
func TestDataMissingVersions_NoReplicasIsNoop(t *testing.T) {
	ctx := context.Background()
	versions := []types.VersionMeta{pendingVersion(5, 0)}
	checker := &stubPresence{}

	if got := dataMissingVersions(ctx, versions, nil, checker, 1000, 60); got != nil {
		t.Errorf("with no replica set nothing can be concluded, got %v", got)
	}
	if got := dataMissingVersions(ctx, versions, []string{"n1"}, nil, 1000, 60); got != nil {
		t.Errorf("with no checker nothing can be probed, got %v", got)
	}
}
