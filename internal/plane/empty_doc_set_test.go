package plane

import (
	"context"
	"errors"
	"testing"
	"time"

	stratinternalsync "stratum/internal/sync"
)

// versionDigestFunc adapts a function to VersionDigest, the same way the other
// tests in this package build their seams.
type versionDigestFunc func(ctx context.Context, kbID string, versionID int64) (string, error)

func (f versionDigestFunc) DigestOf(ctx context.Context, kbID string, versionID int64) (string, error) {
	return f(ctx, kbID, versionID)
}

// TestLocalDataPlane_EnsureIndex_AdvancesCursorForAnEmptyVersion pins the fix for
// "a version with no documents never converges".
//
// The state it describes is ordinary: deleting a knowledge base's last document
// produces a version whose document set is EMPTY, and no writer commits a
// document-set digest for it (there is no set to hash). §7.5's pull loop waits for
// that digest to match, so it would spin out its whole timeout while the node's
// contiguous cursor stayed below a version it in fact holds. The station's
// freshness check (§9.3(2)) reads that cursor, so queries were refused with
// "local history reaches version 0".
//
// The empty set is a property of a version's DOCUMENT SET, which is why it
// outlives the empty CHANGES list that used to be conflated with it
// (docs/cursor-persistence-plan.md §5.4): the coordinator now refuses a change-less
// version, but a version that removes every document is still perfectly legal and
// still carries no digest.
func TestLocalDataPlane_EnsureIndex_AdvancesCursorForAnEmptyVersion(t *testing.T) {
	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
		Puller:       &sourceRecordingPuller{},
		// An empty version has no digest to converge on: verification can never
		// succeed for it, which is the whole point.
		Verify: func(context.Context, string, int64) bool { return false },
		Resolve: func(context.Context, string, int64) (string, bool, error) {
			return "writer:7000", true, nil
		},
		Digest: versionDigestFunc(func(context.Context, string, int64) (string, error) {
			return stratinternalsync.ComputeDocIDSetHash(nil), nil
		}),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const versionID = 51
	if err := dp.EnsureIndex(ctx, "kb-empty", versionID); err != nil {
		t.Fatalf("EnsureIndex on an empty version: %v", err)
	}
	if got := dp.LocalVersionOf("kb-empty"); got != versionID {
		t.Fatalf("cursor = %d, want %d — a version with no documents is held once its (empty) pull succeeds",
			got, versionID)
	}
}

// TestLocalDataPlane_VersionHasNoDocuments pins the judgement itself, including
// the direction it must fail in: with no digest source the answer is "no", so a
// missing seam degrades to the conservative behaviour (wait, then fail) rather
// than advancing a cursor on evidence the node does not have.
func TestLocalDataPlane_VersionHasNoDocuments(t *testing.T) {
	empty := stratinternalsync.ComputeDocIDSetHash(nil)

	cases := []struct {
		name    string
		digest  VersionDigest
		want    bool
		wantErr bool
	}{
		{
			name: "empty document set",
			digest: versionDigestFunc(func(context.Context, string, int64) (string, error) {
				return empty, nil
			}),
			want: true,
		},
		{
			name: "non-empty document set",
			digest: versionDigestFunc(func(context.Context, string, int64) (string, error) {
				return "not-the-empty-set-hash", nil
			}),
			want: false,
		},
		{name: "no digest source", digest: nil, want: false},
		{
			name: "digest error",
			digest: versionDigestFunc(func(context.Context, string, int64) (string, error) {
				return "", errors.New("boom")
			}),
			want:    false,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dp := NewLocalDataPlane(LocalDataPlaneConfig{Digest: tc.digest})
			got, err := dp.versionHasNoDocuments(context.Background(), "kb-1", 7)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("versionHasNoDocuments = %v, want %v", got, tc.want)
			}
		})
	}
}
