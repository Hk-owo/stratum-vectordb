package versiondoc

import (
	"context"
	"testing"
)

// WriteMany is how the write path records a version's WHOLE document-ID set now:
// one durable commit instead of one per document. The old shape cost 0.42 ms per
// document — 423 ms of a 1,000-document version's 1.44 s storage write — and every
// replica paid it, because every replica runs the same transaction.
//
// Both implementations are exercised together, for the same reason
// list_versions_test.go does it: the write path is written against the interface,
// and a divergence between the two is a bug in whichever one the test did not run.
//
// 撤掉批量即变红: with WriteMany implemented as a no-op (or as one Write for the
// first id only), the read-backs below come back short.
func TestWriteMany_RecordsTheWholeSet(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		open func(t *testing.T) VersionDocList
	}{
		{"pebble", func(t *testing.T) VersionDocList {
			l, err := NewPebbleVersionDocList(t.TempDir())
			if err != nil {
				t.Fatalf("NewPebbleVersionDocList: %v", err)
			}
			t.Cleanup(func() { _ = l.Close() })
			return l
		}},
		{"mock", func(t *testing.T) VersionDocList { return NewMockVersionDocList() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := tc.open(t)

			// An empty batch is a no-op, not an error: a version whose document set
			// is empty still goes through this path.
			if err := l.WriteMany(ctx, "kb-1", 1, nil); err != nil {
				t.Fatalf("WriteMany(empty): %v", err)
			}
			if got, err := l.ListDocIDs(ctx, "kb-1", 1); err != nil || len(got) != 0 {
				t.Fatalf("an empty batch wrote (%v, %v), want nothing", got, err)
			}

			// A whole set arrives at once and reads back whole.
			want := []string{"d1", "d2", "d3"}
			if err := l.WriteMany(ctx, "kb-1", 1, want); err != nil {
				t.Fatalf("WriteMany: %v", err)
			}
			got, err := l.ListDocIDs(ctx, "kb-1", 1)
			if err != nil {
				t.Fatalf("ListDocIDs: %v", err)
			}
			assertSetEqualVDL(t, got, want)

			// Version scoping holds: a neighbouring version must not be touched.
			if err := l.WriteMany(ctx, "kb-1", 2, []string{"d9"}); err != nil {
				t.Fatalf("WriteMany(v2): %v", err)
			}
			got, _ = l.ListDocIDs(ctx, "kb-1", 1)
			assertSetEqualVDL(t, got, want)
			if other, _ := l.ListDocIDs(ctx, "kb-1", 2); len(other) != 1 || other[0] != "d9" {
				t.Fatalf("version 2 = %v, want [d9]", other)
			}

			// Idempotent: the same batch again changes nothing. That is what makes
			// re-running a write transaction safe after a partial failure.
			if err := l.WriteMany(ctx, "kb-1", 1, want); err != nil {
				t.Fatalf("WriteMany(repeat): %v", err)
			}
			got, _ = l.ListDocIDs(ctx, "kb-1", 1)
			assertSetEqualVDL(t, got, want)

			// And it composes with the single-document Write the replay path still
			// uses (internal/sync replays WAL entries one at a time).
			if err := l.Write(ctx, "kb-1", 1, "d4"); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got, _ = l.ListDocIDs(ctx, "kb-1", 1)
			assertSetEqualVDL(t, got, []string{"d1", "d2", "d3", "d4"})
		})
	}
}
