package versiondoc

import (
	"context"
	"testing"
)

// TestListVersions_EnumeratesTheLocalVersions pins the read §B's reconciliation
// needs: the version ids this store actually holds, ascending and deduplicated, a
// version vanishing the moment its last document does, and knowledge bases kept
// apart. Both implementations must answer identically — the reconciler is written
// against the interface.
func TestListVersions_EnumeratesTheLocalVersions(t *testing.T) {
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

			// Nothing held: empty, and not an error. (This store cannot tell "no such
			// knowledge base" from "no versions yet" — both mean "nothing here", and
			// the metadata is what knows the difference.)
			if got, err := l.ListVersions(ctx, "kb-1"); err != nil || len(got) != 0 {
				t.Fatalf("ListVersions(empty) = (%v, %v), want empty", got, err)
			}

			// Several documents per version (they must collapse to one id), a gap in
			// the id space, and a second knowledge base that must not leak across.
			for _, w := range []struct {
				kb   string
				v    int64
				docs []string
			}{
				{"kb-1", 1, []string{"d1", "d2", "d3"}},
				{"kb-1", 7, []string{"d1"}},
				{"kb-2", 3, []string{"d1"}},
			} {
				for _, doc := range w.docs {
					if err := l.Write(ctx, w.kb, w.v, doc); err != nil {
						t.Fatalf("Write(%s,%d,%s): %v", w.kb, w.v, doc, err)
					}
				}
			}

			got, err := l.ListVersions(ctx, "kb-1")
			if err != nil {
				t.Fatalf("ListVersions: %v", err)
			}
			if len(got) != 2 || got[0] != 1 || got[1] != 7 {
				t.Fatalf("ListVersions(kb-1) = %v, want [1 7]", got)
			}
			if other, err := l.ListVersions(ctx, "kb-2"); err != nil || len(other) != 1 || other[0] != 3 {
				t.Fatalf("ListVersions(kb-2) = (%v, %v), want [3]", other, err)
			}

			// A version disappears with its last document, and the answer stays sorted.
			if err := l.Write(ctx, "kb-1", 2, "d1"); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := l.DeleteByVersion(ctx, "kb-1", 1); err != nil {
				t.Fatalf("DeleteByVersion: %v", err)
			}
			got, err = l.ListVersions(ctx, "kb-1")
			if err != nil {
				t.Fatalf("ListVersions: %v", err)
			}
			if len(got) != 2 || got[0] != 2 || got[1] != 7 {
				t.Fatalf("after DeleteByVersion(1): ListVersions = %v, want [2 7]", got)
			}
		})
	}
}
