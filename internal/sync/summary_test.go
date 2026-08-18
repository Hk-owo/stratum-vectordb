package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"stratum/internal/versiondoc"
)

// TestComputeDocIDSetHash_Deterministic verifies the digest is stable
// across calls for the same set.
func TestComputeDocIDSetHash_Deterministic(t *testing.T) {
	ids := []string{"doc-a", "doc-b", "doc-c"}
	a := ComputeDocIDSetHash(ids)
	b := ComputeDocIDSetHash(ids)
	if a != b || a == "" {
		t.Fatalf("digest not deterministic: %q vs %q", a, b)
	}
}

// TestComputeDocIDSetHash_OrderIndependent verifies the digest does not
// depend on insertion order (leader and follower may observe different
// orders from their stores).
func TestComputeDocIDSetHash_OrderIndependent(t *testing.T) {
	a := ComputeDocIDSetHash([]string{"doc-a", "doc-b", "doc-c"})
	b := ComputeDocIDSetHash([]string{"doc-c", "doc-a", "doc-b"})
	c := ComputeDocIDSetHash([]string{"doc-b", "doc-c", "doc-a"})
	if a != b || b != c {
		t.Fatalf("digest depends on insertion order: %q / %q / %q", a, b, c)
	}
}

// TestComputeDocIDSetHash_DistinctSets verifies different sets produce
// different digests (and superset/subset are not conflated).
func TestComputeDocIDSetHash_DistinctSets(t *testing.T) {
	base := ComputeDocIDSetHash([]string{"doc-a", "doc-b"})
	oneMore := ComputeDocIDSetHash([]string{"doc-a", "doc-b", "doc-c"})
	different := ComputeDocIDSetHash([]string{"doc-a", "doc-b2"})
	if base == oneMore {
		t.Error("subset produced the same digest as its superset")
	}
	if base == different {
		t.Error("different sets produced the same digest")
	}
}

// TestComputeDocIDSetHash_EmptySet verifies the empty set has a defined
// digest (SHA-256 of empty input), distinct from "no digest committed".
func TestComputeDocIDSetHash_EmptySet(t *testing.T) {
	empty := ComputeDocIDSetHash(nil)
	if empty == "" {
		t.Fatal("empty set must have a defined digest, not '' ('' means no digest committed)")
	}
	if ComputeDocIDSetHash(nil) != ComputeDocIDSetHash([]string{}) {
		t.Error("nil and empty slice must hash identically")
	}
}

// TestComputeDocIDSetHash_CrossChecked verifies the digest against an
// independent reference implementation (guards against refactor drift).
func TestComputeDocIDSetHash_CrossChecked(t *testing.T) {
	for _, ids := range [][]string{
		{},
		{"a"},
		{"b", "a"},
		{"doc-1", "doc-2", "doc-3"},
		{"doc-3", "doc-1", "doc-2"},
	} {
		if got, want := ComputeDocIDSetHash(ids), referenceDocIDSetHash(t, ids); got != want {
			t.Errorf("ComputeDocIDSetHash(%v) = %q, reference = %q", ids, got, want)
		}
	}
}

// VerifyDocIDSet tests

func TestVerifyDocIDSet_Matches(t *testing.T) {
	vd := versiondoc.NewMockVersionDocList()
	ctx := context.Background()
	for _, id := range []string{"doc-a", "doc-b"} {
		if err := vd.Write(ctx, "kb1", 3, id); err != nil {
			t.Fatal(err)
		}
	}
	expected := ComputeDocIDSetHash([]string{"doc-b", "doc-a"}) // different order on purpose

	ok, got, err := VerifyDocIDSet(ctx, vd, "kb1", 3, expected)
	if err != nil {
		t.Fatalf("VerifyDocIDSet: %v", err)
	}
	if !ok || got != expected {
		t.Errorf("verified=%v got=%q want=%q", ok, got, expected)
	}
}

func TestVerifyDocIDSet_Mismatch(t *testing.T) {
	vd := versiondoc.NewMockVersionDocList()
	ctx := context.Background()
	if err := vd.Write(ctx, "kb1", 3, "doc-a"); err != nil {
		t.Fatal(err)
	}

	// Expected digest for a DIFFERENT set (leader has doc-a + doc-b).
	expected := ComputeDocIDSetHash([]string{"doc-a", "doc-b"})
	ok, _, err := VerifyDocIDSet(ctx, vd, "kb1", 3, expected)
	if err != nil {
		t.Fatalf("VerifyDocIDSet: %v", err)
	}
	if ok {
		t.Error("verification passed for an incomplete local set")
	}
}

func TestVerifyDocIDSet_EmptyExpectedSkips(t *testing.T) {
	vd := versiondoc.NewMockVersionDocList()
	ctx := context.Background()
	if err := vd.Write(ctx, "kb1", 3, "doc-a"); err != nil {
		t.Fatal(err)
	}

	// No committed digest (""): unverifiable, must not error and must not
	// report verified — the caller decides the fallback.
	ok, got, err := VerifyDocIDSet(ctx, vd, "kb1", 3, "")
	if err != nil {
		t.Fatalf("VerifyDocIDSet with empty expected: %v", err)
	}
	if ok || got != "" {
		t.Errorf("empty expected: verified=%v got=%q, want (false, \"\")", ok, got)
	}
}

func TestVerifyDocIDSet_StoreError(t *testing.T) {
	vd := &failingVersionDocList{}
	_, _, err := VerifyDocIDSet(context.Background(), vd, "kb1", 3, ComputeDocIDSetHash([]string{"x"}))
	if err == nil {
		t.Fatal("expected error when the local store read fails")
	}
}

// failingVersionDocList is a VersionDocList whose ListDocIDs always fails.
type failingVersionDocList struct{}

func (f *failingVersionDocList) Write(context.Context, string, int64, string) error { return nil }
func (f *failingVersionDocList) ListDocIDs(context.Context, string, int64) ([]string, error) {
	return nil, errors.New("store down")
}
func (f *failingVersionDocList) DeleteByVersion(context.Context, string, int64) error { return nil }
func (f *failingVersionDocList) DeleteByKB(context.Context, string) error             { return nil }

// referenceDocIDSetHash is a straightforward reference implementation used
// to cross-check ComputeDocIDSetHash (guards against refactor drift).
func referenceDocIDSetHash(t *testing.T, ids []string) string {
	t.Helper()
	sorted := append([]string(nil), ids...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	var b strings.Builder
	for i, id := range sorted {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(id)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
