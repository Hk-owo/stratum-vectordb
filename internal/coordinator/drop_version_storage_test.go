package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"stratum/internal/bloom"
)

// discardingIndexMgr records Discard calls while inheriting every other
// IndexManager method from the shared test double.
type discardingIndexMgr struct {
	*testIndexManager
	mu        sync.Mutex
	discarded []indexManagerKey
}

func newDiscardingIndexMgr() *discardingIndexMgr {
	return &discardingIndexMgr{testIndexManager: newTestIndexManager()}
}

func (m *discardingIndexMgr) Discard(_ context.Context, kbID string, versionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.discarded = append(m.discarded, indexManagerKey{kbID, versionID})
	return nil
}

func (m *discardingIndexMgr) discardedVersion(kbID string, versionID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range m.discarded {
		if k == (indexManagerKey{kbID, versionID}) {
			return true
		}
	}
	return false
}

func (m *discardingIndexMgr) discardCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.discarded)
}

// TestWriteCoordinatorImpl_DropVersionStorage_ReclaimsIndexArtifacts pins
// §10.6(4): once a version is declared FAILED_PERMANENT its physical data is
// reclaimed, and that includes its index artifacts — not just the docstore rows.
// A surviving index file would never be queryable (visibility is driven by the
// control layer's state, not by what is on disk) and never be rebuilt (the
// verdict is terminal), so it would only linger and skew the disk retention
// window.
func TestWriteCoordinatorImpl_DropVersionStorage_ReclaimsIndexArtifacts(t *testing.T) {
	ctx := context.Background()
	ds := newTestDocStore()
	vd := newTestVersionDocList()
	idx := newDiscardingIndexMgr()

	const kbID = "kb-1"
	const versionID int64 = 7
	if err := ds.Write(ctx, kbID, "doc-1", versionID, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := vd.Write(ctx, kbID, versionID, "doc-1"); err != nil {
		t.Fatal(err)
	}

	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		DocStore:       ds,
		VersionDocList: vd,
		IndexManager:   idx,
	})

	if err := c.DropVersionStorage(ctx, kbID, versionID); err != nil {
		t.Fatalf("DropVersionStorage: %v", err)
	}
	if !idx.discardedVersion(kbID, versionID) {
		t.Error("the version's index artifacts must be discarded together with its data (§10.6(4))")
	}

	// Idempotent: a retried cleanup (the cleanup queue retries, and every
	// candidate replica runs it) must not fail.
	if err := c.DropVersionStorage(ctx, kbID, versionID); err != nil {
		t.Fatalf("DropVersionStorage (retry): %v", err)
	}
	if got := idx.discardCalls(); got != 2 {
		t.Errorf("Discard calls = %d, want 2 (one per cleanup attempt, both idempotent)", got)
	}
}

// A node that never wired an index manager must still be able to reclaim what it
// can: the docstore rows and the version document list. Skipping the index step
// is a degradation, not a failure.
func TestWriteCoordinatorImpl_DropVersionStorage_WithoutIndexManager(t *testing.T) {
	ctx := context.Background()
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		DocStore:       newTestDocStore(),
		VersionDocList: newTestVersionDocList(),
	})

	if err := c.DropVersionStorage(ctx, "kb-1", 7); err != nil {
		t.Fatalf("DropVersionStorage without an index manager: %v", err)
	}
}

// The per-version reclaim must take the version's bloom filter too. It used to be the
// one layer this path skipped while the DeleteVersion flow's LocalVersionDropper always
// removed it — so a version retired by a terminal verdict left a filter on disk for
// good: never queryable (the verdict is terminal), never rebuilt, and invisible to the
// reverse reconciliation (which looks for versions whose metadata is GONE, while this
// one is still in the metadata, marked terminal).
func TestWriteCoordinatorImpl_DropVersionStorage_ReclaimsTheVersionBloom(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	vd := newTestVersionDocList()
	blooms := bloom.NewVersionBloomStore(dir, 100, 0.01, vd)

	const kbID = "kb-1"
	const versionID int64 = 7
	if _, err := blooms.BuildAndPersist(kbID, versionID, []string{"doc-1", "doc-2"}); err != nil {
		t.Fatalf("BuildAndPersist: %v", err)
	}
	if n := countVersionBloomFiles(t, dir, kbID); n != 1 {
		t.Fatalf("bloom files before the reclaim = %d, want 1", n)
	}

	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		DocStore:       newTestDocStore(),
		VersionDocList: vd,
		IndexManager:   newDiscardingIndexMgr(),
		VersionBloom:   blooms,
	})
	if err := c.DropVersionStorage(ctx, kbID, versionID); err != nil {
		t.Fatalf("DropVersionStorage: %v", err)
	}
	if n := countVersionBloomFiles(t, dir, kbID); n != 0 {
		t.Errorf("bloom files after the reclaim = %d, want 0", n)
	}

	// Idempotent like every other layer: the cleanup queue retries, and every candidate
	// replica runs this.
	if err := c.DropVersionStorage(ctx, kbID, versionID); err != nil {
		t.Fatalf("DropVersionStorage (retry): %v", err)
	}
}

// A node that wired no filter store must still reclaim what it can; skipping this step
// is a degradation, not a failure.
func TestWriteCoordinatorImpl_DropVersionStorage_WithoutVersionBloom(t *testing.T) {
	ctx := context.Background()
	c := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
		DocStore:       newTestDocStore(),
		VersionDocList: newTestVersionDocList(),
		IndexManager:   newDiscardingIndexMgr(),
	})
	if err := c.DropVersionStorage(ctx, "kb-1", 7); err != nil {
		t.Fatalf("DropVersionStorage without a bloom store: %v", err)
	}
}

func countVersionBloomFiles(t *testing.T, dir, kbID string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "bloom-version", kbID))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read the version-bloom dir: %v", err)
	}
	return len(entries)
}
