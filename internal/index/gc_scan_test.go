package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

// gcChunkID builds a chunk id of the shape the sidecar records: SHA-256 in hex.
func gcChunkID(n int) string { return fmt.Sprintf("%064x", n) }

// gcArtifact writes a sealed-looking pair for (kbID, versionID): the .index file
// (existence is all the scanner checks) and a sidecar shaped like the C++ writer's
// — a small header followed by one chunk id per line.
func gcArtifact(t *testing.T, dataDir, kbID string, versionID int64, header []string, chunkIDs []string) {
	t.Helper()
	dir := filepath.Join(dataDir, "index", kbID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.index", versionID)), []byte("index"), 0o644); err != nil {
		t.Fatal(err)
	}
	lines := append(append([]string{}, header...), chunkIDs...)
	body := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.index.ids", versionID)), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// gcHeader is the sidecar's current header, verbatim in shape (magic, dim,
// metric, checksum). The scanner must not depend on its length.
func gcHeader() []string {
	return []string{"stratum-index-ids-v1", "768", "1", "0xdeadbeef"}
}

// newGCManager wires a manager whose document lookups are the supplied maps.
func newGCManager(t *testing.T, dataDir string, liveChunksByVersion map[int64][]string, active map[string]int64) *IndexManagerImpl {
	t.Helper()
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: time.Second,
		IndexDataDir:    dataDir,
	})
	im.logger = zap.NewNop()
	im.SetBuildDataSources(
		func(_ context.Context, _ string, versionID int64) ([]string, error) {
			if _, ok := liveChunksByVersion[versionID]; !ok {
				return nil, nil
			}
			// One document per live chunk is enough: the scan counts DISTINCT
			// chunks, not documents.
			docs := make([]string, 0, len(liveChunksByVersion[versionID]))
			for i := range liveChunksByVersion[versionID] {
				docs = append(docs, fmt.Sprintf("doc-%d", i))
			}
			return docs, nil
		},
		func(_ context.Context, _ string, _ []string) ([]string, error) {
			// Any doc id resolves to the version's live chunk set; the mapping
			// itself is not what this test is about.
			for _, chunks := range liveChunksByVersion {
				return chunks, nil
			}
			return nil, nil
		},
		nil,
	)
	if active != nil {
		im.SetActiveVersionsProvider(func(context.Context) (map[string]int64, error) { return active, nil })
	}
	return im
}

// TestGCScan_ReportsAnActiveVersionCarryingDeadWeight is the §8.6(d) phase-1
// job: an ACTIVE version whose artifact holds more vectors than its current
// documents justify gets reported. This is the version the append path can never
// reclaim — a version with no successor never attempts an append, so nothing has
// ever measured its dead share.
func TestGCScan_ReportsAnActiveVersionCarryingDeadWeight(t *testing.T) {
	dir := t.TempDir()
	// 10 vectors in the artifact, 5 of them still justified by live documents.
	all := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		all = append(all, gcChunkID(i))
	}
	gcArtifact(t, dir, "kb-1", 7, gcHeader(), all)

	im := newGCManager(t, dir, map[int64][]string{7: all[:5]}, map[string]int64{"kb-1": 7})

	got := im.scanGCCandidates(context.Background())
	if len(got) != 1 {
		t.Fatalf("candidates = %v, want exactly one (kb-1 v7)", got)
	}
	if got[0].KBID != "kb-1" || got[0].VersionID != 7 {
		t.Fatalf("candidate = %+v, want kb-1 v7", got[0])
	}
	if want := 0.5; got[0].DeadShare != want {
		t.Fatalf("dead share = %v, want %v (5 of 10 vectors are no longer justified)", got[0].DeadShare, want)
	}
}

// TestGCScan_LeavesVersionsAtOrBelowTheThreshold: the threshold is what keeps the
// scan from proposing to take a version out of service over a little dead weight.
func TestGCScan_LeavesVersionsAtOrBelowTheThreshold(t *testing.T) {
	dir := t.TempDir()
	all := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		all = append(all, gcChunkID(i))
	}
	gcArtifact(t, dir, "kb-1", 7, gcHeader(), all)

	// 9 of 10 still live: 0.1 dead, below the default 0.2.
	im := newGCManager(t, dir, map[int64][]string{7: all[:9]}, map[string]int64{"kb-1": 7})

	if got := im.scanGCCandidates(context.Background()); len(got) != 0 {
		t.Fatalf("candidates = %v, want none (dead share is below the threshold)", got)
	}
}

// TestGCScan_CoversOnlyActiveVersions: a version WITH a successor is the append
// path's business (the next append's dead-ratio check rebuilds it), and a
// historical version is queried too rarely to be worth a service interruption.
func TestGCScan_CoversOnlyActiveVersions(t *testing.T) {
	dir := t.TempDir()
	dead := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		dead = append(dead, gcChunkID(i))
	}
	// The older version is entirely dead weight, and must still be ignored.
	gcArtifact(t, dir, "kb-1", 6, gcHeader(), dead)
	gcArtifact(t, dir, "kb-1", 7, gcHeader(), dead)

	im := newGCManager(t, dir,
		map[int64][]string{6: nil, 7: dead[:1]}, // v6: nothing live; v7: 90% dead
		map[string]int64{"kb-1": 7},             // only v7 is active
	)

	got := im.scanGCCandidates(context.Background())
	if len(got) != 1 || got[0].VersionID != 7 {
		t.Fatalf("candidates = %v, want only the active version 7", got)
	}
}

// TestGCScan_SkipsUnsealedArtifacts: an <v>.index with no sidecar is a Save that
// died between its two renames — §8.8's sweep owns it, and it has no recorded
// vector count to reason about.
func TestGCScan_SkipsUnsealedArtifacts(t *testing.T) {
	dir := t.TempDir()
	kbDir := filepath.Join(dir, "index", "kb-1")
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(kbDir, "7.index"), []byte("index"), 0o644); err != nil {
		t.Fatal(err)
	}

	im := newGCManager(t, dir, map[int64][]string{7: nil}, map[string]int64{"kb-1": 7})

	if got := im.scanGCCandidates(context.Background()); len(got) != 0 {
		t.Fatalf("candidates = %v, want none (an unsealed artifact is §8.8's business)", got)
	}
}

// TestGCScan_IsANoOpWithoutAProviderOrPersistence: the scanner is off unless
// something can tell it what the active versions are, and there is nothing to
// scan without a data directory.
func TestGCScan_IsANoOpWithoutAProviderOrPersistence(t *testing.T) {
	dir := t.TempDir()
	all := []string{gcChunkID(1), gcChunkID(2)}
	gcArtifact(t, dir, "kb-1", 7, gcHeader(), all)

	// No provider: nothing to scan, even though the artifact is right there.
	noProvider := newGCManager(t, dir, map[int64][]string{7: nil}, nil)
	if got := noProvider.scanGCCandidates(context.Background()); len(got) != 0 {
		t.Fatalf("candidates = %v, want none without an active-version provider", got)
	}

	// No data dir: no artifact to look at.
	noDir := newGCManager(t, "", map[int64][]string{7: nil}, map[string]int64{"kb-1": 7})
	if got := noDir.scanGCCandidates(context.Background()); len(got) != 0 {
		t.Fatalf("candidates = %v, want none without persistence", got)
	}
}

// TestGCScan_StartStop: a negative interval disables the scanner, and starting
// it twice leaves one running.
func TestGCScan_StartStop(t *testing.T) {
	dir := t.TempDir()
	im := newGCManager(t, dir, map[int64][]string{}, map[string]int64{})

	disabled := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: time.Second,
		IndexDataDir:    dir,
		GCSweepInterval: -1,
	})
	disabled.logger = zap.NewNop()
	disabled.SetActiveVersionsProvider(func(context.Context) (map[string]int64, error) { return nil, nil })
	disabled.StartGCScanner()
	disabled.mu.Lock()
	running := disabled.gcCancel != nil
	disabled.mu.Unlock()
	if running {
		t.Error("a negative interval must leave the scanner off")
	}
	disabled.Close()

	im.StartGCScanner()
	im.StartGCScanner() // idempotent
	im.mu.Lock()
	running = im.gcCancel != nil
	im.mu.Unlock()
	if !running {
		t.Fatal("the scanner must be running after StartGCScanner")
	}
	im.StopGCScanner()
	im.mu.Lock()
	running = im.gcCancel != nil
	im.mu.Unlock()
	if running {
		t.Error("StopGCScanner must leave the scanner off")
	}
	im.Close()
}

// TestLooksLikeChunkID pins the one piece of the C++ sidecar format this code
// depends on: chunk ids are SHA-256 hex. Recognising them (rather than skipping a
// fixed header length) is what keeps the count correct when the header grows a
// field — which it already did once.
func TestLooksLikeChunkID(t *testing.T) {
	cases := []struct {
		line string
		want bool
	}{
		{gcChunkID(42), true},
		{"stratum-index-ids-v1", false}, // header: magic
		{"768", false},                  // header: dimension
		{"3", false},                    // header: metric
		{"0xdeadbeef", false},           // header: checksum
		// Uppercase hex must not count. Built from a value with LETTERS in it:
		// "0000…0001" is all digits, so uppercasing it proves nothing.
		{strings.ToUpper(gcChunkID(0xabc)), false},
		{gcChunkID(1)[:63], false}, // too short
		{"", false},
	}
	for _, tc := range cases {
		if got := looksLikeChunkID(tc.line); got != tc.want {
			t.Errorf("looksLikeChunkID(%q) = %v, want %v", tc.line, got, tc.want)
		}
	}
}

// TestGCScan_ReportsAChainTailThatNothingServes: the second target source. On a
// knowledge base that was never rolled back the active map is empty, so without the
// tail the scan examines nothing at all — and the tail is exactly the version the
// append path can never reclaim (there is no successor to trigger a rebuild).
func TestGCScan_ReportsAChainTailThatNothingServes(t *testing.T) {
	dir := t.TempDir()
	all := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		all = append(all, gcChunkID(i))
	}
	gcArtifact(t, dir, "kb-1", 7, gcHeader(), all)

	// Nothing is active: the tail is the only thing that can name this version.
	im := newGCManager(t, dir, map[int64][]string{7: all[:5]}, nil)
	im.SetChainTailVersionsProvider(func(context.Context) (map[string]int64, error) {
		return map[string]int64{"kb-1": 7}, nil
	})

	got := im.scanGCCandidates(context.Background())
	if len(got) != 1 || got[0].VersionID != 7 {
		t.Fatalf("candidates = %v, want exactly kb-1 v7 from the chain tail", got)
	}
	if want := 0.5; got[0].DeadShare != want {
		t.Fatalf("dead share = %v, want %v", got[0].DeadShare, want)
	}
}

// TestGCScan_ExaminesAVersionNamedByBothSourcesOnce: the served version that is also
// the tail is the ordinary case (a fresh write, nothing rolled back yet), and
// examining it twice would spend a second document scan and count it twice in the
// rejection tallies that exist to explain an empty result.
func TestGCScan_ExaminesAVersionNamedByBothSourcesOnce(t *testing.T) {
	dir := t.TempDir()
	all := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		all = append(all, gcChunkID(i))
	}
	gcArtifact(t, dir, "kb-1", 7, gcHeader(), all)

	im := newGCManager(t, dir, map[int64][]string{7: all[:5]}, map[string]int64{"kb-1": 7})
	im.SetChainTailVersionsProvider(func(context.Context) (map[string]int64, error) {
		return map[string]int64{"kb-1": 7}, nil
	})

	got := im.scanGCCandidates(context.Background())
	if len(got) != 1 {
		t.Fatalf("candidates = %v, want one: the two sources named the same version", got)
	}
}
