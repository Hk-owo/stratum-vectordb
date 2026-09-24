package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// H1 of docs/code-review-2026-09-24.md: kbID becomes a directory that this node
// REMOVES. filepath.Join cleans ".." before handing the path to os.RemoveAll, so
// an id that is not a single path component does not fail — it deletes somewhere
// else entirely. The name-derived ids that made this reachable are gone (the
// control layer now mints an opaque handle), and this is the second line of
// defence: the check lives next to the RemoveAll.
func TestDeleteFilesByKB_RefusesAKBIDThatWouldEscapeTheDataDir(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("prepare IndexDataDir: %v", err)
	}
	victim := filepath.Join(root, "victim")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatalf("prepare the directory outside IndexDataDir: %v", err)
	}
	keep := filepath.Join(victim, "keep.txt")
	if err := os.WriteFile(keep, []byte("still here"), 0o644); err != nil {
		t.Fatalf("prepare victim file: %v", err)
	}

	vc := newMockVectorIndexClient()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: time.Second,
		IndexDataDir:    dataDir,
	})

	for _, kbID := range []string{"../victim", "..", ".", "", "/tmp/somewhere-else"} {
		if err := im.DeleteFilesByKB(context.Background(), kbID); err == nil {
			t.Errorf("DeleteFilesByKB(%q) must be refused, not resolved", kbID)
		}
	}

	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("the directory outside IndexDataDir must be untouched: %v", err)
	}
}

// The retention sweeper deletes files under the same derived directory, so it
// gets the same guard.
func TestEnforceDiskRetention_RefusesAKBIDThatWouldEscapeTheDataDir(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("prepare IndexDataDir: %v", err)
	}
	victim := filepath.Join(root, "victim")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatalf("prepare the directory outside IndexDataDir: %v", err)
	}
	keep := filepath.Join(victim, "9.index")
	if err := os.WriteFile(keep, []byte("index bytes"), 0o644); err != nil {
		t.Fatalf("prepare victim file: %v", err)
	}

	vc := newMockVectorIndexClient()
	im := newColdPolicyManager(t, vc, newDocSource(), IndexManagerConfig{
		LRUCapacity:         4,
		LoadWaitTimeout:     time.Second,
		IndexDataDir:        dataDir,
		IndexRetentionCount: 1,
	})

	if err := im.EnforceDiskRetention(context.Background(), "../victim", nil); err == nil {
		t.Fatal("EnforceDiskRetention must refuse a kb_id that leaves IndexDataDir")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("the file outside IndexDataDir must be untouched: %v", err)
	}
}
