package kvstorage

import (
	"os"
	"path/filepath"
	"testing"

	kvraftpb "stratum/api/proto/kvraft"
)

func TestPersister_SaveAndLoadState(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	entries := []*kvraftpb.Entry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 1, Data: []byte("first")},
		{Index: 2, Term: 1, Data: []byte("second")},
	}
	if err := p.SaveState(3, 7, entries); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	term, voteFor, log, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 3 || voteFor != 7 {
		t.Fatalf("LoadState term=%d voteFor=%d, want term=3 voteFor=7", term, voteFor)
	}
	if len(log) != 3 {
		t.Fatalf("LoadState log len=%d, want 3", len(log))
	}
	if string(log[1].Data) != "first" || string(log[2].Data) != "second" {
		t.Fatalf("LoadState log content mismatch: %+v", log)
	}
}

func TestPersister_LoadState_NoFileYet(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	term, voteFor, log, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState on first run should not error: %v", err)
	}
	if term != 0 || voteFor != 0 || log != nil {
		t.Fatalf("LoadState on first run = (%d,%d,%v), want zero values", term, voteFor, log)
	}
}

func TestPersister_SaveAndLoadSnapshot(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	if err := p.SaveSnapshot([]byte("snapshot-bytes"), 42); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	data, lastIndex, err := p.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if string(data) != "snapshot-bytes" || lastIndex != 42 {
		t.Fatalf("LoadSnapshot = (%q, %d), want (snapshot-bytes, 42)", data, lastIndex)
	}
}

func TestPersister_LoadSnapshot_NoneYet(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	data, lastIndex, err := p.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot on first run should not error: %v", err)
	}
	if data != nil || lastIndex != 0 {
		t.Fatalf("LoadSnapshot on first run = (%v,%d), want (nil,0)", data, lastIndex)
	}
}

func TestPersister_SaveState_OverwritesPreviousState(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	mustSaveState(t, p, 1, 1, []*kvraftpb.Entry{{Index: 0, Term: 0}})
	mustSaveState(t, p, 5, 2, []*kvraftpb.Entry{{Index: 0, Term: 0}, {Index: 1, Term: 5}})

	term, voteFor, log, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 5 || voteFor != 2 || len(log) != 2 {
		t.Fatalf("LoadState after overwrite = (%d,%d,len=%d), want (5,2,2)", term, voteFor, len(log))
	}
}

func TestPersister_AtomicWrite_NoPartialFileOnDisk(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	if err := p.SaveState(1, 1, []*kvraftpb.Entry{{Index: 0, Term: 0}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// The .tmp staging file must not be left behind after a successful
	// save: atomicWrite renames it into place, so only the final path
	// should exist.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("leftover .tmp file after successful save: %s", e.Name())
		}
	}
}

func TestPersister_StateAndSnapshotAreIndependent(t *testing.T) {
	dir := t.TempDir()
	p := NewPersister(filepath.Join(dir, "raft"))

	mustSaveState(t, p, 9, 3, []*kvraftpb.Entry{{Index: 0, Term: 0}})
	if err := p.SaveSnapshot([]byte("snap"), 10); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	term, voteFor, _, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 9 || voteFor != 3 {
		t.Fatalf("LoadState after SaveSnapshot = (%d,%d), want unaffected (9,3)", term, voteFor)
	}

	data, lastIndex, err := p.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if string(data) != "snap" || lastIndex != 10 {
		t.Fatalf("LoadSnapshot after SaveState = (%q,%d), want unaffected (snap,10)", data, lastIndex)
	}
}

func mustSaveState(t *testing.T, p *Persister, term uint64, voteFor int64, log []*kvraftpb.Entry) {
	t.Helper()
	if err := p.SaveState(term, voteFor, log); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
}
