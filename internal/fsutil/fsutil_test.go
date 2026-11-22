package fsutil

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAtomicWriteFile_CreateAndOverwrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "state.dat")

	first := []byte("initial-payload-1")
	if err := AtomicWriteFile(target, first); err != nil {
		t.Fatalf("initial AtomicWriteFile: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read initial: %v", err)
	}
	if !bytes.Equal(got, first) {
		t.Fatalf("initial content = %q, want %q", got, first)
	}

	second := []byte("replacement-payload-2")
	if err := AtomicWriteFile(target, second); err != nil {
		t.Fatalf("overwrite AtomicWriteFile: %v", err)
	}
	got, err = os.ReadFile(target)
	if err != nil {
		t.Fatalf("read overwritten: %v", err)
	}
	if !bytes.Equal(got, second) {
		t.Fatalf("overwritten content = %q, want %q", got, second)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.dat" {
		t.Fatalf("unexpected directory entries after write: %v", entries)
	}
}

func TestAtomicWriteFile_Options(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "nested", "dir", "artifact.bin")

	var syncedDir string
	payload := []byte("hello-options")
	err := AtomicWriteFile(
		target,
		payload,
		WithMkdirAll(0o755),
		WithPerm(0o666),
		WithTempPattern("artifact.bin.installing.*"),
		WithSyncDir(func(d string) error {
			syncedDir = d
			return SyncDir(d)
		}),
	)
	if err != nil {
		t.Fatalf("AtomicWriteFile with options: %v", err)
	}
	if syncedDir != filepath.Dir(target) {
		t.Fatalf("syncedDir = %q, want %q", syncedDir, filepath.Dir(target))
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if info.Mode().Perm() != 0o666 {
		t.Fatalf("perm = %#o, want 0o666", info.Mode().Perm())
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content = %q, want %q", got, payload)
	}

	// Verify WithTempPattern actually shapes the temp filename before rename.
	patternTarget := filepath.Join(dir, "nested", "dir", "pattern.bin")
	var seenTemp string
	hooks := defaultWriteHooks
	hooks.rename = func(oldpath, newpath string) error {
		seenTemp = filepath.Base(oldpath)
		return os.Rename(oldpath, newpath)
	}
	if err := atomicWriteFile(patternTarget, []byte("x"), []WriteOption{WithTempPattern(".install-*.tmp")}, hooks); err != nil {
		t.Fatalf("atomicWriteFile pattern check: %v", err)
	}
	if !strings.HasPrefix(seenTemp, ".install-") || !strings.HasSuffix(seenTemp, ".tmp") {
		t.Fatalf("temp filename = %q, want .install-*.tmp", seenTemp)
	}
}

func TestAtomicWriteFile_CleansUpTempAndPreservesExistingOnFailure(t *testing.T) {
	injected := errors.New("injected failure")
	orig := []byte("original-intact-content")

	cases := []struct {
		name  string
		opts  []WriteOption
		hooks func() writeHooks
	}{
		{
			name: "write fails",
			hooks: func() writeHooks {
				h := defaultWriteHooks
				h.writeFile = func(*os.File, []byte) (int, error) { return 0, injected }
				return h
			},
		},
		{
			name: "sync fails",
			hooks: func() writeHooks {
				h := defaultWriteHooks
				h.syncFile = func(*os.File) error { return injected }
				return h
			},
		},
		{
			name: "close fails",
			hooks: func() writeHooks {
				h := defaultWriteHooks
				h.closeFile = func(f *os.File) error {
					_ = f.Close()
					return injected
				}
				return h
			},
		},
		{
			name: "rename fails",
			hooks: func() writeHooks {
				h := defaultWriteHooks
				h.rename = func(string, string) error { return injected }
				return h
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "target.json")
			if err := os.WriteFile(target, orig, 0o600); err != nil {
				t.Fatal(err)
			}
			err := atomicWriteFile(target, []byte("new-bad-content"), tc.opts, tc.hooks())
			if !errors.Is(err, injected) {
				t.Fatalf("err = %v, want %v", err, injected)
			}
			got, rerr := os.ReadFile(target)
			if rerr != nil {
				t.Fatalf("read target after failure: %v", rerr)
			}
			if !bytes.Equal(got, orig) {
				t.Fatalf("target mutated on failure: got %q, want %q", got, orig)
			}
			entries, derr := os.ReadDir(dir)
			if derr != nil {
				t.Fatal(derr)
			}
			if len(entries) != 1 || entries[0].Name() != "target.json" {
				t.Fatalf("temp file leaked in %s: %v", dir, entries)
			}
		})
	}

	t.Run("syncDir fails", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target.json")
		err := AtomicWriteFile(target, []byte("payload"), WithSyncDir(func(string) error { return injected }))
		if !errors.Is(err, injected) {
			t.Fatalf("err = %v, want %v", err, injected)
		}
		entries, derr := os.ReadDir(dir)
		if derr != nil {
			t.Fatal(derr)
		}
		if len(entries) != 1 || entries[0].Name() != "target.json" {
			t.Fatalf("temp file leaked after syncDir failure: %v", entries)
		}
	})

	t.Run("missing dir without WithMkdirAll", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "missing", "file.txt")
		if err := AtomicWriteFile(target, []byte("x")); err == nil {
			t.Fatal("expected error for missing parent directory")
		}
	})

	t.Run("empty path", func(t *testing.T) {
		if err := AtomicWriteFile("", []byte("x")); err == nil {
			t.Fatal("expected error for empty path")
		}
	})

	t.Run("target is non-empty directory", func(t *testing.T) {
		dir := t.TempDir()
		targetDir := filepath.Join(dir, "blocking")
		if err := os.MkdirAll(filepath.Join(targetDir, "keep"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := AtomicWriteFile(targetDir, []byte("x")); err == nil {
			t.Fatal("expected error renaming over non-empty directory")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != "blocking" {
			t.Fatalf("temp file leaked after rename failure: %v", entries)
		}
	})
}

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir(existing): %v", err)
	}
	if err := SyncDir(filepath.Join(dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SyncDir(missing) = %v, want os.ErrNotExist", err)
	}
}

func TestValidatePathSegment(t *testing.T) {
	invalid := []string{
		"",
		".",
		"..",
		"a/b",
		"/a",
		"a/",
		"../victim",
		"a\x00b",
		"\x00",
	}
	for _, s := range invalid {
		if err := ValidatePathSegment(s); err == nil {
			t.Errorf("ValidatePathSegment(%q) = nil, want error", s)
		}
	}

	valid := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"kb-1",
		"kb_alpha_02",
		"A",
		"v1.2.3",
	}
	for _, s := range valid {
		if err := ValidatePathSegment(s); err != nil {
			t.Errorf("ValidatePathSegment(%q) = %v, want nil", s, err)
		}
	}
}
