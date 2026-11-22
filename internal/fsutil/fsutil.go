// Package fsutil provides small, zero-business-dependency local filesystem
// primitives shared across Stratum packages: atomic file replacement with full
// data + directory durability, directory fsync, and path-segment traversal
// validation.
//
// It intentionally knows nothing about file formats, encoding, or checksums;
// callers own payload encoding and integrity verification.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type writeOptions struct {
	hasPerm     bool
	perm        os.FileMode
	tempPattern string
	hasMkdirAll bool
	dirPerm     os.FileMode
	syncDir     func(string) error
}

// WriteOption configures AtomicWriteFile.
type WriteOption func(*writeOptions)

// WithPerm sets explicit file permissions on the temporary file via Chmod
// before it is renamed into place, bypassing the process umask.
func WithPerm(perm os.FileMode) WriteOption {
	return func(o *writeOptions) {
		o.hasPerm = true
		o.perm = perm
	}
}

// WithTempPattern overrides the os.CreateTemp pattern used for the sibling
// temporary file (for example ".installing.*" or ".install-*.tmp").
func WithTempPattern(pattern string) WriteOption {
	return func(o *writeOptions) {
		o.tempPattern = pattern
	}
}

// WithMkdirAll ensures parent directories of the target path exist before
// creating the temporary file.
func WithMkdirAll(perm os.FileMode) WriteOption {
	return func(o *writeOptions) {
		o.hasMkdirAll = true
		o.dirPerm = perm
	}
}

// WithSyncDir overrides the directory fsync function used after os.Rename.
// Passing nil leaves the default (SyncDir).
func WithSyncDir(fn func(string) error) WriteOption {
	return func(o *writeOptions) {
		if fn != nil {
			o.syncDir = fn
		}
	}
}

type writeHooks struct {
	writeFile func(*os.File, []byte) (int, error)
	syncFile  func(*os.File) error
	closeFile func(*os.File) error
	rename    func(string, string) error
}

var defaultWriteHooks = writeHooks{
	writeFile: func(f *os.File, b []byte) (int, error) { return f.Write(b) },
	syncFile:  func(f *os.File) error { return f.Sync() },
	closeFile: func(f *os.File) error { return f.Close() },
	rename:    os.Rename,
}

// AtomicWriteFile writes data to path atomically and durably: optional
// MkdirAll, CreateTemp in the target directory, optional Chmod, Write, file
// Sync, Close, Rename, and SyncDir on the target directory. Any failure before
// Rename removes the temporary file automatically.
func AtomicWriteFile(path string, data []byte, opts ...WriteOption) error {
	return atomicWriteFile(path, data, opts, defaultWriteHooks)
}

func atomicWriteFile(path string, data []byte, opts []WriteOption, hooks writeHooks) error {
	if path == "" {
		return fmt.Errorf("fsutil: atomic write: path is required")
	}
	var o writeOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.syncDir == nil {
		o.syncDir = SyncDir
	}
	pattern := o.tempPattern
	if pattern == "" {
		pattern = ".tmp-*"
	}

	dir := filepath.Dir(path)
	if o.hasMkdirAll {
		if err := os.MkdirAll(dir, o.dirPerm); err != nil {
			return fmt.Errorf("fsutil: mkdir %s: %w", dir, err)
		}
	}

	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return fmt.Errorf("fsutil: create temp in %s: %w", dir, err)
	}
	tmp := f.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			_ = hooks.closeFile(f)
		}
		if !renamed {
			_ = os.Remove(tmp)
		}
	}()

	if o.hasPerm {
		if err := f.Chmod(o.perm); err != nil {
			return fmt.Errorf("fsutil: chmod %s: %w", tmp, err)
		}
	}
	if _, err := hooks.writeFile(f, data); err != nil {
		return fmt.Errorf("fsutil: write %s: %w", tmp, err)
	}
	if err := hooks.syncFile(f); err != nil {
		return fmt.Errorf("fsutil: sync %s: %w", tmp, err)
	}
	closed = true
	if err := hooks.closeFile(f); err != nil {
		return fmt.Errorf("fsutil: close %s: %w", tmp, err)
	}
	if err := hooks.rename(tmp, path); err != nil {
		return fmt.Errorf("fsutil: rename %s -> %s: %w", tmp, path, err)
	}
	renamed = true
	if err := o.syncDir(dir); err != nil {
		return fmt.Errorf("fsutil: sync dir %s: %w", dir, err)
	}
	return nil
}

// SyncDir opens dir, fsyncs it so entry additions, renames, and deletions
// survive power loss, and closes the directory handle.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("fsutil: open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("fsutil: sync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("fsutil: close dir %s: %w", dir, err)
	}
	return nil
}

// ValidatePathSegment verifies that segment is a single ordinary path name
// without traversal components, path separators, or NUL bytes.
func ValidatePathSegment(segment string) error {
	if segment == "" || segment == "." || segment == ".." ||
		strings.ContainsRune(segment, '/') ||
		strings.ContainsRune(segment, filepath.Separator) ||
		strings.ContainsRune(segment, '\x00') ||
		filepath.Base(segment) != segment {
		return fmt.Errorf("fsutil: invalid path segment %q", segment)
	}
	return nil
}
