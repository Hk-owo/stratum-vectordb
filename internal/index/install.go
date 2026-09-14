package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// InstallIndex writes an index built elsewhere into this node's index
// directory and loads it, so a replica can serve a version it never built
// (Stratum_设计文档v13.md §8.4: "建一次、分发 N 份").
//
// Both files go in through a temporary name and are renamed into place, the
// same discipline the local build path follows: a concurrent Load — or a crash
// mid-install — must never observe a half-written index. The temporary names
// are per-target so two installs of different versions cannot collide.
//
// The sidecar is written first: it carries the checksum the index file is
// validated against, so an index that lands without it is the one state we
// must not reach.
func (im *IndexManagerImpl) InstallIndex(ctx context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error {
	if im.cfg.IndexDataDir == "" {
		return fmt.Errorf("index: InstallIndex: persistence not configured")
	}
	if len(indexData) == 0 {
		return fmt.Errorf("index: InstallIndex(%s, %d): empty index payload", kbID, versionID)
	}

	dir := filepath.Dir(im.indexPath(kbID, versionID))
	// 0777 for the same reason saveToDisk does it: this node writes here, so
	// the directory has to be usable regardless of which user the writer runs
	// as.
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("index: InstallIndex: mkdir: %w", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		return fmt.Errorf("index: InstallIndex: chmod: %w", err)
	}

	if err := installFile(im.sidecarPath(kbID, versionID), sidecarData); err != nil {
		return err
	}
	if err := installFile(im.indexPath(kbID, versionID), indexData); err != nil {
		return err
	}

	// Load immediately: the data is on disk, and a replica that has to wait for
	// its first query to find it would look like a version that is not ready.
	if err := im.loadFromDisk(ctx, kbID, versionID); err != nil {
		return err
	}
	// §8.6a: receiving an artifact counts as this node's baseline for the
	// version, exactly as building it does. Without this, a version a replica
	// only ever received would never appear in the access table, so the cold
	// evaluator would never look at it and the shipped shape would be permanent
	// there.
	im.mu.Lock()
	im.seedAccessLocked(indexKey{kbID, versionID})
	im.mu.Unlock()
	return nil
}

// installFile atomically replaces path with data: write a temporary sibling,
// fsync it, then rename over the target.
func installFile(path string, data []byte) error {
	tmp := path + ".installing"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return fmt.Errorf("index: InstallIndex: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("index: InstallIndex: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("index: InstallIndex: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("index: InstallIndex: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("index: InstallIndex: rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// sidecarPath returns the on-disk path of (kbID, versionID)'s sidecar — the
// metadata + checksum file the vecstore's Save writes alongside the index, and
// which Load validates the index against.
func (im *IndexManagerImpl) sidecarPath(kbID string, versionID int64) string {
	return filepath.Join(im.cfg.IndexDataDir, "index", kbID, fmt.Sprintf("%d.index.ids", versionID))
}

// ReadIndexFiles returns the raw bytes of (kbID, versionID)'s persisted index
// and sidecar, for shipping them to a replica (§8.4). The caller decides who
// receives them; this side only knows what is on its own disk.
func (im *IndexManagerImpl) ReadIndexFiles(kbID string, versionID int64) (indexData, sidecarData []byte, err error) {
	indexData, err = os.ReadFile(im.indexPath(kbID, versionID))
	if err != nil {
		return nil, nil, fmt.Errorf("index: read %s: %w", im.indexPath(kbID, versionID), err)
	}
	sidecarData, err = os.ReadFile(im.sidecarPath(kbID, versionID))
	if err != nil {
		return nil, nil, fmt.Errorf("index: read %s: %w", im.sidecarPath(kbID, versionID), err)
	}
	return indexData, sidecarData, nil
}
