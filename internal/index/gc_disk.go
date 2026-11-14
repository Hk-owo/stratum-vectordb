// Disk-side collection (§5.7): abandoned build residue, dead chunks inside an
// artifact, orphaned KB directories, and the retention window.
package index

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sort"
	vecstorepb "stratum/api/proto/vecstore"
	"strconv"
	"strings"
	"time"
)

// StartAbandonSweeper reclaims index artifacts whose build was abandoned
// (coordinator-selection-and-node-liveness-design.md §6).
//
// The gap it closes: a build that dies or times out leaves a half-written
// artifact behind, and nothing will ever come back for it. The control layer
// moves on to the next candidate, that candidate succeeds, the version reaches
// READY — so it never reaches FAILED_PERMANENT either, and §1.4's cleanup
// broadcast never fires. The remains sit there for good. This is the everyday
// case (an ordinary timeout or transient failure), far more frequent than a
// version being condemned outright.
//
// Two deliberate properties:
//
//   - The verdict is read from the ARTIFACT's mtime, not from an in-process
//     timer. A candidate may itself restart, and an in-memory timer would go
//     with it, losing all knowledge of how long the remains have been there.
//   - It waits for no external signal. §1.4's broadcast only covers the case
//     where the version is condemned; here nobody will ever send "you were
//     abandoned" — the control layer has already chosen someone else. Like the
//     local health view (§5) and the leader's soft state (§4.3), the node
//     cleans up on its own rather than depending on a message that may never
//     arrive.
//
// A negative BuildAbandonTimeout disables it; <= 0 takes the default. It is
// idempotent, and it does nothing when persistence is unconfigured (there is no
// on-disk artifact to reclaim).
func (im *IndexManagerImpl) StartAbandonSweeper() {
	if im.cfg.BuildAbandonTimeout < 0 || im.cfg.IndexDataDir == "" {
		return
	}
	im.mu.Lock()
	if im.abandonCancel != nil {
		im.mu.Unlock()
		return // already running
	}
	ctx, cancel := context.WithCancel(context.Background())
	im.abandonCancel = cancel
	im.mu.Unlock()

	im.abandonWG.Add(1)
	go func() {
		defer im.abandonWG.Done()
		ticker := time.NewTicker(im.cfg.BuildAbandonSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				im.sweepAbandonedArtifacts(now)
			}
		}
	}()
}

// StopAbandonSweeper stops the sweeper and waits for the running sweep to
// return. Safe to call when it was never started.
func (im *IndexManagerImpl) StopAbandonSweeper() {
	im.mu.Lock()
	cancel := im.abandonCancel
	im.abandonCancel = nil
	im.mu.Unlock()
	if cancel != nil {
		cancel()
		im.abandonWG.Wait()
	}
}

// sweepAbandonedArtifacts reclaims what a dead Save left behind.
//
// Two kinds of remains, and only two — v13 §8.8 named both:
//
//   - An <v>.index with no <v>.index.ids. Save renames the Faiss file into
//     place FIRST and its .ids sidecar SECOND (§8.3's atomic-write ordering),
//     so a process that dies between the two renames leaves exactly this: an
//     index nothing can load (Load rejects an unpaired file) and nothing will
//     ever rewrite.
//   - <v>.index.tmp / <v>.index.ids.tmp, the temporaries Save writes before
//     renaming. A stale one is a Save that died mid-write; the next Save
//     overwrites it and EnforceDiskRetention ignores it, but until then it
//     costs its full size.
//
// A SEALED pair is never touched here, however old — a READY version's artifact
// staying on disk is EnforceDiskRetention's business, not this one's.
//
// The mtime test exists for the two-rename window, not for "a build in flight":
// a build runs entirely in memory and touches no file (§8.8 finding 1 and 3), so
// a stale mtime cannot mean "still building". What it means is that the remains
// have been there for a while, and waiting one timeout keeps the sweeper away
// from a Save that is merely slow to finish. Nothing is reported to the control
// layer — the version's state there is authoritative, and this node's local
// remains are not its concern.
func (im *IndexManagerImpl) sweepAbandonedArtifacts(now time.Time) {
	timeout := im.cfg.BuildAbandonTimeout
	if timeout <= 0 {
		timeout = DefaultBuildAbandonTimeout
	}
	root := filepath.Join(im.cfg.IndexDataDir, "index")
	kbs, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			im.logger.Warn("index: abandoned-artifact sweep could not read the index directory", zap.Error(err))
		}
		return
	}
	for _, kb := range kbs {
		if !kb.IsDir() {
			continue
		}
		dir := filepath.Join(root, kb.Name())
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			switch {
			case strings.HasSuffix(name, ".index.tmp"), strings.HasSuffix(name, ".index.ids.tmp"):
				im.removeIfStale(dir, kb.Name(), e, now, timeout,
					"an abandoned Save temporary")
			case strings.Contains(name, ".installing."):
				// InstallIndex writes through os.CreateTemp with a ".installing." infix
				// (see install.go's installFile). Those names matched none of the
				// patterns here, so a process killed mid-fetch left one remainder per
				// attempt — each a whole index's size — that nothing would ever delete
				// (L5 of docs/code-review-2026-10-18.md). The payload is another node's
				// artifact, and an abandoned copy is as much a remainder as a Save's.
				im.removeIfStale(dir, kb.Name(), e, now, timeout,
					"an abandoned InstallIndex temporary")
			case strings.HasSuffix(name, ".index"):
				version := strings.TrimSuffix(name, ".index")
				// Sealed by a Save: a completed artifact, not a remainder.
				if fileExists(filepath.Join(dir, version+".index.ids")) {
					continue
				}
				im.removeIfStale(dir, kb.Name(), e, now, timeout,
					"an index artifact that no Save ever sealed")
			}
		}
	}
}

// removeIfStale deletes one entry whose mtime is older than timeout, and says
// so. A sweep is best-effort by design: a file that cannot be stat'ed or
// removed is left for the next pass rather than failing the whole sweep.
func (im *IndexManagerImpl) removeIfStale(dir, kbID string, e os.DirEntry, now time.Time, timeout time.Duration, what string) {
	info, err := e.Info()
	if err != nil {
		return
	}
	age := now.Sub(info.ModTime())
	if age < timeout {
		return
	}
	if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
		im.logger.Warn("index: could not remove an abandoned artifact",
			zap.String("kb_id", kbID), zap.String("file", e.Name()), zap.Error(err))
		return
	}
	im.logger.Warn("index: removed "+what+" — nothing else would have reclaimed it",
		zap.String("kb_id", kbID), zap.String("file", e.Name()),
		zap.Duration("age", age), zap.Duration("timeout", timeout))
}

// removeDeadChunks asks the vecstore to drop these chunks' vectors and reports
// how many it actually removed. Only a graph-free index supports it (faiss
// compacts IndexFlatCodes but cannot repair an HNSW graph) and only while the
// build is open — the vecstore answers FailedPrecondition otherwise, which the
// caller turns into a fallback to a full rebuild.
func (im *IndexManagerImpl) removeDeadChunks(
	ctx context.Context, kbID string, versionID int64, dead []string,
) (int64, error) {
	resp, err := im.vectorIndexClient.RemoveChunks(ctx, &vecstorepb.RemoveChunksRequest{
		KbId: kbID, VersionId: versionID, ChunkIds: dead,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetRemoved(), nil
}

// fileExists reports whether path names an existing regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// checkKBDirName refuses a knowledge base id that is not a single, ordinary
// path component. Callers use it before turning kbID into a directory that is
// then created, walked or REMOVED.
//
// The control layer mints these ids — a random, opaque handle (see
// service.generateKBID), never the knowledge base's display name — so an id
// carrying a separator or ".." should be impossible. This is the second line of
// defence at the point where the consequence is irreversible: filepath.Join
// cleans "..", so `filepath.Join(IndexDataDir, "index", kbID)` with an
// unvalidated id names a directory OUTSIDE IndexDataDir, and the RemoveAll
// built from it deletes whatever the caller chose.
func checkKBDirName(kbID string) error {
	if kbID == "" || kbID == "." || kbID == ".." || filepath.Base(kbID) != kbID {
		return fmt.Errorf("index: refused kb_id %q: must be a single path component", kbID)
	}
	return nil
}

// DeleteFilesByKB implements IndexManager: removes kbID's on-disk index
// directory (<IndexDataDir>/index/<kbID>/) entirely — the Faiss file, the
// .ids sidecar, and any size sidecars. A missing directory is not an
// error (idempotent re-run after a crash). It also drops every in-memory
// entry for the KB (idempotent with EvictByKB) and sets a KB tombstone so
// an in-flight Search-triggered Load RPC cannot resurrect the index after
// the deletion. No-op when disk persistence is unconfigured.
func (im *IndexManagerImpl) DeleteFilesByKB(_ context.Context, kbID string) error {
	if err := checkKBDirName(kbID); err != nil {
		return err
	}
	im.mu.Lock()
	im.deletedKBs[kbID] = true
	for k := range im.loaded {
		if k.kbID == kbID {
			delete(im.loaded, k)
			if size, ok := im.sizeByKey[k]; ok {
				im.loadedBytes -= size
				delete(im.sizeByKey, k)
			}
		}
	}
	for k := range im.lastSearch {
		if k.kbID == kbID {
			im.forgetSearchLocked(k)
		}
	}
	for k := range im.builtGraphFree {
		if k.kbID == kbID {
			delete(im.builtGraphFree, k)
		}
	}
	im.mu.Unlock()

	if im.cfg.IndexDataDir == "" {
		return nil
	}
	dir := filepath.Join(im.cfg.IndexDataDir, "index", kbID)
	// Every shard, because this removes a directory whose versions are not named
	// here: a fetch reading one version's pair must not have the tree pulled out from
	// under it (M8 of docs/code-review-2026-10-18.md).
	return im.withAllInstallShards(func() error {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("index: DeleteFilesByKB(%s): %w", kbID, err)
		}
		return nil
	})
}

// EnforceDiskRetention implements the per-KB on-disk retention policy
// (Stratum_设计文档v10.md "磁盘保留策略"): keeps the most recent
// IndexRetentionCount index files per knowledge base and deletes older
// ones (plus their .ids / size sidecars). protectedIDs are never
// deleted — used to shield the active version at startup. Missing files
// and missing directories are ignored (idempotent). No-op when retention
// is unconfigured (IndexRetentionCount <= 0) or disk persistence is off.
//
// Deleting an index file does not affect a loaded in-memory index; a
// later query against an evicted, retention-dropped version reports
// ErrIndexNotReady and can be recovered via RebuildIndex ("需要时重建").
func (im *IndexManagerImpl) EnforceDiskRetention(_ context.Context, kbID string, protectedIDs []int64) error {
	if im.cfg.IndexRetentionCount <= 0 || im.cfg.IndexDataDir == "" {
		return nil
	}
	if err := checkKBDirName(kbID); err != nil {
		return err
	}
	dir := filepath.Join(im.cfg.IndexDataDir, "index", kbID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("index: EnforceDiskRetention(%s): read dir: %w", kbID, err)
	}

	// Orphaned sidecars first: a `<v>.index.used` or `<v>.index.mem` whose
	// `<v>.index` is gone. The drop loop below walks artifacts, so it can never
	// reach them — a version whose artifact was lost (an interrupted install, a
	// manual delete, a botched partial write) leaves its sidecars behind for
	// good, and nothing else in the retention path ever looks at them.
	//
	// Only these two suffixes. saveToDisk and recordInterestNow write them AFTER
	// the index is in place, whereas InstallIndex writes the `.ids` pair BEFORE
	// the index — so a `.ids` without its index is a normal in-flight state, not
	// garbage, and must be left alone. (That one is §8.8's business, not
	// retention's.)
	withIndex := make(map[string]bool, len(entries))
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, ".index") {
			withIndex[strings.TrimSuffix(name, ".index")] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		var base string
		switch {
		case strings.HasSuffix(name, ".index.used"):
			base = strings.TrimSuffix(name, ".index.used")
		case strings.HasSuffix(name, ".index.mem"):
			base = strings.TrimSuffix(name, ".index.mem")
		default:
			continue
		}
		if withIndex[base] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("index: EnforceDiskRetention(%s): remove orphan sidecar %s: %w", kbID, name, err)
		}
	}

	protected := make(map[int64]bool, len(protectedIDs))
	for _, id := range protectedIDs {
		protected[id] = true
	}

	type idxFile struct {
		versionID int64
		base      string // file base name without the ".index" suffix
	}
	var files []idxFile
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".index") {
			continue
		}
		base := strings.TrimSuffix(name, ".index")
		id, perr := strconv.ParseInt(base, 10, 64)
		if perr != nil || protected[id] {
			continue
		}
		files = append(files, idxFile{versionID: id, base: base})
	}

	if len(files) <= im.cfg.IndexRetentionCount {
		return nil
	}

	// Shield the versions queried here recently, on top of the newest
	// IndexRetentionCount. Read from the .used sidecars rather than from the
	// in-memory access table, because this pass also runs at startup — when
	// memory holds nothing and the drop is at its largest.
	if window := im.retentionProtectWindow(); window > 0 {
		candidates := make([]int64, len(files))
		for i, f := range files {
			candidates[i] = f.versionID
		}
		if keep := im.accessProtectedIDs(kbID, candidates, window, im.retentionProtectMax()); len(keep) > 0 {
			rest := files[:0]
			for _, f := range files {
				if !keep[f.versionID] {
					rest = append(rest, f)
				}
			}
			files = rest
			if len(files) <= im.cfg.IndexRetentionCount {
				return nil
			}
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].versionID < files[j].versionID })

	// Drop the oldest (len(files) - retentionCount) versions' files.
	// Sidecar names mirror the vecstore's Save layout: the Faiss file is
	// <versionID>.index, its chunk-ID sidecar is <versionID>.index.ids, the
	// size sidecar is <versionID>.index.mem, and the access-time sidecar (the
	// retention shield above) is <versionID>.index.used.
	for _, f := range files[:len(files)-im.cfg.IndexRetentionCount] {
		// This version's shard: retention must not delete an artifact another node is
		// in the middle of fetching (M8 of docs/code-review-2026-10-18.md). Taken per
		// version rather than for the whole loop, so one slow fetch cannot stall the
		// rest of the pass.
		f := f
		if err := im.withInstallShard(kbID, f.versionID, func() error {
			for _, suffix := range []string{".index", ".index.ids", ".index.mem", ".index.used"} {
				path := filepath.Join(dir, f.base+suffix)
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("index: EnforceDiskRetention(%s): remove %s: %w", kbID, f.base+suffix, err)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		// docs/storage-node-state-machine-plan.md §5.2 and §7 item 5: the disk is
		// the authority and the state machine is its projection, so a version
		// whose artifact just went away stops being READY here. Reported per
		// version rather than in a batch, because the machine's correction is
		// per version too and this pass is already O(files).
		im.reportIndexDropped(kbID, f.versionID)
	}
	return nil
}
