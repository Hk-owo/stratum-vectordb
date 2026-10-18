package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"stratum/internal/types"
)

// installShardCount is the size of the install-lock table (see
// IndexManagerImpl.installShards). Fixed, so nothing ever has to be reclaimed:
// two versions hashing to one shard merely serialise, which costs nothing.
const installShardCount = 64

// installShardOf maps a version to its install lock. FNV-1a over the knowledge
// base id, mixed with both halves of the version id — cheap, and all it has to
// do is spread versions.
func installShardOf(key indexKey) int {
	h := uint32(2166136261)
	for i := 0; i < len(key.kbID); i++ {
		h ^= uint32(key.kbID[i])
		h *= 16777619
	}
	v := uint64(key.versionID)
	h ^= uint32(v) ^ uint32(v>>32)
	h *= 16777619
	return int(h % installShardCount)
}

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
//
// Installs of the SAME version are serialised. A version can be shipped more
// than once (the build callback retries, and every attempt ships again), and two
// installs interleaving their renames is precisely how a new index ends up
// beside the previous version's sidecar — which the receiver then refuses to
// Load with `checksum mismatch` (measured on a 3-node cluster). The lock spans
// both renames, so "sidecar first, then index" is atomic to a reader.
func (im *IndexManagerImpl) InstallIndex(ctx context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error {
	shard := &im.installShards[installShardOf(indexKey{kbID, versionID})]
	shard.Lock()
	defer shard.Unlock()

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
	// policy would never look at it and the shipped shape would be permanent
	// there.
	im.mu.Lock()
	im.seedAccessLocked(indexKey{kbID, versionID})
	// Drop any remembered shape: the artifact that just landed decides it, and
	// the sidecar beside it is now the authority (§8.6a). Keeping a stale entry
	// here would make the policy treat a received graph-free artifact as
	// still graphed and rebuild it into the shape it already has.
	delete(im.builtGraphFree, indexKey{kbID, versionID})
	im.mu.Unlock()
	// §8.4(a): the shield goes on disk too. seedAccessLocked writes only the
	// in-memory table, so without this a received artifact — especially one
	// outside the newest IndexRetentionCount — would be deleted by the next
	// retention pass or the startup EnforceRetention: installed, then dropped,
	// for a version the sender just spent bandwidth shipping.
	im.recordInterestNow(kbID, versionID)

	// docs/storage-node-state-machine-plan.md §7 item 4: the artifact is on this
	// node's disk, so the version is READY here — recorded as a DISK FACT rather
	// than as a build, because no build happened on this node. The artifact was
	// produced elsewhere and copied, which is the whole point of §8.4's "build
	// once, distribute N".
	im.reportIndexInstalled(kbID, versionID)

	// Report the artifact as READY on THIS node, through the same callback the
	// build path uses.
	//
	// This is not bookkeeping. The control layer aggregates these reports into
	// the version's IndexReadyNodes, and §8.6(d)'s rolling cleanup asks it "how
	// many OTHER replicas still serve this version?" before it takes one out of
	// service. A replica that received its artifact — which is every replica
	// under §8.4's "build once, distribute N" — never reported one, so that count
	// only ever held the builder. Measured on the 3+3 cluster, on a version all
	// three replicas were serving:
	//
	//	index: gc: skipping collection — too few replicas would remain serving
	//	  others_serving=0 minimum_required=2
	//
	// With serving_replica_min >= 2 the collection therefore could not start
	// anywhere — the permanent-dead-weight shape the collector's own comment
	// warns about. It was not "the deployment has no spare replica": the view was
	// wrong.
	//
	// Off the caller's goroutine on purpose: this runs inside the install shard's
	// lock, the callback is a Raft proposal, and the sender of the push is waiting
	// on this call's ack. invokeCallback covers its own retries.
	im.mu.Lock()
	callbacks := append([]BuildCompleteCallback(nil), im.callbacks...)
	im.mu.Unlock()
	if len(callbacks) > 0 {
		go func() {
			for _, cb := range callbacks {
				im.invokeCallback(cb, kbID, versionID, types.IndexStatusReady)
			}
		}()
	}
	return nil
}

// installFile atomically replaces path with data: write a temporary sibling,
// fsync it, then rename over the target.
//
// The temporary name is unique per call rather than derived from the target
// alone. The target is the same for every source pushing this (kb, version) —
// and under §8.4 "build once, distribute N" every replica announces what it
// holds, so several of them push the SAME artifact here at once. With one shared
// ".installing" name those writers truncate and rename each other's temporary
// file: the first rename consumes it and the rest fail with ENOENT, and an
// interleaved pair can publish a half-written artifact. Unique names let each
// writer publish its own complete copy and the last rename win — the payloads
// are the same version's index, so either winner is correct.
func installFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".installing.*")
	if err != nil {
		return fmt.Errorf("index: InstallIndex: create temp beside %s: %w", path, err)
	}
	tmp := f.Name()
	// 0666 for the same reason the target directory is 0777: the file has to be
	// readable however the writer happens to be running.
	if err := f.Chmod(0o666); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("index: InstallIndex: chmod %s: %w", tmp, err)
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
	// The fsync above makes the artifact's CONTENTS durable; this makes its NAME
	// durable. rename is atomic with respect to the directory entry, not with respect
	// to a power loss: without the directory fsync the index file can be missing after
	// a crash even though the rename returned success — while the metadata on the
	// control layer already says READY for this version, so every query routed to this
	// node fails until something rebuilds the artifact.
	if err := syncDirOf(dir); err != nil {
		return fmt.Errorf("index: InstallIndex: fsync %s: %w", dir, err)
	}
	return nil
}

// syncDirOf fsyncs a directory so renames inside it survive a power loss.
func syncDirOf(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// sidecarPath returns the on-disk path of (kbID, versionID)'s sidecar — the
// metadata + checksum file the vecstore's Save writes alongside the index, and
// which Load validates the index against.
func (im *IndexManagerImpl) sidecarPath(kbID string, versionID int64) string {
	return filepath.Join(im.cfg.IndexDataDir, "index", kbID, fmt.Sprintf("%d.index.ids", versionID))
}

// artifactRead is one in-flight ReadIndexFiles: the bytes it produced, and the
// signal that they are ready. Waiters read the fields only after <-done — the
// close is the happens-before edge that publishes them.
type artifactRead struct {
	done        chan struct{}
	indexData   []byte
	sidecarData []byte
	err         error
}

// ReadIndexFiles returns the raw bytes of (kbID, versionID)'s persisted index
// and sidecar, for shipping them to a replica (§5.8). The caller decides who
// receives them; this side only knows what is on its own disk.
//
// Concurrent reads of the SAME version are merged: the first caller reads the
// pair, the rest wait for it and share the bytes. Two reasons, and the second is
// the one that was missing:
//
//   - one pass over the pair instead of N. N leaves pulling one artifact at once
//     is the ordinary shape of a cold replica catching up (§5.8), and the source
//     has no reason to pay N times for the same bytes;
//   - memory peaks once instead of N times. The read holds the whole artifact, so
//     "N concurrent readers" used to mean N whole copies resident at the source
//     (v14 §10.3 recorded that peak as unmeasured).
//
// Deliberately NOT a cache: the entry lives only while the read is in flight, so
// the next call after a rebuild reads the NEW artifact. That is the same property
// the shard lock protects — a receiver must never be handed a mixed pair.
//
// The returned bytes are SHARED with the other callers of the same read: treat
// them as read-only.
//
// It takes the same shard lock the writers take (see installShards): the two
// files are read one after the other, and a rebuild landing between those two
// reads would hand the receiver a new index beside the previous sidecar — a pair
// it cannot Load. Holding the lock keeps the pair consistent for the whole read.
func (im *IndexManagerImpl) ReadIndexFiles(kbID string, versionID int64) (indexData, sidecarData []byte, err error) {
	key := indexKey{kbID, versionID}

	im.readMu.Lock()
	if inflight, ok := im.readInflight[key]; ok {
		im.readMu.Unlock()
		<-inflight.done
		return inflight.indexData, inflight.sidecarData, inflight.err
	}
	inflight := &artifactRead{done: make(chan struct{})}
	im.readInflight[key] = inflight
	im.readMu.Unlock()

	inflight.indexData, inflight.sidecarData, inflight.err = im.readIndexFilesUnshared(kbID, versionID)

	im.readMu.Lock()
	delete(im.readInflight, key)
	im.readMu.Unlock()
	close(inflight.done) // publishes the fields above to every waiter
	return inflight.indexData, inflight.sidecarData, inflight.err
}

// readIndexFilesUnshared is the disk half of ReadIndexFiles: one caller reads,
// the others share its result. Callers must already be registered in
// readInflight, or two reads of the same version can interleave.
func (im *IndexManagerImpl) readIndexFilesUnshared(kbID string, versionID int64) (indexData, sidecarData []byte, err error) {
	shard := &im.installShards[installShardOf(indexKey{kbID, versionID})]
	shard.Lock()
	defer shard.Unlock()

	indexData, err = im.readFile(im.indexPath(kbID, versionID))
	if err != nil {
		return nil, nil, fmt.Errorf("index: read %s: %w", im.indexPath(kbID, versionID), err)
	}
	sidecarData, err = im.readFile(im.sidecarPath(kbID, versionID))
	if err != nil {
		return nil, nil, fmt.Errorf("index: read %s: %w", im.sidecarPath(kbID, versionID), err)
	}
	return indexData, sidecarData, nil
}
