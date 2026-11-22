package coordinator

import (
	"context"
	"fmt"

	"stratum/internal/types"
)

// ReplayVersionStorageWrites implements WriteCoordinator: replays steps
// 3-6 (plus summary + async build) for an already-committed version after
// a crash, without writing a BEGIN record or proposing a new version.
func (c *WriteCoordinatorImpl) ReplayVersionStorageWrites(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) error {
	unlock := c.locks.Lock(kbID)
	defer unlock()

	// The WAL already framed this transaction (its BEGIN record is what the
	// recovery path read the replay input from), so the storage layer resumes
	// it rather than starting a second one.
	return c.cfg.DataPlane.ResumeVersionWrite(ctx, kbID, versionID, parentVersionID, changes)
}

// DropVersionStorage removes one version's physical storage writes: its MVCC
// records in the document store and its document-ID list. It is the cleanup of
// docs/参考/历史/Stratum_设计文档v13.md §10.6 — a version the control layer declared
// FAILED_PERMANENT may still have landed on a replica whose acknowledgement was
// lost, and nobody else would reclaim it.
//
// The reclaim is a plain prefix delete, as §10.6 specifies. It deliberately does
// NOT apply the visibility-anchor rule the delete-version flow uses: that rule
// protects a SURVIVING version's reads, and a permanently failed version has
// none — visibility follows the control layer's state, not what is physically
// present, so its records are unreachable by construction. Applying the anchor
// here would also bolt a metadata lookup onto a cleanup path that has to work
// precisely when the control layer is unreachable.
//
// Idempotent: both deletes are prefix scans, so a version that never arrived
// here is a no-op rather than an error.
func (c *WriteCoordinatorImpl) DropVersionStorage(ctx context.Context, kbID string, versionID int64) error {
	// Chunk vectors first, while the version's document set still names them. This
	// is where a chunk can become an orphan at all (the version that referenced it
	// is going away), so it belongs here rather than only in the periodic sweep;
	// the order is also what makes a FAILURE retryable, since the retry (the
	// tombstone's PENDING entry, or the delete flow's own backoff) needs the
	// document list to find the candidates again.
	if c.cfg.ChunkReclaimer != nil {
		if err := c.cfg.ChunkReclaimer.ReclaimVersionChunks(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: chunk reclaim %s v%d: %w", kbID, versionID, err)
		}
	}
	// Each store is optional because this runs on nodes that hold none of the
	// data. A control-role node has no local docstore and no version-doc list:
	// its job is to ASK the nodes that do (§7.13.2, "asking rather than
	// doing"), and it reaches them through the data plane — not through these
	// fields. IndexManager was already guarded below for the same reason.
	//
	// The guards are not cosmetic. Without them a plain knowledge-base delete on
	// a control node panics with a nil dereference deep inside Pebble
	// (docstore.(*PebbleDocStore).DeleteByVersion on a nil receiver) and takes
	// the whole process down: measured on the 2-tier container cluster, all three
	// control nodes died mid-cleanup, each left its KB stuck in
	// KB_STATUS_DELETING, and the API had already answered success.
	if c.cfg.DocStore != nil {
		if err := c.cfg.DocStore.DeleteByVersion(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: docstore %s v%d: %w", kbID, versionID, err)
		}
	}
	if c.cfg.VersionDocList != nil {
		if err := c.cfg.VersionDocList.DeleteByVersion(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: versiondoc %s v%d: %w", kbID, versionID, err)
		}
	}
	// §10.6(4): reclaim the version's index artifacts too — the vecstore-side
	// index object and its persisted files (.index, .ids, .index.mem). A version
	// that reached FAILED_PERMANENT is never queryable and never rebuilt, so a
	// surviving artifact would only linger and skew the disk retention window.
	// Discard is idempotent, and server-side it is a no-op for an index that was
	// never built — so a replica that never received this version can run the
	// same cleanup harmlessly.
	if c.cfg.IndexManager != nil {
		if err := c.cfg.IndexManager.Discard(ctx, kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: index %s v%d: %w", kbID, versionID, err)
		}
	}
	// And the version's document filter. The DeleteVersion flow has always removed it
	// (LocalVersionDropper), but THIS path did not — so a version retired by a terminal
	// verdict left one behind, for good. Nothing else would collect it: the version was
	// never queryable (visibility is the control layer's judgement, not a disk fact) and
	// the reverse reconciliation looks for versions the node holds whose metadata is
	// GONE, while this one is still in the metadata — marked terminal. Idempotent like
	// the layers above, and a replica that never built a filter for the version removes
	// nothing.
	if c.cfg.VersionViews != nil {
		if err := c.cfg.VersionViews.DeleteVersion(kbID, versionID); err != nil {
			return fmt.Errorf("coordinator: DropVersionStorage: versionview %s v%d: %w", kbID, versionID, err)
		}
	}
	return nil
}
