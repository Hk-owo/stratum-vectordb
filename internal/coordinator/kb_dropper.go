package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/versiondoc"
)

// KBStorageDropper reclaims a knowledge base's physical data.
//
// It exists so the delete flow can say "this knowledge base is gone" without
// knowing where its bytes live. The layered deletes behind it — in-memory
// indexes, on-disk index and bloom files, documents, chunk vectors, chunk→doc
// mappings, version→doc lists — are storage-layer business; on a node that
// keeps no storage of its own there is nothing local to delete, and reclaiming
// becomes a broadcast to the nodes that do hold the data
// (Stratum_设计文档v13.md §7.0 的契约边界、§11 阶段 ④).
type KBStorageDropper interface {
	DropKnowledgeBase(ctx context.Context, kbID string) error
}

// LocalKBDropper reclaims a knowledge base on this node, layer by layer. It is
// the all-in-one role's implementation: the node that holds the metadata also
// holds the data, so the reclaim is local.
type LocalKBDropper struct {
	IndexManager   index.IndexManager
	DocStore       docstore.DocStore
	ChunkStore     chunkstore.ChunkStore
	ChunkDocMapper chunkdoc.ChunkDocMapper
	VersionDocList versiondoc.VersionDocList

	// VersionBloom may be nil when the filter store is not wired; its on-disk
	// file deletion is then skipped.
	VersionBloom *bloom.VersionBloomStore
}

var _ KBStorageDropper = LocalKBDropper{}

// DropKnowledgeBase deletes every physical layer of kbID on this node.
//
// Each layer's delete ignores missing entries, so re-running after a crash is
// safe — the idempotence the delete flow depends on to resume from any step.
func (d LocalKBDropper) DropKnowledgeBase(ctx context.Context, kbID string) error {
	if err := d.IndexManager.EvictByKB(ctx, kbID); err != nil {
		return fmt.Errorf("IndexManager.EvictByKB: %w", err)
	}
	if err := d.IndexManager.DeleteFilesByKB(ctx, kbID); err != nil {
		return fmt.Errorf("IndexManager.DeleteFilesByKB: %w", err)
	}
	if d.VersionBloom != nil {
		if err := d.VersionBloom.DeleteByKB(kbID); err != nil {
			return fmt.Errorf("VersionBloom.DeleteByKB: %w", err)
		}
	}
	if err := d.DocStore.DeleteByKB(ctx, kbID); err != nil {
		return fmt.Errorf("DocStore.DeleteByKB: %w", err)
	}
	if err := d.ChunkStore.DeleteByKB(ctx, kbID); err != nil {
		return fmt.Errorf("ChunkStore.DeleteByKB: %w", err)
	}
	if err := d.ChunkDocMapper.DeleteByKB(ctx, kbID); err != nil {
		return fmt.Errorf("ChunkDocMapper.DeleteByKB: %w", err)
	}
	if err := d.VersionDocList.DeleteByKB(ctx, kbID); err != nil {
		return fmt.Errorf("VersionDocList.DeleteByKB: %w", err)
	}
	return nil
}

// VersionDataDropper reclaims one version's physical data wherever it landed.
// plane.DataPlane satisfies it; the narrow form keeps this package from
// depending on the whole storage contract just to delete something.
type VersionDataDropper interface {
	DropVersionData(ctx context.Context, kbID string, versionID int64) error
}

// BroadcastKBDropper reclaims a knowledge base by asking the nodes that hold
// its data, one version at a time.
//
// This is the control layer's implementation: it holds no data, so it has none
// to delete. Version-by-version is the granularity the storage contract already
// offers (§7.0's DropVersionData, §10.6's broadcast), and it is sufficient here
// because the version list comes from the control layer's own metadata — the
// same metadata that placed the data in the first place.
type BroadcastKBDropper struct {
	// Metadata lists the knowledge base's versions. This is replicated control
	// state, never a storage-layer observation.
	Metadata raft.RaftNode

	// Dropper carries one version's reclaim to the replicas. On a control node
	// this is a data plane with no local store wired, so it broadcasts and does
	// nothing else.
	Dropper VersionDataDropper
}

var _ KBStorageDropper = BroadcastKBDropper{}

// DropKnowledgeBase broadcasts a reclaim for every version of kbID.
func (b BroadcastKBDropper) DropKnowledgeBase(ctx context.Context, kbID string) error {
	versions, err := b.Metadata.ListVersions(ctx, kbID)
	if err != nil {
		if errors.Is(err, stratumerrors.ErrKnowledgeBaseNotFound) {
			// Already gone: nothing was ever placed, so nothing to reclaim.
			return nil
		}
		return fmt.Errorf("DropKnowledgeBase: list versions of %s: %w", kbID, err)
	}

	// Every version is attempted even after one fails: one unreachable replica
	// must not leave the rest of the knowledge base's data behind.
	var firstErr error
	for _, v := range versions {
		if err := b.Dropper.DropVersionData(ctx, kbID, v.VersionID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// KBStorageDroppers runs several droppers and reports the first failure.
//
// A split deployment needs two halves of the same reclaim: the storage group
// holds the data, and an all-in-one node holds a copy of its own. Running them
// all is safe because every layer delete is idempotent — the same property the
// delete flow already relies on to resume after a crash — and it is the shape
// the data plane's own DropVersionData uses, which broadcasts and drops locally
// rather than choosing between them.
type KBStorageDroppers []KBStorageDropper

var _ KBStorageDropper = KBStorageDroppers(nil)

// DropKnowledgeBase runs each dropper, reporting the first failure.
func (ds KBStorageDroppers) DropKnowledgeBase(ctx context.Context, kbID string) error {
	var firstErr error
	for _, d := range ds {
		if d == nil {
			continue
		}
		if err := d.DropKnowledgeBase(ctx, kbID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// VersionStorageDropper reclaims one version's physical data.
//
// It is the per-version counterpart of KBStorageDropper, and it exists for the
// same reason: the delete-version flow decides *that* a version is gone, while
// where its bytes are and which of them a survivor still reads is the storage
// layer's business (Stratum_设计文档v13.md §7.0).
type VersionStorageDropper interface {
	DropVersion(ctx context.Context, kbID string, versionID int64) error
}

// LocalVersionDropper reclaims a version on this node: its index, its
// document-ID list, the MVCC records no survivor reads, and its bloom filter.
type LocalVersionDropper struct {
	IndexManager   index.IndexManager
	DocStore       docstore.DocStore
	VersionDocList versiondoc.VersionDocList

	// VersionBloom may be nil when the filter store is not wired; its file is
	// then left for the next whole-KB delete.
	VersionBloom *bloom.VersionBloomStore

	// Metadata supplies the version chain the visibility anchor is computed
	// from. On a storage node this is the remote proxy, so the rule is applied
	// by whichever node holds the records.
	Metadata raft.RaftNode
}

var _ VersionStorageDropper = LocalVersionDropper{}

// DropVersion reclaims one version's physical layers on this node.
//
// The bloom filter is dropped here rather than after the metadata removal the
// delete flow used to wait for. That ordering existed so no new query could
// reach a version whose filter was gone; the consequence of losing it is the
// one the flow already documents as harmless — an in-flight query rebuilds an
// empty filter from the (already deleted) document list and leaves a stray file
// that the next whole-KB delete removes. Reclaiming everything reachable in one
// contract call is worth that, and it is what lets the whole step travel to
// another node.
func (d LocalVersionDropper) DropVersion(ctx context.Context, kbID string, versionID int64) error {
	if err := d.IndexManager.Discard(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("IndexManager.Discard: %w", err)
	}
	if err := d.VersionDocList.DeleteByVersion(ctx, kbID, versionID); err != nil {
		return fmt.Errorf("VersionDocList.DeleteByVersion: %w", err)
	}

	anchor, err := d.visibilityAnchor(ctx, kbID, versionID)
	if err != nil {
		return err
	}
	if err := d.DocStore.DeleteByVersionExceptVisibleFrom(ctx, kbID, versionID, anchor); err != nil {
		return fmt.Errorf("DocStore.DeleteByVersionExceptVisibleFrom: %w", err)
	}

	if d.VersionBloom != nil {
		if err := d.VersionBloom.DeleteByVersion(kbID, versionID); err != nil {
			return fmt.Errorf("VersionBloom.DeleteByVersion: %w", err)
		}
	}
	return nil
}

// visibilityAnchor returns the smallest surviving version greater than
// versionID, or 0 when none exists — the records that version still reads are
// the ones that must be kept.
func (d LocalVersionDropper) visibilityAnchor(ctx context.Context, kbID string, versionID int64) (int64, error) {
	versions, err := d.Metadata.ListVersions(ctx, kbID)
	if err != nil {
		return 0, fmt.Errorf("list versions of %s: %w", kbID, err)
	}
	anchor := int64(0)
	for _, v := range versions {
		if v.Deleting || v.VersionID <= versionID {
			continue
		}
		if anchor == 0 || v.VersionID < anchor {
			anchor = v.VersionID
		}
	}
	return anchor, nil
}

// BroadcastVersionDropper reclaims a version by asking the nodes that hold it.
// On a control node the per-version reclaim is a broadcast; the receiving node
// applies the visibility rule itself, since it is the one holding the records.
type BroadcastVersionDropper struct {
	Dropper VersionDataDropper
}

var _ VersionStorageDropper = BroadcastVersionDropper{}

// DropVersion broadcasts the reclaim.
func (b BroadcastVersionDropper) DropVersion(ctx context.Context, kbID string, versionID int64) error {
	return b.Dropper.DropVersionData(ctx, kbID, versionID)
}

// VersionStorageDroppers runs several droppers and reports the first failure —
// the two halves of a split deployment, exactly as KBStorageDroppers does.
type VersionStorageDroppers []VersionStorageDropper

var _ VersionStorageDropper = VersionStorageDroppers(nil)

// DropVersion runs each dropper, reporting the first failure.
func (ds VersionStorageDroppers) DropVersion(ctx context.Context, kbID string, versionID int64) error {
	var firstErr error
	for _, d := range ds {
		if d == nil {
			continue
		}
		if err := d.DropVersion(ctx, kbID, versionID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// DeferredKBDropper resolves the real dropper on first use.
//
// The delete coordinator is assembled before the data plane exists (crash
// recovery needs it earlier), and the dropper a control node wants is the data
// plane's broadcast — so the choice cannot be made at construction time.
// Resolving on first call is the same late-binding trick the write dispatcher
// uses, and it keeps the assembly order-independent instead of forcing a
// second, duplicate construction path.
type DeferredKBDropper struct {
	mu      sync.Mutex
	resolve func() KBStorageDropper
	cached  KBStorageDropper
}

// NewDeferredKBDropper returns a dropper that calls resolve once, on first use.
func NewDeferredKBDropper(resolve func() KBStorageDropper) *DeferredKBDropper {
	return &DeferredKBDropper{resolve: resolve}
}

var _ KBStorageDropper = (*DeferredKBDropper)(nil)

// DropKnowledgeBase resolves the dropper and delegates.
func (d *DeferredKBDropper) DropKnowledgeBase(ctx context.Context, kbID string) error {
	d.mu.Lock()
	if d.cached == nil {
		d.cached = d.resolve()
	}
	inner := d.cached
	d.mu.Unlock()

	if inner == nil {
		return errors.New("DropKnowledgeBase: no dropper resolved")
	}
	return inner.DropKnowledgeBase(ctx, kbID)
}

// DeferredVersionDropper resolves the real dropper on first use, for the same
// reason DeferredKBDropper does: the delete-version coordinator is assembled
// before the data plane exists, and a control node's dropper is the data
// plane's broadcast.
type DeferredVersionDropper struct {
	mu      sync.Mutex
	resolve func() VersionStorageDropper
	cached  VersionStorageDropper
}

// NewDeferredVersionDropper returns a dropper that calls resolve once.
func NewDeferredVersionDropper(resolve func() VersionStorageDropper) *DeferredVersionDropper {
	return &DeferredVersionDropper{resolve: resolve}
}

var _ VersionStorageDropper = (*DeferredVersionDropper)(nil)

// DropVersion resolves the dropper and delegates.
func (d *DeferredVersionDropper) DropVersion(ctx context.Context, kbID string, versionID int64) error {
	d.mu.Lock()
	if d.cached == nil {
		d.cached = d.resolve()
	}
	inner := d.cached
	d.mu.Unlock()

	if inner == nil {
		return errors.New("DropVersion: no dropper resolved")
	}
	return inner.DropVersion(ctx, kbID, versionID)
}
