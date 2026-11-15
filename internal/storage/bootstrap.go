package storage

import (
	"context"
	"errors"
	"fmt"

	serrors "stratum/internal/errors"
	"stratum/internal/types"
)

type Inventory interface {
	ListKnowledgeBases(context.Context) ([]types.KnowledgeBaseMeta, error)
}

// Initialize registers deployment hints only for an empty directory. Once
// persisted, the directory is authoritative across restarts and leader changes;
// an old configuration must not erase dynamic registrations or draining state.
func Initialize(ctx context.Context, manager Manager, nodes []Node) error {
	d, err := manager.StorageDirectory(ctx)
	if err != nil {
		return err
	}
	if len(d.Nodes) > 0 {
		return nil
	}
	return manager.ProposeRegisterStorageNodes(ctx, d.Revision, nodes)
}

// Bootstrap registers the ORIGINAL full deployment and backfills legacy KBs.
// The leader invokes this outside apply. A crash between commands is safe: the
// next leader repeats the scan and every completed assignment is idempotent.
// A different deployment cannot silently reinterpret the existing directory.
func Bootstrap(ctx context.Context, manager Manager, inventory Inventory, nodes []Node) error {
	d, err := manager.StorageDirectory(ctx)
	if err != nil {
		return err
	}
	if len(d.Nodes) == 0 {
		if err := manager.ProposeRegisterStorageNodes(ctx, d.Revision, nodes); err != nil {
			return err
		}
		d, err = manager.StorageDirectory(ctx)
		if err != nil {
			return err
		}
	}
	if len(d.Nodes) != len(nodes) {
		return serrors.ErrPlacementChangeUnsafe
	}
	seen := make(map[int64]bool, len(nodes))
	for _, n := range nodes {
		stored, ok := d.Nodes[n.ID]
		if !ok || stored.Addr != n.Addr || seen[n.ID] {
			return serrors.ErrPlacementChangeUnsafe
		}
		seen[n.ID] = true
	}
	kbs, err := inventory.ListKnowledgeBases(ctx)
	if err != nil {
		return err
	}
	for _, kb := range kbs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if kb.Status != types.KBStatusActive {
			continue
		}
		if _, ok := d.Placements[kb.KBID]; ok {
			continue
		}
		p := d.InitialPlacement(kb.KBID, "bootstrap/"+kb.KBID)
		if err := manager.ProposeAssignKBPlacement(ctx, p); err != nil {
			// Creation may have atomically assigned it after our directory read.
			// A deletion may also have won the race. Neither may be overwritten.
			if errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
				snap, readErr := manager.GetKBPlacement(ctx, kb.KBID)
				if readErr == nil && sameMembers(snap.Placement, p) {
					continue
				}
			}
			if errors.Is(err, serrors.ErrKnowledgeBaseDeleted) || errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
				continue
			}
			return fmt.Errorf("bootstrap placement %s: %w", kb.KBID, err)
		}
	}
	return nil
}

func sameMembers(a, b Placement) bool {
	if a.Epoch != b.Epoch || a.RequiredReplicas != b.RequiredReplicas || len(a.ReplicaIDs) != len(b.ReplicaIDs) {
		return false
	}
	for i := range a.ReplicaIDs {
		if a.ReplicaIDs[i] != b.ReplicaIDs[i] {
			return false
		}
	}
	return true
}
