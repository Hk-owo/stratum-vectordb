package plane

import (
	"context"

	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func (d *LocalDataPlane) ExecutionGate() *storage.ExecutionGate { return d.executionGate }

func (d *LocalDataPlane) enterMutation(ctx context.Context, kbID string) (context.Context, func(), error) {
	var err error
	ctx, err = d.bindPlacement(ctx, kbID)
	if err != nil {
		return ctx, func() {}, err
	}
	if snap, ok := storage.Bound(ctx, kbID); ok {
		return storage.EnterMutation(ctx, d.executionGate, snap)
	}
	if d.executionGate != nil {
		return ctx, func() {}, serrors.ErrPlacementUnknown
	}
	return ctx, func() {}, nil
}

type gatedVersionDropper struct {
	gate   *storage.ExecutionGate
	reader storage.Reader
	nodeID int64
	next   VersionDataDropper
}

func (d gatedVersionDropper) DropVersionStorage(ctx context.Context, kbID string, v int64) error {
	ctx, release, err := storage.EnterCleanup(ctx, d.gate, d.reader, d.nodeID, kbID)
	if err != nil {
		return err
	}
	defer release()
	return d.next.DropVersionStorage(ctx, kbID, v)
}

type gatedKBDropper struct {
	gate   *storage.ExecutionGate
	reader storage.Reader
	nodeID int64
	next   KBStorageDropper
}

func (d gatedKBDropper) DropKnowledgeBase(ctx context.Context, kbID string) error {
	ctx, release, err := storage.EnterCleanup(ctx, d.gate, d.reader, d.nodeID, kbID)
	if err != nil {
		return err
	}
	defer release()
	return d.next.DropKnowledgeBase(ctx, kbID)
}

func (d *LocalDataPlane) bindPlacement(ctx context.Context, kbID string) (context.Context, error) {
	if d.placements == nil {
		return ctx, nil
	}
	s, err := storage.Resolve(ctx, d.placements, kbID)
	if err != nil {
		return ctx, err
	}
	if !s.Placement.Contains(d.nodeID) {
		return ctx, serrors.ErrNotKBReplica
	}
	return storage.Bind(ctx, s), nil
}

func (d *LocalDataPlane) OwnsKnowledgeBase(ctx context.Context, kbID string) (bool, error) {
	_, err := d.bindPlacement(ctx, kbID)
	if err == serrors.ErrNotKBReplica {
		return false, nil
	}
	return err == nil, err
}

func (d *LocalDataPlane) hasReplicaResolver() bool {
	return d.placements != nil || d.resolveReplicas != nil
}

func (d *LocalDataPlane) replicaPeers(ctx context.Context, kbID string) ([]string, error) {
	if d.placements != nil {
		s, err := storage.Resolve(ctx, d.placements, kbID)
		if err != nil {
			return nil, err
		}
		return s.Addresses(d.nodeID), nil
	}
	if d.resolveReplicas == nil {
		return nil, nil
	}
	return d.resolveReplicas(ctx)
}

func (d *LocalDataPlane) sourceAllowed(ctx context.Context, kbID, addr string) bool {
	if d.placements == nil {
		return addr != ""
	}
	s, err := storage.Resolve(ctx, d.placements, kbID)
	return err == nil && s.HasAddress(addr)
}

func (d *LocalDataPlane) resolveSource(ctx context.Context, kbID string, versionID int64) (string, bool, error) {
	addr, ok, err := d.resolve(ctx, kbID, versionID)
	if err != nil || d.placements == nil {
		return addr, ok, err
	}
	if ok && d.sourceAllowed(ctx, kbID, addr) {
		return addr, true, nil
	}
	addr, ok = d.pickTransferSource(ctx, kbID, versionID, "")
	return addr, ok, nil
}

// Cleanup uses retained deletion targets when the live placement is gone.
// These addresses are never used as a write or query configuration.
func (d *LocalDataPlane) cleanupPeers(ctx context.Context, kbID string) (context.Context, []string, error) {
	if d.directory == nil {
		peers, err := d.replicaPeers(ctx, kbID)
		return ctx, peers, err
	}
	dir, err := d.directory.StorageDirectory(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if t, ok := dir.Tombstones[kbID]; ok {
		var out []string
		for _, id := range t.CleanupIDs {
			if id == d.nodeID {
				continue
			}
			n, ok := dir.Nodes[id]
			if !ok || n.Addr == "" {
				return ctx, nil, serrors.ErrPlacementUnknown
			}
			out = append(out, n.Addr)
		}
		credential := storage.Snapshot{Placement: storage.Placement{KBID: kbID, Epoch: t.Epoch}}
		return storage.Bind(ctx, credential), out, nil
	}
	s, err := dir.Resolve(kbID)
	if err != nil {
		return ctx, nil, err
	}
	if err := storage.CheckRequestEpoch(ctx, kbID, s.Placement.Epoch); err != nil {
		return ctx, nil, err
	}
	if s.Migration != nil && s.Migration.BlocksDataWrites() {
		return ctx, nil, serrors.ErrKBWritePaused
	}
	return storage.Bind(ctx, s), s.Addresses(d.nodeID), nil
}
