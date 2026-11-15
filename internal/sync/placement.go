package sync

import (
	"context"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func WithStoragePlacement(resolver storage.Resolver) PushHandlerOption {
	return func(h *PushHandler) { h.placements = resolver }
}

func WithExecutionGate(gate *storage.ExecutionGate, reader storage.Reader) PushHandlerOption {
	return func(h *PushHandler) { h.executionGate = gate; h.directory = reader }
}

// enterMigrationImport is the dedicated entrance for a task target that is not a
// member yet. It deliberately does NOT reuse bindPlacement: membership is not the
// authorization here, the committed migration task names this node as its target.
// What it does reuse is the service authorization and the durable fence floor, so
// an installation drains against a concurrent fence/cleanup of the same knowledge
// base and is cut off when this node's service lease expires.
func (h *PushHandler) enterMigrationImport(ctx context.Context, kbID string, epoch uint64, operationID string) (context.Context, func(), error) {
	if h == nil || h.executionGate == nil {
		return ctx, func() {}, serrors.ErrPlacementChangeUnsafe
	}
	if kbID == "" || epoch == 0 {
		return ctx, func() {}, serrors.ErrInvalidArgument
	}
	return h.executionGate.EnterImport(ctx, kbID, epoch, operationID)
}

func (h *PushHandler) enterMutation(ctx context.Context, kbID string) (context.Context, func(), error) {
	if h.executionGate != nil {
		if err := storage.RequireRequestEpoch(ctx, kbID); err != nil {
			return ctx, func() {}, err
		}
	}
	ctx, err := h.bindPlacement(ctx, kbID)
	if err != nil {
		return ctx, func() {}, err
	}
	if snap, ok := storage.Bound(ctx, kbID); ok {
		return storage.EnterMutation(ctx, h.executionGate, snap)
	}
	if h.executionGate != nil {
		return ctx, func() {}, serrors.ErrPlacementUnknown
	}
	return ctx, func() {}, nil
}

func (h *PushHandler) bindPlacement(ctx context.Context, kbID string) (context.Context, error) {
	if h == nil {
		return ctx, nil
	}
	return bindStoragePlacement(ctx, h.placements, h.nodeID, kbID)
}

func bindStoragePlacement(ctx context.Context, resolver storage.Resolver, nodeID int64, kbID string) (context.Context, error) {
	if resolver == nil {
		return ctx, nil
	}
	snap, err := storage.Resolve(ctx, resolver, kbID)
	if err != nil {
		return ctx, err
	}
	if !snap.Placement.Contains(nodeID) {
		return ctx, serrors.ErrNotKBReplica
	}
	return storage.Bind(ctx, snap), nil
}
