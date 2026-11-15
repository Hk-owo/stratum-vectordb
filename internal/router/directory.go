package router

import (
	"context"
	"sort"
	"time"

	serrors "stratum/internal/errors"
	"stratum/internal/owner"
	"stratum/internal/storage"
)

func forwardDirectory[T any](r *Router, ctx context.Context, _ string, kbID string, fn func(int, context.Context) (T, error), fo forwardOpts, started time.Time) (T, error) {
	var zero T
	for attempt := 0; attempt < 2; attempt++ {
		snap, serial, err := r.directoryPlacement(ctx, kbID)
		if err != nil {
			return zero, serrors.ToGRPCStatus(err)
		}
		version := snap.ActiveVersion
		if fo.pinnedVersion > 0 {
			version = fo.pinnedVersion
		}
		if r.priority != nil && version > 0 {
			r.priority.Observe(kbID, version)
		}

		if r.storagePool != nil {
			peers, release, err := r.storagePool.acquire(snap.Nodes)
			if err != nil {
				return zero, serrors.ToGRPCStatus(err)
			}
			result, err := func() (T, error) {
				defer release()
				return forwardPeers(r, storage.Bind(ctx, snap), kbID, version, &snap, peers, fn, started)
			}()
			reason := serrors.ReasonOf(err)
			if attempt == 0 && (reason == "placement_stale" || reason == "not_kb_replica") {
				r.routes.RefreshAfter(ctx, serial)
				continue
			}
			return result, err
		}
		tiers, err := r.directoryTiers(snap, version, time.Now())
		if err != nil {
			return zero, serrors.ToGRPCStatus(err)
		}
		opCtx := storage.Bind(ctx, snap)
		result, err := forwardTiers(r, opCtx, readTrace{kbID: kbID, layer: "storage", admission: time.Since(started)}, tiers, r.storageBreakers, r.storageInflight, func(idx int, c context.Context) (T, error) {
			result, err := fn(idx, c)
			if cursor, ok := serrors.ReplicaCursorOf(err); ok {
				r.lag.note(kbID, idx, cursor, time.Now())
			}
			return result, err
		})
		reason := serrors.ReasonOf(err)
		if attempt == 0 && (reason == "placement_stale" || reason == "not_kb_replica") {
			r.routes.RefreshAfter(ctx, serial)
			continue
		}
		return result, err
	}
	return zero, serrors.ToGRPCStatus(serrors.ErrPlacementStale)
}

func (t *RouteTable) placement(kbID string) (storage.Snapshot, uint64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.ready || t.snapshot.directory == nil {
		return storage.Snapshot{}, t.serial, serrors.ErrPlacementUnknown
	}
	s, err := t.snapshot.directory.Resolve(kbID)
	return s, t.serial, err
}

// RefreshAfter merges concurrent misses/stale responses. A wave which observed
// one serial can publish at most one successful refresh.
func (t *RouteTable) RefreshAfter(ctx context.Context, serial uint64) {
	t.refreshMu.Lock()
	defer t.refreshMu.Unlock()
	t.mu.RLock()
	current := t.serial
	t.mu.RUnlock()
	if current != serial {
		return
	}
	t.refreshLocked(ctx)
}

func (r *Router) directoryPlacement(ctx context.Context, kbID string) (storage.Snapshot, uint64, error) {
	if r.routes == nil {
		return storage.Snapshot{}, 0, serrors.ErrPlacementUnknown
	}
	s, serial, err := r.routes.placement(kbID)
	if err != nil && err != serrors.ErrKnowledgeBaseDeleted {
		r.routes.RefreshAfter(ctx, serial)
		return r.routes.placement(kbID)
	}
	return s, serial, err
}

func (r *Router) directoryTiers(s storage.Snapshot, versionID int64, now time.Time) ([][]int, error) {
	// Connection pools are immutable for this router's lifetime. Map stable
	// directory IDs through addresses, never through the order of replica_ids.
	indexes := make(map[string]int, len(r.storageAddrs))
	for i, addr := range r.storageAddrs {
		indexes[addr] = i
	}
	addrs := s.Addresses(0)
	k := r.ownerInactiveK
	if s.ActiveVersion == versionID {
		k = 0
	} else if r.priority != nil {
		k += r.priority.Tier(s.Placement.KBID, versionID)
	}
	if k > len(addrs) {
		k = len(addrs)
	}
	owners := make(map[string]bool)
	for _, addr := range owner.Owners(s.Placement.KBID, versionID, addrs, k) {
		owners[addr] = true
	}
	var primary, other, behind []int
	for _, n := range s.Nodes {
		i, ok := indexes[n.Addr]
		if !ok {
			return nil, serrors.ErrPlacementUnknown
		}
		switch {
		case r.lag.behind(s.Placement.KBID, i, versionID, now):
			behind = append(behind, i)
		case owners[n.Addr]:
			primary = append(primary, i)
		default:
			other = append(other, i)
		}
	}
	var tiers [][]int
	for _, tier := range [][]int{primary, other, behind} {
		if len(tier) > 0 {
			tiers = append(tiers, leastLoadedFirst(tier, r.storageInflight))
		}
	}
	return tiers, nil
}

// Cluster-wide storage methods also discover nodes from the directory; startup
// addresses are bootstrap hints, not a second runtime membership authority.
func forwardDirectoryNodes[T any](r *Router, ctx context.Context, fn func(int, context.Context) (T, error), started time.Time) (T, error) {
	var zero T
	if r.routes == nil {
		return zero, serrors.ToGRPCStatus(serrors.ErrPlacementUnknown)
	}
	r.routes.mu.RLock()
	ready := r.routes.ready && r.routes.snapshot.directory != nil
	serial := r.routes.serial
	r.routes.mu.RUnlock()
	if !ready {
		r.routes.RefreshAfter(ctx, serial)
	}
	r.routes.mu.RLock()
	var nodes []storage.Node
	if r.routes.ready && r.routes.snapshot.directory != nil {
		for _, n := range r.routes.snapshot.directory.Nodes {
			if n.State == storage.NodeRetired {
				continue
			}
			nodes = append(nodes, n)
		}
	}
	r.routes.mu.RUnlock()
	if len(nodes) == 0 {
		return zero, serrors.ToGRPCStatus(serrors.ErrPlacementUnknown)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	peers, release, err := r.storagePool.acquire(nodes)
	if err != nil {
		return zero, serrors.ToGRPCStatus(err)
	}
	defer release()
	return forwardPeers(r, ctx, "", 0, nil, peers, fn, started)
}
