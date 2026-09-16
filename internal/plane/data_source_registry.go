package plane

import (
	"context"
	"sync"
)

// DataSourceRegistry records, per version, the peer that announced it holds
// that version's data (Stratum_设计文档v13.md §8.5).
//
// Why a table instead of a probe: the source lookup runs on the **Raft apply
// path** — replaying history re-runs every CreateVersion through it — so it may
// never dial anyone (the first attempt at §8.5 probed peers there and drove
// `integration` from 26s to 462s and a timeout). Turning "who holds this
// version" into a fact the writer *pushes* keeps that lookup a map read.
//
// The table is in-memory on purpose: losing it on restart costs nothing, since
// every node falls back to the pre-§8.5 answer (ask the leader) and learns the
// announced sources again from the next confirmations. Persisting it would add
// a second source of truth for something that is only an optimisation.
type DataSourceRegistry struct {
	mu    sync.RWMutex
	byKey map[dataSourceKey]string
}

type dataSourceKey struct {
	kbID      string
	versionID int64
}

// NewDataSourceRegistry returns an empty registry.
func NewDataSourceRegistry() *DataSourceRegistry {
	return &DataSourceRegistry{byKey: make(map[dataSourceKey]string)}
}

// Register records that addr holds (kbID, versionID)'s data. An empty addr (a
// peer that does not announce itself) is ignored rather than stored, so a
// lookup can never return "known source: nowhere".
func (r *DataSourceRegistry) Register(kbID string, versionID int64, addr string) {
	if r == nil || addr == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byKey[dataSourceKey{kbID: kbID, versionID: versionID}] = addr
}

// Lookup reports the announced holder of (kbID, versionID), if any node has
// announced one.
func (r *DataSourceRegistry) Lookup(kbID string, versionID int64) (string, bool) {
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	addr, ok := r.byKey[dataSourceKey{kbID: kbID, versionID: versionID}]
	return addr, ok
}

// ForgetVersion drops one version's entry, used when the version's data is
// reclaimed (§10.6) so a later pull is not aimed at a peer that no longer has
// it.
func (r *DataSourceRegistry) ForgetVersion(kbID string, versionID int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byKey, dataSourceKey{kbID: kbID, versionID: versionID})
}

// ForgetKB drops every entry of a knowledge base, used when the KB is deleted.
func (r *DataSourceRegistry) ForgetKB(kbID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.byKey {
		if k.kbID == kbID {
			delete(r.byKey, k)
		}
	}
}

// Len reports how many versions have an announced holder. Diagnostics and
// tests.
func (r *DataSourceRegistry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byKey)
}

// ResolverWithRegistry answers with, in order:
//
//  1. the announced holder of (kbID, versionID) — a writer said so (§8.5);
//  2. the control leader's §7.13.4 aggregate, read from a LOCAL mirror: never an
//     RPC, because this runs on the Raft apply path (see the history above). The
//     mirror is filled by the report heartbeat even when this node's own
//     confirmation never arrived, which is what makes a replica that missed one
//     still able to find a source (docs/data-source-holders-fallback-plan.md);
//  3. the pre-§8.5 answer — the leader. In a two-tier deployment that address
//     exports no data and refuses, but it is what starts the pull loop: an
//     ok=false here makes EnsureIndex give up silently, which is worse than an
//     address that fails.
//
// All three layers are pure map reads, and nothing on this path dials a peer —
// that is the whole point (see §8.5's history above). Layer 2 is filled by the
// report heartbeat's response, never by a call from here: the answer arrives
// carrying the interval this node already sends.
func ResolverWithRegistry(reg *DataSourceRegistry, holders *HoldersCache, fallback SourceResolver) SourceResolver {
	return func(ctx context.Context, kbID string, versionID int64) (string, bool, error) {
		if addr, ok := reg.Lookup(kbID, versionID); ok {
			return addr, true, nil
		}
		if holders != nil {
			if addrs, ok := holders.Lookup(kbID, versionID); ok && len(addrs) > 0 {
				// Ascending node id ⇒ reproducible; the rest are retry
				// candidates for callers that keep the list.
				return addrs[0], true, nil
			}
		}
		return fallback(ctx, kbID, versionID)
	}
}
