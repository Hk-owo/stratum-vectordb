// Graph-free shaping for cold versions (§5.5): whether this version should be
// built without HNSW edges, and when a build has to change shape.
package index

import (
	"context"
	"time"
)

// needsReshape reports whether the shape of this version's artifact no longer
// matches how the version is being used — the artifact is graph-free and the
// version has heated up, or it has a graph and the version has gone idle.
//
// BOTH directions live here because a query is the only thing that can notice
// either of them. A build decides its shape from coldShapeFor, but a version whose
// artifact already exists is not built again on its own: nothing about the passage
// of time rebuilds it. So a query against an artifact whose shape no longer fits
// is what schedules the rebuild — and "the caller is here, the shape is wrong, fix
// it" is exactly the demand-driven trigger §8.6(b) asks for.
//
// Half the threshold for the reheat direction, the whole threshold for the idle
// one: the gap between the bars is hysteresis, so a version sitting near the
// boundary is not rebuilt back and forth on consecutive queries. That gap is
// inherited from the background evaluator this replaced (§8.6a).
//
// It does not keep any "already reshaping" state: triggerBuild de-duplicates
// in-flight builds, so a second query arriving during the rebuild is a no-op
// rather than a second build.
func (im *IndexManagerImpl) needsReshape(kbID string, versionID int64) bool {
	threshold := im.cfg.ColdThreshold
	if threshold <= 0 {
		return false // policy off: every artifact is graphed and stays that way
	}
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	defer im.mu.Unlock()
	graphFree, known := im.shapeGraphFreeLocked(key)
	if !known {
		return false // no artifact here to reshape
	}
	last, ok := im.lastSearch[key]
	if !ok {
		return false
	}
	age := time.Since(last)
	if graphFree {
		return age <= threshold/2 // hot again: restore the graph
	}
	return age > threshold // gone idle: drop the graph
}

// operatorActive reports whether the control layer has designated versionID as the
// version this knowledge base serves — RollbackVersion is the only thing that sets
// it (internal/raft/state_machine.go).
//
// A read failure answers "no", so the shape falls back to the cold threshold. That
// direction is deliberate: the caller has already decided to build something, and
// refusing to build on an unreachable metadata read would trade a possibly-wrong
// shape for a version that cannot be served at all.
func (im *IndexManagerImpl) operatorActive(ctx context.Context, kbID string, versionID int64) bool {
	im.mu.Lock()
	provider := im.activeVersions
	im.mu.Unlock()
	if provider == nil {
		return false
	}
	active, err := provider(ctx)
	if err != nil {
		return false
	}
	id, ok := active[kbID]
	return ok && id == versionID
}

// coldShapeFor decides the SHAPE a build of this version should use, from this
// node's own access record: a version nothing has asked about recently is built
// graph-free (§8.6a), and one that is being served keeps its graph.
//
// It replaces the background evaluator that used to make this decision on a
// timer — sweepCold, which walked every version and called TriggerBuildGraphFree
// directly. That timer was wrong in two ways at once, and both came from deciding
// OUTSIDE a request:
//
//   - It built things nobody had asked for. That is the eager behaviour §8.6(b)
//     replaced; measured on the 3+3 cluster, a 900-version import produced 404
//     backfill builds and 3.5 s of build-pool queueing for versions nothing had
//     ever queried.
//   - It ignored §8.4's owner sets. A version's index lives on the replicas the
//     router routes to, so a node outside that set building one produces an
//     artifact no query will ever reach — and its lastSearch can never say
//     otherwise, because queries do not arrive there.
//
// Deciding here has neither problem: a build only happens because something asked
// for the version, and "did anything ask on THIS node recently" is exactly the
// question the shape should answer. ColdThreshold <= 0 disables the policy, which
// keeps the historical "every version carries a full graph" behaviour.
//
// USAGE is the whole criterion. It used to be two proxies on top of the silence —
// "is this the control layer's ActiveVersionID" and "is this the end of the version
// chain" — and both are obsolete under this design. The active pointer is moved
// only by RollbackVersion, so it says nothing about a knowledge base nobody has
// rolled back; the chain end is just "the newest version number", which correlates
// with being read and is not the same thing. The router already ranks versions by
// actual use (internal/router/index_priority.go scores recent queries with decay),
// and the shape should agree with it rather than with a version counter.
func (im *IndexManagerImpl) coldShapeFor(ctx context.Context, kbID string, versionID int64) bool {
	threshold := im.cfg.ColdThreshold
	if threshold <= 0 {
		return false
	}
	// A version the OPERATOR has made active is never built graph-free, however
	// long it has gone unqueried. That is the one "active" signal worth keeping:
	// RollbackVersion is an explicit "serve this one from now on", which is a
	// stronger statement of use than any query.
	//
	// The other signal that used to sit here — "is this the end of the version
	// chain" — is gone. It was a proxy for popularity ("the newest version is
	// probably the one being read"), and a version counter is a poor substitute for
	// the actual record of who asked. The router already ranks versions by recent
	// queries (internal/router/index_priority.go), so this agrees with it.
	if im.operatorActive(ctx, kbID, versionID) {
		return false
	}
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	last, ok := im.lastSearch[key]
	if !ok {
		// Never asked on this node: age it from startup rather than treating it as
		// permanently hot (the policy would never apply) or as instantly cold (a
		// restart would reshape everything at once).
		last = im.startedAt
	}
	im.mu.Unlock()
	return time.Since(last) >= threshold
}

// shapeGraphFreeLocked reports the version's current index shape and whether it
// is known. The in-memory record is consulted first (a version this process
// just built needs no disk read); the sidecar is the fallback, and the only
// source that survives a restart or describes an artifact this node merely
// received from a peer.
func (im *IndexManagerImpl) shapeGraphFreeLocked(key indexKey) (graphFree bool, known bool) {
	if graphFree, ok := im.builtGraphFree[key]; ok {
		return graphFree, true
	}
	if im.cfg.IndexDataDir == "" {
		return false, false
	}
	return im.artifactGraphFree(key.kbID, key.versionID)
}

// shapeGraphFree is shapeGraphFreeLocked for callers that do not hold im.mu.
//
// It exists because §8.6(c)'s reuse and §8.6(d)'s two collections used to read
// the in-memory record ALONE, while §8.6(a)'s cold/hot decision read the sidecar
// as well (shapeGraphFreeLocked above). Those two answers differ exactly where it
// matters: `builtGraphFree` is written only by this node's own successful build
// (doBuild), so a version §8.4 handed this node has no entry — and "no entry" was
// being read as "shape unknown", which sent such a version down the silent
// full-rebuild path in appendBase and off both collection paths entirely. Measured
// on the 3+3 cluster: 2 of 3 replicas rebuilt every child from scratch while
// holding the parent's artifact, and their tombstones were never collected.
//
// The disk read is the point, not a fallback of convenience (see artifactGraphFree):
// the shape line is written by vecstore's Save right next to the artifact, so it
// survives a restart, travels with a §8.4 handoff, and is cross-checked against
// the file's actual shape when the artifact is loaded — a mismatched pair refuses
// to load rather than being served as the other shape. A sidecar predating the
// line (§8.6a) reports known=false, which every caller must keep treating as the
// conservative answer.
func (im *IndexManagerImpl) shapeGraphFree(kbID string, versionID int64) (graphFree bool, known bool) {
	key := indexKey{kbID, versionID}
	im.mu.Lock()
	graphFree, known = im.builtGraphFree[key]
	im.mu.Unlock()
	if known {
		return graphFree, true
	}
	if im.cfg.IndexDataDir == "" {
		return false, false
	}
	return im.artifactGraphFree(kbID, versionID)
}
