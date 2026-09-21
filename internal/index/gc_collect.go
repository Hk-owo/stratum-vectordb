package index

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	vecstorepb "stratum/api/proto/vecstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// §8.6(d) phase 2: collect the dead vectors out of a sealed graph-free artifact.
//
// The rolling rule the whole design rests on lives here: before this node takes a
// version out of service, it asks the CONTROL LAYER how many other replicas are
// serving it — not itself, not the station, not a guess. The control layer is the
// authority on index readiness (§1.2), so its answer is state rather than
// opinion; the station's health view is a lagging soft signal and using it for a
// hard precondition is exactly the mistake §8.6(d) rules out.

// ReplicaCounter answers "how many OTHER replicas are currently serving this
// version's index".
//
// An interface, injected, rather than a call into the control layer, because this
// package must keep the storage layer's seams narrow: it talks to the control
// layer through interfaces the assembly wires, and this is one of them.
type ReplicaCounter interface {
	IndexReadyReplicaCount(ctx context.Context, kbID string, versionID int64, except int64) (int, error)
}

// SetGCReplicaCounter wires the serving-replica lookup and this node's identity.
//
// nodeID matters because the question is "how many OTHERS are serving": a node
// about to step out must not count itself among the servers still up. A nil
// counter or a zero nodeID leaves collection off — without them this node cannot
// tell whether stepping out would leave anyone behind, and assuming it would not
// is a gamble with read availability.
func (im *IndexManagerImpl) SetGCReplicaCounter(counter ReplicaCounter, nodeID int64) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.replicaCounter = counter
	if nodeID != 0 {
		im.cfg.NodeID = nodeID
	}
}

// inMaintenance reports whether this node has the version out of service for
// collection.
func (im *IndexManagerImpl) inMaintenance(key indexKey) bool {
	im.mu.Lock()
	defer im.mu.Unlock()
	return im.maintenance[key]
}

// setMaintenance marks a version as in service or out of it.
func (im *IndexManagerImpl) setMaintenance(key indexKey, active bool) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if active {
		im.maintenance[key] = true
		return
	}
	delete(im.maintenance, key)
}

// collectCandidates turns scan candidates into collections, one at a time, each
// gated on the control layer's answer about remaining service capacity.
func (im *IndexManagerImpl) collectCandidates(ctx context.Context, candidates []gcCandidate) {
	if !im.cfg.GCEnabled || len(candidates) == 0 {
		return
	}
	im.mu.Lock()
	counter := im.replicaCounter
	im.mu.Unlock()
	if counter == nil || im.cfg.NodeID == 0 {
		return
	}
	minimum := im.cfg.IndexServingReplicaMin
	if minimum <= 0 {
		minimum = DefaultIndexServingReplicaMin
	}

	for _, c := range candidates {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Asked BEFORE anything is taken out of service, and asked of the control
		// layer: a wrong answer here costs reads, not just work.
		others, err := counter.IndexReadyReplicaCount(ctx, c.KBID, c.VersionID, im.cfg.NodeID)
		if err != nil {
			im.logger.Warn("index: gc: could not ask how many replicas serve the version; leaving it alone",
				zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID), zap.Error(err))
			continue
		}
		if others < minimum {
			// §8.6(d) names the shape this can become: if every replica is needed,
			// collection can never start and the artifact keeps its dead weight
			// forever. That must not pass silently, so it is reported here — the
			// log line is the observable half until the system-status report
			// (phase 4 of the design) lands.
			im.logger.Info("index: gc: skipping collection — too few replicas would remain serving",
				zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID),
				zap.Int("others_serving", others), zap.Int("minimum_required", minimum))
			// Recorded, not just logged (§8.6(d)): if every replica is needed, this
			// condition never clears on its own, and "the dead weight is permanent
			// because the deployment has no slack" is something an operator has to
			// be able to see — see BlockedCollections → GetSystemStatus.
			im.noteGCBlocked(indexKey{c.KBID, c.VersionID}, c.DeadShare, others, minimum)
			continue
		}

		dead, err := im.deadChunksOf(ctx, c.KBID, c.VersionID)
		if err != nil {
			im.logger.Warn("index: gc: could not determine the dead chunks; leaving the version alone",
				zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID), zap.Error(err))
			continue
		}
		if len(dead) == 0 {
			continue
		}

		// Which collection applies depends on the artifact's SHAPE, and the two
		// are not interchangeable: a graph-free artifact is reopened and edited, a
		// graphed one can only be rebuilt. The graphed path also needs a higher bar
		// (see GCGraphRebuildRatio) — rebuilding a grid costs the whole graph, so it
		// is only worth it for a version that is clearly not about to be replaced.
		//
		// Through shapeGraphFree: the shape line in the artifact's sidecar describes a
		// version this node received as well as one it built, and refusing to collect
		// the former is how handed-off artifacts kept their tombstones forever.
		graphFree, shapeKnown := im.shapeGraphFree(c.KBID, c.VersionID)
		if !shapeKnown {
			// Neither path can be chosen safely. collectGraphFree would refuse and
			// rebuildGraphed would too; saying so here makes the reason visible
			// instead of leaving two identical refusals in the log.
			im.logger.Info("index: gc: skipping — this node does not know the version's index shape",
				zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID))
			continue
		}
		if !graphFree && c.DeadShare < im.graphRebuildRatio() {
			continue
		}

		if graphFree {
			err = im.collectGraphFree(ctx, c.KBID, c.VersionID, dead)
		} else {
			err = im.rebuildGraphed(ctx, c.KBID, c.VersionID)
		}
		if err != nil {
			im.logger.Warn("index: gc: collection failed; the artifact is unchanged",
				zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID),
				zap.Bool("graph_free", graphFree), zap.Int("dead_chunks", len(dead)), zap.Error(err))
			continue
		}
		im.logger.Info("index: gc: collected dead vectors",
			zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID),
			zap.Bool("graph_free", graphFree), zap.Int("dead_chunks", len(dead)),
			zap.Float64("was_dead_share", c.DeadShare))
		// Collected, so whatever blocked this version before no longer describes
		// it. Clearing here (and only here, on success) is what makes the report
		// mean "stuck now" rather than "was stuck once".
		im.clearGCBlocked(indexKey{c.KBID, c.VersionID})
		im.notifyArtifactRewritten(c.KBID, c.VersionID)
	}
}

// graphRebuildRatio is the dead-share bar for rebuilding a graphed artifact.
func (im *IndexManagerImpl) graphRebuildRatio() float64 {
	if im.cfg.GCGraphRebuildRatio > 0 {
		return im.cfg.GCGraphRebuildRatio
	}
	return DefaultGCGraphRebuildRatio
}

// rebuildGraphed rebuilds a GRAPHED artifact from the current document set.
//
// A graphed artifact cannot be edited in place: faiss rejects remove_ids on HNSW
// (§8.6c's verification table), so its tombstones are permanent unless the graph is
// built again. The rebuild passes skipReuse=true — reusing the parent's artifact
// would inherit exactly the tombstones this exists to discard.
//
// The rolling rule is the caller's (it already asked the control layer), and the
// safety is the same as the graph-free path: build() writes through Save, which is
// atomic, so a failure leaves the previous artifact untouched. The version is out
// of service on this node for the duration — longer than the graph-free edit,
// which is why the bar for starting is higher.
func (im *IndexManagerImpl) rebuildGraphed(ctx context.Context, kbID string, versionID int64) error {
	key := indexKey{kbID, versionID}

	graphFree, known := im.shapeGraphFree(kbID, versionID)
	if !known {
		return fmt.Errorf("%w: version %d's index shape is unknown on this node",
			stratumerrors.ErrInvalidArgument, versionID)
	}
	if graphFree {
		return fmt.Errorf("%w: version %d is graph-free; that artifact is edited in place, not rebuilt",
			stratumerrors.ErrInvalidArgument, versionID)
	}
	if im.vectorIndexClient == nil {
		return errors.New("index: gc: no vecstore client is wired on this node")
	}

	im.setMaintenance(key, true)
	defer im.setMaintenance(key, false)

	if _, err := im.build(ctx, kbID, versionID, false /* graphFree */, true /* skipReuse */); err != nil {
		return fmt.Errorf("index: gc: graphed rebuild: %w", err)
	}
	return nil
}

// gcBlockedState is what this node remembers about a version whose collection is
// stuck behind the service-capacity check.
type gcBlockedState struct {
	deadShare float64
	others    int
	minimum   int
	since     time.Time
}

// GCPressure is one version whose §8.6(d) collection is blocked: its artifact
// carries more dead weight than the ratio allows, and too few other replicas are
// serving it for this node to step out and collect.
//
// Reported, never acted on. The condition is a configuration problem
// (IndexServingReplicaMin leaves no slack), the data is intact, and the version is
// queryable — the only thing wrong is that the dead weight cannot be reclaimed.
type GCPressure struct {
	KBID            string
	VersionID       int64
	DeadShare       float64
	OthersServing   int
	MinimumRequired int
	// Since is when this node FIRST saw the version blocked. The age is the point:
	// a version blocked for a minute is a moment, one blocked for a day is a
	// deployment that needs its replica count raised.
	Since time.Time
}

// noteGCBlocked records that a version's collection is blocked.
//
// The first sighting is kept as `since`: refreshing it on every pass would make a
// long-standing problem look perpetually new, which is the opposite of what this
// signal is for.
func (im *IndexManagerImpl) noteGCBlocked(key indexKey, deadShare float64, others, minimum int) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if existing, ok := im.gcBlocked[key]; ok {
		existing.deadShare = deadShare
		existing.others = others
		existing.minimum = minimum
		return
	}
	im.gcBlocked[key] = &gcBlockedState{
		deadShare: deadShare,
		others:    others,
		minimum:   minimum,
		since:     time.Now(),
	}
}

// clearGCBlocked forgets a version's blocked state — called when a collection
// actually succeeds, because the condition described by the record has ended.
func (im *IndexManagerImpl) clearGCBlocked(key indexKey) {
	im.mu.Lock()
	defer im.mu.Unlock()
	delete(im.gcBlocked, key)
}

// BlockedCollections reports the versions whose §8.6(d) collection is stuck.
//
// Sorted by (kb, version) so repeated status calls produce the same order: an
// alerting surface that reshuffles itself between calls is harder to read than one
// that does not. Nil when nothing is blocked, which is the normal state.
func (im *IndexManagerImpl) BlockedCollections() []GCPressure {
	im.mu.Lock()
	defer im.mu.Unlock()
	if len(im.gcBlocked) == 0 {
		return nil
	}
	out := make([]GCPressure, 0, len(im.gcBlocked))
	for key, state := range im.gcBlocked {
		out = append(out, GCPressure{
			KBID:            key.kbID,
			VersionID:       key.versionID,
			DeadShare:       state.deadShare,
			OthersServing:   state.others,
			MinimumRequired: state.minimum,
			Since:           state.since,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].KBID != out[j].KBID {
			return out[i].KBID < out[j].KBID
		}
		return out[i].VersionID < out[j].VersionID
	})
	return out
}

// collectGraphFree reopens a sealed GRAPH-FREE artifact, drops the dead vectors
// and reseals it.
//
// Graph-free only: faiss can remove from the IndexFlatCodes family but not from
// HNSW (§8.6c's verification table), and a graphed artifact's collection is a full
// rebuild instead (§8.6(d) phase 3). The shape check happens BEFORE the reopen, so
// a graphed version is never even taken out of service.
//
// Failure is safe by construction. `Save` is the only step that makes the new
// artifact visible, and it is atomic (.tmp + rename + CRC32), so an error before
// it leaves the previous artifact exactly as it was — "it did not get collected
// this time", not "it is broken now". The version IS out of service on this node
// for the duration (see Search / ErrIndexMaintenance); that is why the caller
// checked the serving count first, and why the flag is cleared on every path out.
func (im *IndexManagerImpl) collectGraphFree(ctx context.Context, kbID string, versionID int64, dead []string) error {
	key := indexKey{kbID, versionID}

	graphFree, known := im.shapeGraphFree(kbID, versionID)
	if !known || !graphFree {
		return fmt.Errorf("%w: version %d is not known to be graph-free on this node, refusing to reopen it",
			stratumerrors.ErrInvalidArgument, versionID)
	}
	// Both checks above are "may I do this", and they come before the reopen: a
	// version whose shape cannot be collected is never disturbed at all.
	if im.vectorIndexClient == nil {
		return errors.New("index: gc: no vecstore client is wired on this node")
	}

	im.setMaintenance(key, true)
	defer im.setMaintenance(key, false)

	path := im.indexPath(kbID, versionID)
	if _, err := im.vectorIndexClient.LoadForAppend(ctx, &vecstorepb.LoadIndexForAppendRequest{
		KbId: kbID, VersionId: versionID, Path: path,
	}); err != nil {
		return fmt.Errorf("index: gc: LoadForAppend: %w", err)
	}
	// Reopened, so it is BUILDING now — which is the only state RemoveChunks
	// accepts, and the reason the version had to be taken out of service.
	if _, err := im.removeDeadChunks(ctx, kbID, versionID, dead); err != nil {
		return fmt.Errorf("index: gc: RemoveChunks: %w", err)
	}
	if _, err := im.vectorIndexClient.Save(ctx, &vecstorepb.SaveIndexRequest{
		KbId: kbID, VersionId: versionID, Path: path,
	}); err != nil {
		return fmt.Errorf("index: gc: Save: %w", err)
	}
	return nil
}

// notifyArtifactRewritten tells the assembly that this version's artifact changed
// on disk, so the new bytes can be shipped to the replicas (§8.4).
//
// It reuses the build-completion callbacks on purpose: the assemblies already wire
// those to distributeIndex, and "the artifact is present and different" is the
// same instruction the §8.6a cold reshape gives through the same path. A version's
// state does not change here — it stays READY throughout — only its bytes do,
// which is exactly what distribution cares about.
func (im *IndexManagerImpl) notifyArtifactRewritten(kbID string, versionID int64) {
	im.mu.Lock()
	callbacks := append([]BuildCompleteCallback(nil), im.callbacks...)
	im.mu.Unlock()

	for _, cb := range callbacks {
		if cb == nil {
			continue
		}
		if err := cb(kbID, versionID, types.IndexStatusReady); err != nil {
			im.logger.Warn("index: gc: artifact-changed notification failed; replicas keep the previous artifact",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}
}
