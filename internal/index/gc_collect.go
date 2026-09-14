package index

import (
	"context"
	"errors"
	"fmt"

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

		if err := im.collectGraphFree(ctx, c.KBID, c.VersionID, dead); err != nil {
			im.logger.Warn("index: gc: collection failed; the artifact is unchanged",
				zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID),
				zap.Int("dead_chunks", len(dead)), zap.Error(err))
			continue
		}
		im.logger.Info("index: gc: collected dead vectors from a sealed artifact",
			zap.String("kb_id", c.KBID), zap.Int64("version_id", c.VersionID),
			zap.Int("dead_chunks", len(dead)), zap.Float64("was_dead_share", c.DeadShare))
		im.notifyArtifactRewritten(c.KBID, c.VersionID)
	}
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

	im.mu.Lock()
	graphFree := im.builtGraphFree[key]
	im.mu.Unlock()
	if !graphFree {
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
