// Quantizer selection and codebook refresh (§5.1, §6.3). The thresholds here are
// what turn "the codebook has drifted" into a rebuild instead of a silent loss of
// recall.
package index

import (
	"context"
	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/types"
)

// quantizerForKB maps a knowledge base's quantizer metadata to the
// vecstore Build RPC fields. OFF (or an unknown value) maps to no
// quantization, keeping the historical full-precision behavior.
func quantizerForKB(kb types.KnowledgeBaseMeta, graphFree bool) (vecstorepb.QuantizerTypeProto, int32, int32) {
	var q vecstorepb.QuantizerTypeProto
	switch kb.QuantizerType {
	case "SQ8":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8
	case "SQ_BF16":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_BF16
	case "SQ_FP16":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_FP16
	case "PQ":
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_PQ
	default:
		q = vecstorepb.QuantizerTypeProto_QUANTIZER_OFF
	}
	pqM, pqNBits := int32(0), int32(0)
	if kb.QuantizerType == "PQ" {
		pqM = int32(kb.QuantizerPQM)
		pqNBits = int32(kb.QuantizerPQNBits)
	}
	return graphFreeVariant(q, graphFree), pqM, pqNBits
}

// graphFreeVariant maps a quantizer to its graph-free twin (§8.6a). The
// quantizer itself is unchanged; only the HNSW graph is dropped.
func graphFreeVariant(q vecstorepb.QuantizerTypeProto, graphFree bool) vecstorepb.QuantizerTypeProto {
	if !graphFree {
		return q
	}
	switch q {
	case vecstorepb.QuantizerTypeProto_QUANTIZER_OFF:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_OFF_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_BF16:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_BF16_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_FP16:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_SQ_FP16_FLAT
	case vecstorepb.QuantizerTypeProto_QUANTIZER_PQ:
		return vecstorepb.QuantizerTypeProto_QUANTIZER_PQ_FLAT
	default:
		return q
	}
}

// buildQuantizerFromKB returns the vecstore Build RPC quantizer fields
// for kbID, consulting kbMetaGetter when set (default OFF otherwise).
func (im *IndexManagerImpl) buildQuantizerFromKB(ctx context.Context, kbID string, graphFree bool) (vecstorepb.QuantizerTypeProto, int32, int32) {
	if im.kbMetaGetter == nil {
		return graphFreeVariant(vecstorepb.QuantizerTypeProto_QUANTIZER_OFF, graphFree), 0, 0
	}
	kb, err := im.kbMetaGetter(ctx, kbID)
	if err != nil {
		return graphFreeVariant(vecstorepb.QuantizerTypeProto_QUANTIZER_OFF, graphFree), 0, 0
	}
	return quantizerForKB(kb, graphFree)
}

// appendMaxDeadRatio is the dead-vector share above which appendBase's reuse is
// abandoned in favour of a full rebuild. <= 0 means DefaultAppendMaxDeadRatio;
// 1.0 disables the check (always reuse when the reuse is otherwise legal).
func (im *IndexManagerImpl) appendMaxDeadRatio() float64 {
	if im.cfg.AppendMaxDeadRatio <= 0 {
		return DefaultAppendMaxDeadRatio
	}
	return im.cfg.AppendMaxDeadRatio
}

// codebookDriftTrigger / codebookAppendsTrigger resolve the two §3 triggers that
// retire a stale codebook (docs/codebook-refresh-plan.md). A NEGATIVE config
// value disables that trigger; 0 takes the default.
//
// 1.0 does NOT disable the ratio trigger, unlike AppendMaxDeadRatio's 1.0: a
// dead-vector share cannot exceed 1, but a growth ratio can (a codebook trained
// on N vectors can face 2N), so 1.0 merely means "the corpus must double". Use a
// negative value to switch a trigger off.
func (im *IndexManagerImpl) codebookDriftTrigger() (limit float64, enabled bool) {
	switch {
	case im.cfg.MaxCodebookDriftRatio < 0:
		return 0, false
	case im.cfg.MaxCodebookDriftRatio == 0:
		return DefaultMaxCodebookDriftRatio, true
	default:
		return im.cfg.MaxCodebookDriftRatio, true
	}
}

func (im *IndexManagerImpl) codebookAppendsTrigger() (limit int64, enabled bool) {
	switch {
	case im.cfg.MaxCodebookAppends < 0:
		return 0, false
	case im.cfg.MaxCodebookAppends == 0:
		return DefaultMaxCodebookAppends, true
	default:
		return im.cfg.MaxCodebookAppends, true
	}
}

// minCodebookBaselineVectors is the baseline size below which the cumulative
// ratio is not consulted (see DefaultMinCodebookBaselineVectors). <= 0 takes the
// default; a negative value removes the floor.
func (im *IndexManagerImpl) minCodebookBaselineVectors() int64 {
	if im.cfg.MinCodebookBaselineVectors < 0 {
		return 0
	}
	if im.cfg.MinCodebookBaselineVectors == 0 {
		return DefaultMinCodebookBaselineVectors
	}
	return im.cfg.MinCodebookBaselineVectors
}

// quantizerNeedsTraining reports whether this shape keeps a TRAINED codebook.
// Only those can drift: SQ8 learns a per-dimension range and PQ learns k-means
// centroids, while SQ_FP16 / SQ_BF16 are pure bit truncation (already trained at
// construction) and OFF has no codebook at all.
//
// This gate is what keeps the mechanism off every default deployment: OFF is the
// default quantizer, so an ungated trigger would buy a periodic full rebuild for
// exactly zero benefit. The drift benchmark measures the untrained shapes'
// codebook-drift cost as zero, which is why excluding them costs nothing.
func quantizerNeedsTraining(q vecstorepb.QuantizerTypeProto) bool {
	switch q {
	case vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8,
		vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8_FLAT,
		vecstorepb.QuantizerTypeProto_QUANTIZER_PQ,
		vecstorepb.QuantizerTypeProto_QUANTIZER_PQ_FLAT:
		return true
	}
	return false
}

// codebookRefreshNeeded decides whether this build should give up append reuse
// and rebuild from scratch — the only path that retrains the quantizer
// (docs/codebook-refresh-plan.md §3).
//
// Two triggers, either suffices:
//   - cumulative new-vector share: (totalChunks - trainedNtotal) / trainedNtotal
//   - append-reuses since training: the fallback, which also covers a KB that
//     adds a few vectors per version (the share never reaches the threshold but
//     the codebook still ages)
//
// The baseline is read from the PARENT version's sealed sidecar, where it rides
// along with the artifact — so it survives a restart and a §8.4 handoff with no
// Go-side state. Three cases return true with an explicit reason, and all three
// resolve after a single rebuild (which writes a baseline):
//   - the parent's sidecar has no baseline line (artifact from before §3)
//   - the parent's sidecar cannot be read
//   - the baseline is zero, which is meaningless as a denominator
//
// cumulativeRatio is returned even when no refresh is needed, so the caller can
// log how close a build is to the threshold.
func (im *IndexManagerImpl) codebookRefreshNeeded(
	ctx context.Context, kbID string, versionID int64,
	quantizerType vecstorepb.QuantizerTypeProto, totalChunks int,
) (needed bool, reason string, cumulativeRatio float64) {
	if !quantizerNeedsTraining(quantizerType) || im.versionParent == nil {
		return false, "", 0
	}
	parent, err := im.versionParent(ctx, kbID, versionID)
	if err != nil || parent <= 0 || parent == versionID {
		// No parent (or none can be looked up): this build trains a fresh
		// codebook anyway, so there is nothing stale to refresh.
		return false, "", 0
	}
	// The parent's artifact must be on THIS node. When it is not — ordinary in a
	// multi-node deployment, where a node may never have built the parent — that
	// is not a lost baseline but the case appendBase already handles by giving up
	// the reuse. Reporting "baseline unknown" here would send an operator looking
	// for a damaged artifact when the real reason is just "the artifact is not
	// here", so let appendBase do the attributing.
	if !fileExists(im.indexPath(kbID, parent)) || !fileExists(im.sidecarPath(kbID, parent)) {
		return false, "", 0
	}
	trainedNtotal, appends, known := im.artifactTrainedBaseline(kbID, parent)
	if !known || trainedNtotal <= 0 {
		return true, "baseline unknown", 0
	}
	if limit, enabled := im.codebookAppendsTrigger(); enabled && appends >= limit {
		return true, "appends since training", 0
	}
	ratio := (float64(totalChunks) - float64(trainedNtotal)) / float64(trainedNtotal)
	if floor := im.minCodebookBaselineVectors(); trainedNtotal < floor {
		// Baseline too small for the ratio to mean anything (see
		// minCodebookBaselineVectors): report the ratio, decide nothing.
		return false, "", ratio
	}
	if limit, enabled := im.codebookDriftTrigger(); enabled && ratio >= limit {
		return true, "cumulative drift ratio", ratio
	}
	return false, "", ratio
}
