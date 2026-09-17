package plane

import (
	"context"
	"errors"
	"testing"

	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
)

// stubMetadata is a MetadataLister over a fixed set of versions.
type stubMetadata struct {
	kbs      []types.KnowledgeBaseMeta
	versions map[string][]types.VersionMeta
	err      error
}

func (m *stubMetadata) ListKnowledgeBases(context.Context) ([]types.KnowledgeBaseMeta, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.kbs, nil
}

func (m *stubMetadata) ListVersions(_ context.Context, kbID string) ([]types.VersionMeta, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.versions[kbID], nil
}

// TestLocalDataPlane_RecoverLocalCursors pins the fix for the restart hole the
// 3+3 stress run found (TestT4_QueryLatency): the contiguous cursor lives in
// memory (§7.8), so a restarted storage node answers "0" for every knowledge
// base while its disk still holds the data — and the station's freshness check
// (§9.3(2)) then refuses every query routed to it with "local history reaches
// version 0, below the required N".
//
// The knowledge base here is the ordinary shape: an initial version with no
// document set (and therefore no artifact — the index manager answers "version
// has no chunks; nothing to build"), followed by versions whose artifacts are
// on this node. The recovery must walk past the empty version, not stop at it.
func TestLocalDataPlane_RecoverLocalCursors(t *testing.T) {
	empty := stratinternalsync.ComputeDocIDSetHash(nil)
	const kbID = "kb-restart"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{exists: map[int64]bool{11: true, 12: true, 13: true}},
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 10, DocIDSetHash: empty},
			{KBID: kbID, VersionID: 11, DocIDSetHash: "h11"},
			{KBID: kbID, VersionID: 12, DocIDSetHash: "h12"},
			{KBID: kbID, VersionID: 13, DocIDSetHash: "h13"},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 13 {
		t.Fatalf("cursor = %d, want 13 — every version's data is on this node", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_StopsAtAGap pins the direction the
// cursor must fail in: it is a claim that this node's history is UNBROKEN to
// here, so a version whose artifact is absent stops it. Claiming a version the
// node cannot serve would trade a refusal for an incomplete answer, which is
// the silent staleness §9.1 risk 1 exists to prevent.
func TestLocalDataPlane_RecoverLocalCursors_StopsAtAGap(t *testing.T) {
	empty := stratinternalsync.ComputeDocIDSetHash(nil)
	const kbID = "kb-gap"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{exists: map[int64]bool{21: true}},
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 20, DocIDSetHash: empty},
			{KBID: kbID, VersionID: 21, DocIDSetHash: "h21"},
			{KBID: kbID, VersionID: 22, DocIDSetHash: "h22"}, // no local artifact
			{KBID: kbID, VersionID: 23, DocIDSetHash: "h23"},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 21 {
		t.Fatalf("cursor = %d, want 21 — 22 has no local artifact, so 23 must not be claimed", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_NoEvidenceNoClaim covers the node that
// holds nothing for the knowledge base: a version with a document set and no
// artifact is not held, so the cursor stays at 0 rather than inventing a claim.
func TestLocalDataPlane_RecoverLocalCursors_NoEvidenceNoClaim(t *testing.T) {
	const kbID = "kb-nothing"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 31, DocIDSetHash: "h31"},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor = %d, want 0 — nothing local backs a claim", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_NeverMovesBackwards keeps the recovery
// from undoing a cursor that is already ahead: it runs at startup, but a node
// that has been serving writes must not be pushed back by a disk view that is
// merely conservative.
func TestLocalDataPlane_RecoverLocalCursors_NeverMovesBackwards(t *testing.T) {
	const kbID = "kb-ahead"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{exists: map[int64]bool{41: true}},
	})
	dp.MarkVersionContiguous(kbID, 44)
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 41, DocIDSetHash: "h41"},
			{KBID: kbID, VersionID: 42, DocIDSetHash: "h42"},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 44 {
		t.Fatalf("cursor = %d, want 44 — recovery must never move the cursor back", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_EmptyInitialVersionIsHeld covers the exact
// shape the 3+3 cluster has: every knowledge base's first version is created with no
// changes, so the control layer records the EMPTY set's digest for it, and no artifact
// is built for it (the index manager answers "version has no chunks; nothing to
// build"). That digest is what identifies it as a version with no content rather than
// one this node never received — and, unlike the pair it replaced ("durable, no
// digest"), it cannot be produced by a version that does have content.
func TestLocalDataPlane_RecoverLocalCursors_EmptyInitialVersionIsHeld(t *testing.T) {
	const kbID = "kb-empty-head"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{exists: map[int64]bool{61: true, 62: true}},
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			// Created with no changes: the empty set's digest, no artifact, durably replicated.
			{KBID: kbID, VersionID: 60, DocIDSetHash: types.EmptyDocIDSetHash, DataStatus: types.DataStatusDurable},
			{KBID: kbID, VersionID: 61, DocIDSetHash: "h61"},
			{KBID: kbID, VersionID: 62, DocIDSetHash: "h62"},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 62 {
		t.Fatalf("cursor = %d, want 62 — the empty initial version must not break the chain", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_EmptyKnowledgeBaseIsServable pins the shape a
// 3+3 cluster actually produced (KB datavolume-178944190-11): a knowledge base holding
// nothing but empty versions — both durably replicated, both with parent 0, the empty
// set's digest on both, and not one artifact on any replica.
//
// Before, "no artifact" was read as "never received" whenever the knowledge base
// had no footprint at all, so every replica reported cursor 0 while the station
// demanded 71: `local history reaches version 0, below the required 71` on all
// three, forever. Nothing would ever build an index for a version with no
// chunks, so no later write could advance the cursor either — the replicas were
// unreachable for a knowledge base they could answer (with an empty result set).
func TestLocalDataPlane_RecoverLocalCursors_EmptyKnowledgeBaseIsServable(t *testing.T) {
	const kbID = "kb-empty"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{}, // no artifact anywhere
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 71, DocIDSetHash: types.EmptyDocIDSetHash, DataStatus: types.DataStatusDurable},
			{KBID: kbID, VersionID: 72, DocIDSetHash: types.EmptyDocIDSetHash, DataStatus: types.DataStatusDurable},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 72 {
		t.Fatalf("cursor = %d, want 72 — an empty knowledge base has no artifact anywhere and must still serve", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_DurableWithoutDigestIsNotAHold is the
// regression the two states above were untangled for.
//
// "Durable with no digest" looks like an empty version and is NOT one: a version
// carrying real documents whose digest was never committed has exactly that pair,
// because a cursor promotion (§7.9) settles the data side without a digest. Reading it
// as "held here" is what let a replica that had missed ONE version report the whole
// chain as its own cursor — and then skip the fetch it needed, since EnsureIndex treats
// "the cursor already reaches it" as "nothing to do".
//
// Measured in the 3+3 cluster: a storage node came back from a restart reporting v6
// while its disk held only v5, and never pulled v6.
func TestLocalDataPlane_RecoverLocalCursors_DurableWithoutDigestIsNotAHold(t *testing.T) {
	const kbID = "kb-uncommitted-digest"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{exists: map[int64]bool{10: true}},
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 10, DocIDSetHash: types.EmptyDocIDSetHash, DataStatus: types.DataStatusDurable},
			// Real documents, digest never committed, no local artifact: NOT held.
			{KBID: kbID, VersionID: 11, DocIDSetHash: "", DataStatus: types.DataStatusDurable},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 10 {
		t.Fatalf("cursor = %d, want 10: a durable version with no committed digest and no local "+
			"artifact must NOT be claimed — claiming it makes the node skip the very fetch that "+
			"would give it the data", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_ShortFanOutIsNotAHold is the regression
// that replaced the index-READY test with a data-side one.
//
// A version whose fan-out fell short of quorum has NO committed digest — by
// design, not by accident (WriteVersionData: "making it without quorum would be a
// lie the control layer acts on") — while its INDEX still reaches READY on every
// replica that builds it. Reading index-READY as "held here" therefore claims a
// version whose records never arrived, and since the claim is made per version the
// cursor does not come out one too high: it runs to the chain tail.
//
// Measured on a 3-node cluster before this: a replica offline for 55 versions came
// back reporting the tail as its own cursor, so it never looked behind. The same
// overclaim is what makes the station's freshness check trust a replica whose data
// is incomplete.
func TestLocalDataPlane_RecoverLocalCursors_ShortFanOutIsNotAHold(t *testing.T) {
	const kbID = "kb-short-fanout"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{}, // no artifact anywhere
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			// Built here (index READY) but never durably replicated: no digest, and
			// the data side never left PENDING (its zero value).
			{KBID: kbID, VersionID: 80, DocIDSetHash: "", IndexStatus: types.IndexStatusReady},
			{KBID: kbID, VersionID: 81, DocIDSetHash: "", IndexStatus: types.IndexStatusReady},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor = %d, want 0 — index-READY without durable data backs no claim", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_UnreadyVersionsAreNotClaimed is the
// conservative half: a version still being written (PENDING — the zero value of
// IndexStatus) with no artifact and no digest is NOT claimed. It may hold records
// this node does not have, and a too-high cursor would turn a refusal into an
// incomplete answer.
func TestLocalDataPlane_RecoverLocalCursors_UnreadyVersionsAreNotClaimed(t *testing.T) {
	const kbID = "kb-foreign"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{},
	})
	meta := &stubMetadata{
		kbs: []types.KnowledgeBaseMeta{{KBID: kbID}},
		versions: map[string][]types.VersionMeta{kbID: {
			{KBID: kbID, VersionID: 71, DocIDSetHash: "", IndexStatus: types.IndexStatusPending},
			{KBID: kbID, VersionID: 72, DocIDSetHash: "", IndexStatus: types.IndexStatusPending},
		}},
	}

	if err := dp.RecoverLocalCursors(context.Background(), meta); err != nil {
		t.Fatalf("RecoverLocalCursors: %v", err)
	}
	if got := dp.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor = %d, want 0 — a PENDING version with no artifact backs no claim", got)
	}
}

// TestLocalDataPlane_RecoverLocalCursors_SurvivesMetadataFailures pins the
// startup-path contract: a control layer that cannot be reached must not take
// the node down, and a per-KB failure must not stop the other knowledge bases
// from being recovered.
func TestLocalDataPlane_RecoverLocalCursors_SurvivesMetadataFailures(t *testing.T) {
	const kbID = "kb-ok"

	dp := NewLocalDataPlane(LocalDataPlaneConfig{
		IndexManager: &stubIndexStore{exists: map[int64]bool{51: true}},
	})

	if err := dp.RecoverLocalCursors(context.Background(), nil); err != nil {
		t.Fatalf("nil metadata must be a no-op, got %v", err)
	}

	errMeta := &stubMetadata{err: errors.New("control layer unreachable")}
	if err := dp.RecoverLocalCursors(context.Background(), errMeta); err == nil {
		t.Fatal("listing knowledge bases failing must surface as an error the caller can log")
	}
	if got := dp.LocalVersionOf(kbID); got != 0 {
		t.Fatalf("cursor = %d, want 0 — a failed list must not be read as a successful recovery", got)
	}
}
