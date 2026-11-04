package plane

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

// chainTailMeta is metadata that counts whole-chain reads and lists its knowledge bases.
type chainTailMeta struct {
	*stubMeta
	kbs               []types.KnowledgeBaseMeta
	listErr           error
	listVersionsCalls int
}

func (c *chainTailMeta) ListVersions(ctx context.Context, kbID string) ([]types.VersionMeta, error) {
	c.listVersionsCalls++
	return c.stubMeta.ListVersions(ctx, kbID)
}

func (c *chainTailMeta) ListKnowledgeBases(context.Context) ([]types.KnowledgeBaseMeta, error) {
	return c.kbs, c.listErr
}

// TestLocalControlPlane_ChainTailsReadsTheMetadata: the knowledge bases come from the
// metadata, so a chain nobody has reported about is still named, and each tail is the
// extreme value rather than a whole-chain read.
func TestLocalControlPlane_ChainTailsReadsTheMetadata(t *testing.T) {
	meta := &chainTailMeta{
		stubMeta: &stubMeta{versions: map[string][]types.VersionMeta{
			"kb-1": {
				{KBID: "kb-1", VersionID: 3},
				{KBID: "kb-1", VersionID: 9},
				{KBID: "kb-1", VersionID: 7},
			},
		}},
		kbs: []types.KnowledgeBaseMeta{{KBID: "kb-1"}, {KBID: "kb-empty"}},
	}
	cp := NewLocalControlPlane(meta)

	tails, err := cp.ChainTails(context.Background())
	if err != nil {
		t.Fatalf("ChainTails: %v", err)
	}
	if tails["kb-1"] != 9 {
		t.Errorf("tails[kb-1] = %d, want 9", tails["kb-1"])
	}
	// No versions is "no signal", never "nothing to catch up".
	if _, present := tails["kb-empty"]; present {
		t.Error("a knowledge base with no versions must be left out, not reported at zero")
	}
	if meta.listVersionsCalls != 0 {
		t.Errorf("ListVersions calls = %d, want 0: the tail is an extreme-value read", meta.listVersionsCalls)
	}
}

// A list that cannot be read must surface: an empty answer would read as "no chains".
func TestLocalControlPlane_ChainTailsReportsAnUnreadableList(t *testing.T) {
	meta := &chainTailMeta{stubMeta: &stubMeta{}, listErr: errors.New("no quorum")}
	if _, err := NewLocalControlPlane(meta).ChainTails(context.Background()); err == nil {
		t.Fatal("want the read failure, not an empty answer")
	}
}

// A shape that cannot name a generation offers no signal rather than a made-up one.
func TestLocalControlPlane_ChainGenerationNeedsASource(t *testing.T) {
	if _, _, ok := NewLocalControlPlane(&chainTailMeta{stubMeta: &stubMeta{}}).ChainGeneration(); ok {
		t.Fatal("this metadata cannot name a generation")
	}
	if _, _, ok := NewLocalControlPlane(generationMeta{stubMeta: &stubMeta{}}).ChainGeneration(); !ok {
		t.Fatal("this metadata can")
	}
}

type generationMeta struct{ *stubMeta }

func (generationMeta) ChainGeneration() (uint64, uint64) { return 3, 7 }
