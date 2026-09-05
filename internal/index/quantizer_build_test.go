package index

// quantizer_build_test.go — verifies that async builds forward the
// KB-level quantizer configuration (read via SetKBMetaGetter) in the
// vecstore Build RPC (Stratum_设计文档v12.md 2.4 "透传路径").

import (
	"context"
	"testing"
	"time"

	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/types"
)

func testIndexManagerWithEmptyKB(t *testing.T) *IndexManagerImpl {
	t.Helper()
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
	})
	im.vectorIndexClient = newMockVectorIndexClient()
	// Empty version: the build still issues one (empty) Build RPC, which
	// carries the quantizer fields — enough to assert config forwarding.
	im.SetBuildDataSources(
		func(_ context.Context, _ string, _ int64) ([]string, error) { return nil, nil },
		func(_ context.Context, _ string, _ []string) ([]string, error) { return nil, nil },
		func(_ context.Context, _, _ string) ([]float32, error) { return nil, nil },
	)
	return im
}

func TestBuildForwardsKBQuantizerConfig(t *testing.T) {
	im := testIndexManagerWithEmptyKB(t)
	im.SetKBMetaGetter(func(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
		return types.KnowledgeBaseMeta{KBID: kbID, QuantizerType: "SQ8"}, nil
	})

	if _, err := im.buildWithRetry("kb-quant", 1); err != nil {
		t.Fatalf("buildWithRetry: %v", err)
	}
	vc := im.vectorIndexClient.(*mockVectorIndexClient)
	if vc.buildCalls != 1 {
		t.Fatalf("buildCalls = %d, want 1", vc.buildCalls)
	}
	if vc.lastBuildQuantizer != vecstorepb.QuantizerTypeProto_QUANTIZER_SQ8 {
		t.Errorf("Build quantizer = %v, want QUANTIZER_SQ8", vc.lastBuildQuantizer)
	}
}

func TestBuildForwardsPQParams(t *testing.T) {
	im := testIndexManagerWithEmptyKB(t)
	im.SetKBMetaGetter(func(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
		return types.KnowledgeBaseMeta{
			KBID: kbID, QuantizerType: "PQ", QuantizerPQM: 32, QuantizerPQNBits: 8,
		}, nil
	})

	if _, err := im.buildWithRetry("kb-pq", 1); err != nil {
		t.Fatalf("buildWithRetry: %v", err)
	}
	vc := im.vectorIndexClient.(*mockVectorIndexClient)
	if vc.lastBuildQuantizer != vecstorepb.QuantizerTypeProto_QUANTIZER_PQ {
		t.Errorf("Build quantizer = %v, want QUANTIZER_PQ", vc.lastBuildQuantizer)
	}
	if vc.lastBuildPqM != 32 || vc.lastBuildPqNbits != 8 {
		t.Errorf("Build pq_m/pq_nbits = %d/%d, want 32/8", vc.lastBuildPqM, vc.lastBuildPqNbits)
	}
}

func TestBuildDefaultsToOffWithoutKBMeta(t *testing.T) {
	im := testIndexManagerWithEmptyKB(t)
	// No SetKBMetaGetter and no meta registered: OFF default.
	if _, err := im.buildWithRetry("kb-plain", 1); err != nil {
		t.Fatalf("buildWithRetry: %v", err)
	}
	vc := im.vectorIndexClient.(*mockVectorIndexClient)
	if vc.lastBuildQuantizer != vecstorepb.QuantizerTypeProto_QUANTIZER_OFF {
		t.Errorf("Build quantizer = %v, want QUANTIZER_OFF (default)", vc.lastBuildQuantizer)
	}
}

// newIndexManagerWithData returns an IndexManager whose build data flow
// yields exactly one chunk ("c1", 4 dims), so a single Build RPC fires.
func newIndexManagerWithData(t *testing.T) *IndexManagerImpl {
	t.Helper()
	im := NewIndexManager(IndexManagerConfig{
		LRUCapacity:     4,
		LoadWaitTimeout: 5 * time.Second,
		IndexDataDir:    t.TempDir(),
	})
	im.vectorIndexClient = newMockVectorIndexClient()
	im.SetBuildDataSources(
		func(_ context.Context, _ string, _ int64) ([]string, error) { return []string{"d1"}, nil },
		func(_ context.Context, _ string, _ []string) ([]string, error) { return []string{"c1"}, nil },
		func(_ context.Context, _, _ string) ([]float32, error) { return []float32{1, 1, 1, 1}, nil },
	)
	return im
}

// TestBuildAccountingPrefersReportedMemForQuantized verifies v12.md 3.3:
// for quantized KBs, the vecstore-reported memory estimate (coarse
// retriever basis) replaces the historical 4×d×n payload estimate in the
// returned size (which feeds .index.mem and LRU byte accounting).
func TestBuildAccountingPrefersReportedMemForQuantized(t *testing.T) {
	im := newIndexManagerWithData(t)
	vc := im.vectorIndexClient.(*mockVectorIndexClient)
	vc.memToReport = 50000
	im.SetKBMetaGetter(func(_ context.Context, kbID string) (types.KnowledgeBaseMeta, error) {
		return types.KnowledgeBaseMeta{KBID: kbID, QuantizerType: "SQ8"}, nil
	})

	size, err := im.buildWithRetry("kb-sq8", 1)
	if err != nil {
		t.Fatalf("buildWithRetry: %v", err)
	}
	if size != 50000 {
		t.Errorf("accounted size = %d, want vecstore-reported 50000", size)
	}
}

// TestBuildAccountingKeepsPayloadEstimateForOff verifies the OFF path is
// unchanged: reported mem is ignored and the historical 4×d×n payload
// estimate (here 4 dims × 1 chunk × 4 bytes = 16) is used.
func TestBuildAccountingKeepsPayloadEstimateForOff(t *testing.T) {
	im := newIndexManagerWithData(t)
	vc := im.vectorIndexClient.(*mockVectorIndexClient)
	vc.memToReport = 50000 // must be ignored for OFF KBs

	size, err := im.buildWithRetry("kb-plain", 1)
	if err != nil {
		t.Fatalf("buildWithRetry: %v", err)
	}
	if size != 16 {
		t.Errorf("accounted size = %d, want historical payload estimate 16", size)
	}
}
