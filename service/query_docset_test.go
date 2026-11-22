package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	pb "stratum/api/proto/stratum"
	"stratum/internal/chunkdoc"
	"stratum/internal/docstore"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/index"
	"stratum/internal/raft"
	stratinternalsync "stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/versionview"
	"stratum/internal/wal"
)

type countingQueryVDL struct {
	versiondoc.VersionDocList
	calls int
}

func (c *countingQueryVDL) ListDocIDs(ctx context.Context, kbID string, versionID int64) ([]string, error) {
	c.calls++
	return c.VersionDocList.ListDocIDs(ctx, kbID, versionID)
}

type failingQueryVDL struct {
	versiondoc.VersionDocList
	err error
}

func (f *failingQueryVDL) ListDocIDs(context.Context, string, int64) ([]string, error) {
	return nil, f.err
}

type stagedQueryVDL struct {
	stages [][]string
	calls  int
}

func (s *stagedQueryVDL) Write(context.Context, string, int64, string) error       { return nil }
func (s *stagedQueryVDL) ListVersions(context.Context, string) ([]int64, error)    { return nil, nil }
func (s *stagedQueryVDL) DeleteByVersion(context.Context, string, int64) error     { return nil }
func (s *stagedQueryVDL) DeleteByKB(context.Context, string) error                 { return nil }
func (s *stagedQueryVDL) WriteMany(context.Context, string, int64, []string) error { return nil }
func (s *stagedQueryVDL) ListDocIDs(context.Context, string, int64) ([]string, error) {
	i := s.calls
	if i >= len(s.stages) {
		i = len(s.stages) - 1
	}
	s.calls++
	return s.stages[i], nil
}

func TestQueryService_WarmQueryHitsL0AndEmitsVcmHit(t *testing.T) {
	ctx := context.Background()
	core, logs := observer.New(zapcore.DebugLevel)
	h := newQuerySvcHarness(t, WithQueryLogger(zap.New(core)))
	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
		"doc-b": {"chunk-2"},
	})

	ids, err := h.versionDocs.ListDocIDs(ctx, kbID, vID)
	if err != nil {
		t.Fatalf("ListDocIDs: %v", err)
	}
	if err := h.raftNode.ProposeUpdateVersionSummary(ctx, vID, stratinternalsync.ComputeDocIDSetHash(ids)); err != nil {
		t.Fatalf("ProposeUpdateVersionSummary: %v", err)
	}

	for i := 0; i < 3; i++ {
		resp, err := h.svc.Query(ctx, &pb.QueryRequest{
			KnowledgeBaseId: kbID,
			VersionId:       &vID,
			Vector:          []float32{0.1, 0.2, 0.3, 0.4},
			TopK:            2,
		})
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if len(resp.Results) != 2 {
			t.Fatalf("query %d results = %d, want 2", i, len(resp.Results))
		}
	}

	st := h.versionViews.Stats()
	if st.L2Builds != 1 || st.L0Hits != 2 {
		t.Fatalf("versionview stats = %+v, want L2Builds=1 L0Hits=2", st)
	}

	entries := logs.FilterMessage("query: stage timings").All()
	if len(entries) != 3 {
		t.Fatalf("expected 3 stage timing log lines, got %d", len(entries))
	}
	if got := entries[0].ContextMap()["vcm_hit"]; got != "l2" {
		t.Fatalf("first query vcm_hit = %v, want l2", got)
	}
	if got := entries[1].ContextMap()["vcm_hit"]; got != "l0" {
		t.Fatalf("second query vcm_hit = %v, want l0", got)
	}
	if got := entries[1].ContextMap()["chunkmap_calls"]; got != int64(0) {
		t.Fatalf("warm query chunkmap_calls = %v, want 0", got)
	}
}

func TestQueryService_UnverifiedAndEarlyEmptyNeverCached(t *testing.T) {
	ctx := context.Background()
	full := []string{"d1", "d2", "d3"}
	digest := stratinternalsync.ComputeDocIDSetHash(full)

	staged := &stagedQueryVDL{stages: [][]string{nil, full}}
	cdm := chunkdoc.NewMockChunkDocMapper()
	store := versionview.NewStore(versionview.Config{
		DataDir:        t.TempDir(),
		MaxEntries:     4,
		VersionDocList: staged,
		ChunkDocMapper: cdm,
	})

	if _, err := store.Get(ctx, "kb-1", 7, digest); !errors.Is(err, stratumerrors.ErrIndexNotReady) {
		t.Fatalf("early empty lookup = %v, want ErrIndexNotReady", err)
	}
	v, src, err := store.Lookup(ctx, "kb-1", 7, digest)
	if err != nil {
		t.Fatalf("lookup after data arrived: %v", err)
	}
	if src != versionview.HitL2 || len(v.DocIDs) != 3 {
		t.Fatalf("got (%s, %v), want l2 with 3 docs", src, v.DocIDs)
	}
	before := staged.calls
	if _, src, err := store.Lookup(ctx, "kb-1", 7, digest); err != nil || src != versionview.HitL0 || staged.calls != before {
		t.Fatalf("cached lookup = (%s, %v, calls %d->%d)", src, err, before, staged.calls)
	}

	// Uncommitted digest ("") must not populate L0.
	cvdl := &countingQueryVDL{VersionDocList: versiondoc.NewMockVersionDocList()}
	_ = cvdl.Write(ctx, "kb-1", 9, "d1")
	uncached := versionview.NewStore(versionview.Config{
		DataDir:        t.TempDir(),
		MaxEntries:     4,
		VersionDocList: cvdl,
		ChunkDocMapper: cdm,
	})
	for i := 0; i < 2; i++ {
		if _, err := uncached.Get(ctx, "kb-1", 9, ""); err != nil {
			t.Fatalf("uncommitted lookup %d: %v", i, err)
		}
	}
	if cvdl.calls != 2 || uncached.Stats().Entries != 0 {
		t.Fatalf("uncommitted digest must not cache: calls=%d entries=%d", cvdl.calls, uncached.Stats().Entries)
	}
}

func TestQueryService_VDLIOErrorFallsBackToChunkDocMapper(t *testing.T) {
	ctx := context.Background()
	h := newQuerySvcHarness(t)
	kbID, vID := h.setupQueryableKB(t, map[string][]string{
		"doc-a": {"chunk-1"},
	})

	failingVDL := &failingQueryVDL{VersionDocList: h.versionDocs, err: errors.New("disk read error")}
	h.svc.versionViews = versionview.NewStore(versionview.Config{
		DataDir:        t.TempDir(),
		MaxEntries:     4,
		VersionDocList: failingVDL,
		ChunkDocMapper: h.chunkMapper,
	})

	resp, err := h.svc.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &vID,
		Vector:          []float32{0.1, 0.2, 0.3, 0.4},
		TopK:            2,
	})
	if err != nil {
		t.Fatalf("Query fallback on VDL error failed: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].DocId != "doc-a" {
		t.Fatalf("fallback results = %+v, want doc-a", resp.Results)
	}
}

func BenchmarkQuery(b *testing.B) {
	for _, docCount := range []int{2000, 8000} {
		b.Run(fmt.Sprintf("docs=%d", docCount), func(b *testing.B) {
			ctx := context.Background()
			w := wal.NewMockWAL()
			rn := raft.NewMockRaftNode(w)
			cdm := chunkdoc.NewMockChunkDocMapper()
			vd := versiondoc.NewMockVersionDocList()
			ds := docstore.NewMockDocStore()

			im := index.NewMockIndexManager(index.MockIndexManagerDeps{
				ListDocIDs:         vd.ListDocIDs,
				ListChunkIDsByDocs: cdm.ListChunkIDsByDocs,
				ReadChunkVector: func(context.Context, string, string) ([]float32, error) {
					return []float32{0.1, 0.2, 0.3, 0.4}, nil
				},
			}, 4, 5*time.Second)

			const kbID = "kb-bench"
			_ = rn.ProposeCreateKB(ctx, types.KnowledgeBaseMeta{
				KBID: kbID, Name: "bench",
				ChunkWindowSize: 512, ChunkOverlapSize: 64,
				EmbedConfig: types.EmbedConfig{ServiceAddr: "x", ModelID: "m1"},
				Status:      types.KBStatusActive,
			})
			vID, _ := rn.ProposeCreateVersion(ctx, kbID, 0)
			docIDs := make([]string, docCount)
			for i := 0; i < docCount; i++ {
				docID := fmt.Sprintf("doc-%05d", i)
				docIDs[i] = docID
				chunkID := fmt.Sprintf("chunk-%02d", i%24)
				_ = ds.Write(ctx, kbID, docID, vID, []byte("body"))
				_ = cdm.Write(ctx, kbID, chunkID, docID)
			}
			_ = vd.WriteMany(ctx, kbID, vID, docIDs)
			_ = rn.ProposeUpdateVersionSummary(ctx, vID, stratinternalsync.ComputeDocIDSetHash(docIDs))
			_ = im.TriggerBuild(ctx, kbID, vID)
			for !im.Resident(kbID, vID) {
				time.Sleep(time.Millisecond)
			}
			_ = rn.ProposeUpdateVersionStatus(ctx, vID, types.IndexStatusReady, 0)
			_ = rn.ProposeMarkVersionDataDurable(ctx, vID)

			views := versionview.NewStore(versionview.Config{
				DataDir:        b.TempDir(),
				MaxEntries:     8,
				VersionDocList: vd,
				ChunkDocMapper: cdm,
			})
			svc := NewQueryService(rn, im, cdm, vd, ds, views)
			req := &pb.QueryRequest{
				KnowledgeBaseId: kbID,
				VersionId:       &vID,
				Vector:          []float32{0.1, 0.2, 0.3, 0.4},
				TopK:            10,
			}
			if _, err := svc.Query(ctx, req); err != nil {
				b.Fatalf("warmup Query: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := svc.Query(ctx, req); err != nil {
					b.Fatalf("Query: %v", err)
				}
			}
		})
	}
}
