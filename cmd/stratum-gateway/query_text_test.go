package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
)

// The text-search endpoint exists so a caller with no embedder of its own can ask
// in words. What has to hold: the embedding uses the KNOWLEDGE BASE's config (not
// anything from the request), the resulting vector is what gets queried, and an
// unreachable embedder is an error rather than a silently meaningless answer.

// fakeEmbedService is the over-the-wire embed service the gateway calls.
type fakeEmbedService struct {
	gotIDs       []string
	gotContents  []string
	unavailable  bool
	vectorWanted []float32
}

func (f *fakeEmbedService) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if f.unavailable {
			http.Error(w, "embedder down", http.StatusInternalServerError)
			return
		}
		var body struct {
			Chunks []struct {
				ChunkID string `json:"chunk_id"`
				Content string `json:"content"`
			} `json:"chunks"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		vectors := map[string][]float32{}
		for _, ch := range body.Chunks {
			f.gotIDs = append(f.gotIDs, ch.ChunkID)
			f.gotContents = append(f.gotContents, ch.Content)
			vec := f.vectorWanted
			if vec == nil {
				vec = []float32{0.5, 0.25}
			}
			vectors[ch.ChunkID] = vec
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"vectors": vectors})
	}
}

// fakeControlTier answers the two calls /api/query-text makes.
type fakeControlTier struct {
	pb.UnimplementedKnowledgeBaseServiceServer
	pb.UnimplementedQueryServiceServer

	embedAddr string
	modelID   string

	querySeen *pb.QueryRequest
}

func (f *fakeControlTier) GetKnowledgeBase(context.Context, *pb.GetKnowledgeBaseRequest) (*pb.GetKnowledgeBaseResponse, error) {
	return &pb.GetKnowledgeBaseResponse{KnowledgeBase: &pb.KnowledgeBaseInfo{
		KnowledgeBaseId: "kb-1",
		EmbedConfig:     &pb.EmbedConfig{ServiceAddr: f.embedAddr, ModelId: f.modelID},
	}}, nil
}

func (f *fakeControlTier) Query(_ context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	f.querySeen = req
	return &pb.QueryResponse{
		VersionId: 7,
		Results:   []*pb.QueryResult{{DocId: "doc-1", Content: "命中", Score: 0.9}},
	}, nil
}

func newTestGateway(t *testing.T, tier *fakeControlTier) *gateway {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(srv, tier)
	pb.RegisterQueryServiceServer(srv, tier)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &gateway{
		kb:    pb.NewKnowledgeBaseServiceClient(conn),
		query: pb.NewQueryServiceClient(conn),
	}
}

func postQueryText(t *testing.T, g *gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/query-text", strings.NewReader(body))
	rec := httptest.NewRecorder()
	g.handleQueryText(rec, req)
	return rec
}

func TestQueryText_EmbedsWithTheKnowledgeBasesOwnConfig(t *testing.T) {
	embed := &fakeEmbedService{}
	srv := httptest.NewServer(embed.handler())
	defer srv.Close()

	tier := &fakeControlTier{embedAddr: srv.URL, modelID: "mock-embed-v1"}
	g := newTestGateway(t, tier)

	rec := postQueryText(t, g, `{"knowledge_base_id":"kb-1","text":"什么是回滚","top_k":3}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	// The embed call used the KB's model id for the chunk id (SHA-256(text+model)),
	// and carried the query text itself.
	if len(embed.gotContents) != 1 || embed.gotContents[0] != "什么是回滚" {
		t.Errorf("embedder saw contents %v, want the query text", embed.gotContents)
	}
	wantID := queryChunkID("什么是回滚", "mock-embed-v1")
	if len(embed.gotIDs) != 1 || embed.gotIDs[0] != wantID {
		t.Errorf("embedder saw chunk id %v, want %s (SHA-256(text+model_id))", embed.gotIDs, wantID)
	}

	if tier.querySeen == nil {
		t.Fatal("QueryService was never called")
	}
	if got := tier.querySeen.GetTopK(); got != 3 {
		t.Errorf("top_k = %d, want 3", got)
	}
	if got := tier.querySeen.GetVector(); len(got) != 2 || got[0] != 0.5 {
		t.Errorf("vector = %v, want the embedder's answer", got)
	}
	if tier.querySeen.GetKnowledgeBaseId() != "kb-1" {
		t.Errorf("kb_id = %q, want kb-1", tier.querySeen.GetKnowledgeBaseId())
	}

	// The response is /api/query's shape, so the console reads one thing whatever
	// it searched with.
	var out struct {
		VersionID string `json:"version_id"`
		Results   []struct {
			DocID string `json:"doc_id"`
		} `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, rec.Body.String())
	}
	if out.VersionID != "7" || len(out.Results) != 1 || out.Results[0].DocID != "doc-1" {
		t.Errorf("response = %s, want version 7 with doc-1", rec.Body.String())
	}
}

func TestQueryText_PassesThroughTheOptionalFields(t *testing.T) {
	embed := &fakeEmbedService{}
	srv := httptest.NewServer(embed.handler())
	defer srv.Close()

	tier := &fakeControlTier{embedAddr: srv.URL, modelID: "m"}
	g := newTestGateway(t, tier)

	rec := postQueryText(t, g, `{"knowledge_base_id":"kb-1","text":"x","top_k":5,"version_id":"12","aggregation":"AGGREGATION_METHOD_MAX","threshold":0.4}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	// version_id arrives as a STRING (protojson encodes int64 that way) and has to
	// come back out as the number 12.
	if got := tier.querySeen.GetVersionId(); got != 12 {
		t.Errorf("version_id = %d, want 12", got)
	}
	if got := tier.querySeen.GetAggregation(); got != pb.AggregationMethod_AGGREGATION_METHOD_MAX {
		t.Errorf("aggregation = %v, want MAX", got)
	}
	if got := tier.querySeen.GetThreshold(); got != 0.4 {
		t.Errorf("threshold = %v, want 0.4", got)
	}
}

func TestQueryText_RejectsBadRequests(t *testing.T) {
	embed := &fakeEmbedService{}
	srv := httptest.NewServer(embed.handler())
	defer srv.Close()

	cases := []struct {
		name string
		body string
		want int
	}{
		{"no text", `{"knowledge_base_id":"kb-1"}`, http.StatusBadRequest},
		{"blank text", `{"knowledge_base_id":"kb-1","text":"   "}`, http.StatusBadRequest},
		{"no knowledge base", `{"text":"x"}`, http.StatusBadRequest},
		{"broken json", `{`, http.StatusBadRequest},
		{"version_id is not a number", `{"knowledge_base_id":"kb-1","text":"x","version_id":"v12"}`, http.StatusBadRequest},
		{"unknown aggregation", `{"knowledge_base_id":"kb-1","text":"x","aggregation":"MAX"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tier := &fakeControlTier{embedAddr: srv.URL, modelID: "m"}
			g := newTestGateway(t, tier)
			rec := postQueryText(t, g, tc.body)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// An embedder the gateway cannot reach must be an error, not a quiet fallback: a
// vector the index never saw still returns results, and those are meaningless
// rather than empty — the worst possible answer.
func TestQueryText_UnreachableEmbedderIsAnErrorWithTheAddress(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	addr := dead.URL
	dead.Close() // nothing is listening now

	tier := &fakeControlTier{embedAddr: addr, modelID: "m"}
	g := newTestGateway(t, tier)

	rec := postQueryText(t, g, `{"knowledge_base_id":"kb-1","text":"x","top_k":1}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %s)", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, addr) {
		t.Errorf("error %s does not name the address it could not reach (%s)", body, addr)
	}
	if tier.querySeen != nil {
		t.Error("QueryService was called with a vector that was never embedded")
	}
}

// A knowledge base without an embedder cannot be text-searched here, and saying
// so beats pretending the query was answered.
func TestQueryText_KnowledgeBaseWithoutEmbedderIsRefused(t *testing.T) {
	tier := &fakeControlTier{embedAddr: "", modelID: ""}
	g := newTestGateway(t, tier)

	rec := postQueryText(t, g, `{"knowledge_base_id":"kb-1","text":"x","top_k":1}`)
	if rec.Code != http.StatusPreconditionFailed {
		t.Errorf("status = %d, want 412 (body %s)", rec.Code, rec.Body.String())
	}
}
