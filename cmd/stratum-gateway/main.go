// Command stratum-gateway is the HTTP/JSON → gRPC gateway for the Stratum
// web console. It exposes the three external gRPC services
// (KnowledgeBaseService / QueryService / AdminService) over a small REST
// API and serves the frontend static assets from the same origin (so no
// CORS is required).
//
// Design notes:
//   - This is a separate process from cmd/stratum. The core Stratum server
//     is untouched.
//   - The gateway dials the **routing layer** (cmd/stratum-router): leader
//     discovery, write-to-leader forwarding, and read load-balancing are
//     handled by the router, so the gateway keeps a single gRPC connection
//     and no cluster awareness of its own.
//   - It uses the already-generated gRPC client stubs plus protojson
//     (google.golang.org/protobuf, an existing dependency), so it adds no
//     new Go module dependency and requires no proto regeneration.
//   - DataSyncService (internal) is intentionally not exposed here.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "stratum/api/proto/stratum"
	"stratum/internal/embed"
	"stratum/internal/types"
)

var (
	// EmitUnpopulated keeps zero-valued enums (e.g. HEALTH_STATUS_HEALTHY=0,
	// INDEX_TYPE_HNSW=0, KB_STATUS_ACTIVE=0) present in the JSON, so the
	// frontend always receives an explicit enum string instead of having to
	// infer a missing field as the zero value.
	marshalOpts   = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	unmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// gateway holds the three external service clients, all dialing the
// routing layer (stratum-router). The router handles leader discovery,
// write forwarding and read load-balancing across the cluster.
type gateway struct {
	kb    pb.KnowledgeBaseServiceClient
	query pb.QueryServiceClient
	admin pb.AdminServiceClient

	// embedClients keeps one HTTP embed client per service address, for
	// /api/query-text. A search is a short-lived request, so re-dialing per call
	// would add a round trip to every one of them. Guarded: the gateway serves
	// concurrently.
	embedMu      sync.Mutex
	embedClients map[string]embed.EmbedClient
}

// embedClientFor returns the embed client for an address, dialing once per address.
func (g *gateway) embedClientFor(addr string) embed.EmbedClient {
	g.embedMu.Lock()
	defer g.embedMu.Unlock()
	if g.embedClients == nil {
		g.embedClients = map[string]embed.EmbedClient{}
	}
	if c, ok := g.embedClients[addr]; ok {
		return c
	}
	c := embed.NewHTTPEmbedClient(addr, 10*time.Second)
	g.embedClients[addr] = c
	return c
}

func main() {
	// -grpc-addr 指向路由层（stratum-router，默认 0.0.0.0:7009），由 router
	// 负责 leader 发现、写转发与读负载均衡；gateway 只维护一条 gRPC 连接。
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:7009", "routing layer (stratum-router) gRPC address")
	httpAddr := flag.String("http-addr", "0.0.0.0:8081", "gateway HTTP listen address")
	// web/dist is the Vite build output: web/ is now a Vite project whose
	// sources live in web/src/ (`npm --prefix web run build` produces dist/).
	// Pointing at web/ itself would serve the un-transpiled entry index.html.
	staticDir := flag.String("static", "./web/dist", "frontend static asset directory")
	opsConfigPath := flag.String("ops-config", "", "console ops config YAML (default ./run/console.yaml)")
	nodeID := flag.Int("node-id", 1, "this node's ID for the ops console")
	flag.Parse()

	// Non-blocking dial: the gateway starts even if the backend is down,
	// and /api calls return UNAVAILABLE until the connection is established
	// (so the frontend health badge can report the outage instead of the
	// gateway itself hanging on startup).
	conn, err := grpc.Dial(*grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to dial %s: %v", *grpcAddr, err)
	}
	defer conn.Close()

	g := &gateway{
		kb:    pb.NewKnowledgeBaseServiceClient(conn),
		query: pb.NewQueryServiceClient(conn),
		admin: pb.NewAdminServiceClient(conn),
	}

	// --- Ops console (control plane) ---
	// Serves /ops/* independently of the database stack: cluster node
	// list, service start/stop/restart, startup parameter edit, and log
	// tailing. Default config path follows the start.sh run/ layout.
	opsPath := *opsConfigPath
	if opsPath == "" {
		opsPath = filepath.Join("run", "console.yaml")
	}
	if _, err := os.Stat(opsPath); os.IsNotExist(err) {
		// Materialize a default console config so it is inspectable and
		// editable from the very first run.
		def := defaultOpsConfig(*nodeID)
		if err := saveOpsConfig(opsPath, &def); err != nil {
			log.Printf("ops: cannot write default config %s: %v", opsPath, err)
		}
	}
	opsMgr, err := newOpsManager(opsPath, *nodeID)
	if err != nil {
		log.Fatalf("failed to init ops console: %v", err)
	}

	mux := http.NewServeMux()
	g.registerRoutes(mux)
	mux.Handle("/ops/", opsMgr.opsMux)

	// Same-origin static assets; /api/ routes are matched above.
	// noCacheStatic forces revalidation on every load so a rebuilt frontend
	// is always picked up (the default FileServer only sends Last-Modified,
	// which lets browsers serve stale cached copies after the files change).
	mux.Handle("/", noCacheStatic(http.FileServer(http.Dir(*staticDir))))

	log.Printf("stratum-gateway listening on %s (grpc: %s, static: %s, ops: %s)",
		*httpAddr, *grpcAddr, *staticDir, opsPath)

	// Graceful shutdown: on SIGINT/SIGTERM stop accepting requests, then
	// stop the managed local services so no orphan processes survive the
	// gateway (start.sh's Ctrl+C path relies on this). main() waits for
	// the shutdown goroutine to finish before exiting, otherwise the
	// managed child processes would be orphaned.
	srv := &http.Server{Addr: *httpAddr, Handler: logRequests(mux)}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	shutdownDone := make(chan struct{})
	go func() {
		<-sigCh
		log.Printf("received signal, shutting down console and stopping managed services")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		opsMgr.sup.StopAll(5 * time.Second)
		close(shutdownDone)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway server failed: %v", err)
	}
	<-shutdownDone
	log.Printf("stratum-gateway stopped")
}

func (g *gateway) registerRoutes(mux *http.ServeMux) {
	// --- AdminService ---
	mux.HandleFunc("GET /api/health", handle(
		func() *pb.HealthCheckRequest { return &pb.HealthCheckRequest{} },
		func(ctx context.Context, r *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
			return g.admin.HealthCheck(ctx, r)
		},
	))
	mux.HandleFunc("GET /api/system-status", handle(
		func() *pb.GetSystemStatusRequest { return &pb.GetSystemStatusRequest{} },
		func(ctx context.Context, r *pb.GetSystemStatusRequest) (*pb.GetSystemStatusResponse, error) {
			return g.admin.GetSystemStatus(ctx, r)
		},
	))

	// --- KnowledgeBaseService ---
	mux.HandleFunc("POST /api/knowledge-bases", handle(
		func() *pb.CreateKnowledgeBaseRequest { return &pb.CreateKnowledgeBaseRequest{} },
		func(ctx context.Context, r *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
			return g.kb.CreateKnowledgeBase(ctx, r)
		},
	))
	mux.HandleFunc("GET /api/knowledge-bases", handle(
		func() *pb.ListKnowledgeBasesRequest { return &pb.ListKnowledgeBasesRequest{} },
		func(ctx context.Context, r *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
			return g.kb.ListKnowledgeBases(ctx, r)
		},
	))
	mux.HandleFunc("GET /api/knowledge-bases/{id}", func(w http.ResponseWriter, r *http.Request) {
		resp, err := g.kb.GetKnowledgeBase(r.Context(), &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: r.PathValue("id")})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /api/knowledge-bases/delete", handle(
		func() *pb.DeleteKnowledgeBaseRequest { return &pb.DeleteKnowledgeBaseRequest{} },
		func(ctx context.Context, r *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
			return g.kb.DeleteKnowledgeBase(ctx, r)
		},
	))
	mux.HandleFunc("GET /api/knowledge-bases/{id}/versions", func(w http.ResponseWriter, r *http.Request) {
		resp, err := g.kb.ListVersions(r.Context(), &pb.ListVersionsRequest{KnowledgeBaseId: r.PathValue("id")})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /api/knowledge-bases/{id}/versions", handleWithID(
		func() *pb.CreateVersionRequest { return &pb.CreateVersionRequest{} },
		func(r *pb.CreateVersionRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
			return g.kb.CreateVersion(ctx, r)
		},
	))

	// AwaitVersion is a read exposed over POST because it carries a body
	// (version_id, target, wait_timeout_ms). The HTTP request is held until the
	// target is reached or the server's wait cap expires, and "not reached yet"
	// comes back as a stage rather than as an HTTP error.
	mux.HandleFunc("POST /api/knowledge-bases/{id}/await", handleWithID(
		func() *pb.AwaitVersionRequest { return &pb.AwaitVersionRequest{} },
		func(r *pb.AwaitVersionRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.AwaitVersionRequest) (*pb.AwaitVersionResponse, error) {
			return g.kb.AwaitVersion(ctx, r)
		},
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/rollback", handleWithID(
		func() *pb.RollbackVersionRequest { return &pb.RollbackVersionRequest{} },
		func(r *pb.RollbackVersionRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
			return g.kb.RollbackVersion(ctx, r)
		},
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/rebuild", handleWithID(
		func() *pb.RebuildIndexRequest { return &pb.RebuildIndexRequest{} },
		func(r *pb.RebuildIndexRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.RebuildIndexRequest) (*pb.RebuildIndexResponse, error) {
			return g.admin.RebuildIndex(ctx, r)
		},
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/warmup", handleWithID(
		func() *pb.WarmupVersionRequest { return &pb.WarmupVersionRequest{} },
		func(r *pb.WarmupVersionRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.WarmupVersionRequest) (*pb.WarmupVersionResponse, error) {
			return g.admin.WarmupVersion(ctx, r)
		},
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/delete-version", handleWithID(
		func() *pb.DeleteVersionRequest { return &pb.DeleteVersionRequest{} },
		func(r *pb.DeleteVersionRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
			return g.kb.DeleteVersion(ctx, r)
		},
	))

	// DiscardVersion abandons a version whose write never landed: a write to the
	// leader, admissible only while the version is still PENDING
	// (docs/await-version-plan.md §7 Step 6). A version that already has data is
	// delete-version's business, and is refused with version_not_pending.
	mux.HandleFunc("POST /api/knowledge-bases/{id}/discard-version", handleWithID(
		func() *pb.DiscardVersionRequest { return &pb.DiscardVersionRequest{} },
		func(r *pb.DiscardVersionRequest, id string) { r.KnowledgeBaseId = id },
		func(ctx context.Context, r *pb.DiscardVersionRequest) (*pb.DiscardVersionResponse, error) {
			return g.kb.DiscardVersion(ctx, r)
		},
	))

	// --- QueryService ---
	mux.HandleFunc("POST /api/query", handle(
		func() *pb.QueryRequest { return &pb.QueryRequest{} },
		func(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
			return g.query.Query(ctx, r)
		},
	))

	// Text search: the console asks in words, the gateway embeds with the
	// knowledge base's OWN embed config and forwards the vector to /api/query's
	// service. It is not a proto message on purpose — the wire contract stays
	// "a vector", and this is a convenience for callers that have no embedder of
	// their own (web/src/api/queries.ts describes the same split from the UI side).
	mux.HandleFunc("POST /api/query-text", g.handleQueryText)
}

// handle adapts a no-path-parameter gRPC method into an http.HandlerFunc.
func handle[Req, Resp proto.Message](
	newReq func() Req,
	call func(context.Context, Req) (Resp, error),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := newReq()
		if err := readBody(r, req); err != nil {
			writeError(w, err)
			return
		}
		resp, err := call(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, resp)
	}
}

// handleWithID adapts a gRPC method whose request is built from the JSON
// body plus a knowledge_base_id path parameter.
func handleWithID[Req, Resp proto.Message](
	newReq func() Req,
	setID func(Req, string),
	call func(context.Context, Req) (Resp, error),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := newReq()
		if err := readBody(r, req); err != nil {
			writeError(w, err)
			return
		}
		setID(req, r.PathValue("id"))
		resp, err := call(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, resp)
	}
}

func readBody(r *http.Request, m proto.Message) error {
	if r.Body == nil {
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return status.Error(codes.InvalidArgument, "failed to read body: "+err.Error())
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	if err := unmarshalOpts.Unmarshal(body, m); err != nil {
		return status.Error(codes.InvalidArgument, "invalid JSON body: "+err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, m proto.Message) {
	b, err := marshalOpts.Marshal(m)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	httpCode := http.StatusInternalServerError
	switch st.Code() {
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.FailedPrecondition:
		httpCode = http.StatusPreconditionFailed
	case codes.Unavailable:
		httpCode = http.StatusServiceUnavailable
	case codes.Unimplemented:
		httpCode = http.StatusNotImplemented
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":     st.Message(),
		"grpc_code": st.Code().String(),
	})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if r.URL.Path != "/favicon.ico" {
			log.Printf("%s %s", r.Method, r.URL.Path)
		}
	})
}

// noCacheStatic disables browser caching for the frontend assets, so that
// after a rebuild (or a gateway restart) the browser always fetches the
// current files instead of serving a stale cached copy.
func noCacheStatic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		next.ServeHTTP(w, r)
	})
}

// queryTextRequest is the body of POST /api/query-text.
type queryTextRequest struct {
	KnowledgeBaseID string   `json:"knowledge_base_id"`
	Text            string   `json:"text"`
	TopK            int32    `json:"top_k"`
	Threshold       *float32 `json:"threshold,omitempty"`
	// VersionID is a string because protojson encodes int64 as a JSON string, and
	// the console passes back the value it read from a response verbatim.
	VersionID   *string `json:"version_id,omitempty"`
	Aggregation string  `json:"aggregation,omitempty"`
}

// handleQueryText embeds the text with the knowledge base's own embed config and
// forwards the resulting vector to QueryService.
//
// The embed config comes from the knowledge base, never from the request: a
// caller may choose WHAT to ask, but only the knowledge base decides HOW its text
// is turned into vectors — a model chosen per request could silently answer from
// a different vector space, and similarity across two models means nothing.
func (g *gateway) handleQueryText(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var body queryTextRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, status.Error(codes.InvalidArgument, "invalid JSON body: "+err.Error()))
		return
	}
	if body.KnowledgeBaseID == "" {
		writeError(w, status.Error(codes.InvalidArgument, "knowledge_base_id is required"))
		return
	}
	if strings.TrimSpace(body.Text) == "" {
		writeError(w, status.Error(codes.InvalidArgument, "text is required"))
		return
	}

	kbResp, err := g.kb.GetKnowledgeBase(ctx, &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: body.KnowledgeBaseID})
	if err != nil {
		writeError(w, err)
		return
	}
	cfg := kbResp.GetKnowledgeBase().GetEmbedConfig()
	if cfg.GetServiceAddr() == "" {
		writeError(w, status.Error(codes.FailedPrecondition,
			"this knowledge base has no embed service configured, so its text cannot be embedded here"))
		return
	}

	chunkID := queryChunkID(body.Text, cfg.GetModelId())
	vectors, err := g.embedClientFor(cfg.GetServiceAddr()).Embed(ctx,
		[]types.Chunk{{ChunkID: chunkID, Content: body.Text}})
	if err != nil {
		// No silent fallback. A vector the index never saw would still return
		// results, and those results would be meaningless rather than empty —
		// worse than an error. Say what could not be reached and what to do.
		writeError(w, status.Errorf(codes.Unavailable,
			"cannot embed the query text: %v (the gateway has to be able to reach the knowledge base's embed service at %s; otherwise embed the text yourself and call POST /api/query)",
			err, cfg.GetServiceAddr()))
		return
	}
	vector, ok := vectors[chunkID]
	if !ok {
		writeError(w, status.Error(codes.Internal, "the embed service returned no vector for the query text"))
		return
	}

	q := &pb.QueryRequest{
		KnowledgeBaseId: body.KnowledgeBaseID,
		Vector:          vector,
		TopK:            body.TopK,
		Threshold:       body.Threshold,
	}
	if body.VersionID != nil {
		v, err := strconv.ParseInt(*body.VersionID, 10, 64)
		if err != nil {
			writeError(w, status.Errorf(codes.InvalidArgument, "version_id %q is not a number", *body.VersionID))
			return
		}
		q.VersionId = &v
	}
	if body.Aggregation != "" {
		v, ok := pb.AggregationMethod_value[body.Aggregation]
		if !ok {
			writeError(w, status.Errorf(codes.InvalidArgument, "unknown aggregation %q", body.Aggregation))
			return
		}
		q.Aggregation = pb.AggregationMethod(v)
	}

	resp, err := g.query.Query(ctx, q)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, resp)
}

// queryChunkID is the id the embed layer keys a chunk by: SHA-256 of the text plus
// the knowledge base's model id — the same rule internal/splitter applies to
// document chunks. It has to match, or the embed service would compute a vector
// under one key while we look it up under another.
func queryChunkID(text, modelID string) string {
	sum := sha256.Sum256([]byte(text + modelID))
	return hex.EncodeToString(sum[:])
}
