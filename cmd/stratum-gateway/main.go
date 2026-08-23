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
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "stratum/api/proto/stratum"
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
}

func main() {
	// -grpc-addr 指向路由层（stratum-router，默认 0.0.0.0:7009），由 router
	// 负责 leader 发现、写转发与读负载均衡；gateway 只维护一条 gRPC 连接。
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:7009", "routing layer (stratum-router) gRPC address")
	httpAddr := flag.String("http-addr", "0.0.0.0:8081", "gateway HTTP listen address")
	staticDir := flag.String("static", "./web", "frontend static asset directory")
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

	// --- QueryService ---
	mux.HandleFunc("POST /api/query", handle(
		func() *pb.QueryRequest { return &pb.QueryRequest{} },
		func(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
			return g.query.Query(ctx, r)
		},
	))
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
