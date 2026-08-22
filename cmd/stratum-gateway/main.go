// Command stratum-gateway is the HTTP/JSON → gRPC gateway for the Stratum
// web console. It exposes the three external gRPC services
// (KnowledgeBaseService / QueryService / AdminService) over a small REST
// API and serves the frontend static assets from the same origin (so no
// CORS is required).
//
// Design notes:
//   - This is a separate process from cmd/stratum. The core Stratum server
//     is untouched; the gateway dials its gRPC address (default :7000).
//   - It uses the already-generated gRPC client stubs plus protojson
//     (google.golang.org/protobuf, an existing dependency), so it adds no
//     new Go module dependency and requires no proto regeneration.
//   - DataSyncService (internal) is intentionally not exposed here.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
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

type gateway struct {
	addrs  []string // 全部节点地址（逗号分隔 -grpc-addr 传入）
	leader atomic.Int32
	conns  []*grpc.ClientConn
	kbs    []pb.KnowledgeBaseServiceClient
	querys []pb.QueryServiceClient
	admins []pb.AdminServiceClient
}

// cur 返回当前 leader 地址对应的客户端三元组。
func (g *gateway) cur() (pb.KnowledgeBaseServiceClient, pb.QueryServiceClient, pb.AdminServiceClient) {
	i := int(g.leader.Load())
	return g.kbs[i], g.querys[i], g.admins[i]
}

// rotate 把 leader 指针轮换到下一个节点。
func (g *gateway) rotate() {
	i := g.leader.Load()
	g.leader.Store((i + 1) % int32(len(g.addrs)))
}

// isRetryableErr 判断写操作是否应该轮换到下一个节点重试：
//   - kvraft.ErrNotLeader（codes.Internal + "not leader"）：本节点不是 leader
//   - codes.Unavailable：节点宕机/连接失败（leader 漂移或容器停止）
//
// 其余错误（参数错误、索引失败等）不重试。
func isRetryableErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() == codes.Unavailable {
		return true
	}
	return st.Code() == codes.Internal && strings.Contains(st.Message(), "not leader")
}

// withLeaderRetry 包装写方法：在 leader 上执行，遇 not leader 或节点不可用
// 自动轮换到下一个节点重试（最多 len(addrs) 次），成功后更新 leader 指针。
// 其余错误直接返回，不重试。
// （泛型函数；方法不支持类型参数，故为包级函数。）
func withLeaderRetry[Req, Resp proto.Message](
	g *gateway,
	fn func(int, context.Context, Req) (Resp, error),
) func(context.Context, Req) (Resp, error) {
	return func(ctx context.Context, req Req) (Resp, error) {
		var zero Resp
		for i := 0; i < len(g.addrs); i++ {
			idx := int(g.leader.Load())
			resp, err := fn(idx, ctx, req)
			if err == nil {
				return resp, nil
			}
			if !isRetryableErr(err) {
				return zero, err
			}
			g.rotate()
		}
		return zero, errors.New("no leader available")
	}
}

func main() {
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:7000", "Stratum gRPC address")
	httpAddr := flag.String("http-addr", "0.0.0.0:8081", "gateway HTTP listen address")
	staticDir := flag.String("static", "./web", "frontend static asset directory")
	opsConfigPath := flag.String("ops-config", "", "console ops config YAML (default ./run/console.yaml)")
	nodeID := flag.Int("node-id", 1, "this node's ID for the ops console")
	flag.Parse()

	// Non-blocking dial: the gateway starts even if the backend is down,
	// and /api calls return UNAVAILABLE until the connection is established
	// (so the frontend health badge can report the outage instead of the
	// gateway itself hanging on startup).
	//
	// -grpc-addr 支持逗号分隔的多个节点地址：写操作自动在 leader 上执行，
	// leader 漂移时自动轮换重试（见 withLeaderRetry）。
	addrs := []string{}
	for _, a := range strings.Split(*grpcAddr, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		log.Fatalf("invalid -grpc-addr %q", *grpcAddr)
	}
	g := &gateway{addrs: addrs}
	for _, a := range addrs {
		conn, err := grpc.Dial(a,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("failed to dial %s: %v", a, err)
		}
		defer conn.Close()
		g.conns = append(g.conns, conn)
		g.kbs = append(g.kbs, pb.NewKnowledgeBaseServiceClient(conn))
		g.querys = append(g.querys, pb.NewQueryServiceClient(conn))
		g.admins = append(g.admins, pb.NewAdminServiceClient(conn))
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
			_, _, admin := g.cur()
			return admin.HealthCheck(ctx, r)
		},
	))
	mux.HandleFunc("GET /api/system-status", handle(
		func() *pb.GetSystemStatusRequest { return &pb.GetSystemStatusRequest{} },
		func(ctx context.Context, r *pb.GetSystemStatusRequest) (*pb.GetSystemStatusResponse, error) {
			_, _, admin := g.cur()
			return admin.GetSystemStatus(ctx, r)
		},
	))

	// --- KnowledgeBaseService ---
	mux.HandleFunc("POST /api/knowledge-bases", handle(
		func() *pb.CreateKnowledgeBaseRequest { return &pb.CreateKnowledgeBaseRequest{} },
		withLeaderRetry(g, func(i int, ctx context.Context, r *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
			return g.kbs[i].CreateKnowledgeBase(ctx, r)
		}),
	))
	mux.HandleFunc("GET /api/knowledge-bases", handle(
		func() *pb.ListKnowledgeBasesRequest { return &pb.ListKnowledgeBasesRequest{} },
		func(ctx context.Context, r *pb.ListKnowledgeBasesRequest) (*pb.ListKnowledgeBasesResponse, error) {
			kb, _, _ := g.cur()
			return kb.ListKnowledgeBases(ctx, r)
		},
	))
	mux.HandleFunc("GET /api/knowledge-bases/{id}", func(w http.ResponseWriter, r *http.Request) {
		kb, _, _ := g.cur()
		resp, err := kb.GetKnowledgeBase(r.Context(), &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: r.PathValue("id")})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /api/knowledge-bases/delete", handle(
		func() *pb.DeleteKnowledgeBaseRequest { return &pb.DeleteKnowledgeBaseRequest{} },
		withLeaderRetry(g, func(i int, ctx context.Context, r *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
			return g.kbs[i].DeleteKnowledgeBase(ctx, r)
		}),
	))
	mux.HandleFunc("GET /api/knowledge-bases/{id}/versions", func(w http.ResponseWriter, r *http.Request) {
		kb, _, _ := g.cur()
		resp, err := kb.ListVersions(r.Context(), &pb.ListVersionsRequest{KnowledgeBaseId: r.PathValue("id")})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /api/knowledge-bases/{id}/versions", handleWithID(
		func() *pb.CreateVersionRequest { return &pb.CreateVersionRequest{} },
		func(r *pb.CreateVersionRequest, id string) { r.KnowledgeBaseId = id },
		withLeaderRetry(g, func(i int, ctx context.Context, r *pb.CreateVersionRequest) (*pb.CreateVersionResponse, error) {
			return g.kbs[i].CreateVersion(ctx, r)
		}),
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/rollback", handleWithID(
		func() *pb.RollbackVersionRequest { return &pb.RollbackVersionRequest{} },
		func(r *pb.RollbackVersionRequest, id string) { r.KnowledgeBaseId = id },
		withLeaderRetry(g, func(i int, ctx context.Context, r *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
			return g.kbs[i].RollbackVersion(ctx, r)
		}),
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/rebuild", handleWithID(
		func() *pb.RebuildIndexRequest { return &pb.RebuildIndexRequest{} },
		func(r *pb.RebuildIndexRequest, id string) { r.KnowledgeBaseId = id },
		withLeaderRetry(g, func(i int, ctx context.Context, r *pb.RebuildIndexRequest) (*pb.RebuildIndexResponse, error) {
			return g.admins[i].RebuildIndex(ctx, r)
		}),
	))
	mux.HandleFunc("POST /api/knowledge-bases/{id}/warmup", handleWithID(
		func() *pb.WarmupVersionRequest { return &pb.WarmupVersionRequest{} },
		func(r *pb.WarmupVersionRequest, id string) { r.KnowledgeBaseId = id },
		withLeaderRetry(g, func(i int, ctx context.Context, r *pb.WarmupVersionRequest) (*pb.WarmupVersionResponse, error) {
			return g.admins[i].WarmupVersion(ctx, r)
		}),
	))

	// --- QueryService ---
	mux.HandleFunc("POST /api/query", handle(
		func() *pb.QueryRequest { return &pb.QueryRequest{} },
		func(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
			_, query, _ := g.cur()
			return query.Query(ctx, r)
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
