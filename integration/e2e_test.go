// Package integration_test — real-stack end-to-end tests.
//
// Unlike integration_test.go (which wires every dependency to a mock),
// these tests run the *complete real* Stratum stack in-process: real
// PebbleDB-backed DocStore/ChunkDocMapper/VersionDocList, real FileWAL,
// real RaftNodeImpl (single- and multi-node), real vecstore gRPC client
// against a real vecstore_server subprocess, real IndexManagerImpl,
// real coordinators, real services over real gRPC, and a real
// HTTPEmbedClient against an in-process mock embed server. This is the
// 全模块联合存取测试 per Stratum_测试顺序.md 第三批 (T3) with zero mocks in
// the data path.
//
// The vecstore_server binary must be built first (see
// vecstore/CMakeLists.txt); these tests skip (not fail) if it is missing.
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	vecstorepb "stratum/api/proto/vecstore"
	"stratum/internal/bloom"
	"stratum/internal/chunkdoc"
	"stratum/internal/chunkstore"
	"stratum/internal/coordinator"
	"stratum/internal/docstore"
	"stratum/internal/embed"
	"stratum/internal/index"
	"stratum/internal/raft"
	"stratum/internal/splitter"
	"stratum/internal/sync"
	"stratum/internal/types"
	"stratum/internal/versiondoc"
	"stratum/internal/wal"
	"stratum/service"
)

// --- helpers: addresses, vecstore subprocess, mock embed server ---

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// startVecstoreServerForTest starts a real vecstore_server subprocess on a
// fresh temp-dir RocksDB. Skips the test if the binary is unavailable.
func startVecstoreServerForTest(t *testing.T) (addr string) {
	t.Helper()
	binPath := vecstoreServerBin(t)

	addr = freeLoopbackAddr(t)
	dbDir := t.TempDir()

	cmd := exec.Command(binPath,
		"--rocksdb_path="+filepath.Join(dbDir, "db"),
		"--grpc_addr="+addr,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start vecstore_server: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("vecstore_server at %s did not become ready", addr)
	return ""
}

func vecstoreServerBin(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("STRATUM_VECSTORE_SERVER_BIN"); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("STRATUM_VECSTORE_SERVER_BIN=%s does not exist", p)
		}
		return p
	}
	candidate := filepath.Join("..", "build", "vecstore", "vecstore_server")
	if _, err := os.Stat(candidate); err == nil {
		abs, err := filepath.Abs(candidate)
		if err == nil {
			return abs
		}
		return candidate
	}
	t.Skip("vecstore_server binary not found; build it first (cmake -B build vecstore && cmake --build build --target vecstore_server) " +
		"or set STRATUM_VECSTORE_SERVER_BIN to its path")
	return ""
}

// contentVector derives a deterministic, L2-normalized vector from a
// chunk's text. The test queries with the same function, so a query whose
// text matches a stored doc's chunk content scores ~1.0 (cosine) and
// different texts score lower — enabling meaningful recall assertions.
func contentVector(content string, dim int) []float32 {
	v := make([]float32, dim)
	for i, c := range content {
		v[i%dim] += float32(c) * float32(i+1)
	}
	var norm float64
	for _, x := range v {
		norm += float64(x * x)
	}
	if norm > 0 {
		s := float32(math.Sqrt(norm))
		for i := range v {
			v[i] /= s
		}
	}
	return v
}

// mockEmbedHandler answers embed requests with deterministic
// content-derived vectors, mirroring the real embed service's protocol.
type mockEmbedHandler struct {
	dim int
}

func (h *mockEmbedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Chunks []struct {
			ChunkID string `json:"chunk_id"`
			Content string `json:"content"`
		} `json:"chunks"`
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := struct {
		Vectors map[string][]float32 `json:"vectors"`
	}{Vectors: make(map[string][]float32, len(req.Chunks))}
	for _, ch := range req.Chunks {
		resp.Vectors[ch.ChunkID] = contentVector(ch.Content, h.dim)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func startMockEmbedForTest(t *testing.T, dim int) string {
	t.Helper()
	srv := httptest.NewServer(&mockEmbedHandler{dim: dim})
	t.Cleanup(srv.Close)
	return srv.URL
}

// --- realNode: a fully-wired real Stratum node ---

type realNode struct {
	t          *testing.T
	nodeID     int64
	raftAddr   string
	grpcAddr   string
	raftNode   *raft.RaftNodeImpl
	fileWAL    *wal.FileWAL
	docStore   *docstore.PebbleDocStore
	chunkDoc   *chunkdoc.PebbleChunkDocMapper
	versionDoc *versiondoc.PebbleVersionDocList
	chunkStore *chunkstore.VecstoreChunkStore
	indexMgr   *index.IndexManagerImpl

	srv  *grpc.Server
	conn *grpc.ClientConn

	KB    pb.KnowledgeBaseServiceClient
	Query pb.QueryServiceClient
	Admin pb.AdminServiceClient
}

// newRealNode wires the complete real stack for one node using
// auto-assigned loopback addresses. It does not wait for leader election —
// call waitForLeader before issuing proposes.
func newRealNode(t *testing.T, nodeID int64, peers []raft.PeerConfig, vecstoreAddr, embedAddr string) *realNode {
	t.Helper()
	return newRealNodeWithAddrs(t, nodeID, peers, vecstoreAddr, embedAddr,
		freeLoopbackAddr(t), freeLoopbackAddr(t))
}

// newRealNodeWithAddrs is newRealNode with explicit raft/service
// addresses, so multi-node tests can agree on peer addresses up front.
func newRealNodeWithAddrs(t *testing.T, nodeID int64, peers []raft.PeerConfig, vecstoreAddr, embedAddr, raftAddr, grpcAddr string) *realNode {
	t.Helper()

	w, err := wal.NewFileWAL(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatalf("node %d: NewFileWAL: %v", nodeID, err)
	}
	ds, err := docstore.NewPebbleDocStore(filepath.Join(t.TempDir(), "docstore"))
	if err != nil {
		t.Fatalf("node %d: docstore: %v", nodeID, err)
	}
	cdm, err := chunkdoc.NewPebbleChunkDocMapper(filepath.Join(t.TempDir(), "chunkdoc"))
	if err != nil {
		t.Fatalf("node %d: chunkdoc: %v", nodeID, err)
	}
	vd, err := versiondoc.NewPebbleVersionDocList(filepath.Join(t.TempDir(), "versiondoc"))
	if err != nil {
		t.Fatalf("node %d: versiondoc: %v", nodeID, err)
	}
	cs, err := chunkstore.NewVecstoreChunkStore(vecstoreAddr)
	if err != nil {
		t.Fatalf("node %d: vecstore client: %v", nodeID, err)
	}

	rn, err := raft.NewRaftNodeImpl(raft.Config{
		NodeID:             nodeID,
		DataDir:            t.TempDir(),
		RaftAddr:           raftAddr,
		Peers:              peers,
		WAL:                w,
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("node %d: NewRaftNodeImpl: %v", nodeID, err)
	}

	ec := embed.NewHTTPEmbedClient(embedAddr, 30*time.Second)
	chunkBF := bloom.NewBitsAndBloomsFilter(4096, 0.01)
	versionBF := bloom.NewBitsAndBloomsFilter(4096, 0.01)
	sp := splitter.NewSlidingWindowSplitter()

	im := index.NewIndexManager(index.IndexManagerConfig{
		LRUCapacity:         8,
		LoadWaitTimeout:     5 * time.Second,
		CallbackMaxRetries:  3,
		CallbackRetryBaseMS: 10,
		VecstoreAddr:        vecstoreAddr,
	})
	im.SetBuildDataSources(
		vd.ListDocIDs,
		cdm.ListChunkIDsByDocs,
		func(ctx context.Context, kbID, chunkID string) ([]float32, error) {
			resp, err := cs.VecstoreClient().Read(ctx, &vecstorepb.ReadChunkRequest{Key: chunkstore.EncodeKey(kbID, chunkID)})
			if err != nil {
				return nil, err
			}
			return resp.GetVector(), nil
		},
	)
	im.RegisterBuildCallback(func(kbID string, versionID int64, status types.IndexStatus) error {
		return rn.ProposeUpdateVersionStatus(context.Background(), versionID, status)
	})

	wc := coordinator.NewWriteCoordinatorImpl(coordinator.WriteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		Splitter:            sp,
		EmbedClient:         ec,
		ChunkBloom:          chunkBF,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		DocStore:            ds,
		VersionDocList:      vd,
		IndexManager:        im,
	})
	dc := coordinator.NewDeleteCoordinatorImpl(coordinator.DeleteCoordinatorConfig{
		MaxRetries:          2,
		RetryBaseIntervalMS: 10,
		WAL:                 w,
		RaftNode:            rn,
		IndexManager:        im,
		DocStore:            ds,
		ChunkStore:          cs,
		ChunkDocMapper:      cdm,
		VersionDocList:      vd,
	})

	kbSvc := service.NewKnowledgeBaseService(rn, wc, dc)
	querySvc := service.NewQueryService(rn, im, cdm, vd, ds, versionBF)
	adminSvc := service.NewAdminService(rn, im, ds, cs, w)

	srv := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(srv, kbSvc)
	pb.RegisterQueryServiceServer(srv, querySvc)
	pb.RegisterAdminServiceServer(srv, adminSvc)
	pb.RegisterDataSyncServiceServer(srv, sync.NewLeaderHandler(ds.DB(), cdm.DB(), vd.DB(), cs.VecstoreClient()))

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("node %d: listen %s: %v", nodeID, grpcAddr, err)
	}
	go srv.Serve(lis)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("node %d: dial grpc: %v", nodeID, err)
	}

	n := &realNode{
		t:          t,
		nodeID:     nodeID,
		raftAddr:   raftAddr,
		grpcAddr:   grpcAddr,
		raftNode:   rn,
		fileWAL:    w,
		docStore:   ds,
		chunkDoc:   cdm,
		versionDoc: vd,
		chunkStore: cs,
		indexMgr:   im,
		srv:        srv,
		conn:       conn,
		KB:         pb.NewKnowledgeBaseServiceClient(conn),
		Query:      pb.NewQueryServiceClient(conn),
		Admin:      pb.NewAdminServiceClient(conn),
	}
	t.Cleanup(func() {
		conn.Close()
		srv.GracefulStop()
		rn.Stop()
		im.Close()
		cs.Close()
		vd.Close()
		cdm.Close()
		ds.Close()
		w.Close()
	})
	return n
}

// waitForLeader polls GetClusterStatus on all nodes and returns the node
// the cluster believes is leader.
func waitForLeader(t *testing.T, nodes ...*realNode) *realNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			st, err := n.raftNode.GetClusterStatus(context.Background())
			if err == nil && st.HasLeader && st.LeaderID == n.nodeID {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within timeout among %d nodes", len(nodes))
	return nil
}

// versionHash returns the committed document-ID set digest for the
// version, or "" if absent.
func versionHash(t *testing.T, n *realNode, kbID string, versionID int64) string {
	t.Helper()
	versions, err := n.raftNode.ListVersions(context.Background(), kbID)
	if err != nil {
		t.Fatalf("node %d: ListVersions: %v", n.nodeID, err)
	}
	for _, v := range versions {
		if v.VersionID == versionID {
			return v.DocIDSetHash
		}
	}
	t.Fatalf("node %d: version %d not found", n.nodeID, versionID)
	return ""
}

// waitVersionReady polls ListVersions until versionID reports READY.
func (n *realNode) waitVersionReady(ctx context.Context, kbID string, versionID int64) {
	n.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := n.KB.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
		if err == nil {
			for _, v := range resp.Versions {
				if v.VersionId == versionID && v.IndexStatus == pb.IndexStatus_INDEX_STATUS_READY {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	n.t.Fatalf("node %d: version %d did not become READY within timeout", n.nodeID, versionID)
}

// query runs a Query RPC with the given explicit version, failing the
// test on error.
func (n *realNode) query(ctx context.Context, kbID string, versionID int64, vec []float32, topK int32) []*pb.QueryResult {
	n.t.Helper()
	resp, err := n.Query.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &versionID,
		Vector:          vec,
		TopK:            topK,
	})
	if err != nil {
		n.t.Fatalf("node %d: Query(kb=%s, v=%d): %v", n.nodeID, kbID, versionID, err)
	}
	return resp.Results
}

func findResult(results []*pb.QueryResult, docID string) *pb.QueryResult {
	for _, r := range results {
		if r.DocId == docID {
			return r
		}
	}
	return nil
}

// createTestKB creates a knowledge base with short-window defaults and
// returns its ID plus the initial (READY) version ID.
func (n *realNode) createTestKB(ctx context.Context, name string) (string, int64) {
	n.t.Helper()
	resp, err := n.KB.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             name,
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "mock-embed",
			ModelId:     "m1",
		},
	})
	if err != nil {
		n.t.Fatalf("node %d: CreateKnowledgeBase: %v", n.nodeID, err)
	}
	return resp.KnowledgeBaseId, resp.InitialVersionId
}

// chunkIDFor computes the chunk ID the splitter would produce for a
// single-chunk document, so the test can predict vecstore keys.
func chunkIDFor(t *testing.T, content string) string {
	t.Helper()
	sp := splitter.NewSlidingWindowSplitter()
	chunks := sp.Split(content, 512, 64, "m1")
	if len(chunks) == 0 {
		t.Fatalf("no chunks for content %q", content)
	}
	return chunks[0].ChunkID
}

// --- Test 1: full store/retrieve lifecycle on a single real node ---
//
// 全模块联合存取主链路: CreateKB → CreateVersion(ADD) → Query → CreateVersion
// (UPDATE+DELETE) → 版本隔离 → Rollback → DeleteKB → 存储层清空校验。

func TestRealStack_FullLifecycle(t *testing.T) {
	vecAddr := startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, 4)

	node := newRealNode(t, 1, nil, vecAddr, embedURL)
	leader := waitForLeader(t, node)
	ctx := context.Background()

	kbID, v1 := leader.createTestKB(ctx, "e2e-lifecycle")

	// --- Write: version 2 adds two short documents (single chunk each) ---
	v2Resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "beta"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion(ADD): %v", err)
	}
	v2 := v2Resp.VersionId
	leader.waitVersionReady(ctx, kbID, v2)

	// --- Metadata: the version must carry a document-ID set digest that
	// matches the local versiondoc store (leader-side propose) ---
	metaHash := versionHash(t, leader, kbID, v2)
	if metaHash == "" {
		t.Error("version metadata has no DocIDSetHash after CreateVersion")
	}
	docs2, err := leader.versionDoc.ListDocIDs(ctx, kbID, v2)
	if err != nil {
		t.Fatalf("ListDocIDs(v%d): %v", v2, err)
	}
	if want := sync.ComputeDocIDSetHash(docs2); metaHash != want {
		t.Errorf("metadata digest = %q, local recompute = %q", metaHash, want)
	}

	// --- Read: nearest-doc retrieval works through the real stack ---
	res := leader.query(ctx, kbID, v2, contentVector("alpha", 4), 5)
	if r := findResult(res, "doc-1"); r == nil {
		t.Fatalf("query(alpha) did not return doc-1; got %+v", res)
	} else if r.Content != "alpha" {
		t.Errorf("doc-1 content = %q, want %q", r.Content, "alpha")
	} else if r.Score < 0.9 {
		t.Errorf("doc-1 score = %f, want >= 0.9 for an exact content match", r.Score)
	}

	res = leader.query(ctx, kbID, v2, contentVector("beta", 4), 5)
	if r := findResult(res, "doc-2"); r == nil || r.Content != "beta" {
		t.Fatalf("query(beta) did not return doc-2 with content 'beta'; got %+v", res)
	}

	// --- Write: version 3 updates doc-1 and deletes doc-2 ---
	v3Resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v2,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_UPDATE, DocId: "doc-1", Content: "alpha-2"},
			{Op: pb.ChangeOp_CHANGE_OP_DELETE, DocId: "doc-2"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion(UPDATE+DELETE): %v", err)
	}
	v3 := v3Resp.VersionId
	leader.waitVersionReady(ctx, kbID, v3)

	// --- Read: v3 reflects the update; the deleted doc is gone ---
	res = leader.query(ctx, kbID, v3, contentVector("alpha-2", 4), 5)
	if r := findResult(res, "doc-1"); r == nil {
		t.Fatalf("query(alpha-2) on v%d did not return doc-1; got %+v", v3, res)
	} else if r.Content != "alpha-2" {
		t.Errorf("v%d doc-1 content = %q, want %q", v3, r.Content, "alpha-2")
	}
	res = leader.query(ctx, kbID, v3, contentVector("beta", 4), 5)
	if r := findResult(res, "doc-2"); r != nil {
		t.Errorf("deleted doc-2 still returned from v%d: %+v", v3, r)
	}

	// --- Read: version isolation — v2 still has the deleted doc ---
	res = leader.query(ctx, kbID, v2, contentVector("beta", 4), 5)
	if r := findResult(res, "doc-2"); r == nil || r.Content != "beta" {
		t.Errorf("v2 lost doc-2 after v3 deletion (isolation broken): %+v", res)
	}

	// --- Rollback: active version switches to v1 ---
	if _, err := leader.KB.RollbackVersion(ctx, &pb.RollbackVersionRequest{
		KnowledgeBaseId: kbID,
		TargetVersionId: v1,
	}); err != nil {
		t.Fatalf("RollbackVersion: %v", err)
	}
	kb, err := leader.raftNode.GetKB(ctx, kbID)
	if err != nil {
		t.Fatalf("GetKB after rollback: %v", err)
	}
	if kb.ActiveVersionID != v1 {
		t.Errorf("active version = %d, want %d", kb.ActiveVersionID, v1)
	}

	// --- Delete: async cleanup empties every store ---
	if _, err := leader.KB.DeleteKnowledgeBase(ctx, &pb.DeleteKnowledgeBaseRequest{
		KnowledgeBaseId: kbID,
	}); err != nil {
		t.Fatalf("DeleteKnowledgeBase: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := leader.raftNode.GetKB(ctx, kbID)
		docsV3, _ := leader.versionDoc.ListDocIDs(ctx, kbID, v3)
		chunkOK, _ := leader.chunkStore.Exists(ctx, kbID, chunkIDFor(t, "alpha"))
		if err != nil && len(docsV3) == 0 && !chunkOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete did not converge: GetKB err=%v docsV3=%v chunkOK=%v", err, docsV3, chunkOK)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Definitive per-store emptiness checks.
	if _, err := leader.docStore.ReadAt(ctx, kbID, "doc-1", 1<<30); err == nil {
		t.Error("docstore still holds doc-1 after DeleteKnowledgeBase")
	}
	docs, err := leader.versionDoc.ListDocIDs(ctx, kbID, v2)
	if err != nil || len(docs) != 0 {
		t.Errorf("versiondoc not empty after delete: %v, %v", docs, err)
	}
	chunks, err := leader.chunkDoc.ListChunkIDsByDocs(ctx, kbID, []string{"doc-1"})
	if err != nil || len(chunks) != 0 {
		t.Errorf("chunkdoc reverse not empty after delete: %v, %v", chunks, err)
	}
	for _, content := range []string{"alpha", "beta", "alpha-2"} {
		if ok, _ := leader.chunkStore.Exists(ctx, kbID, chunkIDFor(t, content)); ok {
			t.Errorf("vecstore still holds chunk for %q after delete", content)
		}
	}

	// WAL must show no pending delete-mark after a completed deletion.
	records, err := leader.fileWAL.Recover(ctx)
	if err != nil {
		t.Fatalf("WAL.Recover: %v", err)
	}
	for _, r := range records {
		if r.Type == types.PendingRecordTypeDeleteMark && r.KBID == kbID {
			t.Errorf("WAL still has pending delete mark for %s after completed delete", kbID)
		}
	}
}

// --- Test 2: two real nodes — raft replication + leader→follower data
// sync + independent index build on the follower ---
//
// 联动测试核心: leader 写入 → raft 复制 → follower 通过 DataSyncService 拉取
// 存储层数据 → follower 独立建索引 → 从 follower 查询命中。

func TestRealStack_TwoNodeReplication(t *testing.T) {
	vecLeader := startVecstoreServerForTest(t)
	vecFollower := startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, 4)

	// Deterministic addresses so each node knows the other's raft and
	// service addresses up front.
	raft1, raft2 := freeLoopbackAddr(t), freeLoopbackAddr(t)
	grpc1, grpc2 := freeLoopbackAddr(t), freeLoopbackAddr(t)

	peersFor := func(self int64) []raft.PeerConfig {
		return []raft.PeerConfig{
			{ID: 1, RaftAddr: raft1, ServiceAddr: grpc1},
			{ID: 2, RaftAddr: raft2, ServiceAddr: grpc2},
		}
	}

	n1 := newRealNodeWithAddrs(t, 1, peersFor(1), vecLeader, embedURL, raft1, grpc1)
	n2 := newRealNodeWithAddrs(t, 2, peersFor(2), vecFollower, embedURL, raft2, grpc2)

	leader := waitForLeader(t, n1, n2)
	var follower *realNode
	if leader == n1 {
		follower = n2
	} else {
		follower = n1
	}
	t.Logf("leader = node %d, follower = node %d", leader.nodeID, follower.nodeID)

	// Wire the follower's data-sync: when it applies a CreateVersion
	// written by the leader, pull storage-layer data from the leader and
	// build its own index. The pull may race ahead of the leader's
	// post-propose storage writes, so retry until the pulled data is
	// VERIFIED complete: the digest recomputed from the local
	// VersionDocList must match the digest the leader committed into the
	// version metadata (see sync.VerifyDocIDSet). The initial version
	// (v1, created by CreateKnowledgeBase) carries no digest and no data —
	// pull it once and move on.
	leaderGRPC := leader.grpcAddr
	syncFollower := sync.NewFollower(
		follower.docStore, follower.chunkDoc, follower.versionDoc,
		follower.chunkStore, follower.indexMgr,
	)
	follower.raftNode.SetOnVersionCreated(func(kbID string, versionID int64) {
		ctx := context.Background()
		if versionID <= 1 {
			_ = syncFollower.PullVersion(ctx, leaderGRPC, kbID, versionID)
			return
		}
		deadline := time.Now().Add(10 * time.Second)
		backoff := 50 * time.Millisecond
		for {
			_ = syncFollower.PullVersion(ctx, leaderGRPC, kbID, versionID)

			// Expected digest from this node's replicated metadata.
			var metaHash string
			if versions, err := follower.raftNode.ListVersions(ctx, kbID); err == nil {
				for _, v := range versions {
					if v.VersionID == versionID {
						metaHash = v.DocIDSetHash
						break
					}
				}
			}
			ok, _, err := sync.VerifyDocIDSet(ctx, follower.versionDoc, kbID, versionID, metaHash)
			if err == nil && ok {
				return
			}
			if metaHash == "" {
				// Digest not committed yet (leader's propose races us, or a
				// missed propose): accept a pull that produced data.
				docs, derr := follower.versionDoc.ListDocIDs(ctx, kbID, versionID)
				if derr == nil && len(docs) > 0 {
					return
				}
			}
			if time.Now().After(deadline) {
				follower.t.Logf("follower pull for %s v%d did not converge (metaHash=%s)", kbID, versionID, metaHash)
				return
			}
			time.Sleep(backoff)
			if backoff < time.Second {
				backoff *= 2
			}
		}
	})

	ctx := context.Background()
	kbID, v1 := leader.createTestKB(ctx, "e2e-two-node")

	// Write a version with two documents on the leader.
	v2Resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "alpha"},
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-2", Content: "beta"},
		},
	})
	if err != nil {
		t.Fatalf("leader CreateVersion: %v", err)
	}
	v2 := v2Resp.VersionId

	// Both nodes must converge to READY (leader via its own build; the
	// follower's status arrives through raft replication of the leader's
	// build-callback propose).
	leader.waitVersionReady(ctx, kbID, v2)
	follower.waitVersionReady(ctx, kbID, v2)

	// --- Metadata: the digest committed by the leader must have
	// replicated to the follower, and both must match each node's local
	// recompute of the version's document set ---
	leaderHash := versionHash(t, leader, kbID, v2)
	followerHash := versionHash(t, follower, kbID, v2)
	if leaderHash == "" || followerHash == "" {
		t.Errorf("missing digest: leader=%q follower=%q", leaderHash, followerHash)
	}
	if leaderHash != followerHash {
		t.Errorf("digest mismatch across replicas: leader=%q follower=%q", leaderHash, followerHash)
	}
	followerDocs, err := follower.versionDoc.ListDocIDs(ctx, kbID, v2)
	if err != nil {
		t.Fatalf("follower ListDocIDs(v%d): %v", v2, err)
	}
	if want := sync.ComputeDocIDSetHash(followerDocs); want != followerHash {
		t.Errorf("follower local recompute = %q, metadata = %q", want, followerHash)
	}

	// The follower's local stores must hold the replicated data.
	docs, err := follower.versionDoc.ListDocIDs(ctx, kbID, v2)
	if err != nil || len(docs) != 2 {
		t.Fatalf("follower versiondoc for v%d = %v, %v; want 2 docs", v2, docs, err)
	}
	if got, err := follower.docStore.ReadAt(ctx, kbID, "doc-1", v2); err != nil || string(got) != "alpha" {
		t.Errorf("follower docstore doc-1 = (%q, %v), want (alpha, nil)", got, err)
	}
	chunks, err := follower.chunkDoc.ListChunkIDsByDocs(ctx, kbID, []string{"doc-1"})
	if err != nil || len(chunks) != 1 {
		t.Errorf("follower chunkdoc doc-1 chunks = %v, %v; want 1", chunks, err)
	}

	// Query the FOLLOWER: its independently built index must answer. The
	// follower's build is asynchronous and an early (pre-data) pull may
	// have transiently built an empty index, so poll until doc-1 is
	// actually retrievable — an eventually-consistent read.
	queryDeadline := time.Now().Add(15 * time.Second)
	var res []*pb.QueryResult
	for {
		res = follower.query(ctx, kbID, v2, contentVector("alpha", 4), 5)
		if findResult(res, "doc-1") != nil {
			break
		}
		if time.Now().After(queryDeadline) {
			t.Fatalf("follower query(alpha) never returned doc-1; last results: %+v, follower index loaded=%v",
				res, follower.indexMgr.IsLoaded(kbID, v2))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if r := findResult(res, "doc-1"); r == nil || r.Content != "alpha" {
		t.Fatalf("follower query(alpha) = %+v; want doc-1/alpha", res)
	}
	res = follower.query(ctx, kbID, v2, contentVector("beta", 4), 5)
	if r := findResult(res, "doc-2"); r == nil || r.Content != "beta" {
		t.Errorf("follower query(beta) = %+v; want doc-2/beta", res)
	}

	// Deleting on the leader must replicate: the follower's raft state
	// machine removes the KB metadata too.
	if _, err := leader.KB.DeleteKnowledgeBase(ctx, &pb.DeleteKnowledgeBaseRequest{KnowledgeBaseId: kbID}); err != nil {
		t.Fatalf("leader DeleteKnowledgeBase: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := follower.raftNode.GetKB(ctx, kbID); err != nil {
			return // follower converged
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("follower still has KB %s after leader-side deletion", kbID)
}
