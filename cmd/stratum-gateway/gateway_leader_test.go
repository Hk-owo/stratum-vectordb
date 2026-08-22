package main

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
)

// fakeKBService 实现 KnowledgeBaseServiceServer，CreateKnowledgeBase 行为可注入。
type fakeKBService struct {
	pb.UnimplementedKnowledgeBaseServiceServer
	createFunc func(ctx context.Context, req *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error)
}

func (f *fakeKBService) CreateKnowledgeBase(ctx context.Context, req *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
	return f.createFunc(ctx, req)
}

// testGatewayWithNodes 起 nodes 个 bufconn gRPC server，构造 gateway 连接它们，
// 返回 gateway 与每个 server 的 CreateKnowledgeBase 行为设置器。
func testGatewayWithNodes(t *testing.T, nodes int) (*gateway, []*fakeKBService) {
	t.Helper()
	svcs := make([]*fakeKBService, nodes)
	listeners := make([]*bufconn.Listener, nodes)
	for i := 0; i < nodes; i++ {
		svcs[i] = &fakeKBService{}
		lis := bufconn.Listen(1 << 20)
		listeners[i] = lis
		srv := grpc.NewServer()
		pb.RegisterKnowledgeBaseServiceServer(srv, svcs[i])
		go func(s *grpc.Server) { _ = s.Serve(lis) }(srv)
		t.Cleanup(srv.Stop)
	}

	g := &gateway{addrs: make([]string, nodes)}
	for i := 0; i < nodes; i++ {
		addr := bufconnAddr(i)
		g.addrs[i] = addr
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(_ context.Context, a string) (net.Conn, error) {
				// a 是 passthrough target（不含 scheme 前缀），映射回对应 listener
				for j, la := range g.addrs {
					if strings.TrimPrefix(la, "passthrough:///") == a {
						return listeners[j].Dial()
					}
				}
				return nil, status.Errorf(codes.Internal, "unknown addr %s", a)
			}),
		)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		g.conns = append(g.conns, conn)
		g.kbs = append(g.kbs, pb.NewKnowledgeBaseServiceClient(conn))
		g.querys = append(g.querys, nil) // 本测试只用 kb
		g.admins = append(g.admins, nil)
	}
	return g, svcs
}

func bufconnAddr(i int) string {
	// 显式 passthrough scheme：裸地址会被 grpc.NewClient 当作 dns 解析失败。
	return "passthrough:///node" + string(rune('0'+i))
}

func TestIsNotLeaderErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"not leader internal", status.Error(codes.Internal, "kvraft: not leader"), true},
		{"wrapped message", status.Error(codes.Internal, "rpc error: kvraft: not leader"), true},
		{"other internal", status.Error(codes.Internal, "index build failed"), false},
		{"unavailable not leader", status.Error(codes.Unavailable, "not leader anyway"), true},
		{"unavailable connection refused", status.Error(codes.Unavailable, "connection refused"), true},
		{"plain error", context.DeadlineExceeded, false},
	}
	for _, c := range cases {
		if got := isRetryableErr(c.err); got != c.want {
			t.Errorf("%s: isRetryableErr(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}

// TestWithLeaderRetry_DeadNode 用「node0 连接被拒」模拟宕机：把 node0 的
// client 指向一个已关闭的 listener，node1 正常。
func TestWithLeaderRetry_DeadNode(t *testing.T) {
	g, svcs := testGatewayWithNodes(t, 2)
	// node0 真实宕机：关闭其 listener
	// （testGatewayWithNodes 的 listener 未暴露，这里通过替换 dial 目标模拟：
	// 将 g.addrs[0] 改为指向已关闭端口）
	svcs[1].createFunc = func(_ context.Context, _ *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return &pb.CreateKnowledgeBaseResponse{KnowledgeBaseId: "kb-on-node1"}, nil
	}
	_ = svcs[0]

	// 关闭 node0 的底层连接：直接替换其 client 无法模拟传输层失败，
	// 因此这里构造一个指向已关闭端口的连接作为 node0 的 client。
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	dead.Close() // 立即关闭 → 后续 dial 拒绝

	conn, err := grpc.NewClient("passthrough:///"+deadAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	g.kbs[0] = pb.NewKnowledgeBaseServiceClient(conn)
	g.querys[0] = nil
	g.admins[0] = nil

	call := withLeaderRetry(g, func(i int, ctx context.Context, r *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return g.kbs[i].CreateKnowledgeBase(ctx, r)
	})
	resp, err := call(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: "test"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.KnowledgeBaseId != "kb-on-node1" {
		t.Errorf("resp = %q, want kb-on-node1", resp.KnowledgeBaseId)
	}
	if got := g.leader.Load(); got != 1 {
		t.Errorf("leader pointer = %d, want 1", got)
	}
}

func TestWithLeaderRetry_FindsLeader(t *testing.T) {
	g, svcs := testGatewayWithNodes(t, 3)
	// node0/node1 拒绝（not leader），node2 接受
	for i := 0; i < 2; i++ {
		idx := i
		svcs[idx].createFunc = func(_ context.Context, _ *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
			return nil, status.Error(codes.Internal, "kvraft: not leader")
		}
	}
	svcs[2].createFunc = func(_ context.Context, _ *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return &pb.CreateKnowledgeBaseResponse{KnowledgeBaseId: "kb-on-node2"}, nil
	}

	call := withLeaderRetry(g, func(i int, ctx context.Context, r *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return g.kbs[i].CreateKnowledgeBase(ctx, r)
	})
	resp, err := call(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: "test"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.KnowledgeBaseId != "kb-on-node2" {
		t.Errorf("resp = %q, want kb-on-node2", resp.KnowledgeBaseId)
	}
	if got := g.leader.Load(); got != 2 {
		t.Errorf("leader pointer = %d, want 2（找到 leader 后应停留在该节点）", got)
	}
}

func TestWithLeaderRetry_NoLeader(t *testing.T) {
	g, svcs := testGatewayWithNodes(t, 2)
	for _, s := range svcs {
		s.createFunc = func(_ context.Context, _ *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
			return nil, status.Error(codes.Internal, "kvraft: not leader")
		}
	}
	call := withLeaderRetry(g, func(i int, ctx context.Context, r *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return g.kbs[i].CreateKnowledgeBase(ctx, r)
	})
	if _, err := call(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: "test"}); err == nil {
		t.Fatal("expected error when no node accepts the write")
	}
}

func TestWithLeaderRetry_NonLeaderErrorNotRetried(t *testing.T) {
	g, svcs := testGatewayWithNodes(t, 2)
	// node0 返回与 leader 无关的错误，不应轮换重试
	svcs[0].createFunc = func(_ context.Context, _ *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "invalid name")
	}
	svcs[1].createFunc = func(_ context.Context, _ *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		t.Error("node1 不应被调用（非 not leader 错误不重试）")
		return nil, nil
	}
	call := withLeaderRetry(g, func(i int, ctx context.Context, r *pb.CreateKnowledgeBaseRequest) (*pb.CreateKnowledgeBaseResponse, error) {
		return g.kbs[i].CreateKnowledgeBase(ctx, r)
	})
	if _, err := call(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: "test"}); err == nil {
		t.Fatal("expected the invalid-argument error to surface")
	}
	if got := g.leader.Load(); got != 0 {
		t.Errorf("leader pointer = %d, want 0（不应轮换）", got)
	}
}
