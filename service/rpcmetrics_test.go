package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "stratum/api/proto/stratum"
)

// testMessage is a real generated protobuf, not a stand-in with a Size() method.
//
// That distinction is the whole regression: this test file used to define its own
// type with Size() int, which the interceptor matched happily — while every REAL
// message failed the same assertion, because protobuf-go ≥ 1.27 generates only
// Reset/String/ProtoReflect. The counters were silently never incremented. A
// fixture that does not share the production type cannot catch that.
func testMessage(kbID, docID string) *pb.CreateVersionRequest {
	return &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: "req-1",
		Changes:         []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: docID, Content: "hello"}},
	}
}

// scrape serves the collectors over HTTP the way the node does and returns the
// text a Prometheus scrape would read.
func scrape(t *testing.T, m *RPCMetrics) string {
	t.Helper()
	reg := prometheus.NewRegistry()
	for _, c := range m.Collectors() {
		reg.MustRegister(c)
	}
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	return string(body)
}

// TestRPCMetrics_CountsCallsBytesAndCodes is the contract the performance
// measurement depends on: a scraped series must say how many calls of a given
// method the control side handled, how big they were, and whether they failed.
func TestRPCMetrics_CountsCallsBytesAndCodes(t *testing.T) {
	m := NewRPCMetrics()
	info := &grpc.UnaryServerInfo{FullMethod: "/stratum.KnowledgeBaseService/CreateVersion"}

	req := testMessage("kb-1", "doc-1")
	resp := testMessage("kb-1", "doc-2")
	wantReq := proto.Size(req)
	wantResp := proto.Size(resp)
	ok := func(ctx context.Context, r any) (any, error) { return resp, nil }
	if _, err := m.UnaryInterceptor()(context.Background(), req, info, ok); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	denied := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}
	if _, err := m.UnaryInterceptor()(context.Background(), req, info, denied); err == nil {
		t.Fatal("interceptor swallowed the handler error")
	}

	text := scrape(t, m)
	for _, want := range []string{
		`stratum_grpc_requests_total{code="OK",method="CreateVersion",service="KnowledgeBaseService"} 1`,
		`stratum_grpc_requests_total{code="PermissionDenied",method="CreateVersion",service="KnowledgeBaseService"} 1`,
		fmt.Sprintf(`stratum_grpc_request_bytes_total{method="CreateVersion",service="KnowledgeBaseService"} %d`, 2*wantReq),
		fmt.Sprintf(`stratum_grpc_response_bytes_total{method="CreateVersion",service="KnowledgeBaseService"} %d`, wantResp),
		`stratum_grpc_duration_seconds_count{method="CreateVersion",service="KnowledgeBaseService"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics does not contain %q\n%s", want, text)
		}
	}
}

// TestRPCMetrics_FailedCallCountsNoResponseBytes: a refused call returned no
// message, so its response byte counter must stay put rather than gain a zero.
// A zero-valued series and an absent one read the same to a rate() query, but
// only the absent one is honest about what happened.
func TestRPCMetrics_FailedCallCountsNoResponseBytes(t *testing.T) {
	m := NewRPCMetrics()
	info := &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}
	fail := func(ctx context.Context, req any) (any, error) { return nil, errors.New("boom") }
	if _, err := m.UnaryInterceptor()(context.Background(), testMessage("kb-1", "doc-1"), info, fail); err == nil {
		t.Fatal("interceptor swallowed the handler error")
	}

	text := scrape(t, m)
	if strings.Contains(text, "stratum_grpc_response_bytes_total{") {
		t.Errorf("a failed call gained response bytes\n%s", text)
	}
	// An unclassified error must still be labelled with its real gRPC code.
	if !strings.Contains(text, `stratum_grpc_requests_total{code="Unknown",method="Query",service="QueryService"} 1`) {
		t.Errorf("failed call not counted with its code\n%s", text)
	}
}

// TestRPCMetrics_TypedNilResponseIsSafe: a handler returning a typed-nil
// pointer satisfies the sizer interface, and calling Size() on it dereferences
// nil inside the generated code. An observation path must never be able to take
// the node down.
func TestRPCMetrics_TypedNilResponseIsSafe(t *testing.T) {
	m := NewRPCMetrics()
	info := &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}
	var typedNil *pb.QueryResponse
	handler := func(ctx context.Context, req any) (any, error) { return typedNil, nil }
	if _, err := m.UnaryInterceptor()(context.Background(), testMessage("kb-1", "doc-1"), info, handler); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	text := scrape(t, m)
	if strings.Contains(text, "stratum_grpc_response_bytes_total{") {
		t.Errorf("typed-nil response contributed bytes\n%s", text)
	}
}

// TestRPCMetrics_NilReceiverIsInert pins the "metrics off" deployment: the node
// always installs the interceptor, and a nil counter set must neither panic nor
// change the call.
func TestRPCMetrics_NilReceiverIsInert(t *testing.T) {
	var m *RPCMetrics
	info := &grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"}
	handler := func(ctx context.Context, req any) (any, error) { return testMessage("kb-1", "doc-2"), nil }
	if _, err := m.UnaryInterceptor()(context.Background(), testMessage("kb-1", "doc-1"), info, handler); err != nil {
		t.Fatalf("nil interceptor: %v", err)
	}
	if len(m.Collectors()) != 0 {
		t.Errorf("nil metrics registered %d collectors", len(m.Collectors()))
	}
}

// TestRPCMetrics_StreamCallsAreCounted: PullVersionChanges is the one streaming
// RPC, and a per-method counter that silently ignores it would understate the
// replication traffic the measurement is about.
func TestRPCMetrics_StreamCallsAreCounted(t *testing.T) {
	m := NewRPCMetrics()
	info := &grpc.StreamServerInfo{FullMethod: "/stratum.DataSyncService/PullVersionChanges"}
	handler := func(srv any, ss grpc.ServerStream) error { return nil }
	if err := m.StreamInterceptor()(nil, nil, info, handler); err != nil {
		t.Fatalf("stream interceptor: %v", err)
	}
	text := scrape(t, m)
	want := `stratum_grpc_requests_total{code="OK",method="PullVersionChanges",service="DataSyncService"} 1`
	if !strings.Contains(text, want) {
		t.Errorf("/metrics does not contain %q\n%s", want, text)
	}
}

// TestSplitFullMethod names the service without the package prefix and survives
// a name it cannot parse: a label built from a malformed method must still be
// usable in a query.
func TestSplitFullMethod(t *testing.T) {
	cases := []struct{ in, svc, method string }{
		{"/stratum.QueryService/Query", "QueryService", "Query"},
		{"/stratum.AdminService/GetClusterStatus", "AdminService", "GetClusterStatus"},
		{"/stratum.StorageAdminService/DrainNode", "StorageAdminService", "DrainNode"},
		{"Query", "Query", "unknown"},
		{"", "unknown", "unknown"},
	}
	for _, c := range cases {
		svc, method := splitFullMethod(c.in)
		if svc != c.svc || method != c.method {
			t.Errorf("splitFullMethod(%q) = (%q, %q), want (%q, %q)", c.in, svc, method, c.svc, c.method)
		}
	}
}
