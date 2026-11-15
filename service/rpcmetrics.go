package service

// rpcmetrics.go — per-method gRPC counters for the node's /metrics endpoint.
//
// Why this exists: the node already exported runtime and process collectors, and
// the per-stage timings answer "where did THIS request spend its time" — but
// nothing answered "how much traffic is the control side handling". The storage
// topology plan (§10, docs/实施中/storage-topology-plan.md) asks for control-side
// RPC/s, bytes/s and handling latency before anyone can claim the per-node
// direct reporting is or is not a bottleneck, and a measurement taken from client
// behaviour alone cannot separate "the control group is saturated" from "the
// client is slow".
//
// What it deliberately does NOT do: no scraping of payload contents, no labels
// that grow with traffic (service/method/code are a fixed, small set), no
// mutation of any request, and no effect on the call itself. A nil receiver is
// safe everywhere, so a deployment (or a unit test) that never constructs one
// still works unchanged.
//
// Cost when installed: one counter increment and one observe per call, plus size
// queries on messages the handler already serialized. Nothing is buffered and
// nothing is flushed: Prometheus scrapes pull from the same counters.

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RPCMetrics is the per-method traffic and latency counters for one gRPC server.
//
// One per process, shared by both interceptors and the metrics endpoint. It is
// safe for concurrent use because prometheus counters are.
type RPCMetrics struct {
	requests   *prometheus.CounterVec
	reqBytes   *prometheus.CounterVec
	respBytes  *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	collectors []prometheus.Collector
}

// NewRPCMetrics builds the collectors. The caller registers Collectors() in its
// registry; nothing here touches the global default registry (which would panic
// when a second node is started in the same process, as the tests do).
func NewRPCMetrics() *RPCMetrics {
	m := &RPCMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stratum_grpc_requests_total",
			Help: "gRPC calls handled by this node, by service, method and status code.",
		}, []string{"service", "method", "code"}),
		reqBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stratum_grpc_request_bytes_total",
			Help: "Encoded request bytes handled by this node, by service and method (unary calls only).",
		}, []string{"service", "method"}),
		respBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stratum_grpc_response_bytes_total",
			Help: "Encoded response bytes returned by this node, by service and method (unary calls only).",
		}, []string{"service", "method"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "stratum_grpc_duration_seconds",
			Help:    "Wall-clock time a gRPC handler took on this node, by service and method.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16), // 0.5 ms … ~16 s
		}, []string{"service", "method"}),
	}
	m.collectors = []prometheus.Collector{m.requests, m.reqBytes, m.respBytes, m.duration}
	return m
}

// Collectors is what the metrics endpoint registers. Nil-safe.
func (m *RPCMetrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}
	return m.collectors
}

// UnaryInterceptor counts a unary call and its wire sizes.
//
// It sits behind RecoveryUnary (so it never sees a panic) and in front of the
// authorization gates (so a refused call is still counted: "how much of the
// traffic is being rejected" is exactly the question an operator asks when a
// credential rolls over). The code label comes from the returned status, so a
// gated call reads as its real code rather than as OK.
func (m *RPCMetrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.observe(info.FullMethod, err, time.Since(start), sizeOf(req), sizeOf(resp))
		return resp, err
	}
}

// StreamInterceptor counts a streaming call. Message sizes are not attributed:
// a stream has no single request/response pair, and inventing one (per-message
// increments) would make the byte counters incomparable with the unary ones.
func (m *RPCMetrics) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		m.observe(info.FullMethod, err, time.Since(start), 0, 0)
		return err
	}
}

// observe records one completed call. A nil receiver does nothing, which is what
// keeps the interceptor installable in a node whose metrics endpoint is off.
func (m *RPCMetrics) observe(fullMethod string, err error, elapsed time.Duration, reqSize, respSize int) {
	if m == nil {
		return
	}
	svc, method := splitFullMethod(fullMethod)
	m.requests.WithLabelValues(svc, method, status.Code(err).String()).Inc()
	if reqSize > 0 {
		m.reqBytes.WithLabelValues(svc, method).Add(float64(reqSize))
	}
	if respSize > 0 {
		m.respBytes.WithLabelValues(svc, method).Add(float64(respSize))
	}
	m.duration.WithLabelValues(svc, method).Observe(elapsed.Seconds())
}

// splitFullMethod turns "/stratum.QueryService/Query" into
// ("QueryService", "Query").
//
// The package prefix is dropped on purpose: it is a constant ("stratum") and a
// label whose value never varies only makes every query carry it. A malformed
// name still produces a usable pair rather than an empty label value.
func splitFullMethod(full string) (svc, method string) {
	if i := strings.LastIndex(full, "/"); i >= 0 {
		method = full[i+1:]
		full = full[:i]
	}
	if i := strings.LastIndex(full, "/"); i >= 0 {
		full = full[i+1:]
	}
	svc = full
	if dot := strings.LastIndex(svc, "."); dot >= 0 {
		svc = svc[dot+1:]
	}
	if svc == "" {
		svc = "unknown"
	}
	if method == "" {
		method = "unknown"
	}
	return svc, method
}

// sizeOf is the encoded size of a message, or 0 when it is not a protobuf
// message at all (a nil response, a non-proto payload).
//
// It goes through proto.Size rather than asserting an interface with a Size()
// method, and that is not a style choice: the code generator (protobuf-go ≥ 1.27,
// this repo generates with 1.35) no longer emits Size() at all — only
// Reset/String/ProtoReflect. An interface assertion therefore matched NOTHING, the
// byte counters were never incremented, and the series were absent from /metrics
// while the call counters beside them looked healthy. Measured on the 3+5 cluster:
// `stratum_grpc_requests_total` reported 10 CreateVersion calls and
// `stratum_grpc_request_bytes_total` reported no series at all.
//
// proto.Size is also nil-safe, including a typed-nil pointer, which is the case an
// observation path must never panic on.
func sizeOf(msg any) int {
	if msg == nil {
		return 0
	}
	m, ok := msg.(proto.Message)
	if !ok {
		return 0
	}
	return proto.Size(m)
}
