package service

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// top_k is an int32 off the wire, and the result slice is sized from it. The
// refusal has to happen at the entry, before any allocation, or a single request
// takes the node down with it — so this test asserts on a service with no
// knowledge base, no version and no index: the check must not depend on any of
// them.
func TestQueryService_RejectsOutOfRangeTopK(t *testing.T) {
	h := newQuerySvcHarness(t)

	for _, topK := range []int32{0, -1, MaxQueryTopK + 1, 2147483647} {
		resp, err := h.svc.Query(context.Background(), &pb.QueryRequest{
			KnowledgeBaseId: "kb-anything",
			TopK:            topK,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("top_k=%d: expected InvalidArgument, got %v (resp=%v)", topK, err, resp)
		}
	}
}

// The bound is a ceiling, not a default: a value inside it must reach the rest
// of the query path (here: fail on the unknown knowledge base, which is a
// different error entirely).
func TestQueryService_AcceptsTopKInsideTheBound(t *testing.T) {
	h := newQuerySvcHarness(t)

	for _, topK := range []int32{1, MaxQueryTopK} {
		_, err := h.svc.Query(context.Background(), &pb.QueryRequest{
			KnowledgeBaseId: "kb-anything",
			TopK:            topK,
		})
		if status.Code(err) == codes.InvalidArgument {
			t.Errorf("top_k=%d must be accepted, got %v", topK, err)
		}
	}
}
