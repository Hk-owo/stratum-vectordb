package service

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	pb "stratum/api/proto/stratum"
)

// TestKnowledgeBaseService_CreateVersionReportsStageTimings pins stage 2's line:
// the entry the client's clock is measuring, split into the storage gate, the
// proto conversion, and the coordinator call it delegates to.
//
// The line is what joins the station's line to the storage node's: kb_id +
// version_id appear on all of them. Its execute_us is the coordinator's total, so
// the two lines agree by construction — a disagreement is a real finding rather
// than a rounding difference, which is only true because both are measured at the
// same boundary.
func TestKnowledgeBaseService_CreateVersionReportsStageTimings(t *testing.T) {
	h := newKBSvcTestHarness()
	core, logs := observer.New(zap.DebugLevel)
	h.svc.SetLogger(zap.New(core))

	createResp, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "localhost:8080",
			ModelId:     "test-model",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}
	h.writeC.SetExecuteResult(2, nil)

	resp, err := h.svc.CreateVersion(context.Background(), &pb.CreateVersionRequest{
		KnowledgeBaseId: createResp.KnowledgeBaseId,
		ParentVersionId: 1,
		ClientRequestId: "req-1",
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "hello world"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}

	entry, ok := createVersionTimingsEntry(logs, "service: create version timings")
	if !ok {
		t.Fatalf("no create-version timings line was emitted; got %v", createVersionLogMessages(logs))
	}
	fields := entry.ContextMap()
	if got := fields["kb_id"]; got != createResp.KnowledgeBaseId {
		t.Errorf("kb_id = %v, want %q", got, createResp.KnowledgeBaseId)
	}
	if got := fields["version_id"]; got != resp.VersionId {
		t.Errorf("version_id = %v, want the id that was returned (%d)", got, resp.VersionId)
	}
	if got := fields["client_request_id"]; got != "req-1" {
		t.Errorf("client_request_id = %v, want req-1 (the key an idempotent retry reuses)", got)
	}
	if got := fields["changes"]; got != int64(1) {
		t.Errorf("changes = %v, want 1", got)
	}
	for _, name := range []string{"gate_us", "convert_us", "execute_us", "total_us"} {
		if _, present := fields[name]; !present {
			t.Errorf("field %s is missing", name)
		}
	}
	// The gate and the conversion are local work on one request, so they cannot
	// exceed the whole call. This is the assertion that catches a stage being
	// measured around the wrong boundary.
	total, _ := fields["total_us"].(int64)
	if gate, ok := fields["gate_us"].(int64); ok && gate > total {
		t.Errorf("gate_us = %d exceeds total_us = %d", gate, total)
	}
	if execute, ok := fields["execute_us"].(int64); ok && execute > total {
		t.Errorf("execute_us = %d exceeds total_us = %d", execute, total)
	}
}

// A refused write still reports its line, with no version id: the storage gate's
// refusal is exactly the case an operator wants the cost of, and a
// success-only line would not show it.
func TestKnowledgeBaseService_CreateVersionReportsTimingsWhenRefused(t *testing.T) {
	h := newKBSvcTestHarness()
	core, logs := observer.New(zap.DebugLevel)
	h.svc.SetLogger(zap.New(core))

	// The harness wires no storage-degradation source, so the gate allows this
	// write. The failure comes from the coordinator instead — which is the case
	// where the gate's cost is still worth seeing, since the request paid it.
	h.writeC.SetExecuteResult(0, context.DeadlineExceeded)

	if _, err := h.svc.CreateVersion(context.Background(), &pb.CreateVersionRequest{
		KnowledgeBaseId: "kb-unknown",
		ParentVersionId: 0,
		Changes:         []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "x"}},
	}); err == nil {
		t.Fatal("a failing write must return an error")
	}

	entry, ok := createVersionTimingsEntry(logs, "service: create version timings")
	if !ok {
		t.Fatalf("no create-version timings line was emitted; got %v", createVersionLogMessages(logs))
	}
	if got := entry.ContextMap()["version_id"]; got != int64(0) {
		t.Errorf("version_id = %v, want 0 when no version was allocated", got)
	}
}

func createVersionTimingsEntry(logs *observer.ObservedLogs, message string) (observer.LoggedEntry, bool) {
	for _, entry := range logs.All() {
		if entry.Message == message {
			return entry, true
		}
	}
	return observer.LoggedEntry{}, false
}

func createVersionLogMessages(logs *observer.ObservedLogs) []string {
	var out []string
	for _, entry := range logs.All() {
		out = append(out, entry.Message)
	}
	return out
}
