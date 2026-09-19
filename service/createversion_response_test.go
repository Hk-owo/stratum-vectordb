package service

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	pb "stratum/api/proto/stratum"
)

// Step 4 of docs/await-version-plan.md: the idempotency key travels back to the
// caller.
//
// What these tests pin is the key's JOURNEY, not just its presence. A caller told
// one key while the write was committed under another cannot recover it (§7.12):
// the re-send would allocate a second version, and the first one would stay
// PENDING forever with nobody able to name it.

func TestCreateVersion_EchoesTheCallersKey(t *testing.T) {
	h := newKBSvcTestHarness()
	ctx := context.Background()
	kbID := createKBForResponseTest(t, h)

	resp, err := h.svc.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: "caller-owned-key",
		Changes:         []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "d1", Content: "x"}},
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if got := resp.GetClientRequestId(); got != "caller-owned-key" {
		t.Errorf("response key = %q, want the caller's own", got)
	}

	calls := h.writeC.Calls()
	if len(calls) != 1 {
		t.Fatalf("coordinator calls = %d, want 1", len(calls))
	}
	if calls[0].ClientRequestID != "caller-owned-key" {
		t.Errorf("coordinator committed under %q, but the caller was told %q",
			calls[0].ClientRequestID, resp.GetClientRequestId())
	}
}

func TestCreateVersion_GeneratesAKeyAndReturnsIt(t *testing.T) {
	h := newKBSvcTestHarness()
	ctx := context.Background()
	kbID := createKBForResponseTest(t, h)

	first, err := h.svc.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		Changes:         []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "d1", Content: "x"}},
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if first.GetClientRequestId() == "" {
		t.Fatal("no key was returned for a request that sent none: the caller has nothing to re-send under")
	}
	calls := h.writeC.Calls()
	if len(calls) != 1 {
		t.Fatalf("coordinator calls = %d, want 1", len(calls))
	}
	if calls[0].ClientRequestID != first.GetClientRequestId() {
		t.Errorf("coordinator committed under %q, but the caller was told %q",
			calls[0].ClientRequestID, first.GetClientRequestId())
	}

	// A second key-less write is a DIFFERENT write: reusing the generated key
	// would silently collapse the two into one version, which is the opposite of
	// what "no key" has always meant (every call allocates a new version).
	second, err := h.svc.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		Changes:         []*pb.DocChange{{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "d2", Content: "y"}},
	})
	if err != nil {
		t.Fatalf("second CreateVersion: %v", err)
	}
	if second.GetClientRequestId() == first.GetClientRequestId() {
		t.Errorf("two key-less writes were handed the same key %q; they would collapse into one version",
			first.GetClientRequestId())
	}
}

func createKBForResponseTest(t *testing.T, h *kbSvcTestHarness) string {
	t.Helper()
	resp, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{
		Name:             "key-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig:      &pb.EmbedConfig{ServiceAddr: "localhost:8080", ModelId: "test-model"},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}
	return resp.GetKnowledgeBaseId()
}

// TestDiscardVersion_RecordsTheAbandonmentInTheLog is §12 item 6: an abandoned
// version must be reconstructable afterwards. Raft's log holds the command; this
// line is what a human reading the service log can see, and without it the
// version merely stops existing.
func TestDiscardVersion_RecordsTheAbandonmentInTheLog(t *testing.T) {
	svc, _, kbID, v := newAwaitHarness(t)
	core, logs := observer.New(zap.InfoLevel)
	svc.SetLogger(zap.New(core))

	if _, err := svc.DiscardVersion(context.Background(), &pb.DiscardVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
	}); err != nil {
		t.Fatalf("DiscardVersion: %v", err)
	}

	entries := logs.FilterMessage("version discarded by the caller").All()
	if len(entries) != 1 {
		t.Fatalf("discard log entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["kb_id"] != kbID {
		t.Errorf("kb_id = %v, want %q", fields["kb_id"], kbID)
	}
	if fields["version_id"] != v {
		t.Errorf("version_id = %v, want %d", fields["version_id"], v)
	}
	if fields["metadata_removed"] != true {
		t.Errorf("metadata_removed = %v, want true", fields["metadata_removed"])
	}

	// A repeat is recorded too, and says the metadata is already gone: an
	// operator asking "was this version abandoned, or never valid?" needs the
	// difference.
	if _, err := svc.DiscardVersion(context.Background(), &pb.DiscardVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v,
	}); err != nil {
		t.Fatalf("repeated DiscardVersion: %v", err)
	}
	entries = logs.FilterMessage("version discarded by the caller").All()
	if len(entries) != 2 {
		t.Fatalf("discard log entries after the repeat = %d, want 2", len(entries))
	}
	if entries[1].ContextMap()["metadata_removed"] != false {
		t.Errorf("metadata_removed = %v on the repeat, want false", entries[1].ContextMap()["metadata_removed"])
	}
}
