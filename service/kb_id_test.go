package service

import (
	"context"
	"regexp"
	"strings"
	"testing"

	pb "stratum/api/proto/stratum"
)

// H1 of docs/code-review-2026-09-24.md: kbID names the knowledge base's on-disk
// directory, so nothing the caller sent may appear in it. The name is folded in
// by nothing here on purpose — it is a label, and it is stored as one.
func TestCreateKnowledgeBase_IDCarriesNothingTheCallerSent(t *testing.T) {
	h := newKBSvcTestHarness()

	const name = "../../../../tmp/evil"
	resp, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: name})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase failed: %v", err)
	}

	if strings.ContainsAny(resp.KnowledgeBaseId, "/\\") || strings.Contains(resp.KnowledgeBaseId, "..") {
		t.Fatalf("knowledge_base_id %q carries path syntax", resp.KnowledgeBaseId)
	}
	if !regexp.MustCompile(`^kb-[0-9a-f]{32}$`).MatchString(resp.KnowledgeBaseId) {
		t.Errorf("knowledge_base_id = %q, want an opaque kb-<32 hex> handle", resp.KnowledgeBaseId)
	}

	// The name survives as the display field it always was.
	kb, err := h.raftNode.GetKB(context.Background(), resp.KnowledgeBaseId)
	if err != nil {
		t.Fatalf("GetKB after create: %v", err)
	}
	if kb.Name != name {
		t.Errorf("name = %q, want it preserved verbatim as a label: %q", kb.Name, name)
	}
}

// Two knowledge bases created from the SAME name must not end up with related
// ids: the old scheme appended a counter to the name, which made an id guessable
// from the name a tenant already knows.
func TestCreateKnowledgeBase_IdsAreIndependentOfEachOther(t *testing.T) {
	h := newKBSvcTestHarness()

	first, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: "same-name"})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase #1 failed: %v", err)
	}
	second, err := h.svc.CreateKnowledgeBase(context.Background(), &pb.CreateKnowledgeBaseRequest{Name: "same-name"})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase #2 failed: %v", err)
	}
	if first.KnowledgeBaseId == second.KnowledgeBaseId {
		t.Fatalf("two knowledge bases share the id %q", first.KnowledgeBaseId)
	}
}
