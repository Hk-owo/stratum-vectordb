package router

import (
	"testing"

	pb "stratum/api/proto/stratum"
)

func TestIsWriteMethod(t *testing.T) {
	writes := []string{
		pb.KnowledgeBaseService_CreateKnowledgeBase_FullMethodName,
		pb.KnowledgeBaseService_DeleteKnowledgeBase_FullMethodName,
		// CreateVersion is leader-bound again (§7.13.2): the control layer
		// chooses the coordinator, so the entry must reach the leader rather
		// than run on whichever node accepted it.
		pb.KnowledgeBaseService_CreateVersion_FullMethodName,
		pb.KnowledgeBaseService_RollbackVersion_FullMethodName,
		pb.KnowledgeBaseService_DeleteVersion_FullMethodName,
		// DiscardVersion removes replicated metadata, so it is leader-bound like
		// every other proposal (docs/await-version-plan.md §7 Step 6).
		pb.KnowledgeBaseService_DiscardVersion_FullMethodName,
		pb.AdminService_RebuildIndex_FullMethodName,
		pb.AdminService_WarmupVersion_FullMethodName,
	}
	reads := []string{
		pb.KnowledgeBaseService_ListVersions_FullMethodName,
		// AwaitVersion holds no state: it may be repeated and sent to any node,
		// which is what makes reconnecting to a different node cost nothing.
		pb.KnowledgeBaseService_AwaitVersion_FullMethodName,
		pb.KnowledgeBaseService_ListKnowledgeBases_FullMethodName,
		pb.KnowledgeBaseService_GetKnowledgeBase_FullMethodName,
		pb.QueryService_Query_FullMethodName,
		pb.AdminService_HealthCheck_FullMethodName,
		pb.AdminService_GetSystemStatus_FullMethodName,
		pb.AdminService_GetClusterStatus_FullMethodName,
	}
	for _, m := range writes {
		if !isWriteMethod(m) {
			t.Errorf("isWriteMethod(%q) = false, want true", m)
		}
	}
	for _, m := range reads {
		if isWriteMethod(m) {
			t.Errorf("isWriteMethod(%q) = true, want false", m)
		}
	}
}
