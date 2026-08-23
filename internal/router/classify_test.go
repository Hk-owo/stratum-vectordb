package router

import (
	"testing"

	pb "stratum/api/proto/stratum"
)

func TestIsWriteMethod(t *testing.T) {
	writes := []string{
		pb.KnowledgeBaseService_CreateKnowledgeBase_FullMethodName,
		pb.KnowledgeBaseService_DeleteKnowledgeBase_FullMethodName,
		pb.KnowledgeBaseService_CreateVersion_FullMethodName,
		pb.KnowledgeBaseService_RollbackVersion_FullMethodName,
		pb.KnowledgeBaseService_DeleteVersion_FullMethodName,
		pb.AdminService_RebuildIndex_FullMethodName,
		pb.AdminService_WarmupVersion_FullMethodName,
	}
	reads := []string{
		pb.KnowledgeBaseService_ListVersions_FullMethodName,
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
