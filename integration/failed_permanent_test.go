package integration_test

import (
	"context"
	"strings"
	"testing"

	pb "stratum/api/proto/stratum"
	"stratum/internal/plane"
	"stratum/internal/types"
)

// TestIntegration_FailureBudgetDeclaresVersionFailedPermanent is the end-to-end
// path of Stratum_设计文档v13.md §10.1: storage-layer failure reports → the
// control layer's own count → the terminal verdict in replicated state → the
// operator seeing it with its cause chain through GetSystemStatus.
//
// It deliberately drives the control layer directly (rather than a broken
// store) because that is the real seam: the storage layer reports, the control
// layer decides. The reporting side itself is covered by the plane package's
// tests.
func TestIntegration_FailureBudgetDeclaresVersionFailedPermanent(t *testing.T) {
	cluster := newTestCluster(t)
	defer cluster.Close()
	ctx := context.Background()

	createResp, err := cluster.KBClient.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             "test-kb",
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "mock:8080",
			ModelId:     "m1",
		},
	})
	if err != nil {
		t.Fatalf("CreateKnowledgeBase failed: %v", err)
	}
	kbID := createResp.KnowledgeBaseId

	verResp, err := cluster.KBClient.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: 1,
		ClientRequestId: "failed-permanent-1",
		Changes: []*pb.DocChange{
			{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: "doc-1", Content: "hello world"},
		},
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}

	// A bound on the retry budget so the test does not have to know the
	// production default. The same instance must be reused across reports:
	// the count lives there, not in replicated state.
	control := plane.NewLocalControlPlane(cluster.RaftNode, plane.WithFailureBudget(3))
	const cause = "data unavailable on every replica"

	// Inside the budget nothing is declared: the version is still retryable.
	for i := 0; i < 2; i++ {
		terminal, err := control.ReportVersionFailure(ctx, kbID, verResp.VersionId, types.FailureTransient, cause)
		if err != nil {
			t.Fatalf("report %d: %v", i+1, err)
		}
		if terminal {
			t.Fatalf("report %d declared the terminal verdict before the budget was spent", i+1)
		}
	}
	if got := failedPermanentOf(t, cluster, kbID, verResp.VersionId); got != nil {
		t.Fatalf("declared %+v before the budget was spent", got)
	}

	// The budget is now spent.
	terminal, err := control.ReportVersionFailure(ctx, kbID, verResp.VersionId, types.FailureTransient, cause)
	if err != nil {
		t.Fatalf("report 3: %v", err)
	}
	if !terminal {
		t.Fatal("the third report did not declare the terminal verdict, so §10.6's cleanup would never fire")
	}

	got := failedPermanentOf(t, cluster, kbID, verResp.VersionId)
	if got == nil {
		t.Fatal("the version is not reported as FAILED_PERMANENT after the budget was spent")
	}
	if !strings.Contains(got.GetReason(), cause) {
		t.Errorf("reason = %q, want it to carry the reported cause", got.GetReason())
	}
	if got.GetFailureCount() != 3 {
		t.Errorf("failure_count = %d, want 3", got.GetFailureCount())
	}

	// The verdict is replicated state, not this node's memory: the version
	// itself must report FAILED_PERMANENT too, which is what makes every node
	// agree on it.
	versions, err := cluster.RaftNode.ListVersions(ctx, kbID)
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	var found bool
	for _, v := range versions {
		if v.VersionID == verResp.VersionId {
			found = true
			if v.IndexStatus.String() != "FAILED_PERMANENT" {
				t.Errorf("state-machine status = %s, want FAILED_PERMANENT", v.IndexStatus)
			}
		}
	}
	if !found {
		t.Fatalf("version %d vanished from the state machine", verResp.VersionId)
	}
}

// failedPermanentOf returns the reported terminal verdict for a version, or nil.
func failedPermanentOf(t *testing.T, cluster *testCluster, kbID string, versionID int64) *pb.FailedVersion {
	t.Helper()
	status, err := cluster.AdminClient.GetSystemStatus(context.Background(), &pb.GetSystemStatusRequest{})
	if err != nil {
		t.Fatalf("GetSystemStatus: %v", err)
	}
	for _, f := range status.GetFailedPermanentVersions() {
		if f.GetKbId() == kbID && f.GetVersionId() == versionID {
			return f
		}
	}
	return nil
}
