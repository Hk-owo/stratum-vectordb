package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/internal/coordinator"
	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// mustKB / mustVersion keep the tests below about the operator-facing behaviour
// rather than about fixture plumbing.
func mustKB(t *testing.T, h *adminHarness, kbID string) {
	t.Helper()
	if err := h.raftNode.ProposeCreateKB(context.Background(), types.KnowledgeBaseMeta{
		KBID: kbID, Name: kbID, Status: types.KBStatusActive,
	}); err != nil {
		t.Fatalf("ProposeCreateKB(%s): %v", kbID, err)
	}
}

func mustVersion(t *testing.T, h *adminHarness, kbID string) int64 {
	t.Helper()
	id, err := h.raftNode.ProposeCreateVersion(context.Background(), kbID, 0)
	if err != nil {
		t.Fatalf("ProposeCreateVersion(%s): %v", kbID, err)
	}
	return id
}

// ListFailedVersions is the operator's work queue (§10.1): every version carrying a
// terminal verdict on either side, with the cause chain, per knowledge base and
// cluster-wide.
func TestListFailedVersions_ReportsEveryTerminalVerdict(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()

	mustKB(t, h, "kb-1")
	dataV := mustVersion(t, h, "kb-1")
	indexV := mustVersion(t, h, "kb-1")
	healthyV := mustVersion(t, h, "kb-1")
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, healthyV, types.IndexStatusReady, 0); err != nil {
		t.Fatal(err)
	}
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", dataV, types.FailureSideData, "data unavailable on every replica", 5); err != nil {
		t.Fatal(err)
	}
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", indexV, types.FailureSideIndex, "build crashed", 2); err != nil {
		t.Fatal(err)
	}
	// A second knowledge base: the cluster-wide form has to span them, which is the
	// question an operator asks right after finding the first one.
	mustKB(t, h, "kb-2")
	otherV := mustVersion(t, h, "kb-2")
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-2", otherV, types.FailureSideData, "disk full", 5); err != nil {
		t.Fatal(err)
	}

	one, err := h.svc.ListFailedVersions(ctx, &pb.ListFailedVersionsRequest{KnowledgeBaseId: "kb-1"})
	if err != nil {
		t.Fatalf("ListFailedVersions(kb-1): %v", err)
	}
	if got := len(one.GetVersions()); got != 2 {
		t.Fatalf("kb-1 failed versions = %d, want 2 (the healthy version is not in the queue)", got)
	}
	byVersion := map[int64]*pb.FailedVersion{}
	for _, v := range one.GetVersions() {
		byVersion[v.GetVersionId()] = v
	}
	if v := byVersion[dataV]; v == nil {
		t.Fatal("the data-side verdict is missing from the queue")
	} else {
		if v.GetReason() != "data unavailable on every replica" || v.GetFailureCount() != 5 {
			t.Errorf("cause chain = (%q, %d), want the recorded one", v.GetReason(), v.GetFailureCount())
		}
		if v.GetSide() != pb.FailureSide_FAILURE_SIDE_DATA {
			t.Errorf("side = %v, want FAILURE_SIDE_DATA", v.GetSide())
		}
	}
	if v := byVersion[indexV]; v == nil {
		t.Fatal("the index-side verdict is missing from the queue")
	} else if v.GetSide() != pb.FailureSide_FAILURE_SIDE_INDEX {
		t.Errorf("side = %v, want FAILURE_SIDE_INDEX: the two verdicts have different remedies", v.GetSide())
	}
	if _, ok := byVersion[healthyV]; ok {
		t.Error("a READY version is not waiting for anyone")
	}
	if _, ok := byVersion[otherV]; ok {
		t.Error("a version of another knowledge base leaked into the per-knowledge-base answer")
	}

	all, err := h.svc.ListFailedVersions(ctx, &pb.ListFailedVersionsRequest{})
	if err != nil {
		t.Fatalf("ListFailedVersions(everything): %v", err)
	}
	if got := len(all.GetVersions()); got != 3 {
		t.Errorf("cluster-wide failed versions = %d, want 3", got)
	}

	// A version already on its way out is not "waiting for a human": it is being
	// removed, and it is reported as such by deleting_versions instead.
	//
	// This also pins the end of §10.1's dead end from the service side: marking a
	// DATA-side verdict Deleting used to be refused as PENDING (the index side is
	// untouched by a data-side verdict), which left such a version with no way out.
	if _, err := h.raftNode.ProposeMarkVersionDeleting(ctx, "kb-1", dataV, types.VersionDeleteSubtree); err != nil {
		t.Fatalf("MarkVersionDeleting on a data-terminal version: %v", err)
	}
	after, err := h.svc.ListFailedVersions(ctx, &pb.ListFailedVersionsRequest{KnowledgeBaseId: "kb-1"})
	if err != nil {
		t.Fatalf("ListFailedVersions after the delete: %v", err)
	}
	if got := len(after.GetVersions()); got != 1 || after.GetVersions()[0].GetVersionId() != indexV {
		t.Errorf("failed versions after the delete = %v, want only v%d (the one being removed is not a task)", after.GetVersions(), indexV)
	}
}

// ForceRetryVersion revokes the index-side verdict and asks for another build. It
// exists because §10.1's "nothing re-triggers it automatically" is a statement about
// machines, not about the operator who decides the verdict was wrong.
func TestForceRetryVersion_RevokesTheIndexVerdict(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()

	mustKB(t, h, "kb-1")
	versionID := mustVersion(t, h, "kb-1")
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", versionID,
		types.FailureSideIndex, "build crashed", 3); err != nil {
		t.Fatal(err)
	}

	resp, err := h.svc.ForceRetryVersion(ctx, &pb.ForceRetryVersionRequest{KnowledgeBaseId: "kb-1", VersionId: versionID})
	if err != nil {
		t.Fatalf("ForceRetryVersion: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected success=true")
	}
	if resp.GetSide() != pb.FailureSide_FAILURE_SIDE_INDEX {
		t.Errorf("side = %v, want FAILURE_SIDE_INDEX: the answer says what was retried", resp.GetSide())
	}

	// The verdict is gone from the replicated metadata, cause chain included — the
	// difference between this and RebuildIndex's bare status overwrite.
	versions, err := h.raftNode.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range versions {
		if v.VersionID != versionID {
			continue
		}
		if v.IndexStatus != types.IndexStatusPending {
			t.Errorf("index status = %v, want PENDING", v.IndexStatus)
		}
		if v.FailureReason != "" || v.FailureCount != 0 {
			t.Errorf("cause chain = (%q, %d), want it cleared", v.FailureReason, v.FailureCount)
		}
	}

	// And a build was asked for, with the artifact registered as wanted first.
	builds := h.indexMgr.TriggeredBuilds()
	if len(builds) == 0 || builds[len(builds)-1] != versionID {
		t.Errorf("triggered builds = %v, want v%d last", builds, versionID)
	}
	interested := h.indexMgr.Interested()
	if len(interested) == 0 || interested[len(interested)-1] != versionID {
		t.Errorf("registered interest = %v, want v%d: the retention policy must not drop the artifact it just asked for", interested, versionID)
	}
}

// The refusals are the interface: a data-side verdict has no retry (its data will
// never arrive, so the honest answer is to abandon the version), a healthy version
// has nothing to retry, and a missing one is missing.
func TestForceRetryVersion_Refusals(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()

	mustKB(t, h, "kb-1")
	dataV := mustVersion(t, h, "kb-1")
	healthyV := mustVersion(t, h, "kb-1")
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", dataV, types.FailureSideData, "data unavailable", 5); err != nil {
		t.Fatal(err)
	}
	if err := h.raftNode.ProposeUpdateVersionStatus(ctx, healthyV, types.IndexStatusReady, 0); err != nil {
		t.Fatal(err)
	}

	_, err := h.svc.ForceRetryVersion(ctx, &pb.ForceRetryVersionRequest{KnowledgeBaseId: "kb-1", VersionId: dataV})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retrying a data-side verdict = %v (%v), want FailedPrecondition", err, status.Code(err))
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "ForceAbandonVersion") {
		t.Errorf("refusal message = %q, want it to name the operation that does apply", msg)
	}

	_, err = h.svc.ForceRetryVersion(ctx, &pb.ForceRetryVersionRequest{KnowledgeBaseId: "kb-1", VersionId: healthyV})
	if stratumerrors.ReasonOf(err) != "version_not_failed_permanent" {
		t.Errorf("retrying a healthy version = %v (reason %q), want version_not_failed_permanent", err, stratumerrors.ReasonOf(err))
	}

	_, err = h.svc.ForceRetryVersion(ctx, &pb.ForceRetryVersionRequest{KnowledgeBaseId: "kb-1", VersionId: 999})
	if status.Code(err) != codes.NotFound {
		t.Errorf("retrying a missing version = %v (%v), want NotFound", err, status.Code(err))
	}
}

// ForceAbandonVersion is the other answer: the terminated version leaves the chain,
// with the same asynchronous cleanup DeleteVersion uses.
func TestForceAbandonVersion_RemovesATerminatedVersion(t *testing.T) {
	h := newAdminHarness()
	ctx := context.Background()
	dvc := coordinator.NewMockDeleteVersionCoordinator()
	h.svc.SetDeleteVersionCoordinator(dvc)

	mustKB(t, h, "kb-1")
	versionID := mustVersion(t, h, "kb-1")
	if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", versionID,
		types.FailureSideData, "data unavailable", 5); err != nil {
		t.Fatal(err)
	}

	resp, err := h.svc.ForceAbandonVersion(ctx, &pb.ForceAbandonVersionRequest{KnowledgeBaseId: "kb-1", VersionId: versionID})
	if err != nil {
		t.Fatalf("ForceAbandonVersion: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("expected success=true")
	}
	if got := resp.GetDeletedVersionIds(); len(got) != 1 || got[0] != versionID {
		t.Errorf("deleted_version_ids = %v, want [%d]: SINGLE mode, the version the operator named", got, versionID)
	}

	// The cleanup is asynchronous, exactly as DeleteVersion's is.
	time.Sleep(10 * time.Millisecond)
	if calls := dvc.Calls(); len(calls) != 1 || calls[0] != "kb-1" {
		t.Errorf("DeleteVersionCoordinator.Execute calls = %v, want [kb-1]", calls)
	}

	versions, err := h.raftNode.ListVersions(ctx, "kb-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range versions {
		if v.VersionID == versionID && !v.Deleting {
			t.Error("the version should be marked Deleting")
		}
	}
}

func TestForceAbandonVersion_Refusals(t *testing.T) {
	ctx := context.Background()

	t.Run("a healthy version is DeleteVersion's business", func(t *testing.T) {
		h := newAdminHarness()
		h.svc.SetDeleteVersionCoordinator(coordinator.NewMockDeleteVersionCoordinator())
		mustKB(t, h, "kb-1")
		versionID := mustVersion(t, h, "kb-1")
		if err := h.raftNode.ProposeUpdateVersionStatus(ctx, versionID, types.IndexStatusReady, 0); err != nil {
			t.Fatal(err)
		}

		_, err := h.svc.ForceAbandonVersion(ctx, &pb.ForceAbandonVersionRequest{KnowledgeBaseId: "kb-1", VersionId: versionID})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("abandoning a healthy version = %v (%v), want FailedPrecondition", err, status.Code(err))
		}
	})

	t.Run("missing version", func(t *testing.T) {
		h := newAdminHarness()
		h.svc.SetDeleteVersionCoordinator(coordinator.NewMockDeleteVersionCoordinator())
		mustKB(t, h, "kb-1")

		_, err := h.svc.ForceAbandonVersion(ctx, &pb.ForceAbandonVersionRequest{KnowledgeBaseId: "kb-1", VersionId: 999})
		if status.Code(err) != codes.NotFound {
			t.Errorf("abandoning a missing version = %v (%v), want NotFound", err, status.Code(err))
		}
	})

	t.Run("without a cleanup coordinator", func(t *testing.T) {
		h := newAdminHarness() // no SetDeleteVersionCoordinator
		mustKB(t, h, "kb-1")
		versionID := mustVersion(t, h, "kb-1")
		if err := h.raftNode.ProposeMarkVersionFailedPermanent(ctx, "kb-1", versionID,
			types.FailureSideData, "data unavailable", 5); err != nil {
			t.Fatal(err)
		}

		_, err := h.svc.ForceAbandonVersion(ctx, &pb.ForceAbandonVersionRequest{KnowledgeBaseId: "kb-1", VersionId: versionID})
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("abandoning without a coordinator = %v (%v), want Unimplemented: marking a version Deleting and leaving nobody to clean it up is worse than refusing", err, status.Code(err))
		}
	})
}
