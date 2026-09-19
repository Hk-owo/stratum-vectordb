//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
	"stratum/service"
)

// The await/discard half of docs/await-version-plan.md, on a live cluster.
//
// What only a real cluster can show, and why these cases exist on top of the
// unit tests:
//
//   - the answer is READ from replicated state, so every control node gives the
//     same one for the same version — that is what makes "reconnect somewhere
//     else and keep waiting" work, and a per-node cache would break it silently;
//   - DiscardVersion's admission is a compare-and-set inside the Raft apply, so
//     the refusals have to be observed across a real proposal, not in-process;
//   - a write that cannot land is the case the plan is about, and it takes a
//     stopped embedder to produce one (nothing else makes a version stay PENDING
//     long enough to look at).

// --- helpers -------------------------------------------------------------

// createKB provisions a knowledge base, trying every control node: creating one
// may be accepted by a follower and forwarded, so any node can serve the call.
func createKB(t *testing.T, ctx context.Context, label string) string {
	t.Helper()
	req := newKBRequest(label)
	var lastErr error
	for _, addr := range nodeAddrs {
		kb, _, _, conn, err := dialNode(addr)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := kb.CreateKnowledgeBase(ctx, req)
		conn.Close()
		if err == nil {
			t.Logf("created KB %s for %s", resp.GetKnowledgeBaseId(), label)
			return resp.GetKnowledgeBaseId()
		}
		lastErr = err
	}
	t.Fatalf("CreateKnowledgeBase never succeeded: %v", lastErr)
	return ""
}

// createVersionThroughControl submits a batch of changes and returns the
// version it allocated, re-resolving the leader the way a real client does:
// "not leader" is a routing fact, not a failure of the request.
func createVersionThroughControl(t *testing.T, ctx context.Context, kbID string, changes []*pb.DocChange) int64 {
	t.Helper()
	return createVersionThroughControlWithKey(t, ctx, kbID, fmt.Sprintf("t4-await-%d", time.Now().UnixNano()), changes)
}

// createVersionThroughControlWithKey is the same call with the caller's own
// idempotency key, which is what makes a re-send land on the same version.
func createVersionThroughControlWithKey(t *testing.T, ctx context.Context, kbID, clientRequestID string, changes []*pb.DocChange) int64 {
	t.Helper()
	return createVersionResponseThroughControl(t, ctx, kbID, clientRequestID, changes).GetVersionId()
}

// createVersionResponseThroughControl submits a batch and returns the WHOLE
// response, which §7 Step 4 is about: the key the caller is handed is the one
// the write committed under.
func createVersionResponseThroughControl(t *testing.T, ctx context.Context, kbID, clientRequestID string, changes []*pb.DocChange) *pb.CreateVersionResponse {
	t.Helper()
	req := &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: clientRequestID,
		Changes:         changes,
	}
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, addr := range nodeAddrs {
			_, _, _, conn, err := dialNode(addr)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err := pb.NewKnowledgeBaseServiceClient(conn).CreateVersion(ctx, req)
			conn.Close()
			if err == nil {
				return resp
			}
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("CreateVersion for %s never reached a leader: %v", kbID, lastErr)
	return nil
}

// waitUntilVersionGone polls ListVersions until the version is no longer
// listed.
//
// ListVersions is a LOCAL read of replicated state, and the design documents
// that a node can answer it from a state that is complete but slightly behind
// (the same reason GetClusterStatus is documented as eventually-consistent). A
// removal therefore takes a moment to become visible everywhere, and an
// assertion that does not allow for that would be testing the station's
// load-balancing luck rather than the discard.
func waitUntilVersionGone(ctx context.Context, kb pb.KnowledgeBaseServiceClient, kbID string, versionID int64, within time.Duration) error {
	deadline := time.Now().Add(within)
	for {
		versions, err := kb.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
		if err != nil {
			return fmt.Errorf("ListVersions: %w", err)
		}
		listed := false
		for _, v := range versions.GetVersions() {
			if v.GetVersionId() == versionID {
				listed = true
				break
			}
		}
		if !listed {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("version %d is still listed %s after being discarded", versionID, within)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// awaitOnce asks ONE node for one version's current stage.
func awaitOnce(ctx context.Context, client pb.KnowledgeBaseServiceClient, kbID string, versionID int64, waitMs int64) (*pb.AwaitVersionResponse, error) {
	return client.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       versionID,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   waitMs,
	})
}

// settledStage reports whether a stage means "stop asking about this version".
func settledStage(stage string) bool {
	switch stage {
	case service.StageIndexReady, service.StageIndexFailed,
		service.StageDataFailedPermanent, service.StageIndexFailedPermanent,
		service.StageDeleting:
		return true
	}
	return false
}

// awaitUntilSettled polls one node until the version stops moving.
func awaitUntilSettled(t *testing.T, ctx context.Context, addr, kbID string, versionID int64) *pb.AwaitVersionResponse {
	t.Helper()
	kb, _, _, conn, err := dialNode(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(90 * time.Second)
	var last *pb.AwaitVersionResponse
	for time.Now().Before(deadline) {
		resp, err := awaitOnce(ctx, kb, kbID, versionID, 5000)
		if err != nil {
			t.Fatalf("AwaitVersion(%d) on %s: %v", versionID, addr, err)
		}
		last = resp
		if settledStage(resp.GetStage()) {
			return resp
		}
	}
	t.Fatalf("version %d never settled within 90s; last stage = %s", versionID, last.GetStage())
	return nil
}

// --- cases ---------------------------------------------------------------

// TestT4_AwaitVersion_ConvergesFromEveryControlNode pins the property the whole
// design rests on: the wait has no home node.
func TestT4_AwaitVersion_ConvergesFromEveryControlNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "await-converge")
	versionID := writeDocumentThroughControl(t, ctx, kbID, "doc-1", "await 收敛：这篇文档必须在每个控制节点上看到同一个结论")

	for _, addr := range nodeAddrs {
		resp := awaitUntilSettled(t, ctx, addr, kbID, versionID)
		if resp.GetStage() != service.StageIndexReady {
			t.Errorf("%s: stage = %s, want %s (the write is a plain one-document ADD)",
				addr, resp.GetStage(), service.StageIndexReady)
		}
		if got := resp.GetVersion().GetVersionId(); got != versionID {
			t.Errorf("%s: version.version_id = %d, want %d", addr, got, versionID)
		}
		if resp.GetVersion().GetIndexStatus() != pb.IndexStatus_INDEX_STATUS_READY {
			t.Errorf("%s: version.index_status = %v, want READY (the metadata travels whole)",
				addr, resp.GetVersion().GetIndexStatus())
		}
		if resp.GetRetryAfterMs() <= 0 {
			t.Errorf("%s: retry_after_ms = %d, want a positive re-ask interval", addr, resp.GetRetryAfterMs())
		}
		// A READY version is past every question the probe asks; answering
		// "missing" here would be the worst kind of wrong.
		if resp.GetDataMissing() {
			t.Errorf("%s: data_missing = true for a READY version", addr)
		}
	}
}

// TestT4_AwaitVersion_NotYetIsAnAnswerNotAnError is the contract the RPC is
// judged by: an unfinished write must not be reported as a failure, and the
// caller must be told how long to wait before asking again.
func TestT4_AwaitVersion_NotYetIsAnAnswerNotAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "await-not-yet")

	// A batch big enough to be worth waiting on, so the first call has a real
	// chance of landing mid-flight.
	changes := make([]*pb.DocChange, 0, 300)
	for i := 0; i < 300; i++ {
		changes = append(changes, &pb.DocChange{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   fmt.Sprintf("doc-%03d", i),
			Content: fmt.Sprintf("批量写入的第 %d 篇文档，用来让第一次 await 落在写入过程中。", i),
		})
	}
	versionID := createVersionThroughControl(t, ctx, kbID, changes)

	kb, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial %s: %v", nodeAddrs[0], err)
	}
	defer conn.Close()

	// A short window on purpose: the answer must come back either way.
	start := time.Now()
	resp, err := awaitOnce(ctx, kb, kbID, versionID, 200)
	if err != nil {
		t.Fatalf("AwaitVersion = %v, want an answer (unfinished is not an error)", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Errorf("AwaitVersion took %v for a 200 ms window", elapsed)
	}
	switch resp.GetStage() {
	case service.StageDataPending, service.StageDataDurable, service.StageIndexReady:
	default:
		t.Errorf("stage = %s, want an in-progress or READY stage for a write that just started", resp.GetStage())
	}
	if resp.GetRetryAfterMs() <= 0 {
		t.Errorf("retry_after_ms = %d, want a positive re-ask interval", resp.GetRetryAfterMs())
	}

	// And it does converge.
	if settled := awaitUntilSettled(t, ctx, nodeAddrs[0], kbID, versionID); settled.GetStage() != service.StageIndexReady {
		t.Errorf("stage after waiting = %s, want %s", settled.GetStage(), service.StageIndexReady)
	}
}

// TestT4_AwaitVersion_UnknownVersionIsNotFound: "wait for a version that is not
// here" is the one case that IS an error, and it has to be distinguishable from
// "not yet".
func TestT4_AwaitVersion_UnknownVersionIsNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "await-unknown")
	kb, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial %s: %v", nodeAddrs[0], err)
	}
	defer conn.Close()

	_, err = awaitOnce(ctx, kb, kbID, 999999, 1000)
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
	if _, err := awaitOnce(ctx, kb, "no-such-kb", 1, 1000); status.Code(err) != codes.NotFound {
		t.Errorf("unknown KB: code = %v, want NotFound", status.Code(err))
	}
}

// TestT4_DiscardVersion_RefusesASettledVersion: the refusal that keeps a
// caller's belief from destroying durable data. A version whose data landed is
// DeleteVersion's business.
func TestT4_DiscardVersion_RefusesASettledVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "discard-settled")
	versionID := writeDocumentThroughControl(t, ctx, kbID, "doc-1", "discard 拒绝：这篇文档一旦落地，就不能再被放弃")
	if settled := awaitUntilSettled(t, ctx, nodeAddrs[0], kbID, versionID); settled.GetStage() != service.StageIndexReady {
		t.Fatalf("fixture: version %d = %s, want READY", versionID, settled.GetStage())
	}

	kb, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial %s: %v", nodeAddrs[0], err)
	}
	defer conn.Close()

	_, err = kb.DiscardVersion(ctx, &pb.DiscardVersionRequest{KnowledgeBaseId: kbID, VersionId: versionID})
	if err == nil {
		t.Fatal("DiscardVersion on a READY version = nil error, want a refusal")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", code)
	}
	// The wire name is what a caller switches on: the gRPC code alone also
	// covers "the index is not ready yet", which is a different instruction.
	if reason := stratumerrors.ReasonOf(err); reason != "version_not_pending" {
		t.Errorf("reason = %q, want %q", reason, "version_not_pending")
	}
}

// TestT4_DiscardVersion_RefusesTheActiveVersion: discarding the version in
// service would be a way to make a knowledge base read nothing at all.
func TestT4_DiscardVersion_RefusesTheActiveVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "discard-active")
	versionID := writeDocumentThroughControl(t, ctx, kbID, "doc-1", "discard 拒绝：激活版本不能被放弃")
	if settled := awaitUntilSettled(t, ctx, nodeAddrs[0], kbID, versionID); settled.GetStage() != service.StageIndexReady {
		t.Fatalf("fixture: version %d = %s, want READY", versionID, settled.GetStage())
	}

	kb, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial %s: %v", nodeAddrs[0], err)
	}
	defer conn.Close()

	if _, err := kb.RollbackVersion(ctx, &pb.RollbackVersionRequest{KnowledgeBaseId: kbID, TargetVersionId: versionID}); err != nil {
		t.Fatalf("RollbackVersion(%d): %v", versionID, err)
	}

	_, err = kb.DiscardVersion(ctx, &pb.DiscardVersionRequest{KnowledgeBaseId: kbID, VersionId: versionID})
	if err == nil {
		t.Fatal("DiscardVersion on the active version = nil error, want a refusal")
	}
	if reason := stratumerrors.ReasonOf(err); reason != "version_is_active" {
		t.Errorf("reason = %q, want %q", reason, "version_is_active")
	}
}

// TestT4_DiscardVersion_AbandonsAWriteThatCannotLand is the case the plan was
// written for: a version whose data will never arrive, the caller giving up on
// it, and starting over under the same key.
//
// The un-landable write is produced by stopping the embedder: chunking and
// embedding happen on the way to storage, so every attempt fails and the version
// stays PENDING — which is exactly the state await reports and discard admits.
func TestT4_DiscardVersion_AbandonsAWriteThatCannotLand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "discard-pending")

	// The container name is the same in both supported topologies
	// (scripts/cluster.sh --topology single and scripts/cluster.sh --topology two-tier both call it
	// stratum-embed), so this case runs unchanged under CI's all-in-one cluster.
	dockerCmd(t, "stop", "stratum-embed")
	defer func() {
		dockerCmd(t, "start", "stratum-embed")
	}()

	requestID := fmt.Sprintf("discard-t4-%d", time.Now().UnixNano())
	versionID := createVersionThroughControlWithKey(t, ctx, kbID, requestID, []*pb.DocChange{{
		Op:      pb.ChangeOp_CHANGE_OP_ADD,
		DocId:   "doc-1",
		Content: "这篇文档永远不会落地：embedder 停着。",
	}})

	kb, _, _, conn, err := dialNode(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial %s: %v", nodeAddrs[0], err)
	}
	defer conn.Close()

	// It stays PENDING, and that is reported as a stage rather than as a failure.
	resp, err := awaitOnce(ctx, kb, kbID, versionID, 3000)
	if err != nil {
		t.Fatalf("AwaitVersion on an un-landable write = %v, want an answer", err)
	}
	if resp.GetStage() != service.StageDataPending {
		t.Fatalf("stage = %s, want %s (nothing can have landed with the embedder stopped)",
			resp.GetStage(), service.StageDataPending)
	}
	// Below the probe's age threshold the answer must be "unknown", not a claim.
	if resp.GetDataMissing() {
		t.Error("data_missing = true below the age threshold, want false (unknown)")
	}

	// Past the threshold the control layer probes the candidate replicas; with
	// the writer dead and nothing on disk, the honest answer is "nobody has it".
	if !testing.Short() {
		time.Sleep(125 * time.Second)
		resp, err = awaitOnce(ctx, kb, kbID, versionID, 3000)
		if err != nil {
			t.Fatalf("AwaitVersion after the threshold: %v", err)
		}
		if !resp.GetDataMissing() {
			t.Errorf("data_missing = false after 125 s with the embedder stopped, want true")
		}
		if resp.GetStage() != service.StageDataPending {
			t.Errorf("stage = %s, want %s (the server does not declare it dead)", resp.GetStage(), service.StageDataPending)
		}
	}

	// The caller gives up on it.
	discarded, err := kb.DiscardVersion(ctx, &pb.DiscardVersionRequest{KnowledgeBaseId: kbID, VersionId: versionID})
	if err != nil {
		t.Fatalf("DiscardVersion on a version that never landed: %v", err)
	}
	if !discarded.GetDiscarded() {
		t.Error("discarded = false, want true")
	}

	// Gone from the chain (allowing for the local read to catch up), and a
	// repeat says so instead of failing.
	if err := waitUntilVersionGone(ctx, kb, kbID, versionID, 20*time.Second); err != nil {
		t.Error(err)
	}
	repeat, err := kb.DiscardVersion(ctx, &pb.DiscardVersionRequest{KnowledgeBaseId: kbID, VersionId: versionID})
	if err != nil {
		t.Fatalf("repeated DiscardVersion = %v, want a no-op answer", err)
	}
	if repeat.GetDiscarded() {
		t.Error("repeated discards reported discarded = true, want false")
	}

	// Starting over: the abandoned key must not drag the caller back to a
	// version that no longer exists.
	dockerCmd(t, "start", "stratum-embed")
	time.Sleep(2 * time.Second)
	retryID := createVersionThroughControlWithKey(t, ctx, kbID, requestID, []*pb.DocChange{{
		Op:      pb.ChangeOp_CHANGE_OP_ADD,
		DocId:   "doc-1",
		Content: "重发：这次 embedder 回来了。",
	}})
	if retryID == versionID {
		t.Errorf("re-sending under the discarded key returned the discarded version %d", versionID)
	}
	if settled := awaitUntilSettled(t, ctx, nodeAddrs[0], kbID, retryID); settled.GetStage() != service.StageIndexReady {
		t.Errorf("the re-sent version = %s, want %s", settled.GetStage(), service.StageIndexReady)
	}
}

// TestT4_CreateVersion_ReturnsTheKeyThatMakesAResendIdempotent is §7 Step 4 on a
// live cluster: the key in the response is the one the write was committed
// under, so a caller that never generated one can still re-send without
// allocating a second version — which is the only way it could recover a write
// whose data never landed (§7.12).
func TestT4_CreateVersion_ReturnsTheKeyThatMakesAResendIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "step4-key")
	changes := []*pb.DocChange{{
		Op:      pb.ChangeOp_CHANGE_OP_ADD,
		DocId:   "doc-1",
		Content: "Step 4：响应回传的幂等键必须能用来重发。",
	}}

	// No key on the way in.
	first := createVersionResponseThroughControl(t, ctx, kbID, "", changes)
	if first.GetClientRequestId() == "" {
		t.Fatal("CreateVersion returned no client_request_id: the caller has nothing to re-send under")
	}
	if first.GetVersionId() == 0 {
		t.Fatal("CreateVersion returned no version_id")
	}

	// Re-sending the same changes under the key the server handed back must
	// reuse the version. The parent is deliberately left unset here: the
	// idempotency check runs BEFORE the parent constraints, which is what lets a
	// caller re-send for a version that is still PENDING (and therefore cannot be
	// a parent yet).
	resend := createVersionResponseThroughControl(t, ctx, kbID, first.GetClientRequestId(), changes)
	if resend.GetVersionId() != first.GetVersionId() {
		t.Errorf("re-send under the returned key allocated version %d, want the original %d",
			resend.GetVersionId(), first.GetVersionId())
	}

	// And the write really did happen: the version converges on its own.
	if settled := awaitUntilSettled(t, ctx, nodeAddrs[0], kbID, first.GetVersionId()); settled.GetStage() != service.StageIndexReady {
		t.Errorf("the returned version = %s, want %s", settled.GetStage(), service.StageIndexReady)
	}
}
