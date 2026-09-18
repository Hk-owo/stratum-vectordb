//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	stratumerrors "stratum/internal/errors"
)

// The two wire names a storage refusal can carry. They are strings here rather
// than an import of the errors package because this is what crosses the wire — and
// the test asserts on the wire, not on a shared constant.
const (
	reasonKBStorageDegraded  = "kb_storage_degraded"
	reasonStorageUnavailable = "storage_unavailable"
)

// createVersionOnce asks ONE control node for a version and returns whatever comes
// back, refusal included.
//
// The existing retrying helper (writeDocumentThroughControl) cannot see a gate: it
// treats every error as "ask another node", so a refusal would surface only as its
// own timeout and the reason would never be asserted on.
func createVersionOnce(ctx context.Context, addr, kbID, docID string) (*pb.CreateVersionResponse, error) {
	kb, _, _, conn, err := dialNode(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return kb.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: fmt.Sprintf("degraded-%d", time.Now().UnixNano()),
		Changes: []*pb.DocChange{{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   docID,
			Content: "written while the storage layer is being watched",
		}},
	})
}

// awaitWriteRefusal polls the control tier until a write is refused with the named
// reason, or the deadline passes.
//
// The reason is a parameter because the two failure tiers carry two names
// (docs/storage-degradation-signal-plan.md §4.1), and asserting the wrong one
// would pass for the wrong fault.
//
// Nothing here is instantaneous by design: the verdict is built from periodic
// reports, so it takes a silence window (plane.DefaultStorageSilenceWindow, 15s)
// plus a route refresh to appear.
func awaitWriteRefusal(t *testing.T, ctx context.Context, kbID, reason string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		lastErr  error
		accepted int
	)
	for time.Now().Before(deadline) {
		for _, addr := range nodeAddrs {
			resp, err := createVersionOnce(ctx, addr, kbID, fmt.Sprintf("d-degraded-%d", time.Now().UnixNano()))
			if err == nil {
				accepted++
				lastErr = nil
				t.Logf("%s accepted a write as v%d; the verdict has not landed yet", addr, resp.GetVersionId())
				continue
			}
			if stratumerrors.ReasonOf(err) == reason {
				return err
			}
			// Some other failure (a settling election, or the other tier): keep
			// polling rather than reading it as the gate having engaged.
			lastErr = err
			t.Logf("%s returned %q (want %q): %v", addr, stratumerrors.ReasonOf(err), reason, err)
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("no write was refused for %q within %v; last error: %v", reason, timeout, lastErr)
	}
	t.Fatalf("writes were still accepted after %v (%d of them): the storage gate never engaged", timeout, accepted)
	return nil
}

// awaitWriteAcceptance polls until the control tier commits a write again, which is
// how the gate lifting is observed from outside.
func awaitWriteAcceptance(t *testing.T, ctx context.Context, kbID string, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastRefusal error
	for time.Now().Before(deadline) {
		for _, addr := range nodeAddrs {
			resp, err := createVersionOnce(ctx, addr, kbID, fmt.Sprintf("d-recovered-%d", time.Now().UnixNano()))
			if err == nil {
				t.Logf("%s accepted a write again as v%d", addr, resp.GetVersionId())
				return resp.GetVersionId()
			}
			lastRefusal = err
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("writes were still refused %v after a replica came back: %v", timeout, lastRefusal)
	return 0
}

// storageLogsSince returns one container's log lines from the last `since`.
//
// Distinct from nodeLogsSince (stress_test.go), which returns the whole log: the
// question here is "is it busy NOW", and the whole log is history.
func storageLogsSince(service, since string) string {
	out, err := exec.Command("docker", "logs", "--since", since, service).CombinedOutput()
	if err != nil {
		return ""
	}
	return string(out)
}

// awaitStorageReconcileSettled waits until no storage node has logged index
// reconciliation for a whole `quiet` window.
//
// A restarted storage node replays every knowledge base it holds, rebuilding the
// indexes it lacks — and that runs in the BACKGROUND: the container's health check
// only says the process answers. Handing the group back at that point leaves three
// replicas competing with the next test's index loads, which then surfaces as an
// `index load timeout` in a test that has nothing to do with this one.
//
// Measured, not theorized: this test killing all three replicas and returning as
// soon as they were healthy made TestT4_MultiVersionEviction fail 226 s later
// (`index load timeout`), and it passes once the settle is waited out.
//
// Timing out is logged rather than failed: the subject here is the refusal, and a
// still-busy cluster is the next test's problem to report.
func awaitStorageReconcileSettled(t *testing.T, quiet, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		busy := ""
		for _, svc := range storageServices {
			if strings.Contains(storageLogsSince(svc, quiet.String()), "reconcile") {
				busy = svc
				break
			}
		}
		if busy == "" {
			t.Logf("storage group finished reconciling (quiet for %v)", quiet)
			return
		}
		if time.Now().After(deadline) {
			t.Logf("%s was still reconciling after %v; the next test may see slow index loads", busy, timeout)
			return
		}
		time.Sleep(5 * time.Second)
	}
}

// docs/storage-degradation-signal-plan.md §8, the integration case: with the storage
// tier below quorum a WRITE is refused by name and retryably — instead of burning a
// version number, a Raft entry and a retry budget per attempt — a READ of an
// already-durable version keeps working, and the gate lifts once the replicas are
// back.
func TestT4_StorageDegradationRefusesWritesButStillServesReads(t *testing.T) {
	if len(storageServices) < 3 || len(storageAddrs) < 3 {
		t.Skipf("needs at least 3 storage nodes, have %d", len(storageServices))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "storage-degraded", 30*time.Second)
	t.Logf("control leader is node %d (%s), KB %s", leaderIdx, nodeAddrs[leaderIdx], kbID)

	// A durable version, written while the storage tier is healthy. It is what the
	// read half of this test asks for once the write half is being refused.
	versionID := writeDocumentThroughControl(t, ctx, kbID, "d-healthy",
		"健康时写入的版本：存储层降级之后，它必须仍然可读。")
	t.Logf("committed %s v%d while healthy", kbID, versionID)

	// Below quorum: 3 replicas, QuorumSize(3) = 2, so two victims leave one live.
	victims := storageServices[1:]
	for _, victim := range victims {
		killNode(t, victim)
	}
	defer func() {
		for _, victim := range victims {
			startNode(t, victim)
		}
	}()

	refusal := awaitWriteRefusal(t, ctx, kbID, reasonKBStorageDegraded, 120*time.Second)
	if got := status.Code(refusal); got != codes.Unavailable {
		t.Errorf("refusal code = %s, want Unavailable: the verdict is soft state, so the caller has to be able to retry", got)
	}
	// The diagnosis has to survive to the caller: a refusal nobody can act on is
	// worse than the retry it saves (§4.3).
	if msg := status.Convert(refusal).Message(); msg == "" {
		t.Error("the refusal carried no diagnosis")
	} else {
		t.Logf("write refused as expected: %s", msg)
	}

	// READS ARE NOT REFUSED (§2 non-goals). A replica that still holds the data still
	// serves it, and that trade-off is the reason this signal is not a health check.
	if resp := awaitServable(t, ctx, storageAddrs[0], kbID, versionID, 90*time.Second); resp == nil {
		t.Errorf("the surviving replica stopped serving %s v%d: below quorum, reads must keep working",
			kbID, versionID)
	}

	// One replica back is 2 of 3 — a quorum again — so the gate has to lift. The
	// verdict is derived, so this is observed by the write succeeding, not by a flag
	// being cleared.
	startNode(t, victims[0])
	awaitWriteAcceptance(t, ctx, kbID, 180*time.Second)
}

// The extreme tier carries its own name. Losing every storage replica is the same
// "cannot write" answer as being one short, but the error says which happened —
// storage_unavailable rather than kb_storage_degraded — so whoever reads it can
// tell "the storage layer is gone" from "it needs a replica back"
// (docs/storage-degradation-signal-plan.md §4.1).
func TestT4_StorageUnavailabilityIsNamedDistinctly(t *testing.T) {
	if len(storageServices) < 3 {
		t.Skipf("needs at least 3 storage nodes, have %d", len(storageServices))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "storage-unavailable", 30*time.Second)
	t.Logf("control leader is node %d (%s), KB %s", leaderIdx, nodeAddrs[leaderIdx], kbID)

	// Not one replica left: the tier is decided by "is anyone answering at all",
	// not by the quorum arithmetic (which would also be satisfied by this).
	for _, victim := range storageServices {
		killNode(t, victim)
	}
	defer func() {
		for _, victim := range storageServices {
			startNode(t, victim)
		}
		// Hand the group back HEALTHY, not merely started. Three replicas left
		// reconciling make the next test's index loads time out, which then reads
		// as an unrelated failure — this is exactly the trap
		// lag_catchup_test.go's helper exists for.
		waitForStorageGroupReady(t, 10*time.Minute)
		// ...and health is not enough, because it only says the process answers.
		// Measured: with this test handing the group back on health alone,
		// TestT4_MultiVersionEviction failed 226 s later with `index load timeout`;
		// with the rebuild waited out, it passes.
		awaitStorageReconcileSettled(t, 20*time.Second, 8*time.Minute)
	}()

	refusal := awaitWriteRefusal(t, ctx, kbID, reasonStorageUnavailable, 150*time.Second)
	if got := status.Code(refusal); got != codes.Unavailable {
		t.Errorf("refusal code = %s, want Unavailable: same retryable answer as the other tier", got)
	}
	if msg := status.Convert(refusal).Message(); !strings.Contains(msg, "no required replica is live") {
		t.Errorf("message = %q, want the cluster-tier diagnosis rather than a quorum shortfall", msg)
	} else {
		t.Logf("write refused with the cluster tier: %s", msg)
	}
}
