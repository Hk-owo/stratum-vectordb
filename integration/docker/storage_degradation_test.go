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
)

// reasonKBStorageDegraded is the wire name the storage gate's refusal carries. It
// is a string here rather than an import of plane because this is what crosses the
// wire — and the test asserts on the wire, not on a shared constant.
const reasonKBStorageDegraded = "kb_storage_degraded"

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

// awaitWriteRefusal polls the control tier until a write is refused with the
// storage gate's reason, or the deadline passes.
//
// Nothing here is instantaneous by design: the verdict is built from periodic
// reports, so it takes a silence window (plane.DefaultStorageSilenceWindow, 15s)
// plus a route refresh to appear.
func awaitWriteRefusal(t *testing.T, ctx context.Context, kbID string, timeout time.Duration) error {
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
			if stratumerrors.ReasonOf(err) == reasonKBStorageDegraded {
				return err
			}
			// Some other failure (a settling election, say): keep polling rather
			// than reading it as the gate having engaged.
			lastErr = err
			t.Logf("%s returned an unrelated error: %v", addr, err)
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("no write was refused for %q within %v; last error: %v", reasonKBStorageDegraded, timeout, lastErr)
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

	refusal := awaitWriteRefusal(t, ctx, kbID, 120*time.Second)
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
