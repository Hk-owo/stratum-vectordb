//go:build docker
// +build docker

package docker_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// storageReplicaAddrs are the storage nodes THEMSELVES — not the station the
// rest of this suite talks to. Reaching past the station is the whole point of
// the case below (see its comment).
var storageReplicaAddrs = splitEnv("STRATUM_T4_STORAGE_NODE_ADDRS",
	"localhost:17100,localhost:17101,localhost:17102")

// TestT4_UnconvergedReplicaDoesNotAnswerEmpty pins the rule that a replica which
// has not received a version's data yet must REFUSE the query retryably, never
// answer "nothing matches".
//
// Why this needs its own case, and why it must bypass the station:
//
//   - A replica's data arrives in stages (the chunk→doc mapping, the version's
//     document set, the vectors), so there is a window where the replica holds
//     part of a version and cannot serve it. Two judgements inside the node used
//     to collapse that window's "I cannot serve this yet" into "this version has
//     nothing" — the document-set filter (an empty read for a version whose
//     committed digest is NOT the empty set's) and the no-index scan path (not a
//     single chunk vector readable). Both answered `results=0, err=nil`, which
//     is indistinguishable from a genuine "no matching documents".
//   - Going through the station HIDES the defect: the station simply asks
//     another replica, so the caller sees a normal answer. Measured on the 3+3
//     cluster: the same query read `results=0, err=nil` ~14 s when asked of the
//     lagging replica directly, while the station answered from a replica that
//     had the data. That is exactly why the earlier station-based runs never
//     caught it.
//
// The case therefore takes one replica down, writes a version the others
// complete, brings it back, and asks IT (directly) until it catches up. Every
// answer must be one of: a retryable refusal (another replica may serve it), or
// a real result. An empty result with no error fails the case.
func TestT4_UnconvergedReplicaDoesNotAnswerEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if len(storageServices) < 2 || len(storageReplicaAddrs) < 2 {
		t.Skip("needs at least two storage replicas")
	}
	const down = 1 // storage2
	trusted := metadata.AppendToOutgoingContext(ctx, "x-stratum-authenticated", "1")

	// --- Make one replica miss a write ---
	killNode(t, storageServices[down])
	kbID := createKB(t, ctx, "stale-replica")
	versionID := writeDocumentThroughControl(t, ctx, kbID, "doc-1",
		"尚未收敛的副本必须说“我还不能服务这个版本”，而不是“这里没有匹配的文档”。")
	waitVersionStatus(t, ctx, nodeAddrs[0], kbID, versionID,
		pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	startNode(t, storageServices[down])
	t.Logf("kb=%s v=%d written while %s was down; it restarted and is catching up",
		kbID, versionID, storageServices[down])

	// --- Ask the catching-up replica directly until it serves the version ---
	deadline := time.Now().Add(90 * time.Second)
	var refusals, empties, served int
	for time.Now().Before(deadline) {
		resp, err := queryReplicaDirectly(ctx, trusted, storageReplicaAddrs[down], kbID, versionID)
		switch {
		case err != nil:
			// Retryable is the requirement: a caller (the station always is one)
			// has to be able to move to a replica that can serve this version.
			switch status.Code(err) {
			case codes.Unavailable, codes.FailedPrecondition, codes.DeadlineExceeded:
				refusals++
			default:
				t.Fatalf("the refusal is not retryable (code %v): %v", status.Code(err), err)
			}
		case len(resp.GetResults()) > 0:
			served++
		default:
			empties++
		}
		if served > 0 {
			break
		}
		time.Sleep(700 * time.Millisecond)
	}

	if served == 0 {
		t.Fatalf("the replica never served the version inside the window (refusals=%d empty-answers=%d)", refusals, empties)
	}
	if empties > 0 {
		t.Errorf("the replica answered %d times with an empty result and NO error while it had not received the version: "+
			"that answer is indistinguishable from “no matching documents”, and it is not retryable, "+
			"so no caller ever asks a replica that can serve it", empties)
	}
	t.Logf("caught up after %d retryable refusals and %d empty answers", refusals, empties)
}

// queryReplicaDirectly asks one storage node for one version, the way a client
// would if it could reach the layer directly. The trust mark is the same
// harness detail await_direct_test.go uses: a node configured with
// require_authenticated refuses calls that did not arrive through a station.
func queryReplicaDirectly(ctx context.Context, trusted context.Context, addr, kbID string, versionID int64) (*pb.QueryResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return pb.NewQueryServiceClient(conn).Query(trusted, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &versionID,
		Vector:          queryVector(768),
		TopK:            10,
	})
}
