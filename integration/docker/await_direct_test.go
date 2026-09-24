//go:build docker
// +build docker

package docker_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/service"
)

// controlAddrs are the control nodes THEMSELVES — not the station the rest of
// this suite talks to.
var controlAddrs = splitEnv("STRATUM_T4_CONTROL_ADDRS", "localhost:17000,localhost:17001,localhost:17002")

// TestT4_AwaitVersion_EveryControlNodeAnswersOnItsOwn pins the property the whole
// design rests on (docs/await-version-plan.md §0 decision 2): the wait's anchor is
// the version's STATUS, which lives in replicated state, so every control node can
// answer for itself. That is what makes "reconnect somewhere else and keep
// waiting" work.
//
// The station hides the property — it picks a backend per call, so a passing
// station-based test says only that *some* node answered. This case reaches past
// it and asks each node directly.
//
// Dialing a control node needs the station's trust mark: a node configured with
// require_authenticated refuses client-facing calls that did not arrive through
// one. That is a harness detail, and deliberately not something a real client
// does or can do.
func TestT4_AwaitVersion_EveryControlNodeAnswersOnItsOwn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kbID := createKB(t, ctx, "await-direct")
	versionID := writeDocumentThroughControl(t, ctx, kbID, "doc-1", "每个控制节点都要能自己回答 await。")
	if settled := awaitUntilSettled(t, ctx, nodeAddrs[0], kbID, versionID); settled.GetStage() != service.StageIndexReady {
		t.Fatalf("fixture: version %d = %s, want READY", versionID, settled.GetStage())
	}

	// 直连控制节点（不经服务站）的调用必须自己带服务站的信任标记。它是 HMAC 签名的
	// （H4 of docs/code-review-2026-09-24.md），所以不能像以前那样手写一个 "1"：
	// 那是旧格式，现在节点会直接拒绝。
	trusted := asStation(context.Background())
	for _, addr := range controlAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial control node %s: %v", addr, err)
		}
		resp, err := pb.NewKnowledgeBaseServiceClient(conn).AwaitVersion(trusted, &pb.AwaitVersionRequest{
			KnowledgeBaseId: kbID,
			VersionId:       versionID,
			Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
			WaitTimeoutMs:   3000,
		})
		_ = conn.Close()
		if err != nil {
			t.Fatalf("%s: AwaitVersion = %v", addr, err)
		}
		if resp.GetStage() != service.StageIndexReady {
			t.Errorf("%s: stage = %s, want %s — every control node holds the same replicated state",
				addr, resp.GetStage(), service.StageIndexReady)
		}
		if resp.GetVersion().GetVersionId() != versionID {
			t.Errorf("%s: version.version_id = %d, want %d", addr, resp.GetVersion().GetVersionId(), versionID)
		}
	}
}
