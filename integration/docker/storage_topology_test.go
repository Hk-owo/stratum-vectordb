//go:build docker
// +build docker

package docker_test

// storage_topology_test.go — the cluster-level acceptance for per-KB replica
// placement (docs/实施中/storage-topology-plan.md §10, P6).
//
// Everything below runs against a REAL five-storage-node cluster:
//
//	scripts/cluster.sh --topology two-tier --storage-nodes 5 up
//	STRATUM_T4_STORAGE_NODE_ADDRS=localhost:17100,localhost:17101,localhost:17102,localhost:17103,localhost:17104 \
//	STRATUM_T4_STORAGE_SERVICES=stratum-node-storage1,stratum-node-storage2,stratum-node-storage3,stratum-node-storage4,stratum-node-storage5 \
//	go test ./integration/docker/ -tags=docker -run TestStorageTopology -v -timeout 40m
//
// Why it has to be a cluster and not a unit test: the plan's §10 asks for
// overlapping sets A=[11,12,13], B=[12,14,15] where writes, catch-up and queries
// stay inside the named range. In-process fixtures cannot show that, because the
// claim is about what bytes land on which machine — and about what a node does
// with metadata it can see but does not own.
//
// Node IDs are storage1..5 = 11..15 (scripts/cluster.sh: STORAGE_ID_BASE=10), which
// is also the order of the address list above.
//
// ORDER MATTERS: TestStorageTopology_DrainRetireAndAddressRules retires storage5,
// and a retirement is irreversible by design — so it must be the LAST case in this
// file, and running the suite again requires a clean cluster
// (scripts/cluster.sh --topology two-tier --storage-nodes 5 clean).

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
)

// storageNodeID is the stable node ID of the i-th storage node (0-based), i.e.
// the ID the directory, the placement sets and the admin API all use.
func storageNodeID(i int) int64 { return int64(11 + i) }

// requireFiveStorageNodes skips (with the command that fixes it) rather than
// failing: a three-node cluster is a legitimate deployment, just not the one
// this file's claims are about.
func requireFiveStorageNodes(t *testing.T) {
	t.Helper()
	if len(storageReplicaAddrs) < 5 || len(storageServices) < 5 {
		t.Skipf("needs five storage nodes (got %d addresses / %d services): "+
			"scripts/cluster.sh --topology two-tier --storage-nodes 5 up, "+
			"STRATUM_T4_STORAGE_NODE_ADDRS=localhost:17100,…,localhost:17104, "+
			"STRATUM_T4_STORAGE_SERVICES=stratum-node-storage1,…,stratum-node-storage5",
			len(storageReplicaAddrs), len(storageServices))
	}
}

// dialStorageAdmin opens the cluster-wide admin service THROUGH the station,
// which is the only supported path (§10 of docs/参考/client-integration-guide.md).
func dialStorageAdmin(addr string) (pb.StorageAdminServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewStorageAdminServiceClient(conn), conn, nil
}

// dialDataSync opens a storage node's replication surface. The tests read two
// facts from it — a node's contiguous cursor for a KB and whether a version is
// present — because those are the only statements that separate "owns no data"
// from "simply not asked".
func dialDataSync(addr string) (pb.DataSyncServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewDataSyncServiceClient(conn), conn, nil
}

// storageDirectory reads the authoritative directory through the station.
func storageDirectory(t *testing.T, ctx context.Context) *pb.GetStorageDirectoryResponse {
	t.Helper()
	admin, conn, err := dialStorageAdmin(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial the station: %v", err)
	}
	defer conn.Close()
	dir, err := admin.GetDirectory(ctx, &pb.GetStorageDirectoryRequest{})
	if err != nil {
		t.Fatalf("GetDirectory: %v", err)
	}
	return dir
}

// placementOf returns the KB's placement entry, or nil when the directory has
// none (which for a live KB is a real answer, not a failure).
func placementOf(dir *pb.GetStorageDirectoryResponse, kbID string) *pb.KBPlacementEntry {
	for _, p := range dir.GetPlacements() {
		if p.GetKnowledgeBaseId() == kbID {
			return p
		}
	}
	return nil
}

// sameIDs compares two ID sets, ignoring order: a placement is a set, and the
// directory is allowed to store whichever order the command arrived in.
func sameIDs(got []int64, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	g, w := append([]int64(nil), got...), append([]int64(nil), want...)
	sort.Slice(g, func(i, j int) bool { return g[i] < g[j] })
	sort.Slice(w, func(i, j int) bool { return w[i] < w[j] })
	for i := range g {
		if g[i] != w[i] {
			return false
		}
	}
	return true
}

// waitForPlacement polls the directory until the KB's effective set and epoch
// match, and fails with the last observed placement — "which set was actually
// committed" is the first question any failure here raises.
func waitForPlacement(t *testing.T, ctx context.Context, kbID string, want []int64, wantEpoch uint64, timeout time.Duration) *pb.KBPlacementEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *pb.KBPlacementEntry
	var lastPhase string
	for {
		dir := storageDirectory(t, ctx)
		last = placementOf(dir, kbID)
		if last != nil {
			lastPhase = ""
			for _, m := range dir.GetMigrations() {
				if m.GetKnowledgeBaseId() == kbID {
					lastPhase = m.GetPhase()
				}
			}
			if sameIDs(last.GetReplicaIds(), want) && last.GetPlacementEpoch() == wantEpoch {
				return last
			}
		}
		if time.Now().After(deadline) {
			if last == nil {
				t.Fatalf("KB %s never got a placement (last migration phase %q)", kbID, lastPhase)
			}
			t.Fatalf("KB %s placement stayed %v epoch %d (last migration phase %q), want %v epoch %d",
				kbID, last.GetReplicaIds(), last.GetPlacementEpoch(), lastPhase, want, wantEpoch)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// localVersion asks one storage node for its contiguous cursor for a KB.
func localVersion(ctx context.Context, addr, kbID string) (int64, error) {
	ds, conn, err := dialDataSync(addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	resp, err := ds.LocalVersion(ctx, &pb.LocalVersionRequest{KnowledgeBaseId: kbID})
	if err != nil {
		return 0, err
	}
	return resp.GetVersion(), nil
}

// versionPresent asks one storage node whether it holds (kbID, version).
func versionPresent(ctx context.Context, addr, kbID string, versionID int64) (bool, error) {
	ds, conn, err := dialDataSync(addr)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	resp, err := ds.VersionPresence(ctx, &pb.VersionPresenceRequest{KnowledgeBaseId: kbID, VersionId: versionID})
	if err != nil {
		return false, err
	}
	return resp.GetPresent(), nil
}

// waitLocalVersion polls until the node's cursor reaches want. A replica that
// was just handed a version needs the flush and the cursor advance to finish
// before it can serve; polling is the honest wait, asserting would test
// scheduling.
func waitLocalVersion(t *testing.T, ctx context.Context, addr, kbID string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	var lastErr error
	for time.Now().Before(deadline) {
		if v, err := localVersion(ctx, addr, kbID); err == nil {
			last = v
			if v >= want {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s cursor for %s = %d (last error %v), want >= %d", addr, kbID, last, lastErr, want)
}

// queryDirect asks one storage node itself, bypassing the station's routing.
// That is the only way to see a refusal: through the station a non-member is
// simply never a candidate.
func queryDirect(ctx context.Context, addr, kbID string, versionID int64) (*pb.QueryResponse, error) {
	_, q, _, conn, err := dialNode(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	version := versionID
	return q.Query(asStation(ctx), &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &version,
		Vector:          queryVector(768),
		TopK:            5,
	})
}

// awaitDirectServable polls one storage node until it serves the version with a
// result. Returns nil when it never does.
func awaitDirectServable(t *testing.T, ctx context.Context, addr, kbID string, versionID int64, timeout time.Duration) *pb.QueryResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := queryDirect(ctx, addr, kbID, versionID)
		if err == nil && len(resp.GetResults()) > 0 {
			return resp
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("%s never served %s v%d directly (last error: %v)", addr, kbID, versionID, lastErr)
	return nil
}

// createKBWithReplicas creates a KB whose effective replica set is explicit.
// The IDs are storage node IDs (11..15), not container ordinals.
func createKBWithReplicas(t *testing.T, ctx context.Context, label string, ids []int64) string {
	t.Helper()
	req := newKBRequest(label)
	req.StorageReplicaIds = ids
	deadline := time.Now().Add(30 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	var lastErr error
	for {
		for _, addr := range nodeAddrs {
			kb, _, _, conn, err := dialNode(addr)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err := kb.CreateKnowledgeBase(ctx, req)
			conn.Close()
			if err == nil {
				t.Logf("created KB %s (%s) with replicas %v", resp.GetKnowledgeBaseId(), label, ids)
				return resp.GetKnowledgeBaseId()
			}
			if !isTransientSetupError(err) {
				t.Fatalf("CreateKnowledgeBase(%s, replicas %v): %v", label, ids, err)
			}
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("CreateKnowledgeBase(%s) never succeeded: %v", label, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// writeAndAwaitServable writes one document, waits for the named replicas to hold
// it, then asks the station for the version once so the index build is triggered.
//
// The last step is not a convenience. §8.6(b) builds a version's index when
// something needs it, so waiting for INDEX_STATUS_READY before anyone has asked
// for the version waits forever — the status is a consequence of the first read,
// not of the write. (This cost a run: the write landed, the cursor advanced, and
// the test sat on PENDING for ten minutes.)
func writeAndAwaitServable(t *testing.T, ctx context.Context, kbID, docID, content string, members []int64) int64 {
	t.Helper()
	versionID := writeDocumentThroughControl(t, ctx, kbID, docID, content)
	for _, id := range members {
		waitLocalVersion(t, ctx, storageAddrOf(t, id), kbID, versionID, 2*time.Minute)
	}
	if resp := awaitServable(t, ctx, nodeAddrs[0], kbID, versionID, 3*time.Minute); resp == nil {
		t.Fatalf("station never served %s v%d after its data landed on %v", kbID, versionID, members)
	}
	return versionID
}

// isNotReplica reports whether a node refused the call because the KB's
// effective replica set does not include it.
//
// The check reads the gRPC status rather than a bare substring: the wire name
// (not_kb_replica) is added by the station's error mapper, and a DIRECT call to a
// node — which is the only way to see this refusal — carries the human message
// and code instead. Both are accepted so the assertion says what it means: the
// node refused, and it refused for placement, not for a missing index.
func isNotReplica(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		msg := strings.ToLower(st.Message())
		return st.Code() == codes.FailedPrecondition &&
			(strings.Contains(msg, "not_kb_replica") || strings.Contains(msg, "not a replica"))
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not_kb_replica") || strings.Contains(text, "not a replica")
}

// storageAddrOf maps a node ID back to the address/container the fault
// injection helpers use.
func storageAddrOf(t *testing.T, id int64) string {
	t.Helper()
	idx := int(id - 11)
	if idx < 0 || idx >= len(storageReplicaAddrs) {
		t.Fatalf("node %d has no address in %v", id, storageReplicaAddrs)
	}
	return storageReplicaAddrs[idx]
}

func storageServiceOf(t *testing.T, id int64) string {
	t.Helper()
	idx := int(id - 11)
	if idx < 0 || idx >= len(storageServices) {
		t.Fatalf("node %d has no container in %v", id, storageServices)
	}
	return storageServices[idx]
}

// TestStorageTopology_DirectorySeedsEveryStorageNode is the P1 precondition seen
// from outside: the control group's directory knows all five storage nodes,
// they are active, and their addresses match the ones the deployment published.
func TestStorageTopology_DirectorySeedsEveryStorageNode(t *testing.T) {
	requireFiveStorageNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dir := storageDirectory(t, ctx)
	if dir.GetRevision() == 0 {
		t.Fatalf("directory revision is 0: nothing was registered")
	}
	active := 0
	for i := 0; i < 5; i++ {
		id := storageNodeID(i)
		var got *pb.StorageNodeEntry
		for _, n := range dir.GetNodes() {
			if n.GetNodeId() == id {
				got = n
			}
		}
		if got == nil {
			t.Errorf("storage node %d is missing from the directory", id)
			continue
		}
		if state := got.GetManagementState(); state != "active" {
			t.Errorf("storage node %d state = %q, want active", id, state)
		}
		if got.GetAddress() == "" {
			t.Errorf("storage node %d has no address", id)
		}
		active++
	}
	if active < 5 {
		t.Fatalf("%d of 5 storage nodes are registered; the deployment did not seed the directory", active)
	}
}

// TestStorageTopology_OverlappingPlacementsStayInScope is §10's first item: with
// A=[11,12,13] and B=[12,14,15], writes, catch-up and queries happen only inside
// the named set, and the shared node (12) serves both.
//
// Four separate statements are checked per node, because each one alone can be
// true for the wrong reason:
//
//   - a member's cursor reaches the version (the data arrived);
//   - a non-member's cursor stays 0 and VersionPresence says false (it did not);
//   - a direct query to a member returns results;
//   - a direct query to a non-member is refused as not_kb_replica (the node
//     actively fences what it does not own, rather than answering "nothing
//     matches" — the failure mode that reads like a data loss).
func TestStorageTopology_OverlappingPlacementsStayInScope(t *testing.T) {
	requireFiveStorageNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	setA := []int64{11, 12, 13}
	setB := []int64{12, 14, 15}
	kbA := createKBWithReplicas(t, ctx, "topo-overlap-a", setA)
	kbB := createKBWithReplicas(t, ctx, "topo-overlap-b", setB)

	dir := storageDirectory(t, ctx)
	for _, c := range []struct {
		kb   string
		want []int64
		what string
	}{{kbA, setA, "A"}, {kbB, setB, "B"}} {
		p := placementOf(dir, c.kb)
		if p == nil {
			t.Fatalf("KB %s (%s) has no placement", c.kb, c.what)
		}
		if !sameIDs(p.GetReplicaIds(), c.want) {
			t.Fatalf("KB %s (%s) replicas = %v, want %v", c.kb, c.what, p.GetReplicaIds(), c.want)
		}
		if p.GetPlacementEpoch() != 1 {
			t.Fatalf("KB %s (%s) epoch = %d, want 1 for a fresh KB", c.kb, c.what, p.GetPlacementEpoch())
		}
		if p.GetRequiredReplicas() != 3 {
			t.Fatalf("KB %s (%s) required_replicas = %d, want 3", c.kb, c.what, p.GetRequiredReplicas())
		}
	}

	vA := writeAndAwaitServable(t, ctx, kbA, "doc-a-1", "A 的副本集合是 11、12、13。", setA)
	vB := writeAndAwaitServable(t, ctx, kbB, "doc-b-1", "B 的副本集合是 12、14、15。", setB)
	t.Logf("KB A %s v%d, KB B %s v%d", kbA, vA, kbB, vB)

	// Members: the data landed and the version is servable.
	for _, id := range setA {
		addr := storageAddrOf(t, id)
		waitLocalVersion(t, ctx, addr, kbA, vA, 2*time.Minute)
		if resp := awaitDirectServable(t, ctx, addr, kbA, vA, 90*time.Second); resp == nil {
			t.Errorf("member %d never served KB A v%d directly", id, vA)
		}
	}
	for _, id := range setB {
		addr := storageAddrOf(t, id)
		waitLocalVersion(t, ctx, addr, kbB, vB, 2*time.Minute)
	}
	// The shared node holds BOTH, which is the point of allowing overlapping
	// sets instead of fixed replica groups.
	shared := storageAddrOf(t, 12)
	if resp := awaitDirectServable(t, ctx, shared, kbB, vB, 90*time.Second); resp == nil {
		t.Errorf("shared node 12 never served KB B v%d", vB)
	}

	// Non-members: no data, and an explicit refusal rather than an empty answer.
	for _, c := range []struct {
		kb    string
		ver   int64
		outsi []int64
		what  string
	}{
		{kbA, vA, []int64{14, 15}, "A"},
		{kbB, vB, []int64{11, 13}, "B"},
	} {
		for _, id := range c.outsi {
			addr := storageAddrOf(t, id)
			if v, err := localVersion(ctx, addr, c.kb); err == nil && v != 0 {
				t.Errorf("node %d is outside %s but its cursor for %s is %d, want 0", id, c.what, c.kb, v)
			}
			if present, err := versionPresent(ctx, addr, c.kb, c.ver); err == nil && present {
				t.Errorf("node %d is outside %s but holds %s v%d", id, c.what, c.kb, c.ver)
			}
			resp, err := queryDirect(ctx, addr, c.kb, c.ver)
			if err == nil {
				t.Errorf("node %d is outside %s but answered a query with %d results, want not_kb_replica",
					id, c.what, len(resp.GetResults()))
				continue
			}
			if !isNotReplica(err) {
				t.Errorf("node %d outside %s refused with %v, want a not-kb-replica refusal", id, c.what, err)
			}
		}
	}

	// Through the station (the only client entry point) both KBs are readable,
	// as the active version and as a pinned version.
	station := nodeAddrs[0]
	if resp := awaitServable(t, ctx, station, kbA, vA, 2*time.Minute); resp == nil {
		t.Fatalf("station never served KB A v%d", vA)
	}
	if resp := awaitServable(t, ctx, station, kbB, vB, 2*time.Minute); resp == nil {
		t.Fatalf("station never served KB B v%d", vB)
	}
}

// TestStorageTopology_OfflineReplicaStaysInQuorumAndNonMemberDoesNotPull is §10's
// second and fourth items in one run, because they share the same fixture and
// contradict each other in a way that is worth seeing together:
//
//   - a silent replica stays in the denominator, so one down out of three still
//     writes (quorum 2) while two down refuses (below quorum) — the refusal must
//     be a failure, never a silently smaller replica count;
//   - a node that comes back must catch up, and a node that never owned the KB
//     must stay empty. "Restarted" and "not a member" are the two states that
//     used to be conflated into "pull everything".
func TestStorageTopology_OfflineReplicaStaysInQuorumAndNonMemberDoesNotPull(t *testing.T) {
	requireFiveStorageNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	set := []int64{11, 12, 13}
	kbID := createKBWithReplicas(t, ctx, "topo-quorum", set)
	v1 := writeAndAwaitServable(t, ctx, kbID, "doc-q-1", "第一次写入：三副本齐全。", set)
	for _, id := range set {
		waitLocalVersion(t, ctx, storageAddrOf(t, id), kbID, v1, 2*time.Minute)
	}

	// --- one down: quorum is still met, the write must succeed ---
	downOne := storageServiceOf(t, 11)
	killNode(t, downOne)
	defer func() { startNode(t, downOne) }()

	v2 := writeDocumentThroughControl(t, ctx, kbID, "doc-q-2", "第二次写入：11 已离线，12 与 13 仍在。")
	t.Logf("wrote v%d with node 11 down (quorum 2 of 3)", v2)
	if resp := awaitServable(t, ctx, nodeAddrs[0], kbID, v2, 2*time.Minute); resp == nil {
		t.Fatalf("station never served v%d while one replica was down", v2)
	}

	// --- two down: below quorum, the write must be refused ---
	downTwo := storageServiceOf(t, 13)
	killNode(t, downTwo)
	defer func() { startNode(t, downTwo) }()

	// The verdict is soft state driven by heartbeats: a replica counts as live
	// until it has been silent for the configured window (three intervals, 15 s by
	// default). Waiting it out before judging is what makes the refusal
	// deterministic — a write inside the window is allowed BY DESIGN, and treating
	// that as "the replica count was lowered" would be testing the clock.
	t.Logf("waiting out the redundancy silence window before judging")
	time.Sleep(20 * time.Second)

	var refused error
	accepted := 0
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) && refused == nil {
		req := &pb.CreateVersionRequest{
			KnowledgeBaseId: kbID,
			ClientRequestId: fmt.Sprintf("topo-quorum-%d", time.Now().UnixNano()),
			Changes: []*pb.DocChange{{
				Op:      pb.ChangeOp_CHANGE_OP_ADD,
				DocId:   fmt.Sprintf("doc-q-below-quorum-%d", time.Now().UnixNano()),
				Content: "只剩一个副本时都必须被拒绝。",
			}},
		}
		for _, addr := range nodeAddrs {
			kb, _, _, conn, err := dialNode(addr)
			if err != nil {
				continue
			}
			_, err = kb.CreateVersion(ctx, req)
			conn.Close()
			if err == nil {
				accepted++
				break
			}
			if strings.Contains(err.Error(), "storage") {
				refused = err
				break
			}
			// Anything else ("no leader available", a transport hiccup) is not the
			// verdict this case is about: ask again next round.
		}
		if refused == nil {
			time.Sleep(5 * time.Second)
		}
	}
	if refused == nil {
		t.Fatalf("writes were accepted %d time(s) over 3m with 2 of 3 replicas down: "+
			"the replica requirement was silently reduced", accepted)
	}
	t.Logf("write refused below quorum (accepted %d time(s) while the verdict was still stale): %v", accepted, refused)

	// --- recovery: both come back and must catch up ---
	startNode(t, downOne)
	startNode(t, downTwo)
	for _, svc := range []string{downOne, downTwo} {
		idx := -1
		for i, s := range storageServices {
			if s == svc {
				idx = i
			}
		}
		addr := storageReplicaAddrs[idx]
		if err := waitForListen(addr, 90*time.Second); err != nil {
			t.Fatalf("%s did not come back: %v", svc, err)
		}
	}
	v4 := writeDocumentThroughControl(t, ctx, kbID, "doc-q-4", "第四次写入：副本恢复后重新可以提交。")
	for _, id := range set {
		waitLocalVersion(t, ctx, storageAddrOf(t, id), kbID, v4, 4*time.Minute)
	}
	for _, id := range []int64{11, 13} {
		if resp := awaitDirectServable(t, ctx, storageAddrOf(t, id), kbID, v4, 3*time.Minute); resp == nil {
			t.Errorf("restarted member %d never served v%d after catching up", id, v4)
		}
	}

	// --- a non-member restarts and must not pull the KB it does not own ---
	outsider := 14
	svc := storageServiceOf(t, int64(outsider))
	killNode(t, svc)
	startNode(t, svc)
	if err := waitForListen(storageAddrOf(t, int64(outsider)), 90*time.Second); err != nil {
		t.Fatalf("node %d did not come back: %v", outsider, err)
	}
	// Give the startup reconcile and any catch-up a chance to run: the claim is
	// that nothing happens to a non-member even when it is fully online.
	time.Sleep(20 * time.Second)
	addr := storageAddrOf(t, int64(outsider))
	if v, err := localVersion(ctx, addr, kbID); err != nil {
		t.Logf("non-member %d: LocalVersion error (acceptable, it owes this KB nothing): %v", outsider, err)
	} else if v != 0 {
		t.Errorf("non-member %d pulled KB %s: cursor = %d, want 0", outsider, kbID, v)
	}
	if present, err := versionPresent(ctx, addr, kbID, v4); err == nil && present {
		t.Errorf("non-member %d holds %s v%d after restarting", outsider, kbID, v4)
	}
}

// TestStorageTopology_MigrateSingleMemberActivatesAndCleansOldReplica drives §8.1
// end to end on a real cluster: one member replacement, the old epoch's ACKs
// cannot carry into the new configuration, and the exited replica is reclaimed
// while the target keeps its data.
//
// "Activated" is the only phase that changes membership — staged and installed
// are explicitly not (storage.MigrationPhase). The test therefore waits for the
// DIRECTORY to change, not for an RPC to return.
func TestStorageTopology_MigrateSingleMemberActivatesAndCleansOldReplica(t *testing.T) {
	requireFiveStorageNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	from, to := int64(13), int64(14)
	set := []int64{11, 12, from}
	newSet := []int64{11, 12, to}
	kbID := createKBWithReplicas(t, ctx, "topo-migrate", set)
	v1 := writeAndAwaitServable(t, ctx, kbID, "doc-m-1", "迁移前的版本，三个副本都在。", set)
	for _, id := range set {
		waitLocalVersion(t, ctx, storageAddrOf(t, id), kbID, v1, 2*time.Minute)
	}

	admin, conn, err := dialStorageAdmin(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial the station: %v", err)
	}
	defer conn.Close()
	opID := fmt.Sprintf("topo-migrate-%d", time.Now().UnixNano())
	migrate := func(expectedEpoch uint64, fromID, toID int64, op string) error {
		_, err := admin.MigrateKB(ctx, &pb.MigrateStorageKBRequest{
			OperationId:     op,
			KnowledgeBaseId: kbID,
			ExpectedEpoch:   expectedEpoch,
			FromNodeId:      fromID,
			ToNodeId:        toID,
		})
		return err
	}
	if err := migrate(1, from, to, opID); err != nil {
		t.Fatalf("MigrateKB(%d -> %d): %v", from, to, err)
	}
	t.Logf("migration %s submitted: %d -> %d", opID, from, to)

	p := waitForPlacement(t, ctx, kbID, newSet, 2, 20*time.Minute)
	t.Logf("placement switched to %v at epoch %d", p.GetReplicaIds(), p.GetPlacementEpoch())
	if p.GetRequiredReplicas() != 3 {
		t.Fatalf("after migration required_replicas = %d, want 3 (the plan forbids lowering it)", p.GetRequiredReplicas())
	}

	dir := storageDirectory(t, ctx)
	for _, m := range dir.GetMigrations() {
		if m.GetOperationId() != opID {
			continue
		}
		if m.GetPhase() != "activated" {
			t.Fatalf("operation %s phase = %q, want activated", opID, m.GetPhase())
		}
	}

	// Writes and queries follow the new configuration.
	v2 := writeDocumentThroughControl(t, ctx, kbID, "doc-m-2", "迁移后的版本，新集合承担。")
	for _, id := range newSet {
		if resp := awaitDirectServable(t, ctx, storageAddrOf(t, id), kbID, v2, 3*time.Minute); resp == nil {
			t.Errorf("new member %d never served v%d", id, v2)
		}
	}
	if resp := awaitServable(t, ctx, nodeAddrs[0], kbID, v2, 2*time.Minute); resp == nil {
		t.Fatalf("station never served v%d after migration", v2)
	}

	// The exited replica no longer serves — not "serves stale data".
	if _, err := queryDirect(ctx, storageAddrOf(t, from), kbID, v2); err == nil {
		t.Errorf("exited replica %d still answers queries for %s", from, kbID)
	} else if !isNotReplica(err) {
		t.Errorf("exited replica %d refused with %v, want a not-kb-replica refusal", from, err)
	}

	// The cleanup obligation is durable and completes on its own.
	cleanupDone := func() bool {
		dir := storageDirectory(t, ctx)
		found := false
		for _, c := range dir.GetCleanups() {
			if c.GetKnowledgeBaseId() == kbID && c.GetNodeId() == from {
				found = true
				if !c.GetDone() {
					return false
				}
			}
		}
		return found
	}
	cleanupDeadline := time.Now().Add(10 * time.Minute)
	for !cleanupDone() {
		if time.Now().After(cleanupDeadline) {
			t.Fatalf("cleanup of the exited replica %d for %s never completed", from, kbID)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("exited replica %d reclaimed", from)

	// Retrying the same operation is idempotent and must not move the epoch.
	if err := migrate(1, from, to, opID); err != nil {
		t.Errorf("retrying an activated migration = %v, want nil (same operation ID is safe to repeat)", err)
	}
	if p := waitForPlacement(t, ctx, kbID, newSet, 2, 2*time.Minute); p.GetPlacementEpoch() != 2 {
		t.Errorf("retry moved the epoch to %d", p.GetPlacementEpoch())
	}

	// An old-epoch request cannot act on the new configuration.
	if err := migrate(1, 12, 15, opID+"-stale"); err == nil {
		t.Errorf("a migration planned against epoch 1 was accepted after activation at epoch 2")
	}
}

// drainHasRoom reports whether every KB that still holds drainNode can be
// re-placed OFF it. A KB whose replica set already contains every other active
// node has nowhere to move to, and DrainNode is then refused atomically with
// placement_change_unsafe — correct behaviour, but it means this case needs a
// cluster whose only knowledge bases are the ones it created.
//
// Why it is checked rather than assumed: the default placement strategy for a KB
// created without explicit replicas is "every registered node". A suite run that
// reaches this case after other cases have created such knowledge bases cannot
// drain anything, and the honest outcome there is a skip that says so, not a
// failure that reads like a drain bug.
func drainHasRoom(dir *pb.GetStorageDirectoryResponse, drainNode int64) (bool, string) {
	others := map[int64]bool{}
	for _, n := range dir.GetNodes() {
		if n.GetNodeId() != drainNode && n.GetManagementState() == "active" {
			others[n.GetNodeId()] = true
		}
	}
	for _, p := range dir.GetPlacements() {
		if !placementContains(p, drainNode) {
			continue
		}
		room := false
		for id := range others {
			if !placementContains(p, id) {
				room = true
				break
			}
		}
		if !room {
			return false, p.GetKnowledgeBaseId()
		}
	}
	return true, ""
}

// placementContains reports whether the placement still holds nodeID.
func placementContains(p *pb.KBPlacementEntry, nodeID int64) bool {
	for _, id := range p.GetReplicaIds() {
		if id == nodeID {
			return true
		}
	}
	return false
}

// waitForPlacementExcluding polls until the KB's effective set no longer holds
// nodeID, and returns that placement. The epoch is not pinned: a drain may bump it
// more than once when several replacements are planned for one node.
func waitForPlacementExcluding(t *testing.T, ctx context.Context, kbID string, nodeID int64, timeout time.Duration) *pb.KBPlacementEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *pb.KBPlacementEntry
	for {
		last = placementOf(storageDirectory(t, ctx), kbID)
		if last != nil && !placementContains(last, nodeID) {
			return last
		}
		if time.Now().After(deadline) {
			if last == nil {
				t.Fatalf("KB %s never got a placement while draining %d", kbID, nodeID)
			}
			t.Fatalf("KB %s still holds node %d after %v: %v (epoch %d)",
				kbID, nodeID, timeout, last.GetReplicaIds(), last.GetPlacementEpoch())
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// tombstoneOf returns the KB's storage tombstone, or nil when the directory has
// none.
func tombstoneOf(dir *pb.GetStorageDirectoryResponse, kbID string) *pb.KBStorageTombstone {
	for _, t := range dir.GetTombstones() {
		if t.GetKnowledgeBaseId() == kbID {
			return t
		}
	}
	return nil
}

// waitCleanupDone polls until the directory records the node's cleanup
// obligation for the KB as Done. found=false means no obligation exists at all,
// which is a different (and for a member, wrong) state.
func waitCleanupDone(t *testing.T, ctx context.Context, kbID string, nodeID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last bool
	for time.Now().Before(deadline) {
		found := false
		for _, c := range storageDirectory(t, ctx).GetCleanups() {
			if c.GetKnowledgeBaseId() == kbID && c.GetNodeId() == nodeID {
				found = true
				last = c.GetDone()
			}
		}
		if found && last {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("cleanup of %s on node %d never completed (found=%v done=%v)", kbID, nodeID, true, last)
}

// TestStorageTopology_DeletedKBIsNotResurrected is §10's sixth item: deleting a
// KB keeps its identity fenced, its cleanup obligations cover every replica it
// ever had, and a late write cannot bring it back.
func TestStorageTopology_DeletedKBIsNotResurrected(t *testing.T) {
	requireFiveStorageNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	set := []int64{12, 14, 15}
	kbID := createKBWithReplicas(t, ctx, "topo-delete", set)
	v1 := writeAndAwaitServable(t, ctx, kbID, "doc-d-1", "删除前写入的版本。", set)
	t.Logf("KB %s v%d written on %v", kbID, v1, set)

	var deleteErr error
	for _, addr := range nodeAddrs {
		kb, _, _, conn, err := dialNode(addr)
		if err != nil {
			continue
		}
		_, deleteErr = kb.DeleteKnowledgeBase(asStation(ctx), &pb.DeleteKnowledgeBaseRequest{KnowledgeBaseId: kbID})
		conn.Close()
		if deleteErr == nil {
			break
		}
	}
	if deleteErr != nil {
		t.Fatalf("DeleteKnowledgeBase: %v", deleteErr)
	}

	// The tombstone lists every replica that may still hold bytes. Node 11 and 13
	// never held this KB and must not be given a cleanup for it.
	deadline := time.Now().Add(3 * time.Minute)
	var tomb *pb.KBStorageTombstone
	for time.Now().Before(deadline) {
		if tomb = tombstoneOf(storageDirectory(t, ctx), kbID); tomb != nil {
			break
		}
		time.Sleep(time.Second)
	}
	if tomb == nil {
		t.Fatalf("KB %s was deleted but the directory kept no tombstone", kbID)
	}
	if !sameIDs(tomb.GetCleanupNodeIds(), set) {
		t.Errorf("tombstone cleanup nodes = %v, want the replica set %v", tomb.GetCleanupNodeIds(), set)
	}
	t.Logf("tombstone %s cleanup targets %v", kbID, tomb.GetCleanupNodeIds())

	// Every former replica reclaims its bytes.
	for _, id := range set {
		waitCleanupDone(t, ctx, kbID, id, 8*time.Minute)
	}
	t.Logf("all former replicas reclaimed")

	// A late write to the deleted identity is refused, not applied.
	req := &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: fmt.Sprintf("topo-delete-late-%d", time.Now().UnixNano()),
		Changes: []*pb.DocChange{{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   "doc-d-late",
			Content: "迟到的写入不能复活已删除的 KB。",
		}},
	}
	var lateErr error
	for _, addr := range nodeAddrs {
		kb, _, _, conn, err := dialNode(addr)
		if err != nil {
			continue
		}
		_, lateErr = kb.CreateVersion(asStation(ctx), req)
		conn.Close()
		if lateErr != nil {
			break
		}
	}
	if lateErr == nil {
		t.Fatalf("a write to the deleted KB %s was accepted", kbID)
	}
	t.Logf("late write refused: %v", lateErr)

	// A same-named rebuild is a NEW identity, and the old one stays dead.
	rebuilt := createKBWithReplicas(t, ctx, "topo-delete", []int64{11, 12})
	if rebuilt == kbID {
		t.Fatalf("the rebuild reused the deleted internal identity %s", kbID)
	}
	writeAndAwaitServable(t, ctx, rebuilt, "doc-d-2", "同名重建使用新的内部身份。", []int64{11, 12})
	if len(tombstoneOf(storageDirectory(t, ctx), rebuilt).GetCleanupNodeIds()) > 0 {
		if t2 := tombstoneOf(storageDirectory(t, ctx), rebuilt); t2 != nil {
			t.Errorf("the rebuilt KB %s inherited the old tombstone's cleanup targets %v", rebuilt, t2.GetCleanupNodeIds())
		}
	}
}

// TestStorageTopology_DrainRetireAndAddressRules is §23's node lifecycle on a
// real cluster: an address cannot change while the node is still referenced, a
// drain plans every KB replacement atomically, new placements skip the draining
// node, and a retired identity can never be registered again.
func TestStorageTopology_DrainRetireAndAddressRules(t *testing.T) {
	requireFiveStorageNodes(t)
	// Opt-in, because the case retires a storage node and a retirement is
	// irreversible by design — which makes it the one case here that can poison a
	// SHARED suite run: everything after it (the stress cases, the two-tier cases)
	// would be faulting a four-node storage group while assuming three or five.
	// Run it deliberately, last, against a clean cluster:
	//
	//	STRATUM_TOPO_LIFECYCLE=1 go test ./integration/docker/ -tags=docker \
	//	  -run TestStorageTopology -v -timeout 60m
	if os.Getenv("STRATUM_TOPO_LIFECYCLE") != "1" {
		t.Skip("retires a storage node irreversibly; set STRATUM_TOPO_LIFECYCLE=1 to run it (last, on a clean cluster)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	drainNode := int64(15)
	set := []int64{12, drainNode}
	kbID := createKBWithReplicas(t, ctx, "topo-drain", set)
	writeAndAwaitServable(t, ctx, kbID, "doc-dr-1", "排空前的版本，落在 12 与 15 上。", set)

	admin, conn, err := dialStorageAdmin(nodeAddrs[0])
	if err != nil {
		t.Fatalf("dial the station: %v", err)
	}
	defer conn.Close()

	// An address update is refused while the node still holds placements: the
	// documented order is drain → revoke → address. The address must DIFFER from
	// the registered one, or the call is a no-op that returns nil for an unrelated
	// reason and would read as "the rule was not enforced".
	dir := storageDirectory(t, ctx)
	if _, err := admin.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:           drainNode,
		ExpectedRevision: dir.GetRevision(),
		Action:           "address",
		Address:          "stratum-node-storage5-moved:7000",
	}); err == nil {
		t.Fatalf("an address update was accepted for node %d while it still holds placements", drainNode)
	} else {
		t.Logf("address update refused while referenced: %v", err)
	}

	// Drain: one Raft command plans every KB this node holds, so the target pool has
	// to be able to replace it in EVERY one of them. Naming a single target was a
	// clean-cluster assumption: the planning is atomic, so if another KB still holds
	// this node and already contains the single target, the whole drain is refused
	// with placement_change_unsafe (measured on a cluster that still had earlier
	// cases' knowledge bases). A pool of the other nodes makes the request admissible
	// either way.
	targets := []int64{11, 12, 13, 14}
	drainOp := fmt.Sprintf("topo-drain-%d", time.Now().UnixNano())
	dir = storageDirectory(t, ctx)
	if room, blocker := drainHasRoom(dir, drainNode); !room {
		t.Skipf("cluster still holds KB %s with no replacement room for node %d "+
			"(its replica set already contains every other active node, so a drain is "+
			"correctly refused); run this case alone on a clean cluster: "+
			"scripts/cluster.sh --topology two-tier --storage-nodes 5 clean && … up",
			blocker, drainNode)
	}
	if _, err := admin.DrainNode(ctx, &pb.DrainStorageNodeRequest{
		OperationId:      drainOp,
		NodeId:           drainNode,
		ExpectedRevision: dir.GetRevision(),
		TargetNodeIds:    targets,
	}); err != nil {
		t.Fatalf("DrainNode(%d, targets %v): %v", drainNode, targets, err)
	}
	t.Logf("drain %s submitted for node %d with target pool %v", drainOp, drainNode, targets)

	// What must hold afterwards is "the drained node is gone from the set", not one
	// particular replacement: which node the planner picked is its business.
	moved := waitForPlacementExcluding(t, ctx, kbID, drainNode, 20*time.Minute)
	if !placementContains(moved, 12) {
		t.Fatalf("KB %s lost its kept member 12 while draining %d: %v", kbID, drainNode, moved.GetReplicaIds())
	}
	t.Logf("KB %s moved to %v (drained node %d gone)", kbID, moved.GetReplicaIds(), drainNode)

	// The node is draining, and no NEW allocation may pick it.
	dir = storageDirectory(t, ctx)
	for _, n := range dir.GetNodes() {
		if n.GetNodeId() == drainNode {
			if n.GetManagementState() != "draining" {
				t.Errorf("node %d state = %q after DrainNode, want draining", drainNode, n.GetManagementState())
			}
		}
	}
	fresh := createKBWithReplicas(t, ctx, "topo-drain-new", nil)
	p := placementOf(storageDirectory(t, ctx), fresh)
	if p == nil {
		t.Fatalf("the new KB %s has no placement", fresh)
	}
	for _, id := range p.GetReplicaIds() {
		if id == drainNode {
			t.Errorf("a new KB was allocated onto draining node %d: %v", drainNode, p.GetReplicaIds())
		}
	}
	t.Logf("new KB allocated on %v (draining node %d excluded)", p.GetReplicaIds(), drainNode)

	// Retire needs an explicit revocation proof first.
	dir = storageDirectory(t, ctx)
	blockOp := fmt.Sprintf("topo-retire-%d", time.Now().UnixNano())
	if _, err := admin.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:           drainNode,
		ExpectedRevision: dir.GetRevision(),
		Action:           "block",
		OperationId:      blockOp,
	}); err != nil {
		t.Fatalf("block node %d: %v", drainNode, err)
	}
	dir = storageDirectory(t, ctx)
	block := dir.GetServiceBlocks()[drainNode]
	if block == nil {
		t.Fatalf("node %d has no service block after block", drainNode)
	}
	t.Logf("node %d blocked at revision %d", drainNode, block.GetRevision())

	confirmCtx, confirmCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	if _, err := admin.ManageNode(confirmCtx, &pb.ManageStorageNodeRequest{
		NodeId:        drainNode,
		Action:        "confirm_revoked",
		OperationId:   blockOp,
		BlockRevision: block.GetRevision(),
	}); err != nil {
		confirmCancel()
		t.Fatalf("confirm_revoked for node %d: %v", drainNode, err)
	}
	confirmCancel()

	dir = storageDirectory(t, ctx)
	if _, err := admin.ManageNode(ctx, &pb.ManageStorageNodeRequest{
		NodeId:           drainNode,
		ExpectedRevision: dir.GetRevision(),
		Action:           "retire",
	}); err != nil {
		t.Fatalf("retire node %d: %v", drainNode, err)
	}
	dir = storageDirectory(t, ctx)
	for _, n := range dir.GetNodes() {
		if n.GetNodeId() == drainNode {
			if n.GetManagementState() != "retired" {
				t.Errorf("node %d state = %q after retire, want retired", drainNode, n.GetManagementState())
			}
		}
	}

	// The identity stays reserved: re-registering it must be refused.
	if _, err := admin.RegisterNodes(ctx, &pb.RegisterStorageNodesRequest{
		ExpectedRevision: dir.GetRevision(),
		Nodes: []*pb.StorageNodeEntry{{
			NodeId:          drainNode,
			Address:         "stratum-node-storage5:7000",
			ManagementState: "active",
		}},
	}); err == nil {
		t.Errorf("a retired node ID (%d) was accepted by RegisterNodes", drainNode)
	}
}
