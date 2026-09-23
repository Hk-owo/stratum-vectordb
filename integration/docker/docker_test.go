// Package docker_test contains Stratum's T4 multi-node integration tests
// (Stratum_测试顺序.md 第四批). They are protected by the "docker" build tag.
//
// Two topologies are supported, and the tests below were written to work under
// both — which is why the node addresses and container names come from the
// environment (see splitEnv):
//
//   - all-in-one: every node runs both halves (scripts/cluster.sh --topology single).
//     nodeAddrs are 3 such nodes and every one of them serves reads.
//   - split (Stratum_设计文档v13.md §11 阶段 ④): a control group that holds no
//     storage and a storage group that holds no metadata
//     (scripts/cluster.sh --topology two-tier). nodeAddrs are the control nodes;
//     storageAddrs (see two_tier_test.go) are where reads are served.
//
// Where a test reads, it reads from the storage tier: QueryService takes the
// index manager and the local stores in its constructor, so it is a
// storage-side service in fact, and a control node cannot build it at all.
//
// Run (split topology):
//
//	scripts/cluster.sh --topology two-tier up
//	STRATUM_T4_NODE_SERVICES=stratum-node-control1,stratum-node-control2,stratum-node-control3 \
//	go test ./integration/docker/... -tags=docker -v -timeout 600s
//	scripts/cluster.sh --topology two-tier down
//
//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
	"stratum/internal/authmeta"
)

// nodeAddrs are the gRPC addresses of the control-tier nodes under test.
//
// They default to the all-in-one layout (scripts/cluster.sh --topology single, which CI
// runs) and can be pointed at the two-tier one
// (scripts/cluster.sh --topology two-tier) without editing this file:
//
//	STRATUM_T4_NODE_ADDRS=localhost:17000,localhost:17001,localhost:17002
//	STRATUM_T4_NODE_SERVICES=stratum-node-control1,stratum-node-control2,stratum-node-control3
var nodeAddrs = splitEnv("STRATUM_T4_NODE_ADDRS", "localhost:17000,localhost:17001,localhost:17002")

// splitEnv reads a comma-separated list from the environment, or the default
// when unset.
//
// Two lists are read through it — an address and the container name that serves
// it — and they must stay in step: a mismatch shows up only in a
// fault-injection test, where the failure (a kill that hits the wrong node, or
// none) is the hardest kind to debug from the output alone.
func splitEnv(key, def string) []string {
	raw := os.Getenv(key)
	if raw == "" {
		raw = def
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// nodeServices are the node container names, indexed the same as nodeAddrs.
// They are what the fault-injection helpers below kill and start.
var nodeServices = splitEnv("STRATUM_T4_NODE_SERVICES", "stratum-node1,stratum-node2,stratum-node3")

func dialNode(addr string) (pb.KnowledgeBaseServiceClient, pb.QueryServiceClient, pb.AdminServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return pb.NewKnowledgeBaseServiceClient(conn),
		pb.NewQueryServiceClient(conn),
		pb.NewAdminServiceClient(conn),
		conn, nil
}

// controlLeaderIndex returns the position, in controlAddrs, of the node the
// control tier itself believes leads.
//
// It asks the control nodes directly rather than the station, for the reason
// given on controlAddrs: through a station every answer comes back bearing the
// station's address, so "the leader" is whichever station entry the round-robin
// happened to land on. Majority vote, like the station's own discovery, so one
// stale view cannot decide it.
//
// Direct calls carry the station's trust mark, the same harness detail
// await_direct_test.go documents: a node configured with require_authenticated
// refuses client-facing calls that did not arrive through a station, and this
// question is one of them. A real client cannot ask it — that is what the gate is
// for — but fault injection is not a client; it has to name the node it kills.
func controlLeaderIndex(t *testing.T, ctx context.Context) int {
	t.Helper()
	trusted := authmeta.WithVerifiedMark(ctx)
	votes := map[int64]int{}
	// lastErr travels with the failure: "nobody reported a leader" has two very
	// different causes (nodes answering has_leader=false versus nodes not
	// answering at all — a wrong address, or a gate that refused the call), and
	// without it the message sends the reader to the wrong one.
	var lastErr error
	for _, addr := range controlAddrs {
		_, _, admin, conn, err := dialNode(addr)
		if err != nil {
			lastErr = fmt.Errorf("%s: dial: %w", addr, err)
			continue
		}
		resp, err := admin.GetClusterStatus(trusted, &pb.GetClusterStatusRequest{})
		_ = conn.Close()
		if err != nil {
			lastErr = fmt.Errorf("%s: GetClusterStatus: %w", addr, err)
			continue
		}
		if !resp.GetHasLeader() {
			continue
		}
		votes[resp.GetLeaderId()]++
	}
	best, bestVotes := int64(0), 0
	for id, n := range votes {
		if n > bestVotes {
			best, bestVotes = id, n
		}
	}
	if bestVotes == 0 {
		t.Fatalf("no control node reported a leader (asked %v; last error: %v)", controlAddrs, lastErr)
	}
	// Control nodes are 1..N in the order they are configured, which is the order
	// of controlAddrs (Stratum_设计文档v13.md §11).
	idx := int(best) - 1
	if idx < 0 || idx >= len(controlAddrs) {
		t.Fatalf("leader id %d is outside the %d control nodes (%v)",
			best, len(controlAddrs), controlAddrs)
	}
	return idx
}

// === docker fault-injection helpers ===
//
// 集群由 scripts/cluster.sh --topology single 以原生 docker 容器方式启动（替代 docker-compose），
// 节点容器名即 stratum-node{1,2,3}（与上方 nodeServices 一致），故直接用 docker CLI 管理。

// dockerCmd runs a `docker` command. It fails the test on error.
func dockerCmd(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// killNode SIGKILLs a node, simulating a crash (not a graceful stop).
func killNode(t *testing.T, service string) {
	t.Helper()
	t.Logf("killing %s", service)
	dockerCmd(t, "kill", "-s", "SIGKILL", service)
}

// startNode restarts a previously killed node.
func startNode(t *testing.T, service string) {
	t.Helper()
	t.Logf("starting %s", service)
	dockerCmd(t, "start", service)
}

// === leader discovery / data-visibility helpers ===

// newKBRequest builds a CreateKnowledgeBase request with a globally-unique
// name so re-running tests (against a stateful cluster) never collides.
func newKBRequest(label string) *pb.CreateKnowledgeBaseRequest {
	return &pb.CreateKnowledgeBaseRequest{
		Name:             fmt.Sprintf("%s-%d", label, time.Now().UnixNano()),
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "http://mock-embed:8080",
			ModelId:     "test-model",
		},
	}
}

// stressQuantizer is the quantizer the measurement cases (stress_test.go,
// datavolume_test.go) build their knowledge base with.
//
// Default OFF: full precision, single-stage search. STRATUM_T4_QUANTIZER names
// one of the §2.4 variants so the same case can be run against a quantized
// knowledge base and the two runs compared — that comparison is the point
// (quantization trades memory for a rerank against disk), and it has to use one
// harness, one cluster and one query generator to mean anything.
func stressQuantizer() pb.QuantizerType {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("STRATUM_T4_QUANTIZER"))) {
	case "sq8":
		return pb.QuantizerType_QUANTIZER_SQ8
	case "sq_fp16":
		return pb.QuantizerType_QUANTIZER_SQ_FP16
	case "sq_bf16":
		return pb.QuantizerType_QUANTIZER_SQ_BF16
	case "pq":
		return pb.QuantizerType_QUANTIZER_PQ
	default:
		return pb.QuantizerType_QUANTIZER_OFF
	}
}

// measurementKB returns the knowledge base a measurement case should fill.
//
// With the default quantizer it is the leader probe's own KB, unchanged. With a
// quantizer set it is a fresh KB built for the purpose: the probe's KB is created
// before the quantizer is known and holds a single document — far too few to
// train SQ8/PQ's codebook — and the quantizer is immutable after creation, so
// getting it wrong there could not be repaired by a later call.
func measurementKB(t *testing.T, ctx context.Context, addr, label, probeKB string) string {
	q := stressQuantizer()
	if q == pb.QuantizerType_QUANTIZER_OFF {
		return probeKB
	}
	t.Helper()

	req := newKBRequest(label)
	req.Quantizer = q
	if q == pb.QuantizerType_QUANTIZER_PQ {
		req.PqM = 96 // d=768 → 96 sub-vectors of 8 dims
		req.PqNbits = 8
	}

	kb, _, _, conn, err := dialNode(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()

	resp, err := kb.CreateKnowledgeBase(ctx, req)
	if err != nil {
		t.Fatalf("CreateKnowledgeBase(quantizer=%v): %v", q, err)
	}
	t.Logf("measuring with quantizer %v, KB %s", q, resp.GetKnowledgeBaseId())
	return resp.GetKnowledgeBaseId()
}

// probeLeaderOnce finds the node that actually accepts writes, returning its
// index and the KB it created.
//
// Creating a knowledge base is NOT a leader probe, and treating it as one was a
// real defect in this harness: a follower forwards that proposal to the leader
// and answers success, so a KB proves the cluster HAS a leader — not that the
// node asked is it. A version write is what separates the two, because the
// coordinator refuses with ErrNotLeader unless this node leads and nothing
// forwards it (Stratum_设计文档v13.md §7.13.2). Asking a follower for a version
// is how every test that "mysteriously" came back with "not leader" failed.
//
// The KB from a node that turns out not to lead is left behind. It is harmless
// (test names are unique) and cheaper than a two-phase probe.
func probeLeaderOnce(ctx context.Context, label string) (int, string, bool, error) {
	req := newKBRequest(label)
	var lastErr error
	for i, addr := range nodeAddrs {
		kb, _, _, conn, err := dialNode(addr)
		if err != nil {
			lastErr = fmt.Errorf("dial %s: %w", addr, err)
			continue
		}
		resp, err := kb.CreateKnowledgeBase(ctx, req)
		if err != nil {
			conn.Close()
			lastErr = fmt.Errorf("CreateKnowledgeBase on %s: %w", addr, err)
			continue
		}
		// Confirm this node leads by making it commit something that cannot be
		// forwarded.
		//
		// The change must be NON-EMPTY: the coordinator rejects a CreateVersion
		// with no changes (InvalidArgument, "empty changes"), so a probe built on
		// one can never see verr == nil — every caller of waitForLeader times out,
		// and the whole -tags=docker suite fails at its first step instead of at
		// whatever it was written to check. The document is never read back; the
		// KB is fresh per probe, so committing it only serves to prove that this
		// node is the one that can commit.
		_, verr := kb.CreateVersion(ctx, &pb.CreateVersionRequest{
			KnowledgeBaseId: resp.GetKnowledgeBaseId(),
			ClientRequestId: fmt.Sprintf("probe-%d", time.Now().UnixNano()),
			Changes: []*pb.DocChange{{
				Op:      pb.ChangeOp_CHANGE_OP_ADD,
				DocId:   "probe-doc",
				Content: "leader probe",
			}},
		})
		conn.Close()
		if verr == nil {
			return i, resp.GetKnowledgeBaseId(), true, nil
		}
		// Why it failed matters, and folding the reasons together is how a suite
		// reports "no leader" for a fault that has nothing to do with leadership: a
		// storage refusal, for instance, means the write DID reach a leader and was
		// refused further down. waitForLeader reports this string, so the next
		// person to see a timeout knows which fault they are looking at.
		lastErr = fmt.Errorf("CreateVersion on %s: %w", addr, verr)
	}
	return 0, "", false, lastErr
}

// waitForLeader polls probeLeaderOnce until a leader accepts a write or the
// deadline passes. Used after killing a leader, when re-election takes a few
// seconds (election timeout is 2-4s).
func waitForLeader(t *testing.T, ctx context.Context, label string, timeout time.Duration) (leaderIdx int, kbID string) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	// The same node must answer twice in a row. A single success only proves it
	// led at that instant: right after another test has killed and restarted a
	// control node, leadership can still be settling, and a write sent to the
	// just-deposed leader comes back "not leader" — which is what a one-shot
	// probe produces. Two consecutive answers from one node is what makes the
	// write that follows unlikely to race an election.
	stable := -1
	var lastErr error
	for {
		if idx, kb, ok, err := probeLeaderOnce(ctx, label); ok {
			if idx == stable {
				return idx, kb
			}
			stable = idx
		} else {
			stable = -1
			if err != nil {
				lastErr = err
			}
		}
		if time.Now().After(deadline) {
			// The cause travels with the timeout. Without it a storage refusal and a
			// missing leader look identical from here, which is exactly how a suite
			// ends up blaming leadership for something else entirely.
			t.Fatalf("timed out waiting for a leader to accept writes; last probe error: %v", lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// nodeSeesKB reports whether a node has replicated the given KB's version
// metadata (i.e. it has caught up with the leader's Raft log).
func nodeSeesKB(ctx context.Context, addr, kbID string) bool {
	_, _, _, conn, err := dialNode(addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	kb := pb.NewKnowledgeBaseServiceClient(conn)
	versions, err := kb.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		return false
	}
	return len(versions.Versions) >= 1
}

// waitForNodeToSeeKB polls a node until it can ListVersions for kbID.
func waitForNodeToSeeKB(t *testing.T, ctx context.Context, addr, kbID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !nodeSeesKB(ctx, addr, kbID) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s to see KB %s", addr, kbID)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// === T4-1: Distributed correctness ===

func TestT4_MultiNode_Consistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "consistency", 30*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	t.Logf("leader is node %d (%s), KB %s", leaderIdx, leaderAddr, kbID)

	_, _, _, conn, err := dialNode(leaderAddr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer conn.Close()
	kb := pb.NewKnowledgeBaseServiceClient(conn)

	// Commit a version of this test's own and assert on THAT version.
	//
	// There is no such thing as an "empty initial version" to assert against: the
	// product refuses a CreateVersion with no changes (ErrEmptyChanges), because a
	// version's document set is its parent's plus those changes — so an empty list
	// means "unchanged", not "empty" (internal/coordinator/write_impl.go:254).
	// And a freshly created KB has no version at all: CreateKnowledgeBase answers
	// initial_version_id 0, meaning "no version exists yet"
	// (service/knowledgebase.go:170). So the version under test is one we write.
	const docID = "consistency-doc"
	createResp, err := kb.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ClientRequestId: fmt.Sprintf("consistency-%d", time.Now().UnixNano()),
		Changes: []*pb.DocChange{{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   docID,
			Content: "多节点一致性：版本元数据要在每个节点可见，且该版本可查询到写入的文档。",
		}},
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	versionID := createResp.GetVersionId()

	// Metadata replication: every control node must come to see the version. This
	// is polled, not asserted once — a read through the station is load-balanced
	// across the control nodes (only writes are pinned to the leader; see
	// internal/router/router.go's Forward), so the node answering may simply not
	// have applied the entry yet.
	replication := time.Now().Add(20 * time.Second)
	for {
		allSee := true
		for _, addr := range nodeAddrs {
			if !nodeSeesKB(ctx, addr, kbID) {
				allSee = false
				break
			}
		}
		if allSee {
			break
		}
		if time.Now().After(replication) {
			for i, addr := range nodeAddrs {
				if !nodeSeesKB(ctx, addr, kbID) {
					t.Errorf("node %d (%s) never saw the version created on the leader", i, addr)
				}
			}
			t.FailNow()
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Data landed and is queryable. Reads are served by the storage tier — a
	// control node holds no data to answer with — and the index build is
	// asynchronous, so the version is not queryable the instant it is committed:
	// poll until the document that was written comes back.
	_, q, _, qconn, err := dialNode(storageAddrs[0])
	if err != nil {
		t.Fatalf("dial storage: %v", err)
	}
	defer qconn.Close()

	build := time.Now().Add(90 * time.Second)
	var lastErr error
	for {
		queryResp, qerr := q.Query(ctx, &pb.QueryRequest{
			KnowledgeBaseId: kbID,
			VersionId:       &versionID,
			Vector:          queryVector(768),
			TopK:            5,
		})
		if qerr == nil && len(queryResp.GetResults()) > 0 {
			if got := queryResp.GetResults()[0].GetDocId(); got != docID {
				t.Errorf("top hit docId = %q, want %q", got, docID)
			}
			break
		}
		lastErr = qerr
		if time.Now().After(build) {
			t.Fatalf("the written version never became queryable (last error: %v)", lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// === T4-2: Fault tolerance ===

// TestT4_MinorityFaultTolerance kills one follower: with 2 of 3 nodes up the
// cluster still has a quorum and must keep accepting writes.
//
// Which node is "a follower" is asked of the control tier (controlLeaderIndex),
// not derived from the index waitForLeader returns: that index names a STATION —
// TestMain points every client address at one — so deriving the victim from it
// killed a fixed container, and on a cluster where that container happened to
// lead the case took the leader down while claiming to spare it. Killing the
// leader is TestT4_LeaderFailover's job; doing it here by accident is how this
// case stopped measuring minority faults at all.
func TestT4_MinorityFaultTolerance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	_, kbID := waitForLeader(t, ctx, "minority", 30*time.Second)
	leaderIdx := controlLeaderIndex(t, ctx)

	// Kill a follower (any node other than the leader).
	followerIdx := (leaderIdx + 1) % len(nodeServices)
	followerSvc := nodeServices[followerIdx]
	t.Logf("control node %d leads; killing follower %s", leaderIdx, followerSvc)
	killNode(t, followerSvc)
	defer func() {
		startNode(t, followerSvc)
		waitForLeader(t, context.Background(), "minority-restore", 30*time.Second)
	}()

	// The remaining 2 nodes still form a quorum: a new write must succeed.
	newLeaderIdx, newKBID := waitForLeader(t, ctx, "minority-write", 20*time.Second)
	t.Logf("post-kill write succeeded on node %d, KB %s", newLeaderIdx, newKBID)

	// The pre-kill KB must still be readable (no data loss).
	if !nodeSeesKB(ctx, nodeAddrs[newLeaderIdx], kbID) {
		t.Errorf("leader %d lost pre-fault KB %s", newLeaderIdx, kbID)
	}
}

// TestT4_LeaderFailover kills a control node and requires writes to resume.
//
// What it asserts changed with the topology, and the change is the point: behind
// a service station, "which node is the leader" is not something a client can
// see — that abstraction is what §9.2 exists to build. So the old assertion
// ("a different node became leader") was checking a fact the client deliberately
// does not have, and it broke the moment the suite started going through the
// station.
//
// What a client can observe, and what the system owes it, is availability: with
// one control node gone the remaining two still form a quorum, Raft elects among
// them, and the station forwards the write to whoever won. A write must succeed
// within the budget.
func TestT4_LeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	_, kbID := waitForLeader(t, ctx, "failover", 30*time.Second)

	// Any control node will do — losing one is the fault being injected.
	victim := nodeServices[0]
	t.Logf("killing %s", victim)
	killNode(t, victim)
	defer func() {
		startNode(t, victim)
		waitForLeader(t, context.Background(), "failover-restore", 60*time.Second)
	}()

	// The write is retried against the station for up to a minute: the failure
	// it must survive is an election, which takes seconds, not the write's own
	// latency.
	writeDocumentThroughControl(t, ctx, kbID, "d1", "控制节点消失后,写入仍须在预算内成功。")
	t.Logf("writes resumed with %s down", victim)
}

// TestT4_NodeRestartRecovery kills a node, restarts it, and verifies it
// catches up with the leader's log (data re-sync).
func TestT4_NodeRestartRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "restart", 30*time.Second)
	t.Logf("leader is node %d (%s)", leaderIdx, nodeAddrs[leaderIdx])

	// Kill and restart a follower (not the leader).
	victimIdx := (leaderIdx + 1) % 3
	victimSvc := nodeServices[victimIdx]
	victimAddr := nodeAddrs[victimIdx]

	killNode(t, victimSvc)
	startNode(t, victimSvc)
	defer func() {
		waitForLeader(t, context.Background(), "restart-restore", 30*time.Second)
	}()

	// The restarted node must catch up with the committed KB.
	waitForNodeToSeeKB(t, ctx, victimAddr, kbID, 40*time.Second)
	t.Logf("node %d (%s) caught up with KB %s", victimIdx, victimAddr, kbID)
}

// === T4-3: Cluster performance ===

func TestT4_PerformanceBaseline(t *testing.T) {
	t.Skip("performance benchmarks — run with dedicated benchmarking harness")
}

// === T4-4: Storage efficiency ===

func TestT4_StorageEfficiency(t *testing.T) {
	t.Skip("requires populating 100万 chunks — long-running storage benchmark")
}
