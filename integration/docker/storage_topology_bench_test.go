//go:build docker
// +build docker

package docker_test

// storage_topology_bench_test.go — the measurements docs/实施中/storage-topology-plan.md
// §10 asks for, taken on the five-storage-node placement topology.
//
// What it answers and what it does NOT:
//
//   - It reports THIS build's absolute numbers: write throughput and latency, query
//     p50/p95/p99, per-node bytes and KB counts, the replica amplification the
//     placement model produces, and control-side RPC/s, bytes/s and handling latency
//     from the node's own /metrics.
//   - It is NOT an A/B against the pre-placement (full-copy) code. A cross-version
//     comparison needs its own cluster built from the other commit, and mixing the
//     two would make every number a statement about the machines rather than about
//     the topology. Plan §23.5 says the same: historical numbers cannot be used to
//     claim a performance change.
//
// Run (five storage nodes, station inside the cluster network):
//
//	scripts/cluster.sh --topology two-tier --storage-nodes 5 clean
//	scripts/cluster.sh --topology two-tier --storage-nodes 5 up
//	STRATUM_T4_STORAGE_NODE_ADDRS=localhost:17100,localhost:17101,localhost:17102,localhost:17103,localhost:17104 \
//	STRATUM_T4_STORAGE_SERVICES=stratum-node-storage1,stratum-node-storage2,stratum-node-storage3,stratum-node-storage4,stratum-node-storage5 \
//	STRATUM_T4_STATION_NETWORK=stratum-net STRATUM_PERF=1 \
//	go test ./integration/docker/ -tags=docker -run TestStorageTopologyPerf -v -count=1 -timeout 60m
//
// Scale via STRATUM_PERF_KBS / STRATUM_PERF_DOCS / STRATUM_PERF_QUERIES.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// perfKBs is how many knowledge bases the profile creates. They are spread over
// overlapping replica sets so no node owns everything — which is the property the
// measurement is about.
func perfKBs() int {
	return envInt("STRATUM_PERF_KBS", 5)
}

// perfDocs is documents per knowledge base.
func perfDocs() int {
	return envInt("STRATUM_PERF_DOCS", 2000)
}

// perfQueries is warm queries per knowledge base.
func perfQueries() int {
	return envInt("STRATUM_PERF_QUERIES", 100)
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// perfReplicaSets returns the replica set for KB i: three nodes out of five,
// rotating so every node is a member of some sets and excluded from others.
func perfReplicaSets(n int) [][]int64 {
	base := [][]int64{
		{11, 12, 13},
		{12, 13, 14},
		{13, 14, 15},
		{14, 15, 11},
		{15, 11, 12},
	}
	out := make([][]int64, n)
	for i := range out {
		out[i] = base[i%len(base)]
	}
	return out
}

// kbPerf is the per-knowledge-base record the summary lines are built from.
type kbPerf struct {
	kbID      string
	set       []int64
	versions  []int64
	batchDur  []time.Duration
	docs      int
	latencies []time.Duration
}

// writeBatchThroughControl submits one CreateVersion and returns the version it
// created plus how long the call took.
//
// Unlike writeDocumentThroughControl it does not turn a failed call into a fresh
// request (only "no leader available" is retried, which is a routing fact): the
// measurement has to see a real failure as a failure.
func writeBatchThroughControl(t *testing.T, ctx context.Context, kbID string, docs []docUnit, key string) (int64, time.Duration, error) {
	t.Helper()
	changes := make([]*pb.DocChange, 0, len(docs))
	for _, d := range docs {
		changes = append(changes, &pb.DocChange{Op: pb.ChangeOp_CHANGE_OP_ADD, DocId: d.id, Content: d.content})
	}
	req := &pb.CreateVersionRequest{KnowledgeBaseId: kbID, ClientRequestId: key, Changes: changes}

	start := time.Now()
	var lastErr error
	for _, addr := range nodeAddrs {
		kb, _, _, conn, err := dialNode(addr)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := kb.CreateVersion(ctx, req)
		conn.Close()
		if err == nil {
			return resp.GetVersionId(), time.Since(start), nil
		}
		lastErr = err
		if isTransientSetupError(err) {
			continue
		}
		break
	}
	return 0, time.Since(start), lastErr
}

// scrapeMetrics fetches one node's /metrics from inside the cluster network.
//
// The metrics port is not published to the host (scripts/cluster.sh maps gRPC
// only), and the endpoint has no authentication of its own, so a short-lived
// container on the same network is the least invasive way to read it.
func scrapeMetrics(t *testing.T, container string) string {
	t.Helper()
	out, err := exec.Command("docker", "run", "--rm", "--network", "stratum-net", "alpine:latest",
		"wget", "-qO-", fmt.Sprintf("http://%s:9000/metrics", container)).CombinedOutput()
	if err != nil {
		t.Logf("scrape %s: %v (%s)", container, err, strings.TrimSpace(string(out)))
		return ""
	}
	return string(out)
}

// metricCounter reads one series' value out of a Prometheus text scrape.
func metricCounter(text, series string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

// grpcSeries builds the exact series text the exposition format prints for one
// (service, method, code) triple.
func grpcSeries(service, method, code string) string {
	return fmt.Sprintf(`stratum_grpc_requests_total{code=%q,method=%q,service=%q}`, code, method, service)
}

// controlScrape is one control node's counters, before and after the workload.
type controlScrape struct {
	name          string
	before, after string
}

// delta is the counter's increase over the window, or -1 when it is missing on
// either side (a series that did not exist before the first call).
func (c controlScrape) delta(series string) (float64, bool) {
	b, okB := metricCounter(c.before, series)
	a, okA := metricCounter(c.after, series)
	if !okA {
		return 0, false
	}
	if !okB {
		// The series appeared during the window, so its counter starts at its
		// current value: the increase is the whole value.
		return a, true
	}
	return a - b, true
}

// nodeBytes reports the on-disk footprint of a storage node's data directory.
func nodeBytes(t *testing.T, id int64) int64 {
	t.Helper()
	out, err := exec.Command("docker", "exec", storageServiceOf(t, id), "du", "-sb",
		fmt.Sprintf("/var/lib/stratum/node%d", id)).CombinedOutput()
	if err != nil {
		t.Logf("du on node %d: %v (%s)", id, err, strings.TrimSpace(string(out)))
		return 0
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return 0
	}
	n, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// TestStorageTopologyPerf_PlacementProfile is the §10 measurement run.
//
// Output is a set of `PERF …` lines. They are PRINTED, not asserted: what counts
// as "good" depends on the machine, and a benchmark that fails on a slow runner
// teaches nothing. The one thing asserted is that the workload completed — a
// profile of a half-finished run is worse than no profile.
func TestStorageTopologyPerf_PlacementProfile(t *testing.T) {
	if os.Getenv("STRATUM_PERF") != "1" {
		t.Skip("set STRATUM_PERF=1 to run the placement performance profile")
	}
	requireFiveStorageNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Minute)
	defer cancel()

	kbs := perfKBs()
	docsPerKB := perfDocs()
	queriesPerKB := perfQueries()
	sets := perfReplicaSets(kbs)
	const batch = 1000 // one CreateVersion must stay under the 4 MiB message limit

	controls := []controlScrape{
		{name: "stratum-node-control1"},
		{name: "stratum-node-control2"},
		{name: "stratum-node-control3"},
	}
	for i := range controls {
		controls[i].before = scrapeMetrics(t, controls[i].name)
	}
	workStart := time.Now()

	// --- Phase 1: create the KBs with explicit replica sets ---
	profiles := make([]*kbPerf, 0, kbs)
	for i := 0; i < kbs; i++ {
		kbID := createKBWithReplicas(t, ctx, fmt.Sprintf("perf-%d", i), sets[i])
		p := placementOf(storageDirectory(t, ctx), kbID)
		if p == nil || !sameIDs(p.GetReplicaIds(), sets[i]) {
			t.Fatalf("KB %s placement = %v, want %v", kbID, p.GetReplicaIds(), sets[i])
		}
		profiles = append(profiles, &kbPerf{kbID: kbID, set: sets[i]})
	}
	t.Logf("PERF setup: kbs=%d docs_per_kb=%d queries_per_kb=%d replica_sets=%v", kbs, docsPerKB, queriesPerKB, sets)

	// --- Phase 2: write, timing each batch end to end ---
	var totalDocs, totalBatches int
	var totalWrite, totalCatchup time.Duration
	for i, prof := range profiles {
		docs := genUniqueDocs(docsPerKB, 1000)
		for offset := 0; offset < len(docs); offset += batch {
			end := offset + batch
			if end > len(docs) {
				end = len(docs)
			}
			versionID, dur, err := writeBatchThroughControl(t, ctx, prof.kbID, docs[offset:end], fmt.Sprintf("perf-%d-%d", i, offset))
			if err != nil {
				t.Fatalf("write batch %d of KB %s: %v", offset/batch, prof.kbID, err)
			}
			prof.versions = append(prof.versions, versionID)
			prof.batchDur = append(prof.batchDur, dur)
			prof.docs += end - offset
			totalDocs += end - offset
			totalBatches++
			totalWrite += dur
			t.Logf("PERF write: kb=%s batch=%d docs=%d version=%d dur=%v",
				prof.kbID, offset/batch, end-offset, versionID, dur.Round(time.Millisecond))
		}
		// Every member must hold the newest version before the read phase: a query
		// against a replica that has not caught up measures catch-up, not search.
		//
		// This wait is counted SEPARATELY from the commit latencies above. The commit
		// is an asynchronous acceptance (the RPC returns once the version is
		// allocated and dispatched), so reporting the two together would produce a
		// "docs/sec" number that describes the fixture's patience rather than the
		// system's throughput — measured: 10 batches of 1000 documents committed in
		// ~410 ms total, then 2m08s spent waiting for every replica's cursor.
		newest := prof.versions[len(prof.versions)-1]
		catchupStart := time.Now()
		for _, id := range prof.set {
			waitLocalVersion(t, ctx, storageAddrOf(t, id), prof.kbID, newest, 5*time.Minute)
		}
		totalCatchup += time.Since(catchupStart)
	}

	writeElapsed := time.Since(workStart)
	meanBatch := time.Duration(0)
	if totalBatches > 0 {
		meanBatch = totalWrite / time.Duration(totalBatches)
	}
	t.Logf("PERF write summary: docs=%d batches=%d commit_total=%v mean_commit=%v commit_docs_per_sec=%.0f catchup_total=%v window=%v",
		totalDocs, totalBatches, totalWrite.Round(time.Millisecond), meanBatch.Round(time.Millisecond),
		float64(totalDocs)/totalWrite.Seconds(), totalCatchup.Round(time.Millisecond), writeElapsed.Round(time.Second))

	// --- Phase 3: query through the station, per KB ---
	var all []time.Duration
	for _, prof := range profiles {
		newest := prof.versions[len(prof.versions)-1]
		// The FIRST servable read pays for whatever is not ready yet (index build, or
		// a fetch of the owner's artifact). Measured separately, because folding it
		// into the latency distribution is what turns a steady-state p99 of ~10 ms into
		// a p99 of seconds and hides the real tail.
		firstStart := time.Now()
		if resp := awaitServable(t, ctx, nodeAddrs[0], prof.kbID, newest, 5*time.Minute); resp == nil {
			t.Fatalf("station never served %s v%d", prof.kbID, newest)
		}
		firstServable := time.Since(firstStart)
		// Then every member, so the warm numbers below are not measuring a fan-out of
		// artifact fetches triggered by the measurement itself.
		for _, id := range prof.set {
			_ = awaitDirectServable(t, ctx, storageAddrOf(t, id), prof.kbID, newest, 3*time.Minute)
		}
		for q := 0; q < queriesPerKB; q++ {
			start := time.Now()
			if _, err := serveVector(ctx, nodeAddrs[0], prof.kbID, newest, queryVector(768), 5); err != nil {
				t.Fatalf("query %d of %s: %v", q, prof.kbID, err)
			}
			d := time.Since(start)
			prof.latencies = append(prof.latencies, d)
			all = append(all, d)
		}
		p50, p95, p99, mean := percentiles(append([]time.Duration(nil), prof.latencies...))
		t.Logf("PERF query: kb=%s set=%v first_servable=%v n=%d p50=%v p95=%v p99=%v mean=%v",
			prof.kbID, prof.set, firstServable.Round(time.Millisecond), len(prof.latencies),
			p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), mean.Round(time.Microsecond))
	}
	p50, p95, p99, mean := percentiles(append([]time.Duration(nil), all...))
	t.Logf("PERF query summary: n=%d p50=%v p95=%v p99=%v mean=%v",
		len(all), p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), mean.Round(time.Microsecond))

	elapsed := time.Since(workStart)
	for i := range controls {
		controls[i].after = scrapeMetrics(t, controls[i].name)
	}

	// --- Phase 4: placement footprint (the structural claim) ---
	perNodeKBs := map[int64]int{}
	totalReplicas := 0
	for _, prof := range profiles {
		totalReplicas += len(prof.set)
		for _, id := range prof.set {
			perNodeKBs[id]++
		}
	}
	for i := 0; i < 5; i++ {
		id := storageNodeID(i)
		t.Logf("PERF node: id=%d bytes=%d kbs=%d of %d", id, nodeBytes(t, id), perNodeKBs[id], kbs)
	}
	fullCopy := kbs * 5
	t.Logf("PERF placement: kbs=%d configured_replicas=%d full_copy_would_be=%d amplification=%.2fx",
		kbs, totalReplicas, fullCopy, float64(totalReplicas)/float64(fullCopy))

	// --- Phase 5: control-side traffic (§10: RPC/s, bytes/s, handling latency) ---
	for _, m := range []struct{ service, method string }{
		{"KnowledgeBaseService", "CreateVersion"},
		{"DataSyncService", "Heartbeat"},
		{"DataSyncService", "GetChainTails"},
		{"InternalService", "GetStorageDirectory"},
	} {
		var calls, reqBytes, durSum, durCount float64
		for _, c := range controls {
			if v, ok := c.delta(grpcSeries(m.service, m.method, "OK")); ok {
				calls += v
			}
			if v, ok := c.delta(fmt.Sprintf(`stratum_grpc_request_bytes_total{method=%q,service=%q}`, m.method, m.service)); ok {
				reqBytes += v
			}
			if v, ok := c.delta(fmt.Sprintf(`stratum_grpc_response_bytes_total{method=%q,service=%q}`, m.method, m.service)); ok {
				reqBytes += v
			}
			sum, okS := c.delta(fmt.Sprintf(`stratum_grpc_duration_seconds_sum{method=%q,service=%q}`, m.method, m.service))
			cnt, okC := c.delta(fmt.Sprintf(`stratum_grpc_duration_seconds_count{method=%q,service=%q}`, m.method, m.service))
			if okS && okC {
				durSum += sum
				durCount += cnt
			}
		}
		meanMs := 0.0
		if durCount > 0 {
			meanMs = durSum / durCount * 1000
		}
		t.Logf("PERF control rpc: %s.%s calls=%.0f bytes_in_out=%.0f rps=%.3f bytes_per_sec=%.0f handler_mean_ms=%.2f",
			m.service, m.method, calls, reqBytes, calls/elapsed.Seconds(), reqBytes/elapsed.Seconds(), meanMs)
	}
	var refused float64
	for _, c := range controls {
		for _, code := range []string{"Unavailable", "FailedPrecondition", "Internal", "Aborted", "DeadlineExceeded", "InvalidArgument"} {
			for _, svc := range []string{"KnowledgeBaseService", "QueryService", "DataSyncService", "InternalService", "AdminService"} {
				for _, method := range []string{"CreateVersion", "Query", "Heartbeat", "GetChainTails", "PushVersionData", "GetStorageDirectory", "LocalVersion", "PullVersionChanges"} {
					if v, ok := c.delta(grpcSeries(svc, method, code)); ok {
						refused += v
					}
				}
			}
		}
	}
	t.Logf("PERF control errors: non_ok_calls=%.0f (retries/backpressure show up here, not as lost writes)", refused)

	// --- Phase 6: resource use ---
	if out, err := exec.Command("docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}} {{.MemUsage}}",
		"stratum-node-control1", "stratum-node-control2", "stratum-node-control3",
		"stratum-node-storage1", "stratum-node-storage2", "stratum-node-storage3",
		"stratum-node-storage4", "stratum-node-storage5").CombinedOutput(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line != "" {
				t.Logf("PERF resource: %s", line)
			}
		}
	} else {
		t.Logf("PERF resource: docker stats failed: %v", err)
	}
	t.Logf("PERF window: elapsed=%v kbs=%d docs=%d queries=%d heartbeat_interval=5s",
		elapsed.Round(time.Second), kbs, totalDocs, len(all))
}
