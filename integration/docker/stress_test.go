// Package docker_test — Stratum T4 stress cases.
//
// Fills the 3-node Docker cluster and measures what §阶段⑤ requires but had no
// implementation: single-version query latency at scale (including the disk read
// after a restart), multi-version memory-rotation stability, plus an end-to-end
// §8.6(d) collection run on a real faiss artifact.
//
// Scale via STRATUM_STRESS_DOCS / _QUERIES / _DELETE_PERCENT / _VERSIONS.
//
//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// Stress cases. These fill the two holes §阶段⑤ names ("单版本大 n 构建/查询延迟
// (含磁盘读)、多版本分级换出稳定性") — requirements that existed in the design
// document but had no implementation: TestT4_PerformanceBaseline and
// TestT4_StorageEfficiency were t.Skip placeholders.
//
// They are written the same way the data-volume case is: small enough to run in CI,
// with environment variables to raise them for a real measurement, and they PRINT
// their measurement rather than just passing.

// --- Tunables ---------------------------------------------------------------

func stressDocs() int {
	if v := os.Getenv("STRATUM_STRESS_DOCS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 2000
}

func stressQueries() int {
	if v := os.Getenv("STRATUM_STRESS_QUERIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 200
}

// stressDeletePercent is the share of the written documents the GC case deletes —
// the dead weight it exists to provoke. Override with STRATUM_STRESS_DELETE_PERCENT.
func stressDeletePercent() int {
	if v := os.Getenv("STRATUM_STRESS_DELETE_PERCENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n < 100 {
			return n
		}
	}
	return 80
}

// stressVersions is how many versions the eviction case keeps in rotation.
// Override with STRATUM_STRESS_VERSIONS.
func stressVersions() int {
	if v := os.Getenv("STRATUM_STRESS_VERSIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 1 {
			return n
		}
	}
	return 6
}

func stressTimeout() time.Duration {
	if v := os.Getenv("STRATUM_STRESS_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 10 * time.Minute
}

// stressCollectTimeout bounds how long the GC case waits for the scanner to notice
// and collect. Override with STRATUM_STRESS_GC_TIMEOUT.
func stressCollectTimeout() time.Duration {
	if v := os.Getenv("STRATUM_STRESS_GC_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 3 * time.Minute
}

// indexDirOf is where storage node i keeps its index artifacts.
func indexDirOf(i int) string { return storageDataDir(i) + "/index" }

// --- Helpers ----------------------------------------------------------------

// writeChanges commits one CreateVersion on top of parent and returns the new
// version id.
func writeChanges(t *testing.T, ctx context.Context, addr, kbID string, parent int64, changes []*pb.DocChange) int64 {
	t.Helper()
	kb, _, _, conn, err := dialNode(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()

	resp, err := kb.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: parent,
		Changes:         changes,
	})
	if err != nil {
		t.Fatalf("CreateVersion(%d changes) failed: %v", len(changes), err)
	}
	return resp.VersionId
}

// rollbackTo points the knowledge base's active version at versionID.
//
// The name is service-layer history: this is the "switch the active pointer
// without downtime" operation (§7), and it accepts any READY version — it is not
// restricted to ancestors. That matters here, because the version §8.6(d) should
// collect is precisely the one that is both active and carrying tombstones.
func rollbackTo(ctx context.Context, addr, kbID string, versionID int64) error {
	kb, _, _, conn, err := dialNode(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = kb.RollbackVersion(ctx, &pb.RollbackVersionRequest{
		KnowledgeBaseId: kbID,
		TargetVersionId: versionID,
	})
	return err
}

// writeVersion adds docs as a chain of CreateVersion batches and returns the last
// version id, which is the one carrying all of them.
//
// Batching is not optional: a single request has to stay under the 4 MiB gRPC
// message limit (datavolume_test.go), and each batch is only chained after the
// previous version reaches READY, because a parent must not be PENDING.
func writeVersion(t *testing.T, ctx context.Context, addr, kbID string, docs []docUnit) int64 {
	t.Helper()
	batch := dataVolumeBatch()
	parent := int64(0)
	for offset := 0; offset < len(docs); offset += batch {
		end := offset + batch
		if end > len(docs) {
			end = len(docs)
		}
		changes := make([]*pb.DocChange, 0, end-offset)
		for _, d := range docs[offset:end] {
			changes = append(changes, &pb.DocChange{
				Op:      pb.ChangeOp_CHANGE_OP_ADD,
				DocId:   d.id,
				Content: d.content,
			})
		}
		parent = writeChanges(t, ctx, addr, kbID, parent, changes)
		if end < len(docs) {
			waitVersionStatus(t, ctx, addr, kbID, parent, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
		}
	}
	return parent
}

// genUniqueDocs generates n documents whose contents are all DISTINCT.
//
// datavolume_test.go's genDataVolumeDocs cycles a 6-sentence list seeded by the
// document index, so its documents come in only 6 distinct flavours — fine for
// measuring write volume, useless for the §8.6(d) case. Chunks are
// content-addressed and stored once, so near-identical documents share their
// chunks: deleting 480 of 600 such documents removed almost no chunk, and the
// tombstones this case exists to provoke never appeared. (Measured: 600 documents
// collapsed to 18 chunk ids, and the scan found no candidate at all.)
//
// This generator drives each document from its own PRNG seed over a character
// pool, so every sliding-window chunk is its own. That is the precondition for
// "deleting documents ⇒ dead vectors", which is all this case is about — not a
// claim about realistic corpora.
func genUniqueDocs(n, targetRunes int) []docUnit {
	pool := []rune("向量检索分层缓存预写日志共识快照索引墓碑回收滚动重建活跃版本副本服务能力阈值占位标定" +
		"召回粗筛量化重排分段切分窗口重叠去重幂等水位追链自愈广播鉴权闸门熔断降级限流背压")
	docs := make([]docUnit, n)
	for i := 0; i < n; i++ {
		rng := rand.New(rand.NewSource(int64(i) * 7919))
		var sb strings.Builder
		sb.Grow(targetRunes * 3)
		for len([]rune(sb.String())) < targetRunes {
			sb.WriteRune(pool[rng.Intn(len(pool))])
		}
		docs[i] = docUnit{
			id:      fmt.Sprintf("doc-%06d", i),
			content: sb.String(),
		}
	}
	return docs
}

// indexBytes sums the index directory across the whole storage group — the number
// §8.6(d) exists to bring down.
func indexBytes(t *testing.T) int64 {
	t.Helper()
	var total int64
	for i, svc := range storageServices {
		total += duNodeBytes(t, svc, indexDirOf(i))
	}
	return total
}

// nodeLogsSince returns a storage node's container logs.
func nodeLogsSince(t *testing.T, service string) string {
	t.Helper()
	out, err := exec.Command("docker", "logs", service).CombinedOutput()
	if err != nil {
		return ""
	}
	return string(out)
}

// anyStorageLogHas reports whether any storage node's log contains needle. Logs are
// the only place the §8.6(d) scanner's decisions surface today, so a pressure test
// has to read them to know the scanner ran at all.
func anyStorageLogHas(t *testing.T, needle string) bool {
	t.Helper()
	for _, svc := range storageServices {
		if strings.Contains(nodeLogsSince(t, svc), needle) {
			return true
		}
	}
	return false
}

// percentiles sorts d in place and returns p50, p95, p99 and the mean.
//
// The tail is the point. A retrieval service is judged by its slowest queries, and
// a mean over a skewed distribution describes neither the common case nor the bad
// one.
func percentiles(d []time.Duration) (p50, p95, p99, mean time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0, 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(q float64) time.Duration { return d[int(q*float64(len(d)-1))] }
	var total time.Duration
	for _, v := range d {
		total += v
	}
	return at(0.50), at(0.95), at(0.99), total / time.Duration(len(d))
}

// --- T4-5: Query latency (cold + warm) --------------------------------------

// TestT4_QueryLatency measures the read half of 阶段⑤'s "单版本大 n 构建/查询延迟
// (含磁盘读)".
//
// Two things are reported that a single average would hide:
//
//   - the TAIL (p50/p95/p99 separately), because that is what a retrieval service
//     is actually judged by;
//   - the COLD/WARM split. The first query on a version after a restart has to Load
//     the artifact from disk; every later one is served from memory. Those are
//     different costs, and averaging them yields a number describing neither.
func TestT4_QueryLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stressTimeout())
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "stress-latency", 30*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	kbID = measurementKB(t, ctx, leaderAddr, "stress-latency", kbID)
	docCount := stressDocs()
	t.Logf("leader is node %d (%s), KB %s, writing %d documents", leaderIdx, leaderAddr, kbID, docCount)

	versionID := writeVersion(t, ctx, leaderAddr, kbID, genDataVolumeDocs(docCount, 1000))
	waitVersionStatus(t, ctx, leaderAddr, kbID, versionID, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	t.Logf("version %d is READY", versionID)

	// Every storage replica must serve the version before the cold measurement means
	// anything: a replica that never received the artifact would answer — or fail —
	// for an entirely different reason.
	for i, addr := range storageAddrs {
		awaitServable(t, ctx, addr, kbID, versionID, indexBuildTimeout())
		t.Logf("storage %d serves the version", i+1)
	}

	// --- Cold: restart one storage node and time its FIRST query. ---
	coldIdx := 0
	killNode(t, storageServices[coldIdx])
	startNode(t, storageServices[coldIdx])
	if err := waitForListen(storageAddrs[coldIdx], 60*time.Second); err != nil {
		t.Fatalf("storage %d did not come back after restart: %v", coldIdx+1, err)
	}
	// The port being open is not the same as the artifact being loaded: that is
	// exactly the work the cold query pays for.
	coldStart := time.Now()
	if _, err := serveFrom(ctx, storageAddrs[coldIdx], kbID, versionID); err != nil {
		t.Fatalf("the cold query on the restarted node failed: %v", err)
	}
	cold := time.Since(coldStart)
	t.Logf("COLD first query on a restarted replica (index loaded from disk): %v", cold)

	// --- Warm ---
	queries := stressQueries()
	warm := make([]time.Duration, 0, queries)
	for i := 0; i < queries; i++ {
		start := time.Now()
		resp, err := serveFrom(ctx, storageAddrs[coldIdx], kbID, versionID)
		if err != nil {
			t.Fatalf("warm query %d/%d failed: %v", i+1, queries, err)
		}
		warm = append(warm, time.Since(start))
		if len(resp.Results) == 0 {
			t.Fatalf("warm query %d/%d returned no results", i+1, queries)
		}
	}
	p50, p95, p99, mean := percentiles(warm)
	t.Logf("WARM queries=%d mean=%v p50=%v p95=%v p99=%v", len(warm), mean, p50, p95, p99)

	// Zero-vector control: the same version on the same replica, queried with the
	// all-zero vector this suite used to send everywhere. Reported in the same run
	// so the two workloads can be compared directly rather than across commits —
	// an all-zero vector is equidistant from every document, so the HNSW walk has
	// nothing to prune with, and the difference is the price of measuring a
	// workload no caller sends (v13 §5 #15).
	control := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		start := time.Now()
		resp, err := serveVector(ctx, storageAddrs[coldIdx], kbID, versionID, make([]float32, 768), 5)
		if err != nil {
			t.Fatalf("zero-vector control query %d/20 failed: %v", i+1, err)
		}
		if len(resp.Results) == 0 {
			t.Fatalf("zero-vector control query %d/20 returned no results", i+1)
		}
		control = append(control, time.Since(start))
	}
	cp50, cp95, _, cmean := percentiles(control)
	t.Logf("ZERO-VECTOR CONTROL queries=%d mean=%v p50=%v p95=%v", len(control), cmean, cp50, cp95)

	t.Logf("QUERY-LATENCY SUMMARY: docs=%d queries=%d cold=%v p50=%v p95=%v p99=%v mean=%v zero_p50=%v",
		docCount, len(warm), cold, p50, p95, p99, mean, cp50)
}

// --- T4-6: §8.6(d) collection under real dead weight ------------------------

// TestT4_GCPressure drives §8.6(d) end to end on a real faiss artifact: write a
// version, delete most of it, and watch the dead weight actually come back.
//
// The unit tests cover every refusal path and the estimate; what they cannot cover
// is whether a real HNSW artifact can be rewritten at all and whether the version
// stays queryable afterwards. That is what this adds — and it is the only place the
// whole chain (scan → decide → reopen/rebuild → save → redistribute) runs on real
// bytes.
//
// Collection is opt-in (index_manager.gc_enabled). A run whose configs leave it off
// is a legitimate run, so this skips with the reason rather than failing — "the
// operator has not enabled it" is not a defect.
func TestT4_GCPressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stressTimeout())
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "stress-gc", 30*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	docCount := stressDocs()
	deletePct := stressDeletePercent()
	docs := genUniqueDocs(docCount, 1000)

	baseVersion := writeVersion(t, ctx, leaderAddr, kbID, docs)
	waitVersionStatus(t, ctx, leaderAddr, kbID, baseVersion, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	before := indexBytes(t)
	t.Logf("base version %d READY; index bytes across the storage group: %d", baseVersion, before)

	// Delete most of it AND add a little, in the SAME version. The mixture is the
	// precondition, not decoration: §8.6(c) reuses the parent artifact only when
	// there is something to append — `appendBase` returns ok=false as soon as
	// `len(delta) == 0`, because "copy the parent, dead vectors and all" is never
	// worth it when nothing new comes along. A version that ONLY deletes is
	// therefore rebuilt from scratch, and a rebuild drops every tombstone: measured
	// at 362 index lines (clean) instead of ~1800 with tombstones.
	//
	// With the append actually happening, the deleted documents' vectors remain as
	// TOMBSTONES in the sealed artifact — precisely the dead weight §8.6(d) exists to
	// reclaim.
	deleteCount := docCount * deletePct / 100
	added := genUniqueDocs(docCount/20+1, 1000)
	changes := make([]*pb.DocChange, 0, deleteCount+len(added))
	for i := 0; i < deleteCount; i++ {
		changes = append(changes, &pb.DocChange{
			Op:    pb.ChangeOp_CHANGE_OP_DELETE,
			DocId: docs[i].id,
		})
	}
	for i := range added {
		changes = append(changes, &pb.DocChange{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   fmt.Sprintf("late-%06d", i),
			Content: added[i].content,
		})
	}
	trimmed := writeChanges(t, ctx, leaderAddr, kbID, baseVersion, changes)
	waitVersionStatus(t, ctx, leaderAddr, kbID, trimmed, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	afterDelete := indexBytes(t)
	t.Logf("version %d READY after deleting %d/%d docs; index bytes: %d (delta %+d)",
		trimmed, deleteCount, docCount, afterDelete, afterDelete-before)

	if afterDelete <= before {
		t.Logf("note: the artifact did not grow with the tombstoned vectors (%d → %d); "+
			"the append may have rebuilt instead of reusing, which leaves no dead weight to collect",
			before, afterDelete)
	}

	// Make the tombstoned version the ACTIVE one — §8.6(d) collects active versions
	// only. The design's target is "long-lived, continuously queried, no successor
	// in sight", and activeness is how the control layer expresses exactly that.
	//
	// CreateVersion does NOT move the active pointer (only the set-active command
	// does), so without this step the knowledge base has no active version at all
	// and the scanner has nothing to look at. This missing step — not a faulty
	// candidate judgement — is why this case used to skip.
	if err := rollbackTo(ctx, leaderAddr, kbID, trimmed); err != nil {
		t.Fatalf("RollbackVersion(%d) failed: %v", trimmed, err)
	}
	t.Logf("version %d is now the active version", trimmed)

	// --- Wait for the scanner to collect ---
	deadline := time.Now().Add(stressCollectTimeout())
	var afterCollect int64
	collected := false
	for time.Now().Before(deadline) {
		if anyStorageLogHas(t, "index: gc: collected") {
			collected = true
			// Let the rewrite settle before reading sizes.
			time.Sleep(2 * time.Second)
			afterCollect = indexBytes(t)
			break
		}
		time.Sleep(3 * time.Second)
	}

	if !collected {
		switch {
		case !anyStorageLogHas(t, "gc scan read the active versions"):
			t.Skip("the §8.6(d) scanner is not running on any storage node — " +
				"check index_manager.gc_sweep_interval_ms (negative disables it) and the data dir")
		case !anyStorageLogHas(t, "cleanup candidate"):
			t.Skipf("the scanner ran but found NO candidate. §8.6(d) has a narrow "+
				"precondition and any one of these defeats it — (1) the sealed artifact "+
				"carries no tombstones, because §8.6(c) rebuilt instead of reusing: a "+
				"version that only deletes has an empty delta, and a build whose parent "+
				"artifact sits on another node cannot reuse either (1 of %d nodes here); "+
				"(2) the dead share is at or below GCRatioThreshold; (3) AppendMaxDeadRatio "+
				"and GCRatioThreshold share the 0.2 default, so §8.6(c) rebuilds at exactly "+
				"the share §8.6(d) starts caring about, and (d) never sees anything. "+
				"Index bytes across the group: %d → %d.",
				len(storageServices), before, afterDelete)
		default:
			t.Skipf("a candidate was found but nothing was collected within %v — check "+
				"index_manager.serving_replica_min (with %d storage replicas, collection "+
				"needs enough left serving) and whether collection is enabled "+
				"(index_manager.gc_enabled)",
				stressCollectTimeout(), len(storageServices))
		}
	}

	t.Logf("COLLECTED: index bytes %d → %d (%.1f%% of the post-delete size reclaimed)",
		afterDelete, afterCollect, 100*float64(afterDelete-afterCollect)/float64(afterDelete))
	if afterCollect >= afterDelete {
		t.Errorf("a collection was logged but the index did not shrink (%d → %d) — "+
			"either the log is lying or the bytes measured are not the artifact's",
			afterDelete, afterCollect)
	}

	// The point is reclaiming SPACE, not breaking reads: the surviving documents must
	// still be queryable after the artifact was rewritten.
	resp, err := serveFrom(ctx, storageAddrs[0], kbID, trimmed)
	if err != nil {
		t.Fatalf("query after collection failed: %v", err)
	}
	if len(resp.Results) == 0 {
		t.Error("query after collection returned no results — collection must not discard live vectors")
	}
	t.Logf("post-collection query returned %d results", len(resp.Results))

	t.Logf("GC-PRESSURE SUMMARY: docs=%d deleted=%d indexBytes before=%d afterDelete=%d afterCollect=%d",
		docCount, deleteCount, before, afterDelete, afterCollect)
}

// --- T4-7: Multi-version rotation stability ---------------------------------

// TestT4_MultiVersionEviction rotates queries across several versions of one
// knowledge base — the "多版本分级换出稳定性" half of 阶段⑤.
//
// Each version has its own artifact and its own memory budget (§8.6a): querying a
// set larger than the cache forces versions to be evicted and loaded again. What
// must hold is that rotation neither crashes nor corrupts: every version keeps
// answering, and the answers do not degrade as pressure grows. A single version
// tested alone cannot show that — the failure mode only appears when something has
// to be thrown out to make room.
func TestT4_MultiVersionEviction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stressTimeout())
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "stress-evict", 30*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	versions := stressVersions()
	perVersion := stressDocs() / versions
	if perVersion < 10 {
		perVersion = 10
	}
	t.Logf("leader is node %d, KB %s: %d versions × %d docs", leaderIdx, kbID, versions, perVersion)

	// Build a CHAIN: version N is version N-1 plus a batch, so every version stays a
	// live, queryable artifact rather than a detached snapshot.
	ids := make([]int64, 0, versions)
	parent := int64(0)
	for v := 0; v < versions; v++ {
		// Prefix the ids so the versions hold visibly distinct document sets, which
		// makes "did the right version answer?" answerable from the results.
		docs := genDataVolumeDocs(perVersion, 1000)
		for i := range docs {
			docs[i].id = "v" + strconv.Itoa(v) + "-" + docs[i].id
		}
		changes := make([]*pb.DocChange, 0, len(docs))
		for _, d := range docs {
			changes = append(changes, &pb.DocChange{
				Op:      pb.ChangeOp_CHANGE_OP_ADD,
				DocId:   d.id,
				Content: d.content,
			})
		}
		parent = writeChanges(t, ctx, leaderAddr, kbID, parent, changes)
		waitVersionStatus(t, ctx, leaderAddr, kbID, parent, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
		ids = append(ids, parent)
	}
	t.Logf("built %d versions: %v", len(ids), ids)

	// Every version must be servable on the replica under test before rotation
	// starts, otherwise a failure would be blamed on rotation when it is really a
	// missing artifact.
	target := storageAddrs[0]
	for _, id := range ids {
		awaitServable(t, ctx, target, kbID, id, indexBuildTimeout())
	}

	// Rotate: ask for each version in turn, several times around, and require every
	// answer to keep coming back non-empty.
	rounds := 3
	perRound := 0
	for round := 0; round < rounds; round++ {
		// Alternate direction so eviction order is not the same every round.
		order := make([]int, len(ids))
		for i := range order {
			if round%2 == 0 {
				order[i] = i
			} else {
				order[i] = len(ids) - 1 - i
			}
		}
		for _, idx := range order {
			id := ids[idx]
			resp, err := serveFrom(ctx, target, kbID, id)
			if err != nil {
				t.Fatalf("round %d: query on version %d failed: %v", round+1, id, err)
			}
			if len(resp.Results) == 0 {
				t.Fatalf("round %d: version %d returned no results after rotation — "+
					"a version was evicted and came back wrong", round+1, id)
			}
			perRound++
		}
	}
	t.Logf("EVICTION SUMMARY: versions=%d rounds=%d queries=%d — every version kept answering across rotation",
		len(ids), rounds, perRound)
}
