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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
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

// reuseWait is how long the collection case waits for §8.6(c)'s "built by
// appending" line to show up on SOME replicas, after the version is READY.
// Override with STRATUM_STRESS_REUSE_WAIT.
//
// It has to be minutes, not a single read: see awaitVersionBuildLog for why the
// reuse of a distributed parent can land that late.
func reuseWait() time.Duration {
	if v := os.Getenv("STRATUM_STRESS_REUSE_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 2 * time.Minute
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

// versionIndexBytes sums the on-disk size of ONE version's artifact across the
// storage group: the .index payload plus the .index.ids sidecar beside it.
//
// Deliberately not indexBytes, which du's the WHOLE index directory — every past
// run of every case has written into it, so its movement says nothing about this
// version's artifact. Measured: a collection that had just dropped most of a
// version's vectors still showed the directory GROWING 33 MB from unrelated
// builds, and the case failed on an assertion about the artifact (its own
// message guessed the cause: "the bytes measured are not the artifact's"). The
// artifact is what §8.6(d) reclaims, so that is what to measure.
func versionIndexBytes(t *testing.T, kbID string, versionID int64) int64 {
	t.Helper()
	var total int64
	for i, svc := range storageServices {
		dir := fmt.Sprintf("%s/index/%s", storageDataDir(i), kbID)
		out, err := exec.Command("docker", "exec", svc, "sh", "-c",
			fmt.Sprintf("du -sb %s/%d.index %s/%d.index.ids 2>/dev/null | awk '{s+=$1} END {print s+0}'",
				dir, versionID, dir, versionID)).CombinedOutput()
		if err != nil {
			t.Fatalf("measuring %s v%d in %s: %v\n%s", kbID, versionID, svc, err, out)
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			t.Fatalf("measuring %s v%d in %s: parse %q: %v", kbID, versionID, svc, out, err)
		}
		total += n
	}
	return total
}

// awaitReplicaArtifact waits until every storage replica holds the version's
// artifact on disk.
//
// Why the collection case needs it: §8.6(c) reuses the parent artifact only where
// that parent IS. Its build ran on one node and §8.4 distributes it to the rest,
// so a child version written while that distribution is still in flight reuses
// nothing and is rebuilt from scratch — carrying no tombstones, which leaves
// §8.6(d) with nothing to collect. Measured twice: 74 MB (reused, tombstones
// present, collection ran and reclaimed 7,322 chunks) versus 41 MB against the
// parent's 65 MB (rebuilt, no tombstones, case correctly refuses to continue).
// The case is about collection, not about distribution timing, so it waits the
// race out instead of rolling dice on which node dispatches picked.
//
// Note this is NOT sufficient on its own to make the child reuse the parent: a
// replica that received the parent this way knows the artifact is on disk but not
// its SHAPE, and appendBase requires the latter (see awaitVersionBuildLog). What
// this wait buys is the other half of the measurement — every replica holding the
// version's artifact, so `du` reads the whole group rather than whoever finished
// first.
func awaitReplicaArtifact(t *testing.T, kbID string, versionID int64, timeout time.Duration) {
	t.Helper()
	for i, svc := range storageServices {
		dir := fmt.Sprintf("%s/index/%s", storageDataDir(i), kbID)
		deadline := time.Now().Add(timeout)
		for {
			out, _ := exec.Command("docker", "exec", svc, "sh", "-c",
				fmt.Sprintf("test -s %s/%d.index && test -s %s/%d.index.ids && echo yes",
					dir, versionID, dir, versionID)).Output()
			if strings.Contains(string(out), "yes") {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replica %s never received the artifact for %s v%d within %v", svc, kbID, versionID, timeout)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// nodeLogTailLines is how many of a node's most recent log lines the log helpers
// read. It is a WINDOW, not the whole file: every assertion that uses it asks what a
// node did in the last few seconds, and the file grows without bound across a suite.
const nodeLogTailLines = 5000

// nodeLogReaderImage runs `tail` against the host's docker log directory. It cannot
// be a node image (the storage image is built from scratch) and it is never used for
// anything but reading a file from a read-only mount.
const nodeLogReaderImage = "alpine:latest"

// nodeLogsSince returns a storage node's recent container logs.
//
// It reads the container's json log FILE instead of shelling out to `docker logs`,
// because the daemon's reader stops at the separator a container restart leaves in
// that file, and everything after it disappears from `docker logs`.
//
// Measured on the 3+3 cluster (docker 29.1.3, 2026-09-21): stratum-node-storage1's log
// file held 26815 lines and its last line was written seconds earlier, while
// `docker logs --tail 5000 <node>` returned 22634 lines — ending exactly at the blank
// line a SIGKILL+start had put there, 30 minutes in the past. The
// "caught up with the chain tail" line the caller was looking for was in the file the
// whole time: a run this suite reported as "never caught up" had in fact logged the
// catch-up 15 s after the node came back, with exactly the chain tail the case
// printed. Smaller `--tail` values happened to work (the daemon reads those backwards
// from the end), which is what makes the shell-out version a trap — it fails exactly
// when a node has been restarted often enough to matter, and no window size the
// caller picks is safe from it.
//
// Reading the file also disposes of the sibling problem: a SIGKILLed container leaves
// a torn record, and the daemon treats that as end-of-log too. Below, each line is
// parsed on its own, so a torn line costs one line rather than the tail of the file.
//
// The log directory is root-only on the host, so a throwaway container reads the file
// (read-only) and hands back the last nodeLogTailLines lines. If that cannot be done
// — no LogPath, no reader image, a docker that keeps its logs elsewhere — it falls
// back to `docker logs`: second best, but never a hard failure.
func nodeLogsSince(t *testing.T, service string) string {
	t.Helper()
	if out, ok := nodeLogFileTail(service, nodeLogTailLines); ok {
		return out
	}
	out, err := exec.Command("docker", "logs", "--tail", strconv.Itoa(nodeLogTailLines), service).CombinedOutput()
	if err != nil {
		return ""
	}
	return string(out)
}

// nodeLogFileTail reads the last `lines` records of a container's json log file and
// returns them as the plain text `docker logs` would have printed. ok is false when
// the path could not be resolved or read, so the caller can fall back.
func nodeLogFileTail(service string, lines int) (string, bool) {
	pathOut, err := exec.Command("docker", "inspect", "--format", "{{.LogPath}}", service).Output()
	if err != nil {
		return "", false
	}
	path := strings.TrimSpace(string(pathOut))
	if path == "" {
		return "", false
	}
	out, err := exec.Command("docker", "run", "--rm",
		"-v", filepath.Dir(path)+":/log:ro",
		nodeLogReaderImage,
		"tail", "-n", strconv.Itoa(lines), "/log/"+filepath.Base(path)).Output()
	if err != nil || len(out) == 0 {
		return "", false
	}
	return decodeContainerJSONLog(out), true
}

// decodeContainerJSONLog turns the docker json-file format back into log text: one
// record per line, the payload under "log". Lines that do not parse are DROPPED
// rather than read as the end of the log — the whole point of reading the file
// ourselves (see nodeLogsSince).
func decodeContainerJSONLog(raw []byte) string {
	var b strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec struct {
			Log string `json:"log"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		b.WriteString(rec.Log)
	}
	return b.String()
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

// appendLogMessage is §8.6(c)'s reuse line — the evidence the collection case
// asserts on, instead of the artifact's size (see the assertion for why size
// stopped being usable).
const appendLogMessage = "index: built by appending to the parent version's artifact (§8.6c)"

// builtFromScratchMessage is the other half of the reuse rate: the line a full
// rebuild leaves behind when §8.6(c)'s reuse did not apply (build → appendBase
// declined). Until this change that path logged NOTHING, which is why "only the
// node that built a parent ever reuses it" was invisible in production logs.
const builtFromScratchMessage = "index: built from scratch — no reusable parent artifact on this node (§8.6c)"

// messageNeedle scopes a log message to one (kb, version): the cluster runs dozens
// of versions at once, and a bare message search would let any node's build of any
// version stand in for this one.
func messageNeedle(message, kbID string, versionID int64) string {
	return fmt.Sprintf(`%s","kb_id":"%s","version_id":%d,`, message, kbID, versionID)
}

// versionLogLines returns, per storage service, the line carrying this message for
// this (kb, version) — absent from the map for a node that did not log one.
func versionLogLines(t *testing.T, message, kbID string, versionID int64) map[string]string {
	t.Helper()
	needle := messageNeedle(message, kbID, versionID)
	out := make(map[string]string, len(storageServices))
	for _, svc := range storageServices {
		for _, line := range strings.Split(nodeLogsSince(t, svc), "\n") {
			if strings.Contains(line, needle) {
				out[svc] = line
				break
			}
		}
	}
	return out
}

// versionBuildLog returns a storage node's log line recording §8.6(c)'s reuse of
// the parent artifact for this version, or "" when no node logged one.
//
// The needle is scoped to the message and this (kb, version) — the cluster runs
// dozens of other versions, and a bare message search would let any node's build
// of any version stand in for this one.
//
// Whichever replica answers first is enough, because they can legitimately
// disagree: each one reuses ITS OWN local artifact, so a replica that holds a
// different shape of the parent (measured: deleted_chunks 1153 against the other
// two's 7322 for one and the same version) reports its own numbers. The assertion
// only asks that a reuse happened with something deleted.
func versionBuildLog(t *testing.T, kbID string, versionID int64) string {
	t.Helper()
	for _, line := range versionLogLines(t, appendLogMessage, kbID, versionID) {
		return line
	}
	return ""
}

// logIntField reads one numeric field out of a JSON log line.
func logIntField(t *testing.T, line, field string) int64 {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("parsing log line %q: %v", line, err)
	}
	v, ok := rec[field].(float64)
	if !ok {
		t.Fatalf("log line carries no numeric %q field: %s", field, line)
	}
	return int64(v)
}

// awaitVersionBuildLog waits up to timeout for a storage node to log §8.6(c)'s
// reuse of the parent artifact for this version, returning "" on timeout.
//
// Why it waits rather than reading once: WHICH node builds a version is §8.4's
// decision, and §8.6(c) reuses a parent artifact only where this node knows the
// parent's SHAPE — in appendBase's terms, `im.builtGraphFree[kb][parent]`, which
// only a node that built that parent itself has written. A replica whose copy of
// the parent arrived by distribution therefore rebuilds the child from scratch
// (that path logs NOTHING — no "appending", no "rebuilding"), and picks the reuse
// up later, when it catches up with the chain tail on its own.
//
// Measured on the 3+3 cluster with the containers throttled to 0.6 cores each and
// the test process pinned to 4 (the CI runner's shape): the version under test
// was first served from a replica-side rebuild, and `built by appending` appeared
// on two replicas 10–15 s later as they caught up — so a single read right after
// READY was a coin flip, which is exactly how this case failed 3 of 8 runs before.
func awaitVersionBuildLog(t *testing.T, kbID string, versionID int64, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if line := versionBuildLog(t, kbID, versionID); line != "" {
			return line
		}
		if !time.Now().Before(deadline) {
			return ""
		}
		time.Sleep(500 * time.Millisecond)
	}
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
	// Every replica must hold the parent's artifact before the child is written:
	// §8.6(c) can only reuse what is locally there (see awaitReplicaArtifact).
	awaitReplicaArtifact(t, kbID, baseVersion, 120*time.Second)
	baseBytes := versionIndexBytes(t, kbID, baseVersion)
	t.Logf("base version %d READY on every replica; artifact bytes across the storage group: %d", baseVersion, baseBytes)

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
			Op:    pb.ChangeOp_CHANGE_OP_ADD,
			DocId: fmt.Sprintf("late-%06d", i),
			// The content has to be NEW, not a copy of an existing document's.
			//
			// Chunks are content-addressed, so re-adding text the knowledge base
			// already holds produces no new chunk at all — and with nothing to
			// append, §8.6(c) rebuilds instead of reusing the parent (its own
			// comment: "copy the parent, dead vectors and all, is never worth it
			// when nothing new comes along"). A rebuilt artifact carries no
			// tombstones, so §8.6(d) has nothing to collect and this case can never
			// reach the chain it exists to exercise — it skips with "found NO
			// candidate" no matter how it is configured.
			//
			// Measured with the reused text (genUniqueDocs is deterministic, so
			// `added[i].content` duplicates document i): the version's artifact held
			// 2,379 chunks against a parent's 9,590 — exactly the live set, i.e. a
			// full rebuild — and the scan logged dead_share 0.000 with
			// live_docs 501.
			Content: fmt.Sprintf("late-%06d %s", i, added[i].content),
		})
	}
	trimmed := writeChanges(t, ctx, leaderAddr, kbID, baseVersion, changes)
	waitVersionStatus(t, ctx, leaderAddr, kbID, trimmed, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
	// READY is the version's state, not the group's: §8.4's distribution is still
	// in flight when it flips, and measuring before it lands reads a partial set of
	// replicas — measured: 66.7 MB across the group for an artifact that settles at
	// 99.2 MB (two replicas plus a fraction of the third), which then made the
	// post-collection reading look unchanged.
	awaitReplicaArtifact(t, kbID, trimmed, 120*time.Second)
	afterDelete := versionIndexBytes(t, kbID, trimmed)
	t.Logf("version %d READY after deleting %d/%d docs; artifact bytes: %d (parent's clean artifact: %d)",
		trimmed, deleteCount, docCount, afterDelete, baseBytes)

	// §8.6(c)'s reuse is asserted from the LOG LINE, not from the artifact's size,
	// and it is AWAITED, not read once. Both changes come from the same place: the
	// log is the honest evidence, but who writes it and when is decided elsewhere
	// (see awaitVersionBuildLog — a replica that received the parent by
	// distribution rebuilds this version silently first, and only reuses later).
	//
	// Size was the evidence until the tombstones it was measuring turned out to be
	// reclaimable before it was read: a reuse keeps the parent's vectors as
	// TOMBSTONES and is therefore BIGGER than the parent's clean artifact
	// (measured: 99,194,878 against 98,060,247), while a rebuild of the 501 live
	// documents is a fraction of it — but §8.6(d)'s second target source (the chain
	// tail) collects those tombstones within seconds, so the same reading could
	// come back as the POST-collection 74,238,890 and the case then reported
	// "rebuilt, no tombstones" about a version whose tombstones had just been
	// reclaimed.
	//
	// The line is the direct evidence, and the stronger one: it names the parent it
	// appended to and how many chunks it deleted (deleted_chunks = exactly the
	// tombstones §8.6(d) then reclaims).
	reuseLine := awaitVersionBuildLog(t, kbID, trimmed, reuseWait())
	if reuseLine == "" {
		// Skip, not fail. Whether §8.6(c) reuses anything is this case's precondition
		// (no reuse ⇒ no tombstones ⇒ nothing for §8.6(d) to reclaim), not a defect —
		// the same honest-skip outcome the original size-based assertion described
		// ("without them the case below skips"). The shape judge is now the sidecar
		// as well as the in-memory record (appendBase → shapeGraphFree), so this
		// should only fire on a sidecar predating §8.6a or a cluster whose parent
		// artifact never landed here at all.
		t.Skipf("no storage node logged %q for %s v%d within %v, so the version carries no tombstones "+
			"for §8.6(d) to reclaim. The artifact is %d bytes against the parent's clean %d. §8.6(c) "+
			"needs the parent's artifact on this node AND its shape (in-memory for a version this "+
			"node built, otherwise the shape line in its sidecar).",
			appendLogMessage, kbID, trimmed, reuseWait(), afterDelete, baseBytes)
	}
	deletedChunks := logIntField(t, reuseLine, "deleted_chunks")
	if deletedChunks <= 0 {
		// Not a defect: a reuse with nothing deleted leaves no tombstones, so there
		// is nothing for §8.6(d) to reclaim and the case cannot run. That is the same
		// honest-skip outcome the original size-based assertion described ("without
		// them the case below skips").
		t.Skipf("§8.6(c) reused the parent with deleted_chunks=%d, so the artifact carries no tombstones "+
			"for §8.6(d) to reclaim: %s", deletedChunks, reuseLine)
	}

	// Every replica that BUILT this version itself must have reused the parent.
	//
	// §8.4 has already put the parent's artifact — and with it the shape line — on
	// every replica, so a replica logging "built from scratch" is a full rebuild
	// §8.6(c) was supposed to save, i.e. the silent one: before the shape judge
	// read the sidecar, 2 of 3 replicas took that path on every version (see
	// shapeGraphFree). A replica that never builds the version logs neither line —
	// it serves what distribution handed it — so this is asserted per replica, not
	// as a count.
	reuseOn := versionLogLines(t, appendLogMessage, kbID, trimmed)
	scratchOn := versionLogLines(t, builtFromScratchMessage, kbID, trimmed)
	for svc, line := range scratchOn {
		t.Errorf("%s rebuilt %s v%d from scratch while holding the parent's artifact (and its sidecar): "+
			"the reuse §8.6(c) exists for did not trigger here: %s", svc, kbID, trimmed, line)
	}
	t.Logf("§8.6(c) reused the parent on %d/%d storage replicas, built from scratch on %d/%d",
		len(reuseOn), len(storageServices), len(scratchOn), len(storageServices))

	// Make the tombstoned version the ACTIVE one. §8.6(d) scans two sources: the
	// active version, and the knowledge base's chain tail — the tail source exists
	// precisely so a KB that was never rolled back is not left unscanned, and it is
	// how the tombstones above got collected before this point in some runs.
	// Setting the version active names it as a target by the other route as well,
	// which is the design's own description of the case it exists for: "long-lived,
	// continuously queried, no successor in sight".
	//
	// CreateVersion does NOT move the active pointer (only the set-active command
	// does), so without this step the version would be a target only while nothing
	// follows it in the chain — the next version written would drop it from the
	// scan. This missing step — not a faulty candidate judgement — is why this case
	// used to skip.
	if err := rollbackTo(ctx, leaderAddr, kbID, trimmed); err != nil {
		t.Fatalf("RollbackVersion(%d) failed: %v", trimmed, err)
	}
	t.Logf("version %d is now the active version", trimmed)

	// --- Wait for the scanner to collect ---
	deadline := time.Now().Add(stressCollectTimeout())
	var afterCollect int64
	collected := false
	// Scoped to THIS knowledge base and version. "Some collection happened" is not
	// this case's collection: the cluster carries dozens of other active versions,
	// any of whose collections would satisfy a bare "gc: collected" search — the
	// same false signal a bare "caught up" search produced in the lag-catch-up
	// cases (see lag_catchup_test.go).
	collectedLog := fmt.Sprintf(`"index: gc: collected dead vectors","kb_id":"%s","version_id":%d`, kbID, trimmed)
	for time.Now().Before(deadline) {
		if anyStorageLogHas(t, collectedLog) {
			collected = true
			// The log line is written when the collection succeeds; the rewritten
			// artifact lands on disk after it, and tens of megabytes of index take
			// seconds to rebuild and save. Poll for the size to move instead of
			// sleeping a fixed 2 s — measured: with the fixed sleep the case read the
			// artifact BEFORE the rewrite and reported 0.0% reclaimed on a collection
			// that had just dropped 7,322 chunks (verified by hand: 24.7 MB → 19 MB
			// per replica).
			//
			// Two references, because the collection may have landed BEFORE the
			// reading above: afterDelete is then already post-collection and can never
			// shrink, which would park this loop for the whole settle window. The
			// parent's clean artifact still works — the reused one carried its vectors
			// plus tombstones, so it is ≥ the parent, and a collection that reclaimed
			// anything ends up under it.
			settleBy := time.Now().Add(45 * time.Second)
			afterCollect = versionIndexBytes(t, kbID, trimmed)
			for afterCollect >= afterDelete && afterCollect >= baseBytes && time.Now().Before(settleBy) {
				time.Sleep(time.Second)
				afterCollect = versionIndexBytes(t, kbID, trimmed)
			}
			break
		}
		time.Sleep(3 * time.Second)
	}

	if !collected {
		switch {
		case !anyStorageLogHas(t, "index: gc scan read its targets"):
			t.Skip("the §8.6(d) scanner is not running on any storage node — " +
				"check index_manager.gc_sweep_interval_ms (negative disables it) and the data dir")
		case !anyStorageLogHas(t, "cleanup candidate"):
			t.Skipf("the scanner ran but found NO candidate, with the artifact at %d bytes "+
				"against the parent's clean %d. The reuse line above recorded deleted_chunks=%d, so the "+
				"tombstones are there; this points at the threshold (GCRatioThreshold, against the dead "+
				"share §8.6(c) left behind) or at the estimate itself.",
				afterDelete, baseBytes, deletedChunks)
		default:
			t.Skipf("a candidate was found but nothing was collected within %v — check "+
				"index_manager.serving_replica_min (with %d storage replicas, collection "+
				"needs enough left serving) and whether collection is enabled "+
				"(index_manager.gc_enabled)",
				stressCollectTimeout(), len(storageServices))
		}
	}

	t.Logf("COLLECTED: artifact bytes %d → %d, against the parent's clean %d (%d chunks were deleted at reuse, i.e. the tombstones this collection was about)",
		afterDelete, afterCollect, baseBytes, deletedChunks)
	// The artifact must end up smaller than the REUSED one: that is the space coming
	// back. Two references, for the same reason as the settle loop — afterDelete when
	// this run's reading preceded the collection, and the parent's clean artifact when
	// it did not (a reuse is ≥ the parent, so anything under the parent has dropped
	// tombstones). Reading only afterDelete made this assertion fail in exactly the
	// runs where the scanner had already won the race.
	if afterCollect >= afterDelete && afterCollect >= baseBytes {
		t.Errorf("a collection was logged but the version's artifact did not shrink (%d → %d, parent's clean %d); "+
			"the bytes are the version's own .index and .index.ids, so either the log is lying "+
			"or the rewrite did not drop the tombstones",
			afterDelete, afterCollect, baseBytes)
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

	t.Logf("GC-PRESSURE SUMMARY: docs=%d deleted=%d artifactBytes base=%d afterDelete=%d afterCollect=%d",
		docCount, deleteCount, baseBytes, afterDelete, afterCollect)
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
