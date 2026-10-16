// Package docker_test — Stratum T4 data-volume test.
//
// Fills a 3-node Docker cluster with a configurable number of documents and
// samples the data-volume cost: CreateVersion latency, index-build latency,
// per-node storage usage, and query correctness (see Stratum_测试顺序.md
// T3-4 / T4-4).
//
// Scale via STRATUM_VOLUME_DOCS (default 1000 documents).
//
//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "stratum/api/proto/stratum"
	"stratum/service"
)

// storageDataDir is where storage node i keeps its data inside its container.
// Storage nodes are numbered from 11 (see scripts/cluster.sh --topology two-tier), and
// that number is what the directory is named after.
func storageDataDir(i int) string {
	return fmt.Sprintf("/var/lib/stratum/node%d", 10+i+1)
}

// dataVolumeDocs is the number of documents to write. Override with
// STRATUM_VOLUME_DOCS (e.g. 10000) for a heavier run.
func dataVolumeDocs() int {
	if v := os.Getenv("STRATUM_VOLUME_DOCS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1000
}

// dataVolumeTimeout is the overall deadline for the whole test (write +
// index build + sampling). It scales with the document count — at 1000 docs
// a 15-minute budget is ample, while 100000 docs need much longer. Override
// with STRATUM_VOLUME_TIMEOUT (a Go duration, e.g. "60m").
func dataVolumeTimeout() time.Duration {
	if v := os.Getenv("STRATUM_VOLUME_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 15 * time.Minute
}

// indexBuildTimeout is how long we wait for the async index build to reach
// READY. Override with STRATUM_INDEX_BUILD_TIMEOUT (a Go duration).
func indexBuildTimeout() time.Duration {
	if v := os.Getenv("STRATUM_INDEX_BUILD_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 10 * time.Minute
}

// dataVolumeBatch is how many documents go into one CreateVersion call.
// A single request must stay under the 4 MiB gRPC message limit (~1400 docs
// at ~2.8 KB/doc), so large volumes are written as a chain of batches, each
// version inheriting the previous one. Override with STRATUM_VOLUME_BATCH.
func dataVolumeBatch() int {
	if v := os.Getenv("STRATUM_VOLUME_BATCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1000
}

// docUnit is a (doc_id, content) pair used to populate a version.
type docUnit struct {
	id      string
	content string
}

// genDataVolumeDocs generates n documents of deterministic Chinese prose,
// each roughly targetRunes runes long (≈1-2 chunks at window=512).
func genDataVolumeDocs(n, targetRunes int) []docUnit {
	sentences := []string{
		"向量检索服务在十亿级规模下依然保持亚秒级查询延迟，并通过分层缓存进一步降低尾延迟。",
		"RAG 流水线先对文档进行滑动窗口分块，再调用嵌入模型生成稠密向量并写入 HNSW 索引。",
		"多版本并发控制允许同一知识库同时维护多个索引版本，用于灰度发布与 A/B 对比实验。",
		"写入协调器通过两阶段提交与预写日志保证崩溃后数据一致，重启后可自动续传未完成版本。",
		"查询服务按相似度降序返回结果，并对同一文档的多个命中分块做中位数聚合去重。",
		"索引管理器采用 LRU 缓存与引用计数，冷版本按需加载，超出内存阈值时自动驱逐。",
	}
	docs := make([]docUnit, n)
	for i := 0; i < n; i++ {
		var sb strings.Builder
		for j := 0; ; j++ {
			sb.WriteString(sentences[(i+j)%len(sentences)])
			if len([]rune(sb.String())) >= targetRunes {
				break
			}
		}
		docs[i] = docUnit{
			id:      fmt.Sprintf("doc-%06d", i),
			content: sb.String(),
		}
	}
	return docs
}

// duBytes returns the recursive byte size of a path on the host filesystem
// (used for the shared vecstore RocksDB directory).
func duBytes(path string) (int64, error) {
	out, err := exec.Command("du", "-sb", path).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("du %s: %w\n%s", path, err, out)
	}
	fields := strings.Fields(string(out))
	if len(fields) < 1 {
		return 0, fmt.Errorf("du %s: unexpected output %q", path, out)
	}
	return strconv.ParseInt(fields[0], 10, 64)
}

// duNodeBytes returns the recursive byte size of a path inside a running
// node container. The cluster is started by scripts/cluster.sh --topology single with
// plain `docker run` (no compose project), so plain `docker exec` is used.
func duNodeBytes(t *testing.T, service, path string) int64 {
	t.Helper()
	out, err := exec.Command("docker", "exec", service, "du", "-sb", path).CombinedOutput()
	if err != nil {
		t.Fatalf("du in %s: %v\n%s", service, err, out)
	}
	fields := strings.Fields(string(out))
	if len(fields) < 1 {
		t.Fatalf("du in %s: unexpected output %q", service, out)
	}
	n, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		t.Fatalf("du in %s: parse %q: %v", service, fields[0], err)
	}
	return n
}

// awaitDataDurable blocks until the version's data has settled on the cluster.
//
// It is the precondition for asking for a build: a build that starts before the
// records land produces an empty index, and nothing re-runs it.
func awaitDataDurable(t *testing.T, ctx context.Context, addr, kbID string, versionID int64, timeout time.Duration) {
	t.Helper()
	_, _, _, conn, err := dialNode(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	kb := pb.NewKnowledgeBaseServiceClient(conn)
	// Its own deadline, not the caller's: this helper waits for a convergence the
	// test is deliberately slow to reach (holders_fallback exists precisely
	// because a version's data can take a long time to arrive), and inheriting a
	// tighter caller context turned that wait into a DeadlineExceeded instead of a
	// plain "not yet".
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	deadline := time.Now().Add(timeout)
	for {
		// A short window per call, re-asked in a loop: AwaitVersion's contract is
		// that running out of window is an ANSWER, not an error ("not yet" is not a
		// failure), so one call returning DATA_PENDING says nothing about whether
		// the data will settle.
		// A per-call deadline, independent of the loop's: WaitTimeoutMs is the
		// window the server promises to CLOSE, and a window the server fails to
		// close must not be able to stall this loop. Measured on the two-tier
		// suite: one AwaitVersion did not return for six minutes, which pinned this
		// case until go test's own timeout fired — a panic, not a readable failure.
		// The loop's own deadline stays what reports the outcome.
		callCtx, callCancel := context.WithTimeout(waitCtx, 8*time.Second)
		resp, err := kb.AwaitVersion(callCtx, &pb.AwaitVersionRequest{
			KnowledgeBaseId: kbID,
			VersionId:       versionID,
			Target:          pb.AwaitTarget_AWAIT_TARGET_DATA_DURABLE,
			WaitTimeoutMs:   5000,
		})
		callCancel()
		if err != nil {
			// Running out of our own window is the same answer as "not yet" — the
			// loop's deadline is what decides, so poll on rather than failing here.
			if st, ok := status.FromError(err); ok && st.Code() == codes.DeadlineExceeded && time.Now().Before(deadline) {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			t.Fatalf("AwaitVersion(DATA_DURABLE) for %s v%d on %s: %v", kbID, versionID, addr, err)
		}
		switch resp.GetStage() {
		case service.StageDataDurable, service.StageIndexReady:
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("version %d never settled its data within %s (last stage %s) before its index was requested",
				versionID, timeout, resp.GetStage())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitVersionStatus polls ListVersions until the version reaches the wanted
// index status (or deadline passes).
func waitVersionStatus(t *testing.T, ctx context.Context, addr, kbID string, versionID int64, want pb.IndexStatus, timeout time.Duration) {
	t.Helper()
	// §8.6(b): a version is built when something asks for it, not when it is
	// written. Only the ACTIVE version is built eagerly, and every caller of this
	// helper is waiting for a version a test just created — i.e. not the active
	// one. So the ask belongs here, in the ONE place all fourteen call sites
	// funnel through, rather than repeated at each of them: this is the same class
	// of change as the build trigger itself, and it is worth keeping in one spot.
	//
	// It is not a test-only concession. WarmupVersion is the real request an
	// operator makes ("keep this version ready"), and a query needing the version
	// makes the same thing happen implicitly.
	//
	// It goes to a STORAGE node rather than to `addr`, which is often a control
	// node: the ask is implemented by the index manager, and a control node has
	// none (its AdminService answers only the cluster questions it can). Any
	// storage node will do — the request names the version, and the node that
	// holds its data builds it.
	if want == pb.IndexStatus_INDEX_STATUS_READY {
		// Wait for the DATA first, and this ordering is not cosmetic: a build
		// triggered before the version's records land builds an EMPTY index
		// (measured: size_bytes=0, 18 s before DATA_PRESENT), and nothing rebuilds
		// it afterwards — the version then reports READY with an artifact that
		// cannot answer a query, and every artifact assertion below fails.
		//
		// AwaitVersion is exactly the right wait here: it settles on durable data,
		// which is the precondition a build needs.
		awaitDataDurable(t, ctx, addr, kbID, versionID, timeout)
	}
	deadline := time.Now().Add(timeout)
	for {
		if want == pb.IndexStatus_INDEX_STATUS_READY {
			// ASK on every round, not once up front, and ask by QUERYING.
			//
			// A query goes through the station, which routes it to the version's
			// index owners (internal/router, docs/index-owner-sharding-plan.md §5);
			// that node builds the index and the replicated metadata turns READY.
			//
			// Repeated because ONE query is not a guarantee: the router narrows to
			// owner ∩ holders, and a round where neither the owner set nor the
			// routing table has caught up answers from an empty candidate list
			// without ever reaching a node that would build (measured: a version sat
			// DATA_PRESENT on all three replicas while every query returned
			// "candidates: 0"). Asking again next round is what makes it converge;
			// triggerBuild de-duplicates, so the extra asks are cheap.
			//
			// Warming a specific node up instead was wrong twice over: it built an
			// EMPTY index on a node that did not hold the version's data, and on a
			// degraded storage group it failed outright ("all nodes unavailable").
			// The error is ignored on purpose — the query is the request, and its
			// answer is not what this helper waits for.
			_, _ = serveFrom(ctx, addr, kbID, versionID)
		}
		_, _, _, conn, err := dialNode(addr)
		if err == nil {
			kb := pb.NewKnowledgeBaseServiceClient(conn)
			// Bounded like the metadata reads in the other helpers: ListVersions
			// answers from replicated state, so a call that does not return soon
			// means the node is wedged rather than slow — and the loop's own
			// deadline must stay the thing that decides the outcome. (The query
			// above deliberately keeps the caller's budget: a first query may
			// legitimately wait for a build.)
			readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
			resp, err := kb.ListVersions(readCtx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
			readCancel()
			conn.Close()
			if err == nil {
				for _, v := range resp.Versions {
					if v.VersionId == versionID {
						if v.IndexStatus == want {
							return
						}
						if v.IndexStatus == pb.IndexStatus_INDEX_STATUS_FAILED {
							t.Fatalf("version %d index build FAILED", versionID)
						}
						break
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for version %d to reach %s", versionID, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestT4_DataVolume fills the cluster with documents and samples volume cost.
func TestT4_DataVolume(t *testing.T) {
	docCount := dataVolumeDocs()
	ctx, cancel := context.WithTimeout(context.Background(), dataVolumeTimeout())
	defer cancel()

	leaderIdx, kbID := waitForLeader(t, ctx, "datavolume", 30*time.Second)
	leaderAddr := nodeAddrs[leaderIdx]
	kbID = measurementKB(t, ctx, leaderAddr, "datavolume", kbID)
	t.Logf("leader is node %d (%s), KB %s, writing %d documents", leaderIdx, leaderAddr, kbID, docCount)

	_, _, _, conn, err := dialNode(leaderAddr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer conn.Close()
	kb := pb.NewKnowledgeBaseServiceClient(conn)

	// --- Write the documents as a chain of CreateVersion batches ---
	// A single CreateVersion request must stay under the gRPC 4 MiB message
	// limit, so large volumes are split into batches; each batch becomes a
	// version inheriting the previous one.
	docs := genDataVolumeDocs(docCount, 1000)
	changes := make([]*pb.DocChange, len(docs))
	for i, d := range docs {
		changes[i] = &pb.DocChange{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   d.id,
			Content: d.content,
		}
	}

	batch := dataVolumeBatch()
	writeStart := time.Now()
	parentVersion := int64(0)
	var lastVersionID int64
	writeDur := time.Duration(0)
	buildDur := time.Duration(0)
	for offset := 0; offset < len(changes); offset += batch {
		end := offset + batch
		if end > len(changes) {
			end = len(changes)
		}
		// A parent version must not be PENDING (index build pending), so each
		// batch is only chained after the previous version reaches READY.
		b0 := time.Now()
		createResp, err := kb.CreateVersion(ctx, &pb.CreateVersionRequest{
			KnowledgeBaseId: kbID,
			ParentVersionId: parentVersion,
			Changes:         changes[offset:end],
		})
		writeDur += time.Since(b0)
		if err != nil {
			t.Fatalf("CreateVersion(batch %d-%d / %d docs) failed: %v", offset, end, docCount, err)
		}
		parentVersion = createResp.VersionId
		lastVersionID = createResp.VersionId
		// The build is asked for inside waitVersionStatus (§8.6b); what is measured
		// here is how long it takes once asked.
		b1 := time.Now()
		waitVersionStatus(t, ctx, leaderAddr, kbID, lastVersionID, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
		buildDur += time.Since(b1)
	}
	writeTotal := time.Since(writeStart)
	t.Logf("CreateVersion(%d docs, %d batches of %d): write=%v build-accum=%v total=%v",
		docCount, (len(changes)+batch-1)/batch, batch, writeDur, buildDur, writeTotal)

	// --- Sample storage usage ---
	//
	// Sampled on the storage tier, because that is where documents, chunks and
	// indexes live — under the split topology the control containers hold no
	// data at all, so measuring them would report zero and mean nothing. The
	// directory is keyed by node_id, which the container name no longer spells
	// out. The vecstore's RocksDB sits in the same container, so its bytes are
	// already inside these numbers.
	var totalNodeBytes int64
	for i, svc := range storageServices {
		n := duNodeBytes(t, svc, storageDataDir(i))
		t.Logf("storage %s: %d bytes (%.2f MiB)", svc, n, float64(n)/(1024*1024))
		totalNodeBytes += n
	}
	t.Logf("storage total (%d nodes): %d bytes (%.2f MiB)",
		len(storageServices), totalNodeBytes, float64(totalNodeBytes)/(1024*1024))

	// --- Query correctness: the index must return results ---
	// Reads are served by the storage tier: QueryService reads the index manager
	// and the local stores directly, so a control node has nothing to answer
	// with. Asking a storage node also checks the data actually got there — the
	// version was committed by the control tier and dispatched here.
	_, q, _, qconn, err := dialNode(storageAddrs[0])
	if err != nil {
		t.Fatalf("dial leader for query: %v", err)
	}
	defer qconn.Close()
	queryResp, err := q.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &lastVersionID,
		Vector:          queryVector(768),
		TopK:            10,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(queryResp.Results) == 0 {
		t.Errorf("Query on %d-document version returned 0 results", docCount)
	} else {
		t.Logf("Query returned %d results (top-k=10)", len(queryResp.Results))
	}

	t.Logf("DATA-VOLUME SUMMARY: docs=%d write=%v build=%v nodeBytes=%d",
		docCount, writeDur, buildDur, totalNodeBytes)
}
