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

	pb "stratum/api/proto/stratum"
)

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
// node container. The cluster is started by scripts/docker-cluster.sh with
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

// waitVersionStatus polls ListVersions until the version reaches the wanted
// index status (or deadline passes).
func waitVersionStatus(t *testing.T, ctx context.Context, addr, kbID string, versionID int64, want pb.IndexStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, _, _, conn, err := dialNode(addr)
		if err == nil {
			kb := pb.NewKnowledgeBaseServiceClient(conn)
			resp, err := kb.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
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
		b1 := time.Now()
		waitVersionStatus(t, ctx, leaderAddr, kbID, lastVersionID, pb.IndexStatus_INDEX_STATUS_READY, indexBuildTimeout())
		buildDur += time.Since(b1)
	}
	writeTotal := time.Since(writeStart)
	t.Logf("CreateVersion(%d docs, %d batches of %d): write=%v build-accum=%v total=%v",
		docCount, (len(changes)+batch-1)/batch, batch, writeDur, buildDur, writeTotal)

	// --- Sample storage usage ---
	var totalNodeBytes int64
	for _, svc := range nodeServices {
		n := duNodeBytes(t, svc, "/var/lib/stratum/"+strings.TrimPrefix(svc, "stratum-"))
		t.Logf("storage %s: %d bytes (%.2f MiB)", svc, n, float64(n)/(1024*1024))
		totalNodeBytes += n
	}
	t.Logf("storage total (3 nodes): %d bytes (%.2f MiB)", totalNodeBytes, float64(totalNodeBytes)/(1024*1024))
	// Shared vecstore RocksDB lives on the host (host.docker.internal).
	if vs, err := duBytes("/tmp/vecstore_data"); err == nil {
		t.Logf("storage vecstore (host): %d bytes (%.2f MiB)", vs, float64(vs)/(1024*1024))
	}

	// --- Query correctness: the index must return results ---
	_, q, _, qconn, err := dialNode(leaderAddr)
	if err != nil {
		t.Fatalf("dial leader for query: %v", err)
	}
	defer qconn.Close()
	queryResp, err := q.Query(ctx, &pb.QueryRequest{
		KnowledgeBaseId: kbID,
		VersionId:       &lastVersionID,
		Vector:          make([]float32, 768),
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
