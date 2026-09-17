package integration_test

// quantizer_e2e_test.go — 量化器在真实链路上的证明。
//
// 在它之前，量化只被两处 mock 单测碰过：internal/index/quantizer_build_test.go
// （断言 Build RPC 的字段转发）与 service/quantizer_mapping_test.go（断言枚举
// 映射）。`grep -i quantiz integration/` 零命中——也就是说 v13 的完成标准
// 「CreateKnowledgeBase(quantizer=…) → CreateVersion → Query 全链路通过」
// 从来没在真实 vecstore 上跑过：量化器功能一直是关着的，没人知道它开了会怎样。
//
// 本用例把 KB 级量化器真正打开，分别证明三件事：
//  1. 元数据：量化器（含 PQ 的 m/nbits）创建时落库，GetKnowledgeBase 原样回读；
//  2. 可用性：量化 KB 走完写入 → 索引 READY → 查询命中正确的最近邻；
//  3. 真的生效：同一份数据、同一维度下，SQ 量化版本的索引产物显著小于 OFF
//     版本——否则「打开了量化」只是个元数据字段，产物一个字节都没变。
//
// 断言 3 是这组用例的重点：它把「Build RPC 里带了 quantizer」与「vecstore 真的
// 按量化形态建了索引」区分开。过期的 vecstore_server 二进制（不认识 QUANTIZER_*）
// 会在这一条上失败，而不是静默地建出全精度索引。

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// quantizerTestDim 是这些用例的向量维度。取得比默认嵌向量的玩具维度大，是为了
// 让「每维量化后的字节数」在产物里占主导：维度太小的话 HNSW 图边开销会盖过
// 向量载荷，量化省下的那部分就看不出来了。
const quantizerTestDim = 128

// quantizerVariant 描述「开哪一种量化」。
type quantizerVariant struct {
	name      string
	enum      string // 控制台/API 面的枚举名，日志用
	pqM       int32  // 仅 PQ：子向量数
	pqNBits   int32  // 仅 PQ：每个子向量的位宽
	wantProto pb.QuantizerType
}

// quantizerVariants 是要覆盖的形态。SQ8 / SQ_FP16 逐维压缩（分别 1 / 2 字节每维），
// 因此「SQ8 产物 < FP16 产物 < 全精度产物」是量化生效的硬证据；PQ 是子空间码本，
// 在这么小的数据集上码本的固定开销与向量载荷同量级，字节数不再是干净的判据，
// 所以它只参与「可用 + 元数据」两条断言。
func quantizerVariants() []quantizerVariant {
	return []quantizerVariant{
		{name: "off", enum: "QUANTIZER_OFF", wantProto: pb.QuantizerType_QUANTIZER_OFF},
		{name: "sq8", enum: "QUANTIZER_SQ8", wantProto: pb.QuantizerType_QUANTIZER_SQ8},
		{name: "sq_fp16", enum: "QUANTIZER_SQ_FP16", wantProto: pb.QuantizerType_QUANTIZER_SQ_FP16},
		// PQ 需要一次 train：m=4 / nbits=4 意味着每个子空间 2^4 = 16 个中心。
		// 实测：样本数（下面的 40 篇）远少于 faiss 建议的 39×ksub 时，它只打
		// 一条 WARNING 就照常训练完——构建不会落到 FAILED，但码本质量差、召回
		// 会变弱，所以 PQ 在这里只参与「可用 + 元数据」两条断言（下面第 3 条
		// 的字节比较不覆盖它）。
		//
		// 真实栈实测（start.sh 单机 + REST，mock-embed dim=768，40 篇文档）：用
		// 控制台的默认 PQ（m=96 / nbits=8）时构建直接失败、版本停在 PENDING，
		// 错误只有 `Build RPC: rpc error: code = Unknown desc = Unexpected error
		// in RPC handling`——faiss 训练抛出的异常穿过了 gRPC 边界，把「训练样本
		// 不足」这个真正的原因丢了。下面的 m=4/nbits=4 把 ksub 压到 16 才能训出
		// 来；m=96 在 dim=128 下还除不尽，本身也必须显式给。
		{name: "pq", enum: "QUANTIZER_PQ", pqM: 4, pqNBits: 4, wantProto: pb.QuantizerType_QUANTIZER_PQ},
	}
}

// createKBWithQuantizer 是 createTestKB 的量化版：除了名字与 embed 配置，还带上
// KB 级量化器（以及 PQ 的 m/nbits），并照旧把测试要 fork 的空 READY 根版本建出来。
func (n *realNode) createKBWithQuantizer(ctx context.Context, name string, v quantizerVariant) (string, int64) {
	n.t.Helper()
	resp, err := n.KB.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             name,
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig: &pb.EmbedConfig{
			ServiceAddr: "mock-embed",
			ModelId:     "m1",
		},
		Quantizer: v.wantProto,
		PqM:       v.pqM,
		PqNbits:   v.pqNBits,
	})
	if err != nil {
		n.t.Fatalf("node %d: CreateKnowledgeBase(%s): %v", n.nodeID, v.name, err)
	}
	v1 := seedRootVersion(n.t, n.raftNode, ctx, resp.KnowledgeBaseId)
	return resp.KnowledgeBaseId, v1
}

// indexArtifactSize 返回某版本在本地磁盘上的索引产物字节数。
// 布局是 <IndexDataDir>/index/<kbID>/<versionID>.index（internal/index/impl.go
// 的 indexFilePath），而 realNode 把 IndexDataDir 设在 <baseDir>/indexdata
// （integration/e2e_test.go 的 newRealNodeWithAddrsAndDirOpts）。这里只数
// Faiss 产物，不含 .index.ids / .index.mem / .index.used 这些旁车。
func indexArtifactSize(t *testing.T, n *realNode, kbID string, versionID int64) int64 {
	t.Helper()
	p := filepath.Join(n.baseDir, "indexdata", "index", kbID, fmt.Sprintf("%d.index", versionID))
	st, err := os.Stat(p)
	if err != nil {
		t.Fatalf("index artifact %s: %v", p, err)
	}
	return st.Size()
}

// TestRealStack_QuantizerEnabledEndToEnd 把四种量化形态各开一个 KB，写完同一份
// 数据后断言：量化器落库可见、查询命中、SQ 形态的产物字节数按压缩比排序。
func TestRealStack_QuantizerEnabledEndToEnd(t *testing.T) {
	vecAddr := startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, quantizerTestDim)

	node := newRealNode(t, 1, nil, vecAddr, embedURL)
	leader := waitForLeader(t, node)
	ctx := context.Background()

	const docs = 40
	contents := make([]string, docs)
	changes := make([]*pb.DocChange, docs)
	for i := range contents {
		contents[i] = fmt.Sprintf("doc-%d content", i)
		changes[i] = &pb.DocChange{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   fmt.Sprintf("doc-%d", i),
			Content: contents[i],
		}
	}

	sizes := make(map[string]int64, len(quantizerVariants()))

	for _, v := range quantizerVariants() {
		t.Run(v.name, func(t *testing.T) {
			kbID, v1 := leader.createKBWithQuantizer(ctx, "q-"+v.name, v)

			// --- 1. 元数据：量化器必须原样落库、原样回读 ---
			kb, err := leader.KB.GetKnowledgeBase(ctx, &pb.GetKnowledgeBaseRequest{KnowledgeBaseId: kbID})
			if err != nil {
				t.Fatalf("GetKnowledgeBase(%s): %v", kbID, err)
			}
			info := kb.KnowledgeBase
			if info.Quantizer != v.wantProto {
				t.Fatalf("%s: 量化器回读 = %v, want %v", v.enum, info.Quantizer, v.wantProto)
			}
			if info.PqM != v.pqM || info.PqNbits != v.pqNBits {
				t.Errorf("%s: PQ 参数回读 = (m=%d, nbits=%d), want (m=%d, nbits=%d)",
					v.enum, info.PqM, info.PqNbits, v.pqM, v.pqNBits)
			}

			// --- 2. 可用性：写入 → 索引 READY → 查询命中 ---
			resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
				KnowledgeBaseId: kbID,
				ParentVersionId: v1,
				Changes:         changes,
			})
			if err != nil {
				t.Fatalf("%s: CreateVersion: %v", v.enum, err)
			}
			v2 := resp.VersionId
			leader.waitVersionReady(ctx, kbID, v2)

			for _, wantDoc := range []string{"doc-0", "doc-17", "doc-39"} {
				vec := contentVector(contents[docIndex(t, wantDoc, docs)], quantizerTestDim)
				res := leader.query(ctx, kbID, v2, vec, 10)
				if r := findResult(res, wantDoc); r == nil {
					t.Fatalf("%s: 查询 %s 未命中；topK=%d 结果 %+v", v.enum, wantDoc, 10, res)
				}
			}

			// --- 3. 量化真的生效：产物字节数 ---
			size := indexArtifactSize(t, leader, kbID, v2)
			sizes[v.name] = size
			t.Logf("%s: v%d 索引产物 %d 字节（%d 个 chunk，dim=%d）",
				v.enum, v2, size, docs, quantizerTestDim)
		})
	}

	// 逐维压缩的两种 SQ 必须比全精度小，且 SQ8（1 字节/维）必须比 FP16
	// （2 字节/维）更小。任何一个不成立都说明量化没有作用到产物上——最典型的
	// 原因是一个不认识 QUANTIZER_* 的过期 vecstore_server 静默建了全精度索引。
	off, sq8, fp16 := sizes["off"], sizes["sq8"], sizes["sq_fp16"]
	if off == 0 || sq8 == 0 || fp16 == 0 {
		t.Fatalf("产物字节数缺失：off=%d sq8=%d fp16=%d", off, sq8, fp16)
	}
	if sq8 >= off {
		t.Errorf("SQ8 产物 %d 字节不小于全精度 %d 字节——量化没有作用到产物上", sq8, off)
	}
	if fp16 >= off {
		t.Errorf("SQ_FP16 产物 %d 字节不小于全精度 %d 字节——量化没有作用到产物上", fp16, off)
	}
	if sq8 >= fp16 {
		t.Errorf("SQ8 产物 %d 字节不小于 SQ_FP16 的 %d 字节（1 字节/维 vs 2 字节/维）", sq8, fp16)
	}
	if pq := sizes["pq"]; pq == 0 {
		t.Error("PQ 形态没有产出索引产物")
	}
}

// docIndex 把 "doc-17" 解析回下标，避免测试里写字面量下标和在内容表之间漂移。
func docIndex(t *testing.T, docID string, docs int) int {
	t.Helper()
	var i int
	if _, err := fmt.Sscanf(docID, "doc-%d", &i); err != nil {
		t.Fatalf("解析 doc ID %q: %v", docID, err)
	}
	if i < 0 || i >= docs {
		t.Fatalf("doc ID %q 越界（共 %d 篇）", docID, docs)
	}
	return i
}

// TestRealStack_QuantizerIsVisibleInSystemStatus 是「打开量化」的运维侧证据：
// 控制台/系统状态必须能看出一个 KB 开了哪种量化，否则运维无法判断某个库是
// 全精度还是量化形态（v13 §3.3 的内存记账口径也依赖这一点）。
func TestRealStack_QuantizerIsVisibleInSystemStatus(t *testing.T) {
	vecAddr := startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, quantizerTestDim)

	node := newRealNode(t, 1, nil, vecAddr, embedURL)
	leader := waitForLeader(t, node)
	ctx := context.Background()

	v := quantizerVariant{name: "sq8", enum: "QUANTIZER_SQ8", wantProto: pb.QuantizerType_QUANTIZER_SQ8}
	kbID, _ := leader.createKBWithQuantizer(ctx, "q-status", v)

	resp, err := leader.KB.ListKnowledgeBases(ctx, &pb.ListKnowledgeBasesRequest{})
	if err != nil {
		t.Fatalf("ListKnowledgeBases: %v", err)
	}
	for _, kb := range resp.KnowledgeBases {
		if kb.KnowledgeBaseId != kbID {
			continue
		}
		if kb.Quantizer != v.wantProto {
			t.Fatalf("列表里的量化器 = %v, want %v", kb.Quantizer, v.wantProto)
		}
		if !strings.Contains(kb.KnowledgeBaseId, "q-status") {
			t.Logf("KB ID 由名字派生：%s", kb.KnowledgeBaseId)
		}
		return
	}
	t.Fatalf("新建的量化 KB %s 没有出现在列表里", kbID)
}

// TestRealStack_PQTrainingShortfallFailsReadably 把真实栈推到「PQ 码本训不出来」
// 这个场景：m=8 / nbits=8 意味着每个子空间 256 个中心，而这一版只有 40 个 chunk。
//
// 它钉的是修复后的两条行为，而不是错误字符串本身（字符串由 C++ 侧的
// PQTrainingShortfallIsReportedNotThrown 断言）：
//  1. 版本落到 FAILED，而不是在 PENDING 上耗着。faiss 对训练样本不足的拒绝是确定性的
//     ——重试多少次都还是同一批数据——所以 Go 的重试窗（buildRetryTimeout，5 分钟）
//     不该在它上面空转；修复前 vecstore 抛出的异常穿过 gRPC 边界变成
//     `Unknown: Unexpected error in RPC handling`，构建结果与原因一起丢了。
//  2. 索引绝不变成 READY：一个没有索引的版本不能对外声称可查。
//
// 注意这里刻意用能整除 dim 的 pq_m（dim=128 时 8 整除、96 不整除）。m=96 是控制台
// 默认值，它在 dim=768 下是另一个独立的确定性失败（dim % pq_m != 0），报错同样是
// 可读的 InvalidArgument，但那条路走的是参数校验分支，钉不住「训练下限」这条。
func TestRealStack_PQTrainingShortfallFailsReadably(t *testing.T) {
	vecAddr := startVecstoreServerForTest(t)
	embedURL := startMockEmbedForTest(t, quantizerTestDim)

	node := newRealNode(t, 1, nil, vecAddr, embedURL)
	leader := waitForLeader(t, node)
	ctx := context.Background()

	v := quantizerVariant{
		name: "pq-training-shortfall", enum: "QUANTIZER_PQ", pqM: 8, pqNBits: 8,
		wantProto: pb.QuantizerType_QUANTIZER_PQ,
	}
	kbID, v1 := leader.createKBWithQuantizer(ctx, "q-pq-shortfall", v)

	changes := make([]*pb.DocChange, 40)
	for i := range changes {
		changes[i] = &pb.DocChange{
			Op:      pb.ChangeOp_CHANGE_OP_ADD,
			DocId:   fmt.Sprintf("doc-%d", i),
			Content: fmt.Sprintf("doc-%d content", i),
		}
	}
	resp, err := leader.KB.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: kbID,
		ParentVersionId: v1,
		Changes:         changes,
	})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	vid := resp.VersionId

	deadline := time.Now().Add(90 * time.Second)
	last := pb.IndexStatus_INDEX_STATUS_PENDING
	for time.Now().Before(deadline) {
		last = versionIndexStatus(t, leader, ctx, kbID, vid)
		switch last {
		case pb.IndexStatus_INDEX_STATUS_FAILED, pb.IndexStatus_INDEX_STATUS_FAILED_PERMANENT:
			return // 可读的失败：版本落到终态，不再空转
		case pb.IndexStatus_INDEX_STATUS_READY:
			t.Fatalf("训练样本不足却建出了索引：v%d 变成了 READY", vid)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("v%d 在 90s 内没有离开 PENDING（最后状态 %s）——训练不足是确定性失败，"+
		"不该被重试窗拖着", vid, last)
}

// versionIndexStatus 读某版本当前的索引状态（读不到时当作 PENDING）。
func versionIndexStatus(t *testing.T, n *realNode, ctx context.Context, kbID string, versionID int64) pb.IndexStatus {
	t.Helper()
	resp, err := n.KB.ListVersions(ctx, &pb.ListVersionsRequest{KnowledgeBaseId: kbID})
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	for _, ver := range resp.Versions {
		if ver.VersionId == versionID {
			return ver.IndexStatus
		}
	}
	return pb.IndexStatus_INDEX_STATUS_PENDING
}
