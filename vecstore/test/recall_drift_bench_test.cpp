// recall_drift_bench_test.cpp — 码本漂移的召回代价（docs/codebook-refresh-plan.md §6）。
//
// 问题：码本只在"全量重建"时训练一次（hnsw_index.cpp:340+ 的 !is_trained 分支），
// 而"只 append、不删除"的库几乎不会走到全量重建 ⇒ 新向量一直用旧码本编码，量化
// 误差随分布漂移上升，粗筛的候选覆盖变差。排序由全精度 rerank 决定（README:17），
// 所以查询不报错、不超时、看起来正确，只是"本该进候选的近邻在 stage 1 就漏掉了"。
//
// 本基准直接量它 —— 同一份数据（base + delta），两种构建方式：
//
//   A「渐进」 Build(base) → AddChunks(delta)   码本只训一次，delta 用旧码本编码
//                                               （现状：§8.6(c) 追加复用）
//   B「重训」 Build(base + delta)              码本重训（全量重建之后的现实）
//
// ⚠️ 两者的差别**不止码本**：Build(base)+AddChunks(delta) 与 Build(all) 的 HNSW
// **图结构**也不同（插入顺序不同 ⇒ 图质量不同）。所以差不能直接当作漂移代价，
// 必须用**基线减法**：
//
//   spread = 1.0（delta 与 base 同分布 ⇒ 码本没有可漂移的东西）
//       ⇒ 这一格的 drift_cost 只剩图结构差异 ⇒ 它**就是基线**
//   spread > 1.0 的 drift_cost − 基线 = 码本漂移的**净代价**
//
// 第一次跑（kBaseN=6000、candidate_n 固定 80）同时踩到两个坑：免训练的 SQ_FP16
// 也报出 +4.1% 的差（它没有码本可漂移 ⇒ 那只能来自图），而 K=100 的差被
// candidate_n=80 顶死在 0.8 的天花板上。现在 candidate_n 随 k 缩放，且读数必须
// 先看 spread=1.0 那一格的基线。
//
// ground truth 用**暴力全精度 cosine**，而不是 kOff-HNSW：HNSW 图本身是近似的，
// 用它当 GT 会把图损失混进"漂移代价"。同时报 kOff-HNSW 的 recall，作为"无量化时
// 能到哪"的参照上限。
//
// 对照假设（每题都要能被本基准证伪）：
//   1. SQ8 / PQ：净代价随 spread（漂移幅度）与 ratio（累积新增比例）上升。
//   2. SQ_FP16：免训练（§5），净代价 ≈ 0 ⇒ 逃生舱口成立。
//   3. spread=1.0 的基线明显小于 spread=2.0 的 drift_cost，否则说明整个信号都来自
//      图结构，与码本无关（那就该先去修 append 的图质量，而不是刷码本）。
//
// DISABLED_ 默认，不进 ctest。运行：
//   ./vecstore_tests --gtest_filter='RecallDriftBenchmark.*' \
//       --gtest_also_run_disabled_tests
#include "vecstore/src/hnsw_index.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexScalarQuantizer.h"
#include "gtest/gtest.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/include/types.h"
#include "vecstore/src/rocksdb_storage.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// 规模与检索参数（与 latency_bench_test.cpp 对齐，便于横向比较）
// ---------------------------------------------------------------------------
constexpr int kDim = 64;
constexpr int kClusters = 24;  // 语料的"主题"数
// kBaseN 取 12000 而不是 latency_bench 的 6000：PQ(pq_m=16) 的 k-means 需要
// 39 × 2^8 = 9984 个训练样本才不发 "please provide at least ... training
// points" 警告，样本不足的码本会让"漂移代价"糊在量化误差里，分不清归因。
constexpr int kBaseN = 12000;  // 初始版本 —— 训练码本的那一版
constexpr int kQueries = 100;  // 一半来自 base，一半来自 delta
// 检索档位。candidate_n 按 top_k 的固定倍率算（产品默认策略 ceil(top_k × 8)）。
// **必须随 k 缩放**：若把 candidate_n 固定成 80 而 k=100，rerank 只能从 80 个候选
// 里挑 100 个，recall@100 会被候选池大小顶死在 0.8 —— 那测到的是池子不是漂移。
const std::vector<int> kRetrievalKs = {10, 100};
constexpr int kCandidateMultiple = 8;
constexpr int kGtK = 100;  // ground truth 的宽度：K=10 取其前 10 即可

struct Item {
  std::string id;
  std::vector<float> vec;
};

// MakeClusterItems 造 n 个向量：kClusters 个中心 + 高斯噪声。
//
// center_scale 放大中心的取值范围，它就是"分布漂移"的旋钮：base 用 1.0，
// delta 用更大的值表示新data落在训练分布之外 —— 对 SQ8 是超出训练时估出的
// per-dim range（⇒ 截断），对 PQ 是落在 k-means centroid 的空档（⇒ 码字失配）。
std::vector<Item> MakeClusterItems(const std::string& prefix, int n,
                                   float center_scale, std::mt19937* rng) {
  std::normal_distribution<float> noise(0.f, 1.f);
  std::uniform_real_distribution<float> center(-8.f * center_scale,
                                               8.f * center_scale);
  std::vector<std::vector<float>> centers(kClusters, std::vector<float>(kDim));
  for (auto& c : centers) {
    for (auto& x : c) x = center(*rng);
  }
  std::vector<Item> items;
  items.reserve(n);
  for (int i = 0; i < n; ++i) {
    const auto& c = centers[static_cast<size_t>(i) % centers.size()];
    Item it;
    it.id = prefix + "-" + std::to_string(i);
    it.vec.resize(kDim);
    for (int d = 0; d < kDim; ++d) it.vec[d] = c[d] + noise(*rng) * 0.3f;
    items.push_back(std::move(it));
  }
  return items;
}

std::vector<ChunkVector> ToChunkVectors(const std::vector<Item>& items) {
  std::vector<ChunkVector> out;
  out.reserve(items.size());
  for (const auto& it : items) out.push_back(ChunkVector{it.id, it.vec});
  return out;
}

std::vector<float> Normalized(const std::vector<float>& v) {
  double norm = 0;
  for (float x : v) norm += static_cast<double>(x) * static_cast<double>(x);
  norm = std::sqrt(norm);
  std::vector<float> out = v;
  if (norm > 0) {
    for (float& x : out) x = static_cast<float>(static_cast<double>(x) / norm);
  }
  return out;
}

// BruteForceTopK 是精确的 ground truth：全量向量上的全精度 cosine top-K。
std::vector<std::string> BruteForceTopK(
    const std::vector<std::vector<float>>& corpus_norm,
    const std::vector<std::string>& corpus_ids,
    const std::vector<float>& query, int k) {
  const std::vector<float> q = Normalized(query);
  std::vector<std::pair<float, size_t>> scored;
  scored.reserve(corpus_norm.size());
  for (size_t i = 0; i < corpus_norm.size(); ++i) {
    float s = 0;
    for (int d = 0; d < kDim; ++d) s += q[d] * corpus_norm[i][d];
    scored.emplace_back(s, i);
  }
  const size_t keep = std::min<size_t>(scored.size(), static_cast<size_t>(k));
  std::partial_sort(scored.begin(), scored.begin() + keep, scored.end(),
                    [](const auto& a, const auto& b) { return a.first > b.first; });
  std::vector<std::string> ids;
  ids.reserve(keep);
  for (size_t i = 0; i < keep; ++i) {
    ids.push_back(corpus_ids[scored[i].second]);
  }
  return ids;
}

double Recall(const std::vector<SearchResult>& got,
              const std::vector<std::string>& gt, int k) {
  const size_t want = std::min<size_t>(gt.size(), static_cast<size_t>(k));
  if (want == 0) return 0.0;
  const std::unordered_set<std::string> g(gt.begin(), gt.end());
  size_t hit = 0;
  for (const auto& r : got) {
    if (g.count(r.chunk_id)) ++hit;
  }
  return static_cast<double>(std::min(hit, want)) / static_cast<double>(want);
}

struct Acc {
  double progressive = 0;  // 码本只训一次（现状）
  double retrained = 0;    // 全量重建后的码本
  double off = 0;          // kOff-HNSW 参照：无量化时的上限
  int n = 0;
};

void Accumulate([[maybe_unused]] HNSWVectorIndex* progressive,
                [[maybe_unused]] HNSWVectorIndex* retrained,
                [[maybe_unused]] HNSWVectorIndex* off, ChunkStorage* storage,
                const std::string& kb_id,
                const std::vector<std::vector<float>>& queries,
                const std::vector<std::vector<std::string>>& gts, int k,
                int candidate_n, Acc* acc) {
  for (size_t i = 0; i < queries.size(); ++i) {
    auto a = progressive->SearchWithRerank(storage, kb_id, queries[i], k, candidate_n);
    auto b = retrained->SearchWithRerank(storage, kb_id, queries[i], k, candidate_n);
    auto c = off->SearchWithRerank(storage, kb_id, queries[i], k, candidate_n);
    if (!a.ok() || !b.ok() || !c.ok()) continue;
    acc->progressive += Recall(a.value(), gts[i], k);
    acc->retrained += Recall(b.value(), gts[i], k);
    acc->off += Recall(c.value(), gts[i], k);
    ++acc->n;
  }
}

double Mean(double sum, int n) { return n == 0 ? 0.0 : sum / static_cast<double>(n); }

// 量化类型矩阵。SQ_FP16 是免训练对照（§5）。
std::vector<std::pair<const char*, QuantizerConfig>> TypeMatrix() {
  std::vector<std::pair<const char*, QuantizerConfig>> types;

  QuantizerConfig sq8;
  sq8.type = QuantizerType::kSQ8;
  types.emplace_back("SQ8", sq8);

  QuantizerConfig pq;
  pq.type = QuantizerType::kPQ;
  pq.pq_m = 16;  // 必须整除 kDim=64
  types.emplace_back("PQ(m16,b8)", pq);

  QuantizerConfig fp16;
  fp16.type = QuantizerType::kSQFP16;
  types.emplace_back("SQ_FP16", fp16);

  return types;
}

TEST(RecallDriftBenchmark, DISABLED_QuantizerDriftVsRetrain) {
  std::mt19937 rng(20260917);
  const fs::path root =
      fs::temp_directory_path() /
      ("stratum_recalldrift_" +
       std::to_string(reinterpret_cast<uintptr_t>(this)));
  fs::remove_all(root);
  fs::create_directories(root);

  // spread: delta 相对 base 的分布漂移幅度（1.0 = 同分布，harness 自检格）。
  // ratio:  累积新增向量数 / base 向量数（§3.1 主判据的横轴）。
  const std::vector<float> spreads = {1.0f, 2.0f};
  const std::vector<float> ratios = {0.25f, 1.0f};

  std::printf(
      "recall,type,spread,ratio,K,progressive,retrained,delta_drift_cost,"
      "koff_reference\n");

  for (float spread : spreads) {
    for (float ratio : ratios) {
      const int delta_n = static_cast<int>(kBaseN * ratio);
      const std::string tag =
          "s" + std::to_string(int(spread * 10)) + "r" + std::to_string(int(ratio * 100));

      // 每个 (spread, ratio) 组合独立采样语料，避免不同格共享随机性。
      auto base = MakeClusterItems("base", kBaseN, 1.0f, &rng);
      auto delta = MakeClusterItems("delta", delta_n, spread, &rng);

      // rerank 要从 chunk store 读全精度向量 ⇒ base + delta 都要写进去。
      auto storage_or = RocksDBChunkStorage::Open((root / ("db_" + tag)).string());
      ASSERT_TRUE(storage_or.ok()) << storage_or.status();
      ChunkStorage* storage = storage_or.value().get();
      const std::string kb_id = "kb-" + tag;
      for (const auto& it : base) {
        ASSERT_TRUE(storage->Write(EncodeKey(kb_id, it.id), it.vec).ok());
      }
      for (const auto& it : delta) {
        ASSERT_TRUE(storage->Write(EncodeKey(kb_id, it.id), it.vec).ok());
      }

      // 暴力 GT 用的语料视图（归一化 + id）。
      std::vector<std::vector<float>> corpus_norm;
      std::vector<std::string> corpus_ids;
      corpus_norm.reserve(base.size() + delta.size());
      corpus_ids.reserve(base.size() + delta.size());
      for (const auto& it : base) {
        corpus_norm.push_back(Normalized(it.vec));
        corpus_ids.push_back(it.id);
      }
      for (const auto& it : delta) {
        corpus_norm.push_back(Normalized(it.vec));
        corpus_ids.push_back(it.id);
      }

      // 一半 query 来自 base（老数据）、一半来自 delta（新数据）：漂移伤害的正是
      // "新数据的近邻在旧码本下筛不出来"，所以两组分开累计。
      std::vector<std::vector<float>> base_q;
      std::vector<std::vector<float>> delta_q;
      {
        std::uniform_int_distribution<int> pb(0, static_cast<int>(base.size()) - 1);
        std::uniform_int_distribution<int> pd(0, static_cast<int>(delta.size()) - 1);
        for (int i = 0; i < kQueries / 2; ++i) base_q.push_back(base[pb(rng)].vec);
        for (int i = 0; i < kQueries - kQueries / 2; ++i) {
          delta_q.push_back(delta[pd(rng)].vec);
        }
      }
      std::vector<std::vector<std::string>> gt_base;
      std::vector<std::vector<std::string>> gt_delta;
      for (const auto& q : base_q) {
        gt_base.push_back(BruteForceTopK(corpus_norm, corpus_ids, q, kGtK));
      }
      for (const auto& q : delta_q) {
        gt_delta.push_back(BruteForceTopK(corpus_norm, corpus_ids, q, kGtK));
      }

      std::vector<ChunkVector> all = ToChunkVectors(base);
      {
        const auto d = ToChunkVectors(delta);
        all.insert(all.end(), d.begin(), d.end());
      }
      const std::vector<ChunkVector> base_cv = ToChunkVectors(base);
      const std::vector<ChunkVector> delta_cv = ToChunkVectors(delta);

      for (const auto& [name, cfg] : TypeMatrix()) {
        // A「渐进」：Build 训练码本 → AddChunks 用同一个码本编码 delta。
        HNSWVectorIndex prog(cfg);
        ASSERT_TRUE(prog.Build(base_cv, MetricType::COSINE).ok());
        ASSERT_TRUE(prog.AddChunks(delta_cv).ok());
        // Save 把索引封成 kReady —— 未封的 kBuilding 状态不允许查询。
        ASSERT_TRUE(prog.Save((root / (tag + "_" + name + "_prog.bin")).string()).ok());

        // B「重训」：全量重建 ⇒ 码本重训。这就是"码本被刷新"之后的现实。
        HNSWVectorIndex retr(cfg);
        ASSERT_TRUE(retr.Build(all, MetricType::COSINE).ok());
        ASSERT_TRUE(retr.Save((root / (tag + "_" + name + "_retr.bin")).string()).ok());

        // 参照：kOff（无量化、单阶段）。它的损失全部来自 HNSW 图近似。
        HNSWVectorIndex off;
        ASSERT_TRUE(off.Build(all, MetricType::COSINE).ok());
        ASSERT_TRUE(off.Save((root / (tag + "_" + name + "_off.bin")).string()).ok());

        for (int k : kRetrievalKs) {
          // candidate_n 随 k 缩放（默认策略 8×）。固定成 80 会让 k=100 的
          // recall 被候选池大小截断在 0.8，测到的是池子而不是漂移。
          const int candidate_n = kCandidateMultiple * k;
          Acc on_base;
          Acc on_delta;
          Accumulate(&prog, &retr, &off, storage, kb_id, base_q, gt_base, k,
                     candidate_n, &on_base);
          Accumulate(&prog, &retr, &off, storage, kb_id, delta_q, gt_delta, k,
                     candidate_n, &on_delta);

          // 汇总：两类 query 等权（各 kQueries/2）。
          const double p = 0.5 * Mean(on_base.progressive, on_base.n) +
                           0.5 * Mean(on_delta.progressive, on_delta.n);
          const double r = 0.5 * Mean(on_base.retrained, on_base.n) +
                           0.5 * Mean(on_delta.retrained, on_delta.n);
          const double o = 0.5 * Mean(on_base.off, on_base.n) +
                           0.5 * Mean(on_delta.off, on_delta.n);
          std::printf(
              "recall,%s,spread=%.1f,ratio=%.2f,K=%d,cand_n=%d,"
              "progressive=%.4f,retrained=%.4f,drift_cost=%+.4f,koff=%.4f\n",
              name, spread, ratio, k, candidate_n, p, r, r - p, o);
          // 细分：漂移应该只伤 delta-query 那一半。
          std::printf(
              "recall_split,%s,%.1f,%.2f,%d,baseQ_prog,%.4f,baseQ_retr,%.4f,"
              "deltaQ_prog,%.4f,deltaQ_retr,%.4f\n",
              name, spread, ratio, k, Mean(on_base.progressive, on_base.n),
              Mean(on_base.retrained, on_base.n), Mean(on_delta.progressive, on_delta.n),
              Mean(on_delta.retrained, on_delta.n));
        }
        std::fflush(stdout);
      }

      fs::remove_all(root / ("db_" + tag));
    }
  }

  fs::remove_all(root);
}

// ---------------------------------------------------------------------------
// 干净隔离：只让码本不同（下到 faiss 层）
// ---------------------------------------------------------------------------
//
// 上面的 TEST 是端到端口径，但它**无法把"码本"与"图"分开**：Build(base) 之后
// AddChunks(delta) 同时改了码本和图（HNSW 的图取决于 add 的顺序），而实测下来
// 图结构差异才是读数里的主导项 —— spread=1.0（同分布、无码本可漂移）那一格就已
// 有 10%~28% 的差，且 spread 变大后差异方向不一致。
//
// 所以这里退到 faiss 层，把变量钉死成一个：
//
//   索引 C1：train(base) → add(all)   码本来自 base —— 现状：只训一次，之后一直用
//   索引 C2：train(all)  → add(all)   码本来自 all  —— 全量重建（码本被刷新）
//
// 两者 add 的数据与顺序**完全相同** ⇒ HNSW 图相同 ⇒ 唯一差别就是码本。
// 度量的是**候选覆盖**：真近邻有没有进 stage-1 的候选池。按 README:17，量化只
// 影响候选覆盖，所以这既是漂移唯一可能的危害路径，也是最直接的读数。
//
// 索引参数必须与 hnsw_index.cpp 一致（kM / kEfConstruction / kEfSearch 是该 TU
// 匿名命名空间的私有常量，这里复制其值）；COSINE 在 faiss 层就是"归一化 + 内积"。
//
// 注意归一化本身会**削弱** SQ8 的 range 漂移：COSINE 下所有向量都落在单位球上，
// delta 即使原始幅度大 2 倍，归一化后每维值域仍与 base 接近。所以这一组同时也在
// 回答"漂移在 COSINE + SQ8 下到底还剩多少"——若测不出，那本身就是结论。
constexpr int kBenchM = 32;
constexpr int kBenchEfConstruction = 200;
constexpr int kBenchEfSearch = 128;
constexpr int kBenchPqM = 16;  // 必须整除 kDim
constexpr int kBenchPqNBits = 8;

std::unique_ptr<faiss::Index> MakeBenchIndex(QuantizerType type) {
  std::unique_ptr<faiss::Index> idx;
  switch (type) {
    case QuantizerType::kSQ8:
      idx = std::make_unique<faiss::IndexHNSWSQ>(
          kDim, faiss::ScalarQuantizer::QT_8bit, kBenchM,
          faiss::METRIC_INNER_PRODUCT);
      break;
    case QuantizerType::kSQFP16:
      idx = std::make_unique<faiss::IndexHNSWSQ>(
          kDim, faiss::ScalarQuantizer::QT_fp16, kBenchM,
          faiss::METRIC_INNER_PRODUCT);
      break;
    case QuantizerType::kPQ:
      idx = std::make_unique<faiss::IndexHNSWPQ>(
          kDim, kBenchPqM, kBenchM, kBenchPqNBits, faiss::METRIC_INNER_PRODUCT);
      break;
    case QuantizerType::kOffFlat:
      // harness 自检用：全精度、无码本（train 是 no-op）⇒ 候选覆盖应 ≈ 1.0。
      idx = std::make_unique<faiss::IndexFlat>(kDim, faiss::METRIC_INNER_PRODUCT);
      break;
    default:
      return nullptr;
  }
  if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(idx.get()); hnsw != nullptr) {
    hnsw->hnsw.efConstruction = kBenchEfConstruction;
    hnsw->hnsw.efSearch = kBenchEfSearch;
  }
  return idx;
}

// Flatten 把 vector<vector<float>> 铺成 faiss 需要的连续数组。
std::vector<float> Flatten(const std::vector<std::vector<float>>& vs) {
  std::vector<float> out;
  out.reserve(vs.size() * static_cast<size_t>(kDim));
  for (const auto& v : vs) out.insert(out.end(), v.begin(), v.end());
  return out;
}

// BruteForceIds 用归一化内积给出精确 top-K 的**下标**。干净隔离这一组不需要
// chunk store，候选与 GT 都只用位置索引对话，省掉 id 映射。
std::vector<std::vector<int>> BruteForceIds(
    const std::vector<std::vector<float>>& corpus_norm,
    const std::vector<std::vector<float>>& queries, int k) {
  std::vector<std::vector<int>> out;
  out.reserve(queries.size());
  for (const auto& q : queries) {
    const std::vector<float> qn = Normalized(q);
    std::vector<std::pair<float, int>> scored;
    scored.reserve(corpus_norm.size());
    for (size_t i = 0; i < corpus_norm.size(); ++i) {
      float s = 0;
      for (int d = 0; d < kDim; ++d) s += qn[d] * corpus_norm[i][d];
      scored.emplace_back(s, static_cast<int>(i));
    }
    const size_t keep = std::min<size_t>(scored.size(), static_cast<size_t>(k));
    std::partial_sort(scored.begin(), scored.begin() + keep, scored.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<int> ids;
    ids.reserve(keep);
    for (size_t i = 0; i < keep; ++i) ids.push_back(scored[i].second);
    out.push_back(std::move(ids));
  }
  return out;
}

// CandidateRecall 度量候选覆盖：stage-1 取 candidate_n 个候选，看 GT@K 有几个
// 落在里面。
double CandidateRecall(faiss::Index* idx,
                       const std::vector<std::vector<float>>& queries,
                       const std::vector<std::vector<int>>& gt, int k,
                       int candidate_n) {
  if (queries.empty()) return 0.0;
  double acc = 0;
  std::vector<faiss::idx_t> labels(static_cast<size_t>(candidate_n));
  std::vector<float> distances(static_cast<size_t>(candidate_n));
  for (size_t i = 0; i < queries.size(); ++i) {
    std::vector<float> q = Normalized(queries[i]);
    idx->search(1, q.data(), candidate_n, distances.data(), labels.data());
    const std::unordered_set<int> want(gt[i].begin(), gt[i].end());
    std::unordered_set<int> got;
    for (int j = 0; j < candidate_n; ++j) {
      if (labels[static_cast<size_t>(j)] < 0) break;  // faiss 用 -1 补齐
      got.insert(static_cast<int>(labels[static_cast<size_t>(j)]));
    }
    size_t hit = 0;
    for (int id : want) {
      if (got.count(id)) ++hit;
    }
    const size_t denom = std::min<size_t>(want.size(), static_cast<size_t>(k));
    if (denom > 0) acc += static_cast<double>(hit) / static_cast<double>(denom);
  }
  return acc / static_cast<double>(queries.size());
}

TEST(RecallDriftBenchmark, DISABLED_PureCodebookDriftCandidateCoverage) {
  const std::vector<unsigned> seeds = {20260917u, 777u, 4242u};
  const std::vector<float> spreads = {1.0f, 2.0f};
  const std::vector<float> ratios = {0.25f, 1.0f};
  // OFF-Flat 是 harness 自检：无码本 ⇒ drift_cost 必须为 0、覆盖必须 ≈ 1.0。
  const std::vector<std::pair<const char*, QuantizerType>> types = {
      {"SQ8", QuantizerType::kSQ8},
      {"PQ(m16,b8)", QuantizerType::kPQ},
      {"SQ_FP16", QuantizerType::kSQFP16},
      {"OFF-Flat(selftest)", QuantizerType::kOffFlat},
  };

  std::printf(
      "codebook_only,type,seed,spread,ratio,K,cand_n,codebook_base,"
      "codebook_all,drift_cost\n");

  for (unsigned seed : seeds) {
    std::mt19937 rng(seed);
    for (float spread : spreads) {
      for (float ratio : ratios) {
        const int delta_n = static_cast<int>(kBaseN * ratio);
        const auto base = MakeClusterItems("base", kBaseN, 1.0f, &rng);
        const auto delta = MakeClusterItems("delta", delta_n, spread, &rng);

        // corpus 的顺序固定为 base 在前、delta 在后：两个索引 add 的都必须是
        // 这一个顺序，图才会一样。
        std::vector<std::vector<float>> corpus_norm;
        corpus_norm.reserve(base.size() + delta.size());
        for (const auto& it : base) corpus_norm.push_back(Normalized(it.vec));
        for (const auto& it : delta) corpus_norm.push_back(Normalized(it.vec));

        std::vector<std::vector<float>> base_norm;
        base_norm.reserve(base.size());
        for (const auto& it : base) base_norm.push_back(Normalized(it.vec));

        std::vector<std::vector<float>> queries;
        {
          std::uniform_int_distribution<int> pb(0, static_cast<int>(base.size()) - 1);
          std::uniform_int_distribution<int> pd(0, static_cast<int>(delta.size()) - 1);
          for (int i = 0; i < kQueries / 2; ++i) queries.push_back(base[pb(rng)].vec);
          for (int i = 0; i < kQueries - kQueries / 2; ++i) {
            queries.push_back(delta[pd(rng)].vec);
          }
        }

        const std::vector<float> all_flat = Flatten(corpus_norm);
        const std::vector<float> base_flat = Flatten(base_norm);

        for (int k : kRetrievalKs) {
          const int candidate_n = kCandidateMultiple * k;
          const auto gt = BruteForceIds(corpus_norm, queries, k);
          for (const auto& [name, type] : types) {
            // C1：码本只从 base 训（现状 —— 训练只发生一次）。
            auto c1 = MakeBenchIndex(type);
            ASSERT_NE(c1, nullptr);
            c1->train(static_cast<faiss::idx_t>(base_norm.size()), base_flat.data());
            c1->add(static_cast<faiss::idx_t>(corpus_norm.size()), all_flat.data());

            // C2：码本从 all 训（全量重建 ⇒ 重训）。add 的数据与顺序同 C1 ⇒ 图相同。
            auto c2 = MakeBenchIndex(type);
            ASSERT_NE(c2, nullptr);
            c2->train(static_cast<faiss::idx_t>(corpus_norm.size()), all_flat.data());
            c2->add(static_cast<faiss::idx_t>(corpus_norm.size()), all_flat.data());

            const double r1 = CandidateRecall(c1.get(), queries, gt, k, candidate_n);
            const double r2 = CandidateRecall(c2.get(), queries, gt, k, candidate_n);
            std::printf(
                "codebook_only,%s,seed=%u,spread=%.1f,ratio=%.2f,K=%d,cand_n=%d,"
                "codebook_base=%.4f,codebook_all=%.4f,drift_cost=%+.4f\n",
                name, seed, spread, ratio, k, candidate_n, r1, r2, r2 - r1);
            std::fflush(stdout);
          }
        }
      }
    }
  }
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
