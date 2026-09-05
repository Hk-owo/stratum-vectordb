// latency_bench_test.cpp — end-to-end query latency comparison: OFF
// (full-precision, single-stage, the "quantization-pre" path) vs SQ8 /
// SQ_BF16 / SQ_FP16 / PQ (two-stage: coarse pass + disk rerank), all
// through the real HNSWVectorIndex::SearchWithRerank with the per-
// instance lifecycle shared_lock held across the whole call.
//
// Tests are DISABLED_ by default so they never run in ctest; run them
// explicitly with:
//   ./vecstore_tests --gtest_filter='LatencyBenchmark.*' \
//       --gtest_also_run_disabled_tests
#include "vecstore/src/hnsw_index.h"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/include/types.h"
#include "vecstore/src/rocksdb_storage.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

constexpr int kDim = 64;
constexpr int kClusters = 24;
constexpr int kN = 6000;
constexpr int kQueries = 300;
constexpr int kTopK = 10;
constexpr int kCandidateN = 80;  // default: ceil(10 × 8), clamped

std::vector<ChunkVector> MakeCorpus(std::mt19937* rng) {
  std::normal_distribution<float> noise(0.f, 1.f);
  std::uniform_real_distribution<float> center(-8.f, 8.f);
  std::vector<std::vector<float>> centers;
  for (int c = 0; c < kClusters; ++c) {
    std::vector<float> v(kDim);
    for (auto& x : v) x = center(*rng);
    centers.push_back(std::move(v));
  }
  std::vector<ChunkVector> chunks;
  chunks.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    const auto& c = centers[i % centers.size()];
    ChunkVector cv;
    cv.chunk_id = "chunk-" + std::to_string(i);
    cv.vector.resize(kDim);
    for (int d = 0; d < kDim; ++d) {
      cv.vector[d] = c[d] + noise(*rng) * 0.3f;
    }
    chunks.push_back(std::move(cv));
  }
  return chunks;
}

void Measure(const char* name, const QuantizerConfig& cfg,
             const fs::path& store_dir, const std::string& save_path,
             const std::vector<ChunkVector>& chunks,
             const std::vector<std::vector<float>>& queries) {
  auto storage_or = RocksDBChunkStorage::Open(store_dir.string());
  ASSERT_TRUE(storage_or.ok()) << storage_or.status();
  ChunkStorage* storage = storage_or.value().get();
  const std::string kb_id = std::string("kb-") + name;
  for (const auto& c : chunks) {
    auto st = storage->Write(EncodeKey(kb_id, c.chunk_id), c.vector);
    ASSERT_TRUE(st.ok()) << st;
  }

  HNSWVectorIndex index(cfg);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Save(save_path).ok());

  // Warm-up: prime the RocksDB block cache and the HNSW path.
  auto warm = index.SearchWithRerank(storage, kb_id, queries[0], kTopK,
                                     kCandidateN);
  ASSERT_TRUE(warm.ok()) << warm.status();

  auto t0 = std::chrono::steady_clock::now();
  int empty_results = 0;
  for (const auto& q : queries) {
    auto r = index.SearchWithRerank(storage, kb_id, q, kTopK, kCandidateN);
    ASSERT_TRUE(r.ok()) << r.status();
    if (r.value().empty()) ++empty_results;
  }
  auto t1 = std::chrono::steady_clock::now();
  const double ms =
      std::chrono::duration<double, std::milli>(t1 - t0).count() / queries.size();
  std::printf("latency,%s,%.4f,%d,%d\n", name, ms, kQueries, empty_results);
  std::fflush(stdout);
}

TEST(LatencyBenchmark, DISABLED_CompareQueryLatencyQuantizedVsOff) {
  std::mt19937 rng(20260905);
  auto chunks = MakeCorpus(&rng);
  std::vector<std::vector<float>> queries;
  {
    std::uniform_int_distribution<int> pick(0, kN - 1);
    for (int i = 0; i < kQueries; ++i) queries.push_back(chunks[pick(rng)].vector);
  }

  const fs::path root = fs::temp_directory_path() /
                        ("stratum_latbench_" +
                         std::to_string(reinterpret_cast<uintptr_t>(this)));
  fs::remove_all(root);
  fs::create_directories(root);
  std::printf("latency,type,avg_ms_per_query,n_queries,empty_results\n");

  QuantizerConfig off;
  off.type = QuantizerType::kOff;
  Measure("OFF-Flat", off, root / "off_db", (root / "off.bin").string(), chunks, queries);

  QuantizerConfig sq8;
  sq8.type = QuantizerType::kSQ8;
  Measure("SQ8", sq8, root / "sq8_db", (root / "sq8.bin").string(), chunks, queries);

  QuantizerConfig bf16;
  bf16.type = QuantizerType::kSQBF16;
  Measure("SQ_BF16", bf16, root / "bf16_db", (root / "bf16.bin").string(), chunks, queries);

  QuantizerConfig fp16;
  fp16.type = QuantizerType::kSQFP16;
  Measure("SQ_FP16", fp16, root / "fp16_db", (root / "fp16.bin").string(), chunks, queries);

  QuantizerConfig pq;
  pq.type = QuantizerType::kPQ;
  pq.pq_m = 16;  // divides kDim=64
  Measure("PQ(m16,b8)", pq, root / "pq_db", (root / "pq.bin").string(), chunks, queries);

  fs::remove_all(root);
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
