// hnsw_index_test.cpp — T1-8 VectorIndex test suite, per
// Stratum_测试顺序.md. Written before src/hnsw_index.cpp exists (TDD):
// this file does not compile until HNSWVectorIndex is added.
#include "vecstore/include/vector_index.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "vecstore/src/hnsw_index.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

constexpr int kDim = 32;

// RandomVector generates a deterministic-per-seed pseudo-random vector of
// dimension kDim.
std::vector<float> RandomVector(std::mt19937& rng) {
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> v(kDim);
  for (auto& x : v) x = dist(rng);
  return v;
}

float CosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
  double dot = 0, norm_a = 0, norm_b = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    dot += static_cast<double>(a[i]) * b[i];
    norm_a += static_cast<double>(a[i]) * a[i];
    norm_b += static_cast<double>(b[i]) * b[i];
  }
  if (norm_a == 0 || norm_b == 0) return 0.0f;
  return static_cast<float>(dot / (std::sqrt(norm_a) * std::sqrt(norm_b)));
}

// BruteForceTopK returns the chunk_ids of the topK nearest neighbors to
// query by exact cosine similarity, used as ground truth for the recall
// test.
std::vector<std::string> BruteForceTopK(const std::vector<ChunkVector>& chunks,
                                         const std::vector<float>& query, int top_k) {
  std::vector<std::pair<float, std::string>> scored;
  scored.reserve(chunks.size());
  for (const auto& c : chunks) {
    scored.emplace_back(CosineSimilarity(query, c.vector), c.chunk_id);
  }
  std::sort(scored.begin(), scored.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });
  std::vector<std::string> out;
  for (int i = 0; i < top_k && i < static_cast<int>(scored.size()); ++i) {
    out.push_back(scored[i].second);
  }
  return out;
}

std::vector<ChunkVector> MakeRandomChunks(int n, std::mt19937& rng) {
  std::vector<ChunkVector> chunks;
  chunks.reserve(n);
  for (int i = 0; i < n; ++i) {
    chunks.push_back(ChunkVector{"chunk-" + std::to_string(i), RandomVector(rng)});
  }
  return chunks;
}

class HNSWVectorIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() /
                ("stratum_hnsw_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
  }

  void TearDown() override { fs::remove_all(test_dir_); }

  fs::path test_dir_;
};

TEST_F(HNSWVectorIndexTest, BuildThenSearchRecallAboveThreshold) {
  std::mt19937 rng(42);
  constexpr int kNumChunks = 500;
  constexpr int kTopK = 10;
  constexpr int kNumQueries = 30;

  auto chunks = MakeRandomChunks(kNumChunks, rng);

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());

  int total_hits = 0;
  int total_expected = 0;
  for (int q = 0; q < kNumQueries; ++q) {
    auto query = RandomVector(rng);

    auto result_or = index.Search(query, kTopK);
    ASSERT_TRUE(result_or.ok()) << result_or.status();
    const auto& results = result_or.value();
    EXPECT_LE(results.size(), static_cast<size_t>(kTopK));

    std::set<std::string> got_ids;
    for (const auto& r : results) got_ids.insert(r.chunk_id);

    auto ground_truth = BruteForceTopK(chunks, query, kTopK);
    for (const auto& id : ground_truth) {
      total_expected++;
      if (got_ids.count(id)) total_hits++;
    }
  }

  double recall = static_cast<double>(total_hits) / total_expected;
  EXPECT_GT(recall, 0.95) << "HNSW recall = " << recall
                           << " (" << total_hits << "/" << total_expected
                           << "), want > 0.95 vs brute-force ground truth";
}

TEST_F(HNSWVectorIndexTest, SearchResultsIncludeExactMatchAtTop) {
  std::mt19937 rng(7);
  auto chunks = MakeRandomChunks(200, rng);

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());

  // Querying with a vector identical to an indexed chunk should return
  // that chunk as (one of) the top result(s).
  auto result_or = index.Search(chunks[50].vector, 5);
  ASSERT_TRUE(result_or.ok()) << result_or.status();
  const auto& results = result_or.value();
  ASSERT_FALSE(results.empty());
  EXPECT_EQ(results[0].chunk_id, "chunk-50");
}

TEST_F(HNSWVectorIndexTest, SaveThenLoadProducesConsistentSearchResults) {
  std::mt19937 rng(123);
  auto chunks = MakeRandomChunks(300, rng);
  auto query = RandomVector(rng);

  std::string index_path = (test_dir_ / "index.bin").string();

  std::vector<SearchResult> before;
  {
    HNSWVectorIndex index;
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    auto result_or = index.Search(query, 10);
    ASSERT_TRUE(result_or.ok());
    before = result_or.value();
    ASSERT_TRUE(index.Save(index_path).ok());
  }

  std::vector<SearchResult> after;
  {
    HNSWVectorIndex index2;
    ASSERT_TRUE(index2.Load(index_path).ok());
    auto result_or = index2.Search(query, 10);
    ASSERT_TRUE(result_or.ok());
    after = result_or.value();
  }

  ASSERT_EQ(before.size(), after.size());
  for (size_t i = 0; i < before.size(); ++i) {
    EXPECT_EQ(before[i].chunk_id, after[i].chunk_id) << "mismatch at result " << i;
    EXPECT_NEAR(before[i].score, after[i].score, 1e-4) << "score mismatch at result " << i;
  }
}

TEST_F(HNSWVectorIndexTest, ResetThenSearchReturnsEmpty) {
  std::mt19937 rng(99);
  auto chunks = MakeRandomChunks(100, rng);

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());

  // Confirm the index has data before reset.
  auto before = index.Search(chunks[0].vector, 5);
  ASSERT_TRUE(before.ok());
  EXPECT_FALSE(before.value().empty());

  ASSERT_TRUE(index.Reset().ok());

  auto after = index.Search(chunks[0].vector, 5);
  // Searching a reset (unbuilt) index should either return an empty result
  // set or a clear error — never stale data from before the reset and
  // never a crash.
  if (after.ok()) {
    EXPECT_TRUE(after.value().empty());
  }
}

TEST_F(HNSWVectorIndexTest, BuildOnAlreadyBuiltIndexReplacesContents) {
  std::mt19937 rng(55);
  auto first_chunks = MakeRandomChunks(50, rng);
  auto second_chunks = MakeRandomChunks(50, rng);  // disjoint chunk_ids would collide names; rename:
  for (auto& c : second_chunks) c.chunk_id = "second-" + c.chunk_id;

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(first_chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Build(second_chunks, MetricType::COSINE).ok());

  auto result_or = index.Search(second_chunks[0].vector, 50);
  ASSERT_TRUE(result_or.ok());
  for (const auto& r : result_or.value()) {
    EXPECT_EQ(r.chunk_id.substr(0, 7), "second-")
        << "found stale chunk_id from before rebuild: " << r.chunk_id;
  }
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
