// lifecycle_state_test.cpp — verifies the index-object lifecycle state
// machine (Stratum_设计文档v12.md 2.6): state transitions, per-state
// read/write admission (strict mode: searches are rejected while
// BUILDING), and a concurrent Search ∥ Reset/Load smoke test that must
// neither crash nor deadlock.
#include "vecstore/src/hnsw_index.h"

#include <atomic>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "vecstore/include/types.h"
#include "vecstore/src/rocksdb_storage.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

constexpr int kDim = 16;

std::vector<ChunkVector> MakeChunks(int n) {
  std::mt19937 rng(1234);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<ChunkVector> chunks;
  for (int i = 0; i < n; ++i) {
    ChunkVector c;
    c.chunk_id = "chunk-" + std::to_string(i);
    c.vector.resize(kDim);
    for (auto& x : c.vector) x = dist(rng);
    chunks.push_back(std::move(c));
  }
  return chunks;
}

std::vector<float> Query() {
  std::vector<float> q(kDim);
  for (auto& x : q) x = 0.5f;
  return q;
}

class LifecycleStateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() /
                ("stratum_lifecycle_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    save_path_ = (test_dir_ / "idx.bin").string();
  }
  void TearDown() override { fs::remove_all(test_dir_); }

  fs::path test_dir_;
  std::string save_path_;
};

TEST_F(LifecycleStateTest, EmptyIndexSearchReturnsEmptyAndStateReportsEmpty) {
  HNSWVectorIndex index;
  EXPECT_EQ(index.state(), LifecycleState::kEmpty);

  auto r1 = index.Search(Query(), 5);
  ASSERT_TRUE(r1.ok()) << r1.status();
  EXPECT_TRUE(r1.value().empty());
  auto r2 = index.SearchWithRerank(nullptr, "kb", Query(), 5, 100);
  ASSERT_TRUE(r2.ok()) << r2.status();
  EXPECT_TRUE(r2.value().empty());

  // Saving an empty (never-built) index is refused, as before.
  EXPECT_EQ(index.Save(save_path_).code(), absl::StatusCode::kFailedPrecondition);
}

TEST_F(LifecycleStateTest, SearchIsRejectedWhileBuildingAndAllowedAfterSave) {
  HNSWVectorIndex index;
  auto chunks = MakeChunks(50);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  EXPECT_EQ(index.state(), LifecycleState::kBuilding);

  // Strict mode: BUILDING is not queryable.
  auto before_save = index.Search(chunks[0].vector, 5);
  ASSERT_FALSE(before_save.ok());
  EXPECT_EQ(before_save.status().code(), absl::StatusCode::kFailedPrecondition);
  auto before_save_rr = index.SearchWithRerank(nullptr, "kb", chunks[0].vector, 5, 100);
  ASSERT_FALSE(before_save_rr.ok());
  EXPECT_EQ(before_save_rr.status().code(), absl::StatusCode::kFailedPrecondition);

  // Appending keeps BUILDING; Save seals READY.
  ASSERT_TRUE(index.AddChunks(MakeChunks(10)).ok());
  EXPECT_EQ(index.state(), LifecycleState::kBuilding);
  ASSERT_TRUE(index.Save(save_path_).ok());
  EXPECT_EQ(index.state(), LifecycleState::kReady);

  auto after_save = index.Search(chunks[0].vector, 5);
  ASSERT_TRUE(after_save.ok()) << after_save.status();
  EXPECT_FALSE(after_save.value().empty());
}

TEST_F(LifecycleStateTest, WritesAreRefusedOutsideTheirAllowedStates) {
  HNSWVectorIndex index;
  auto chunks = MakeChunks(30);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Save(save_path_).ok());  // READY
  EXPECT_EQ(index.state(), LifecycleState::kReady);

  // READY + AddChunks is refused (the build is sealed by Save).
  auto add = index.AddChunks(MakeChunks(5));
  EXPECT_EQ(add.code(), absl::StatusCode::kFailedPrecondition);

  // Load is allowed on READY (whole replacement) and on EMPTY, but not
  // while BUILDING.
  ASSERT_TRUE(index.Load(save_path_).ok());
  EXPECT_EQ(index.state(), LifecycleState::kReady);

  HNSWVectorIndex building;
  ASSERT_TRUE(building.Build(MakeChunks(5), MetricType::COSINE).ok());
  auto load_while_building = building.Load(save_path_);
  EXPECT_EQ(load_while_building.code(), absl::StatusCode::kFailedPrecondition);

  // Reset returns to EMPTY and searches yield empty results again.
  ASSERT_TRUE(building.Reset().ok());
  EXPECT_EQ(building.state(), LifecycleState::kEmpty);
  auto r = building.Search(Query(), 5);
  ASSERT_TRUE(r.ok()) << r.status();
  EXPECT_TRUE(r.value().empty());
}

TEST_F(LifecycleStateTest, ConcurrentSearchResetLoadSmoke) {
  // A READY index is hammered by one search thread while another thread
  // alternates Reset (EMPTY) and Load (READY). Searches may observe either
  // state (results or empty set) but must never crash or deadlock, and the
  // final state after the mutator stops must be READY and queryable.
  HNSWVectorIndex index;
  auto chunks = MakeChunks(200);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Save(save_path_).ok());
  auto query = chunks[0].vector;

  std::atomic<bool> stop{false};
  std::atomic<int> searches_ok{0};
  std::thread searcher([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      auto r = index.Search(query, 5);
      if (r.ok()) {
        searches_ok.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });
  // Mutator: Reset→EMPTY then Load→READY, repeatedly.
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(index.Reset().ok());
    ASSERT_TRUE(index.Load(save_path_).ok());
  }
  stop.store(true);
  searcher.join();

  // Read the counter only after the thread joined.
  EXPECT_GT(searches_ok.load(), 0);
  EXPECT_EQ(index.state(), LifecycleState::kReady);
  auto final_search = index.Search(query, 5);
  ASSERT_TRUE(final_search.ok()) << final_search.status();
  EXPECT_FALSE(final_search.value().empty());
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
