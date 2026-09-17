// hnsw_index_test.cpp — T1-8 VectorIndex test suite, per
// Stratum_测试顺序.md. Written before src/hnsw_index.cpp exists (TDD):
// this file does not compile until HNSWVectorIndex is added.
#include "vecstore/include/vector_index.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexHNSW.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/index_io.h"
#include "gtest/gtest.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/src/hnsw_index.h"
#include "vecstore/src/rocksdb_storage.h"

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

// ReadLines returns the file's lines without their newlines; WriteLines
// rewrites the file from such lines.
std::vector<std::string> ReadLines(const std::string& path) {
  std::vector<std::string> lines;
  std::ifstream in(path);
  std::string line;
  while (std::getline(in, line)) lines.push_back(line);
  return lines;
}

void WriteLines(const std::string& path, const std::vector<std::string>& lines) {
  std::ofstream out(path, std::ios::trunc);
  for (const auto& l : lines) out << l << "\n";
}

// FlipOneByteInMiddle corrupts the middle byte of a file in place, standing
// in for a torn write, bit rot, or a corrupted copy between nodes.
bool FlipOneByteInMiddle(const std::string& path) {
  std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
  if (!f) return false;
  f.seekg(0, std::ios::end);
  const std::streamoff size = f.tellg();
  if (size < 2) return false;
  const std::streamoff pos = size / 2;
  f.seekg(pos);
  char c = 0;
  f.read(&c, 1);
  c = static_cast<char>(c ^ 0xFF);
  f.seekp(pos);
  f.write(&c, 1);
  return f.good();
}

class HNSWVectorIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() /
                ("stratum_hnsw_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    save_path_ = (test_dir_ / "idx.bin").string();
  }

  void TearDown() override { fs::remove_all(test_dir_); }

  // SaveReady seals a built index into READY (Stratum_设计文档v12.md 2.6
  // strict mode): tests follow the product sequence Build → Save → Search.
  void SaveReady(HNSWVectorIndex* index, const std::string& path) {
    ASSERT_TRUE(index->Save(path).ok());
  }

  fs::path test_dir_;
  std::string save_path_;
};

TEST_F(HNSWVectorIndexTest, BuildThenSearchRecallAboveThreshold) {
  std::mt19937 rng(42);
  constexpr int kNumChunks = 500;
  constexpr int kTopK = 10;
  constexpr int kNumQueries = 30;

  auto chunks = MakeRandomChunks(kNumChunks, rng);

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  SaveReady(&index, save_path_);

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
  SaveReady(&index, save_path_);

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
    // Strict lifecycle: seal BUILDING→READY before searching.
    ASSERT_TRUE(index.Save(index_path).ok());
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

// --- atomic + checksummed persistence (Stratum_设计文档v13.md §8.3) --------

// A corrupted index file must be rejected at Load rather than silently
// searched: the sidecar records the index's CRC-32, so a torn write, bit
// rot, or a corrupted copy between nodes is caught here.
TEST_F(HNSWVectorIndexTest, LoadRejectsCorruptedIndexFile) {
  std::mt19937 rng(7);
  auto chunks = MakeRandomChunks(64, rng);

  {
    HNSWVectorIndex index;
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(save_path_).ok());
  }
  ASSERT_TRUE(FlipOneByteInMiddle(save_path_)) << "could not corrupt " << save_path_;

  HNSWVectorIndex loaded;
  EXPECT_FALSE(loaded.Load(save_path_).ok())
      << "a corrupted index file must not load";
}

// A sidecar listing a different number of chunk ids than the index holds
// would mis-map ids onto positions, naming the wrong chunk for every hit.
TEST_F(HNSWVectorIndexTest, LoadRejectsSidecarIdCountMismatch) {
  std::mt19937 rng(11);
  auto chunks = MakeRandomChunks(32, rng);

  {
    HNSWVectorIndex index;
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(save_path_).ok());
  }

  auto lines = ReadLines(save_path_ + ".ids");
  ASSERT_GT(lines.size(), 4u);  // magic + dim + metric + crc + >= 1 chunk id
  lines.pop_back();             // one chunk id fewer than the index holds
  WriteLines(save_path_ + ".ids", lines);

  HNSWVectorIndex loaded;
  EXPECT_FALSE(loaded.Load(save_path_).ok());
}

// A crash between Save's two renames leaves the new index next to the stale
// sidecar. The recorded checksum no longer matches, so Load must reject the
// pair instead of pairing mismatched files.
TEST_F(HNSWVectorIndexTest, LoadRejectsIndexFileWithStaleSidecar) {
  std::mt19937 rng(13);

  {
    HNSWVectorIndex first;
    ASSERT_TRUE(first.Build(MakeRandomChunks(24, rng), MetricType::COSINE).ok());
    ASSERT_TRUE(first.Save(save_path_).ok());
  }
  const auto stale_sidecar = ReadLines(save_path_ + ".ids");

  {
    HNSWVectorIndex second;
    ASSERT_TRUE(second.Build(MakeRandomChunks(48, rng), MetricType::COSINE).ok());
    ASSERT_TRUE(second.Save(save_path_).ok());
  }
  WriteLines(save_path_ + ".ids", stale_sidecar);  // the interrupted pair

  HNSWVectorIndex loaded;
  EXPECT_FALSE(loaded.Load(save_path_).ok());
}

// Save must be atomic: after a successful call both live files exist and no
// temporary file is left behind.
TEST_F(HNSWVectorIndexTest, SaveLeavesNoTemporaryFilesBehind) {
  std::mt19937 rng(3);
  auto chunks = MakeRandomChunks(16, rng);

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Save(save_path_).ok());

  EXPECT_TRUE(fs::exists(save_path_));
  EXPECT_TRUE(fs::exists(save_path_ + ".ids"));

  int temp_files = 0;
  for (const auto& entry : fs::directory_iterator(test_dir_)) {
    const std::string name = entry.path().filename().string();
    if (name.size() > 4 && name.compare(name.size() - 4, 4, ".tmp") == 0) {
      ++temp_files;
      ADD_FAILURE() << "leftover temporary file: " << name;
    }
  }
  EXPECT_EQ(temp_files, 0);
}

// Files written before the checked format (bare dimension first, no checksum)
// must keep loading.
TEST_F(HNSWVectorIndexTest, LoadAcceptsLegacySidecarWithoutChecksum) {
  std::mt19937 rng(5);
  auto chunks = MakeRandomChunks(20, rng);

  {
    HNSWVectorIndex index;
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(save_path_).ok());
  }

  // Rewrite the sidecar in the legacy layout: dim / metric / chunk ids.
  // The checked layout is magic / dim / metric / crc / [shape] / ids, so the
  // id block starts after the three header values — plus the §8.6a shape line
  // when the writer recorded one.
  const auto checked = ReadLines(save_path_ + ".ids");
  ASSERT_GE(checked.size(), 4u);
  size_t ids_begin = 4;
  if (ids_begin < checked.size() &&
      checked[ids_begin].rfind("graph_free ", 0) == 0) {
    ++ids_begin;
  }
  std::vector<std::string> legacy{checked[1], checked[2]};
  legacy.insert(legacy.end(), checked.begin() + ids_begin, checked.end());
  WriteLines(save_path_ + ".ids", legacy);

  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(save_path_).ok())
      << "legacy sidecars must keep loading";
  auto results = loaded.Search(RandomVector(rng), 5);
  ASSERT_TRUE(results.ok());
  EXPECT_EQ(results->size(), 5u);
}

TEST_F(HNSWVectorIndexTest, ResetThenSearchReturnsEmpty) {
  std::mt19937 rng(99);
  auto chunks = MakeRandomChunks(100, rng);

  HNSWVectorIndex index;
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  SaveReady(&index, save_path_);

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
  SaveReady(&index, save_path_);

  auto result_or = index.Search(second_chunks[0].vector, 50);
  ASSERT_TRUE(result_or.ok());
  for (const auto& r : result_or.value()) {
    EXPECT_EQ(r.chunk_id.substr(0, 7), "second-")
        << "found stale chunk_id from before rebuild: " << r.chunk_id;
  }
}

// ---------------------------------------------------------------------------
// Two-stage search (quantized coarse pass + full-precision rerank) tests.
// Stratum_设计文档v12.md "两段式检索设计": quantized indexes are coarse
// retrievers; the final top-k comes from re-scoring the coarse candidates
// against full-precision vectors read back from the chunk store.
// ---------------------------------------------------------------------------

// WriteChunksToStore persists each chunk's raw (unnormalized) vector under
// the Go-side (kb_id, chunk_id) key, mirroring the production write path.
void WriteChunksToStore(ChunkStorage* storage, const std::string& kb_id,
                        const std::vector<ChunkVector>& chunks) {
  for (const auto& c : chunks) {
    auto st = storage->Write(EncodeKey(kb_id, c.chunk_id), c.vector);
    ASSERT_TRUE(st.ok()) << st;
  }
}

// ExpectRerankIsExactOnCandidates verifies the two-stage search
// MECHANISM: given the coarse pass's candidates, SearchWithRerank must
// return exactly the brute-force cosine top-k computed over that
// candidate set (full-precision re-scoring, ordered by descending exact
// score). Whether the candidate set itself covers the true nearest
// neighbors is a recall property of the quantizer — tuned in stage ④, not
// asserted here (Faiss HNSW search never exhausts the graph, so even
// candidate_n ≥ corpus size does not guarantee full coverage).
void ExpectRerankIsExactOnCandidates(HNSWVectorIndex* index,
                                     ChunkStorage* storage,
                                     const std::string& kb_id,
                                     const std::vector<ChunkVector>& chunks,
                                     const std::vector<float>& query,
                                     int top_k, int candidate_n) {
  auto cand_or = index->SearchCandidates(query, candidate_n);
  ASSERT_TRUE(cand_or.ok()) << cand_or.status();

  std::map<std::string, std::vector<float>> by_id;
  for (const auto& c : chunks) {
    by_id.emplace(c.chunk_id, c.vector);
  }

  // Exact brute-force top-k restricted to the coarse candidates.
  std::vector<std::pair<float, std::string>> exact;
  for (const auto& cand : cand_or.value()) {
    auto it = by_id.find(cand.chunk_id);
    ASSERT_NE(it, by_id.end()) << "candidate not in corpus: " << cand.chunk_id;
    exact.emplace_back(CosineSimilarity(query, it->second), cand.chunk_id);
  }
  std::sort(exact.begin(), exact.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });
  if (exact.size() > static_cast<size_t>(top_k)) {
    exact.resize(top_k);
  }

  auto result_or =
      index->SearchWithRerank(storage, kb_id, query, top_k, candidate_n);
  ASSERT_TRUE(result_or.ok()) << result_or.status();
  const auto& results = result_or.value();
  ASSERT_EQ(results.size(), exact.size())
      << "rerank returned " << results.size() << " results, want "
      << exact.size();
  for (size_t i = 0; i < exact.size(); ++i) {
    EXPECT_EQ(results[i].chunk_id, exact[i].second) << "rank=" << i;
    EXPECT_NEAR(results[i].score, exact[i].first, 1e-4)
        << "score mismatch at rank=" << i;
  }
}

// RunQuantizedTwoStageMechanism builds a quantized index with chunks
// persisted in a real chunk store, then checks the two-stage mechanism
// across num_queries random queries.
void RunQuantizedTwoStageMechanism(const QuantizerConfig& cfg,
                                   std::mt19937* rng,
                                   const fs::path& store_dir,
                                   const std::string& save_path,
                                   int num_queries) {
  constexpr int kNumChunks = 300;
  constexpr int kTopK = 5;
  auto chunks = MakeRandomChunks(kNumChunks, *rng);

  auto storage_or = RocksDBChunkStorage::Open(store_dir.string());
  ASSERT_TRUE(storage_or.ok()) << storage_or.status();
  WriteChunksToStore(storage_or.value().get(), "kb-quant", chunks);

  HNSWVectorIndex index(cfg);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Save(save_path).ok());  // seal BUILDING→READY (v12 2.6)

  for (int q = 0; q < num_queries; ++q) {
    auto query = RandomVector(*rng);
    ExpectRerankIsExactOnCandidates(&index, storage_or.value().get(),
                                    "kb-quant", chunks, query, kTopK,
                                    kNumChunks + 10);
  }
}

TEST_F(HNSWVectorIndexTest, QuantizedSQ8SearchWithRerankMechanism) {
  std::mt19937 rng(4242);
  QuantizerConfig cfg;
  cfg.type = QuantizerType::kSQ8;
  RunQuantizedTwoStageMechanism(cfg, &rng, test_dir_ / "sq8_rocksdb",
                                save_path_, /*num_queries=*/10);
}

TEST_F(HNSWVectorIndexTest, QuantizedPQSearchWithRerankMechanism) {
  std::mt19937 rng(4243);
  QuantizerConfig cfg;
  cfg.type = QuantizerType::kPQ;
  cfg.pq_m = 8;       // divides kDim=32
  cfg.pq_nbits = 4;   // 16 centroids per subspace; 300 training samples ≫ 16
  RunQuantizedTwoStageMechanism(cfg, &rng, test_dir_ / "pq_rocksdb",
                                save_path_, /*num_queries=*/4);
}

// A PQ codebook needs 2^pq_nbits centroids, and faiss refuses to train below
// that by throwing. The throw has to reach the caller as a status that *names*
// the floor: across gRPC an exception decays into "Unknown: Unexpected error in
// RPC handling", and a version failing with that tells an operator nothing about
// what to change (see FaissRejected in src/hnsw_index.cpp).
TEST_F(HNSWVectorIndexTest, PQTrainingShortfallIsReportedNotThrown) {
  std::mt19937 rng(4244);
  QuantizerConfig cfg;
  cfg.type = QuantizerType::kPQ;
  cfg.pq_m = 8;      // divides kDim=32
  cfg.pq_nbits = 8;  // 256 centroids — far more than the batch below can seed

  HNSWVectorIndex index(cfg);
  auto chunks = MakeRandomChunks(40, rng);  // 40 < 2^8

  auto status = index.Build(chunks, MetricType::COSINE);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument)
      << "a rejected batch must not surface as an opaque gRPC Unknown: " << status;
  const std::string msg = std::string(status.message());
  EXPECT_NE(msg.find("2^pq_nbits = 256"), std::string::npos) << msg;
  EXPECT_NE(msg.find("this version has 40"), std::string::npos) << msg;

  // The graph-free twin must answer identically (§8.6a): the floor comes from
  // the quantizer, not from the graph.
  QuantizerConfig flat_cfg = cfg;
  flat_cfg.type = QuantizerType::kPQFlat;
  HNSWVectorIndex flat_index(flat_cfg);
  auto flat_status = flat_index.Build(chunks, MetricType::COSINE);
  ASSERT_FALSE(flat_status.ok());
  EXPECT_EQ(flat_status.code(), absl::StatusCode::kInvalidArgument)
      << flat_status;
}

TEST_F(HNSWVectorIndexTest, QuantizedSaveLoadRestoresRerankBehavior) {
  std::mt19937 rng(7);
  constexpr int kNumChunks = 200;
  constexpr int kTopK = 5;
  auto chunks = MakeRandomChunks(kNumChunks, rng);
  auto query = RandomVector(rng);

  auto storage_or = RocksDBChunkStorage::Open((test_dir_ / "rocksdb").string());
  ASSERT_TRUE(storage_or.ok()) << storage_or.status();
  WriteChunksToStore(storage_or.value().get(), "kb-q", chunks);

  std::string index_path = (test_dir_ / "quant.bin").string();
  {
    QuantizerConfig cfg;
    cfg.type = QuantizerType::kSQFP16;  // training-free quantizer
    HNSWVectorIndex index(cfg);
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(index_path).ok());
  }

  // Load through the default (kOff) constructor: the file is
  // self-describing, so the restored index must behave as quantized and
  // route through the two-stage search.
  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(index_path).ok());
  ExpectRerankIsExactOnCandidates(&loaded, storage_or.value().get(), "kb-q",
                                  chunks, query, kTopK, kNumChunks + 10);
}

TEST_F(HNSWVectorIndexTest, FlatSearchWithRerankIsSingleStageWithoutStorage) {
  std::mt19937 rng(99);
  auto chunks = MakeRandomChunks(150, rng);
  auto query = RandomVector(rng);

  HNSWVectorIndex index;  // default kOff: full precision
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  SaveReady(&index, save_path_);

  // Unquantized indexes never touch the chunk store: storage may be null
  // and results must match the plain in-memory search exactly.
  auto rerank_or = index.SearchWithRerank(nullptr, "kb", query, 5, 1000);
  ASSERT_TRUE(rerank_or.ok()) << rerank_or.status();
  auto plain_or = index.Search(query, 5);
  ASSERT_TRUE(plain_or.ok()) << plain_or.status();
  const auto& rerank_results = rerank_or.value();
  const auto& plain_results = plain_or.value();
  ASSERT_EQ(rerank_results.size(), plain_results.size());
  for (size_t i = 0; i < plain_results.size(); ++i) {
    EXPECT_EQ(rerank_results[i].chunk_id, plain_results[i].chunk_id);
    EXPECT_NEAR(rerank_results[i].score, plain_results[i].score, 1e-4);
  }
}

// §8.6a: a graph-free index must build, answer queries, and — the point of the
// change — survive Save/Load. Before this, Load rejected anything that was not
// an HNSW index, which would have made a distributed cold index unloadable:
// §8.4 ships index files between nodes and installs them by loading.
TEST_F(HNSWVectorIndexTest, GraphFreeIndexBuildsSearchesAndRoundTripsThroughDisk) {
  std::mt19937 rng(31);
  constexpr int kNumChunks = 150;
  constexpr int kTopK = 5;
  auto chunks = MakeRandomChunks(kNumChunks, rng);
  auto query = RandomVector(rng);

  std::string index_path = (test_dir_ / "graphfree.bin").string();
  {
    QuantizerConfig cfg;
    cfg.type = QuantizerType::kSQ8Flat;  // graph-free, training-free
    HNSWVectorIndex index(cfg);
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    // Save seals the build (BUILDING -> READY), so it comes before any query —
    // the same order every other test in this file uses.
    ASSERT_TRUE(index.Save(index_path).ok());

    auto results_or = index.Search(query, kTopK);
    ASSERT_TRUE(results_or.ok()) << results_or.status();
    EXPECT_EQ(results_or.value().size(), static_cast<size_t>(kTopK));
  }

  // The file is self-describing, so a default-constructed index (which would
  // otherwise expect an HNSW graph) must load it and answer from it.
  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(index_path).ok());
  auto results_or = loaded.Search(query, kTopK);
  ASSERT_TRUE(results_or.ok()) << results_or.status();
  EXPECT_EQ(results_or.value().size(), static_cast<size_t>(kTopK));
}

// §8.6a: the artifact sidecar records the shape, because that is a fact the Go
// IndexManager cannot derive from the file (it does not parse Faiss) and must
// not lose across a restart or a peer-to-peer handoff. A pair that disagrees
// about the shape does not belong together, and is refused like a checksum
// mismatch rather than served as the wrong shape.
TEST_F(HNSWVectorIndexTest, SidecarRecordsShapeAndLoadRejectsAMismatchedOne) {
  std::mt19937 rng(41);
  auto chunks = MakeRandomChunks(20, rng);

  {
    HNSWVectorIndex index;
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(save_path_).ok());
  }
  // Layout: magic / dim / metric / crc / shape / ids.
  auto lines = ReadLines(save_path_ + ".ids");
  ASSERT_GE(lines.size(), 5u);
  EXPECT_EQ(lines[4], "graph_free 0");

  // A sidecar claiming the other shape must not be paired with this index.
  lines[4] = "graph_free 1";
  WriteLines(save_path_ + ".ids", lines);
  HNSWVectorIndex mismatched;
  EXPECT_FALSE(mismatched.Load(save_path_).ok())
      << "a sidecar that disagrees about the shape must be refused";

  {
    QuantizerConfig cfg;
    cfg.type = QuantizerType::kSQ8Flat;  // graph-free, training-free
    HNSWVectorIndex index(cfg);
    ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(save_path_).ok());
  }
  auto free_lines = ReadLines(save_path_ + ".ids");
  ASSERT_GE(free_lines.size(), 5u);
  EXPECT_EQ(free_lines[4], "graph_free 1");
  HNSWVectorIndex loaded;
  EXPECT_TRUE(loaded.Load(save_path_).ok());
}

// The full-precision graph-free variant stores raw vectors and scans them: no
// graph, no quantization. Its answers must therefore be the exact ones.
TEST_F(HNSWVectorIndexTest, GraphFreeFullPrecisionReturnsExactNearestNeighbour) {
  std::mt19937 rng(32);
  constexpr int kNumChunks = 120;
  auto chunks = MakeRandomChunks(kNumChunks, rng);
  auto query = RandomVector(rng);

  QuantizerConfig cfg;
  cfg.type = QuantizerType::kOffFlat;
  HNSWVectorIndex index(cfg);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(index.Save((test_dir_ / "graphfree-exact.bin").string()).ok());

  auto results_or = index.Search(query, 1);
  ASSERT_TRUE(results_or.ok()) << results_or.status();
  ASSERT_EQ(results_or.value().size(), 1u);

  // Brute-force the same answer: with no graph and no quantization, Search is
  // a full scan, so the two must agree exactly.
  float best = -2.0f;
  std::string best_chunk;
  for (const auto& chunk : chunks) {
    const float score = CosineSimilarity(query, chunk.vector);
    if (score > best) {
      best = score;
      best_chunk = chunk.chunk_id;
    }
  }
  EXPECT_EQ(results_or.value()[0].chunk_id, best_chunk);
  EXPECT_NEAR(results_or.value()[0].score, best, 1e-4);
}

// §8.6a: the memory estimate the Go IndexManager budgets on must describe the
// shape actually resident. A graph-free index is the same quantizer without
// the HNSW graph, so charging it the graph term would overstate exactly the
// cost the reshape removed — and would make the byte budget evict a reshaped
// version as eagerly as a graphed one.
TEST_F(HNSWVectorIndexTest, GraphFreeEstimateOmitsTheGraph) {
  std::mt19937 rng(33);
  constexpr int kChunks = 200;
  auto chunks = MakeRandomChunks(kChunks, rng);

  // Full precision: both shapes hold n * dim * 4 bytes of vectors, so the
  // graph-free figure must be exactly that payload — no graph term.
  QuantizerConfig graphed;
  graphed.type = QuantizerType::kOff;
  HNSWVectorIndex with_graph(graphed);
  ASSERT_TRUE(with_graph.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(with_graph.Save((test_dir_ / "estimate-graphed.bin").string()).ok());

  QuantizerConfig graph_free;
  graph_free.type = QuantizerType::kOffFlat;
  HNSWVectorIndex without_graph(graph_free);
  ASSERT_TRUE(without_graph.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(without_graph.Save((test_dir_ / "estimate-graphfree.bin").string()).ok());

  const int64_t graphed_bytes = with_graph.EstimatedMemoryBytes();
  const int64_t graphfree_bytes = without_graph.EstimatedMemoryBytes();
  EXPECT_EQ(graphfree_bytes, static_cast<int64_t>(kChunks) * kDim * sizeof(float));
  EXPECT_GT(graphed_bytes, graphfree_bytes);

  // Quantized: the same SQ8 codes on both sides, so the gap is still only the
  // graph, and the graph-free figure stays positive (its codes are counted).
  QuantizerConfig sq_graphed;
  sq_graphed.type = QuantizerType::kSQ8;
  HNSWVectorIndex sq_with_graph(sq_graphed);
  ASSERT_TRUE(sq_with_graph.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(sq_with_graph.Save((test_dir_ / "estimate-sq8.bin").string()).ok());

  QuantizerConfig sq_free;
  sq_free.type = QuantizerType::kSQ8Flat;
  HNSWVectorIndex sq_without_graph(sq_free);
  ASSERT_TRUE(sq_without_graph.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(sq_without_graph.Save((test_dir_ / "estimate-sq8flat.bin").string()).ok());

  const int64_t sq_graphed_bytes = sq_with_graph.EstimatedMemoryBytes();
  const int64_t sq_free_bytes = sq_without_graph.EstimatedMemoryBytes();
  EXPECT_GT(sq_free_bytes, 0);
  EXPECT_LT(sq_free_bytes, sq_graphed_bytes);
  // Both shapes estimate the graph with the same formula, so the two gaps are
  // the same number: the thing the reshape removes.
  EXPECT_EQ(sq_graphed_bytes - sq_free_bytes, graphed_bytes - graphfree_bytes);
}

// ---------------------------------------------------------------------------
// §8.6(c) 增量复用的最小复核
//
// v13 §8.6(c) 的结论原先基于"官方文档与社区 issue，**非本仓库 vendored 源码
// 实测**"，并明确要求实现阶段用最小用例复核。下面这组用例就是那次复核：直接
// 对本仓库链接的 faiss 断言三件事，以及本项目封装层的一处现状。
// ---------------------------------------------------------------------------

// 1) HNSW 支持对**已写盘、再读回**的索引继续 add —— 这是"以父版本产物为
//    起点、只对 delta 增量 add"的前提。faiss 的 id 是追加序号，所以新点接着
//    旧 id 往后排（n_first 起），检索时按 id 精确命中。
TEST_F(HNSWVectorIndexTest, FaissHNSWAppendOnLoadedIndexWorks) {
  const int d = kDim;
  const int n_first = 60;
  const int n_second = 40;
  std::mt19937 rng(41);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> xb_first(static_cast<size_t>(n_first) * d);
  std::vector<float> xb_second(static_cast<size_t>(n_second) * d);
  for (auto& x : xb_first) x = dist(rng);
  for (auto& x : xb_second) x = dist(rng);

  const std::string path = (test_dir_ / "append-base.bin").string();
  {
    faiss::IndexHNSWFlat index(d, 16, faiss::METRIC_L2);
    index.add(n_first, xb_first.data());
    faiss::write_index(&index, path.c_str());
  }

  std::unique_ptr<faiss::Index> loaded(faiss::read_index(path.c_str()));
  ASSERT_NE(loaded, nullptr);
  ASSERT_EQ(loaded->ntotal, n_first);
  loaded->add(n_second, xb_second.data());
  EXPECT_EQ(loaded->ntotal, n_first + n_second);

  std::vector<faiss::idx_t> labels(1);
  std::vector<float> distances(1);
  loaded->search(1, xb_first.data(), 1, distances.data(), labels.data());
  EXPECT_EQ(labels[0], 0);  // 旧点仍可检索
  loaded->search(1, xb_second.data(), 1, distances.data(), labels.data());
  EXPECT_EQ(labels[0], n_first);  // 新点也在图里
}

// 2) HNSW **不支持** remove_ids：IndexHNSW 未覆写它，落到 Index::remove_ids
//    的 "not implemented" 抛错，索引内容保持不变。含删除的场景因此不能真删。
TEST_F(HNSWVectorIndexTest, FaissHNSWRemoveIdsIsUnsupported) {
  const int d = kDim;
  const int n = 30;
  std::vector<float> xb(static_cast<size_t>(n) * d, 0.0f);
  faiss::IndexHNSWFlat index(d, 16, faiss::METRIC_L2);
  index.add(n, xb.data());

  faiss::IDSelectorRange sel(0, 5);
  EXPECT_THROW(index.remove_ids(sel), faiss::FaissException);
  EXPECT_EQ(index.ntotal, n);
}

// 3) 免图形态属于 IndexFlatCodes 家族，**支持** remove_ids 且会压缩存储：
//    这也是 §8.6(a) 免图分层的一个附带好处——免图版本可以真删，而不是只能
//    打墓碑。
TEST_F(HNSWVectorIndexTest, FaissFlatCodesRemoveIdsCompacts) {
  const int d = kDim;
  const int n = 30;
  std::mt19937 rng(43);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> xb(static_cast<size_t>(n) * d);
  for (auto& x : xb) x = dist(rng);

  faiss::IndexFlat index(d, faiss::METRIC_L2);
  index.add(n, xb.data());
  ASSERT_EQ(index.ntotal, n);

  faiss::IDSelectorRange sel(0, 10);
  EXPECT_EQ(index.remove_ids(sel), 10u);
  EXPECT_EQ(index.ntotal, n - 10);

  // 查询刚被删掉的 id 0：它自己是自己的最近邻，所以 top-1 必须换成别的 id。
  std::vector<faiss::idx_t> labels(1);
  std::vector<float> distances(1);
  index.search(1, xb.data(), 1, distances.data(), labels.data());
  EXPECT_NE(labels[0], 0);
}

// 4) 本项目封装层的现状（§8.6(c) 要动的那一处）：Save 会封印构建
//    （BUILDING → READY），于是一个从磁盘 Load 回来的索引**拒绝**再
//    AddChunks —— 要在它上面做增量追加，必须先让封装层开放一条
//    "从产物继续构建"的路径。
TEST_F(HNSWVectorIndexTest, LoadedIndexRejectsAppendUntilReset) {
  std::mt19937 rng(44);
  const std::string path = (test_dir_ / "append-vecstore.bin").string();
  {
    HNSWVectorIndex index;
    ASSERT_TRUE(index.Build(MakeRandomChunks(40, rng), MetricType::COSINE).ok());
    ASSERT_TRUE(index.Save(path).ok());
  }

  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(path).ok());
  const auto status = loaded.AddChunks(MakeRandomChunks(5, rng));
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.message().find("READY"), std::string::npos) << status.message();
}

// §8.6(c) 纯追加复用的落地形态：`LoadForAppend` 把已完成的产物读回来，但把
// 状态留在 BUILDING —— 于是同一个对象可以继续 AddChunks(delta)，最后由 Save
// 封印。上面那条用例证明 `Load` 之后追加会被拒；这条证明这条新路径是通的，
// 并且追加后的产物本身仍能被独立 Load 回来（跨节点分发的前提）。
TEST_F(HNSWVectorIndexTest, LoadForAppendContinuesABuildFromAnArtifact) {
  std::mt19937 rng(45);
  const auto base_chunks = MakeRandomChunks(50, rng);
  // 独立的 id 前缀：MakeRandomChunks 的 id 从 "chunk-0" 开始，直接复用它做
  // delta 会与 base 撞 id，把映射断言变成假阳性。
  std::vector<ChunkVector> delta_chunks;
  for (int i = 0; i < 20; ++i) {
    delta_chunks.push_back(ChunkVector{"delta-" + std::to_string(i), RandomVector(rng)});
  }

  const std::string base_path = (test_dir_ / "append-continue-v1.bin").string();
  const std::string next_path = (test_dir_ / "append-continue-v2.bin").string();
  {
    HNSWVectorIndex v1;
    ASSERT_TRUE(v1.Build(base_chunks, MetricType::COSINE).ok());
    ASSERT_TRUE(v1.Save(base_path).ok());
  }

  // v2 = 以 v1 的产物为起点 + 只追加 delta。
  {
    HNSWVectorIndex v2;
    ASSERT_TRUE(v2.LoadForAppend(base_path).ok());
    // TotalVectors 就是 LoadForAppend 响应里回报给 Go 侧的那个数：它让调用方
    // 知道自己接手了多少向量（从而算出本版本不再需要的"墓碑"有多少）。
    EXPECT_EQ(v2.TotalVectors(), static_cast<int64_t>(base_chunks.size()));
    ASSERT_TRUE(v2.AddChunks(delta_chunks).ok());  // BUILDING：追加被允许
    EXPECT_EQ(v2.TotalVectors(),
              static_cast<int64_t>(base_chunks.size() + delta_chunks.size()));
    ASSERT_TRUE(v2.Save(next_path).ok());
  }

  HNSWVectorIndex v2_loaded;
  ASSERT_TRUE(v2_loaded.Load(next_path).ok());
  EXPECT_EQ(v2_loaded.TotalVectors(),
            static_cast<int64_t>(base_chunks.size() + delta_chunks.size()));

  // 两批的点都要在：各取一个，用 top-5 里包含它来断言（HNSW 是近似检索，
  // 不假定它一定排第一）。
  const auto contains = [](const std::vector<SearchResult>& hits, const std::string& id) {
    for (const auto& hit : hits) {
      if (hit.chunk_id == id) return true;
    }
    return false;
  };

  auto base_hits = v2_loaded.Search(base_chunks[0].vector, 5);
  ASSERT_TRUE(base_hits.ok()) << base_hits.status();
  EXPECT_TRUE(contains(*base_hits, base_chunks[0].chunk_id));

  auto delta_hits = v2_loaded.Search(delta_chunks[0].vector, 5);
  ASSERT_TRUE(delta_hits.ok()) << delta_hits.status();
  EXPECT_TRUE(contains(*delta_hits, delta_chunks[0].chunk_id));
}

// ---------------------------------------------------------------------------
// §8.6(c) 删除场景：免图形态可以真删（faiss 压缩 IndexFlatCodes），带图 HNSW
// 不行（faiss 没有 remove_ids），已封印（READY）的索引也不能改。
// ---------------------------------------------------------------------------

// 免图索引删除后：被删的查不到、留下的还能查到，而且 Save → Load 往返之后
// chunk→id 映射依然对得上（remove_ids 会压缩并重排 id，映射必须跟着重建）。
TEST_F(HNSWVectorIndexTest, RemoveChunksCompactsGraphFreeIndexAndKeepsTheMapping) {
  std::mt19937 rng(46);
  auto chunks = MakeRandomChunks(30, rng);
  QuantizerConfig cfg;
  cfg.type = QuantizerType::kOffFlat;  // graph-free, so removal is supported
  HNSWVectorIndex index(cfg);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());
  ASSERT_EQ(index.TotalVectors(), static_cast<int64_t>(chunks.size()));

  const std::string removed_id = chunks[5].chunk_id;
  const std::string removed_vector_id = chunks[7].chunk_id;
  const std::string kept_id = chunks[0].chunk_id;

  auto removed_or = index.RemoveChunks({removed_id, removed_vector_id, "never-was-here"});
  ASSERT_TRUE(removed_or.ok()) << removed_or.status();
  EXPECT_EQ(*removed_or, 2u);  // the unknown id is simply not counted
  EXPECT_EQ(index.TotalVectors(), static_cast<int64_t>(chunks.size() - 2));

  const auto contains = [](const std::vector<SearchResult>& hits, const std::string& id) {
    for (const auto& hit : hits) {
      if (hit.chunk_id == id) return true;
    }
    return false;
  };

  // Save seals the build, so queries come after it (removal itself has to
  // happen while the index is still open — that is what the test above this
  // one rejects).
  const std::string path = (test_dir_ / "remove-compacted.bin").string();
  ASSERT_TRUE(index.Save(path).ok());

  // A deleted vector must never come back, and an untouched one must still be
  // found under its own id (which is the check that catches a stale mapping).
  auto removed_hits = index.Search(chunks[5].vector, 3);
  ASSERT_TRUE(removed_hits.ok()) << removed_hits.status();
  EXPECT_FALSE(contains(*removed_hits, removed_id));
  auto kept_hits = index.Search(chunks[0].vector, 3);
  ASSERT_TRUE(kept_hits.ok()) << kept_hits.status();
  EXPECT_TRUE(contains(*kept_hits, kept_id));

  // The mapping must survive Save -> Load: ids were compacted in memory, and
  // the sidecar has to describe the compacted order.
  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(path).ok());
  EXPECT_EQ(loaded.TotalVectors(), static_cast<int64_t>(chunks.size() - 2));
  auto loaded_hits = loaded.Search(chunks[0].vector, 3);
  ASSERT_TRUE(loaded_hits.ok()) << loaded_hits.status();
  EXPECT_TRUE(contains(*loaded_hits, kept_id));
  auto loaded_removed_hits = loaded.Search(chunks[5].vector, 3);
  ASSERT_TRUE(loaded_removed_hits.ok()) << loaded_removed_hits.status();
  EXPECT_FALSE(contains(*loaded_removed_hits, removed_id));
}

// 两条拒绝路径：已封印（READY）的索引要先 LoadForAppend 重开；带图 HNSW 根本
// 不支持删除（既不能真删，也不该抛异常穿过 gRPC 边界）。
TEST_F(HNSWVectorIndexTest, RemoveChunksRejectsSealedAndGraphedIndexes) {
  std::mt19937 rng(47);
  auto chunks = MakeRandomChunks(10, rng);

  QuantizerConfig graph_free;
  graph_free.type = QuantizerType::kOffFlat;
  HNSWVectorIndex sealed(graph_free);
  ASSERT_TRUE(sealed.Build(chunks, MetricType::COSINE).ok());
  ASSERT_TRUE(sealed.Save((test_dir_ / "remove-sealed.bin").string()).ok());
  auto sealed_or = sealed.RemoveChunks({chunks[0].chunk_id});
  ASSERT_FALSE(sealed_or.ok());
  EXPECT_NE(sealed_or.status().message().find("sealed"), std::string::npos)
      << sealed_or.status().message();

  QuantizerConfig graphed;
  graphed.type = QuantizerType::kOff;  // HNSWFlat
  HNSWVectorIndex hnsw(graphed);
  ASSERT_TRUE(hnsw.Build(chunks, MetricType::COSINE).ok());
  auto graphed_or = hnsw.RemoveChunks({chunks[0].chunk_id});
  ASSERT_FALSE(graphed_or.ok());
  EXPECT_NE(graphed_or.status().message().find("HNSW"), std::string::npos)
      << graphed_or.status().message();
  // Nothing was touched.
  EXPECT_EQ(hnsw.TotalVectors(), static_cast<int64_t>(chunks.size()));
}

// §8.6(c) 的 delta 是"集合差"（本版本 − 父版本），不是"产物内容差"，所以它可能
// 指到一个 base 产物里已经有的 chunk（祖先留下的向量：删掉、再在后面的版本里
// 加回来）。chunk id 是内容寻址的（SHA-256(文本 + embed 配置)）⇒ 同一 id 必然
// 同一向量 ⇒ 重复的直接跳过即可，不必覆盖写。
TEST_F(HNSWVectorIndexTest, AddChunksSkipsChunksAlreadyInTheIndex) {
  std::mt19937 rng(48);
  QuantizerConfig cfg;
  cfg.type = QuantizerType::kOffFlat;
  HNSWVectorIndex index(cfg);
  auto first = MakeRandomChunks(20, rng);
  ASSERT_TRUE(index.Build(first, MetricType::COSINE).ok());
  ASSERT_EQ(index.TotalVectors(), 20);

  std::vector<ChunkVector> batch;
  batch.push_back(first[3]);  // already in the index
  batch.push_back(first[7]);  // already in the index
  for (int i = 0; i < 5; ++i) {
    batch.push_back(ChunkVector{"extra-" + std::to_string(i), RandomVector(rng)});
  }
  ASSERT_TRUE(index.AddChunks(batch).ok());
  EXPECT_EQ(index.TotalVectors(), 25);  // 20 + 5: the two duplicates added nothing

  const std::string path = (test_dir_ / "dedup-append.bin").string();
  ASSERT_TRUE(index.Save(path).ok());
  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(path).ok());
  EXPECT_EQ(loaded.TotalVectors(), 25);
  // The duplicates were dropped, so every id in the table is still resolvable:
  // searching a chunk that was re-offered must name it, not fall off the end.
  auto hits = loaded.Search(first[3].vector, 3);
  ASSERT_TRUE(hits.ok()) << hits.status();
  bool found = false;
  for (const auto& hit : *hits) found = found || hit.chunk_id == first[3].chunk_id;
  EXPECT_TRUE(found);
}

// 去重用的镜像集合必须与删除同步：被删掉的 chunk 要能再次加回来（否则"删了又
// 加回来"的版本会静默丢掉这个 chunk）。
TEST_F(HNSWVectorIndexTest, RemovedChunksBecomeAddableAgain) {
  std::mt19937 rng(49);
  QuantizerConfig cfg;
  cfg.type = QuantizerType::kOffFlat;
  HNSWVectorIndex index(cfg);
  auto chunks = MakeRandomChunks(3, rng);
  ASSERT_TRUE(index.Build(chunks, MetricType::COSINE).ok());

  auto removed_or = index.RemoveChunks({chunks[1].chunk_id});
  ASSERT_TRUE(removed_or.ok()) << removed_or.status();
  EXPECT_EQ(index.TotalVectors(), 2);

  // Same chunk id, same vector (content-addressed): it has to go back in.
  ASSERT_TRUE(index.AddChunks({chunks[1]}).ok());
  EXPECT_EQ(index.TotalVectors(), 3);

  const std::string path = (test_dir_ / "re-add.bin").string();
  ASSERT_TRUE(index.Save(path).ok());
  HNSWVectorIndex loaded;
  ASSERT_TRUE(loaded.Load(path).ok());
  EXPECT_EQ(loaded.TotalVectors(), 3);
  auto hits = loaded.Search(chunks[1].vector, 3);
  ASSERT_TRUE(hits.ok()) << hits.status();
  bool found = false;
  for (const auto& hit : *hits) found = found || hit.chunk_id == chunks[1].chunk_id;
  EXPECT_TRUE(found);
}

// 老产物可能把同一个 chunk 列两次（去重之前写下的、或来自 Load 的 sidecar）。
// "删除这个 chunk"必须删掉它的**每一份**向量，否则死向量会永远留在索引里。
// 这里手工造这么一个产物：faiss 文件 2 个向量 + legacy sidecar（无 magic、无
// checksum，第一行是维度）列出两个相同的 chunk id。
TEST_F(HNSWVectorIndexTest, RemoveChunksDropsEveryCopyOfAChunk) {
  const int d = kDim;
  const std::string path = (test_dir_ / "duplicate-copies.bin").string();
  {
    faiss::IndexFlat flat(d, faiss::METRIC_L2);
    std::vector<float> vectors(static_cast<size_t>(2) * d, 0.25f);
    flat.add(2, vectors.data());
    faiss::write_index(&flat, path.c_str());
    std::ofstream sidecar(path + ".ids");
    ASSERT_TRUE(sidecar.good());
    sidecar << d << "\n" << 1 << "\nchunk-dup\nchunk-dup\n";
  }

  HNSWVectorIndex index;
  ASSERT_TRUE(index.LoadForAppend(path).ok());
  ASSERT_EQ(index.TotalVectors(), 2);

  auto removed_or = index.RemoveChunks({"chunk-dup"});
  ASSERT_TRUE(removed_or.ok()) << removed_or.status();
  EXPECT_EQ(*removed_or, 2u);  // both copies, not just the first
  EXPECT_EQ(index.TotalVectors(), 0);
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
