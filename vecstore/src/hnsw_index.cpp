#include "vecstore/src/hnsw_index.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "faiss/Index.h"
#include "faiss/IndexFlatCodes.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexPQ.h"
#include "faiss/IndexScalarQuantizer.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/impl/ScalarQuantizer.h"
#include "faiss/index_io.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/include/types.h"
#include "vecstore/include/vector_index.h"

namespace stratum {
namespace vecstore {

namespace {

// efConstruction / efSearch control the HNSW build/search quality-speed
// tradeoff. These generous defaults favor recall over latency/build time,
// appropriate for Stratum's per-version full-rebuild model (see
// Stratum_设计文档v10.md "向量索引": "每次 CreateVersion 触发一次全量 HNSW
// 重建" — builds are infrequent relative to queries, so spending more time
// per build for better recall is a reasonable tradeoff). Revisit if
// production index sizes show this needs to be configurable per knowledge
// base.
constexpr int kEfConstruction = 200;
constexpr int kEfSearch = 128;
constexpr int kM = 32;  // HNSW graph connectivity parameter

// Sidecar magic line, introduced together with the atomic + checksummed
// write path. A legacy sidecar starts with the bare dimension instead, which
// is what keeps pre-existing index files loadable.
constexpr char kSidecarMagic[] = "stratum-index-1";

// Crc32Table returns the standard CRC-32 (IEEE 802.3, reflected, polynomial
// 0xEDB88320) lookup table, built once on first use.
const std::array<uint32_t, 256>& Crc32Table() {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int bit = 0; bit < 8; ++bit) {
        c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      }
      t[i] = c;
    }
    return t;
  }();
  return table;
}

// Crc32 folds len bytes into a running CRC-32 (seed with 0 for a fresh run).
uint32_t Crc32(const unsigned char* data, size_t len, uint32_t crc = 0) {
  const auto& table = Crc32Table();
  uint32_t c = ~crc;
  for (size_t i = 0; i < len; ++i) {
    c = table[(c ^ data[i]) & 0xFFu] ^ (c >> 8);
  }
  return ~c;
}

// Crc32OfFile streams a file through Crc32 in fixed-size chunks, so indexes
// far larger than memory can still be checksummed.
absl::StatusOr<uint32_t> Crc32OfFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return absl::NotFoundError("hnsw_index: cannot open " + path + " for checksum");
  }
  std::vector<char> buf(1 << 20);
  uint32_t crc = 0;
  while (in) {
    in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
    const std::streamsize n = in.gcount();
    if (n > 0) {
      crc = Crc32(reinterpret_cast<const unsigned char*>(buf.data()),
                  static_cast<size_t>(n), crc);
    }
  }
  if (in.bad()) {
    return absl::InternalError("hnsw_index: read error while checksumming " + path);
  }
  return crc;
}

// FsyncPath flushes a file's (or directory's) contents and metadata to stable
// storage, so that a later rename into place cannot be lost in a crash.
absl::Status FsyncPath(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return absl::InternalError("hnsw_index: open for fsync failed: " + path);
  }
  const int rc = ::fsync(fd);
  ::close(fd);
  if (rc != 0) {
    return absl::InternalError("hnsw_index: fsync failed: " + path);
  }
  return absl::OkStatus();
}

// ParentDirOf returns the directory holding path ("." when there is none),
// which Save fsyncs after renaming so the renames themselves are durable.
std::string ParentDirOf(const std::string& path) {
  const std::filesystem::path parent = std::filesystem::path(path).parent_path();
  return parent.empty() ? std::string(".") : parent.string();
}

faiss::MetricType ToFaissMetric(MetricType metric) {
  switch (metric) {
    case MetricType::EUCLIDEAN:
      return faiss::METRIC_L2;
    case MetricType::COSINE:
    case MetricType::INNER_PRODUCT:
      // COSINE is implemented as inner product over L2-normalized
      // vectors (see NormalizeInPlace), so both share METRIC_INNER_PRODUCT
      // at the Faiss layer.
      return faiss::METRIC_INNER_PRODUCT;
  }
  return faiss::METRIC_INNER_PRODUCT;
}

void NormalizeInPlace(std::vector<float>* vec) {
  double sum_sq = 0;
  for (float x : *vec) sum_sq += static_cast<double>(x) * x;
  if (sum_sq == 0) return;
  double norm = std::sqrt(sum_sq);
  for (float& x : *vec) x = static_cast<float>(x / norm);
}

}  // namespace

HNSWVectorIndex::HNSWVectorIndex() = default;

HNSWVectorIndex::HNSWVectorIndex(QuantizerConfig config) : config_(config) {}

absl::Status HNSWVectorIndex::Build(const std::vector<ChunkVector>& chunks,
                                     MetricType metric) {
  // Build = full rebuild: whatever the previous state (EMPTY / BUILDING /
  // READY), reset first, then (re)start the BUILDING phase.
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  metric_ = metric;
  ResetLocked();
  state_ = LifecycleState::kBuilding;
  return AddChunksLocked(chunks);
}

absl::Status HNSWVectorIndex::AddChunks(const std::vector<ChunkVector>& chunks) {
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  if (state_ == LifecycleState::kReady) {
    return absl::FailedPreconditionError(
        "hnsw_index: AddChunks: index is READY; cannot append (the build is "
        "sealed by Save)");
  }
  if (state_ == LifecycleState::kEmpty) {
    // First append without a preceding Build (direct-call path): entering
    // the BUILDING phase is implied.
    state_ = LifecycleState::kBuilding;
  }
  return AddChunksLocked(chunks);
}

absl::Status HNSWVectorIndex::AddChunksLocked(
    const std::vector<ChunkVector>& chunks) {
  if (chunks.empty()) {
    return absl::OkStatus();
  }

  const int dim = static_cast<int>(chunks[0].vector.size());
  for (const auto& c : chunks) {
    if (static_cast<int>(c.vector.size()) != dim) {
      return absl::InvalidArgumentError(
          "hnsw_index: AddChunks: all chunk vectors must share the same "
          "dimension");
    }
  }

  if (index_ == nullptr) {
    // First call (no prior Build/Load): create the index of the type
    // selected by config_ (full-precision HNSWFlat by default, quantized
    // SQ/PQ variants when configured), then fall through to append this
    // batch.
    if (config_.type == QuantizerType::kPQ &&
        (config_.pq_m <= 0 || config_.pq_nbits <= 0 ||
         dim % config_.pq_m != 0)) {
      return absl::InvalidArgumentError(
          "hnsw_index: AddChunks: PQ requires pq_m > 0, pq_nbits > 0, and "
          "dim % pq_m == 0");
    }
    switch (config_.type) {
      case QuantizerType::kOff:
        index_ = std::make_unique<faiss::IndexHNSWFlat>(
            dim, kM, ToFaissMetric(metric_));
        break;
      case QuantizerType::kSQ8:
        index_ = std::make_unique<faiss::IndexHNSWSQ>(
            dim, faiss::ScalarQuantizer::QT_8bit, kM, ToFaissMetric(metric_));
        break;
      case QuantizerType::kSQBF16:
        index_ = std::make_unique<faiss::IndexHNSWSQ>(
            dim, faiss::ScalarQuantizer::QT_bf16, kM, ToFaissMetric(metric_));
        break;
      case QuantizerType::kSQFP16:
        index_ = std::make_unique<faiss::IndexHNSWSQ>(
            dim, faiss::ScalarQuantizer::QT_fp16, kM, ToFaissMetric(metric_));
        break;
      case QuantizerType::kPQ:
        index_ = std::make_unique<faiss::IndexHNSWPQ>(
            dim, config_.pq_m, kM, config_.pq_nbits, ToFaissMetric(metric_));
        break;
      // Graph-free variants (§8.6a): same quantizers, no HNSW. They scan every
      // stored code per query, which is the point — the graph is what makes a
      // cold version expensive to build and to keep resident.
      case QuantizerType::kOffFlat:
        index_ = std::make_unique<faiss::IndexFlat>(dim, ToFaissMetric(metric_));
        break;
      case QuantizerType::kSQ8Flat:
        index_ = std::make_unique<faiss::IndexScalarQuantizer>(
            dim, faiss::ScalarQuantizer::QT_8bit, ToFaissMetric(metric_));
        break;
      case QuantizerType::kSQBF16Flat:
        index_ = std::make_unique<faiss::IndexScalarQuantizer>(
            dim, faiss::ScalarQuantizer::QT_bf16, ToFaissMetric(metric_));
        break;
      case QuantizerType::kSQFP16Flat:
        index_ = std::make_unique<faiss::IndexScalarQuantizer>(
            dim, faiss::ScalarQuantizer::QT_fp16, ToFaissMetric(metric_));
        break;
      case QuantizerType::kPQFlat:
        index_ = std::make_unique<faiss::IndexPQ>(
            dim, config_.pq_m, config_.pq_nbits, ToFaissMetric(metric_));
        break;
    }
    // The tuning knobs exist only on HNSW indexes; a graph-free index has none
    // to tune.
    if (auto* hnsw = dynamic_cast<faiss::IndexHNSW*>(index_.get()); hnsw != nullptr) {
      hnsw->hnsw.efConstruction = kEfConstruction;
      hnsw->hnsw.efSearch = kEfSearch;
    }
    dim_ = dim;
    // Quantized variants are approximate coarse retrievers whose search
    // results need re-ranking against full-precision vectors (see
    // SearchWithRerank); Flat stays exact and single-stage. The graph-free
    // variants quantize the same way, so they rerank the same way.
    quantized_ = isQuantized(config_.type);
  } else if (dim != dim_) {
    return absl::InvalidArgumentError(
        "hnsw_index: AddChunks: dimension mismatch with existing index");
  }

  id_to_chunk_id_.reserve(id_to_chunk_id_.size() + chunks.size());

  std::vector<float> flat;
  flat.reserve(chunks.size() * static_cast<size_t>(dim));
  for (const auto& c : chunks) {
    // Chunk ids are content-addressed (SHA-256 of the chunk text plus the
    // embed config), so the same id always means the same vector: a chunk
    // that is already in the index is skipped rather than appended twice.
    // Duplicates reach this point through §8.6(c)'s pure-append reuse, where
    // the delta is a set difference (this version minus its parent) and the
    // base artifact may still hold a vector inherited from an ancestor —
    // e.g. delete a chunk, then add it back in a later version. Skipping
    // keeps the id table (and therefore the sidecar, and the memory estimate)
    // without double entries.
    if (!known_chunk_ids_.insert(c.chunk_id).second) {
      continue;
    }
    std::vector<float> v = c.vector;
    if (metric_ == MetricType::COSINE) {
      NormalizeInPlace(&v);
    }
    flat.insert(flat.end(), v.begin(), v.end());
    id_to_chunk_id_.push_back(c.chunk_id);
  }

  const faiss::idx_t n = static_cast<faiss::idx_t>(flat.size() / static_cast<size_t>(dim));
  if (n == 0) {
    // Every chunk in this batch was already present (the dedup above exited
    // for all of them). Nothing to train or add.
    return absl::OkStatus();
  }
  if (!index_->is_trained) {
    // Codebook / range training must happen before the first add. Train
    // once on this batch (Faiss samples internally when appropriate);
    // every later AddChunks batch only encodes against the trained
    // quantizer. Full-precision (Flat) indexes are always is_trained.
    index_->train(n, flat.data());
  }
  index_->add(n, flat.data());
  return absl::OkStatus();
}

absl::StatusOr<std::vector<SearchResult>> HNSWVectorIndex::Search(
    const std::vector<float>& vector, int top_k) {
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  if (state_ == LifecycleState::kEmpty) {
    return std::vector<SearchResult>{};  // nothing built/loaded: empty result
  }
  if (state_ == LifecycleState::kBuilding) {
    return absl::FailedPreconditionError(
        "hnsw_index: search: index is still building; not queryable yet");
  }
  return SearchTopN(vector, top_k);
}

absl::StatusOr<std::vector<SearchResult>> HNSWVectorIndex::SearchCandidates(
    const std::vector<float>& vector, int top_n) {
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  if (state_ == LifecycleState::kEmpty) {
    return std::vector<SearchResult>{};
  }
  if (state_ == LifecycleState::kBuilding) {
    return absl::FailedPreconditionError(
        "hnsw_index: search: index is still building; not queryable yet");
  }
  return SearchTopN(vector, top_n);
}

absl::StatusOr<std::vector<SearchResult>> HNSWVectorIndex::SearchTopN(
    const std::vector<float>& vector, int top_n) {
  // Requires the caller to hold the shared lock (state_ == kReady).
  if (index_ == nullptr || index_->ntotal == 0) {
    return std::vector<SearchResult>{};
  }
  if (static_cast<int>(vector.size()) != dim_) {
    return absl::InvalidArgumentError(
        "hnsw_index: search: query vector dimension does not match index "
        "dimension");
  }
  if (top_n <= 0) {
    return std::vector<SearchResult>{};
  }

  std::vector<float> query = vector;
  if (metric_ == MetricType::COSINE) {
    NormalizeInPlace(&query);
  }

  std::vector<float> distances(top_n);
  std::vector<faiss::idx_t> labels(top_n);
  index_->search(1, query.data(), top_n, distances.data(), labels.data());

  std::vector<SearchResult> results;
  results.reserve(top_n);
  for (int i = 0; i < top_n; ++i) {
    if (labels[i] < 0) {
      break;  // Faiss pads short result sets with -1; nothing more to read.
    }
    float score = distances[i];
    if (metric_ == MetricType::EUCLIDEAN) {
      // Faiss returns squared L2 distance (lower = more similar); negate
      // so "higher score = more similar" holds across all metrics, per
      // the convention documented on HNSWVectorIndex.
      score = -score;
    }
    results.push_back(SearchResult{id_to_chunk_id_[labels[i]], score});
  }
  return results;
}

absl::StatusOr<std::vector<SearchResult>> HNSWVectorIndex::SearchWithRerank(
    ChunkStorage* storage, const std::string& kb_id,
    const std::vector<float>& vector, int top_k, int candidate_n) {
  // The shared lock is held for the WHOLE two-stage body, including the
  // rerank disk reads: a concurrent Reset/Load/Build (writers) cannot tear
  // the index away mid-search (Stratum_设计文档v12.md 2.6).
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  if (state_ == LifecycleState::kEmpty) {
    return std::vector<SearchResult>{};  // nothing built/loaded: empty result
  }
  if (state_ == LifecycleState::kBuilding) {
    return absl::FailedPreconditionError(
        "hnsw_index: search: index is still building; not queryable yet");
  }
  if (index_ == nullptr || index_->ntotal == 0) {
    return std::vector<SearchResult>{};
  }
  if (!quantized_) {
    // Full-precision index: single-stage exact search, no disk reads.
    return SearchTopN(vector, top_k);
  }
  if (storage == nullptr) {
    return absl::FailedPreconditionError(
        "hnsw_index: SearchWithRerank: quantized index requires a "
        "ChunkStorage to read full-precision vectors");
  }
  if (top_k <= 0) {
    return std::vector<SearchResult>{};
  }

  // Stage 1: coarse candidates from the in-memory quantized index.
  // (Directly calls SearchTopN — the lock-free core — instead of the
  // public SearchCandidates, which would try to take the shared lock
  // again; std::shared_mutex is not reentrant.)
  auto candidates_or = SearchTopN(vector, candidate_n);
  if (!candidates_or.ok()) {
    return candidates_or.status();
  }
  const auto& candidates = candidates_or.value();
  if (candidates.empty()) {
    return std::vector<SearchResult>{};
  }

  // Stage 2: read the candidates' full-precision vectors from the chunk
  // store, keyed by the Go-side (kb_id, chunk_id) encoding.
  std::vector<std::string> keys;
  keys.reserve(candidates.size());
  for (const auto& c : candidates) {
    keys.push_back(EncodeKey(kb_id, c.chunk_id));
  }
  auto read_or = storage->ReadMulti(keys);
  if (!read_or.ok()) {
    return read_or.status();
  }
  const auto& read = read_or.value();
  if (read.found.empty()) {
    return std::vector<SearchResult>{};
  }

  // Stage 3: exact re-score of the candidates whose vectors were found;
  // missing keys are skipped (defensive; see Stratum_设计文档v12.md 2.3.3).
  std::vector<std::pair<float, std::string>> scored;  // (score, chunk_id)
  scored.reserve(candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    auto it = read.found.find(keys[i]);
    if (it == read.found.end()) {
      continue;
    }
    scored.emplace_back(ExactScore(vector, it->second), candidates[i].chunk_id);
  }
  if (scored.empty()) {
    return std::vector<SearchResult>{};
  }

  const size_t keep = std::min<size_t>(scored.size(), static_cast<size_t>(top_k));
  std::partial_sort(
      scored.begin(), scored.begin() + keep, scored.end(),
      [](const auto& a, const auto& b) { return a.first > b.first; });

  std::vector<SearchResult> results;
  results.reserve(keep);
  for (size_t i = 0; i < keep; ++i) {
    results.push_back(SearchResult{scored[i].second, scored[i].first});
  }
  return results;
}

float HNSWVectorIndex::ExactScore(const std::vector<float>& query,
                                  const std::vector<float>& stored) const {
  double acc = 0;
  switch (metric_) {
    case MetricType::EUCLIDEAN: {
      double sq = 0;
      for (size_t i = 0; i < stored.size() && i < query.size(); ++i) {
        double d = static_cast<double>(query[i]) - stored[i];
        sq += d * d;
      }
      return static_cast<float>(-sq);  // negate: higher = more similar
    }
    case MetricType::COSINE: {
      // Cosine of the raw vectors; equivalent to inner product over
      // L2-normalized vectors (matches how the index was built).
      double dot = 0, norm_q = 0, norm_s = 0;
      for (size_t i = 0; i < stored.size() && i < query.size(); ++i) {
        dot += static_cast<double>(query[i]) * stored[i];
        norm_q += static_cast<double>(query[i]) * query[i];
        norm_s += static_cast<double>(stored[i]) * stored[i];
      }
      if (norm_q == 0 || norm_s == 0) {
        return 0.0f;
      }
      return static_cast<float>(dot / (std::sqrt(norm_q) * std::sqrt(norm_s)));
    }
    case MetricType::INNER_PRODUCT: {
      for (size_t i = 0; i < stored.size() && i < query.size(); ++i) {
        acc += static_cast<double>(query[i]) * stored[i];
      }
      return static_cast<float>(acc);
    }
  }
  return 0.0f;
}

absl::Status HNSWVectorIndex::Save(const std::string& path) {
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  if (index_ == nullptr) {
    return absl::FailedPreconditionError(
        "hnsw_index: Save: no index has been built or loaded");
  }

  const std::string index_tmp = path + ".tmp";
  const std::string ids_path = path + ".ids";
  const std::string ids_tmp = ids_path + ".tmp";
  auto cleanup_tmp = [&]() {
    std::remove(index_tmp.c_str());
    std::remove(ids_tmp.c_str());
  };

  // Both files are written to temporary paths and only then renamed into
  // place: a crash or a write failure can therefore never leave a torn index
  // (or half-written sidecar) at the live path — readers observe either the
  // previous complete pair or the new one.
  faiss::write_index(index_.get(), index_tmp.c_str());

  // Checksum the freshly written index. The value travels in the sidecar and
  // is re-checked on Load, which is what turns a torn file, a half-applied
  // update (new index + stale sidecar) or a corrupted copy between nodes
  // into a loud error instead of silently wrong search results.
  auto crc_or = Crc32OfFile(index_tmp);
  if (!crc_or.ok()) {
    cleanup_tmp();
    return crc_or.status();
  }

  {
    // Sidecar: id_to_chunk_id_, one chunk_id per line in Faiss insertion
    // order (== array index order), since Faiss's own serialization format
    // has no concept of our string chunk_id. The magic first line marks the
    // checked format; legacy sidecars (bare dimension first) still load.
    std::ofstream sidecar(ids_tmp, std::ios::trunc);
    if (!sidecar) {
      cleanup_tmp();
      return absl::InternalError("hnsw_index: Save: could not open sidecar file " + ids_tmp);
    }
    sidecar << kSidecarMagic << "\n";
    sidecar << dim_ << "\n";
    sidecar << static_cast<int>(metric_) << "\n";
    sidecar << *crc_or << "\n";
    for (const auto& id : id_to_chunk_id_) {
      sidecar << id << "\n";
    }
    if (!sidecar.good()) {
      sidecar.close();
      cleanup_tmp();
      return absl::InternalError("hnsw_index: Save: error writing sidecar file " + ids_tmp);
    }
    sidecar.close();  // flush before fsync
  }

  if (auto st = FsyncPath(index_tmp); !st.ok()) {
    cleanup_tmp();
    return st;
  }
  if (auto st = FsyncPath(ids_tmp); !st.ok()) {
    cleanup_tmp();
    return st;
  }

  // rename(2) is atomic within a filesystem. The index file goes first: if
  // the process dies between the two renames, the stale sidecar's checksum
  // no longer matches the new index and Load rejects the pair rather than
  // pairing mismatched files silently.
  if (std::rename(index_tmp.c_str(), path.c_str()) != 0) {
    cleanup_tmp();
    return absl::InternalError("hnsw_index: Save: rename into place failed: " + path);
  }
  if (std::rename(ids_tmp.c_str(), ids_path.c_str()) != 0) {
    std::remove(ids_tmp.c_str());
    return absl::InternalError("hnsw_index: Save: rename of sidecar failed: " + ids_path);
  }
  if (auto st = FsyncPath(ParentDirOf(path)); !st.ok()) {
    return st;
  }

  // Save seals the build: BUILDING -> READY (Stratum_设计文档v12.md 2.6).
  state_ = LifecycleState::kReady;
  return absl::OkStatus();
}

absl::Status HNSWVectorIndex::Load(const std::string& path) {
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  if (state_ == LifecycleState::kBuilding) {
    return absl::FailedPreconditionError(
        "hnsw_index: Load: cannot load while the index is building; Reset "
        "first");
  }
  return LoadLocked(path, "Load", LifecycleState::kReady);
}

absl::Status HNSWVectorIndex::LoadForAppend(const std::string& path) {
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  if (state_ == LifecycleState::kBuilding) {
    return absl::FailedPreconditionError(
        "hnsw_index: LoadForAppend: cannot load while the index is building; "
        "Reset first");
  }
  // Same read, different final state: the build stays open so AddChunks can
  // extend it (§8.6c pure-append reuse). Save is what seals it afterwards.
  return LoadLocked(path, "LoadForAppend", LifecycleState::kBuilding);
}

// LoadLocked is the shared body of Load and LoadForAppend: it reads path and
// its .ids sidecar into this index and leaves it in final_state — READY for a
// finished artifact, BUILDING for a build that will keep appending. Callers
// must hold state_mu_ exclusively.
absl::Status HNSWVectorIndex::LoadLocked(const std::string& path, const char* op,
                                         LifecycleState final_state) {
  const std::string pre = std::string("hnsw_index: ") + op + ": ";

  std::ifstream sidecar(path + ".ids");
  if (!sidecar) {
    return absl::NotFoundError(pre + "sidecar file not found: " + path + ".ids");
  }

  std::string first_line;
  if (!std::getline(sidecar, first_line)) {
    return absl::InternalError(pre + "sidecar file is empty: " + path + ".ids");
  }

  int dim = 0;
  int metric_int = 0;
  uint32_t want_crc = 0;
  bool checksummed = false;
  if (first_line == kSidecarMagic) {
    checksummed = true;
    if (!(sidecar >> dim >> metric_int >> want_crc)) {
      return absl::InternalError(pre + "malformed sidecar header: " + path + ".ids");
    }
  } else {
    // Legacy sidecar (written before the atomic + checksummed path): the
    // first line is the bare dimension and no checksum was recorded.
    try {
      dim = std::stoi(first_line);
    } catch (const std::exception&) {
      return absl::InternalError(pre + "malformed sidecar dimension: " + path + ".ids");
    }
    if (!(sidecar >> metric_int)) {
      return absl::InternalError(pre + "malformed sidecar metric: " + path + ".ids");
    }
  }
  sidecar.ignore();  // consume the trailing newline before reading chunk_id lines

  std::vector<std::string> ids;
  std::string line;
  while (std::getline(sidecar, line)) {
    if (!line.empty()) {
      ids.push_back(line);
    }
  }

  // Verify the index file before handing it to Faiss. This is the only check
  // that catches a torn/truncated file, or an index that does not belong with
  // this sidecar (e.g. a crash between the two renames in Save).
  if (checksummed) {
    auto crc_or = Crc32OfFile(path);
    if (!crc_or.ok()) {
      return crc_or.status();
    }
    if (*crc_or != want_crc) {
      return absl::InternalError(
          pre + "index file checksum mismatch for " + path +
          " (sidecar records " + std::to_string(want_crc) + ", computed " +
          std::to_string(*crc_or) + ")");
    }
  }

  faiss::Index* raw = faiss::read_index(path.c_str());
  if (raw == nullptr) {
    return absl::InternalError(pre + "faiss::read_index returned null for " + path);
  }
  // Any faiss::Index is accepted: the file is self-describing, and since
  // §8.6a it may hold a graph-free variant that is not HNSW at all. Rejecting
  // non-HNSW here would make a distributed cold index unloadable.
  //
  // The sidecar and the index must describe the same vectors: otherwise chunk
  // ids would be mapped onto the wrong positions and every hit would name the
  // wrong chunk.
  if (static_cast<size_t>(raw->ntotal) != ids.size()) {
    const size_t listed = ids.size();
    const int64_t ntotal = static_cast<int64_t>(raw->ntotal);
    delete raw;
    return absl::InternalError(
        pre + "sidecar lists " + std::to_string(listed) +
        " chunk ids but the index holds " + std::to_string(ntotal) + ": " + path);
  }

  index_.reset(raw);
  dim_ = dim;
  metric_ = static_cast<MetricType>(metric_int);
  SetChunkIDTable(std::move(ids));
  // The on-disk file is self-describing: restore the runtime retrieval
  // mode from the actual stored type rather than from config_, so legacy
  // Flat files keep working and quantized files behave as coarse
  // retrievers regardless of the in-memory config.
  quantized_ = (dynamic_cast<faiss::IndexHNSWSQ*>(raw) != nullptr ||
                dynamic_cast<faiss::IndexHNSWPQ*>(raw) != nullptr ||
                dynamic_cast<faiss::IndexScalarQuantizer*>(raw) != nullptr ||
                dynamic_cast<faiss::IndexPQ*>(raw) != nullptr);
  state_ = final_state;
  return absl::OkStatus();
}

absl::Status HNSWVectorIndex::Reset() {
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  ResetLocked();
  state_ = LifecycleState::kEmpty;
  return absl::OkStatus();
}

absl::Status HNSWVectorIndex::ResetLocked() {
  index_.reset();
  ResetChunkIDTable();
  dim_ = 0;
  quantized_ = false;
  return absl::OkStatus();
}

void HNSWVectorIndex::ResetChunkIDTable() {
  id_to_chunk_id_.clear();
  known_chunk_ids_.clear();
}

void HNSWVectorIndex::SetChunkIDTable(std::vector<std::string> chunk_ids) {
  id_to_chunk_id_ = std::move(chunk_ids);
  known_chunk_ids_.clear();
  known_chunk_ids_.reserve(id_to_chunk_id_.size());
  // Deliberately plain inserts, not insert(begin, end): a legacy artifact can
  // list the same chunk twice, and the mirror must still answer "yes, present"
  // for it (the vector count and the id table keep those duplicates).
  for (const auto& chunk_id : id_to_chunk_id_) {
    known_chunk_ids_.insert(chunk_id);
  }
}

int64_t HNSWVectorIndex::EstimatedMemoryBytes() const {
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  if (index_ == nullptr) {
    return 0;
  }
  const faiss::idx_t n = index_->ntotal;
  if (n <= 0) {
    return 0;
  }

  // Payload: quantized indexes report their code size, full-precision ones
  // the float32 vectors. The two graph-free variants (§8.6a) hold the codes
  // directly (IndexScalarQuantizer / IndexPQ), while their graphed twins
  // keep them behind a storage member — hence the four cases.
  int64_t payload = 0;
  if (const auto* sq = dynamic_cast<const faiss::IndexHNSWSQ*>(index_.get())) {
    const auto* storage =
        dynamic_cast<const faiss::IndexScalarQuantizer*>(sq->storage);
    if (storage != nullptr) {
      payload = static_cast<int64_t>(n) * storage->code_size;
    }
  } else if (const auto* pq =
                 dynamic_cast<const faiss::IndexHNSWPQ*>(index_.get())) {
    const auto* storage = dynamic_cast<const faiss::IndexPQ*>(pq->storage);
    if (storage != nullptr) {
      payload = static_cast<int64_t>(n) * storage->code_size;
    }
  } else if (const auto* flat_sq =
                 dynamic_cast<const faiss::IndexScalarQuantizer*>(index_.get())) {
    payload = static_cast<int64_t>(n) * flat_sq->code_size;
  } else if (const auto* flat_pq =
                 dynamic_cast<const faiss::IndexPQ*>(index_.get())) {
    payload = static_cast<int64_t>(n) * flat_pq->code_size;
  } else {
    // IndexHNSWFlat / IndexFlat (unquantized): float32 payload.
    payload = static_cast<int64_t>(n) * dim_ * sizeof(float);
  }

  // The graph exists only in the graphed shapes. A graph-free index (§8.6a's
  // *_FLAT) is the same quantizer without the graph, so charging it the
  // graph term would overstate exactly the cost the reshape removed — and
  // the Go IndexManager's byte budget reads this number to decide what to
  // evict. Decide from the resident object rather than from config_: after a
  // Load, the object's configuration and the file's shape need not agree.
  if (dynamic_cast<const faiss::IndexHNSW*>(index_.get()) == nullptr) {
    return payload;
  }

  // HNSW level-0 neighbor lists average ~2*M entries of storage_idx_t
  // (int32) per node, plus per-node vector/list overhead (~16B). Upper
  // levels add a small, bounded share on top of the level-0 estimate.
  const int64_t graph =
      static_cast<int64_t>(n) * (2 * kM * static_cast<int64_t>(sizeof(int32_t)) + 16);
  return payload + graph;
}

bool HNSWVectorIndex::MatchesConfig(const QuantizerConfig& config) const {
  // config_ is set at construction and never mutated, so no lock is needed.
  return config_.type == config.type && config_.pq_m == config.pq_m &&
         config_.pq_nbits == config.pq_nbits;
}

int64_t HNSWVectorIndex::TotalVectors() const {
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  if (index_ == nullptr) {
    return 0;
  }
  return static_cast<int64_t>(index_->ntotal);
}

absl::StatusOr<size_t> HNSWVectorIndex::RemoveChunks(
    const std::vector<std::string>& chunk_ids) {
  std::lock_guard<std::shared_mutex> write_lock(state_mu_);
  if (index_ == nullptr) {
    return absl::FailedPreconditionError(
        "hnsw_index: RemoveChunks: no index has been built or loaded");
  }
  if (state_ != LifecycleState::kBuilding) {
    // Same discipline as AddChunks: Save seals the build, and a sealed index
    // is what queries read. Modifying it would need a reopen (LoadForAppend),
    // which is exactly how §8.6(c) uses this.
    return absl::FailedPreconditionError(
        "hnsw_index: RemoveChunks: the index is sealed (READY); reopen it with "
        "LoadForAppend before removing vectors");
  }
  if (dynamic_cast<faiss::IndexHNSW*>(index_.get()) != nullptr) {
    // faiss does not implement remove_ids for HNSW (it would have to repair
    // the neighbour lists); only the graph-free IndexFlatCodes family can
    // compact. Checked before calling so we surface a status instead of an
    // exception crossing the gRPC boundary.
    return absl::FailedPreconditionError(
        "hnsw_index: RemoveChunks: HNSW indexes cannot remove vectors; rebuild "
        "instead");
  }
  if (chunk_ids.empty()) {
    return static_cast<size_t>(0);
  }

  // Every position whose chunk was requested, not just the first one: an
  // artifact written before the append path deduplicated ids can hold the same
  // chunk more than once, and "remove this chunk" has to mean all of its
  // vectors. The survivors are collected in the same pass, which is also the
  // order IndexFlatCodes::remove_ids compacts them into.
  const std::unordered_set<std::string> wanted(chunk_ids.begin(), chunk_ids.end());
  std::vector<faiss::idx_t> ids;
  std::vector<std::string> kept;
  kept.reserve(id_to_chunk_id_.size());
  for (size_t i = 0; i < id_to_chunk_id_.size(); ++i) {
    if (wanted.find(id_to_chunk_id_[i]) != wanted.end()) {
      ids.push_back(static_cast<faiss::idx_t>(i));
    } else {
      kept.push_back(id_to_chunk_id_[i]);
    }
  }
  if (ids.empty()) {
    return static_cast<size_t>(0);
  }

  faiss::IDSelectorBatch selector(ids.size(), ids.data());
  const size_t removed = index_->remove_ids(selector);

  // remove_ids compacted the code array: survivors shifted down and their
  // positions are now 0..ntotal-1 in the original order — which is exactly the
  // order `kept` was built in, so the chunk-id table (and the set that mirrors
  // it) can simply be swapped in.
  SetChunkIDTable(std::move(kept));
  return removed;
}

LifecycleState HNSWVectorIndex::state() const {
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  return state_;
}

}  // namespace vecstore
}  // namespace stratum
