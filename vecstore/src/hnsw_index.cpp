#include "vecstore/src/hnsw_index.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "faiss/Index.h"
#include "faiss/IndexFlatCodes.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexPQ.h"
#include "faiss/IndexScalarQuantizer.h"
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
    }
    index_->hnsw.efConstruction = kEfConstruction;
    index_->hnsw.efSearch = kEfSearch;
    dim_ = dim;
    // Quantized variants are approximate coarse retrievers whose search
    // results need re-ranking against full-precision vectors (see
    // SearchWithRerank); Flat stays exact and single-stage.
    quantized_ = (config_.type != QuantizerType::kOff);
  } else if (dim != dim_) {
    return absl::InvalidArgumentError(
        "hnsw_index: AddChunks: dimension mismatch with existing index");
  }

  id_to_chunk_id_.reserve(id_to_chunk_id_.size() + chunks.size());

  std::vector<float> flat;
  flat.reserve(chunks.size() * static_cast<size_t>(dim));
  for (const auto& c : chunks) {
    std::vector<float> v = c.vector;
    if (metric_ == MetricType::COSINE) {
      NormalizeInPlace(&v);
    }
    flat.insert(flat.end(), v.begin(), v.end());
    id_to_chunk_id_.push_back(c.chunk_id);
  }

  const faiss::idx_t n = static_cast<faiss::idx_t>(chunks.size());
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

  faiss::write_index(index_.get(), path.c_str());

  // Sidecar file: id_to_chunk_id_, one chunk_id per line in Faiss
  // insertion order (== array index order), since Faiss's own
  // serialization format has no concept of our string chunk_id.
  std::ofstream sidecar(path + ".ids", std::ios::trunc);
  if (!sidecar) {
    return absl::InternalError("hnsw_index: Save: could not open sidecar file " + path + ".ids");
  }
  sidecar << dim_ << "\n";
  sidecar << static_cast<int>(metric_) << "\n";
  for (const auto& id : id_to_chunk_id_) {
    sidecar << id << "\n";
  }
  if (!sidecar.good()) {
    return absl::InternalError("hnsw_index: Save: error writing sidecar file " + path + ".ids");
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

  std::ifstream sidecar(path + ".ids");
  if (!sidecar) {
    return absl::NotFoundError("hnsw_index: Load: sidecar file not found: " + path + ".ids");
  }

  int dim = 0;
  int metric_int = 0;
  sidecar >> dim;
  sidecar >> metric_int;
  sidecar.ignore();  // consume the trailing newline before reading chunk_id lines

  std::vector<std::string> ids;
  std::string line;
  while (std::getline(sidecar, line)) {
    if (!line.empty()) {
      ids.push_back(line);
    }
  }

  faiss::Index* raw = faiss::read_index(path.c_str());
  if (raw == nullptr) {
    return absl::InternalError("hnsw_index: Load: faiss::read_index returned null for " + path);
  }
  auto* hnsw_index = dynamic_cast<faiss::IndexHNSW*>(raw);
  if (hnsw_index == nullptr) {
    delete raw;
    return absl::InternalError(
        "hnsw_index: Load: index file does not contain an HNSW index: " + path);
  }

  index_.reset(hnsw_index);
  dim_ = dim;
  metric_ = static_cast<MetricType>(metric_int);
  id_to_chunk_id_ = std::move(ids);
  // The on-disk file is self-describing: restore the runtime retrieval
  // mode from the actual stored type rather than from config_, so legacy
  // Flat files keep working and quantized files behave as coarse
  // retrievers regardless of the in-memory config.
  quantized_ = (dynamic_cast<faiss::IndexHNSWSQ*>(hnsw_index) != nullptr ||
                dynamic_cast<faiss::IndexHNSWPQ*>(hnsw_index) != nullptr);
  state_ = LifecycleState::kReady;
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
  id_to_chunk_id_.clear();
  dim_ = 0;
  quantized_ = false;
  return absl::OkStatus();
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
  } else {
    // IndexHNSWFlat (unquantized): float32 payload.
    payload = static_cast<int64_t>(n) * dim_ * sizeof(float);
  }

  // HNSW level-0 neighbor lists average ~2*M entries of storage_idx_t
  // (int32) per node, plus per-node vector/list overhead (~16B). Upper
  // levels add a small, bounded share on top of the level-0 estimate.
  const int64_t graph =
      static_cast<int64_t>(n) * (2 * kM * static_cast<int64_t>(sizeof(int32_t)) + 16);
  return payload + graph;
}

LifecycleState HNSWVectorIndex::state() const {
  std::shared_lock<std::shared_mutex> read_lock(state_mu_);
  return state_;
}

}  // namespace vecstore
}  // namespace stratum
