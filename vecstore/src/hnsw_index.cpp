#include "vecstore/src/hnsw_index.h"

#include <cmath>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "faiss/Index.h"
#include "faiss/IndexHNSW.h"
#include "faiss/index_io.h"
#include "vecstore/include/types.h"

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

absl::Status HNSWVectorIndex::Build(const std::vector<ChunkVector>& chunks,
                                     MetricType metric) {
  metric_ = metric;
  id_to_chunk_id_.clear();
  id_to_chunk_id_.reserve(chunks.size());

  if (chunks.empty()) {
    dim_ = 0;
    index_.reset();
    return absl::OkStatus();
  }

  dim_ = static_cast<int>(chunks[0].vector.size());
  for (const auto& c : chunks) {
    if (static_cast<int>(c.vector.size()) != dim_) {
      return absl::InvalidArgumentError(
          "hnsw_index: Build: all chunk vectors must share the same "
          "dimension");
    }
  }

  index_ = std::make_unique<faiss::IndexHNSWFlat>(dim_, kM, ToFaissMetric(metric));
  index_->hnsw.efConstruction = kEfConstruction;
  index_->hnsw.efSearch = kEfSearch;

  std::vector<float> flat;
  flat.reserve(chunks.size() * static_cast<size_t>(dim_));
  for (const auto& c : chunks) {
    std::vector<float> v = c.vector;
    if (metric == MetricType::COSINE) {
      NormalizeInPlace(&v);
    }
    flat.insert(flat.end(), v.begin(), v.end());
    id_to_chunk_id_.push_back(c.chunk_id);
  }

  index_->add(static_cast<faiss::idx_t>(chunks.size()), flat.data());
  return absl::OkStatus();
}

absl::StatusOr<std::vector<SearchResult>> HNSWVectorIndex::Search(
    const std::vector<float>& vector, int top_k) {
  if (index_ == nullptr || index_->ntotal == 0) {
    return std::vector<SearchResult>{};
  }
  if (static_cast<int>(vector.size()) != dim_) {
    return absl::InvalidArgumentError(
        "hnsw_index: Search: query vector dimension does not match index "
        "dimension");
  }
  if (top_k <= 0) {
    return std::vector<SearchResult>{};
  }

  std::vector<float> query = vector;
  if (metric_ == MetricType::COSINE) {
    NormalizeInPlace(&query);
  }

  std::vector<float> distances(top_k);
  std::vector<faiss::idx_t> labels(top_k);
  index_->search(1, query.data(), top_k, distances.data(), labels.data());

  std::vector<SearchResult> results;
  results.reserve(top_k);
  for (int i = 0; i < top_k; ++i) {
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

absl::Status HNSWVectorIndex::Save(const std::string& path) {
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
  return absl::OkStatus();
}

absl::Status HNSWVectorIndex::Load(const std::string& path) {
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
  auto* hnsw_index = dynamic_cast<faiss::IndexHNSWFlat*>(raw);
  if (hnsw_index == nullptr) {
    delete raw;
    return absl::InternalError(
        "hnsw_index: Load: index file does not contain an IndexHNSWFlat: " + path);
  }

  index_.reset(hnsw_index);
  dim_ = dim;
  metric_ = static_cast<MetricType>(metric_int);
  id_to_chunk_id_ = std::move(ids);
  return absl::OkStatus();
}

absl::Status HNSWVectorIndex::Reset() {
  index_.reset();
  id_to_chunk_id_.clear();
  dim_ = 0;
  return absl::OkStatus();
}

}  // namespace vecstore
}  // namespace stratum
