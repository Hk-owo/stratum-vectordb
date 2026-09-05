// HNSWVectorIndex is the real VectorIndex implementation, backed by a
// Faiss HNSW index whose vector storage is selected by QuantizerConfig:
//   - kOff:  faiss::IndexHNSWFlat (float32 payloads) — the historical
//            behavior, exact single-stage search;
//   - SQ/PQ: faiss::IndexHNSWSQ / IndexHNSWPQ (quantized codes inside the
//            HNSW structure) — approximate coarse retrievers whose
//            results must be re-ranked against full-precision vectors via
//            SearchWithRerank (Stratum_设计文档v12.md "两段式检索设计").
//
// Concurrency model (Stratum_设计文档v12.md 2.6): every instance carries an
// explicit LifecycleState and a per-instance std::shared_mutex. Read
// operations (Search / SearchCandidates / SearchWithRerank /
// EstimatedMemoryBytes) hold the shared lock for their whole body —
// including the rerank disk reads — and are only admitted in READY;
// write operations (Build / AddChunks / Save / Load / Reset) hold the
// exclusive lock and perform the state transitions. This makes
// "quantizer writes (build/load/delete) vs reads (search)" mutually
// exclusive per object and makes illegal calls (search while building,
// add after ready) fail with a clear error instead of racing.
//
// See Stratum_接口设计v9.md "VectorIndex" for the interface contract this
// satisfies.
#ifndef STRATUM_VECSTORE_SRC_HNSW_INDEX_H_
#define STRATUM_VECSTORE_SRC_HNSW_INDEX_H_

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "faiss/IndexHNSW.h"
#include "vecstore/include/chunk_storage.h"
#include "vecstore/include/types.h"
#include "vecstore/include/vector_index.h"

namespace stratum {
namespace vecstore {

// LifecycleState is the explicit lifecycle of one (kb_id, version_id)
// index object / quantized coarse retriever:
//   kEmpty:    constructed or after Reset; no content. Search returns an
//              empty result (historical behavior); Build / Load may leave.
//   kBuilding: Build started (first batch) — writable (AddChunks/Save),
//              NOT readable: Search is rejected with a clear error.
//   kReady:    Save succeeded or Load completed — readable; no more
//              AddChunks (the build is sealed by Save).
enum class LifecycleState {
  kEmpty,
  kBuilding,
  kReady,
};

// HNSWVectorIndex wraps a faiss::IndexHNSW subclass. Faiss indexes
// address vectors by sequential integer position in insertion order
// (idx_t), not by our string chunk_id, so this class maintains its own
// position-to-chunk_id side table (id_to_chunk_id_) alongside the Faiss
// index, persisted as a sidecar file next to the Faiss index file on
// Save/Load.
//
// Metric handling: Faiss natively supports METRIC_L2 and
// METRIC_INNER_PRODUCT, not cosine similarity directly. COSINE is
// implemented by L2-normalizing every vector before insertion and query,
// then using METRIC_INNER_PRODUCT internally — for unit vectors, inner
// product equals cosine similarity. All three MetricType values are
// surfaced to callers as a "higher score = more similar" scale:
//   - COSINE / INNER_PRODUCT: score is the raw (post-normalization, for
//     COSINE) inner product.
//   - EUCLIDEAN: Faiss returns squared L2 distance (lower = more similar);
//     this class negates it so the same "higher = more similar"
//     convention holds across all three metrics.
class HNSWVectorIndex : public VectorIndex {
 public:
  // Default constructor keeps the historical full-precision (kOff)
  // behavior. Pass an explicit QuantizerConfig to build quantized
  // coarse-retriever indexes.
  HNSWVectorIndex();
  explicit HNSWVectorIndex(QuantizerConfig config);
  ~HNSWVectorIndex() override = default;

  HNSWVectorIndex(const HNSWVectorIndex&) = delete;
  HNSWVectorIndex& operator=(const HNSWVectorIndex&) = delete;

  absl::Status Build(const std::vector<ChunkVector>& chunks,
                      MetricType metric) override;
  absl::Status AddChunks(const std::vector<ChunkVector>& chunks) override;
  absl::StatusOr<std::vector<SearchResult>> Search(
      const std::vector<float>& vector, int top_k) override;
  absl::StatusOr<std::vector<SearchResult>> SearchCandidates(
      const std::vector<float>& vector, int top_n) override;
  absl::StatusOr<std::vector<SearchResult>> SearchWithRerank(
      ChunkStorage* storage, const std::string& kb_id,
      const std::vector<float>& vector, int top_k,
      int candidate_n) override;
  absl::Status Save(const std::string& path) override;
  absl::Status Load(const std::string& path) override;
  absl::Status Reset() override;

  // EstimatedMemoryBytes reports the index's in-memory footprint estimate
  // (bytes) to the Go IndexManager's LRU byte accounting via the
  // Build/AddChunks RPC responses (Stratum_设计文档v12.md 3.3, coarse-
  // retriever basis: graph edges + quantized codes for SQ/PQ, float32
  // payload otherwise). Approximate is fine; monotone in ntotal required.
  int64_t EstimatedMemoryBytes() const override;

  // state returns the current lifecycle state. Diagnostic / test helper.
  LifecycleState state() const;

 private:
  // Lock-free implementations. Callers must already hold state_mu_
  // (exclusive for *Locked; shared for SearchTopN / ExactScore).
  absl::Status ResetLocked();
  absl::Status AddChunksLocked(const std::vector<ChunkVector>& chunks);

  // SearchTopN is the shared coarse search: top_n candidates from the
  // in-memory Faiss index. Exact on a full-precision index, approximate
  // on a quantized one (used by both Search and SearchWithRerank).
  // Requires the caller to hold the shared lock.
  absl::StatusOr<std::vector<SearchResult>> SearchTopN(
      const std::vector<float>& vector, int top_n);

  // ExactScore computes the full-precision similarity between the raw
  // query vector and one stored vector under metric_, on the
  // "higher = more similar" scale used by the public API. Requires the
  // caller to hold the shared lock (reads metric_).
  float ExactScore(const std::vector<float>& query,
                   const std::vector<float>& stored) const;

  mutable std::shared_mutex state_mu_;
  LifecycleState state_ = LifecycleState::kEmpty;

  std::unique_ptr<faiss::IndexHNSW> index_;
  std::vector<std::string> id_to_chunk_id_;
  MetricType metric_ = MetricType::COSINE;
  int dim_ = 0;
  QuantizerConfig config_;
  // Whether index_ stores quantized codes (SQ/PQ). True only after a
  // quantized Build/AddChunks or a Load of a quantized file; Reset clears
  // it. Full-precision (Flat) indexes always have quantized_ == false.
  bool quantized_ = false;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_SRC_HNSW_INDEX_H_
