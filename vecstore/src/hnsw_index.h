// HNSWVectorIndex is the real VectorIndex implementation, backed by
// Faiss's IndexHNSWFlat. See Stratum_接口设计v9.md "VectorIndex" for the
// interface contract this satisfies, and Stratum_设计文档v10.md "向量索引"
// for the design rationale (HNSW is currently the only real
// implementation; IVF/FLAT are reserved for later iterations).
#ifndef STRATUM_VECSTORE_SRC_HNSW_INDEX_H_
#define STRATUM_VECSTORE_SRC_HNSW_INDEX_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "faiss/IndexHNSW.h"
#include "vecstore/include/types.h"
#include "vecstore/include/vector_index.h"

namespace stratum {
namespace vecstore {

// HNSWVectorIndex wraps a faiss::IndexHNSWFlat. Faiss indexes address
// vectors by sequential integer position in insertion order (idx_t), not
// by our string chunk_id, so this class maintains its own
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
  HNSWVectorIndex() = default;
  ~HNSWVectorIndex() override = default;

  HNSWVectorIndex(const HNSWVectorIndex&) = delete;
  HNSWVectorIndex& operator=(const HNSWVectorIndex&) = delete;

  absl::Status Build(const std::vector<ChunkVector>& chunks,
                      MetricType metric) override;
  absl::StatusOr<std::vector<SearchResult>> Search(
      const std::vector<float>& vector, int top_k) override;
  absl::Status Save(const std::string& path) override;
  absl::Status Load(const std::string& path) override;
  absl::Status Reset() override;

 private:
  std::unique_ptr<faiss::IndexHNSWFlat> index_;
  std::vector<std::string> id_to_chunk_id_;
  MetricType metric_ = MetricType::COSINE;
  int dim_ = 0;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_SRC_HNSW_INDEX_H_
