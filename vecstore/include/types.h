// Shared data types used across the vecstore C++ module. These are pure
// data carriers with no behavior, mirroring the Go-side types defined in
// internal/types/types.go where applicable.
//
// See Stratum_接口设计v9.md "数据类型定义 / C++ 侧" for the authoritative
// definitions.
#ifndef STRATUM_VECSTORE_TYPES_H_
#define STRATUM_VECSTORE_TYPES_H_

#include <string>
#include <vector>

namespace stratum {
namespace vecstore {

// MetricType is the vector distance metric used by a VectorIndex. Fixed
// per knowledge base at creation time (mirrors KnowledgeBaseMeta.Similarity
// on the Go side); never changes for the lifetime of the knowledge base.
enum class MetricType {
  COSINE,
  EUCLIDEAN,
  INNER_PRODUCT,
};

// ChunkVector is a single chunk's vector, used as VectorIndex::Build input.
struct ChunkVector {
  std::string chunk_id;
  std::vector<float> vector;
};

// SearchResult is a single vector-search hit, returned by
// VectorIndex::Search. Mirrors the Go-side types.SearchResult.
struct SearchResult {
  std::string chunk_id;
  float score;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_TYPES_H_
