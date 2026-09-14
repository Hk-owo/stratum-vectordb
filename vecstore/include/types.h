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

// QuantizerType selects how the in-memory index stores vector payloads.
// kOff keeps the current full-precision IndexHNSWFlat behavior; the other
// types store quantized codes (ScalarQuantizer / ProductQuantizer) inside
// the HNSW structure and act as approximate coarse retrievers whose
// results are re-ranked against full-precision vectors read from the
// chunk store (see VectorIndex::SearchWithRerank and Stratum_设计文档v12.md).
enum class QuantizerType {
  kOff,       // full-precision flat storage (现状)
  kSQ8,       // 8-bit scalar quantization (1 byte/component)
  kSQBF16,    // bfloat16 scalar quantization (2 bytes/component)
  kSQFP16,    // fp16 scalar quantization (2 bytes/component)
  kPQ,        // product quantization (pq_m * pq_nbits / 8 bytes/vector)

  // Graph-free variants (Stratum_设计文档v13.md §8.6a): the same quantizers
  // without the HNSW graph. The graph dominates both the memory footprint and
  // the build time, so a version that is unlikely to be queried — a cold,
  // non-active one — is better served by scanning quantized codes, which costs
  // O(n) per query but builds and stores far less. Hot versions keep the graph.
  kOffFlat,   // full precision, linear scan
  kSQ8Flat,   // 8-bit scalar quantization, linear scan
  kSQBF16Flat,
  kSQFP16Flat,
  kPQFlat,
};

// isGraphFree reports whether t stores vectors without an HNSW graph.
inline bool isGraphFree(QuantizerType t) {
  switch (t) {
    case QuantizerType::kOffFlat:
    case QuantizerType::kSQ8Flat:
    case QuantizerType::kSQBF16Flat:
    case QuantizerType::kSQFP16Flat:
    case QuantizerType::kPQFlat:
      return true;
    default:
      return false;
  }
}

// isQuantized reports whether t stores approximate codes whose search results
// need re-ranking against full-precision vectors. The graph-free variants use
// the same quantizers, so their results are coarse in the same way.
inline bool isQuantized(QuantizerType t) {
  switch (t) {
    case QuantizerType::kSQ8:
    case QuantizerType::kSQBF16:
    case QuantizerType::kSQFP16:
    case QuantizerType::kPQ:
    case QuantizerType::kSQ8Flat:
    case QuantizerType::kSQBF16Flat:
    case QuantizerType::kSQFP16Flat:
    case QuantizerType::kPQFlat:
      return true;
    default:
      return false;
  }
}

// QuantizerConfig is the per-knowledge-base quantization setting. It is
// fixed at knowledge-base creation time and never changes afterwards;
// each version's index carries its own quantizer (per-version full
// rebuilds do not share codebooks).
struct QuantizerConfig {
  QuantizerType type = QuantizerType::kOff;
  int pq_m = 96;      // PQ sub-vectors; must divide the vector dimension
  int pq_nbits = 8;   // PQ bits per sub-quantizer
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_TYPES_H_
