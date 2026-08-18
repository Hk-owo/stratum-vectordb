// VectorIndex is the vector index interface: HNSW / IVF / FLAT each
// implement this interface. Currently only HNSW (src/hnsw_index.cpp) has a
// real implementation; IVF and FLAT are reserved for later iterations (see
// Stratum_设计文档v10.md "待细化的问题").
//
// Logical call chain:
//   IndexManager.TriggerBuild -> VectorIndex::Build -> VectorIndex::Save
//   IndexManager.Search       -> VectorIndex::Load  -> VectorIndex::Search
//   IndexManager.EvictByKB    -> VectorIndex::Reset
//
// See Stratum_接口设计v9.md "VectorIndex" for the authoritative design.
// This header defines only the interface; the HNSW-backed implementation
// (Faiss-based) is built in Phase 1 (1-B-2).
#ifndef STRATUM_VECSTORE_VECTOR_INDEX_H_
#define STRATUM_VECSTORE_VECTOR_INDEX_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "vecstore/include/types.h"

namespace stratum {
namespace vecstore {

// VectorIndex manages a single version's vector index: building it from a
// batch of chunk vectors, searching it, and persisting/restoring it to/
// from disk.
//
// metric is determined by the owning knowledge base's Similarity field,
// immutable after the knowledge base is created; the default is COSINE.
class VectorIndex {
 public:
  virtual ~VectorIndex() = default;

  // Build constructs the index from chunks using the given distance
  // metric. Implementations should treat repeated Build calls on the same
  // instance as a full rebuild (discarding any previously built state),
  // since each version's index is built exactly once per TriggerBuild
  // invocation.
  virtual absl::Status Build(const std::vector<ChunkVector>& chunks,
                              MetricType metric) = 0;

  // AddChunks appends chunk vectors to the index. If the index has not yet
  // been built (or loaded), the first AddChunks call creates it using the
  // metric established by the preceding Build call (or COSINE by default).
  // The Go side splits a large build into one Build call followed by one or
  // more AddChunks calls so a single gRPC message never exceeds the
  // transport's size limit.
  //
  // Note: a batched build (Build followed by several AddChunks) is not
  // atomic from a direct gRPC caller's perspective — a concurrent Search
  // may observe the partially built index between batches. The product's
  // Go-side IndexManager gates Search behind its loaded/loading state, so
  // it only queries after the whole build completes; direct vecstore
  // callers must provide their own ordering.
  virtual absl::Status AddChunks(const std::vector<ChunkVector>& chunks) = 0;

  // Search returns up to topK approximate nearest neighbors to vector,
  // ordered by descending similarity score. Requires a prior successful
  // Build or Load.
  virtual absl::StatusOr<std::vector<SearchResult>> Search(
      const std::vector<float>& vector, int top_k) = 0;

  // Save persists the current in-memory index to path on disk.
  virtual absl::Status Save(const std::string& path) = 0;

  // Load restores the index from a file previously written by Save,
  // replacing any current in-memory state.
  virtual absl::Status Load(const std::string& path) = 0;

  // Reset clears the index back to an empty, unbuilt state. After Reset,
  // Search must not be called until a subsequent Build or Load.
  virtual absl::Status Reset() = 0;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_VECTOR_INDEX_H_
