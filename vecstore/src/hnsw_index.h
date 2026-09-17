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
#include <unordered_set>
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
  absl::Status LoadForAppend(const std::string& path) override;
  absl::Status Reset() override;

  // EstimatedMemoryBytes reports the index's in-memory footprint estimate
  // (bytes) to the Go IndexManager's LRU byte accounting via the
  // Build/AddChunks RPC responses (Stratum_设计文档v12.md 3.3, coarse-
  // retriever basis: graph edges + quantized codes for SQ/PQ, float32
  // payload otherwise). Approximate is fine; monotone in ntotal required.
  int64_t EstimatedMemoryBytes() const override;

  // MatchesConfig reports whether this index was built with config; a
  // different shape needs a different Faiss index type, so the owner
  // replaces the object instead of reusing it (§8.6a cold reshape).
  bool MatchesConfig(const QuantizerConfig& config) const override;

  // TotalVectors reports how many vectors are currently resident (0 when
  // nothing is built or loaded).
  int64_t TotalVectors() const override;

  // RemoveChunks drops the named chunks and compacts the storage (graph-free
  // shapes only, while open).
  absl::StatusOr<size_t> RemoveChunks(
      const std::vector<std::string>& chunk_ids) override;

  // state returns the current lifecycle state. Diagnostic / test helper.
  LifecycleState state() const;

 private:
  // Lock-free implementations. Callers must already hold state_mu_
  // (exclusive for *Locked; shared for SearchTopN / ExactScore).
  absl::Status ResetLocked();
  absl::Status AddChunksLocked(const std::vector<ChunkVector>& chunks);
  // LoadLocked is the shared body of Load and LoadForAppend, leaving the
  // index in final_state (READY / BUILDING). op names the caller for error
  // messages.
  absl::Status LoadLocked(const std::string& path, const char* op,
                          LifecycleState final_state);

  // ResetChunkIDTable / SetChunkIDTable are the only writers of
  // id_to_chunk_id_ (besides AddChunksLocked's append). They keep the
  // known_chunk_ids_ mirror in step, which is what makes the dedup in
  // AddChunksLocked safe: a chunk that was removed must become addable again.
  void ResetChunkIDTable();
  void SetChunkIDTable(std::vector<std::string> chunk_ids);

  // IsGraphFreeLocked reports whether index_ holds a graph-free variant
  // (§8.6a). It reads the resident Faiss type, not config_: after a Load the
  // config is the caller's request, not what the file actually holds. Callers
  // must hold state_mu_.
  bool IsGraphFreeLocked() const;

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

  // faiss::Index, not faiss::IndexHNSW: graph-free variants (§8.6a) are not
  // HNSW indexes at all. Every use below goes through the base-class interface
  // (is_trained/train/add/search/ntotal); only the two HNSW tuning knobs need a
  // cast, and they are guarded by isGraphFree.
  std::unique_ptr<faiss::Index> index_;
  std::vector<std::string> id_to_chunk_id_;
  // Mirrors id_to_chunk_id_ for O(1) "is this chunk already here?" checks,
  // which AddChunks uses to skip content-addressed duplicates (§8.6c's delta
  // can name a chunk the base artifact already holds). Every write to
  // id_to_chunk_id_ must go through SetChunkIDTable / ResetChunkIDTable so
  // the two never drift apart.
  std::unordered_set<std::string> known_chunk_ids_;
  MetricType metric_ = MetricType::COSINE;
  int dim_ = 0;
  QuantizerConfig config_;
  // Whether index_ stores quantized codes (SQ/PQ). True only after a
  // quantized Build/AddChunks or a Load of a quantized file; Reset clears
  // it. Full-precision (Flat) indexes always have quantized_ == false.
  bool quantized_ = false;

  // 码本基线（docs/codebook-refresh-plan.md §3）。它回答"当前码本是在多大的
  // 语料上训出来的、之后被追加复用了多少次"，是 Go 侧判断"要不要放弃追加复用、
  // 全量重建以重训码本"的依据。两者都随 sidecar 跨版本继承：追加复用改不了码本，
  // 所以值是继承来的；全量重建（新建索引）才是新基线。故意放在 sidecar 而不是
  // 内存里 —— 内存表重启即丢，基线一丢机制就静默失效（该文档 §7 风险 1）。
  //
  // trained_ntotal_ == 0 表示未知（sidecar 里没有这一行，早于本机制写下的产物）。
  // 刻意不把"未知"折算成当期 ntotal：那等于宣告"刚训过"，会让机制就此失效。
  int64_t trained_ntotal_ = 0;
  int64_t appends_since_train_ = 0;
  // baseline_pending_ 表示"本对象刚新建了索引 ⇒ 码本就是此刻训出来的"，
  // Save 时据此把基线写成 (index_->ntotal, 0)。
  bool baseline_pending_ = false;
  // loaded_for_append_ 表示当前内容来自 LoadForAppend。Save 时它意味着
  // "码本没变，只是这个版本又多追加了一批"，据此把追加计数 +1。
  bool loaded_for_append_ = false;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_SRC_HNSW_INDEX_H_
