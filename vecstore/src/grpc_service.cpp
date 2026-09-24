#include "vecstore/src/grpc_service.h"

#include <sys/stat.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "grpcpp/grpcpp.h"
#include "vecstore.grpc.pb.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/include/types.h"
#include "vecstore/src/hnsw_index.h"

namespace stratum {
namespace vecstore {

namespace {

// ToGrpcStatus converts an absl::Status into the equivalent grpc::Status,
// preserving the NotFound / InvalidArgument distinctions callers rely on.
// This is the C++/gRPC analog of the Go side's
// internal/errors.ToGRPCStatus — same idea (one conversion point, used by
// every RPC handler), different language.
grpc::Status ToGrpcStatus(const absl::Status& s) {
  if (s.ok()) {
    return grpc::Status::OK;
  }
  switch (s.code()) {
    case absl::StatusCode::kNotFound:
      return grpc::Status(grpc::StatusCode::NOT_FOUND, std::string(s.message()));
    case absl::StatusCode::kInvalidArgument:
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, std::string(s.message()));
    case absl::StatusCode::kFailedPrecondition:
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, std::string(s.message()));
    default:
      return grpc::Status(grpc::StatusCode::INTERNAL, std::string(s.message()));
  }
}

MetricType FromProtoMetric(::vecstore::MetricTypeProto proto_metric) {
  switch (proto_metric) {
    case ::vecstore::EUCLIDEAN:
      return MetricType::EUCLIDEAN;
    case ::vecstore::INNER_PRODUCT:
      return MetricType::INNER_PRODUCT;
    case ::vecstore::COSINE:
    default:
      return MetricType::COSINE;
  }
}

QuantizerConfig FromProtoQuantizer(::vecstore::QuantizerTypeProto proto_quantizer,
                                   int pq_m, int pq_nbits) {
  QuantizerConfig cfg;
  switch (proto_quantizer) {
    case ::vecstore::QUANTIZER_SQ8:
      cfg.type = QuantizerType::kSQ8;
      break;
    case ::vecstore::QUANTIZER_SQ_BF16:
      cfg.type = QuantizerType::kSQBF16;
      break;
    case ::vecstore::QUANTIZER_SQ_FP16:
      cfg.type = QuantizerType::kSQFP16;
      break;
    case ::vecstore::QUANTIZER_PQ:
      cfg.type = QuantizerType::kPQ;
      break;
    case ::vecstore::QUANTIZER_OFF_FLAT:
      cfg.type = QuantizerType::kOffFlat;
      break;
    case ::vecstore::QUANTIZER_SQ8_FLAT:
      cfg.type = QuantizerType::kSQ8Flat;
      break;
    case ::vecstore::QUANTIZER_SQ_BF16_FLAT:
      cfg.type = QuantizerType::kSQBF16Flat;
      break;
    case ::vecstore::QUANTIZER_SQ_FP16_FLAT:
      cfg.type = QuantizerType::kSQFP16Flat;
      break;
    case ::vecstore::QUANTIZER_PQ_FLAT:
      cfg.type = QuantizerType::kPQFlat;
      break;
    case ::vecstore::QUANTIZER_OFF:
    default:
      cfg.type = QuantizerType::kOff;
      break;
  }
  if (pq_m > 0) {
    cfg.pq_m = pq_m;
  }
  if (pq_nbits > 0) {
    cfg.pq_nbits = pq_nbits;
  }
  return cfg;
}

// Coarse-pass candidate budget for the two-stage search (Stratum_设计文档
// v12.md 2.2): N = clamp(top_k * multiplier). This is a server-side
// default for now; the per-request override (SearchIndexRequest.candidate_n)
// lands with the proto/config work in stage ②.
constexpr int kCandidateMultiplier = 8;
constexpr int kMinCandidates = 16;
constexpr int kMaxCandidates = 4096;

int CandidateCountFor(int top_k) {
  return std::clamp(top_k * kCandidateMultiplier, kMinCandidates, kMaxCandidates);
}

}  // namespace

// ---------------------------------------------------------------------------
// ChunkStorageServiceImpl
// ---------------------------------------------------------------------------

grpc::Status ChunkStorageServiceImpl::Write(grpc::ServerContext* /*context*/,
                                             const ::vecstore::WriteChunkRequest* request,
                                             ::vecstore::WriteChunkResponse* /*response*/) {
  std::vector<float> vec(request->vector().begin(), request->vector().end());
  return ToGrpcStatus(storage_->Write(request->key(), vec));
}

grpc::Status ChunkStorageServiceImpl::Read(grpc::ServerContext* /*context*/,
                                            const ::vecstore::ReadChunkRequest* request,
                                            ::vecstore::ReadChunkResponse* response) {
  auto result = storage_->Read(request->key());
  if (!result.ok()) {
    return ToGrpcStatus(result.status());
  }
  for (float v : result.value()) {
    response->add_vector(v);
  }
  return grpc::Status::OK;
}

grpc::Status ChunkStorageServiceImpl::Exists(grpc::ServerContext* /*context*/,
                                              const ::vecstore::ExistsChunkRequest* request,
                                              ::vecstore::ExistsChunkResponse* response) {
  auto result = storage_->Exists(request->key());
  if (!result.ok()) {
    return ToGrpcStatus(result.status());
  }
  response->set_exists(result.value());
  return grpc::Status::OK;
}

grpc::Status ChunkStorageServiceImpl::Delete(grpc::ServerContext* /*context*/,
                                              const ::vecstore::DeleteChunkRequest* request,
                                              ::vecstore::DeleteChunkResponse* /*response*/) {
  return ToGrpcStatus(storage_->Delete(request->key()));
}

grpc::Status ChunkStorageServiceImpl::DeleteByPrefix(
    grpc::ServerContext* /*context*/, const ::vecstore::DeleteByPrefixRequest* request,
    ::vecstore::DeleteByPrefixResponse* /*response*/) {
  // The boundary check is stated here as well as inside the storage
  // implementation, because this handler is what an unauthenticated caller
  // actually reaches, and the failure mode is total: an empty prefix leaves
  // the scan unbounded and empties the store. The duplicate is deliberate —
  // the storage copy also protects a caller that talks to the store directly.
  if (!IsKBPrefix(request->prefix())) {
    return ToGrpcStatus(absl::InvalidArgumentError(
        "vecstore: DeleteByPrefix: prefix must be the length-prefixed encoding "
        "of a non-empty kb_id"));
  }
  return ToGrpcStatus(storage_->DeleteByPrefix(request->prefix()));
}

grpc::Status ChunkStorageServiceImpl::DiskUsage(
    grpc::ServerContext* /*context*/, const ::vecstore::DiskUsageRequest* /*request*/,
    ::vecstore::DiskUsageResponse* response) {
  auto result = storage_->DiskUsage();
  if (!result.ok()) {
    return ToGrpcStatus(result.status());
  }
  response->set_bytes(result.value());
  return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// VectorIndexServiceImpl
// ---------------------------------------------------------------------------

namespace {

// ValidateChunkVectors rejects a Build/AddChunks batch that contains a
// zero-length vector.
//
// proto3 does not distinguish "unset" from "empty" for a repeated field, so a
// chunk whose vector was never filled in arrives as a zero-dimension vector
// rather than as a missing field. Zero dimensions cannot be indexed, and the
// damage is not "an empty index": downstream, the batch is flattened into one
// contiguous buffer and divided by the dimension, and `flat.size() / 0` is a
// division by zero — undefined behaviour, and on x86 a SIGFPE. That is a
// signal, not a std::exception, so the try/catch that guards the faiss calls
// cannot intercept it and the process dies.
absl::Status ValidateChunkVectors(const std::vector<ChunkVector>& chunks,
                                  const char* rpc) {
  for (const auto& c : chunks) {
    if (c.vector.empty()) {
      return absl::InvalidArgumentError(
          std::string("vecstore: ") + rpc + ": chunk " + c.chunk_id +
          " carries an empty vector; indexes must have dimension > 0");
    }
  }
  return absl::OkStatus();
}

}  // namespace

std::shared_ptr<VectorIndex> VectorIndexServiceImpl::GetOrCreateLocked(
    const IndexKey& key, const QuantizerConfig& config) {
  auto it = indexes_.find(key);
  if (it != indexes_.end()) {
    return it->second;
  }
  auto inserted =
      indexes_.emplace(key, std::make_shared<HNSWVectorIndex>(config));
  return inserted.first->second;
}

std::shared_ptr<VectorIndex> VectorIndexServiceImpl::GetOrCreateForShapeLocked(
    const IndexKey& key, const QuantizerConfig& config) {
  auto it = indexes_.find(key);
  if (it != indexes_.end()) {
    if (it->second->MatchesConfig(config)) {
      return it->second;
    }
    // The resident index has a different shape (a §8.6a cold reshape
    // swapping graphed for graph-free, or a KB quantizer change). The
    // shape is fixed when the object is constructed, so replace it
    // rather than silently rebuilding the old shape.
    //
    // The replacement drops this map's reference, not the object: a Search
    // that resolved the old index and released mu_ still holds its own
    // shared_ptr, so the reshape cannot free memory out from under it.
    it->second = std::make_shared<HNSWVectorIndex>(config);
    return it->second;
  }
  auto inserted =
      indexes_.emplace(key, std::make_shared<HNSWVectorIndex>(config));
  return inserted.first->second;
}

grpc::Status VectorIndexServiceImpl::Build(grpc::ServerContext* /*context*/,
                                            const ::vecstore::BuildIndexRequest* request,
                                            ::vecstore::BuildIndexResponse* response) {
  std::vector<ChunkVector> chunks;
  chunks.reserve(request->chunks_size());
  for (const auto& proto_chunk : request->chunks()) {
    ChunkVector cv;
    cv.chunk_id = proto_chunk.chunk_id();
    cv.vector.assign(proto_chunk.vector().begin(), proto_chunk.vector().end());
    chunks.push_back(std::move(cv));
  }

  IndexKey key{request->kb_id(), request->version_id()};
  const QuantizerConfig config = FromProtoQuantizer(
      request->quantizer(), request->pq_m(), request->pq_nbits());
  if (const absl::Status bad = ValidateChunkVectors(chunks, "Build"); !bad.ok()) {
    return ToGrpcStatus(bad);
  }
  std::lock_guard<std::mutex> lock(mu_);
  // Build names the shape it wants, so a reshape (§8.6a) replaces the
  // resident index rather than rebuilding the shape it already had.
  std::shared_ptr<VectorIndex> index = GetOrCreateForShapeLocked(key, config);
  absl::Status status = index->Build(chunks, FromProtoMetric(request->metric()));
  if (!status.ok()) {
    return ToGrpcStatus(status);
  }
  response->set_mem_bytes(index->EstimatedMemoryBytes());
  return grpc::Status::OK;
}

grpc::Status VectorIndexServiceImpl::AddChunks(grpc::ServerContext* /*context*/,
                                                const ::vecstore::AddChunksRequest* request,
                                                ::vecstore::AddChunksResponse* response) {
  std::vector<ChunkVector> chunks;
  chunks.reserve(request->chunks_size());
  for (const auto& proto_chunk : request->chunks()) {
    ChunkVector cv;
    cv.chunk_id = proto_chunk.chunk_id();
    cv.vector.assign(proto_chunk.vector().begin(), proto_chunk.vector().end());
    chunks.push_back(std::move(cv));
  }

  IndexKey key{request->kb_id(), request->version_id()};
  if (const absl::Status bad = ValidateChunkVectors(chunks, "AddChunks"); !bad.ok()) {
    return ToGrpcStatus(bad);
  }
  std::lock_guard<std::mutex> lock(mu_);
  std::shared_ptr<VectorIndex> index = GetOrCreateLocked(key);
  absl::Status status = index->AddChunks(chunks);
  if (!status.ok()) {
    return ToGrpcStatus(status);
  }
  response->set_mem_bytes(index->EstimatedMemoryBytes());
  return grpc::Status::OK;
}

grpc::Status VectorIndexServiceImpl::Search(grpc::ServerContext* /*context*/,
                                             const ::vecstore::SearchIndexRequest* request,
                                             ::vecstore::SearchIndexResponse* response) {
  IndexKey key{request->kb_id(), request->version_id()};

  std::shared_ptr<VectorIndex> index;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = indexes_.find(key);
    if (it == indexes_.end()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                           "no index built or loaded for kb_id=" + request->kb_id() +
                               " version_id=" + std::to_string(request->version_id()));
    }
    // A copy, not the map's entry: the search below runs with mu_ released
    // (the rerank reads the chunk store), and a concurrent Build may swap the
    // entry for this key in that window. The shared_ptr keeps THIS index
    // alive until the search is done, which is the whole point of storing
    // them as shared_ptr (see indexes_ in grpc_service.h).
    index = it->second;
  }

  std::vector<float> query(request->vector().begin(), request->vector().end());
  // Two-stage search: full-precision indexes resolve exactly in memory
  // (no disk reads); quantized indexes coarse-search candidate_n
  // candidates, read their full-precision vectors back from the chunk
  // store, and re-rank (SearchWithRerank). candidate_n == 0 falls back to
  // the server-side default.
  const int candidate_n = request->candidate_n() > 0
                              ? request->candidate_n()
                              : CandidateCountFor(request->top_k());
  auto result = index->SearchWithRerank(storage_, request->kb_id(), query,
                                        request->top_k(), candidate_n);
  if (!result.ok()) {
    return ToGrpcStatus(result.status());
  }
  for (const auto& r : result.value()) {
    auto* proto_result = response->add_results();
    proto_result->set_chunk_id(r.chunk_id);
    proto_result->set_score(r.score);
  }
  return grpc::Status::OK;
}

grpc::Status VectorIndexServiceImpl::Save(grpc::ServerContext* /*context*/,
                                           const ::vecstore::SaveIndexRequest* request,
                                           ::vecstore::SaveIndexResponse* /*response*/) {
  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  auto it = indexes_.find(key);
  if (it == indexes_.end()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                         "no index built or loaded for kb_id=" + request->kb_id() +
                             " version_id=" + std::to_string(request->version_id()));
  }
  return ToGrpcStatus(it->second->Save(request->path()));
}

grpc::Status VectorIndexServiceImpl::Load(grpc::ServerContext* /*context*/,
                                           const ::vecstore::LoadIndexRequest* request,
                                           ::vecstore::LoadIndexResponse* /*response*/) {
  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  std::shared_ptr<VectorIndex> index = GetOrCreateLocked(key);
  return ToGrpcStatus(index->Load(request->path()));
}

grpc::Status VectorIndexServiceImpl::LoadForAppend(
    grpc::ServerContext* /*context*/,
    const ::vecstore::LoadIndexForAppendRequest* request,
    ::vecstore::LoadIndexForAppendResponse* response) {
  // The object belongs to (kb_id, version_id) — the version being built —
  // while `path` names the base artifact it starts from (§8.6c). No config
  // is passed: the base file is self-describing and Load restores the
  // retrieval mode from the stored type, so reusing whatever object is
  // resident is correct here. A shape change still goes through Build,
  // which does replace the object (see GetOrCreateForShapeLocked).
  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  std::shared_ptr<VectorIndex> index = GetOrCreateLocked(key);
  const absl::Status status = index->LoadForAppend(request->path());
  if (status.ok()) {
    // How big the base artifact is: the caller compares it with the chunk set
    // this version needs to see how many of those vectors became tombstones.
    response->set_base_ntotal(index->TotalVectors());
  }
  return ToGrpcStatus(status);
}

grpc::Status VectorIndexServiceImpl::ExistsIndex(grpc::ServerContext* /*context*/,
                                                  const ::vecstore::ExistsIndexRequest* request,
                                                  ::vecstore::ExistsIndexResponse* response) {
  // Stateless: inspect the filesystem only. A persisted index consists of
  // the Faiss file plus the .ids sidecar — both must exist for Load to
  // succeed (see HNSWVectorIndex::Load). This answers correctly even right
  // after this process restarted, when indexes_ is empty but the on-disk
  // files from a previous run are still there.
  const std::string path = request->path();
  response->set_exists(FileExists(path) && FileExists(path + ".ids"));
  return grpc::Status::OK;
}

// FileExists reports whether path exists and is a regular file. Used by
// ExistsIndex.
bool VectorIndexServiceImpl::FileExists(const std::string& path) {
  struct stat st {};
  return ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

grpc::Status VectorIndexServiceImpl::RemoveChunks(
    grpc::ServerContext* /*context*/,
    const ::vecstore::RemoveChunksRequest* request,
    ::vecstore::RemoveChunksResponse* response) {
  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  // Deliberately not GetOrCreateLocked: asking to remove vectors from an index
  // that does not exist here is a caller error, not a reason to materialise an
  // empty one.
  auto it = indexes_.find(key);
  if (it == indexes_.end()) {
    return ToGrpcStatus(absl::FailedPreconditionError(
        "vecstore: RemoveChunks: no index for this (kb_id, version_id)"));
  }
  const std::vector<std::string> chunk_ids(request->chunk_ids().begin(),
                                           request->chunk_ids().end());
  auto removed_or = it->second->RemoveChunks(chunk_ids);
  if (!removed_or.ok()) {
    return ToGrpcStatus(removed_or.status());
  }
  response->set_removed(static_cast<int64_t>(*removed_or));
  response->set_ntotal(it->second->TotalVectors());
  return grpc::Status::OK;
}

grpc::Status VectorIndexServiceImpl::Reset(grpc::ServerContext* /*context*/,
                                            const ::vecstore::ResetIndexRequest* request,
                                            ::vecstore::ResetIndexResponse* /*response*/) {
  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  auto it = indexes_.find(key);
  if (it == indexes_.end()) {
    return grpc::Status::OK;  // resetting a never-built index is a no-op, not an error
  }
  return ToGrpcStatus(it->second->Reset());
}

grpc::Status VectorIndexServiceImpl::Drop(grpc::ServerContext* /*context*/,
                                           const ::vecstore::DropIndexRequest* request,
                                           ::vecstore::DropIndexResponse* /*response*/) {
  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  // erase() is what actually frees the object — and with it the Faiss index and
  // the id table, the two things that make an index big. Reset (called by the Go
  // side's Discard before this existed) empties an index but keeps the object, so
  // the map only ever grew: a process's RSS tracked how many versions had been
  // touched, not how many it was holding (H5 of docs/code-review-2026-09-24.md).
  //
  // A shared_ptr keeps a concurrent Search's copy alive until that search
  // returns, so this cannot pull the object out from under a reader (see
  // indexes_ in grpc_service.h).
  //
  // Idempotent: erasing a missing key leaves the store in exactly the state the
  // caller asked for. The on-disk artifact is untouched — this reclaims memory.
  indexes_.erase(key);
  return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// VecstoreGrpcServer
// ---------------------------------------------------------------------------

VecstoreGrpcServer::VecstoreGrpcServer(std::unique_ptr<ChunkStorage> storage)
    : storage_(std::move(storage)),
      chunk_service_(std::make_unique<ChunkStorageServiceImpl>(storage_.get())),
      index_service_(std::make_unique<VectorIndexServiceImpl>(storage_.get())) {}

VecstoreGrpcServer::~VecstoreGrpcServer() { Shutdown(); }

bool VecstoreGrpcServer::Start(const std::string& address) {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(chunk_service_.get());
  builder.RegisterService(index_service_.get());
  server_ = builder.BuildAndStart();
  return server_ != nullptr;
}

int VecstoreGrpcServer::StartOnLoopbackWithEphemeralPort() {
  grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &selected_port);
  builder.RegisterService(chunk_service_.get());
  builder.RegisterService(index_service_.get());
  server_ = builder.BuildAndStart();
  if (server_ == nullptr) {
    return 0;
  }
  return selected_port;
}

void VecstoreGrpcServer::Shutdown() {
  if (server_ != nullptr) {
    server_->Shutdown();
    server_.reset();
  }
}

}  // namespace vecstore
}  // namespace stratum
