#include "vecstore/src/grpc_service.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "grpcpp/grpcpp.h"
#include "vecstore.grpc.pb.h"
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
  return ToGrpcStatus(storage_->DeleteByPrefix(request->prefix()));
}

// ---------------------------------------------------------------------------
// VectorIndexServiceImpl
// ---------------------------------------------------------------------------

VectorIndex* VectorIndexServiceImpl::GetOrCreateLocked(const IndexKey& key) {
  auto it = indexes_.find(key);
  if (it != indexes_.end()) {
    return it->second.get();
  }
  auto inserted = indexes_.emplace(key, std::make_unique<HNSWVectorIndex>());
  return inserted.first->second.get();
}

grpc::Status VectorIndexServiceImpl::Build(grpc::ServerContext* /*context*/,
                                            const ::vecstore::BuildIndexRequest* request,
                                            ::vecstore::BuildIndexResponse* /*response*/) {
  std::vector<ChunkVector> chunks;
  chunks.reserve(request->chunks_size());
  for (const auto& proto_chunk : request->chunks()) {
    ChunkVector cv;
    cv.chunk_id = proto_chunk.chunk_id();
    cv.vector.assign(proto_chunk.vector().begin(), proto_chunk.vector().end());
    chunks.push_back(std::move(cv));
  }

  IndexKey key{request->kb_id(), request->version_id()};
  std::lock_guard<std::mutex> lock(mu_);
  VectorIndex* index = GetOrCreateLocked(key);
  return ToGrpcStatus(index->Build(chunks, FromProtoMetric(request->metric())));
}

grpc::Status VectorIndexServiceImpl::Search(grpc::ServerContext* /*context*/,
                                             const ::vecstore::SearchIndexRequest* request,
                                             ::vecstore::SearchIndexResponse* response) {
  IndexKey key{request->kb_id(), request->version_id()};

  VectorIndex* index = nullptr;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = indexes_.find(key);
    if (it == indexes_.end()) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                           "no index built or loaded for kb_id=" + request->kb_id() +
                               " version_id=" + std::to_string(request->version_id()));
    }
    index = it->second.get();
  }

  std::vector<float> query(request->vector().begin(), request->vector().end());
  auto result = index->Search(query, request->top_k());
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
  VectorIndex* index = GetOrCreateLocked(key);
  return ToGrpcStatus(index->Load(request->path()));
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

// ---------------------------------------------------------------------------
// VecstoreGrpcServer
// ---------------------------------------------------------------------------

VecstoreGrpcServer::VecstoreGrpcServer(std::unique_ptr<ChunkStorage> storage)
    : storage_(std::move(storage)),
      chunk_service_(std::make_unique<ChunkStorageServiceImpl>(storage_.get())),
      index_service_(std::make_unique<VectorIndexServiceImpl>()) {}

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
