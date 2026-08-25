// VecstoreGrpcServer implements the vecstore internal gRPC contract
// (ChunkStorageService / VectorIndexService, see vecstore.proto) on top of
// a ChunkStorage and a set of per-(kb_id, version_id) VectorIndex
// instances, and hosts a real grpc::Server.
//
// This is the Go <-> C++ internal communication entry point referenced
// throughout Stratum_接口设计v9.md as "[内部 gRPC]" — Go-side ChunkStore and
// IndexManager are gRPC clients of this server.
#ifndef STRATUM_VECSTORE_SRC_GRPC_SERVICE_H_
#define STRATUM_VECSTORE_SRC_GRPC_SERVICE_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "grpcpp/grpcpp.h"
#include "vecstore.grpc.pb.h"
#include "vecstore/include/chunk_storage.h"
#include "vecstore/include/vector_index.h"

namespace stratum {
namespace vecstore {

// ChunkStorageServiceImpl adapts a ChunkStorage to the
// vecstore::ChunkStorageService gRPC contract.
class ChunkStorageServiceImpl final : public ::vecstore::ChunkStorageService::Service {
 public:
  explicit ChunkStorageServiceImpl(ChunkStorage* storage) : storage_(storage) {}

  grpc::Status Write(grpc::ServerContext* context,
                      const ::vecstore::WriteChunkRequest* request,
                      ::vecstore::WriteChunkResponse* response) override;
  grpc::Status Read(grpc::ServerContext* context,
                     const ::vecstore::ReadChunkRequest* request,
                     ::vecstore::ReadChunkResponse* response) override;
  grpc::Status Exists(grpc::ServerContext* context,
                       const ::vecstore::ExistsChunkRequest* request,
                       ::vecstore::ExistsChunkResponse* response) override;
  grpc::Status Delete(grpc::ServerContext* context,
                       const ::vecstore::DeleteChunkRequest* request,
                       ::vecstore::DeleteChunkResponse* response) override;
  grpc::Status DeleteByPrefix(grpc::ServerContext* context,
                               const ::vecstore::DeleteByPrefixRequest* request,
                               ::vecstore::DeleteByPrefixResponse* response) override;
  grpc::Status DiskUsage(grpc::ServerContext* context,
                          const ::vecstore::DiskUsageRequest* request,
                          ::vecstore::DiskUsageResponse* response) override;

 private:
  ChunkStorage* storage_;  // not owned; outlives this service
};

// VectorIndexServiceImpl adapts a collection of per-(kb_id, version_id)
// VectorIndex instances to the vecstore::VectorIndexService gRPC
// contract. Instances are created on demand (Build, or Load against a
// not-yet-seen key) and looked up by key for Search/Save/Reset.
//
// Concrete VectorIndex instances are always HNSWVectorIndex today (the
// only real implementation; see Stratum_设计文档v10.md "当前实现：仅
// HNSW").
class VectorIndexServiceImpl final : public ::vecstore::VectorIndexService::Service {
 public:
  VectorIndexServiceImpl() = default;

  grpc::Status Build(grpc::ServerContext* context,
                      const ::vecstore::BuildIndexRequest* request,
                      ::vecstore::BuildIndexResponse* response) override;
  grpc::Status AddChunks(grpc::ServerContext* context,
                          const ::vecstore::AddChunksRequest* request,
                          ::vecstore::AddChunksResponse* response) override;
  grpc::Status Search(grpc::ServerContext* context,
                       const ::vecstore::SearchIndexRequest* request,
                       ::vecstore::SearchIndexResponse* response) override;
  grpc::Status Save(grpc::ServerContext* context,
                     const ::vecstore::SaveIndexRequest* request,
                     ::vecstore::SaveIndexResponse* response) override;
  grpc::Status Load(grpc::ServerContext* context,
                     const ::vecstore::LoadIndexRequest* request,
                     ::vecstore::LoadIndexResponse* response) override;
  grpc::Status ExistsIndex(grpc::ServerContext* context,
                            const ::vecstore::ExistsIndexRequest* request,
                            ::vecstore::ExistsIndexResponse* response) override;
  grpc::Status Reset(grpc::ServerContext* context,
                      const ::vecstore::ResetIndexRequest* request,
                      ::vecstore::ResetIndexResponse* response) override;

 private:
  using IndexKey = std::pair<std::string, int64_t>;  // (kb_id, version_id)

  // GetOrCreateLocked returns the VectorIndex for key, constructing a new
  // HNSWVectorIndex if one does not already exist. Must be called with
  // mu_ held.
  VectorIndex* GetOrCreateLocked(const IndexKey& key);

  // FileExists reports whether path exists and is a regular file. Used by
  // ExistsIndex's stateless on-disk existence check.
  static bool FileExists(const std::string& path);

  std::mutex mu_;
  std::map<IndexKey, std::unique_ptr<VectorIndex>> indexes_;
};

// VecstoreGrpcServer owns a ChunkStorage, a VectorIndexServiceImpl, and a
// real grpc::Server hosting both services.
class VecstoreGrpcServer {
 public:
  // Takes ownership of storage; it is kept alive for the server's
  // lifetime.
  explicit VecstoreGrpcServer(std::unique_ptr<ChunkStorage> storage);
  ~VecstoreGrpcServer();

  VecstoreGrpcServer(const VecstoreGrpcServer&) = delete;
  VecstoreGrpcServer& operator=(const VecstoreGrpcServer&) = delete;

  // Start binds to address (e.g. "0.0.0.0:7100", matching the
  // vecstore.grpc_addr config field) and starts serving in the
  // background. Returns true on success.
  bool Start(const std::string& address);

  // StartOnLoopbackWithEphemeralPort binds to 127.0.0.1 on an OS-assigned
  // free port and starts serving in the background. Returns the bound
  // port, or 0 on failure. Intended for tests that need an isolated,
  // collision-free server instance.
  int StartOnLoopbackWithEphemeralPort();

  // Shutdown stops the server. Safe to call multiple times or never (the
  // destructor calls it if needed).
  void Shutdown();

 private:
  std::unique_ptr<ChunkStorage> storage_;
  std::unique_ptr<ChunkStorageServiceImpl> chunk_service_;
  std::unique_ptr<VectorIndexServiceImpl> index_service_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_SRC_GRPC_SERVICE_H_
