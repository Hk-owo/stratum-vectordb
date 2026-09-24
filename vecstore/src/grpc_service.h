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
#include <vector>

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
  // index_dirs are the ONLY directories the on-disk index RPCs may touch
  // (Save, Load, LoadForAppend, ExistsIndex). They exist because those RPCs take a
  // filesystem path straight from the wire: without this, any caller that can
  // reach the vecstore port reads and writes arbitrary files as the vecstore
  // process — with the port unauthenticated, that is "whoever can connect"
  // (M4 of docs/code-review-2026-09-24.md).
  //
  // Passed in normalized (see NormalizeIndexDirs in grpc_service.cpp). Empty means
  // every path is refused: a vecstore that was not told where its indexes live has
  // no business touching the filesystem on a caller's behalf.
  VectorIndexServiceImpl(ChunkStorage* storage, std::vector<std::string> index_dirs)
      : index_dirs_(std::move(index_dirs)), storage_(storage) {}

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
  grpc::Status LoadForAppend(
      grpc::ServerContext* context,
      const ::vecstore::LoadIndexForAppendRequest* request,
      ::vecstore::LoadIndexForAppendResponse* response) override;
  grpc::Status ExistsIndex(grpc::ServerContext* context,
                            const ::vecstore::ExistsIndexRequest* request,
                            ::vecstore::ExistsIndexResponse* response) override;
  grpc::Status RemoveChunks(grpc::ServerContext* context,
                            const ::vecstore::RemoveChunksRequest* request,
                            ::vecstore::RemoveChunksResponse* response) override;
  grpc::Status Reset(grpc::ServerContext* context,
                      const ::vecstore::ResetIndexRequest* request,
                      ::vecstore::ResetIndexResponse* response) override;
  grpc::Status Drop(grpc::ServerContext* context,
                     const ::vecstore::DropIndexRequest* request,
                     ::vecstore::DropIndexResponse* response) override;

 private:
  using IndexKey = std::pair<std::string, int64_t>;  // (kb_id, version_id)

  // GetOrCreateLocked returns the VectorIndex for key, constructing a new
  // HNSWVectorIndex (with config, which is only applied on creation) if
  // one does not already exist. Must be called with mu_ held.
  //
  // A caller that passes no config (AddChunks, Load) means "whatever is
  // already there, or the default": it must never replace an existing
  // index, or a batched build would drop the shape the preceding Build
  // asked for. Callers that know which shape they want use
  // GetOrCreateForShapeLocked instead.
  std::shared_ptr<VectorIndex> GetOrCreateLocked(const IndexKey& key,
                                                 const QuantizerConfig& config = {});

  // GetOrCreateForShapeLocked is GetOrCreateLocked for a caller that
  // requires a specific shape (Build). When the resident index was built
  // with a different config, it is replaced: the quantizer/graph choice
  // is fixed at construction, so reusing the object would silently keep
  // the old shape — which is exactly what §8.6a's cold reshape must not
  // do. Must be called with mu_ held.
  std::shared_ptr<VectorIndex> GetOrCreateForShapeLocked(const IndexKey& key,
                                                         const QuantizerConfig& config);

  // FileExists reports whether path exists and is a regular file. Used by
  // ExistsIndex's stateless on-disk existence check.
  static bool FileExists(const std::string& path);

  // IndexPathAllowed reports whether path may be touched, and why not when it may
  // not. Every on-disk RPC goes through it (M4 of docs/code-review-2026-09-24.md).
  bool IndexPathAllowed(const std::string& path, std::string* reason) const;

  // index_dirs_ are the normalized allowed roots (see the constructor).
  std::vector<std::string> index_dirs_;

  std::mutex mu_;
  // shared_ptr, not unique_ptr, and the reason is Search: it resolves its
  // index under mu_ and then uses it with the lock RELEASED (the rerank
  // reads the chunk store, which must not happen while holding a lock that
  // Build and every other index RPC needs). In that window a concurrent
  // Build for the same key may replace the entry — a cold reshape or a
  // quantizer change — which destroys the object being searched. Holding a
  // shared_ptr copy across the unlock keeps the object alive until the
  // search returns; with unique_ptr it was a use-after-free.
  std::map<IndexKey, std::shared_ptr<VectorIndex>> indexes_;
  ChunkStorage* storage_;  // not owned; supplied by VecstoreGrpcServer
};

// VecstoreGrpcServer owns a ChunkStorage, a VectorIndexServiceImpl, and a
// real grpc::Server hosting both services.
class VecstoreGrpcServer {
 public:
  // Takes ownership of storage; it is kept alive for the server's
  // lifetime. index_dirs are the directories the on-disk index RPCs may touch;
  // they are normalized here and empty means "refuse every path" (see
  // VectorIndexServiceImpl).
  VecstoreGrpcServer(std::unique_ptr<ChunkStorage> storage,
                     std::vector<std::string> index_dirs = {});
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
