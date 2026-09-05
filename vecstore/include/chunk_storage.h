// ChunkStorage is the chunk vector persistence interface, backed by
// RocksDB (src/rocksdb_storage.cpp). It is the authoritative store for
// chunk vectors; the Go side never duplicates vector data in PebbleDB —
// ChunkStore (Go) is purely a gRPC client wrapper around this storage,
// accessed via grpc_service.cpp's ChunkStorageService.
//
// Logical call chain:
//   IndexManager.TriggerBuild -> ChunkStorage::Read
//   ChunkStore.Write          -> ChunkStorage::Write
//   ChunkStore.Exists         -> ChunkStorage::Exists
//   ChunkStore.DeleteByKB     -> ChunkStorage::DeleteByPrefix
//
// See Stratum_接口设计v9.md "ChunkStorage" for the authoritative design.
// This header defines only the interface; the RocksDB-backed
// implementation is built in Phase 1 (1-B-1).
#ifndef STRATUM_VECSTORE_CHUNK_STORAGE_H_
#define STRATUM_VECSTORE_CHUNK_STORAGE_H_

#include <map>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace stratum {
namespace vecstore {

// ChunkStorage persists chunk vectors keyed by an opaque string key
// (constructed Go-side as knowledge_base_id + chunk_id). Chunks are
// content-addressed, so writing the same key with the same vector is
// naturally idempotent.
class ChunkStorage {
 public:
  virtual ~ChunkStorage() = default;

  // MultiReadResult is the outcome of a batch read: the found key→vector
  // pairs plus the ordered list of keys that were not present.
  struct MultiReadResult {
    std::map<std::string, std::vector<float>> found;
    std::vector<std::string> missing;
  };

  // Write stores vector under key, overwriting any existing value.
  virtual absl::Status Write(const std::string& key,
                              const std::vector<float>& vector) = 0;

  // Read returns the vector stored under key, or a NotFound status if no
  // such key exists.
  virtual absl::StatusOr<std::vector<float>> Read(const std::string& key) = 0;

  // ReadMulti reads every present key in one call and reports which keys
  // were missing. Used by the two-stage search rerank: given the coarse
  // pass's candidate chunk_ids (encoded into keys with the knowledge
  // base), read back their full-precision vectors for exact re-scoring.
  // Missing keys are collected in MultiReadResult::missing and skipped,
  // never fatal.
  virtual absl::StatusOr<MultiReadResult> ReadMulti(
      const std::vector<std::string>& keys) = 0;

  // Exists reports whether key is currently stored.
  virtual absl::StatusOr<bool> Exists(const std::string& key) = 0;

  // Delete removes a single key. Deleting a key that does not exist is not
  // an error.
  virtual absl::Status Delete(const std::string& key) = 0;

  // DeleteByPrefix removes every key with the given prefix. Used by
  // knowledge-base-scoped deletion (ChunkStore.DeleteByKB), where prefix
  // is the knowledge base ID.
  virtual absl::Status DeleteByPrefix(const std::string& prefix) = 0;

  // DiskUsage returns the approximate on-disk size in bytes used by the
  // storage. Used by GetSystemStatus's resource-usage snapshot.
  virtual absl::StatusOr<uint64_t> DiskUsage() = 0;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_CHUNK_STORAGE_H_
