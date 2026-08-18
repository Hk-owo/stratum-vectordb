// RocksDBChunkStorage is the real ChunkStorage implementation, backed by
// RocksDB. See Stratum_接口设计v9.md "ChunkStorage" for the interface
// contract this satisfies.
#ifndef STRATUM_VECSTORE_SRC_ROCKSDB_STORAGE_H_
#define STRATUM_VECSTORE_SRC_ROCKSDB_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "rocksdb/db.h"
#include "vecstore/include/chunk_storage.h"

namespace stratum {
namespace vecstore {

// RocksDBChunkStorage stores each chunk vector as a single RocksDB value:
// a flat little-endian float32 array, with the key passed through exactly
// as given by the caller (Go-side ChunkStore constructs keys as
// knowledge_base_id + chunk_id, but RocksDBChunkStorage itself is
// key-format-agnostic — it just stores bytes under whatever string key it
// is given).
class RocksDBChunkStorage : public ChunkStorage {
 public:
  // Open opens (creating if necessary) a RocksDB database at path and
  // returns a ready-to-use RocksDBChunkStorage, or an error status if the
  // database could not be opened.
  static absl::StatusOr<std::unique_ptr<RocksDBChunkStorage>> Open(
      const std::string& path);

  ~RocksDBChunkStorage() override = default;

  // Not copyable (owns a RocksDB handle); movable.
  RocksDBChunkStorage(const RocksDBChunkStorage&) = delete;
  RocksDBChunkStorage& operator=(const RocksDBChunkStorage&) = delete;
  RocksDBChunkStorage(RocksDBChunkStorage&&) = default;
  RocksDBChunkStorage& operator=(RocksDBChunkStorage&&) = default;

  absl::Status Write(const std::string& key,
                      const std::vector<float>& vector) override;
  absl::StatusOr<std::vector<float>> Read(const std::string& key) override;
  absl::StatusOr<bool> Exists(const std::string& key) override;
  absl::Status Delete(const std::string& key) override;
  absl::Status DeleteByPrefix(const std::string& prefix) override;
  absl::StatusOr<uint64_t> DiskUsage() override;

 private:
  explicit RocksDBChunkStorage(std::unique_ptr<rocksdb::DB> db)
      : db_(std::move(db)) {}

  std::unique_ptr<rocksdb::DB> db_;
};

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_SRC_ROCKSDB_STORAGE_H_
