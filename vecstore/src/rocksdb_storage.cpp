#include "vecstore/src/rocksdb_storage.h"

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

namespace stratum {
namespace vecstore {

namespace {

// EncodeVector packs vec as a flat byte string: each float32 in native
// little-endian byte order, concatenated in order. This is a private
// on-disk format for RocksDBChunkStorage; nothing outside this file reads
// it directly (Go-side ChunkStore only ever sees the vector through the
// vecstore gRPC API, never the raw RocksDB bytes).
std::string EncodeVector(const std::vector<float>& vec) {
  std::string out(vec.size() * sizeof(float), '\0');
  if (!vec.empty()) {
    std::memcpy(out.data(), vec.data(), vec.size() * sizeof(float));
  }
  return out;
}

// DecodeVector is the inverse of EncodeVector.
absl::StatusOr<std::vector<float>> DecodeVector(const std::string& raw) {
  if (raw.size() % sizeof(float) != 0) {
    return absl::DataLossError(
        "rocksdb_storage: stored value size is not a multiple of "
        "sizeof(float); data is corrupt");
  }
  std::vector<float> out(raw.size() / sizeof(float));
  if (!out.empty()) {
    std::memcpy(out.data(), raw.data(), raw.size());
  }
  return out;
}

// ToAbslStatus converts a rocksdb::Status into the equivalent
// absl::Status, preserving the NotFound distinction the ChunkStorage
// interface relies on.
absl::Status ToAbslStatus(const rocksdb::Status& s) {
  if (s.ok()) {
    return absl::OkStatus();
  }
  if (s.IsNotFound()) {
    return absl::NotFoundError(s.ToString());
  }
  return absl::InternalError(s.ToString());
}

// PrefixUpperBound returns the smallest string strictly greater than every
// string with the given prefix, suitable as an exclusive iterator upper
// bound for a prefix scan. Mirrors the same logic as the Go-side
// internal/pebbleutil.PrefixSuccessor, reimplemented here since this is a
// separate C++ codebase with no shared dependency on the Go module.
// Returns an empty optional-equivalent (here: returns false via the bool
// return) if prefix is empty or all 0xFF bytes, meaning "no finite upper
// bound — scan to the end of the keyspace."
bool PrefixUpperBound(const std::string& prefix, std::string* out) {
  std::string succ = prefix;
  for (int i = static_cast<int>(succ.size()) - 1; i >= 0; --i) {
    if (static_cast<unsigned char>(succ[i]) < 0xFF) {
      succ[i] = static_cast<char>(static_cast<unsigned char>(succ[i]) + 1);
      succ.resize(i + 1);
      *out = succ;
      return true;
    }
  }
  return false;
}

}  // namespace

absl::StatusOr<std::unique_ptr<RocksDBChunkStorage>> RocksDBChunkStorage::Open(
    const std::string& path) {
  rocksdb::Options options;
  options.create_if_missing = true;

  rocksdb::DB* raw_db = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &raw_db);
  if (!status.ok()) {
    return absl::InternalError("rocksdb_storage: open " + path + ": " +
                                status.ToString());
  }

  return std::unique_ptr<RocksDBChunkStorage>(
      new RocksDBChunkStorage(std::unique_ptr<rocksdb::DB>(raw_db)));
}

absl::Status RocksDBChunkStorage::Write(const std::string& key,
                                         const std::vector<float>& vector) {
  rocksdb::Status status =
      db_->Put(rocksdb::WriteOptions(), key, EncodeVector(vector));
  return ToAbslStatus(status);
}

absl::StatusOr<std::vector<float>> RocksDBChunkStorage::Read(
    const std::string& key) {
  std::string raw;
  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &raw);
  if (!status.ok()) {
    return ToAbslStatus(status);
  }
  return DecodeVector(raw);
}

absl::StatusOr<ChunkStorage::MultiReadResult> RocksDBChunkStorage::ReadMulti(
    const std::vector<std::string>& keys) {
  if (keys.empty()) {
    return ChunkStorage::MultiReadResult{};
  }

  std::vector<rocksdb::Slice> key_slices;
  key_slices.reserve(keys.size());
  for (const auto& key : keys) {
    key_slices.emplace_back(key);
  }

  std::vector<std::string> values(keys.size());
  std::vector<rocksdb::Status> statuses =
      db_->MultiGet(rocksdb::ReadOptions(), key_slices, &values);

  MultiReadResult result;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (statuses[i].ok()) {
      auto vector_or = DecodeVector(values[i]);
      if (!vector_or.ok()) {
        return vector_or.status();
      }
      result.found.emplace(keys[i], std::move(vector_or.value()));
    } else if (statuses[i].IsNotFound()) {
      result.missing.push_back(keys[i]);
    } else {
      return ToAbslStatus(statuses[i]);
    }
  }
  return result;
}

absl::StatusOr<bool> RocksDBChunkStorage::Exists(const std::string& key) {
  std::string raw;  // unused; RocksDB has no cheap existence-only check
                     // that's simpler than Get for this use case, since
                     // KeyMayExist() can still false-positive and would
                     // need a Get to confirm anyway.
  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &raw);
  if (status.ok()) {
    return true;
  }
  if (status.IsNotFound()) {
    return false;
  }
  return ToAbslStatus(status);
}

absl::Status RocksDBChunkStorage::Delete(const std::string& key) {
  rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), key);
  return ToAbslStatus(status);
}

absl::Status RocksDBChunkStorage::DeleteByPrefix(const std::string& prefix) {
  std::string upper_bound;
  bool has_upper_bound = PrefixUpperBound(prefix, &upper_bound);

  rocksdb::ReadOptions read_options;
  rocksdb::Slice upper_bound_slice;
  if (has_upper_bound) {
    upper_bound_slice = rocksdb::Slice(upper_bound);
    read_options.iterate_upper_bound = &upper_bound_slice;
  }

  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));
  rocksdb::WriteBatch batch;
  for (it->Seek(prefix); it->Valid(); it->Next()) {
    batch.Delete(it->key());
  }
  if (!it->status().ok()) {
    return ToAbslStatus(it->status());
  }

  rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
  return ToAbslStatus(status);
}

absl::StatusOr<uint64_t> RocksDBChunkStorage::DiskUsage() {
  uint64_t size = 0;
  if (!db_->GetIntProperty("rocksdb.estimate-live-data-size", &size)) {
    // Property unavailable (e.g. unsupported RocksDB build). Treat as zero
    // rather than failing the resource-usage snapshot.
    return uint64_t{0};
  }
  return size;
}

}  // namespace vecstore
}  // namespace stratum
