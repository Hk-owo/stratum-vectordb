// rocksdb_storage_test.cpp — T1-8 ChunkStorage test suite, per
// Stratum_测试顺序.md. Written before src/rocksdb_storage.cpp exists
// (TDD): this file does not compile until RocksDBChunkStorage is added.
//
// Each test opens a fresh RocksDB instance in a unique temporary
// directory so tests do not interfere with each other.
#include "vecstore/include/chunk_storage.h"

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/src/rocksdb_storage.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

class RocksDBChunkStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() /
                ("stratum_rocksdb_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
                 "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(test_dir_);

    auto storage_or = RocksDBChunkStorage::Open(test_dir_.string());
    ASSERT_TRUE(storage_or.ok()) << storage_or.status();
    storage_ = std::move(storage_or.value());
  }

  void TearDown() override {
    storage_.reset();
    fs::remove_all(test_dir_);
  }

  fs::path test_dir_;
  std::unique_ptr<RocksDBChunkStorage> storage_;
};

TEST_F(RocksDBChunkStorageTest, WriteThenReadReturnsSameVector) {
  std::vector<float> vec = {1.0f, 2.0f, 3.5f, -4.25f};
  ASSERT_TRUE(storage_->Write("kb1#chunk1", vec).ok());

  auto read = storage_->Read("kb1#chunk1");
  ASSERT_TRUE(read.ok()) << read.status();
  EXPECT_EQ(read.value(), vec);
}

TEST_F(RocksDBChunkStorageTest, ReadMissingKeyReturnsNotFound) {
  auto read = storage_->Read("does-not-exist");
  EXPECT_FALSE(read.ok());
  EXPECT_EQ(read.status().code(), absl::StatusCode::kNotFound);
}

TEST_F(RocksDBChunkStorageTest, ExistsTrueAfterWriteFalseAfterDelete) {
  std::vector<float> vec = {1.0f, 2.0f};
  ASSERT_TRUE(storage_->Write("kb1#chunk1", vec).ok());

  auto exists_before = storage_->Exists("kb1#chunk1");
  ASSERT_TRUE(exists_before.ok());
  EXPECT_TRUE(exists_before.value());

  ASSERT_TRUE(storage_->Delete("kb1#chunk1").ok());

  auto exists_after = storage_->Exists("kb1#chunk1");
  ASSERT_TRUE(exists_after.ok());
  EXPECT_FALSE(exists_after.value());
}

TEST_F(RocksDBChunkStorageTest, ExistsFalseForNeverWrittenKey) {
  auto exists = storage_->Exists("never-written");
  ASSERT_TRUE(exists.ok());
  EXPECT_FALSE(exists.value());
}

TEST_F(RocksDBChunkStorageTest, DeletingNonexistentKeyIsNotAnError) {
  EXPECT_TRUE(storage_->Delete("never-written").ok());
}

TEST_F(RocksDBChunkStorageTest, DeleteByPrefixClearsMatchingKeysOnly) {
  const std::string kb1 = EncodeKBPrefix("kb1");
  const std::string kb2 = EncodeKBPrefix("kb2");
  ASSERT_TRUE(storage_->Write(EncodeKey("kb1", "chunk1"), {1.0f}).ok());
  ASSERT_TRUE(storage_->Write(EncodeKey("kb1", "chunk2"), {2.0f}).ok());
  ASSERT_TRUE(storage_->Write(EncodeKey("kb2", "chunk1"), {3.0f}).ok());

  ASSERT_TRUE(storage_->DeleteByPrefix(kb1).ok());

  auto e1 = storage_->Exists(EncodeKey("kb1", "chunk1"));
  auto e2 = storage_->Exists(EncodeKey("kb1", "chunk2"));
  auto e3 = storage_->Exists(EncodeKey("kb2", "chunk1"));
  ASSERT_TRUE(e1.ok() && e2.ok() && e3.ok());
  EXPECT_FALSE(e1.value());
  EXPECT_FALSE(e2.value());
  EXPECT_TRUE(e3.value()) << "DeleteByPrefix(kb1) must not remove kb2's keys";
}

TEST_F(RocksDBChunkStorageTest, DeleteByPrefixOnEmptyStoreIsNotAnError) {
  EXPECT_TRUE(storage_->DeleteByPrefix(EncodeKBPrefix("anything")).ok());
}

// The prefix is the whole of a DeleteByPrefix's scope, and the one prefix that
// names nothing is the one that would otherwise name EVERYTHING: an empty
// prefix has no byte for PrefixUpperBound to bump, so the scan runs to the end
// of the keyspace and batch-deletes every chunk vector in the store. Refusing
// it is the difference between a per-KB cleanup and a wipe, so the test pins
// both halves: the call fails, and the data it would have taken is still there.
TEST_F(RocksDBChunkStorageTest, DeleteByPrefixRefusesPrefixThatNamesNoKnowledgeBase) {
  ASSERT_TRUE(storage_->Write(EncodeKey("kb1", "chunk1"), {1.0f}).ok());
  ASSERT_TRUE(storage_->Write(EncodeKey("kb2", "chunk1"), {2.0f}).ok());

  // Declared kb_id length (9) disagrees with the 3 bytes that follow it: a
  // truncated kb_id would leave the upper bound pointing past other knowledge
  // bases, so the scan would not stop where the caller meant it to.
  const std::string wrong_length = std::string("\x00\x00\x00\x09", 4) + "kb1";
  for (const std::string& bad :
       {std::string(), std::string("kb1"), EncodeKBPrefix(""), wrong_length}) {
    auto refused = storage_->DeleteByPrefix(bad);
    ASSERT_FALSE(refused.ok()) << "a " << bad.size() << "-byte prefix must be refused";
    EXPECT_EQ(refused.code(), absl::StatusCode::kInvalidArgument);
  }

  for (const char* kb : {"kb1", "kb2"}) {
    auto exists = storage_->Exists(EncodeKey(kb, "chunk1"));
    ASSERT_TRUE(exists.ok());
    EXPECT_TRUE(exists.value()) << "the refused calls must not have deleted kb=" << kb;
  }
}

TEST_F(RocksDBChunkStorageTest, WriteOverwritesExistingValue) {
  ASSERT_TRUE(storage_->Write("key1", {1.0f, 2.0f}).ok());
  ASSERT_TRUE(storage_->Write("key1", {9.0f, 9.0f, 9.0f}).ok());

  auto read = storage_->Read("key1");
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read.value(), (std::vector<float>{9.0f, 9.0f, 9.0f}));
}

TEST_F(RocksDBChunkStorageTest, PersistsAcrossReopen) {
  ASSERT_TRUE(storage_->Write("persisted-key", {1.0f, 2.0f, 3.0f}).ok());

  // Close and reopen the same directory.
  storage_.reset();
  auto reopened_or = RocksDBChunkStorage::Open(test_dir_.string());
  ASSERT_TRUE(reopened_or.ok()) << reopened_or.status();
  storage_ = std::move(reopened_or.value());

  auto read = storage_->Read("persisted-key");
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read.value(), (std::vector<float>{1.0f, 2.0f, 3.0f}));
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
