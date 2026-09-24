// grpc_service_test.cpp — verifies "gRPC 接口正常通信" per the 1-B-3 测试节点
// in Stratum_实现顺序.md (this specific check is not part of the formal
// T1-8 table in Stratum_测试顺序.md, which only covers ChunkStorage and
// VectorIndex directly; this file covers the gRPC plumbing on top of
// those two, exercised end-to-end with a real client and a real server
// over a loopback socket).
//
// Written before src/grpc_service.cpp exists (TDD): this file does not
// compile until VecstoreGrpcServer is added.
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "grpcpp/grpcpp.h"
#include "vecstore.grpc.pb.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/src/grpc_service.h"
#include "vecstore/src/hnsw_index.h"
#include "vecstore/src/rocksdb_storage.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

class GrpcServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() /
                ("stratum_grpc_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);

    auto storage_or = RocksDBChunkStorage::Open((test_dir_ / "rocksdb").string());
    ASSERT_TRUE(storage_or.ok()) << storage_or.status();

    server_ = std::make_unique<VecstoreGrpcServer>(std::move(storage_or.value()));
    int port = server_->StartOnLoopbackWithEphemeralPort();
    ASSERT_GT(port, 0) << "server failed to bind to a port";

    channel_ = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                    grpc::InsecureChannelCredentials());
    chunk_stub_ = ::vecstore::ChunkStorageService::NewStub(channel_);
    index_stub_ = ::vecstore::VectorIndexService::NewStub(channel_);
  }

  void TearDown() override {
    server_->Shutdown();
    fs::remove_all(test_dir_);
  }

  fs::path test_dir_;
  std::unique_ptr<VecstoreGrpcServer> server_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<::vecstore::ChunkStorageService::Stub> chunk_stub_;
  std::unique_ptr<::vecstore::VectorIndexService::Stub> index_stub_;
};

TEST_F(GrpcServiceTest, ChunkStorageWriteReadExistsDeleteRoundTrip) {
  grpc::ClientContext write_ctx;
  ::vecstore::WriteChunkRequest write_req;
  write_req.set_key("kb1#chunk1");
  write_req.add_vector(1.0f);
  write_req.add_vector(2.0f);
  write_req.add_vector(3.0f);
  ::vecstore::WriteChunkResponse write_resp;
  auto status = chunk_stub_->Write(&write_ctx, write_req, &write_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();

  grpc::ClientContext read_ctx;
  ::vecstore::ReadChunkRequest read_req;
  read_req.set_key("kb1#chunk1");
  ::vecstore::ReadChunkResponse read_resp;
  status = chunk_stub_->Read(&read_ctx, read_req, &read_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_EQ(read_resp.vector_size(), 3);
  EXPECT_FLOAT_EQ(read_resp.vector(0), 1.0f);
  EXPECT_FLOAT_EQ(read_resp.vector(1), 2.0f);
  EXPECT_FLOAT_EQ(read_resp.vector(2), 3.0f);

  grpc::ClientContext exists_ctx;
  ::vecstore::ExistsChunkRequest exists_req;
  exists_req.set_key("kb1#chunk1");
  ::vecstore::ExistsChunkResponse exists_resp;
  status = chunk_stub_->Exists(&exists_ctx, exists_req, &exists_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_TRUE(exists_resp.exists());

  grpc::ClientContext delete_ctx;
  ::vecstore::DeleteChunkRequest delete_req;
  delete_req.set_key("kb1#chunk1");
  ::vecstore::DeleteChunkResponse delete_resp;
  status = chunk_stub_->Delete(&delete_ctx, delete_req, &delete_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();

  grpc::ClientContext exists_after_ctx;
  ::vecstore::ExistsChunkResponse exists_after_resp;
  status = chunk_stub_->Exists(&exists_after_ctx, exists_req, &exists_after_resp);
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(exists_after_resp.exists());
}

TEST_F(GrpcServiceTest, ChunkStorageDeleteByPrefix) {
  const std::string kb_a = EncodeKBPrefix("kbA");
  for (const std::string& key :
       {EncodeKey("kbA", "c1"), EncodeKey("kbA", "c2"), EncodeKey("kbB", "c1")}) {
    grpc::ClientContext ctx;
    ::vecstore::WriteChunkRequest req;
    req.set_key(key);
    req.add_vector(1.0f);
    ::vecstore::WriteChunkResponse resp;
    ASSERT_TRUE(chunk_stub_->Write(&ctx, req, &resp).ok());
  }

  grpc::ClientContext del_ctx;
  ::vecstore::DeleteByPrefixRequest del_req;
  del_req.set_prefix(kb_a);
  ::vecstore::DeleteByPrefixResponse del_resp;
  ASSERT_TRUE(chunk_stub_->DeleteByPrefix(&del_ctx, del_req, &del_resp).ok());

  for (const auto& [key, want_exists] :
       std::vector<std::pair<std::string, bool>>{
           {EncodeKey("kbA", "c1"), false},
           {EncodeKey("kbA", "c2"), false},
           {EncodeKey("kbB", "c1"), true}}) {
    grpc::ClientContext ctx;
    ::vecstore::ExistsChunkRequest req;
    req.set_key(key);
    ::vecstore::ExistsChunkResponse resp;
    ASSERT_TRUE(chunk_stub_->Exists(&ctx, req, &resp).ok());
    EXPECT_EQ(resp.exists(), want_exists) << "key=" << key;
  }
}

// DeleteByPrefix("") is the one request that empties the whole chunk store: no
// knowledge base is named, so the prefix scan has nothing to stop it. The
// handler must refuse it — the vecstore listener is unauthenticated and bound
// to 0.0.0.0 in the documented deployment, so "who can send this" is not a
// defence.
TEST_F(GrpcServiceTest, ChunkStorageDeleteByPrefixRefusesEmptyPrefix) {
  grpc::ClientContext write_ctx;
  ::vecstore::WriteChunkRequest write_req;
  write_req.set_key(EncodeKey("kbA", "c1"));
  write_req.add_vector(1.0f);
  ::vecstore::WriteChunkResponse write_resp;
  ASSERT_TRUE(chunk_stub_->Write(&write_ctx, write_req, &write_resp).ok());

  grpc::ClientContext del_ctx;
  ::vecstore::DeleteByPrefixRequest del_req;
  del_req.set_prefix("");
  ::vecstore::DeleteByPrefixResponse del_resp;
  const grpc::Status status = chunk_stub_->DeleteByPrefix(&del_ctx, del_req, &del_resp);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);

  grpc::ClientContext ctx;
  ::vecstore::ExistsChunkRequest req;
  req.set_key(EncodeKey("kbA", "c1"));
  ::vecstore::ExistsChunkResponse resp;
  ASSERT_TRUE(chunk_stub_->Exists(&ctx, req, &resp).ok());
  EXPECT_TRUE(resp.exists()) << "the refused call must not have deleted anything";
}

// A zero-dimension vector reaches the handler as "field not set" (proto3 does
// not distinguish it from "empty"), and a zero-dim index cannot be built: the
// flatten-and-divide downstream is a division by zero, i.e. a SIGFPE that no
// catch() block can intercept. Both entry points must refuse it by name.
TEST_F(GrpcServiceTest, VectorIndexRejectsZeroDimensionChunks) {
  grpc::ClientContext build_ctx;
  ::vecstore::BuildIndexRequest build_req;
  build_req.set_kb_id("kb1");
  build_req.set_version_id(1);
  build_req.set_metric(::vecstore::COSINE);
  auto* empty_chunk = build_req.add_chunks();
  empty_chunk->set_chunk_id("chunk-empty");  // vector deliberately never set
  ::vecstore::BuildIndexResponse build_resp;
  grpc::Status status = index_stub_->Build(&build_ctx, build_req, &build_resp);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);

  grpc::ClientContext add_ctx;
  ::vecstore::AddChunksRequest add_req;
  add_req.set_kb_id("kb1");
  add_req.set_version_id(1);
  auto* empty = add_req.add_chunks();
  empty->set_chunk_id("chunk-empty");
  ::vecstore::AddChunksResponse add_resp;
  status = index_stub_->AddChunks(&add_ctx, add_req, &add_resp);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(GrpcServiceTest, VectorIndexBuildThenSearchRoundTrip) {
  grpc::ClientContext build_ctx;
  ::vecstore::BuildIndexRequest build_req;
  build_req.set_kb_id("kb1");
  build_req.set_version_id(1);
  build_req.set_metric(::vecstore::COSINE);
  for (int i = 0; i < 20; ++i) {
    auto* chunk = build_req.add_chunks();
    chunk->set_chunk_id("chunk-" + std::to_string(i));
    for (int d = 0; d < 8; ++d) {
      chunk->add_vector(static_cast<float>((i + d) % 7) - 3.0f);
    }
  }
  ::vecstore::BuildIndexResponse build_resp;
  auto status = index_stub_->Build(&build_ctx, build_req, &build_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();

  // Strict lifecycle (v12 2.6): seal BUILDING→READY via Save before
  // searching, mirroring the product sequence (Build → … → Save → Search).
  std::string save_path = (test_dir_ / "bt_saved.bin").string();
  grpc::ClientContext save_ctx;
  ::vecstore::SaveIndexRequest save_req;
  save_req.set_kb_id("kb1");
  save_req.set_version_id(1);
  save_req.set_path(save_path);
  ::vecstore::SaveIndexResponse save_resp;
  ASSERT_TRUE(index_stub_->Save(&save_ctx, save_req, &save_resp).ok());

  grpc::ClientContext search_ctx;
  ::vecstore::SearchIndexRequest search_req;
  search_req.set_kb_id("kb1");
  search_req.set_version_id(1);
  search_req.set_top_k(5);
  // Query with the exact vector of chunk-0.
  for (int d = 0; d < 8; ++d) {
    search_req.add_vector(static_cast<float>((0 + d) % 7) - 3.0f);
  }
  ::vecstore::SearchIndexResponse search_resp;
  status = index_stub_->Search(&search_ctx, search_req, &search_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_GT(search_resp.results_size(), 0);
  EXPECT_EQ(search_resp.results(0).chunk_id(), "chunk-0");
}

TEST_F(GrpcServiceTest, VectorIndexSaveLoadResetRoundTrip) {
  grpc::ClientContext build_ctx;
  ::vecstore::BuildIndexRequest build_req;
  build_req.set_kb_id("kb1");
  build_req.set_version_id(1);
  build_req.set_metric(::vecstore::COSINE);
  for (int i = 0; i < 10; ++i) {
    auto* chunk = build_req.add_chunks();
    chunk->set_chunk_id("chunk-" + std::to_string(i));
    for (int d = 0; d < 8; ++d) chunk->add_vector(static_cast<float>(i + d));
  }
  ::vecstore::BuildIndexResponse build_resp;
  ASSERT_TRUE(index_stub_->Build(&build_ctx, build_req, &build_resp).ok());

  std::string save_path = (test_dir_ / "saved_index.bin").string();
  grpc::ClientContext save_ctx;
  ::vecstore::SaveIndexRequest save_req;
  save_req.set_kb_id("kb1");
  save_req.set_version_id(1);
  save_req.set_path(save_path);
  ::vecstore::SaveIndexResponse save_resp;
  ASSERT_TRUE(index_stub_->Save(&save_ctx, save_req, &save_resp).ok());

  grpc::ClientContext reset_ctx;
  ::vecstore::ResetIndexRequest reset_req;
  reset_req.set_kb_id("kb1");
  reset_req.set_version_id(1);
  ::vecstore::ResetIndexResponse reset_resp;
  ASSERT_TRUE(index_stub_->Reset(&reset_ctx, reset_req, &reset_resp).ok());

  grpc::ClientContext load_ctx;
  ::vecstore::LoadIndexRequest load_req;
  load_req.set_kb_id("kb1");
  load_req.set_version_id(1);
  load_req.set_path(save_path);
  ::vecstore::LoadIndexResponse load_resp;
  auto status = index_stub_->Load(&load_ctx, load_req, &load_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();

  grpc::ClientContext search_ctx;
  ::vecstore::SearchIndexRequest search_req;
  search_req.set_kb_id("kb1");
  search_req.set_version_id(1);
  search_req.set_top_k(3);
  for (int d = 0; d < 8; ++d) search_req.add_vector(static_cast<float>(0 + d));
  ::vecstore::SearchIndexResponse search_resp;
  status = index_stub_->Search(&search_ctx, search_req, &search_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_GT(search_resp.results_size(), 0);
  EXPECT_EQ(search_resp.results(0).chunk_id(), "chunk-0");
}

// H5 of docs/code-review-2026-09-24.md: Drop releases the resident index object.
// Search afterwards is NOT_FOUND — the object is gone, not merely emptied (which
// is what Reset does) — and the persisted artifact is untouched, so Load brings
// the version back.
TEST_F(GrpcServiceTest, VectorIndexDropReleasesTheResidentIndex) {
  grpc::ClientContext build_ctx;
  ::vecstore::BuildIndexRequest build_req;
  build_req.set_kb_id("kb-drop");
  build_req.set_version_id(1);
  build_req.set_metric(::vecstore::COSINE);
  for (int i = 0; i < 10; ++i) {
    auto* chunk = build_req.add_chunks();
    chunk->set_chunk_id("chunk-" + std::to_string(i));
    for (int d = 0; d < 8; ++d) chunk->add_vector(static_cast<float>(i + d));
  }
  ::vecstore::BuildIndexResponse build_resp;
  ASSERT_TRUE(index_stub_->Build(&build_ctx, build_req, &build_resp).ok());

  const std::string save_path = (test_dir_ / "dropped_index.bin").string();
  grpc::ClientContext save_ctx;
  ::vecstore::SaveIndexRequest save_req;
  save_req.set_kb_id("kb-drop");
  save_req.set_version_id(1);
  save_req.set_path(save_path);
  ::vecstore::SaveIndexResponse save_resp;
  ASSERT_TRUE(index_stub_->Save(&save_ctx, save_req, &save_resp).ok());

  ::vecstore::SearchIndexRequest search_req;
  search_req.set_kb_id("kb-drop");
  search_req.set_version_id(1);
  search_req.set_top_k(3);
  for (int d = 0; d < 8; ++d) search_req.add_vector(static_cast<float>(d));

  grpc::ClientContext before_ctx;
  ::vecstore::SearchIndexResponse before_resp;
  ASSERT_TRUE(index_stub_->Search(&before_ctx, search_req, &before_resp).ok());

  grpc::ClientContext drop_ctx;
  ::vecstore::DropIndexRequest drop_req;
  drop_req.set_kb_id("kb-drop");
  drop_req.set_version_id(1);
  ::vecstore::DropIndexResponse drop_resp;
  ASSERT_TRUE(index_stub_->Drop(&drop_ctx, drop_req, &drop_resp).ok());

  grpc::ClientContext after_ctx;
  ::vecstore::SearchIndexResponse after_resp;
  const grpc::Status after = index_stub_->Search(&after_ctx, search_req, &after_resp);
  EXPECT_EQ(after.error_code(), grpc::StatusCode::NOT_FOUND)
      << "a dropped index must be gone, not merely empty: " << after.error_message();

  // Idempotent: the caller may be reclaiming memory it does not own (another
  // node dropped it first, or the version was never resident here).
  grpc::ClientContext again_ctx;
  ::vecstore::DropIndexResponse again_resp;
  EXPECT_TRUE(index_stub_->Drop(&again_ctx, drop_req, &again_resp).ok());

  // Memory, not disk: the artifact is still there and Load restores the version.
  grpc::ClientContext load_ctx;
  ::vecstore::LoadIndexRequest load_req;
  load_req.set_kb_id("kb-drop");
  load_req.set_version_id(1);
  load_req.set_path(save_path);
  ::vecstore::LoadIndexResponse load_resp;
  ASSERT_TRUE(index_stub_->Load(&load_ctx, load_req, &load_resp).ok());

  grpc::ClientContext restored_ctx;
  ::vecstore::SearchIndexResponse restored_resp;
  ASSERT_TRUE(index_stub_->Search(&restored_ctx, search_req, &restored_resp).ok());
  ASSERT_GT(restored_resp.results_size(), 0);
}

// QuantizedBuildSearchRunsTwoStageRerank exercises the full gRPC path for
// a quantized index: Build with QUANTIZER_SQ8 creates a quantized coarse
// retriever, and Search must run the two-stage pipeline (coarse pass →
// full-precision vectors read back from the chunk store → exact rerank)
// inside the vecstore service. Querying with an indexed chunk's own
// vector must surface that chunk at the top.
TEST_F(GrpcServiceTest, QuantizedBuildSearchRunsTwoStageRerank) {
  const std::string kb_id = "kb-quant-e2e";
  constexpr int kDim = 16;
  constexpr int kNumChunks = 60;

  std::mt19937 rng(2026);
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<std::vector<float>> vecs;
  for (int i = 0; i < kNumChunks; ++i) {
    std::vector<float> v(kDim);
    for (auto& x : v) x = dist(rng);
    vecs.push_back(v);

    ::vecstore::WriteChunkRequest write_req;
    write_req.set_key(EncodeKey(kb_id, "chunk-" + std::to_string(i)));
    for (float x : v) write_req.add_vector(x);
    ::vecstore::WriteChunkResponse write_resp;
    grpc::ClientContext write_ctx;
    auto st = chunk_stub_->Write(&write_ctx, write_req, &write_resp);
    ASSERT_TRUE(st.ok()) << st.error_message();
  }

  ::vecstore::BuildIndexRequest build_req;
  build_req.set_kb_id(kb_id);
  build_req.set_version_id(1);
  build_req.set_metric(::vecstore::COSINE);
  build_req.set_quantizer(::vecstore::QUANTIZER_SQ8);
  for (int i = 0; i < kNumChunks; ++i) {
    auto* chunk = build_req.add_chunks();
    chunk->set_chunk_id("chunk-" + std::to_string(i));
    for (float x : vecs[i]) chunk->add_vector(x);
  }
  ::vecstore::BuildIndexResponse build_resp;
  grpc::ClientContext build_ctx;
  auto status = index_stub_->Build(&build_ctx, build_req, &build_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  // The vecstore reports the quantized index's memory estimate (used by
  // the Go IndexManager's byte-budget LRU accounting, v12.md 3.3).
  EXPECT_GT(build_resp.mem_bytes(), 0);

  // Strict lifecycle (v12 2.6): seal BUILDING→READY via Save before
  // searching, mirroring the product sequence.
  std::string save_path = (test_dir_ / "quant_saved.bin").string();
  grpc::ClientContext save_ctx2;
  ::vecstore::SaveIndexRequest save_req2;
  save_req2.set_kb_id(kb_id);
  save_req2.set_version_id(1);
  save_req2.set_path(save_path);
  ::vecstore::SaveIndexResponse save_resp2;
  ASSERT_TRUE(index_stub_->Save(&save_ctx2, save_req2, &save_resp2).ok());

  ::vecstore::SearchIndexRequest search_req;
  search_req.set_kb_id(kb_id);
  search_req.set_version_id(1);
  search_req.set_top_k(3);
  search_req.set_candidate_n(200);
  for (float x : vecs[0]) search_req.add_vector(x);
  ::vecstore::SearchIndexResponse search_resp;
  grpc::ClientContext search_ctx2;
  status = index_stub_->Search(&search_ctx2, search_req, &search_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_GT(search_resp.results_size(), 0);
  EXPECT_EQ(search_resp.results(0).chunk_id(), "chunk-0");
  // Two-stage rerank returns exact cosine scores in [-1, 1].
  EXPECT_GE(search_resp.results(0).score(), -1.0f - 1e-4f);
  EXPECT_LE(search_resp.results(0).score(), 1.0f + 1e-4f);
}

// SearchWhileBuildingIsRejected verifies the strict lifecycle (v12 2.6):
// an index that was built but not yet Save-sealed is BUILDING, and Search
// must fail with FAILED_PRECONDITION instead of racing the build.
TEST_F(GrpcServiceTest, SearchWhileBuildingIsRejected) {
  ::vecstore::BuildIndexRequest build_req;
  build_req.set_kb_id("kb-building");
  build_req.set_version_id(1);
  build_req.set_metric(::vecstore::COSINE);
  for (int i = 0; i < 10; ++i) {
    auto* chunk = build_req.add_chunks();
    chunk->set_chunk_id("chunk-" + std::to_string(i));
    for (int d = 0; d < 8; ++d) chunk->add_vector(static_cast<float>(i + d));
  }
  ::vecstore::BuildIndexResponse build_resp;
  grpc::ClientContext build_ctx;
  ASSERT_TRUE(index_stub_->Build(&build_ctx, build_req, &build_resp).ok());
  // NOTE: no Save — the index stays BUILDING.

  ::vecstore::SearchIndexRequest search_req;
  search_req.set_kb_id("kb-building");
  search_req.set_version_id(1);
  search_req.set_top_k(3);
  for (int d = 0; d < 8; ++d) search_req.add_vector(static_cast<float>(d));
  ::vecstore::SearchIndexResponse search_resp;
  grpc::ClientContext search_ctx;
  auto status = index_stub_->Search(&search_ctx, search_req, &search_resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
