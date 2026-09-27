// cancellation_e2e_test.cpp — end-to-end cancellation: a real gRPC client over a
// real loopback connection, the PRODUCTION VectorIndexServiceImpl, and a chunk
// store the test can hold INSIDE the rerank. That combination is what makes it
// possible to put a cancellation at a chosen point of the two-stage search and
// then ask the server what it did about it.
//
// Two pieces of scaffolding, and both are about OBSERVATION, not behaviour:
//
//  1. the server is assembled here rather than through VecstoreGrpcServer. The
//     assertion needs THE STATUS THE SERVER ENDED WITH, and a cancelled client
//     cannot see it: its own context is already done and gRPC drops the server's
//     status. Inside the server process that status is visible only to a server
//     interceptor, and VecstoreGrpcServer builds its ServerBuilder internally.
//     Everything registered on the builder below is production code (the real
//     service class, the generated service definition, the real RocksDB chunk
//     store) — the test adds the interceptor, not the behaviour.
//
//  2. BlockingChunkStorage holds ReadMulti until the test releases it, so the
//     cancellation can be delivered during the rerank's disk read — between the
//     seams of the two-stage search, which is exactly where "did the check work?"
//     is otherwise unobservable.
//
// What the three cases pin, in the order they matter:
//   - a client cancel that arrives while the server is mid-rerank: the server
//     ends CANCELLED rather than answering a caller that is gone (delete the
//     post-read seam in hnsw_index.cpp and this case fails with an OK status);
//   - a client DEADLINE that expires the same way: the server ends CANCELLED
//     too. That one is the whole point of "does a deadline passed to C++ do
//     anything": the client is long gone and the server still stops;
//   - no cancellation at all: the same call answers normally, so the checks are
//     not simply refusing everything.
#include <condition_variable>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/support/server_interceptor.h"
#include "vecstore.grpc.pb.h"
#include "vecstore/include/key_codec.h"
#include "vecstore/src/grpc_service.h"
#include "vecstore/src/rocksdb_storage.h"

namespace stratum {
namespace vecstore {
namespace {

namespace fs = std::filesystem;

constexpr int kDim = 32;
constexpr int kChunks = 200;

// kCancelPropagationWait is how long the test keeps the rerank's read held after the
// caller is gone, so that the cancellation has actually reached the server's core
// before the seam after that read is evaluated.
//
// Without it the test races: TryCancel returns to the caller's own context in
// microseconds, while the RST_STREAM still has to be sent and processed by the
// server's core — measured at ~0.3 ms on loopback (see the note on ServerKeepalive
// in grpc_service.h; a deadline is the same, the server computes it from
// grpc-timeout). 100 ms is a ~300x margin over a measured value, and because the
// read stays held for the whole wait, this waits for the signal rather than for
// luck. Note the failure mode if it is ever wrong: the server answers, and the test
// reports it — it cannot silently pass.
constexpr auto kCancelPropagationWait = std::chrono::milliseconds(100);

std::vector<float> RandomVector(std::mt19937& rng) {
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> v(kDim);
  for (auto& x : v) {
    x = dist(rng);
  }
  return v;
}

// BlockingChunkStorage wraps the real chunk store and holds ReadMulti until the
// test releases it.
//
// The hold is what lets a test place a cancellation at a chosen point: the search
// is quantized, so its rerank reads the candidates' full-precision vectors from
// here, and the seam right after this call is where a cancelled caller is supposed
// to be noticed. Holding it puts the server inside "work is in progress and the
// caller is about to be gone", which is the state the seams exist for.
class BlockingChunkStorage final : public ChunkStorage {
 public:
  explicit BlockingChunkStorage(ChunkStorage* inner) : inner_(inner) {}

  // WaitUntilReadStarts blocks until a ReadMulti has been entered (i.e. the rerank
  // reached its disk read), or until the timeout expires. Returns false on timeout.
  bool WaitUntilReadStarts(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return entered_cv_.wait_for(lock, timeout, [&] { return entered_; });
  }

  // Release lets the held read finish.
  void Release() {
    std::lock_guard<std::mutex> lock(mu_);
    released_ = true;
    released_cv_.notify_all();
  }

  int read_multi_calls() const {
    std::lock_guard<std::mutex> lock(mu_);
    return read_multi_calls_;
  }

  absl::StatusOr<MultiReadResult> ReadMulti(
      const std::vector<std::string>& keys) override {
    {
      std::unique_lock<std::mutex> lock(mu_);
      ++read_multi_calls_;
      entered_ = true;
      entered_cv_.notify_all();
      released_cv_.wait(lock, [&] { return released_; });
    }
    return inner_->ReadMulti(keys);
  }

  absl::Status Write(const std::string& key,
                     const std::vector<float>& vector) override {
    return inner_->Write(key, vector);
  }
  absl::StatusOr<std::vector<float>> Read(const std::string& key) override {
    return inner_->Read(key);
  }
  absl::StatusOr<bool> Exists(const std::string& key) override {
    return inner_->Exists(key);
  }
  absl::Status Delete(const std::string& key) override {
    return inner_->Delete(key);
  }
  absl::Status DeleteByPrefix(const std::string& prefix) override {
    return inner_->DeleteByPrefix(prefix);
  }
  absl::StatusOr<uint64_t> DiskUsage() override { return inner_->DiskUsage(); }

 private:
  ChunkStorage* inner_;
  mutable std::mutex mu_;
  std::condition_variable entered_cv_;
  std::condition_variable released_cv_;
  bool entered_ = false;
  bool released_ = false;
  int read_multi_calls_ = 0;
};

// StatusRecorder holds the last status a recorded RPC ended with, written by the
// server's own thread and read by the test's.
class StatusRecorder {
 public:
  void Record(const grpc::Status& status) {
    std::lock_guard<std::mutex> lock(mu_);
    status_ = status;
  }

  // WaitFor returns the recorded status, or nullopt if none arrived in time.
  std::optional<grpc::Status> WaitFor(std::chrono::milliseconds timeout) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      {
        std::lock_guard<std::mutex> lock(mu_);
        if (status_.has_value()) {
          return status_;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return std::nullopt;
  }

 private:
  mutable std::mutex mu_;
  std::optional<grpc::Status> status_;
};

// SearchMethod is the one RPC these tests care about. The recorder must be scoped
// to it: seeding an index goes through the SAME server (BuildIndex then SaveIndex),
// and a recorder that watched every RPC answered "the server returned OK" with the
// seeding's status before the search it was supposed to be watching had finished.
// That is not hypothetical — it is exactly how this test first failed, reporting OK
// for a search whose seam check had in fact fired.
const std::string SearchMethod() {
  return std::string("/") + ::vecstore::VectorIndexService::service_full_name() +
         "/Search";
}

class RecorderInterceptor final : public grpc::experimental::Interceptor {
 public:
  explicit RecorderInterceptor(StatusRecorder* recorder) : recorder_(recorder) {}

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override {
    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_STATUS)) {
      recorder_->Record(methods->GetSendStatus());
    }
    methods->Proceed();
  }

 private:
  StatusRecorder* recorder_;
};

class RecorderFactory final
    : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  explicit RecorderFactory(StatusRecorder* recorder) : recorder_(recorder) {}

  grpc::experimental::Interceptor* CreateServerInterceptor(
      grpc::experimental::ServerRpcInfo* info) override {
    // Only the RPC under test; the seeding calls are ignored, and returning nullptr
    // for an RPC is the documented way to opt out.
    if (std::string(info->method()) != SearchMethod()) {
      return nullptr;
    }
    return new RecorderInterceptor(recorder_);
  }

 private:
  StatusRecorder* recorder_;
};

class CancellationE2ETest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() /
                ("stratum_cancel_e2e_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);

    auto storage_or = RocksDBChunkStorage::Open((test_dir_ / "rocksdb").string());
    ASSERT_TRUE(storage_or.ok()) << storage_or.status();
    storage_ = std::move(storage_or.value());
    blocking_ = std::make_unique<BlockingChunkStorage>(storage_.get());
    // Production service class, production path allow-list.
    service_ = std::make_unique<VectorIndexServiceImpl>(
        blocking_.get(), std::vector<std::string>{test_dir_.string()});

    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>>
        creators;
    creators.push_back(std::make_unique<RecorderFactory>(&recorder_));
    builder.experimental().SetInterceptorCreators(std::move(creators));
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr) << "server failed to bind";

    channel_ = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                   grpc::InsecureChannelCredentials());
    stub_ = ::vecstore::VectorIndexService::NewStub(channel_);
  }

  void TearDown() override {
    if (caller_ctx_ != nullptr) {
      caller_ctx_->TryCancel();
    }
    if (server_ != nullptr) {
      server_->Shutdown();
    }
    fs::remove_all(test_dir_);
  }

  // SeedQuantizedIndex persists each chunk's full-precision vector in the chunk
  // store, then builds an SQ8 index over them and seals it with Save — the state a
  // two-stage search needs (quantized + READY).
  void SeedQuantizedIndex() {
    std::mt19937 rng(2024);
    grpc::ClientContext build_ctx;
    ::vecstore::BuildIndexRequest build;
    build.set_kb_id(kb_id_);
    build.set_version_id(1);
    build.set_metric(::vecstore::COSINE);
    build.set_quantizer(::vecstore::QUANTIZER_SQ8);
    for (int i = 0; i < kChunks; ++i) {
      const std::string chunk_id = "c-" + std::to_string(i);
      const std::vector<float> v = RandomVector(rng);
      auto* chunk = build.add_chunks();
      chunk->set_chunk_id(chunk_id);
      for (float x : v) {
        chunk->add_vector(x);
      }
      ASSERT_TRUE(blocking_->Write(EncodeKey(kb_id_, chunk_id), v).ok());
    }
    ::vecstore::BuildIndexResponse build_resp;
    ASSERT_TRUE(stub_->Build(&build_ctx, build, &build_resp).ok())
        << build_resp.DebugString();

    grpc::ClientContext save_ctx;
    ::vecstore::SaveIndexRequest save;
    save.set_kb_id(kb_id_);
    save.set_version_id(1);
    save.set_path((test_dir_ / "cancel-e2e.bin").string());
    ::vecstore::SaveIndexResponse save_resp;
    ASSERT_TRUE(stub_->Save(&save_ctx, save, &save_resp).ok());
  }

  ::vecstore::SearchIndexRequest MakeSearchRequest() {
    std::mt19937 rng(7);
    ::vecstore::SearchIndexRequest req;
    req.set_kb_id(kb_id_);
    req.set_version_id(1);
    req.set_top_k(5);
    // Non-zero candidate_n so the request takes the quantized two-stage path and
    // the rerank reads the chunk store.
    req.set_candidate_n(50);
    for (float x : RandomVector(rng)) {
      req.add_vector(x);
    }
    return req;
  }

  fs::path test_dir_;
  std::unique_ptr<ChunkStorage> storage_;
  std::unique_ptr<BlockingChunkStorage> blocking_;
  std::unique_ptr<VectorIndexServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<::vecstore::VectorIndexService::Stub> stub_;
  StatusRecorder recorder_;
  const std::string kb_id_ = "kb-cancel-e2e";

  // The caller's context lives here so the test thread can cancel it while the
  // caller thread is blocked inside Search — which is the whole point.
  std::unique_ptr<grpc::ClientContext> caller_ctx_;
  grpc::Status client_status_;
  ::vecstore::SearchIndexResponse search_resp_;
};

// Cancel while the server is inside the rerank's disk read: the server must not
// answer a caller that is gone. Without the seam that follows that read, the server
// finishes the rerank and sends OK — which is what this case catches.
TEST_F(CancellationE2ETest, ClientCancelDuringTheRerankReadStopsTheServer) {
  ASSERT_NO_FATAL_FAILURE(SeedQuantizedIndex());

  caller_ctx_ = std::make_unique<grpc::ClientContext>();
  std::thread caller([&] {
    const ::vecstore::SearchIndexRequest req = MakeSearchRequest();
    client_status_ = stub_->Search(caller_ctx_.get(), req, &search_resp_);
  });

  // The server is now holding the read; the rerank is mid-flight.
  ASSERT_TRUE(blocking_->WaitUntilReadStarts(std::chrono::seconds(5)))
      << "the server never reached the rerank's chunk-store read";
  EXPECT_EQ(blocking_->read_multi_calls(), 1);

  caller_ctx_->TryCancel();
  caller.join();
  EXPECT_EQ(client_status_.error_code(), grpc::StatusCode::CANCELLED)
      << "the caller's own view: " << client_status_.error_message();

  // Let the cancellation reach the server's core, then let the held read finish:
  // the seam after it is where the cancellation has to be noticed.
  std::this_thread::sleep_for(kCancelPropagationWait);
  blocking_->Release();
  const std::optional<grpc::Status> server_status = recorder_.WaitFor(std::chrono::seconds(5));
  ASSERT_TRUE(server_status.has_value()) << "the server never finished the RPC";
  EXPECT_EQ(server_status->error_code(), grpc::StatusCode::CANCELLED)
      << "the server answered a cancelled caller: " << server_status->error_message();
}

// The same point, but the client's disappearance is a DEADLINE expiring rather
// than an explicit cancel. The client is gone by the time the read is released —
// and the server still stops, because its own core computed the deadline from the
// request's grpc-timeout header and marked the call cancelled.
//
// This is the case that answers "does a deadline passed into C++ do anything":
// yes, and here is the proof, with the client unable to observe it.
TEST_F(CancellationE2ETest, ClientDeadlineDuringTheRerankReadStopsTheServer) {
  ASSERT_NO_FATAL_FAILURE(SeedQuantizedIndex());

  caller_ctx_ = std::make_unique<grpc::ClientContext>();
  caller_ctx_->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(300));
  std::thread caller([&] {
    const ::vecstore::SearchIndexRequest req = MakeSearchRequest();
    client_status_ = stub_->Search(caller_ctx_.get(), req, &search_resp_);
  });

  ASSERT_TRUE(blocking_->WaitUntilReadStarts(std::chrono::seconds(5)))
      << "the server never reached the rerank's chunk-store read";

  // The client gives up on its own clock; the server is still held in the read.
  caller.join();
  EXPECT_EQ(client_status_.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED)
      << "the caller's own view: " << client_status_.error_message();

  // Let the server's core expire the deadline it computed for itself from the
  // request's grpc-timeout header, then let the read finish and see what the server
  // decided.
  std::this_thread::sleep_for(kCancelPropagationWait);
  blocking_->Release();
  const std::optional<grpc::Status> server_status = recorder_.WaitFor(std::chrono::seconds(5));
  ASSERT_TRUE(server_status.has_value()) << "the server never finished the RPC";
  EXPECT_EQ(server_status->error_code(), grpc::StatusCode::CANCELLED)
      << "the server finished work for an expired caller: " << server_status->error_message();
}

// The control case: no cancellation, same call, same path — the answer comes back
// normally. Without it, the two cases above would also pass if the seams simply
// refused everything.
TEST_F(CancellationE2ETest, WithoutCancellationTheServerAnswers) {
  ASSERT_NO_FATAL_FAILURE(SeedQuantizedIndex());

  caller_ctx_ = std::make_unique<grpc::ClientContext>();
  std::thread caller([&] {
    const ::vecstore::SearchIndexRequest req = MakeSearchRequest();
    client_status_ = stub_->Search(caller_ctx_.get(), req, &search_resp_);
  });

  ASSERT_TRUE(blocking_->WaitUntilReadStarts(std::chrono::seconds(5)))
      << "the server never reached the rerank's chunk-store read";
  blocking_->Release();
  caller.join();

  EXPECT_TRUE(client_status_.ok()) << client_status_.error_message();
  EXPECT_GT(search_resp_.results_size(), 0);
  const std::optional<grpc::Status> server_status = recorder_.WaitFor(std::chrono::seconds(5));
  ASSERT_TRUE(server_status.has_value()) << "the server never finished the RPC";
  EXPECT_TRUE(server_status->ok()) << server_status->error_message();
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
