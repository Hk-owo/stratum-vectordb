// vecstore_server.cpp — the vecstore process entry point. Opens
// ChunkStorage (RocksDB) at the configured path and serves
// ChunkStorageService / VectorIndexService over gRPC at the configured
// address, matching the vecstore.rocksdb_path / vecstore.grpc_addr config
// fields in Stratum_代码风格v2.md.
//
// This binary is not named in any Stratum design document — it was added
// to satisfy a real, unavoidable requirement: 2-B's ChunkStore Go gRPC
// client needs an actual running vecstore server to connect to, and the
// production architecture (Go <-> C++ vecstore communicating over
// internal gRPC, per vecstore.grpc_addr) requires a standalone vecstore
// process to exist regardless. Kept intentionally minimal (flag-based
// config, no YAML parsing) since full config-file loading is a Phase 6
// concern alongside cmd/stratum/main.go.
//
// Usage:
//   vecstore_server --rocksdb_path=/path/to/db --grpc_addr=127.0.0.1:7100
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "vecstore/src/grpc_service.h"
#include "vecstore/src/rocksdb_storage.h"

namespace {

std::unique_ptr<stratum::vecstore::VecstoreGrpcServer> g_server;

void HandleSignal(int /*signum*/) {
  if (g_server) {
    g_server->Shutdown();
  }
  std::exit(0);
}

// ParseFlag looks for "--name=value" among argv[1:argc) and returns value,
// or default_value if not present. Deliberately simple — no third-party
// flag-parsing dependency for a handful of options.
std::string ParseFlag(int argc, char** argv, const std::string& name,
                       const std::string& default_value) {
  std::string prefix = "--" + name + "=";
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    if (arg.rfind(prefix, 0) == 0) {
      return arg.substr(prefix.size());
    }
  }
  return default_value;
}

}  // namespace

int main(int argc, char** argv) {
  std::string rocksdb_path = ParseFlag(argc, argv, "rocksdb_path", "./vecstore_rocksdb");
  std::string grpc_addr = ParseFlag(argc, argv, "grpc_addr", "127.0.0.1:7100");
  std::string index_dir_flag = ParseFlag(argc, argv, "index_dir", "");

  // index_dirs is the allow-list for the on-disk index RPCs (Save / Load /
  // LoadForAppend / ExistsIndex), which otherwise take any path a caller names
  // (M4 of docs/code-review-2026-09-24.md). Comma-separated, because a node may
  // legitimately keep indexes under more than one root.
  //
  // Unset means the parent of --rocksdb_path: the layout every deployment script
  // uses puts both under one data directory (`<data>/vecstore_rocksdb` and
  // `<data>/index/...`), so that default confines the RPCs without requiring the
  // flag. A deployment that stores indexes elsewhere passes --index_dir explicitly
  // (and gets a refusal, not a silent success, until it does).
  std::vector<std::string> index_dirs;
  for (size_t start = 0; start <= index_dir_flag.size();) {
    const size_t comma = index_dir_flag.find(',', start);
    const std::string piece = index_dir_flag.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!piece.empty()) {
      index_dirs.push_back(piece);
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  if (index_dirs.empty()) {
    index_dirs.push_back(std::filesystem::path(rocksdb_path).parent_path().string());
  }

  auto storage_or = stratum::vecstore::RocksDBChunkStorage::Open(rocksdb_path);
  if (!storage_or.ok()) {
    std::cerr << "vecstore_server: failed to open RocksDB at " << rocksdb_path << ": "
              << storage_or.status() << std::endl;
    return 1;
  }

  g_server = std::make_unique<stratum::vecstore::VecstoreGrpcServer>(
      std::move(storage_or.value()), index_dirs);

  if (!g_server->Start(grpc_addr)) {
    std::cerr << "vecstore_server: failed to bind to " << grpc_addr << std::endl;
    return 1;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  std::cout << "vecstore_server: listening on " << grpc_addr
            << ", rocksdb_path=" << rocksdb_path << ", index_dirs=";
  for (size_t i = 0; i < index_dirs.size(); ++i) {
    std::cout << (i == 0 ? "" : ",") << index_dirs[i];
  }
  std::cout << std::endl;

  // Block forever; HandleSignal exits the process on SIGINT/SIGTERM.
  // grpc::Server itself runs its accept loop on background threads
  // started by BuildAndStart, so the main thread just needs to stay
  // alive.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(24));
  }
}
