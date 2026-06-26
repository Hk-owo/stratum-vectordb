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
#include <iostream>
#include <memory>
#include <string>
#include <thread>

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

  auto storage_or = stratum::vecstore::RocksDBChunkStorage::Open(rocksdb_path);
  if (!storage_or.ok()) {
    std::cerr << "vecstore_server: failed to open RocksDB at " << rocksdb_path << ": "
              << storage_or.status() << std::endl;
    return 1;
  }

  g_server = std::make_unique<stratum::vecstore::VecstoreGrpcServer>(
      std::move(storage_or.value()));

  if (!g_server->Start(grpc_addr)) {
    std::cerr << "vecstore_server: failed to bind to " << grpc_addr << std::endl;
    return 1;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  std::cout << "vecstore_server: listening on " << grpc_addr
            << ", rocksdb_path=" << rocksdb_path << std::endl;

  // Block forever; HandleSignal exits the process on SIGINT/SIGTERM.
  // grpc::Server itself runs its accept loop on background threads
  // started by BuildAndStart, so the main thread just needs to stay
  // alive.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(24));
  }
}
