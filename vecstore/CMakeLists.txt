# vecstore/CMakeLists.txt — builds the C++ vecstore component: proto-
# generated gRPC code, the vecstore library (ChunkStorage / VectorIndex
# implementations + gRPC + health service), and its test binary.
#
# Phase 1 status: ChunkStorage (RocksDB) and VectorIndex (Faiss HNSW) have
# real implementations. grpc_service.cpp / health_service.cpp land in
# Phase 1-B-3.
#
# IVF / FLAT implementation files (ivf_index.cpp / flat_index.cpp) are
# intentionally not referenced here yet, per Stratum_代码风格v2.md's
# convention against creating empty placeholder files for future work.

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(absl REQUIRED)
find_package(RocksDB REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(BLAS REQUIRED)
find_package(LAPACK REQUIRED)
find_package(OpenMP REQUIRED)  # Faiss is built with -fopenmp; link consumers need it too

# ---------------------------------------------------------------------------
# Faiss (HNSW VectorIndex backend)
# ---------------------------------------------------------------------------
# Faiss is not packaged for Ubuntu (no apt package, no CMake config
# package), so it is built from source separately and located here via
# cache variables rather than find_package(Faiss). Build it yourself,
# cloned as a SIBLING of this repository checkout (i.e. if this repo is at
# /path/to/stratum, clone Faiss to /path/to/faiss):
#
#   cd /path/to
#   git clone --depth 1 --branch v1.9.0 \
#     https://github.com/facebookresearch/faiss.git
#   cmake -B faiss/build -S faiss \
#     -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF \
#     -DFAISS_ENABLE_C_API=OFF -DBUILD_TESTING=OFF \
#     -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=Release
#   cmake --build faiss/build -j$(nproc) --target faiss
#
# The defaults below assume that sibling layout; override with
# -DFAISS_SOURCE_DIR=/path/to/faiss -DFAISS_BUILD_DIR=/path/to/faiss/build
# if you put it elsewhere.
set(FAISS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../faiss" CACHE PATH "Path to the Faiss source checkout (for headers)")
set(FAISS_BUILD_DIR "${FAISS_SOURCE_DIR}/build" CACHE PATH "Path to the Faiss CMake build directory (for libfaiss.a)")

find_path(FAISS_INCLUDE_DIR
  NAMES faiss/IndexHNSW.h
  PATHS ${FAISS_SOURCE_DIR}
  NO_DEFAULT_PATH
)
find_library(FAISS_LIBRARY
  NAMES faiss
  PATHS ${FAISS_BUILD_DIR}/faiss
  NO_DEFAULT_PATH
)

if(NOT FAISS_INCLUDE_DIR OR NOT FAISS_LIBRARY)
  message(FATAL_ERROR
    "Faiss not found. Build it from source first (see the comment above "
    "this message in vecstore/CMakeLists.txt), then either let it land at "
    "${FAISS_SOURCE_DIR} or pass -DFAISS_SOURCE_DIR=/path/to/faiss and "
    "-DFAISS_BUILD_DIR=/path/to/faiss/build to cmake.")
endif()

add_library(faiss_external STATIC IMPORTED)
set_target_properties(faiss_external PROPERTIES
  IMPORTED_LOCATION ${FAISS_LIBRARY}
  INTERFACE_INCLUDE_DIRECTORIES ${FAISS_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Proto codegen: vecstore.proto -> generated .pb.cc/.pb.h and
# .grpc.pb.cc/.grpc.pb.h, written into the build directory.
# ---------------------------------------------------------------------------
set(VECSTORE_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/vecstore.proto")
set(VECSTORE_PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${VECSTORE_PROTO_GEN_DIR})

set(VECSTORE_PROTO_SRCS
  "${VECSTORE_PROTO_GEN_DIR}/vecstore.pb.cc"
  "${VECSTORE_PROTO_GEN_DIR}/vecstore.grpc.pb.cc"
)
set(VECSTORE_PROTO_HDRS
  "${VECSTORE_PROTO_GEN_DIR}/vecstore.pb.h"
  "${VECSTORE_PROTO_GEN_DIR}/vecstore.grpc.pb.h"
)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

add_custom_command(
  OUTPUT ${VECSTORE_PROTO_SRCS} ${VECSTORE_PROTO_HDRS}
  COMMAND protobuf::protoc
  ARGS
    --cpp_out=${VECSTORE_PROTO_GEN_DIR}
    --grpc_out=${VECSTORE_PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${VECSTORE_PROTO}
  DEPENDS ${VECSTORE_PROTO}
  COMMENT "Generating C++ code from vecstore.proto"
  VERBATIM
)

add_custom_target(vecstore_proto_gen DEPENDS ${VECSTORE_PROTO_SRCS} ${VECSTORE_PROTO_HDRS})

# ---------------------------------------------------------------------------
# vecstore_core library
# ---------------------------------------------------------------------------
set(VECSTORE_SOURCES
  ${VECSTORE_PROTO_SRCS}
  src/rocksdb_storage.cpp
  src/hnsw_index.cpp
  src/grpc_service.cpp
  # src/health_service.cpp    # not yet scheduled in any implementation phase
)

add_library(vecstore_core ${VECSTORE_SOURCES})
add_dependencies(vecstore_core vecstore_proto_gen)

target_include_directories(vecstore_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${VECSTORE_PROTO_GEN_DIR}
)

target_link_libraries(vecstore_core
  PUBLIC
    absl::status
    absl::statusor
    gRPC::grpc++
    protobuf::libprotobuf
    RocksDB::rocksdb
    prometheus-cpp::core
    prometheus-cpp::pull
    Threads::Threads
    faiss_external
    ${BLAS_LIBRARIES}
    ${LAPACK_LIBRARIES}
    OpenMP::OpenMP_CXX
)

# ---------------------------------------------------------------------------
# vecstore_server executable
# ---------------------------------------------------------------------------
# The standalone vecstore process entry point — see cmd/vecstore_server.cpp
# for why this exists. Used both for production (vecstore.grpc_addr) and
# for cross-language integration testing (2-B's Go ChunkStore client tests
# spawn this binary as a subprocess and connect to it over gRPC).
add_executable(vecstore_server cmd/vecstore_server.cpp)
target_link_libraries(vecstore_server PRIVATE vecstore_core)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
# Phase 0: no test .cpp files exist yet (T1-8 targets the Phase 1-B
# implementations). This target is wired up now so `ctest` works the
# moment test/hnsw_index_test.cpp and test/rocksdb_storage_test.cpp land,
# without further CMake changes.
find_package(GTest QUIET)

if(GTest_FOUND)
  file(GLOB VECSTORE_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/test/*_test.cpp")
  if(VECSTORE_TEST_SOURCES)
    enable_testing()
    add_executable(vecstore_tests ${VECSTORE_TEST_SOURCES})
    target_link_libraries(vecstore_tests
      PRIVATE
        vecstore_core
        GTest::gtest
        GTest::gtest_main
    )
    include(GoogleTest)
    gtest_discover_tests(vecstore_tests)
  endif()
endif()
