# Stratum

Distributed vector-search knowledge base engine with MVCC versioning,
Raft consensus, and HNSW indexing — built for retrieval-augmented
generation (RAG).

## Overview

Stratum manages versioned document collections. Documents are split into
content-addressed chunks, embedded into vectors via an external embed
service, indexed with Faiss HNSW, and served through a gRPC API — plus an
optional HTTP gateway (`cmd/stratum-gateway`) and web console (`web/`) for
monitoring and administration.

**What it solves:**

- **Rollback** — a broken update is one call away from being undone.
- **A/B testing** — multiple versions can coexist and be queried in
  parallel.
- **Auditability** — every query result is traceable to a specific
  version.

Stratum is the storage and retrieval layer for a RAG pipeline. It does
not handle chat history, user sessions, or prompt construction — those
belong to the application layer above it.

## Architecture

```
                   External gRPC Clients
                  ┌─────────────────────┐
                  │ KnowledgeBaseService │
                  │ QueryService         │
                  │ AdminService         │
                  └──────┬──────────────┘
                         │
  ┌──────────────────────┼──────────────────────────┐
  │               Go (orchestration)                │
  │                                                 │
  │  ┌─────────────┐  ┌───────────┐  ┌───────────┐ │
  │  │ WriteCoord  │  │QueryService│  │ AdminSvc  │ │
  │  └──────┬──────┘  └─────┬─────┘  └─────┬─────┘ │
  │         │               │               │       │
  │  ┌──────┴───────────────┴───────────────┴─────┐ │
  │  │              IndexManager                  │ │
  │  │         (LRU + refcounting)                │ │
  │  └──────────────────┬────────────────────────┘ │
  │                     │                           │
  │  ┌──────────────────┼────────────────────────┐ │
  │  │    PebbleDB stores (Go)                   │ │
  │  │    DocStore / ChunkDocMapper / VersionDoc │ │
  │  └──────────────────┼────────────────────────┘ │
  │                     │                           │
  │  ┌──────────────────┴────────────────────────┐ │
  │  │            Raft (kvraft)                   │ │
  │  │      strongly-consistent metadata          │ │
  │  └───────────────────────────────────────────┘ │
  └──────────────────────┬──────────────────────────┘
                         │  internal gRPC
  ┌──────────────────────┴──────────────────────────┐
  │              C++ (vector storage)               │
  │                                                 │
  │  ┌──────────────────┐  ┌──────────────────────┐ │
  │  │  ChunkStorage    │  │   VectorIndex        │ │
  │  │  (RocksDB)       │  │   (Faiss HNSW)       │ │
  │  └──────────────────┘  └──────────────────────┘ │
  └─────────────────────────────────────────────────┘
```

## Quick start

```bash
# Build
go build ./cmd/stratum/

# Run tests (23 packages)
go test ./... -timeout 180s -count=1

# Race detector
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# Single-node server
go run ./cmd/stratum/

# One-click console: vecstore(C++) → stratum(gRPC) → gateway(HTTP) + web UI
./start.sh           # then open http://localhost:8081
```

## gRPC API

### KnowledgeBaseService

| RPC | Description |
|---|---|
| `CreateKnowledgeBase` | Create a KB with embed config, chunk window, and index type |
| `DeleteKnowledgeBase` | Mark a KB for deletion; cleanup runs asynchronously |
| `CreateVersion` | Apply document changes (ADD / DELETE / UPDATE) and produce a new version |
| `ListVersions` | Return the version chain for a KB |
| `RollbackVersion` | Switch the active version (no downtime) |
| `ListKnowledgeBases` | List all KBs with their active versions |
| `GetKnowledgeBase` | Fetch a single KB's config and active version |

### QueryService

| RPC | Description |
|---|---|
| `Query` | Vector similarity search with threshold, top-k, and aggregation |

### AdminService

| RPC | Description |
|---|---|
| `HealthCheck` | Three-state health (HEALTHY / DEGRADED / UNHEALTHY) |
| `GetSystemStatus` | Stuck versions, delete-failed KBs, WAL alerts, resource usage |
| `RebuildIndex` | Re-trigger index build for a failed version |
| `WarmupVersion` | Load a version's index into memory without switching the active version |

## HTTP gateway & Web console

`cmd/stratum-gateway` is a separate process that exposes the three external
gRPC services (`KnowledgeBaseService` / `QueryService` / `AdminService`) over
a small REST/JSON API and serves the frontend static assets (`web/`) from the
same origin, so no CORS is needed. It dials the core server's gRPC address
(default `:7000`) using the already-generated client stubs plus `protojson`,
adding no new Go module dependency. The internal `DataSyncService` is
intentionally not exposed.

`./start.sh` builds and launches the full stack in one go — vecstore(C++) →
stratum(gRPC) → gateway(HTTP) → web UI (default `http://localhost:8081`),
with per-process logs under `run/`.

## Project structure

```
stratum/
├── api/proto/              # Protobuf definitions
│   ├── knowledgebase.proto
│   ├── query.proto
│   ├── admin.proto
│   ├── kvraft/             # Internal Raft RPC
│   └── vecstore/           # Go ↔ C++ internal gRPC
├── internal/
│   ├── types/              # Shared data types
│   ├── errors/             # Business errors → gRPC status mapping
│   ├── docstore/           # MVCC document storage (PebbleDB)
│   ├── chunkdoc/           # Bidirectional chunk ↔ document mapping
│   ├── versiondoc/         # Per-version document ID sets
│   ├── bloom/              # Bloom filters (chunk existence + version membership)
│   ├── splitter/           # Sliding-window document chunking
│   ├── embed/              # External embed service HTTP client
│   ├── chunkstore/         # Vecstore gRPC client wrapper
│   ├── index/              # IndexManager (LRU cache + refcounts + async builds)
│   ├── kvraft/             # Raft consensus library (leader election / log replication / snapshots)
│   ├── kvstorage/          # Raft hard-state persistence
│   ├── raft/               # Stratum Raft state machine (KB + version metadata)
│   ├── wal/                # Write-ahead log for crash consistency
│   ├── sync/               # Leader→Follower data sync (DataSync) + summary
│   ├── pebbleutil/         # PebbleDB helpers (keys, iteration)
│   └── coordinator/        # WriteCoordinator + DeleteCoordinator orchestration
├── service/                # gRPC service implementations
├── integration/            # Mock-based integration tests + real-stack e2e + cluster tests
├── cmd/
│   ├── stratum/main.go     # Entry point
│   └── stratum-gateway/    # HTTP/JSON → gRPC gateway + web console static assets
├── configs/                # Sample configuration files
├── web/                    # Web console frontend (HTML/CSS/JS)
├── scripts/                # Dev/test helper scripts (test-data generation etc.)
├── vecstore/               # C++ vector storage (Faiss HNSW + RocksDB)
└── go.mod
```

## Key design decisions

| Decision | Rationale |
|---|---|
| **Content-addressed chunks** | `ChunkID = SHA-256(text + model ID)` — deduplicates chunks naturally, no coordination needed |
| **MVCC via PebbleDB prefixes** | Document history is compressed; unchanged documents cost zero in new versions |
| **WAL for crash consistency** | Two-phase protocol (BEGIN → VERSION_ID → COMMIT) survives a crash at any point |
| **Cosine via L2 normalization** | Faiss has no native cosine support; normalizing + inner product is mathematically equivalent |
| **Unified score direction** | All three metrics return "higher is more similar" — Euclidean distance is negated |
| **JSON-encoded Raft commands** | Low-volume control-plane commands; human-readable with `jq` for debugging |
| **Interface-first, mock alongside** | Every module is an interface with a mock. Tests isolate cleanly; real implementations slot in behind the interface |
| **Batched index build** | A build splits into one `Build` + multiple `AddChunks` RPCs so each gRPC message stays under the 4 MiB transport limit on large KBs; empty versions still get an index entry |

## Raft consensus

The consensus layer (`internal/kvraft`) is adapted from the
[KVServer](https://github.com/Hk-owo/KVServer) teaching implementation
(MIT 6.5840), rewritten to Stratum's code style and fixed for 6 bugs
discovered through TDD:

| # | Bug | Impact |
|---|---|---|
| 1 | Single-node clusters never elected a leader | Majority check only ran in peer-response handlers |
| 2 | AppendEntries heartbeat skipped log consistency check | Follower could advance commitIndex past a divergent log |
| 3 | InstallSnapshot held the mutex while sending on a channel | Deadlock risk |
| 4 | RequestVote did not check `killed()` | A stopped node could still vote |
| 5 | No no-op entry on election | Old log entries permanently invisible until new write traffic |
| 6 | Leader added itself as a peer | Leader stepped down on its own heartbeat |

## Testing

| Batch | Scope | Status |
|---|---|---|
| T1 | Single-module contracts (8 modules) |  ✅ |
| T2 | Cross-module integration (4 groups) |  ✅ |
| T3 | Single-node full chain (15 scenarios) |  ✅ |
| T4 | 3-node Raft cluster |  ✅ |
| T5 | Real-stack e2e (real Pebble/WAL/Raft/IndexManager + vecstore subprocess, zero mocks) |  ✅ |

```bash
go test ./... -timeout 180s -count=1
# 23 packages, all PASS

go test ./integration/... -run TestRealStack -v
# Full real-stack end-to-end: real DocStore/WAL/Raft/IndexManager plus a real
# vecstore_server subprocess; requires the C++ binary (built by ./start.sh or
# vecstore/CMakeLists.txt) and skips rather than fails if it is missing.

go test ./integration/... -run TestMultiNode -v
# 3-node cluster elects leader, replicates KB + version metadata
```

## Prerequisites

- **Go 1.24**
- **C++17** (for vecstore): Faiss ≥ 1.9.0, RocksDB, gRPC, Protobuf, BLAS/LAPACK, OpenMP
- C++ build only required if you need the Faiss HNSW backend. Go tests use an in-process mock.

