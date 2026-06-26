# Stratum

Distributed vector-search knowledge base engine with MVCC versioning,
Raft consensus, and HNSW indexing — built for retrieval-augmented
generation (RAG).

## Overview

Stratum manages versioned document collections. Documents are split into
content-addressed chunks, embedded into vectors via an external embed
service, indexed with Faiss HNSW, and served through a gRPC API.

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

# Run tests (17 packages)
go test ./... -timeout 180s -count=1

# Race detector
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# Single-node server
go run ./cmd/stratum/
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
│   └── coordinator/        # WriteCoordinator + DeleteCoordinator orchestration
├── service/                # gRPC service implementations
├── integration/            # In-process integration tests + 3-node cluster tests
├── cmd/stratum/main.go     # Entry point
├── configs/                # Sample configuration files
├── doc/                    # Design documents (Chinese)
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

```bash
go test ./... -timeout 180s -count=1
# 17 packages, all PASS

go test ./integration/... -run TestMultiNode -v
# 3-node cluster elects leader, replicates KB + version metadata
```

## Prerequisites

- **Go 1.24**
- **C++17** (for vecstore): Faiss ≥ 1.9.0, RocksDB, gRPC, Protobuf, BLAS/LAPACK, OpenMP
- C++ build only required if you need the Faiss HNSW backend. Go tests use an in-process mock.

