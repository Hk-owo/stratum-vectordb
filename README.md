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

# 3-node Docker cluster (T4): native docker CLI script + tagged tests
scripts/docker-cluster.sh up 3 --with-embed
go test ./integration/docker/... -tags=docker -timeout 300s
scripts/docker-cluster.sh down
```

`cmd/stratum` accepts an optional YAML config file for multi-node
deployments; command-line flags override file values:

```bash
go run ./cmd/stratum/ -config integration/docker/config1.yaml
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

## HTTP gateway, routing layer & Web console

`cmd/stratum-gateway` is a separate process that exposes the three external
gRPC services (`KnowledgeBaseService` / `QueryService` / `AdminService`) over
a small REST/JSON API and serves the frontend static assets (`web/`) from the
same origin, so no CORS is needed. It uses the already-generated client stubs
plus `protojson`, adding no new Go module dependency. The internal
`DataSyncService` is intentionally not exposed.

**Routing layer**：`cmd/stratum-router` is the cluster front: it dials every
node, discovers the Raft leader, forwards **writes to the leader** (re-discovering
on failover) and **load-balances reads** across nodes (round-robin with
failover). The gateway dials the **router** (`-grpc-addr 127.0.0.1:7009` by
default) instead of individual nodes — leader discovery and write/read routing
are the router's job, so the gateway keeps a single gRPC connection and no
cluster awareness of its own:

```
web UI ⇄ gateway (:8081) ⇄ router (:7009) ⇄ node1 / node2 / node3
```

**快捷启动**：`scripts/gateway.sh` 提供两种零参数启动方式（gateway 总是经路由层
访问集群；路由层未监听时会自动构建并后台拉起它，退出/`stop` 时一并清理）：
`scripts/router.sh` 可单独管理路由层：

```bash
scripts/gateway.sh               # Docker 集群模式（默认）：从 run/console.yaml
                                 # 的 docker 段读取节点数/基础端口，自动启动
                                 # 路由层 + gateway
scripts/gateway.sh single        # 单机模式：路由层连 127.0.0.1:7000，启动 gateway
scripts/gateway.sh build         # 强制重新构建后（默认模式）启动
scripts/gateway.sh stop          # 停止 gateway 与本脚本拉起的路由层

scripts/router.sh status         # 查看路由层是否在监听
scripts/router.sh stop           # 单独停止路由层
```

环境变量 `STRATUM_HTTP_ADDR`（监听地址，默认 `0.0.0.0:8081`）、
`STRATUM_ROUTER_ADDR`（路由层地址，默认 `127.0.0.1:7009`）与
`STRATUM_GRPC_ADDR`（单机模式路由层应连接的节点地址，默认 `127.0.0.1:7000`）
可覆盖默认值。手动启动等价于：

```bash
# 路由层（先起；-nodes 为集群节点 gRPC 地址列表）
./run/bin/stratum-router -listen 0.0.0.0:7009 -nodes 127.0.0.1:7000
./run/bin/stratum-router -listen 0.0.0.0:7009 -nodes localhost:17000,localhost:17001,localhost:17002

# gateway（后起；始终指向路由层）
./run/bin/stratum-gateway -grpc-addr 127.0.0.1:7009
```

> Docker 集群模式下 vecstore 是宿主机上的外部依赖（节点配置
> `vecstore.grpc_addr: host.docker.internal:7100`）。它必须监听宿主机的
> **对外接口**（`--grpc_addr=0.0.0.0:7100`），仅绑 `127.0.0.1` 时容器内
> 无法访问，会导致版本索引构建失败、删除（DELETE_FAILED）等连锁问题。

`start.sh` builds and launches the full stack in one go. It starts the
**routing layer** (`stratum-router`) and the **console process**
(`stratum-gateway`), and the database services (vecstore(C++) → stratum(gRPC))
are brought up through the console's `/ops/start` endpoint, so the web UI
(default `http://localhost:8081`) — including the **「运维」** page — is
available even before the database is running, and Ctrl+C stops everything
cleanly. Logs live under `run/log/`.

### Starting the console alone (ops only, database not running)

```bash
./run/bin/stratum-gateway          # default :8081, auto-creates run/console.yaml
# open http://localhost:8081 → 「运维」page to edit startup parameters,
# start/stop services, and tail logs before the database is up
```

The console keeps its own YAML (`run/console.yaml`) with the cluster node
list and the local service startup parameters; edits from the web page are
persisted there and take effect on the next service restart. Cluster-wide
ops are driven from any node's console through `/ops/nodes/{id}/*`.

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
│   └── docker/             # 3-node Docker cluster (scripts/docker-cluster.sh) + T4 tests (`docker` build tag)
├── cmd/
│   ├── stratum/main.go     # Entry point (−config YAML / flags)
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
| T4 | 3-node Raft cluster (in-process + Docker cluster + data-volume) |  ✅ |
| T5 | Real-stack e2e (real Pebble/WAL/Raft/IndexManager + vecstore subprocess, zero mocks) |  ✅ |

```bash
go test ./... -timeout 180s -count=1
# 23 packages, all PASS

go test ./integration/... -run TestRealStack -v
# Full real-stack end-to-end: real DocStore/WAL/Raft/IndexManager plus a real
# vecstore_server subprocess; requires the C++ binary (built by ./start.sh or
# vecstore/CMakeLists.txt) and skips rather than fails if it is missing.

go test ./integration/... -run TestMultiNode -v
# 3-node in-process cluster elects leader, replicates KB + version metadata

scripts/docker-cluster.sh up 3 --with-embed
go test ./integration/docker/... -tags=docker -v -timeout 300s
# 3-node Docker cluster (docker_test.go) + data-volume cost sampling
# (datavolume_test.go, scale with STRATUM_VOLUME_DOCS; requires a vecstore_server
# listening on the host's :7100 — start it via ./start.sh or build vecstore/CMakeLists.txt)
scripts/docker-cluster.sh down
```

## Prerequisites

- **Go 1.24**
- **C++17** (for vecstore): Faiss ≥ 1.9.0, RocksDB, gRPC, Protobuf, BLAS/LAPACK, OpenMP
- C++ build only required if you need the Faiss HNSW backend. Go tests use an in-process mock.

