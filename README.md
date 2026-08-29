# Stratum

分布式向量检索知识库引擎,支持 MVCC 版本化、Raft 共识与 HNSW 索引——为检索增强生成(RAG)而构建。

## 概述

Stratum 管理带版本号的文档集合。文档被切分为内容寻址的 chunk,经外部 embed 服务
转为向量,以 Faiss HNSW 建索引,并通过 gRPC API 对外服务;另提供可选的 HTTP 网关
(`cmd/stratum-gateway`) 与 Web 控制台(`web/`)用于监控与运维。存储卫生自动维护:
未被任何版本引用的 chunk 由周期性垃圾回收器清扫;每版本一份文档布隆过滤器让成员
检查开销极低;启动 reconcile 从磁盘上的索引文件推导版本 READY 状态,而非依赖回调送达。

**解决的问题:**

- **回滚** — 一次有问题的更新,一条调用即可撤销。
- **A/B 测试** — 多个版本可并存并被并行查询。
- **可审计** — 每个查询结果都能追溯到具体版本。

Stratum 是 RAG 管线的存储与检索层。它不处理聊天历史、用户会话或 prompt 构造——
这些属于其上方的应用层。

## 架构

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

## 快速开始

```bash
# 构建
go build ./cmd/stratum/

# 运行测试(23 个包)
go test ./... -timeout 180s -count=1

# 竞态检测
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# 单节点服务
go run ./cmd/stratum/

# 一键控制台:vecstore(C++) → stratum(gRPC) → gateway(HTTP) + web UI
./start.sh           # 然后打开 http://localhost:8081

# 3 节点 Docker 集群(T4):docker CLI 脚本 + 标签测试
scripts/docker-cluster.sh up 3 --with-embed
go test ./integration/docker/... -tags=docker -timeout 300s
scripts/docker-cluster.sh down
```

`cmd/stratum` 接受可选的 YAML 配置文件用于多节点部署;命令行 flag 优先于文件值:

```bash
go run ./cmd/stratum/ -config integration/docker/config1.yaml
```

## gRPC API

### KnowledgeBaseService

| RPC | 说明 |
|---|---|
| `CreateKnowledgeBase` | 创建知识库(embed 配置、chunk 窗口、索引类型) |
| `DeleteKnowledgeBase` | 标记知识库删除;清理异步执行 |
| `CreateVersion` | 应用文档变更(ADD / DELETE / UPDATE)并产出新版本 |
| `ListVersions` | 返回知识库的版本链 |
| `RollbackVersion` | 切换活跃版本(无停机) |
| `ListKnowledgeBases` | 列出所有知识库及其活跃版本 |
| `GetKnowledgeBase` | 获取单个知识库的配置与活跃版本 |
| `DeleteVersion` | 标记版本(及其后代)删除;清理异步执行 |

### QueryService

| RPC | 说明 |
|---|---|
| `Query` | 向量相似度检索(阈值、top-k、聚合) |

### AdminService

| RPC | 说明 |
|---|---|
| `HealthCheck` | 三态健康检查(HEALTHY / DEGRADED / UNHEALTHY) |
| `GetSystemStatus` | 卡住版本、删除失败的知识库、WAL 告警、资源占用、进行中的版本删除 |
| `GetClusterStatus` | 节点自身的 Raft 视图(node_id / leader_id / member_count)——供路由层发现 leader |
| `RebuildIndex` | 为失败版本重新触发索引构建 |
| `WarmupVersion` | 将版本索引载入内存而不切换活跃版本 |

## 后台任务与存储卫生

- **Chunk 垃圾回收** — `ChunkGarbageCollector`(`internal/coordinator`)周期性清扫
  不再被任何版本引用的 chunk(默认清扫间隔 5 分钟,可通过 `chunk_gc.sweep_interval_sec` 配置)。
- **每版本文档布隆过滤器** — `VersionBloomStore`(`internal/bloom`)为每个版本维护
  一份包含其完整文档 ID 集合的布隆过滤器。版本创建时写入(缓存并持久化到磁盘),
  读取时加载;磁盘副本缺失或损坏时从 `VersionDocList` 重建。
- **启动 reconcile** — 启动时 IndexManager 通过 vecstore 的 `ExistsIndex` RPC,
  从磁盘事实(Faiss 索引文件 + `.ids` 侧车文件)推导版本 READY 状态,而不是信任
  构建完成回调确实送达(例如崩溃之后)。

## HTTP 网关、路由层与 Web 控制台

`cmd/stratum-gateway` 是独立进程,把三个外部 gRPC 服务(`KnowledgeBaseService` /
`QueryService` / `AdminService`)暴露为小型 REST/JSON API,并从同源提供前端静态资源
(`web/`),因此无需 CORS。它使用已生成的客户端桩 + `protojson`,不引入新的 Go 模块
依赖。内部 `DataSyncService` 有意不对外暴露。

**路由层**:`cmd/stratum-router` 是集群前端:它拨号所有节点、发现 Raft leader,
将**写操作转发给 leader**(故障转移时重新发现),并将**读操作负载均衡**到各节点
(round-robin + 故障转移)。网关拨号**路由层**(默认 `-grpc-addr 127.0.0.1:7009`)
而非单个节点——leader 发现与读写路由都是路由层的职责,网关因此只需保持单条 gRPC
连接、自身不感知集群:

```
web UI ⇄ gateway (:8081) ⇄ router (:7009) ⇄ node1 / node2 / node3
```

**快捷启动**:`scripts/gateway.sh` 提供两种零参数启动方式(gateway 总是经路由层
访问集群;路由层未监听时会自动构建并后台拉起它,退出/`stop` 时一并清理):
`scripts/router.sh` 可单独管理路由层:

```bash
scripts/gateway.sh               # Docker 集群模式(默认):从 run/console.yaml
                                 # 的 docker 段读取节点数/基础端口,自动启动
                                 # 路由层 + gateway
scripts/gateway.sh single        # 单机模式:路由层连 127.0.0.1:7000,启动 gateway
scripts/gateway.sh build         # 强制重新构建后(默认模式)启动
scripts/gateway.sh stop          # 停止 gateway 与本脚本拉起的路由层

scripts/router.sh status         # 查看路由层是否在监听
scripts/router.sh stop           # 单独停止路由层
```

环境变量 `STRATUM_HTTP_ADDR`(监听地址,默认 `0.0.0.0:8081`)、
`STRATUM_ROUTER_ADDR`(路由层地址,默认 `127.0.0.1:7009`)与
`STRATUM_GRPC_ADDR`(单机模式路由层应连接的节点地址,默认 `127.0.0.1:7000`)
可覆盖默认值。手动启动等价于:

```bash
# 路由层(先起;-nodes 为集群节点 gRPC 地址列表)
./run/bin/stratum-router -listen 0.0.0.0:7009 -nodes 127.0.0.1:7000
./run/bin/stratum-router -listen 0.0.0.0:7009 -nodes localhost:17000,localhost:17001,localhost:17002

# gateway(后起;始终指向路由层)
./run/bin/stratum-gateway -grpc-addr 127.0.0.1:7009
```

> Docker 集群模式下 vecstore 是宿主机上的外部依赖(节点配置
> `vecstore.grpc_addr: host.docker.internal:7100`)。它必须监听宿主机的
> **对外接口**(`--grpc_addr=0.0.0.0:7100`),仅绑 `127.0.0.1` 时容器内
> 无法访问,会导致版本索引构建失败、删除(DELETE_FAILED)等连锁问题。

`start.sh` 一键构建并启动完整链路。它启动**路由层**(`stratum-router`)与
**控制台进程**(`stratum-gateway`),数据库服务(vecstore(C++) → stratum(gRPC))
通过控制台的 `/ops/start` 端点拉起,因此 Web UI(默认 `http://localhost:8081`)
——包括**「运维」**页面——在数据库尚未运行时也可用,Ctrl+C 干净地停止一切。
日志位于 `run/log/`。

### 仅启动控制台(仅运维,数据库未运行)

```bash
./run/bin/stratum-gateway          # 默认 :8081,自动创建 run/console.yaml
# 打开 http://localhost:8081 → 「运维」页面:编辑启动参数、
# 启停服务、在数据库启动前查看日志
```

控制台维护自己的 YAML(`run/console.yaml`),包含集群节点列表与本地服务启动参数;
网页上的修改会持久化到该文件,并在下次服务重启时生效。集群级运维操作可从任意
节点的控制台通过 `/ops/nodes/{id}/*` 驱动。

## 项目结构

```
stratum/
├── .github/workflows/      # CI(Go 全量检查 + Docker T4 集群)与手动 vecstore-cpp
├── api/proto/              # Protobuf 定义
│   ├── knowledgebase.proto
│   ├── query.proto
│   ├── admin.proto
│   ├── kvraft/             # 内部 Raft RPC
│   └── vecstore/           # Go ↔ C++ 内部 gRPC
├── internal/
│   ├── types/              # 共享数据类型
│   ├── errors/             # 业务错误 → gRPC 状态映射
│   ├── docstore/           # MVCC 文档存储(PebbleDB)
│   ├── chunkdoc/           # chunk ↔ 文档双向映射
│   ├── versiondoc/         # 每版本文档 ID 集合
│   ├── bloom/              # 布隆过滤器(chunk 存在性 + 版本成员)+ 每版本文档布隆存储
│   ├── splitter/           # 滑窗文档切分
│   ├── embed/              # 外部 embed 服务 HTTP 客户端
│   ├── chunkstore/         # Vecstore gRPC 客户端封装
│   ├── index/              # IndexManager(LRU 缓存 + 引用计数 + 异步构建)
│   ├── kvraft/             # Raft 共识库(选主 / 日志复制 / 快照)
│   ├── kvstorage/          # Raft 硬状态持久化
│   ├── raft/               # Stratum Raft 状态机(KB + 版本元数据)
│   ├── wal/                # 崩溃一致性写前日志
│   ├── sync/               # Leader→Follower 数据同步(DataSync)+ 摘要
│   ├── pebbleutil/         # PebbleDB 工具(keys、迭代)
│   ├── router/             # gRPC 前端:写 → leader,读负载均衡
│   └── coordinator/        # Write / Delete / DeleteVersion / chunk-GC 编排
├── service/                # gRPC 服务实现
├── integration/            # 基于 mock 的集成测试 + 真实栈 e2e + 集群测试
│   └── docker/             # 3 节点 Docker 集群(scripts/docker-cluster.sh)+ T4 测试(`docker` 构建标签)
├── cmd/
│   ├── stratum/main.go     # 入口(−config YAML / flags)
│   ├── stratum-gateway/    # HTTP/JSON → gRPC 网关 + /ops 控制台控制面
│   └── stratum-router/     # 路由层:单地址接入 Raft 集群
├── configs/                # 示例配置文件
├── web/                    # Web 控制台前端(HTML/CSS/JS)
├── scripts/                # 开发/测试辅助脚本 + 运维脚本(scripts/ops)
├── vecstore/               # C++ 向量存储(Faiss HNSW + RocksDB)
└── go.mod
```

## 关键设计决策

| 决策 | 理由 |
|---|---|
| **内容寻址 chunk** | `ChunkID = SHA-256(text + model ID)` — 天然去重,无需协调 |
| **基于 PebbleDB 前缀的 MVCC** | 文档历史被压缩;未变更文档在新版本中零成本 |
| **WAL 保证崩溃一致性** | 两阶段协议(BEGIN → VERSION_ID → COMMIT)在任何时刻崩溃都可存活 |
| **L2 归一化实现余弦** | Faiss 无原生余弦支持;归一化 + 内积在数学上等价 |
| **统一分数方向** | 三种度量都返回"越相似分数越高" — 欧氏距离取负 |
| **JSON 编码的 Raft 命令** | 控制面命令量小;可用 `jq` 人类可读地调试 |
| **接口优先、mock 同行** | 每个模块都是接口配 mock;测试干净隔离;真实实现从接口后插入 |
| **分批索引构建** | 构建拆分为一次 `Build` + 多次 `AddChunks` RPC,使每个 gRPC 消息不超过 4 MiB 传输上限;空版本仍建索引条目 |

## Raft 共识

共识层(`internal/kvraft`)改编自 [KVServer](https://github.com/Hk-owo/KVServer)
教学实现(MIT 6.5840),重写为 Stratum 代码风格,并通过 TDD 修复了 6 个缺陷:

| # | Bug | 影响 |
|---|---|---|
| 1 | 单节点集群永远选不出 leader | 多数派检查只在 peer 响应处理器里执行 |
| 2 | AppendEntries 心跳跳过了日志一致性检查 | follower 可能把 commitIndex 推进到分叉日志之后 |
| 3 | InstallSnapshot 持锁时向 channel 发送 | 死锁风险 |
| 4 | RequestVote 未检查 `killed()` | 已停止的节点仍可能投票 |
| 5 | 选举后无 no-op 条目 | 旧日志条目在新写入流量到来前永久不可见 |
| 6 | leader 把自己加为 peer | leader 因自己的心跳而退位 |

## 测试

| 批次 | 范围 | 状态 |
|---|---|---|
| T1 | 单模块契约(8 个模块) |  ✅ |
| T2 | 跨模块集成(4 组) |  ✅ |
| T3 | 单节点全链路(15 个场景) |  ✅ |
| T4 | 3 节点 Raft 集群(进程内 + Docker 集群 + 数据量) |  ✅ |
| T5 | 真实栈 e2e(真实 Pebble/WAL/Raft/IndexManager + vecstore 子进程,零 mock) |  ✅ |

```bash
go test ./... -timeout 180s -count=1
# 23 个包,全部 PASS

go test ./integration/... -run TestRealStack -v
# 全真实栈端到端:真实 DocStore/WAL/Raft/IndexManager + 真实 vecstore_server
# 子进程;需要 C++ 二进制(由 ./start.sh 或 vecstore/CMakeLists.txt 构建),
# 缺失时跳过而非失败。

go test ./integration/... -run TestMultiNode -v
# 3 节点进程内集群选出 leader,复制 KB + 版本元数据

scripts/docker-cluster.sh up 3 --with-embed
go test ./integration/docker/... -tags=docker -v -timeout 300s
# 3 节点 Docker 集群(docker_test.go)+ 数据量成本采样
# (datavolume_test.go,用 STRATUM_VOLUME_DOCS 调规模;要求宿主机 :7100 上
# 有 vecstore_server 监听 — 通过 ./start.sh 启动或构建 vecstore/CMakeLists.txt)
scripts/docker-cluster.sh down
```

CI 配置在 `.github/workflows/ci.yml`(推送到 `main` 与 pull request 时运行):
gofmt + `go vet` + `go build` + 单元测试(23 个包)+ raft/kvraft/index 竞态检测 +
3 节点 Docker 集群(T4)容错运行。C++ vecstore 侧由单独的
手动触发工作流(`.github/workflows/vecstore-cpp.yml`)覆盖。

### 数据量实测(3 节点 Docker 集群)

通过 `TestT4_DataVolume` 在 3 节点 Docker 集群上对真实 vecstore(Faiss HNSW +
RocksDB,768 维)采样;文档按 window=512 切分,mock embed 服务按 10 ms/chunk 嵌入。
现在每个节点运行自己独立的 vecstore(`run/docker/vecstore/nodeN`)
——共享一个 vecstore 会引发并发的 `Build = Reset + AddChunks` 竞态。

| 指标 | 1,000 篇(早期) | 10,000 篇 | 100,000 篇 | 文档目标 |
|---|---|---|---|---|
| CreateVersion 写入 | 17.0 s | 3m25s(10×1,000) | 42m14s(100×1,000) | T3-4:每 1,000 篇 < 30 s ✅ |
| 索引构建(累计) | 0.5 s | 5.1 s | 2m23s | — |
| 每节点存储 | 3.84 MiB(leader) | 42.5 / 41.8 / 79.6 MiB | 608.7(leader)/ 132.0 / 136.4 MiB | — |
| 3 节点存储合计 | — | 163.9 MiB | 877.1 MiB | — |
| vecstore RocksDB(宿主,去重) | 0.25 MiB | 4.9 MiB | —(每节点独立 vecstore) | — |
| Query top-k=10 | 10 条命中 | 10 条命中 | 主体完成(收尾 Query 卡死已修复) | — |
| 总测试耗时 | 18.1 s | 3m30s | 44m37s(旧实现 30 分钟即卡死) | — |

说明:写入须分批,因为单条 `CreateVersion` 请求必须保持在 4 MiB gRPC 消息上限内
(约 1,400 篇 / 2.8 KB 每篇);每批成为一个版本,且只有前一批达到 READY 后才链接
(PENDING 父版本会被拒绝)。写入耗时随版本号递增——每个版本都重写完整 doc-ID 集并
重建完整索引(100k 末批 ≈ 40s),这是"每版本独立索引"数据模型的固有开销。
100,000 篇运行还实测了 raft 快照:`max_log_length=150` 触发 2 次快照,日志立即
trim 且写入/心跳不停摆——快照在 RLock 下深拷贝、异步序列化持久化,因此 apply
循环与心跳永不被快照阻塞(此前 leader 会冻结 14 分钟无心跳,follower 无法选出
新 leader)。

## 前置依赖

- **Go 1.24**
- **C++17**(vecstore):Faiss ≥ 1.9.0、RocksDB、gRPC、Protobuf、BLAS/LAPACK、OpenMP
- 仅当需要 Faiss HNSW 后端时才需要 C++ 构建。Go 测试使用进程内 mock。
