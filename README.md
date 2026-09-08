# Stratum

**分布式版本化向量检索引擎 —— 为 RAG 构建的可回滚、可审计知识库存储层。**

文档经 embed 服务向量化后,以 **MVCC 版本** 为单位组织、索引与查询:一次写入产出独立新版本,查询永远落在明确的版本上,回滚 / 并行比对(A/B)/ 审计都是一等公民。元数据由 **Raft** 在节点间强一致,向量由 **C++ Faiss HNSW** 索引;存储按 **L0 内存热层 / L1 磁盘冷层 / L2 永久层** 分层,大知识库可选**量化两段式检索**,内存载荷压缩 4–32× 而精度不变。

名字取自"地层":文档的每次变更形成一层可独立回滚、并行比对的版本;存储亦分热、冷、永久三层。

## 亮点

- **MVCC 版本化知识库** —— 版本链 + 活跃版本随时切换:
  - **回滚**:一次有问题的更新,一条调用即可撤销,不停服;
  - **A/B 测试**:多个版本并存、可被并行查询;
  - **审计**:每个查询结果都追溯到产生它的具体版本。
- **内容寻址 chunk** —— `ChunkID = SHA-256(text + model ID)` 天然去重,无需跨节点协调。
- **每版本独立 HNSW 索引** —— 版本之间零干扰;索引按 LRU 驻留内存,换出/重载廉价,不会影响查询结果精度。
- **量化两段式检索(可选,默认全精度)** —— 开启后内存只驻留量化粗筛器(HNSW 图 + SQ/PQ 码),查询先粗筛出候选、再按候选读 L2 全精度向量做精确 rerank:精度由全精度 rerank 决定,量化只影响候选覆盖。量化后端为 Faiss 内建 `IndexHNSWSQ`/`IndexHNSWPQ`——评估 hnswlib(无量化)、DiskANN(C++ 分支已停止维护)、USearch(仅免训练标量降位、需整体换后端)后选定:零新依赖,且与现有 `write_index`/`read_index` 序列化格式完全兼容。
- **索引对象生命周期状态机** —— 每个 vecstore 索引实例显式区分 `EMPTY`/`BUILDING`/`READY` 三态,读锁贯穿查询(含 rerank 磁盘 IO)、写锁保护构建/加载/重置,杜绝 Search 与并发 Reset/Load/AddChunks 之间的 use-after-free 与错位结果。
- **Raft 强一致 + 崩溃一致性** —— 元数据写操作经 leader 并受 WAL 保护;查询可负载均衡到任意节点;快照不阻塞心跳与写入。
- **自动存储卫生** —— 周期 chunk GC、每版本布隆过滤器、启动时从磁盘事实推导版本 READY 状态(不依赖构建回调确实送达)。
- **开箱可运维** —— 三态健康检查;HTTP 网关 + Web 控制台(同源提供、免 CORS);`start.sh` 一条命令拉起完整链路。

Stratum 是 RAG 管线的**存储与检索层**:不处理聊天历史、用户会话或 prompt 构造——这些属于其上方的应用层。

## 快速开始

```bash
# 构建
go build ./cmd/stratum/

# 运行全部测试(23 个包)
go test ./... -timeout 180s -count=1

# 竞态检测(共识与索引核心)
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# 单节点服务
go run ./cmd/stratum/

# 一键拉起完整链路:vecstore(C++) → stratum(gRPC) → 路由层 → gateway(HTTP) + Web UI
./start.sh            # 然后打开 http://localhost:8081
```

`cmd/stratum` 接受可选 YAML 配置用于多节点部署,命令行 flag 优先于文件:

```bash
go run ./cmd/stratum/ -config integration/docker/config1.yaml
```

**3 节点 Docker 集群**(CI 同款,`integration/docker` + `docker` 构建标签):

```bash
scripts/docker-cluster.sh up 3 --with-embed
go test ./integration/docker/... -tags=docker -timeout 300s
scripts/docker-cluster.sh down
```

## 架构

```
                    外部客户端:gRPC SDK · HTTP 网关 / Web 控制台
                                    │
┌───────────────────────────────────▼───────────────────────────────────┐
│                      Go 编排层(stratum 节点)                           │
│                                                                        │
│   KnowledgeBaseService · QueryService · AdminService                   │
│        │                     │                       │                 │
│   ┌────▼─────┐          ┌────▼─────┐           ┌─────▼────┐            │
│   │ WriteCoord│         │ QuerySvc │           │  AdminSvc │            │
│   └────┬─────┘          └────┬─────┘           └─────┬────┘            │
│        │                     │                       │                 │
│   ┌────▼─────────────────────▼───────────────────────▼────┐            │
│   │ IndexManager:每版本索引 LRU 缓存 + 引用计数 + 异步构建  │            │
│   └────┬───────────────────────────────────────────────────┘            │
│        │                                                               │
│   PebbleDB:DocStore(MVCC) · ChunkDoc 双向映射 · VersionDoc             │
│        │                                                               │
│   Raft 共识:强一致元数据 · 两阶段 WAL · 快照                            │
└───────┼───────────────────────────────────────────────────────────────┘
        │ 内部 gRPC
┌───────▼───────────────────────────────────────────────────────────────┐
│                    C++ vecstore(向量存储与检索)                        │
│                                                                        │
│  L0 内存:每 (kb, version) 一份 HNSW 索引 / 量化粗筛器                  │
│          生命周期状态机 EMPTY → BUILDING → READY + 每实例读写锁         │
│  量化路径:① 粗筛 top-N → ② 按候选读 L2 全精度原向量 → ③ 精确 rerank    │
│  L1 磁盘:<versionID>.index(.ids/.mem 边车)冷层,Load 即恢复             │
│  L2 永久:RocksDB 全精度向量(内容寻址、不压缩,权威数据)                 │
└─────────────────────────────────────────────────────────────────────────┘
```

Go 层只负责编排与元数据,向量计算全部下沉到 C++ vecstore;两段检索的并发语义(Search 持读锁贯穿,含 rerank 磁盘 IO)由索引对象的显式状态机 + 每实例锁保证,杜绝 use-after-free 与错位结果。

## 核心概念:版本化文档库

- **版本链**:`CreateVersion` 一次调用应用任意数量的文档变更(ADD / DELETE / UPDATE),产出新版本并异步构建索引;父版本须已 READY,允许分叉。`RollbackVersion` 无停机切换活跃版本。
- **MVCC 零成本快照**:基于 PebbleDB 前缀编码,未变更文档在新版本中零拷贝;文档历史被压缩保存。
- **布隆过滤器**:每个版本一份完整文档 ID 集合的布隆过滤器,成员检查开销极低;磁盘副本缺失时自动从 `VersionDocList` 重建。
- **垃圾回收**:`ChunkGarbageCollector` 周期性(默认 5 分钟,`chunk_gc.sweep_interval_sec`)清扫不再被任何版本引用的 chunk。sweep 两遍:先无锁枚举孤儿候选,再持写锁按 Raft **当前**版本复查后删除,与并发写入互斥、不依赖过期快照(stale-snapshot race 免疫),锁粒度为一个 chunk,阻塞毫秒级。
- **启动 reconcile**:启动时从磁盘事实(Faiss 索引文件 + `.ids` 边车)推导版本 READY 状态,而不是信任崩溃前可能丢失的构建回调。

## 向量检索:全精度与量化两段式

量化开关在**创建知识库时固定**(创建后不可变,变更需新建 KB 迁移),默认 `OFF`(全精度,行为等同单段精确检索):

| | 全精度 `OFF`(默认) | 量化(SQ8 / SQ_BF16 / SQ_FP16 / PQ) |
|---|---|---|
| 内存驻留 | 完整 HNSW(图 + float32 向量) | 粗筛器(HNSW 图 + 量化码 + 码本),约 1/4–1/32 载荷 |
| 查询路径 | 单段,内存直接出 top-k(精确) | 两段:粗筛 top-N → 读 RocksDB 全精度原向量 → 精确 rerank 出 top-k |
| 最终精度 | 精确 | 由 rerank 的全精度向量决定;量化只影响候选覆盖(召回) |
| 磁盘 | `.index`;向量在 RocksDB | 同左(全精度向量不压缩、永驻 RocksDB) |

评分/阈值口径两条路径一致(统一"分高者相似",COSINE 以归一化 + 内积实现,数学等价;欧氏距离取负)。

**量化类型**(Faiss 内建,码本随 `.index` 持久化,Load 免配置;每向量载荷,示例 = d=768、n=100 万单版本):

| 类型 | 载荷 | 训练 | 示例内存 | 备注 |
|---|---|---|---|---|
| `OFF`(Flat) | 4 B/维 | 无 | ≈ 3.07 GB | 默认,单段精确 |
| `SQ8` | 1 B/维 | 首包一次 | ≈ 768 MB | 合成数据召回 ≈ Flat |
| `SQ_FP16` | 2 B/维 | 免训练 | ≈ 1.54 GB | 同 SQ8,推荐档 |
| `SQ_BF16` | 2 B/维 | 免训练 | ≈ 1.54 GB | 无硬件 bf16 时延迟 ~7×,建议回避 |
| `PQ`(m, 8-bit) | `m` B/向量 | 首包 k-means | ≈ 96 MB(m=96) | 最省;召回随候选 N 提升,建议加大候选 |

> 量化后 HNSW 图边成为内存主导项(实测 d=64、M=32 ≈ 272 B/节点);内存记账按 vecstore 报告的粗筛器口径(图边 + 码)。

**实测**(2026-09,合成数据;方法见 `Stratum_设计文档v12.md` §2.5 与附录 D):

- 召回:SQ8 / SQ_BF16 / SQ_FP16 ≈ Flat HNSW;PQ 随候选 N 提升(0.56@32 → 0.75@256);
- 端到端(top-10、候选 N=80、热 cache、单线程):OFF ≈ 0.27 ms,SQ8/FP16 ≈ 0.71 ms,PQ ≈ 1.0 ms——增量来自候选放大读盘 + rerank,可按需调小 `candidate_n`。

## 部署形态

- **单节点**:`cmd/stratum`(gRPC,默认 `127.0.0.1:7000`)+ 外部 C++ vecstore 进程。
- **多节点集群**:多个 stratum 节点组成 Raft 集群,共享一套元数据。
- **路由层 `cmd/stratum-router`**:集群前端。拨号所有节点、自动发现 Raft leader;**写操作转发给 leader**(故障转移时重新发现),**读操作 round-robin 负载均衡**到各节点。
- **网关 `cmd/stratum-gateway`**:把三个外部 gRPC 服务暴露为 REST/JSON,并从同源提供 Web 控制台静态资源(`web/`),因此无需 CORS。内部 `DataSyncService`(leader→follower 数据同步)有意不对外。

```
Web UI ⇄ gateway(:8081) ⇄ router(:7009) ⇄ node1 / node2 / node3
```

```bash
scripts/gateway.sh [single|build|stop]   # 默认:构建后起 Docker 集群模式;single=连 127.0.0.1:7000
scripts/router.sh status|stop            # 单独管理路由层

# 手动等价:先起路由层,再起 gateway(始终指向路由层)
./run/bin/stratum-router  -listen 0.0.0.0:7009 -nodes 127.0.0.1:7000
./run/bin/stratum-gateway -grpc-addr 127.0.0.1:7009
```

环境变量可覆盖默认:`STRATUM_HTTP_ADDR`(网关监听,默认 `0.0.0.0:8081`)、`STRATUM_ROUTER_ADDR`(默认 `127.0.0.1:7009`)、`STRATUM_GRPC_ADDR`(单机模式下路由层应连的节点,默认 `127.0.0.1:7000`)。

`start.sh` 一键构建并启动完整链路:路由层与控制台先行,数据库服务经控制台 `/ops/start` 端点拉起——Web UI(默认 `http://localhost:8081`,含「运维」页)在数据库未运行时也可用;Ctrl+C 干净停止,日志在 `run/log/`。仅需运维:直接运行 `./run/bin/stratum-gateway`,在「运维」页编辑 `run/console.yaml` 的启动参数并启停服务。

> Docker 集群模式下 vecstore 是宿主机上的外部依赖(`vecstore.grpc_addr: host.docker.internal:7100`),必须监听宿主机的**对外接口**(`--grpc_addr=0.0.0.0:7100`);只绑 `127.0.0.1` 时容器内无法访问,会导致索引构建失败、删除报错等连锁问题。

## gRPC API

Protobuf 定义在 `api/proto/`,含三个外部服务与两个内部服务(`DataSyncService`、vecstore 服务)。

**KnowledgeBaseService**(知识库与版本生命周期)

| RPC | 说明 |
|---|---|
| `CreateKnowledgeBase` | 创建知识库(embed 配置、chunk 窗口、索引类型) |
| `DeleteKnowledgeBase` | 标记删除,清理异步执行 |
| `CreateVersion` | 应用文档变更(ADD / DELETE / UPDATE)并产出新版本 |
| `ListVersions` | 返回知识库版本链 |
| `RollbackVersion` | 切换活跃版本,无停机 |
| `ListKnowledgeBases` / `GetKnowledgeBase` | 列出 / 查询知识库及其活跃版本 |
| `DeleteVersion` | 标记版本(及后代)删除,清理异步执行 |

**QueryService**

| RPC | 说明 |
|---|---|
| `Query` | 向量相似度检索(阈值、top-k、聚合) |

**AdminService**

| RPC | 说明 |
|---|---|
| `HealthCheck` | 三态健康检查(HEALTHY / DEGRADED / UNHEALTHY) |
| `GetSystemStatus` | 卡住版本、删除失败的知识库、WAL 告警、资源占用 |
| `GetClusterStatus` | 节点 Raft 视图(node_id / leader_id / member_count),供路由层发现 leader |
| `RebuildIndex` / `WarmupVersion` | 重试失败版本的索引构建 / 预热版本索引入内存(不切换活跃版本) |

## 工程与测试

共识层(`internal/kvraft`)改编自 [KVServer](https://github.com/Hk-owo/KVServer)(MIT 6.5840 教学实现),重写为 Stratum 代码风格并经 TDD 修复 6 个缺陷——多数派检查缺失导致单节点选不出 leader、心跳跳过日志一致性检查、InstallSnapshot 持锁发送(死锁风险)、RequestVote 未检查 `killed()`、选举后无 no-op 条目、leader 把自己加为 peer。

| 批次 | 范围 | 状态 |
|---|---|---|
| T1 | 单模块契约(8 个模块) | ✅ |
| T2 | 跨模块集成(4 组) | ✅ |
| T3 | 单节点全链路(15 个场景) | ✅ |
| T4 | 3 节点 Raft 集群(进程内 + Docker 集群 + 数据量) | ✅ |
| T5 | 真实栈 e2e(真实 Pebble/WAL/Raft/IndexManager + vecstore 子进程,零 mock) | ✅ |

```bash
go test ./integration/... -run TestRealStack -v   # 全真实栈端到端(需 C++ 二进制,缺失时跳过)
go test ./integration/... -run TestMultiNode -v   # 3 节点进程内集群:选主、复制 KB + 版本元数据
```

全量单测(23 包)与 3 节点 Docker 集群(T4)命令见[快速开始](#快速开始)。

CI(`.github/workflows/ci.yml`,push main 与 PR):gofmt + `go vet` + `go build` + 23 包单测 + raft/kvraft/index 竞态检测 + 3 节点 Docker 集群(T4)容错运行;C++ vecstore 由手动触发的工作流(`vecstore-cpp.yml`)覆盖。

### 数据量实测(3 节点 Docker 集群,真实 Faiss HNSW + RocksDB,768 维)

`TestT4_DataVolume` 在真实栈上采样(window=512 切分,mock embed 10 ms/chunk);每个节点运行独立的 vecstore(共享会引发并发 `Build = Reset + AddChunks` 竞态):

| 指标 | 1,000 篇 | 10,000 篇 | 100,000 篇 |
|---|---|---|---|
| CreateVersion 写入 | 17.0 s | 3m25s | 42m14s |
| 索引构建(累计) | 0.5 s | 5.1 s | 2m23s |
| 3 节点存储合计 | — | 163.9 MiB | 877.1 MiB |
| 总测试耗时 | 18.1 s | 3m30s | 44m37s(旧实现 30 分钟即卡死) |

要点:写入须分批——单条 `CreateVersion` 受 4 MiB gRPC 消息上限约束(约 1,400 篇),每批成一版本且前一批 READY 后才链接;耗时随版本号递增(每版本重写完整 doc-ID 集并重建索引),这是"每版本独立索引"模型的固有开销。100k 轮还验证了 raft 快照(`max_log_length=150` 触发 2 次):日志即时 trim、写入与心跳不停摆——快照在 RLock 下深拷贝并异步持久化,apply 与心跳永不被阻塞(此前 leader 会冻结 14 分钟无心跳)。

## 项目结构

```
api/proto/           # Protobuf 定义(.proto)→ 生成代码在 api/proto/stratum/
internal/            # Go 核心
  docstore/          #   MVCC 文档存储(PebbleDB)
  chunkdoc/          #   chunk ↔ 文档双向映射
  bloom/             #   chunk 存在性 + 每版本文档布隆过滤器
  index/             #   IndexManager(LRU + 引用计数 + 异步构建)
  kvraft/            #   Raft 共识(选主 / 日志复制 / 快照)
  raft/              #   Stratum Raft 状态机(KB + 版本元数据)
  wal/               #   崩溃一致性写前日志
  sync/              #   Leader→Follower 数据同步(DataSync)
  coordinator/       #   Write / Delete / DeleteVersion / chunk-GC 编排
  router/            #   写 → leader、读负载均衡的前端
  splitter/ embed/ chunkstore/ kvstorage/ pebbleutil/ types/ errors/
service/             # gRPC 服务实现(含测试)
cmd/stratum/         # 节点入口(-config YAML / flags)
cmd/stratum-router/  # 路由层:单地址接入 Raft 集群
cmd/stratum-gateway/ # HTTP/JSON 网关 + /ops 控制台控制面
vecstore/            # C++ 向量存储:Faiss HNSW + RocksDB,支持量化两段式检索
web/                 # Web 控制台前端(HTML/CSS/JS)
integration/         # mock 集成 + 真实栈 e2e + docker/(T4 集群测试)
scripts/             # docker-cluster.sh / gateway.sh / router.sh 等
configs/             # 示例配置文件
```

## 配套文档

仓库内还维护一套随实现演进的中文设计文档(与代码同目录、不随本仓库发布,本地阅读用):

- `Stratum_设计文档v12.md` —— 最新版设计总纲(含量化两段式 §2.5/§2.6、附录 D 实测方法)
- `Stratum_接口设计v9.md` —— gRPC/内部接口与语义
- `Stratum_设计目标.md` —— 功能目标、性能指标与验收标准
- `Stratum_测试顺序.md` / `Stratum_实现顺序.md` / `Stratum_代码风格.md`
- `改动内容.md` —— 开发日志

## 前置依赖

- **Go 1.24**
- **C++17**(vecstore):Faiss ≥ 1.9.0、RocksDB、gRPC、Protobuf、BLAS/LAPACK、OpenMP
- C++ 构建仅在需要 Faiss HNSW 后端时必需;Go 单测使用进程内 mock。
