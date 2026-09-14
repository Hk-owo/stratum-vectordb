# Stratum 设计文档

> 面向 RAG 场景的分布式知识库存储系统
> 本文档记录系统的设计方向、数据模型、模块职责和关键流程，不包含代码层面的实现细节。
> 本文档（v11）已与当前代码实现全面对齐；与此前版本的差异见「版本变更记录」。

---

## 版本变更记录

### v11（当前）：与实现全面对齐

v11 将 v10 之后所有已实现的改动整合进正文，主要变更：

1. **接入层与部署形态（新增）**：`stratum-gateway`（HTTP/JSON 网关 + Web 控制台）、`stratum-router`（集群路由层）、`start.sh` 一键启动、Docker 三节点集群。v10 仅描述纯 gRPC 单进程。
2. **Leader→Follower 数据同步（新增）**：`DataSyncService` 流式推送存储层数据 + `DocIDSetHash` 摘要校验，补齐 v10"多副本独立构建"未描述的数据到达路径。
3. **版本删除（新增）**：`DeleteVersion` 完整流程（`Deleting` 标记 → 异步清理 → 元数据移除），v10 无版本删除概念。
4. **对外接口（新增 RPC）**：`ListKnowledgeBases` / `GetKnowledgeBase` / `DeleteVersion` / `GetClusterStatus` / 内部 `DataSyncService`；`Query` 的 `version_id` 变为可选（缺省用活跃版本）。
5. **版本元数据扩展**：新增 `DocIDSetHash`（文档集摘要）与 `Deleting` 标志；版本 ID 为**全局单调计数器**（v10 称"每知识库独立"）。
6. **WAL 与写路径**：BEGIN 记录携带完整重放输入（kbID、parentVersionID、changes）；新增版本删除两类记录与 `PendingRecordTypeVersionWrite/Delete`；写路径新增步骤 6.5（提交文档集摘要）；索引构建分批执行（单 RPC ≤ 4 MiB 预算）+ 落盘持久化 + 启动 reconcile。
7. **索引管理器完善**：磁盘保留策略（`gc.version_retention_count`）、内存字节阈值换出（`index_manager.memory_threshold_mb`）、磁盘恢复（`loadFromDisk`）、`IndexExists` 磁盘事实、`Evict/Discard/EvictByKB/DeleteFilesByKB`。
8. **删除知识库闭环**：WAL 删除标记由 `DeleteCoordinator.Execute` 第 0 步写入（幂等），磁盘索引文件与版本布隆文件清理落地（含删除墓碑防"复活"）。
9. **存储层细节**：chunk 存在性布隆为**全局单例**且崩溃后**从 chunk-doc 映射重建**（v10 称每知识库一份、从 chunk store 重建）。
10. **运维接口补全**：`GetSystemStatus` 真实填充 `stuck_versions` / `delete_failed_kbs` / `resource_usage` / `wal_alerts`，并新增 `deleting_versions`；`HealthCheck` 三态。

---

## 系统定位

Stratum 是一个面向 RAG 场景的分布式知识库存储系统，解决知识库的版本管理问题。

**用户是知识库的管理者**，不是最终的对话用户。系统解决的核心问题是：

- 知识库更新出错时能快速回滚
- 多版本并存时支持 A/B 测试
- 审计：某次召回结果是基于哪个版本的知识库产生的

对话历史、用户会话管理属于上层应用的职责，不归 Stratum 管。

---

## 核心概念

### 知识库

知识库是 Stratum 管理的基本单位。一个系统里可以有多个知识库，每个知识库有独立的版本历史。

### 版本

每次对知识库的变更产生一个新版本。版本之间有父子关系，形成一条版本链，允许分叉（同一父版本可以有多个子版本，支持 A/B 测试场景）。每个知识库同一时刻只有一个活跃版本，查询默认打到活跃版本。版本可以被删除（`DeleteVersion`，含全部后代）。

### 文档

调用方操作的基本单位。文档有唯一的文档 ID，内容是原始文本。Stratum 对外暴露的操作粒度是文档。

### Chunk

Stratum 内部的存储和向量搜索单位，对调用方不可见。文档在写入时被切成若干 chunk，Stratum 内部调用 embed 服务生成每个 chunk 的向量。chunk 是内容寻址的，通过哈希唯一标识，同一知识库内相同文本 + 相同 embed 配置的 chunk 只存一份。

---

## 对外接口

三个外部 gRPC 服务（`KnowledgeBaseService` / `QueryService` / `AdminService`），监听 `:7000`；内部服务 `DataSyncService`（Leader→Follower 数据同步）不对外暴露。HTTP 接入经 gateway，见「接入层与部署形态」。

### 知识库管理（KnowledgeBaseService）

| RPC | 说明 |
|---|---|
| `CreateKnowledgeBase` | 创建知识库（名称、切割窗口/重叠、embed 配置、索引类型、相似度方式），返回知识库 ID 与初始版本 ID |
| `DeleteKnowledgeBase` | 标记知识库删除中，异步清理，立即返回成功 |
| `ListKnowledgeBases` | 列出全部知识库及其配置、活跃版本、状态（供控制台/运维） |
| `GetKnowledgeBase` | 查询单个知识库的配置与活跃版本 |

### 版本管理（KnowledgeBaseService）

| RPC | 说明 |
|---|---|
| `CreateVersion` | 应用文档变更（ADD / DELETE / UPDATE），产生新版本并异步构建索引 |
| `ListVersions` | 返回知识库的版本链（版本 ID、父版本、创建时间、索引状态、是否删除中） |
| `RollbackVersion` | 切换活跃版本（不停服） |
| `DeleteVersion` | 标记版本（及其全部后代）删除中，异步清理，立即返回成功 |

文档变更列表中每条记录包含：操作类型（ADD / DELETE / UPDATE）、文档 ID、文本内容（DELETE 时不需要）。调用方应将一次业务更新的所有文档变更打包成一个变更列表传入，一次 `CreateVersion` = 一个版本 = 一次索引构建。向量由 Stratum 内部调用 embed 服务生成，调用方不感知 chunk 边界。

### 查询（QueryService）

| RPC | 说明 |
|---|---|
| `Query` | 向量相似度检索。入参：知识库 ID、版本 ID（可选，缺省用活跃版本）、查询向量、top_k、相似度阈值（可选）、chunk 聚合方式（可选，默认中位数）；返回文档 ID、原始文本、相似度分数，以及实际查询的版本 ID |

查询向量由调用方使用 embed 模型生成后传入，Stratum 不感知向量的生成方式，只负责检索。

### 运维（AdminService）

| RPC | 说明 |
|---|---|
| `HealthCheck` | 三态健康（HEALTHY / DEGRADED / UNHEALTHY） |
| `GetSystemStatus` | 系统诊断：stuck 版本、删除失败的知识库、WAL 重放告警、资源占用（已加载索引数 / chunk store 字节 / doc store 字节）、删除中版本 |
| `GetClusterStatus` | 本节点视角的 Raft 集群连通性（供路由层发现 leader） |
| `RebuildIndex` | 重新触发某版本的索引构建（异步） |
| `WarmupVersion` | 将某版本索引加载进内存但不切换活跃版本 |

`GetVersion` / `DiffVersions` / `TagVersion` / `BatchQuery` / `HybridQuery` / `GetMetrics` / `GetAuditLog` 为预留项，proto 中仅文档化、不定义 RPC。

---

## 接入层与部署形态

v10 之后新增的接入层与部署能力：

### HTTP 网关（stratum-gateway）

`cmd/stratum-gateway` 是独立进程：用已生成的 gRPC client stub + `protojson` 把三个外部服务的 RPC 暴露为 REST `/api/*`，并同源托管 Web 控制台静态资源（`web/`），无需 CORS。后端未启动时 `/api` 返回 503。内部 `DataSyncService` 不暴露。

### 集群路由层（stratum-router）

`cmd/stratum-router` 是集群前端：拨号全部节点，经 `GetClusterStatus` 发现 Raft leader，**写请求转发给 leader**（failover 时重新发现），**读请求在各节点间轮询负载均衡**（含故障转移）。gateway 只连路由层（默认 `127.0.0.1:7009`），对集群无感知：

```
web UI ⇄ gateway (:8081) ⇄ router (:7009) ⇄ node1 / node2 / node3
```

### 部署形态

- `start.sh` 一键启动全栈（vecstore(C++) → stratum(gRPC) → router → gateway + web）
- `scripts/docker-cluster.sh` 三节点 Docker 集群 + T4 集成测试
- 节点配置经 YAML（`configs/config1.yaml`，见「配置」）与命令行 flags

---

## 存储层设计

### 数据分层

```
┌─────────────────────────────────────────────────┐
│               版本元数据                          │  ← Raft 状态机，强一致
│   版本 ID / 父版本 / 构建状态 / 文档集摘要 / 删除中 │
└─────────────────────────────────────────────────┘

┌──────────────────┐  ┌───────────────────┐  ┌─────────────────────┐
│    doc store     │  │  chunk-doc 映射   │  │   版本文档列表       │  ← PebbleDB（Go），磁盘持久化
│ 文档原始文本      │  │  chunk ↔ 文档 ID  │  │  版本 ID → 文档 ID  │
└──────────────────┘  └───────────────────┘  └─────────────────────┘

┌───────────────────┐  ┌─────────────────────────────────┐
│    chunk store    │  │         HNSW 索引               │  ← RocksDB / Faiss（C++ vecstore），
│   chunk 向量       │  │       向量近似搜索               │     磁盘持久化，Go 侧通过内部 gRPC 访问
└───────────────────┘  └─────────────────────────────────┘

┌──────────────────────────────┐  ┌─────────────────────────────────┐
│  chunk 存在性布隆过滤器       │  │   版本文档布隆过滤器              │  ← 内存；版本布隆持久化到磁盘
│  （全局单例，重建自映射）      │  │   每个版本一份                   │
└──────────────────────────────┘  └─────────────────────────────────┘
```

### 版本元数据（Raft 状态机）

每个版本记录：

- 版本 ID（**全局单调递增计数器**分配，所有知识库共享一个计数器；v10 曾设计为"每知识库独立"，实现为全局计数器）
- 父版本 ID
- 创建时间（leader apply 时的本地时间戳，不要求跨节点严格单调）
- 索引构建状态（PENDING / READY / FAILED）
- `DocIDSetHash`：该版本全量文档 ID 集合的 SHA-256 摘要。leader 在自身存储层写入完成后提交（`ProposeUpdateVersionSummary`），follower 用它校验 DataSync 拉取完整性（见「Leader→Follower 数据同步」）
- `Deleting`：版本删除中标志（见「版本删除」）

版本元数据体积小、一致性要求高，存在 Raft 状态机里，通过 Raft 日志复制保证多副本一致。

**索引状态说明**：

| 状态 | 含义 |
|---|---|
| PENDING | 元数据已分配，存储层写入进行中，不可查询 |
| READY | 索引构建完成，可查询 |
| FAILED | 构建失败，可通过 RebuildIndex 重新触发 |

WAL 的 COMMIT 记录作为"存储层写入已完成"的唯一标志，崩溃恢复时凭此判断是否需要重放存储写入或直接触发索引构建。

### 版本文档列表（PebbleDB）

- key：`知识库 ID + 版本 ID + 文档 ID`
- value：空

记录每个版本包含的全量文档 ID。写入时从父版本 VersionDocList 前缀扫描拿到全量文档 ID，应用本次变更（ADD 追加、DELETE 移除、UPDATE 替换文档 ID 不变），将最终全量文档 ID 集合写入新版本。写入天然幂等。读取时按 `知识库 ID + 版本 ID` 前缀扫描。删除版本时按 `知识库 ID + 版本 ID` 前缀清理；删除知识库时按 `知识库 ID` 前缀清理。

### 布隆过滤器

**版本文档布隆过滤器**：每个版本一份，内容为该版本全量文档 ID 集合。读路径用于快速过滤不属于目标版本的文档 ID，命中后再去版本文档列表确认（处理假阳性）。随版本创建时构建并持久化到磁盘（`bloom-version/<kbID>/<versionID>.bloom`）；磁盘副本缺失或损坏时，从版本文档列表惰性重建。纯加速器，缺失只会降级为权威确认、不会产生错误结果。

**chunk 存在性布隆过滤器**：写路径用于判断 chunk 是否已存在，不在则直接写入，在则去 chunk store 权威确认（处理假阳性）。**实现为全局单例**（不分知识库——chunk ID 不含知识库 ID），崩溃后**从 chunk-doc 映射重建**（vecstore 侧无 chunk 枚举 RPC；已 GC 的映射对应的陈旧正项由写路径的权威确认兜底）。v10 设计的"每知识库一份、从 chunk store 重建"因缺少枚举 RPC 而调整为上述实现。

### doc store（PebbleDB）

- key：`知识库 ID + 文档 ID + 版本 ID`
- value：完整文档原始文本；DELETE 操作写入墓碑标记（带 tag 字节的空 value）

**写入语义**：只有发生变更（ADD / UPDATE / DELETE）的文档才写入新记录，未变更的文档不产生新条目。

**读取语义**：查询某版本的某篇文档时，按 `知识库 ID + 文档 ID` 前缀扫描，取版本 ID ≤ 目标版本 ID 中最大的一条。若该条为墓碑标记，则认为文档在此版本已删除，不返回内容。

**存储放大**：每次变更只写一条记录，未变更文档零开销。

**删除**：删除版本时物理移除该版本的 MVCC 记录；删除知识库时按 `知识库 ID` 前缀扫描清理。

### chunk store（RocksDB，C++ vecstore）

- key：`知识库 ID + chunk ID`，chunk ID 即 `SHA-256(chunk 文本 + embed 配置 ID)`
- value：chunk 对应的向量

chunk 是内容寻址的，相同文本、相同 embed 配置切出来的 chunk 在同一知识库内只存一份。哈希冲突概率极低（SHA-256），忽略不处理。chunk 向量的权威存储在 C++ vecstore 的 RocksDB 中，Go 侧通过内部 gRPC 访问，不在 PebbleDB 中存储向量数据，无数据冗余。

### chunk-doc 映射（PebbleDB）

维护两个方向的 key（带方向 tag 字节），在同一次写入中一起更新：

- 正向：`知识库 ID + chunk ID + 文档 ID`，value 空。供 Query 读路径按 chunk 找文档
- 反向：`知识库 ID + 文档 ID + chunk ID`，value 空。供 IndexManager 异步构建批量反查

写入天然幂等。删除知识库时按 `知识库 ID` 前缀清理两个方向。

**异步 GC**：文档 DELETE 后，doc store 写入墓碑，但 chunk-doc 映射不立即清理。后台 `ChunkGarbageCollector` 定期扫描（`gc.sweep_interval_s`，默认 300s）：

1. 对每个 chunk，检查其关联文档在该知识库最新版本中是否全部被删除或墓碑化
2. 若是孤儿 chunk：先删映射（正向 + 反向），再经 `ChunkStore.Delete` 删 vecstore 向量（先删映射、后删向量，崩溃后重试幂等）
3. 若仍有存活文档引用：保留 chunk

### 向量索引

每个版本维护一份完整的向量索引。索引类型和相似度计算方式是**知识库级别的固定属性**，创建知识库时指定，创建后不可变；所有版本继承知识库的索引类型和相似度方式，不支持版本级覆盖。变更索引类型等价于换 embed 配置，需要创建新知识库并由调用方迁移数据。

**当前实现**：仅 HNSW，相似度默认余弦相似度（L2 归一化 + 内积等价）。IVF / FLAT 作为接口预留，暂不实现。

HNSW 图结构是全局的，每次 CreateVersion 触发一次全量 HNSW 重建。

**磁盘保留策略**：每个知识库磁盘上只保留最近 N 个版本的索引文件（N 由 `gc.version_retention_count` 配置，N ≤ 0 表示不限制），更老的版本索引文件删除，需要时经 `RebuildIndex` 重建。保留策略在以下时机执行：

- 每次索引构建完成（磁盘文件落盘）后，由构建完成回调触发
- 节点启动时、索引状态 reconcile 之前

执行时按版本 ID 保留最新 N 个，并**豁免活跃版本与刚构建完成的版本**——回滚到旧版本后，活跃版本索引不会被误删。删除索引文件不影响仍在内存中的索引；换出后再查询被裁剪版本会得到"索引未就绪"，可调用 `RebuildIndex` 重建。启动时的索引状态 reconcile 对保留窗口外缺失索引的非 FAILED 版本跳过重建，避免"重启重建 → 立即再裁剪"的震荡。

**多副本构建**：每个副本独立异步构建自己的 HNSW 索引。follower 的存储层数据由 Leader 经 `DataSyncService` 推送（见对应章节），落盘后独立构建；HNSW 构建的随机性导致不同副本图结构可能不完全相同，但召回质量等价，无需同步。

**内存管理**：由索引管理器负责，详见下节。

---

## 索引管理器

索引管理器负责管理内存中的活跃 HNSW 索引，对查询层透明。

### 内存换入换出

- 采用 LRU 策略，最久未访问的版本索引优先换出
- 换出条件有两个，满足其一即触发驱逐：**已加载索引数量达到 LRU 容量上限**（`index_manager.lru_capacity`），或**已加载索引的估算内存占用超过阈值**（`index_manager.memory_threshold_mb`，N ≤ 0 表示不启用字节阈值）
- 每个索引的估算占用 = 构建时的向量载荷字节数（4 字节 × 维度 × chunk 数），随索引文件持久化为 `<versionID>.index.mem` sidecar，重启后从磁盘恢复加载时读取，保证内存记账跨重启一致
- 每次新索引加载/构建完成时检查上述条件，若已超限则先按 LRU 顺序换出再加载，不做后台内存监控（实时累计值 O(1) 可查，无需标志位）

### 换出前置条件

每个索引维护引用计数，记录当前有多少查询正在使用该索引。只有引用计数为 0 的索引才能换出。换出时按 LRU 顺序遍历，跳过引用计数不为 0 的候选，找到第一个可换出的执行换出。

### 并发加载

多个查询请求同时访问同一个冷版本索引时，只触发一次加载（构建/加载单飞），其余请求等待共享结果。若所有内存中的索引引用计数均不为 0，新加载请求等待，直到有索引引用计数归零可换出。等待超时（`load_wait_timeout_ms`）后返回错误，由调用方决定重试策略。

### 磁盘恢复与删除

- `Search` 在目标索引不在内存时先尝试 `loadFromDisk`（经 vecstore `Load` RPC 从 `<dataDir>/index/<kbID>/<versionID>.index` 恢复），失败才报"索引未就绪"
- `IndexExists` 查询 vecstore 侧磁盘事实（Faiss 文件 + `.ids` sidecar 均存在），是"该版本索引已构建且持久化"的权威判定，供启动 reconcile 与崩溃恢复使用
- `Evict` / `EvictByKB` 清除内存条目；`Discard` 在版本删除时同时重置 vecstore 侧索引并删除该版本磁盘文件（含 sidecar）；`DeleteFilesByKB` 在知识库删除时删除整个索引目录
- 知识库删除与版本删除会设置**删除墓碑**，防止删除期间在途的加载 RPC 把索引"复活"回内存

### 索引异步构建

HNSW 索引构建不作为写事务的一部分，而是在 WAL COMMIT 后异步执行：

**构建数据来源**：
1. `VersionDocList.ListDocIDs(kbID, versionID)` 拿到该版本全量文档 ID
2. `ChunkDocMapper.ListChunkIDsByDocs(kbID, docIDs)` 批量反查全量 chunk ID，合并去重
3. 逐个读取 chunk 向量并**分批**：单条 `Build` / `AddChunks` RPC 载荷预算 2 MiB（gRPC 默认上限 4 MiB 的一半），第一批用 `Build` 全量建索引，后续批用 `AddChunks` 增量追加；空版本（无 chunk）也调用一次空 `Build`，保证索引条目存在

**构建期间**：查询请求继续使用上一个已构建完成的版本索引提供服务，新索引在后台构建，不影响查询。

**构建完成**：原子切换到新索引，旧索引等引用计数归零后释放。构建成功则经 `Save` 持久化索引文件到磁盘（该磁盘事实是 READY 的权威依据），构建状态通过 Raft propose 更新为 READY；失败则更新为 FAILED，可通过 `RebuildIndex` 手动触发重建。构建完成回调带指数退避重试，并顺带触发磁盘保留策略。

**启动 reconcile（reconcileIndexStatus）**：节点启动时按磁盘事实收敛各版本索引状态：
- PENDING + 磁盘有索引 → propose READY（回调丢失，状态从磁盘事实推导）
- PENDING + 无索引 → 触发构建
- READY + 无索引 → 重建
- 启用保留策略时，**保留窗口外（版本 ID 小于最新 N 个之外）的缺失索引统一跳过**——无论 PENDING 还是 READY，这类版本的有意缺失不被重建，避免"重启重建 → 立即再裁剪"震荡
- FAILED 不动（需显式 RebuildIndex）

**崩溃恢复**：重启后发现 WAL 有 COMMIT 记录但版本状态仍为 PENDING：若索引文件已存在则 propose READY，否则触发异步构建。

---

## 写路径

### 文档切割与向量生成

文档写入时，Stratum 内部按滑动窗口策略切割成 chunk：

- 窗口大小和重叠大小在创建知识库时由调用方配置
- 对每个 chunk 计算 `SHA-256(chunk 文本 + embed 配置 ID)` 作为 chunk ID
- 切割完成后 Stratum 内部调用知识库绑定的 embed 服务生成每个 chunk 的向量
- 文档长度小于窗口大小时，整篇文档作为一个 chunk

### 事务保证（WAL）

写路径使用 WAL 保证崩溃一致性，事务边界如下：

**关键时序约束**：Raft 状态机 apply `ProposeCreateVersion` 时，必须先同步写入 `WAL.WriteVersionID(versionID)`，写入成功后才更新状态机内存状态。`WriteVersionID` 对同一 versionID 重复写入幂等。此约束保证：WAL 有 VERSION_ID 记录时状态机里一定有对应版本，不会产生孤儿版本。

**BEGIN 记录载荷**：BEGIN 记录持久化该事务的完整重放输入（kbID、parentVersionID、changes 列表），使"有 VERSION_ID 但无 COMMIT"的恢复可以直接重放全部存储写入，无需从任何中间状态反推。

**事务内（同步完成，调用方阻塞返回）**：
1. WAL 写入 BEGIN 记录（携带重放输入）
2. 版本元数据提交 Raft（`ProposeCreateVersion`）；apply 阶段先写 WAL VERSION_ID 记录，再分配新版本 ID 并写入状态机，阻塞到多数派确认并 apply 完成后返回 versionID
3. 对每个变更文档切割 chunk、调用 embed 服务生成向量；对每个 chunk，查 chunk 存在性布隆过滤器：不存在则写入 vecstore chunk store 并更新布隆；存在则去 chunk store 权威确认，确认不存在才写入。chunk-doc 映射写入 PebbleDB（正向 + 反向，幂等）
4. 文档原始文本写入 doc store（key 含第 2 步分配的版本 ID；DELETE 写墓碑）
5. 版本文档列表写入 PebbleDB：从父版本全量文档 ID 集合应用本次变更，写入新版本；构建版本文档布隆过滤器并持久化（持久化失败非致命，读路径惰性重建）
6. WAL 写入 COMMIT 记录（含版本 ID）
7. **提交文档集摘要**：`ProposeUpdateVersionSummary` 将全量文档 ID 集合的 SHA-256 摘要写入版本元数据（非致命；供 follower 校验 DataSync 拉取）

**事务外（异步）**：
8. HNSW 索引异步构建（分批 Build + AddChunks → Save 落盘）
9. 构建完成后版本状态 propose 更新为 READY；失败则更新为 FAILED

### 幂等性

- chunk store 写入：内容寻址，重复写入幂等
- chunk-doc 映射写入：key 含文档 ID / chunk ID，重复写入幂等
- doc store 写入：key 含版本 ID，重复写入幂等
- 版本文档列表写入：key 含文档 ID，重复写入幂等
- WAL 各类记录：按 versionID / kbID 幂等
- Raft 提交：由 Raft 本身保证幂等

### 崩溃恢复（WAL 重放）

启动时 `Recover` 返回全部需处理的 PendingRecord，`runCrashRecovery` 分类处理：

| 场景 | 处理 |
|---|---|
| BEGIN 无 VERSION_ID | Raft propose 未完成，无对应版本，重新 propose；无需清理 |
| VERSION_ID 无 COMMIT（`PendingRecordTypeVersionWrite`） | 跳过 Raft propose，使用 WAL 记录版本 ID 与 BEGIN 载荷从头重放存储写入（步骤 3-6），每步幂等；**无本地事务输入**（follower apply 或旧格式 WAL）无法重放，由 Raft 日志重放 + DataSync 恢复，并计入重放失败计数器 |
| COMMIT 但版本仍 PENDING | 检查索引文件是否存在：存在则 propose READY，不存在则触发异步构建 |
| 删除标记无完成记录（`PendingRecordTypeDeleteMark`） | resume 知识库删除流程（幂等） |
| 版本删除标记无完成记录（`PendingRecordTypeVersionDelete`） | resume 版本删除流程（幂等） |

重放失败经 `ReplayCounter` 计数并暴露给 `GetSystemStatus.wal_alerts`（内存态，重启清零）。

**重放失败不中止启动**：任一 PendingRecord 重放失败——KB/版本不在尚未追平的
raft 状态机、KB 已被删除、vecstore/embed 等依赖宕机——经进程内有限退避重试
（3 次 × 100ms 指数）后**跳过并计数**，节点继续启动；记录保留在 WAL、下次重启
重试，数据收敛由 Raft 日志重放 + DataSync 完成。启动不再因重放失败
`logger.Fatal`（修复前 `crash recovery failed` 会让节点崩溃循环、永久离线）。

---

## 读路径

### 完整流程

1. 收到知识库 ID + 版本 ID（可选）+ 查询向量 + top_k + 相似度阈值 + chunk 聚合方式（可选，默认中位数）
2. 解析目标版本：未指定版本 ID 时用活跃版本；校验版本存在且索引状态为 READY（PENDING / FAILED 拒绝）
3. 索引管理器加载目标版本 HNSW 索引（LRU 换入 + 引用计数 + 冷加载/磁盘恢复）
4. HNSW 搜索返回 **top_k × 3** 个 chunk ID 和相似度分数（固定 3 倍冗余供聚合，上限 1000；v10 的"top_k × N"中 N 实现为固定 3）
5. 过滤低于相似度阈值的结果
6. 对每个 chunk ID 前缀扫描 chunk-doc 映射，拿到所有关联文档 ID
7. 按文档 ID 分组，同一篇文档命中多个 chunk 时按指定聚合方式（最高分 / 中位数 / 平均分）计算分数，去重
8. 查版本文档布隆过滤器，过滤不属于目标版本的文档 ID；布隆命中的去版本文档列表确认；布隆缺失时降级为仅权威确认
9. 按分数排序，取前 top_k 篇文档
10. 对每个文档 ID 扫描 doc store，取版本 ID ≤ 目标版本 ID 的最大条目，拿到完整文档文本
11. 引用计数减 1，返回文档 ID、原始文本、相似度分数

### 结果数量

若去重后文档数量不足 top_k，直接返回现有结果，不强求数量。相似度低于阈值的结果不返回。空版本（无文档）返回空结果而非错误。

---

## Leader→Follower 数据同步

v10 只描述"每个副本独立构建索引"，未描述 follower 的存储层数据如何到达。实现补齐为 `internal/sync`：

**数据流**：leader 的 gRPC 服务器注册 `DataSyncService`；follower 在 Raft apply 到 `cmdCreateVersion`（本节点非 leader）时，向 leader 发起 `PullVersionData` 流式拉取（server-side streaming），覆盖五段数据：

1. VersionDocList（该版本全量文档 ID）
2. DocStore（每篇文档在 ≤ 目标版本的最大版本条目，含墓碑，保证 MVCC 状态可重建）
3. ChunkDocMapper 反向（文档 → chunk）
4. ChunkDocMapper 正向（chunk → 文档）
5. ChunkStore（chunk 向量）

follower 将每条 SyncEntry 幂等地写入本地存储（全部按 key 幂等），随后触发独立的 HNSW 构建。

**完整性校验**：leader 在自身存储写入完成后，把该版本全量文档 ID 集合的 SHA-256 摘要提交进版本元数据（`DocIDSetHash`）。follower 每次拉取后重算本地集合摘要并比对：不一致则退避重试，直到一致（闭环"follower 早于 leader 落盘拉到不完整数据"的竞态）；leader 从未提交摘要时（初始/空版本或错过的 propose），拉取产生数据即接受。

**幂等与容错**：拉取失败在 Raft apply 路径退避重试；本节点即 leader 时跳过拉取（数据已由写路径直接落盘）；重启后 Raft 重放历史日志时，对无本地事务输入的版本同样经 DataSync 补齐。

---

## 版本删除

v10 无版本删除；实现新增 `DeleteVersion`：

**流程**：
1. RPC 经 `ProposeMarkVersionDeleting` 在 Raft 状态机 apply 时校验并标记：目标版本属于该知识库；递归子树（含全部后代）内没有活跃版本、没有 PENDING 版本；对已标记删除中的子树幂等
2. `DeleteVersionCoordinator` 异步清理每个 `Deleting` 版本：WAL 写版本删除标记 → `IndexManager.Discard`（清内存 + 重置 vecstore 侧 + 删除磁盘索引文件）→ `VersionDocList.DeleteByVersion` → `DocStore.DeleteByVersion` → `ProposeRemoveVersionMeta`（幂等）→ WAL 写版本删除完成
3. 删除中版本不可查询、不可作父版本、不可回滚；未完成的清理（`Deleting` 未达元数据移除）暴露给 `GetSystemStatus.deleting_versions`

**崩溃恢复**：WAL 有版本删除标记但无完成记录时，resume 该流程（幂等）。

---

## 版本回滚

### 流程

1. 调用方发起回滚请求，指定目标版本 ID
2. 校验目标版本属于同一知识库、索引状态为 READY、且不在删除中
3. Stratum 通过 Raft 提交活跃版本切换（`ProposeRollback`，apply 时校验目标非删除中），更新知识库元数据中的活跃版本 ID
4. apply 完成后活跃版本切换生效
5. 上层应用服务器通过轮询感知活跃版本变化，后续请求切换到新版本
6. 旧版本索引引用计数归零后，由 LRU 自然换出

### 不停服

回滚期间正在进行的查询通过引用计数保护，继续使用旧版本索引直到完成。新请求在活跃版本切换后打到目标版本索引。两个版本索引短暂共存，无需停服。版本切换通知采用轮询而非回调，避免网络异常或重启导致信号丢失。

### 版本链约束

- 父版本必须属于同一知识库
- 父版本不能是 PENDING 状态，也不能是删除中状态
- 允许分叉（同一父版本可以有多个子版本），支持 A/B 测试场景

---

## 删除知识库

删除操作需要清理四类数据，采用 WAL 标记保证崩溃后可继续清理。清理流程由 `DeleteCoordinator` 异步执行（`DeleteKnowledgeBase` RPC 先经 Raft 标记删除中，再启动异步清理）：

0. WAL 写入删除标记 `WriteDeleteMark`（幂等，作为"删除已开始"的持久化记录；由 `DeleteCoordinator.Execute` 第 0 步写入）
1. 清理内存中的 HNSW 索引（`IndexManager.EvictByKB`）
2. 清理磁盘上的 HNSW 索引文件与版本文档布隆过滤器文件（文件/目录不存在时忽略，幂等）：
   - 索引文件：`IndexManager.DeleteFilesByKB` 删除 `dataDir/index/<kbID>/` 下全部 `.index` / `.index.ids` / `.index.mem` 文件，并设置删除墓碑，防止删除期间在途的索引加载 RPC 把索引重新加载回内存
   - 版本布隆文件：`VersionBloomStore.DeleteByKB` 删除 `bloom-version/<kbID>/` 目录并清空缓存
3. 清理 doc store 中该知识库所有版本的文档数据
4. 清理 chunk store 和 chunk-doc 映射中该知识库所有 chunk（含孤儿 chunk 向量）
5. 清理版本文档列表中该知识库所有条目
6. 清理 Raft 状态机中的版本元数据（`ProposeRemoveKBMeta`；若元数据已不存在则视为成功，幂等）
7. WAL 写入删除完成记录 `WriteDeleteComplete`

崩溃恢复时，发现 WAL 有删除标记但没有完成记录，从第 1 步继续执行。WAL 标记幂等（同一知识库重复写不产生重复记录），第 2 步文件删除忽略 `ErrNotExist`，第 6 步 `ProposeRemoveKBMeta` 忽略 `ErrKnowledgeBaseNotFound`，保证每一步均可安全重复执行。

**永久性错误处理**：每一步失败后指数退避重试，重试次数耗尽后将知识库状态标记为 `DeleteFailed`，暴露给 `GetSystemStatus`，由运维人工介入。WAL 记录保留，方便后续离线工具重新执行清理。

---

## 配置

节点启动读取 YAML 配置（`configs/config1.yaml`），命令行 flags 覆盖文件值。主要字段：

| 段 | 字段 | 说明 |
|---|---|---|
| node | `node_id` / `grpc_addr` / `raft_addr` / `gateway_http_addr` | 节点身份与监听地址 |
| raft | `peers` / `heartbeat_interval_ms` / `election_timeout_min_ms` / `election_timeout_max_ms` | 集群成员与选举时序 |
| storage | `data_dir` | 各存储（docstore / chunkdoc / versiondoc / wal / index）的根目录 |
| vecstore | `rocksdb_path` / `grpc_addr` | C++ 侧存储与内部 gRPC 地址 |
| write_coordinator | `max_retries` / `retry_base_interval_ms` | 写路径重试 |
| delete_coordinator | `max_retries` / `retry_base_interval_ms` | 删除路径重试 |
| index_manager | `lru_capacity` / `memory_threshold_mb` / `load_wait_timeout_ms` / `callback_max_retries` / `callback_retry_base_interval_ms` | 索引管理器容量、内存阈值与超时 |
| bloom_filter | `expected_items` / `false_positive_rate` | 两类布隆过滤器参数 |
| knowledge_base_defaults | `chunk_window_size` / `chunk_overlap_size` / `index_type` / `similarity` | 知识库默认参数 |
| gc | `version_retention_count` / `sweep_interval_s` | 索引磁盘保留数（≤ 0 不限制）与孤儿 chunk 扫描间隔 |
| wal | `replay_retry_threshold` | WAL 重放重试阈值（配置预留） |
| logging | `level` | 日志级别 |

注意：`index_manager.memory_threshold_mb` 与 `gc.version_retention_count` 在 v11 起生效（此前仅为配置示例、代码未消费）；`gc.audit_log_retention_days` 等字段仍为配置预留，代码未消费。

---

## 未来分片架构（预留）

当前设计是单 Raft 集群，所有知识库的版本元数据由同一个 Raft group 管理。这在中型企业、知识库数量在几十到上百量级时不构成瓶颈。

如果未来需要分片（多个 Raft group，每组管理一部分知识库），架构上预留以下设计点：

- **分片映射结构**：知识库 ID → Raft group ID 的映射关系。由于所有存储层 key 都以知识库 ID 为前缀，数据本身天然按知识库隔离，分片时不需要重新设计 key 结构，只需按知识库 ID 划分到不同 Raft group。

- **RaftNode 接口不变**：当前 `RaftNode` 接口封装单个 Raft 集群的操作。分片后，Service 层从持有单个 `RaftNode` 实例变为持有"`RaftNode` 实例集合 + 路由表"，`RaftNode` 接口本身保持不变，分片对接口零侵入。

---

## 待细化的问题

- HNSW 索引构建触发后的默认超时时间
- 相似度阈值的默认值设定
- IVF / FLAT 索引的真实实现：Faiss IVF 需要训练阶段（k-means 聚类中心），FLAT 无索引结构、查询为暴力计算，二者的构建流程和内存管理策略与 HNSW 差异较大，需要单独设计
- 审计日志模块：`GetAuditLog` 为预留接口。当前 `Query` 链路**没有**审计日志写入（v10 曾描述"异步写入审计日志"为占位步骤，实现中直接省略）；若未来需要强合规审计，需设计 `AuditLogger` 接口、存储 key 与异步写入的可靠性
- HybridQuery / Bleve 混合查询：Bleve 关键词索引的构建时机、是否按版本隔离、与现有 IndexManager 的索引生命周期管理是否统一
