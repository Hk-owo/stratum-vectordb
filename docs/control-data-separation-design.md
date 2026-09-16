# 控制层 / 数据层分离设计方案

| 项目 | 内容 |
|---|---|
| 文档状态 | 草案（Design Draft），未实现 |
| 适用范围 | 将 Stratum 从"同构全量副本"演进为"控制集群 + 存储集群" |
| 前置阅读 | `README.md` 架构章节 |
| 本文性质 | 设计说明。文中标注 `【现状】` 的部分是已存在的代码事实（附文件:行号）；标注 `【目标】` 的部分是待实现设计 |

---

## 1. 背景与目标

### 1.1 现状

Stratum 当前每个 `stratum` 进程是**同构**的，一个进程同时承载：

```
一个进程 = Raft 副本 + 全量 DocStore/ChunkDoc/VersionDoc + 全量 vecstore
           （元数据）      （数据，PebbleDB）        （索引，C++）
```

数据是**全量副本**而非分片：leader 通过 `DataSync` 把 DocStore、ChunkDocMapper、VersionDocList、ChunkStore **全量**流给每个 follower（`internal/sync/sync.go`），每个 follower 再独立构建一份 HNSW。

### 1.2 现状的三个问题

1. **数据同步绑死 leader**：follower 必须解析出 leader 才能拉数据（`cmd/stratum/main.go:395`）；leader 不可用/切换期间同步停摆。
2. **两个真相源导致状态不一致**：控制层的 `IndexStatus`（Raft）与各节点本地实例/磁盘状态互不隶属——`PENDING` + 实例已就绪、`READY` + 本地无文件，均被允许。
3. **`IndexStatus` 表达力不足**：现在的 `READY` 实为"leader 单点构建成功"的共识化，而查询可负载均衡到任意节点，故对打到 follower 的查询**毫无保证**。

### 1.3 目标

将系统演进为：

```
控制集群（元数据服务）  +  存储集群（自治存储）
```

- **控制集群**：只负责元数据（KB / 版本链 / 活跃版本）与逻辑状态的强一致管理；**不感知任何物理存储细节**（副本、编码、位置、冷热）。
- **存储集群**：自治地负责数据的存储、复制、分层、编码、修复与索引构建。
- **三者关系**：控制层下发**声明式期望**，存储层负责达成并**上报逻辑状态**。

---

## 2. 现状分析（代码事实）

### 2.1 控制层 / 数据层的边界

**控制层**（Raft state machine，`internal/raft/state_machine.go:305-310`）只保存四样：

```go
type snapshotState struct {
    KBs           map[string]types.KnowledgeBaseMeta
    Versions      map[int64]types.VersionMeta
    VersionsByKB  map[string][]int64
    NextVersionID int64
}
```

- `KnowledgeBaseMeta`：name、chunk 窗口/重叠、index type、similarity、quantizer 配置、embed config、`ActiveVersionID`、`Status`（`internal/types/types.go:92-110`）
- `VersionMeta`：VersionID、ParentVersionID、KBID、CreatedAt、`IndexStatus`、`DocIDSetHash`、`Deleting`（`internal/types/types.go:113-141`）

**数据层**：`internal/docstore`、`internal/chunkstore`、`internal/chunkdoc`、`internal/versiondoc`、`internal/bloom`、`vecstore`（HNSW 索引与向量）。

**边界接口**（横跨两层的四类概念）：

| 概念 | 位置 | 性质 |
|---|---|---|
| `IndexStatus` | 控制层（Raft），描述数据层 | 状态投影 |
| `DocIDSetHash` | 控制层（VersionMeta），由数据层计算 | 完整性摘要 |
| `WriteCoordinator` / `IndexManager` | 同时操作两层 | 编排器 |
| `WAL` | 数据层持久化，服务控制层恢复契约 | 恢复契约 |

### 2.2 现有耦合点（分离时必须打破）

| 现有机制 | 位置 | 同节点假设 |
|---|---|---|
| 数据同步由 Raft apply 触发 | `cmd/stratum/main.go:374` `onVersionCreated` | 假设"本节点要持有全量数据" |
| 构建回调直接 propose 状态 | `cmd/stratum/main.go:237-250` | 假设控制层与数据层同进程 |
| `IndexExists` 探本地磁盘 | `internal/index/impl.go:573-584` | 假设控制节点有本地索引文件 |
| `PullVersion(leaderAddr, ...)` | `internal/sync/follower.go:55` | 假设数据源是 leader |
| 启动 `reconcileIndexStatus` 反推状态 | `cmd/stratum/main.go:612` | 假设可用本地磁盘事实修正控制层 |
| WAL 保证跨层一致 | `internal/wal`, `internal/raft/state_machine.go:286` | 假设同磁盘、同提交序 |

### 2.3 现有机制的局限（分离后的核心难点）

现有系统能用 `WAL + Raft 重放` 做到"元数据与数据回到同一一致点"，**完全依赖二者同进程、同磁盘、同提交序**。分离后这三条全部失效，必须显式重建一致性协议（见第 5.3 节）。

另请注意一个**不对称的现状**：

| | 元数据（kvstorage） | 索引（vecstore） |
|---|---|---|
| 原子写 | ✅ `.tmp + rename`（`internal/kvstorage/persister.go:158-162`） | ❌ 直接写最终路径（`vecstore/src/hnsw_index.cpp:382`） |
| 完整性校验 | gob 解码失败即报错 | ❌ 无 checksum、`Load` 不校验 `ids.size() == ntotal` |

即：**控制层已解决"原子保存"，数据层未解决**。分离后这一问题会更突出（见 6.4）。

---

## 3. 目标架构

### 3.1 拓扑

```
                    外部客户端: gRPC SDK · HTTP 网关 / Web 控制台
                                    │
                    ┌───────────────┴───────────────┐
                    │            网关 / 路由          │
                    └───────┬───────────────┬───────┘
                            │ 写             │ 读
              ┌─────────────▼─────┐    ┌────▼──────────────────────┐
              │  控制集群 (K 台)   │    │   存储集群 (S 台, 自治)     │
              │  Raft: KB/版本链   │◄──►│  docstore/chunkstore/     │
              │  /活跃版本/逻辑状态 │契约│  vecstore/索引             │
              │  无数据、无索引     │    │  复制/EC/分层/修复/构建     │
              └───────────────────┘    └───────────────────────────┘
```

- 控制集群规模小、稳定（元数据体量极小）。
- 存储集群内部**有自己的协调机制**（数据放置、副本、EC），但**不承载 Raft 元数据**。
- 读写分离：写经控制层协调，读直连存储层。

### 3.2 职责划分

| 职责 | 控制集群 | 存储集群 |
|---|---|---|
| KB / 版本链 / 活跃版本 | ✅ | ❌ |
| 版本号分配（单调、共识） | ✅ | ❌ |
| 逻辑可用性状态 | ✅（存） | ✅（上报） |
| 数据写入 / 读取 | ❌ | ✅ |
| 数据复制 / 副本放置 | ❌ | ✅ |
| 纠删码 / 冷热分层 | ❌ | ✅ |
| 索引构建 / 加载 / 检索 | ❌ | ✅ |
| 数据修复 / scrub | ❌ | ✅ |

### 3.3 一致性模型

| 维度 | 保证 |
|---|---|
| 元数据 | 强一致（Raft 多数派） |
| 数据 | 最终一致（存储集群内部复制收敛） |
| 跨层 | **单向水位**：控制层状态不得超前于存储层实际数据（由 epoch 协议保证，见 5.3） |

---

## 4. 契约设计

契约只含**逻辑对象**，不出现任何物理概念（副本 / EC / 路径 / 节点）。这是"控制层对物理不可知"的落点。

### 4.1 DataPlane（控制 → 存储）

```go
// 控制层调用存储层的逻辑接口。实现可在同进程或远程。
type DataPlane interface {
    // 持久化一个版本的数据。存储层内部负责复制/编码/放置。
    WriteVersionData(ctx context.Context, kbID string, versionID int64, changes []DocChange) error

    // 确保该版本索引被构建并可服务（幂等）。
    EnsureIndex(ctx context.Context, kbID string, versionID int64) error

    // 删除该版本的物理数据。
    DropVersionData(ctx context.Context, kbID string, versionID int64) error

    // 向量检索。
    Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]SearchResult, error)

    // 声明式持久性策略（存储层执行）：
    //   e.g. Replicas=3, or CodingPolicy={RS, 4, 2}
    SetDurabilityPolicy(ctx context.Context, kbID string, policy DurabilityPolicy) error
}
```

### 4.2 ControlPlane（存储 → 控制）

```go
// 存储层调用控制层的逻辑接口，用于上报。
type ControlPlane interface {
    // 数据写入完成（对应现有 DocIDSetHash 语义）。
    ReportDataDurable(ctx context.Context, kbID string, versionID int64, digest string) error

    // 索引就绪（对应"副本水位"的上报）。
    ReportIndexReady(ctx context.Context, kbID string, versionID int64) error

    // 可用性状态变化。
    ReportAvailability(ctx context.Context, kbID string, versionID int64, state Availability) error

    // 崩溃/重启后的全量对账（见 5.3）。
    ReportEpoch(ctx context.Context, epoch uint64, durableVersions []VersionRef) error
}
```

### 4.3 可用性状态模型

控制层保存的是**抽象可用性**，而非副本数：

```
AVAILABLE    : 数据达到持久性策略，索引可服务      → 控制层置 READY
DEGRADED     : 可服务但未达冗余目标，或需解码冷数据 → 控制层置 READY + 降级标记（供 SLA/告警）
UNAVAILABLE  : 不可服务                            → 控制层置 PENDING / FAILED
```

**关键**：控制层**不知道**降级的原因（副本不足？EC 解码？）。这正是"物理不可知"——控制层只消费抽象状态。

> **与"副本水位"的关系**：早前讨论的"副本水位进控制层"在此被**上移一档抽象**——副本水位降级为存储层内部概念，控制层只看到 `AVAILABLE/DEGRADED/UNAVAILABLE`。这样同时满足"状态一致"与"物理不可知"。

---

## 5. 关键流程

### 5.1 写路径（跨集群 Saga）

`CreateVersion` 从"进程内事务"变为可恢复的 Saga，每一步**幂等 + 可重启恢复**：

```
① 控制集群 ProposeCreateVersion       → 分配 versionID，状态 PENDING
② 控制 → DataPlane.WriteVersionData
③ 存储层写入完成 → ReportDataDurable(versionID, digest)
④ 控制层记录 digest（现 VersionMeta.DocIDSetHash 的等价物）
⑤ 控制 → DataPlane.EnsureIndex(versionID)
⑥ 存储层构建完成且达持久性策略 → ReportIndexReady / ReportAvailability
⑦ 控制层达可用阈值 → READY
```

- 任一步崩溃：重启后从"最后一个已确认步骤"续跑。
- 幂等：重复执行安全（存储写入本身按 key 幂等，`internal/sync/sync.go:22-25`）。

### 5.2 读路径

```
网关 → 存储集群节点
          ├─ 本地有索引 → 直接检索
          └─ 无 → loadFromDisk / 触发构建
```

- **控制层不参与读路径**（避免多一跳与热点）。
- 若需"未就绪快速拒绝"，网关可选缓存控制层状态，但**不强制依赖**。

### 5.3 崩溃恢复协议（核心）

**问题**：控制层说 version X `READY`，但存储集群重启后尚未恢复 X 的数据——控制层凭什么继续相信？

**不变式**：`控制层状态 ≤ 存储层实际数据`（控制层不得超前）。

**机制**：纪元（epoch）+ 块报告（block report）：

```
存储集群每次启动 / 恢复完成:
  1. 生成新 epoch E（单调递增）
  2. 计算本地 durableVersions（完整持久化的版本集合）
  3. ControlPlane.ReportEpoch(E, durableVersions)

控制集群:
  1. 记录 currentEpoch = E
  2. 作废所有 epoch < E 的历史就绪报告
  3. 不在 durableVersions 中的版本 → 降级 PENDING/UNAVAILABLE
     在其中的 → 恢复 AVAILABLE
```

**覆盖场景**：

| 场景 | 处理 |
|---|---|
| 控制层重启 | 加载元数据后，等存储层 `ReportEpoch`，重建真实状态 |
| 存储层重启 | 新 epoch 使旧报告失效 |
| 双向崩溃 | 谁先起都行：控制层等 epoch 才判 READY；存储层不等控制层也能恢复 |
| 存储节点加减 | 存储层内部重新计算 durableVersions 并报告 |

**存储层内部要求**：能回答"哪些版本的数据已完整持久化"。这需要一个存储层内部的**轻量元数据**（本地 manifest + 节点间核对）。

---

## 6. 存储层自治能力（新能力）

> 说明：以下三项**当前均不存在**（代码中无 EC / 冷热分层 / 持久化访问统计）。它们属于存储层内政，与控制层无关。

### 6.1 副本与复制

先实现"复制现有全量副本模型，只是换了归属"。进一步演进为可配置副本数、就近复制、多源引导。

### 6.2 冷热分层

- 热：活跃版本，索引常驻内存（现有 L0）。
- 冷：非活跃、长时间无查询的版本，可下沉、压缩、编码。

**"冷"的判定**（需新建访问统计）：

1. 不被 `ActiveVersionID` 引用（Raft 可直接查）
2. 无子版本引用（否则 rollback 需解码；Raft 可直接查）
3. 最近 N 天无查询（**需新建访问追踪**）
4. 版本序号早于最近 M 个（**需新建**）

### 6.3 纠删码（EC）

- **作用范围**：仅冷数据的**部分**——L2 冷向量 + 冷版本文档。
- **不适用**：HNSW 索引（需随机读、需常驻内存）。
- **解码路径**：冷版本查询 → 存储层透明解码 → 延迟升高。控制层无感，但通过 `DEGRADED` 表达。

### 6.4 EC 与现有设计的冲突（必须先解决）

| 冲突 | 说明 |
|---|---|
| **vs 内容寻址/去重** | chunk ID = SHA-256，多版本共享同一 chunk（MVCC 零拷贝基础，`README.md:97`）。EC 后 chunk 不再独立可寻址，解码单 chunk 需读整条带，破坏 chunk 级随机访问 |
| **vs MVCC 零拷贝** | 共享单元从 chunk 变为编码块，语义需重新定义 |
| **粒度两难** | 太细 → 元数据爆炸 + 条带开销；太粗 → 随机访问差、解码昂贵 |
| **索引完整性** | 索引无 checksum、`Load` 不校验 `ids.size() == ntotal`（`vecstore/src/grpc_service.cpp:282-300`），EC/复制放大该风险 |

**结论**：EC 应作为"自治存储层"建成之后的**可选内部策略**，排在路线最后。

---

## 7. 演进路径

即便目标是物理分离，也**应保留阶段 1**，否则现有物理假设会在拆分时反噬。

```
阶段 1  抽出 DataPlane/ControlPlane 契约，同进程实现
           → 零分布式代价，先把边界划死（此后控制层不再直接探磁盘）
阶段 2  存储集群独立进程；存储层内部重复制（现有 sync 模型搬家）
阶段 3  epoch + block report 恢复协议（跨集群一致性）★ 生产分水岭
阶段 4  存储层内部自治：冷热分层、修复、EC
阶段 5  （按需）存储层分片，突破单节点全量
```

### 阶段 1 接口抽取清单（现有哪些调用点必须改）

| 调用点 | 现状 | 目标 |
|---|---|---|
| `internal/index/impl.go:573` `IndexExists` | 直接探本地磁盘 | 改为查存储层逻辑状态 |
| `internal/index/impl.go:592` `loadFromDisk` | 直接读本地文件 | 移入存储层内部 |
| `cmd/stratum/main.go:374` `onVersionCreated` | 触发本节点全量拉取 | 改为声明式 `EnsureIndex` |
| `internal/sync/follower.go:55` `PullVersion(leaderAddr,…)` | 固定从 leader 拉 | 移入存储层内部复制 |
| `cmd/stratum/main.go:237` 构建回调 | 直接 propose 状态 | 改为 `ReportIndexReady` |
| `cmd/stratum/main.go:612` `reconcileIndexStatus` | 用本地磁盘反推 | 改为 `ReportEpoch` 对账 |

---

## 8. 风险与取舍

### 8.1 风险

| 风险 | 等级 | 说明 |
|---|---|---|
| 跨集群恢复一致性 | **高** | 阶段 3，整条路线的成败关键 |
| 跨集群事务 | 中高 | Saga 每步需幂等 + 补偿 |
| 网络分区（跨集群） | 中高 | 比单集群 Raft 分区更复杂 |
| 双集群运维 | 中 | 部署/监控/升级/扩缩容 ×2 |
| 延迟增加 | 中 | 写路径每步跨网络，RTT 累积 |
| 一致性窗口 | 中 | 控制层状态与存储层实际之间必然有窗口，业务需容忍 |
| EC 与去重/MVCC 冲突 | 中 | 需真正解决，非配置开关 |

### 8.2 取舍

- **状态一致 vs 物理解耦**：本方案选择"控制层知道**逻辑**状态、不知道**物理**实现"来同时满足两者。控制层"完全不可知"是不现实的——至少要知道"是否可用"。
- **控制层轻量 vs 数据层复杂度**：控制面变小、变稳；代价是存储集群长成一个完整的分布式存储系统。
- **强一致 vs 可用性**：元数据强一致，数据最终一致。

---

## 9. 待决事项

| # | 事项 | 建议 |
|---|---|---|
| 1 | 存储集群内部用什么协调？ | **独立**协议（否则又不解耦） |
| 2 | 持久性策略谁定、谁执行？ | 控制层下发**声明式**策略，存储层执行并上报达成情况 |
| 3 | 读路径是否完全绕开控制层？ | **是** |
| 4 | 存储层如何回答"哪些版本 durable"？ | 存储层内部轻量元数据（manifest + 节点核对） |
| 5 | 存储集群是否分片？ | 阶段 5 再定；分片直接挑战"每版本完整索引"模型 |
| 6 | 索引是否参与跨节点复制？ | 建议**否**（索引是派生物，从原始数据本地重算） |

---

## 附录 A：现有代码耦合点索引

| 主题 | 文件:行 |
|---|---|
| Raft 状态机内容 | `internal/raft/state_machine.go:305-310` |
| `VersionMeta` 定义 | `internal/types/types.go:113-141` |
| `KnowledgeBaseMeta` 定义 | `internal/types/types.go:92-110` |
| 非 leader 不能 propose | `internal/kvraft/raft.go:383-385` |
| 无 propose 转发 | `internal/kvraft/errors.go:8` |
| 静态成员（改配置+重启） | `internal/kvraft/transport.go:38-42` |
| 同步流内容（五类 entry） | `internal/sync/follower.go:127-152` |
| `PullVersion` | `internal/sync/follower.go:55` |
| `TriggerBuild`（同步后） | `internal/sync/follower.go:96` |
| 构建回调 propose | `cmd/stratum/main.go:237-250` |
| `onVersionCreated` | `cmd/stratum/main.go:374` |
| `reconcileIndexStatus` | `cmd/stratum/main.go:612` |
| `IndexExists` | `internal/index/impl.go:573` |
| `loadFromDisk` | `internal/index/impl.go:592` |
| 元数据原子写 | `internal/kvstorage/persister.go:148-165` |
| 索引非原子 `Save` | `vecstore/src/hnsw_index.cpp:375-403` |
| `ExistsIndex` 仅查存在性 | `vecstore/src/grpc_service.cpp:282-300` |
| 索引 `Load`（不校验 ids↔ntotal） | `vecstore/src/hnsw_index.cpp:405-455` |

## 附录 B：术语表

| 术语 | 含义 |
|---|---|
| 控制集群 | 运行 Raft、管理元数据与逻辑状态的集群 |
| 存储集群 | 自治管理数据存储、复制、编码、索引的集群 |
| DataPlane | 控制层调用存储层的逻辑接口 |
| ControlPlane | 存储层调用控制层的上报接口 |
| 逻辑可用性 | `AVAILABLE/DEGRADED/UNAVAILABLE`，控制层可见的抽象状态 |
| 纪元（epoch） | 存储集群每次恢复生成的单调递增标识，用于作废旧报告 |
| 块报告（block report） | 存储层向控制层上报"哪些版本已完整持久化" |
| Saga | 长事务拆成多个可补偿步骤的模式 |
| EC（纠删码） | Erasure Coding，纠删码，用于冷数据低成本持久化 |
