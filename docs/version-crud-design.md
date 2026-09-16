# Stratum 版本增删改查设计

> 本文档总结 **Stratum 当前实现**（分支 `main`）中「知识库版本」的创建、删除、变更与读取（CRUD）设计：入口接口、内部编排、数据模型、一致性/崩溃恢复语义与约束边界。所有描述均以仓库内代码为准，文末给出代码索引。

---

## 1. 术语与定位

Stratum 是**版本化**向量检索引擎。版本（version）是数据组织与查询的最小一致单位：

- **版本链**：每个版本有一个 `ParentVersionID`；从 `0`（基底）出发形成一棵**树**，允许同一父版本存在多个子版本（分叉），用于 A/B 比对。
- **活跃版本**：知识库元数据 `ActiveVersionID` 指向当前默认查询的版本；查询可显式指定任意版本。
- **文档不可变、版本不可变**：不存在原地修改。任何文档变更都通过 `CreateVersion` 产出**新版本**。
- **索引 PENDING**：新版本元数据先落 Raft 并标 `PENDING`，存储层写完、索引异步构建完成后才转 `READY`，才可查询。

| 概念 | 类型 | 说明 |
|---|---|---|
| 版本元数据 | `types.VersionMeta` | `VersionID / ParentVersionID / KBID / CreatedAt / IndexStatus / DocIDSetHash / Deleting` |
| 索引状态 | `types.IndexStatus` | `PENDING(0) → READY(1)` / `FAILED(2)` |
| 删除模式 | `types.VersionDeleteMode` | `SUBTREE(0)` / `SINGLE(1)` / `ANCESTORS(2)` |
| 文档变更 | `types.DocChange` | `Op(ADD/DELETE/UPDATE) + DocID + Content` |

映射到 CRUD：

| 操作 | 对应能力 |
|---|---|
| **增** | `CreateVersion`（含 `CreateKnowledgeBase` 产出的初始版本）|
| **删** | `DeleteVersion`（三种模式）+ 整体 `DeleteKnowledgeBase` |
| **改** | 版本本身不可改；由 `RollbackVersion` 切换活跃版本、元数据更新（状态 / 摘要）、`RebuildIndex`/`WarmupVersion` 承载 |
| **查** | `ListVersions` / `GetKnowledgeBase` / `ListKnowledgeBases` / `Query`（按版本检索）|

---

## 2. 分层与调用链

```
HTTP/JSON (gateway, cmd/stratum-gateway)
        │  REST
        ▼
gateway ⇄ router(leader 发现 / 写转发 / 读均衡) ⇄ stratum 节点
        │  gRPC
        ▼
service/  ── 薄层：入参校验 + proto↔内部类型转换 + 错误映射，无业务逻辑
        │
        ├── WriteCoordinator          → CreateVersion 编排
        ├── DeleteVersionCoordinator  → DeleteVersion 异步清理编排
        ├── DeleteCoordinator         → DeleteKnowledgeBase 编排
        │
        ▼
raft.RaftNode (internal/raft)  ── Propose-and-wait，状态机 apply 阶段做确定性校验
        │
        ├── internal/kvraft  共识（选主 / 日志复制 / 快照）
        ├── state_machine.go KB + 版本元数据（强一致，节点间复制）
        │
        ▼ 存储层（本地 PebbleDB / vecstore）
docstore(MVCC) · versiondoc(版本文档集) · chunkdoc · bloom · index(每版本 HNSW)
```

- **元数据**（KB / 版本树 / 状态 / 活跃版本）经 **Raft** 强一致、节点间复制。
- **数据**（文档、chunk、向量、索引文件）由**写路径**本地落盘，follower 通过 `DataSyncService` 拉取（`OnVersionCreated` 钩子触发）。
- `service` 层不做业务校验；一切约束（父版本合法性、活跃/PENDING 不可删等）都在 **Raft apply 阶段**确定性执行，保证 leader/follower 结果一致。

---

## 3. 版本数据模型与键布局

### 3.1 `VersionMeta`（Raft 状态机，`internal/types/types.go`）

```go
type VersionMeta struct {
    VersionID       int64
    ParentVersionID int64        // 0 = 基底
    KBID            string
    CreatedAt       int64        // leader 本地时钟，apply 时刻
    IndexStatus     IndexStatus  // PENDING / READY / FAILED
    DocIDSetHash    string       // 全量 docID 集摘要，兼作"存储写完成"标记
    Deleting        bool         // DeleteVersion 异步清理进行中
}
```

状态机内部维护三张表 + 单调计数器：`kbs`、`versions`、`versionsByKB`、`nextVersionID`。

### 3.2 存储层键布局

| 模块 | 键 | 值 | 读语义 |
|---|---|---|---|
| `docstore` | `kbID + docID + versionID` | tag(`0x00` tombstone / `0x01` 内容) + 文本 | `ReadAt(kbID, docID, maxVersionID)`：取 `versionID ≤ maxVersionID` 的最大条目（MVCC 回退）|
| `versiondoc` | `kbID + versionID + docID` | 空 | `ListDocIDs`：前缀扫描出该版本**完整**文档集 |
| `chunkdoc` | `kbID + chunkID` | docID 集 | chunk ↔ 文档双向映射 |
| `bloom` | 每 KB chunk 存在性 / 每版本文档布隆过滤器 | — | 加速存在性判断，可重建 |

---

## 4. 增：创建版本

### 4.1 入口

`service.CreateVersion`（`service/knowledgebase.go`）把 proto 的 `DocChange` 列表转成 `types.DocChange`，调用 `WriteCoordinator.Execute(kbID, parentVersionID, changes)`，返回新版本 ID。

### 4.2 写路径（`internal/coordinator/write_impl.go`，7 步）

整个事务（BEGIN→COMMIT）在同一把 `txnMu` 下串行执行（与 chunk GC 回收阶段共用，见 §8）。

| 步 | 动作 | 说明 |
|---|---|---|
| 1 | `WAL.WriteBegin(kbID, parentVersionID, changes)` | 持久化重放输入，崩溃后可回放 |
| 2 | `RaftNode.ProposeCreateVersion` | apply 阶段先 `WAL.WriteVersionID` 再分配版本（§4.3）|
| 3 | 逐文档：`Split → Embed → Bloom.Test → ChunkStore.Exists → ChunkStore.Write + Bloom.Add → ChunkDocMapper.Write → DocStore.Write` | `DELETE` 只写 tombstone；`ADD/UPDATE` 生成 chunk |
| 4 | `VersionDocList.Write` | 计算新版本**完整**文档集：父版本集合 ± 本次变更 |
| 5 | `VersionBloomStore.BuildAndPersist` | 写每版本文档布隆过滤器（非致命，读路径可懒重建）|
| 6 | `WAL.WriteCommit(versionID)` | 事务提交 |
| 6.5 | `ProposeUpdateVersionSummary(versionID, hash)` | 提交 docID 集摘要，供 follower 校验拉取完整性（非致命）|
| 7 | `IndexManager.TriggerBuild` | **异步**构建索引，`Execute` 不等待 |

要点：

- **只写变更文档**：未变更文档在新版本零拷贝（MVCC），因此第 4 步的 `VersionDocList` 才需要重算全量集合。
- **内容寻址去重**：`ChunkID = SHA-256(text + modelID)`；布隆过滤器命中时才回查 `ChunkStore.Exists` 确认（防误报）。
- **重试**：非永久性存储错误按 `write_coordinator.max_retries` / `retry_base_interval_ms` 指数退避重试；耗尽后返回错误，调用方重试即可（崩溃由 WAL 兜底）。
- `TriggerBuild` / `UpdateVersionSummary` 失败**不致命**：版本已存在、数据已持久，可后续重建/由启动 reconcile 补救。

### 4.3 版本 ID 分配与关键时序（`internal/raft/state_machine.go`）

在 Raft **apply 阶段**（leader 与所有 follower 执行同一路径）：

1. 校验父版本：存在、属于同一 KB、非 `PENDING`、非 `Deleting`，否则 `ErrInvalidParentVersion`。
2. `versionID := sm.nextVersionID; sm.nextVersionID++`（**复制状态机**内单调递增，保证全局唯一且各节点一致）。
3. 先 `WAL.WriteVersionID(versionID)`（幂等），再写入状态机，初始 `IndexStatus = PENDING`。

> **WAL-before-state-machine** 的顺序保证：只要 WAL 有 `VERSION_ID` 记录，状态机中必有对应版本，杜绝孤儿版本。这里的 WAL 写失败**不阻断**状态机更新（否则会造成本节点与集群状态分歧），仅告警。

**分叉**：同一父版本可有多个子版本（`versionsByKB` 是列表，`collectVersionSubtree` 按 `ParentVersionID` 遍历成树）。

### 4.4 索引异步构建与状态迁移

```
CreateVersion 返回 v(PENDING)
        │  IndexManager.TriggerBuild（异步：ListDocIDs → chunk 向量 → VectorIndex.Build）
        ▼
build 完成 → BuildCompleteCallback(kbID, versionID, status)
        │  cmd/stratum/main.go 注册的回调：成功后先执行磁盘保留策略，再
        ▼
ProposeUpdateVersionStatus(versionID, READY|FAILED)
```

- **PENDING**：元数据已分配、存储写进行中/索引构建中，**不可查询**、不可作父版本。
- **READY**：索引已构建，可查询、可回滚、可作父版本。
- **FAILED**：构建失败，可经 `RebuildIndex` 重试；`RollbackVersion` 会拒绝切到 FAILED。
- **启动 reconcile**（`cmd/stratum/main.go::reconcileIndexStatus`）从磁盘事实推导状态，**不依赖构建回调确实送达**：
  - PENDING + 磁盘有索引 → 提议 READY；
  - PENDING + 无索引 → 触发构建（幂等）；
  - READY + 无索引 → 重建（除非索引被磁盘保留策略有意回收，则按需重建）。

### 4.5 初始版本（`CreateKnowledgeBase`）

`service.CreateKnowledgeBase` 不存在独立的"创建版本"接口，它复用版本流程：

1. `ProposeCreateKB` 写 KB 元数据；
2. `ProposeCreateVersion(kbID, 0)` 建**初始版本**（父 `0`，无文档）；
3. `ProposeUpdateVersionStatus(初始版本, READY)`（无 chunk，直接可查）；
4. `ProposeRollback(kbID, 初始版本)` 把它设为**活跃版本**（`RaftNode` 无独立 "set active" RPC，故复用 Rollback）。

### 4.6 follower 数据同步

- leader 在写路径本地落盘；`cmdCreateVersion` 在**非提案节点**（follower）apply 时触发 `OnVersionCreated` → 异步 `Follower.PullVersion`，从 leader 拉取存储层数据。
- 用 `DocIDSetHash` 校验拉取完整性（§4.2 步 6.5）；摘要未提交时退化为 best-effort 拉取。
- `versionID <= 1`（初始空版本）只拉一次空流。

### 4.7 崩溃恢复重放

启动时 `runCrashRecovery`（`cmd/stratum/main.go`）读 WAL 待处理记录：

| 记录类型 | 处理 |
|---|---|
| `DeleteMark` | 恢复被中断的 `DeleteKnowledgeBase` |
| `VersionDelete` | 恢复被中断的 `DeleteVersion` 清理 |
| `VersionWrite` | 用 BEGIN 记录里的 `(parentVersionID, changes)` **重放存储写（步 3–6）**，不重新提案版本 |

- 所有存储写幂等，从零重放安全。
- 无法重放的记录（follower apply 的、或旧格式无 BEGIN 载荷）不致命，仅计数告警，留在 WAL 待下次重启处理。

---

## 5. 删：删除版本

### 5.1 两阶段：同步标记 + 异步清理

`service.DeleteVersion`：

1. **同步**：`ProposeMarkVersionDeleting(kbID, versionID, mode)` → 在 Raft apply 阶段完成校验 + 树结构改写 + 集合标记 `Deleting`，返回**实际标记删除的版本 ID 清单**（`DeletedVersionIds`）。
2. **异步**：`go DeleteVersionCoordinator.Execute(kbID)` 清理。

**约束**（全部在 apply 阶段确定性执行，service 不做额外校验）：

- 目标版本存在且属于该 KB；
- 选中集合中**任何**版本都不得是**活跃版本**（`ErrVersionIsActive`），也不得是 `PENDING`（`ErrVersionPending`）；
- **幸存者保护**：不在删除集合、但其父在集合中的版本若是 `PENDING`，整次删除被拒（`validateSurvivorsNotPending`）——否则异步清理会删掉它完成存储写所依赖的父版本文档集。
- 幂等：对已 `Deleting` 的集合重复提案通过校验并重标记（no-op），是崩溃后清理可安全续跑的基础。

### 5.2 三种删除模式（`internal/types/types.go` / `state_machine.go`）

以版本链 `A → B → C → D`（`A` 为基底）为例：

```
SUBTREE（默认）: 删目标 + 全部后代
    A
    │        删 B ⇒ 标记删除 {B, C, D}
    B
    │
    C
    │
    D

SINGLE: 只删目标自身, 其直接子改挂到目标的父
    A
    │        删 B ⇒ 标记删除 {B}
    B        C 改挂到 A（链表拼接, 下方分支保留）
    │
    C
    │
    D

ANCESTORS: 删目标全部祖先, 目标成为新基底
    A
    │        删 D ⇒ 标记删除 {A, B, C}
    B        （祖先上挂的兄弟分支一并删除）
    │        D 的 ParentVersionID 清 0, 成为新的根
    C
    │
    D
```

| 模式 | 删除集合 | 结构改写 |
|---|---|---|
| `SUBTREE` | 目标版本 + 全部后代（子树闭合向下的集合）| 无 |
| `SINGLE` | **仅目标版本自身** | 其**直接子版本**改挂到目标的父版本（父不存在/父本身被删则成为根）——`spliceParent` + `reparentChildren`，即链表拼接，保留下方分支 |
| `ANCESTORS` | 目标版本的**全部祖先** + 挂在祖先上的**兄弟分支** | 目标版本 `ParentVersionID` 清 0，成为版本链新的**基底**；已是基底则为 no-op，返回空清单 |

- `SUBTREE` 集合向下闭合，故幸存者保护天然成立（no-op）。
- 结构改写**只在全部校验通过后**执行，避免"改一半被拒"的半删除状态。
- `ANCESTORS` 的兄弟分支会被一并删除，响应里逐条列出。

### 5.3 异步清理（`internal/coordinator/delete_version_impl.go`）

`Execute(kbID)` 先 `ListVersions` 重新发现该 KB **所有** `Deleting` 版本（含上次崩溃遗留），逐个 `deleteOne`：

| 步 | 动作 | 说明 |
|---|---|---|
| 1 | `WAL.WriteVersionDeleteMark` | 幂等，标记清理开始 |
| 2 | `IndexManager.Discard` | 逐出内存索引 + 重置 vecstore 侧索引 |
| 3 | `VersionDocList.DeleteByVersion` | 删该版本文档集 |
| 4 | `DocStore.DeleteByVersionExceptVisibleFrom(kbID, versionID, anchor)` | **选择性**回收 MVCC 记录（见 §5.4）|
| 5 | `ProposeRemoveVersionMeta` | 从状态机删元数据（幂等）|
| 6 | `VersionBloomStore.DeleteByVersion` | 删每版本文档布隆过滤器（可选依赖）|
| 7 | `WAL.WriteVersionDeleteComplete` | 幂等 |

- 每步指数退避重试；耗尽后返回错误，**版本保持 `Deleting`**，经 `GetSystemStatus` 暴露给运维。
- 全流程幂等，可从任意步崩溃后从零重跑。

### 5.4 MVCC 回收的 anchor 语义（`internal/docstore/pebble.go`）

版本只写**变更文档**，`ReadAt` 会向更早版本回退取值。因此删除一个版本时，**不能无条件删掉它的 docstore 记录**——否则仍存活、且从未重写该文档的后继版本会读到旧值或读不到。

`anchor = 大于 versionID 的最小幸存版本`（`visibleAnchor`，剔除 `Deleting`）：

- 若某条目正是 `ReadAt` 在 `anchor` 处会返回的那条（即它是幸存后继版本的读源），**保留**；其余回收。
- `anchor == 0`（无幸存后继）→ 等价全量删除。
- 这一步让"删除最新若干版本"这类常见场景仍能完整回收。

### 5.5 与整库删除（`DeleteKnowledgeBase`）的关系

- `DeleteKnowledgeBase`：`ProposeMarkKBDeleting` 后异步 `DeleteCoordinator.Execute` 清理（索引文件、chunk、映射、docstore、versiondoc、bloom、元数据），失败可标 `KBStatusDeleteFailed`。
- 版本删除是**库内**操作，不动 KB 元数据；两者共用 `Deleting` 可见性语义与幂等清理风格。

---

## 6. 改：版本不可变与元数据更新

**设计取舍：版本是不可变的（immutable）。** 没有 `UpdateVersion`。所谓"改"由三条路径承载：

### 6.1 变更数据 = 产出新版本

对已有数据进行增删改，一律走 `CreateVersion`（§4），旧版本原样保留 → 天然可回滚、可审计、可并行比对。

### 6.2 `RollbackVersion`：切换活跃版本

`service.RollbackVersion` 先做**读层校验**（`ListVersions`）：目标必须存在、且不能是 `PENDING` / `FAILED`；再 `ProposeRollback`。apply 阶段再校验 KB 存在、版本归属、且非 `Deleting`（`ErrVersionDeleting`）。

- 仅切换 `ActiveVersionID` 指针，**不搬数据、不停服**。
- 影响后续**未显式指定版本**的查询（§7.3）与父版本选择。

### 6.3 元数据更新（内部）

| RPC | 作用 |
|---|---|
| `ProposeUpdateVersionStatus` | 更新 `IndexStatus`（构建完成 READY / 失败 FAILED / 重试置 PENDING）|
| `ProposeUpdateVersionSummary` | 更新 `DocIDSetHash`（follower 拉取校验用）|

### 6.4 `RebuildIndex` / `WarmupVersion`（AdminService）

- `RebuildIndex`：先把版本置 `PENDING`，再 `TriggerBuild` → 供 FAILED 版本重试。
- `WarmupVersion`：同样置 `PENDING` + `TriggerBuild`，把索引重新加载进内存（**不切换活跃版本**）；完成经 `BuildCompleteCallback` 翻回 READY/FAILED。
- 二者产物都是状态机元数据更新，不改版本内容或树结构。

---

## 7. 查：版本读取

### 7.1 `ListVersions`

`service.ListVersions` → `RaftNode.ListVersions(kbID)`（按 `versionsByKB` 顺序返回全部 `VersionMeta`），转换为 `VersionInfo`（含 `Deleting` 标记，供控制台显示"删除中"）。

> 状态机读取走 `RLock`（apply 由单分发循环串行拥有写锁），可与其他读并发。

### 7.2 知识库维度

- `GetKnowledgeBase` → `RaftNode.GetKB(kbID)`，返回含 `ActiveVersionID` / `Status` / 量化配置的元数据。
- `ListKnowledgeBases` → 全量 KB，按 `KBID` 字典序排序（稳定可预期）。

### 7.3 `Query` 如何定位版本（`service/query.go`）

```
请求带 version_id  → 用它
请求不带 version_id → GetKB().ActiveVersionID（活跃版本）
        │
        ▼
ListVersions 查找该版本 → 不存在 ErrVersionNotFound
        │
        ├─ PENDING → FailedPrecondition
        ├─ FAILED  → FailedPrecondition
        ▼
IndexManager.Search（LRU 加载/驻留该版本索引，引用计数保护）
        ▼
chunk 结果 → chunkdoc 反查 docID → 版本布隆过滤器 + versiondoc 确认
        ▼
DocStore.ReadAt(kbID, docID, versionID) 读内容 → 聚合 + top-k
```

- 查询结果追溯明确版本，响应回传 `version_id`。
- **空版本**（无文档、无索引条目）被当作空结果集，而非报错。
- 评分/阈值口径全精度与量化两段式一致。

---

## 8. 一致性与并发

- **强一致元数据**：KB / 版本树 / 状态 / 活跃版本均经 Raft；`Propose-and-wait` 阻塞至已 apply（含 term 校验，被新 leader 覆盖时返回 `errSuperseded`）。
- **apply 阶段确定性**：删除目标集合、父版本校验、版本 ID 分配都在 apply 内完成，leader/follower 结果一致。
- **快照不阻塞**：`deepCopy`（RLock 下拷元数据）快，gob 编码与落盘异步；快照安装后对每个被覆盖版本显式补触发数据同步。
- **写事务串行 + GC 互斥**：`CreateVersion` 全程持 `txnMu`；与孤儿 chunk GC 的回收阶段共用同一把锁，闭合"过期快照误删"竞态。
- **WAL 两阶段 + 幂等**：`BEGIN → VERSION_ID → COMMIT`；所有存储写与删除步骤幂等，崩溃任意点可安全重放/续跑。
- **启动 reconcile**：状态从磁盘事实推导，不信任可能丢失的构建回调。

**已知限制**：`GetKB` / `ListVersions` 直接读本节点内存状态机，无线性一致读协议；滞后 follower 上可能短暂读到旧值（对连通性探针/控制台可接受，代码已注明）。

---

## 9. 错误码映射（`internal/errors`）

| 内部错误 | gRPC code | 触发场景 |
|---|---|---|
| `ErrVersionNotFound` | `NotFound` | 版本不存在 / 文档在某版本不可见 |
| `ErrVersionPending` | `FailedPrecondition` | 版本仍在存储写/索引构建 |
| `ErrVersionFailed` | `FailedPrecondition` | 索引构建失败 |
| `ErrVersionDeleting` | `FailedPrecondition` | 版本正在删除 |
| `ErrVersionIsActive` | `FailedPrecondition` | 试图删除活跃版本 |
| `ErrInvalidParentVersion` | `InvalidArgument` | 父版本非法（不存在/跨库/PENDING/Deleting）|
| `ErrInvalidArgument` | `InvalidArgument` | 未知删除模式等 |
| `ErrKnowledgeBaseNotFound` | `NotFound` | KB 不存在 |
| `ErrKnowledgeBaseDeleted` | `FailedPrecondition` | KB 已删除 |
| `ErrIndexNotReady` | `FailedPrecondition` | 索引未就绪 |
| `ErrIndexLoadTimeout` | `DeadlineExceeded` | 索引加载等待超时 |

`ToGRPCStatus` 用 `errors.Is` 走错误链；未登记错误一律 `Internal`。转换只在每个 gRPC 方法最外层做一次。

---

## 10. 接口一览

### 10.1 gRPC（`api/proto/`）

**KnowledgeBaseService**（`knowledgebase.proto`）

| RPC | 版本 CRUD 角色 |
|---|---|
| `CreateKnowledgeBase` | 建 KB + **初始版本**（设为活跃）|
| `DeleteKnowledgeBase` | 整库标记删除，异步清理 |
| `CreateVersion` | **增**：应用 `ADD/DELETE/UPDATE` 产出新版本（PENDING）|
| `ListVersions` | **查**：版本链 |
| `RollbackVersion` | **改**：切换活跃版本 |
| `DeleteVersion` | **删**：`mode` = `SUBTREE`（默认）/`SINGLE`/`ANCESTORS`，返回 `deleted_version_ids` |
| `ListKnowledgeBases` / `GetKnowledgeBase` | 查询知识库与活跃版本 |

**QueryService**：`Query`（可选 `version_id`，缺省用活跃版本）。
**AdminService**：`HealthCheck` / `GetSystemStatus`（卡住版本、删除失败 KB、WAL 告警）/ `GetClusterStatus` / `RebuildIndex` / `WarmupVersion`。

> proto 中 **Reserved（未实现）**：`GetVersion`、`DiffVersions`、`TagVersion`、`BatchQuery`、`HybridQuery`——仅文档注明，无桩代码。

### 10.2 REST（`cmd/stratum-gateway/main.go`）

| 方法 & 路径 | 对应 |
|---|---|
| `POST /api/knowledge-bases` | `CreateKnowledgeBase` |
| `GET /api/knowledge-bases` | `ListKnowledgeBases` |
| `GET /api/knowledge-bases/{id}` | `GetKnowledgeBase` |
| `POST /api/knowledge-bases/delete` | `DeleteKnowledgeBase` |
| `GET /api/knowledge-bases/{id}/versions` | `ListVersions` |
| `POST /api/knowledge-bases/{id}/versions` | `CreateVersion` |
| `POST /api/knowledge-bases/{id}/rollback` | `RollbackVersion` |
| `POST /api/knowledge-bases/{id}/delete-version` | `DeleteVersion`（body 含 `version_id` + `mode`）|
| `POST /api/knowledge-bases/{id}/rebuild` | `RebuildIndex` |
| `POST /api/knowledge-bases/{id}/warmup` | `WarmupVersion` |
| `POST /api/query` | `Query` |
| `GET /api/health` / `GET /api/system-status` | 健康 / 系统状态 |

Web 控制台（`web/app.js`）按 `parent_version_id` 构建版本树、支持分叉展示，并提供回滚 / 删除（三种模式弹窗）/ 设为基底 / 预热 / 重建 的入口。

---

## 11. 代码索引

| 关注点 | 位置 |
|---|---|
| 版本 CRUD 入口（gRPC 薄层）| `service/knowledgebase.go` |
| 查询按版本检索 | `service/query.go` |
| 重建 / 预热 / 系统状态 | `service/admin.go` |
| 写路径编排（7 步）| `internal/coordinator/write_impl.go` |
| 删除版本异步清理 | `internal/coordinator/delete_version_impl.go` |
| 整库删除编排 | `internal/coordinator/delete_impl.go` |
| 版本元数据状态机（增删改查核心校验）| `internal/raft/state_machine.go` |
| RaftNode 接口语义 | `internal/raft/raft.go` |
| Propose 实现（Propose-and-wait）| `internal/raft/impl.go` |
| 命令编码 | `internal/raft/command.go` |
| 数据类型 / 删除模式 | `internal/types/types.go` |
| MVCC 文档存储（含 anchor 回收）| `internal/docstore/pebble.go` |
| 版本文档集 | `internal/versiondoc/pebble.go` |
| 索引管理接口 | `internal/index/index.go` |
| 错误码映射 | `internal/errors/` |
| 启动 reconcile / 崩溃恢复 / 数据同步接线 | `cmd/stratum/main.go` |
| REST 路由 | `cmd/stratum-gateway/main.go` |
| 控制台版本树 UI | `web/app.js` |
