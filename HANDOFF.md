# Stratum 交接文档（控制面/数据面分离 · 实施中）

> 本文档为跨会话交接用。设计权威是 `Stratum_设计文档v13.md`（1476 行），本文只记录**进度、约束、下一步**这三件在文档里不容易一眼看到的事。

---

## 1. 目标与路线

**目标**：把 Stratum 演进为 **控制集群 + 自治存储集群**（v1 `control-data-separation-design.md` §1.3/§3.1 的原意），不是"Go 控制 / C++ 数据"的代码分层。

**路线**：`Stratum_设计文档v13.md` 的 §11 阶段 ⓪–⑦，按依赖顺序推进。四份来源文档（`control-data-separation-design.md`、`storage-coordination-and-service-station-design.md`、`unresolved-issues-resolution-final.md`、`version-linearization-decision.md`）**保留原样**，不修改原文；一切回填写进 v13。

**分层的关键分界**（容易搞错，先记）：

```
控制层（Raft 元数据）              Go，跨节点
──────────────────────────────────────────────
存储层（每节点一份）
   Go 侧：LocalDataPlane / sync / docstore / chunkdoc / versiondoc
   C++ 侧：vecstore（chunkstore + HNSW 索引）
   ↑ 同机进程，走 VectorIndexService

节点间通道：DataSyncService（Go ↔ Go）
```

两处常被混淆：**vecstore 就是存储层的一部分**（只是另一门语言、另一个进程）；**跨节点**那条边界在 `DataSyncService`，**同机**那条在 `VectorIndexService`。让 vecstore 知道"谁是副本"等于把控制层的成员视图漏进 C++，那是要避免的。

---

## 2. 进度总表

| 节 | 状态 |
|---|---|
| 阶段 ⓪ 决策补齐 | ✅ |
| 阶段 ① 契约抽取（`DataPlane` / `ControlPlane`） | ✅ |
| 阶段 ② 版本链线性化 | ✅ |
| 阶段 ③ 索引文件原子写 + checksum | ✅ |
| 阶段 ④a 写事务拆分 + 幂等键 | ✅ |
| 阶段 ④b push / fan-out / quorum / 游标 / 无数据检测 | ✅ |
| **§7 整节**（1–12：quorum、fan-out、崩溃接管、游标、追链、游标交换、有界窗口、恢复安全版本、`ReportEpoch` payload、写事务） | ✅ **整节闭合** |
| §8.3 前置（索引原子写 + checksum） | ✅ |
| **§8.4 索引分发**（"建一次、分发 N 份"） | ✅ 含端到端验证 |
| **§8.6(b) 惰性构建** | ✅ |
| **§8.6(a) 免图分层（含冷热转换策略）** | ✅ 能力 + vecstore 形态切换 + 访问跟踪 + 后台冷阈评估；形态变化经既有 §8.4 回调链重新分发；**集群用例已补** |
| §8.6(c) 增量复用 | ✅ **已实现**：`LoadForAppend` + 复用判定/回退 + 删除场景（免图 `RemoveChunks` 真删、带图墓碑阈值 → 延迟全量重建） |
| §7.5 追链（`changes` 持久化 + 增量追赶） | ✅ **已实现（含物理回收）**：WAL 读取面（`ChangesFor` / `ChangesInRange`，后者返回带**权威 parentID** 的 `VersionDelta`）+ `PullVersionChanges` 流式 RPC（缺口可见、**绝不返回空区间**）+ 客户端 `sync.VersionChangesPuller` + `backfillTo` 先试增量、缺口或失败退全量；重放走 `ApplyBackfillChanges`（只做本地事务，跳过 fan-out / digest 上报 / 索引调度 / 失败上报）。顺带修掉 `FileWAL.beginDataByVersion` 运行时未维护的真缺陷。**WAL 物理回收已落地**：`FileWAL.Compact`（整文件重写 + 原子 rename + 逐字节复制；只丢"≤ 水位且已提交"的 BEGIN/VERSION_ID，未完成流程与尾部 in-flight BEGIN 一律保留）+ `LocalDataPlane.ReclaimChanges` + `plane.WALReclaimer` 后台循环；判据 `ReclaimableChangesThrough` 提到 `ControlPlane` 接口（存储层自己取水位，无需新 RPC），并用 `ReportDataVersionsResponse.reclaimable` **水位回传**覆盖"写数据但不是 leader"的节点 |
| §6.4 已删除中间版本 → 全量状态传输兜底 | ✅ **已实现**：`transferFullState` 在**元数据确认某版本已删除**时，拉 **`versionID`**（本次要 apply 的版本）的整份状态并把游标跳到它——取 `versionID` 而非 `versionID-1`，因为后者可能**正是被删的那个**，会让兜底恰好在该用时失败。判定在**拉取之前**（已删版本根本不会被拉，也让"确实走了快照"可被外部观察）。触发条件刻意收窄——只有"确认删除"才跳，元数据读失败与传输失败**仍中止 apply**。配套修掉"拉取成功但无记录 → 静默推进游标"的既有隐患（`VersionExistenceChecker` + `LocalControlPlane.ExistingVersions`，只读本地元数据副本、不发 Raft 往返、且一次取全表） |
| §8.5 动态指派 | 🔄 **模型已按 §7.13.2 收敛为"控制层指派"**：`CreateVersion` 回到 leader-bound，**受理者必须是 leader（直连 follower 一律拒绝 `NotLeader`）**，由 apply 时现查 `IsLeader()` 的节点按副本拓扑挑候选、派它写（`ExecuteVersionWrite`）。第一步的数据源注册表（`plane.DataSourceRegistry` + 确认信号带 `source_addr` + `ResolverWithRegistry`）**保留**，作"数据在哪"的推送式提示。集成用例 `TestRealStack_ThreeNodeCluster_NonLeaderRefusesTheWrite` 通过；全量 23 包全绿——见 §3.3 |
| §8.7 写入攒批 | ⛔ **已撤销**（目标场景是批量一次写）。**连带**：`storage-coordination...` §3.6 与 `unresolved-issues-resolution-final` §4.1/§4.2 的攒批设计随之失效（v13 §12.1 已注记） |
| §7.13 协调者选择 / 数据位置查询 / 局部健康视图 | ✅ **已实现**（来源 `coordinator-selection-and-node-liveness-design.md`，已并入 v13 §7.13）：§7.13.2 协调者由控制层指派（apply 时现查 `IsLeader()` + `ExecuteVersionWrite`）＋ §7.13.4 周期上报聚合（`plane.DataVersionRegistry` + `ReportDataVersions` RPC + `sync.DataVersionReporter` + `LeaderGate` 接管清空；查询入口 `LocalControlPlane.DataVersionHolders`）＋ 可回收水位（`ReclaimableChangesThrough`，只算水位不真删）。**⚠️ 名字陷阱**：本节说的 `ReportEpoch` 聚合与代码里**同名**的一次性重启对账 `ControlPlane.ReportEpoch` 不是一回事（v13 §7.13.4 已加注记）；与 §8.5 的模型冲突已由 §7.13.2 收敛（v13 §12.3 #8 已更正） |
| §9 读路径服务站 | ❌ 未做（用户指示：**排最后**） |
| §10.1 `FAILED_PERMANENT` 判定与暴露 | ✅ |
| §10.6 判死后清理 + 迟到上报校验 | ✅ |
| §10 其余（线头） | ✅ 已收干净（详见 §10 与变更记录） |

**当前工作区**：119 个文件有改动（含未跟踪的新文件，如 `HANDOFF.md`、`integration/*_test.go`、`internal/plane/*`）；`go build ./...` / `go vet ./...` 干净；`go test ./internal/... ./cmd/...` 全绿，`go test ./integration/` 单包 **28/28** 全绿（含 §5 清单里的 `FaultTolerance`、`SnapshotPipeline`）。**注意**：`go test ./...` 并发跑多个包时偶发停滞（分批跑则都在超时内干净完成），叠加 §5 清单内的偶发失败——两者都**不是**代码问题，用 `-timeout` 分批跑即可定位；vecstore `ctest` **50/50**（`build/` 与 `vecstore/build` 都跑过；`ConcurrentSearchResetLoadSmoke` 偶发、重跑即过）。

---

## 3. 下一步（按优先级）

### 3.1 §8.6(a) 冷热转换策略 —— 已完成（记录，勿重做）

1. **访问跟踪** —— `lastSearch map[indexKey]time.Time`（**不是** `loadedIndex.lastAccess`：后者随换出丢失）。`Search` 入口即记；构建成功、以及 `InstallIndex`（副本装载 §8.4 分发来的产物）都为无记录者种下基准（于是"从未被查询的版本"和"只接收过产物的副本"也会老化）。`Evict` 保留、`Discard` / `DeleteFilesByKB` 清理。读口 `LastAccess`。
2. **后台定时评估 + 冷阈配置** —— `IndexManagerConfig.ColdThreshold` / `ColdSweepInterval`（`<= 0` 关闭＝历史行为），yaml `index_manager.cold_threshold_ms` / `cold_sweep_interval_ms`（`configs/config1.yaml` 里默认 0）。`StartColdPolicy()` 起评估器、`Close()` 停；只读本节点访问表，不参与共识。`builtGraphFree` 记账防止每轮重复重建；`loading` 中的版本跳过（在飞的构建自己决定形态）。
3. **vecstore 侧形态切换**（实测补上的必需一环）—— `VectorIndex::MatchesConfig()` + `VectorIndexServiceImpl::GetOrCreateForShapeLocked()`：Build 请求的 config 与常驻对象不一致时**替换索引对象**。没有它，免图 Build 会复用带图对象、静默重建出同样的产物。
4. **重建后经 §8.4 分发** —— **无需新代码**：免图重建走同一个 `doBuild` → `BuildCompleteCallback(READY)` → `main.go`/`realNode` 装配里无条件调用的 `distributeIndex` → `PushIndexToReplicas`。分发生来形态无关。

**测试**：`internal/index/index_test.go` 的 `LastAccessTracking`、`ColdPolicyPicksOnlyColdVersions`、`ColdPolicyRunsInBackground`、`ColdPolicyLifecycle`、`ColdRebuildReportsThroughBuildCallback`、`InstallIndexSeedsColdPolicyBaseline`；**集群层** `integration/cold_reshape_test.go` 的 `TestRealStack_ColdRebuildRedistributesTheArtifact`（6866 → 1325 字节，两节点字节相等，重建后仍可查；只 node 1 启评估器、node 2 用 `wireSyncPullDataOnly` 只拉数据）。

已知小限制（记录，未修）：副本 `InstallIndex` 不搬 `.index.mem` 字节侧车，且 `loadFromDisk` 遇已有内存条目时保留原 `sizeByKey`；形态换成免图后副本的内存记账仍按旧（带图）值，偏高，方向保守。

### 3.1b 顺带修掉的三个既有缺陷（本次补集群用例时暴露）

- **vecstore 缺形态切换**（`vecstore/src/grpc_service.cpp`）：`GetOrCreateLocked` 命中已有对象时忽略新 config，而形态在构造时定死 → 免图 Build 静默重建出**带图**产物。现由 `VectorIndex::MatchesConfig()` + `GetOrCreateForShapeLocked()`（仅 Build 使用；`AddChunks`/`Load` 不替换）解决。
- **空版本构建死循环**（`internal/index/impl.go` 的 `build()`）：空版本不再调 `Build(empty)` + `Save`（vecstore 对空 batch 不建索引，`Save` 必然失败 → 5 分钟重试 → 该版本查询全部 `index load timeout`）。这是 `TestRealStack_TwoNodeReplication` / `FaultTolerance` "10 次失败 9 次"的真凶。
- **免图内存记账含图项**（`vecstore/src/hnsw_index.cpp` 的 `EstimatedMemoryBytes()`）：免图形态不再叠加 HNSW 图开销，否则冷重建省下的正是这块内存、而 Go 侧字节预算读的恰是它。新增 `HNSWVectorIndexTest.GraphFreeEstimateOmitsTheGraph`（全精度免图估计精确等于 `n×dim×4`；两种形态的"带图 − 免图"差值相等）。
- **测试基础设施**：`realNode` 新增 `logger`（默认 `zap.NewNop()`），异步路径（分发、sync 拉取、构建回调）不再调 `t.Logf` —— 冷评估器让"构建在测试结束后完成"成为常态，在已结束的 `*testing.T` 上记录日志会让整个测试进程 panic。

### 3.2 之后

- **§8.6(c) 增量复用** —— **纯追加路径已实现**（2026-09-13），剩下的是含删除场景：
  - **怎么走的**：Go 侧 `IndexManagerImpl.appendBase()` 判定（有父版本 + 父产物在本节点磁盘 + 父形态已知且与目标一致 + 至少一个可追加的新 chunk）→ `buildFromBase()` 调新 RPC `LoadForAppend(起点产物路径)`（vecstore 侧加载后停在 **BUILDING**，因此可继续 `AddChunks`）→ 判墓碑占比 → 只追加 **delta** → `Save`。任一步失败都**回退全量重建**（增量只是优化）。装配点：`main.go` 与 `integration` 的 `realNode` 都调 `SetVersionParentGetter(...)`（从 Raft 版本元数据读 `ParentVersionID`）。
  - **删除场景（已做）**：被删文档的向量对索引不再需要。**免图形态真删**：`buildFromBase()` 在 `LoadForAppend` 后、`AddChunks` 前调 `RemoveChunks`（新 RPC；vecstore 侧 faiss `remove_ids` + **按压缩后顺序重建 chunk-id 映射**），当场回收。**带图形态只能留墓碑**（faiss 对 HNSW 没有 `remove_ids`）——但**查询路径本来就过滤它们**（`service/query.go` 的 `chunkDocMapper.ListDocIDs` 反查 + per-version bloom/`versionDocList` 确认 + `docStore.ReadAt(versionID)`；`chunkdoc.DeleteByDoc` 会清掉反查映射），所以墓碑只花内存与 top-K 候选名额。带图形态、以及免图里"说不清 id 的祖先遗留墓碑"，统一由阈值兜底：`LoadForAppend` 回报 `base_ntotal`（免图再减去本次真删的），`buildFromBase()` 据此算死亡占比（`≈ base_ntotal − (totalChunks − len(delta))`，上界），**超过 `AppendMaxDeadRatio`（默认 0.2，yaml `index_manager.append_max_dead_ratio`，1.0 = 不检查）就放弃增量改全量重建**，顺带清墓碑。
  - **`RemoveChunks` 的两条前置（vecstore 强制）**：只对免图形态（带图直接 `FailedPrecondition`，既不动索引也不让 faiss 异常穿过 gRPC 边界）；只在 **BUILDING** 时（已 `Save` 封印的索引要先 `LoadForAppend` 重开）。
  - **仍属未做**：已封印（READY）索引的**事后清理**——`RemoveChunks` 要求先重开，所以"给一个已就绪的免图索引做后台清墓碑"暂时走不通（当前靠阈值重建兜底）；真要做得先想清楚"重开期间该版本的查询怎么办"。
  - 四条核实结论（别再去查文档）：HNSW 能对读回的索引继续 `add`；HNSW 不能 `remove_ids`；免图形态能 `remove_ids` 且压缩存储；`Save` 封印构建是"必须加 `LoadForAppend`"的原因。KB 的 quantizer 创建时固定 → 跨版本复用必须**同形态**（`appendBase` 正是照这条判定的）；需训练的 SQ8/PQ 在增量下码本可能轻微失配（免训练的 SQ_BF16/SQ_FP16 无此问题）。
  - **增量特有的两处防御（已实现）**：①`delta` 是**集合差**（本版本 − 父版本），不是"产物内容差"，所以它可能指到 base 里已有的 chunk（典型：某代删了、后代又加回来）——chunk id 是**内容寻址**的（`SHA-256(文本 + embed 配置)`，`internal/splitter/sliding_window.go:77`）⇒ 同一 id 必然同一向量 ⇒ `AddChunksLocked` 对已存在的 id **直接跳过**，并维护 `known_chunk_ids_` 镜像（`Reset`/`Load`/`RemoveChunks` 三处同步，所以"删掉再加回来"仍能插入）；②`RemoveChunks` 删的是该 chunk 的**每一份**向量（老产物可能列两次），不是只删第一个匹配位置。
  - **删除中间版本（SINGLE）× 增量复用**：状态机会把子版本重新接到祖父（`state_machine.go:264`；祖父不可用则新父为 0）。delta 每次按**当前** `ParentVersionID` 现算 ⇒ 被删版本那部分变更自动并入新 delta，**不漏算**；仍被本版本引用的 chunk 不是孤儿（`gc.go:41`）⇒ 不会被回收。但"跨度变大仍是增量"有四个前提：祖父存活、**祖父产物在本节点磁盘**（惰性构建下非活跃版本通常没有，最常见）、祖父形态记录已知且一致、本版本有新增 chunk；否则退回全量。
  - 相关测试：Go `internal/index/index_test.go` 的 `BuildReusesParentArtifactOnPureAppend` / `BuildRebuildsWhenThereIsNothingToAppend` / `BuildReusesParentArtifactWhenDeletionsAreSmall` / `BuildRebuildsWhenTombstonesExceedRatio` / `BuildReusesDespiteTombstonesWhenRatioDisabled` / `BuildRemovesDeadChunksFromGraphFreeIndex` / `BuildDoesNotRemoveChunksForGraphedIndex` / `BuildFallsBackWhenRemoveChunksFails` / `BuildFallsBackWhenAppendReuseFails` / `BuildSkipsReuseWhenParentShapeIsUnknown`；C++ `vecstore/test/hnsw_index_test.cpp` 的 `LoadForAppendContinuesABuildFromAnArtifact` / `RemoveChunksCompactsGraphFreeIndexAndKeepsTheMapping` / `RemoveChunksRejectsSealedAndGraphedIndexes` / `AddChunksSkipsChunksAlreadyInTheIndex` / `RemovedChunksBecomeAddableAgain` / `RemoveChunksDropsEveryCopyOfAChunk` 与那四条 `Faiss*` 核实用例。
- **§9 服务站** —— 用户明确排最后

### 3.3 "任意节点当协调者"（§8.5 动态指派）——模型已按 §7.13.2 收敛为"控制层指派"

> **【2026-09 更新】** 本节下面记的是当时按"受理者就地当协调者"落地的两步（含当时修掉的三处"协调者 = leader"隐含等式）。**现行模型见 §7.13.2**：`CreateVersion` 回 leader-bound、**受理者必须是 leader**（直连 follower 拒绝 `NotLeader`）、apply 时现查 `IsLeader()` 的节点按副本拓扑派活。**那两步的成果保留**：数据源注册表仍作推送式提示；`EnsureIndex` 游标判据、`onVersionCreated` 每 applier 通知、`sourceAddrFor`/`Resolve` 的自我源短路全部仍在用。下面的记录保留以备追溯。

**"任意节点当协调者"**（= §8.5 的动态指派，也是单点压力的第一刀）。曾试过一小步并**已回退**，前置已查明：

> 需要的不是"把 `CreateVersion` 移出 leader-bound 名单"，而是**一套不阻塞 Raft apply 的数据源发现**。

正确形态：协调者写完数据后**主动告知**副本"数据在我这儿"（与 §7.3 的收尾信号同一路数），把"找源"从**拉取方探测**变成**推送方的已知事实**。

**第一步（数据源注册通道，已完成）**：

- `plane.DataSourceRegistry`：`(kb, version) → DataSyncService 地址` 的内存表。内存是刻意的——丢了无代价（回退 leader，下次确认时重新学到），持久化反而多一个真值来源。
- **告知走既有的 §7.3 收尾信号**：`ConfirmVersionWriteRequest` 加了 `source_addr`（协调者填自己的地址，装配点 `main.go` 的 `SelfDataSyncAddr`）；副本侧 `PushHandler.ConfirmVersionWrite` 把它记进注册表，`DeleteVersionData` 回收数据时 `ForgetVersion` 掉。
- **查找路径**：`SourceResolver` 的签名补上了 `(kbID, versionID)`（原来只有 `ctx`，因而只能答"leader"）；`plane.ResolverWithRegistry(reg, fallback)` 先查表、未命中回退 leader。**行为与第一步之前完全一致**（当前协调者就是 leader），而且仍是"廉价、不探测"的查找——第 1 条约束没有被破坏。
- 验证：`internal/plane/data_source_registry_test.go`（注册/覆盖/空地址忽略/按 KB 与版本隔离/并发；resolver 的注册优先、未注册回退、错误透传、nil 安全）。

**第二步（放开 `CreateVersion`，已完成）**：

- `CreateVersion` 移出 `internal/router/classify.go` 的 `writeMethods`：受理它的节点即该版本的写协调者，就地跑写路径（落数据 → 扇出 → 凑 quorum → 提议，提议经既有转发到 leader）。动手前已核查 `WriteCoordinatorImpl.Execute` 全程不依赖"我是 leader"（只有转发提议 + 本地存储事务）。
- **两处"writer == leader"的隐含等式必须一起解除**，否则协调者一变成 follower 就出事：
  1. `EnsureIndex`/`FetchVersionData` 原把 `resolve() → ok=false` 读作"我是 writer" ⇒ 改用**本节点连续游标**（`localVersionOf(kbID) ≥ versionID`）作判据；重试循环里**每轮复查游标、并重新解析源**——前者覆盖"协调者自己的 apply 早于它自己写完数据"，后者覆盖"声明晚到"（非 leader 协调者写完才宣告自己，第一次解析到的 leader 不持有该版本，守着它只会白拉到超时）。回归测试 `TestLocalDataPlane_EnsureIndex_ReResolvesSourceEachAttempt`。
  2. **`onVersionCreated` 过去只在非提案者 apply 时触发**（`internal/raft/impl.go`），编码的正是"提案者 = 写者"。转发形态下 leader 也是提议的中转方（有本地 waiter ⇒ 被跳过），而它恰恰不持有数据 ⇒ **leader 永不拉取、数据收敛失败**（实测 25s 超时）。改为每个 applier 都触发，由数据面按游标判断"要不要拉"——Raft 层本来也不该知道数据在哪。固化旧假设的单测 `…_NoopForProposer` 改名 `…_FiresForProposerToo`。
- 端到端验证：`integration/non_leader_coordinator_test.go` 的 `TestRealStack_ThreeNodeCluster_NonLeaderCanCoordinateWrite` —— 协调者取非 leader，断言调用成功、数据落在协调者、**leader 不持有该版本**（这条对照让后面的收敛只能来自跨节点搬运）、每个节点都收敛到数据且可查。`-count=5` 连跑通过。
- **装配边界**：`integration` 的 `realNode` 写路径是旧形态（不经过 plane），所以 §8.5 的声明广播不会自然发生 —— 测试显式写入每个**读者**的声明表来替代（生产由 plane 的确认广播发往所有候选）；读取侧 `sourceAddrFor` 与生产同构。
- **仍是 leader-bound**：`CreateKnowledgeBase` / `DeleteKnowledgeBase` / `RollbackVersion` / `DeleteVersion` / `RebuildIndex` / `WarmupVersion` 未动；版本删除类操作同样有"协调者 ≠ leader"的数据问题，各自需先确认数据面语义，留作后续独立一项。

### 3.4 设计稿并入 v13 + 一处实现修正（本轮）

- **并入**：`coordinator-selection-and-node-liveness-design.md`（此前**未被任何文档吸收**）的 §1–§5 已并入 `Stratum_设计文档v13.md` **§7.13**：上报时的 leader 发现复用 router 的动态发现、协调者"试了再说"、**apply 阶段谁能触发外部副作用**的边界、`ReportEpoch` 聚合查询"数据在哪"、节点间局部健康视图。v13 §12.1 已登记该稿的定位，§12.3 补冲突 **#8**，并顺带更正 #5（终态**只由控制层单侧持有**，存储层不维护自己的 `FAILED_PERMANENT`）。
- **一条硬边界已写进 v13 §7.13.4**：`ReportEpoch` 聚合是允许滞后的软状态，**不得接入正确性判定**——若把 `service/datamissing.go` 的"数据是否从未落地"（现状：实时逐个探测、不可达算未知）换成查聚合，刚写完未到上报周期或聚合因 leader 切换刚清空的时段会被读成"没有任何副本持有"，进而误送 `FAILED_PERMANENT` ⇒ 触发 §10.6 清理广播**删掉真实数据**（不可逆）。
- **实现修正**：`ReconcileIndexes` 原跳过 `IndexStatusFailed` 但**不跳过** `IndexStatusFailedPermanent` ⇒ 会对已终态、数据可能已回收的版本反复 `TriggerBuild`（每次失败并 Warn），与 `types.go` 的"nothing re-triggers it"矛盾。现两者都跳过；`TestLocalDataPlane_ReconcileIndexes_DecisionTable` 增加 FAILED_PERMANENT 用例锁定（实现漏改则测试失败）。
- **待决 / 已决**：
  1. ✅ **协调者模型已决（v13 §7.13.2）：控制层 apply 时按拓扑指派**，取代此前的"受理者就地当"。理由（§7.13.1）：`CreateVersion` 本质是一次 Raft propose，只有 leader 能写进日志；让 follower 自行转发 = 在客户端逻辑之外再长一条重复转发路径。因此 `CreateVersion` 回 leader-bound、**直连 follower 一律拒绝 `NotLeader`**，协调者由 apply 时现查 `IsLeader()` 的节点按副本拓扑挑候选派出。已落地（见下文第 5 条）。
  2. ✅ **"索引产物清理"（§1.4(4)）已实现**（要求文本与进展均已并入 v13 §10.6）。落地比预期简单：`IndexManagerImpl.Discard` **早已具备**全部所需能力 —— evict 内存条目 + 置版本墓碑（关闭 Load-RPC 复活竞态）+ `Reset` RPC 清 vecstore 侧索引 + 删除 `.index`/`.ids`/`.index.mem`（全幂等）—— 此前只被 `delete_version_impl.go`（删除版本路径）调用，缺口只是"终态清理路径没接它"。现在 `DropVersionStorage` 末尾调 `Discard`（`IndexManager` 为空则跳过），**没有新增 proto、没有改 C++、没有改接口**（`WriteCoordinatorConfig.IndexManager` 本就有）。验证：新增 `internal/coordinator` 2 例（Discard 被调用且重试幂等；未装配 IndexManager 时仍完成其余清理），`coordinator`/`plane`/`sync` 三包全绿。
  3. ⚠️ **§1.4(4) 的另一半（第 6 节）已并入 v13 §8.8，但核实后发现其前提与现状不符**：第 6 节的设计是"扫描处于 `BUILDING` 状态的**磁盘**产物、按文件 mtime 自超时清理"。核实结论是**没有作用对象**——`HNSWVectorIndex` 的 `Build`/`AddChunks` 全程在内存（源码里只有 "sealed by Save" 一句注释），唯一落盘的 `Save` 已是 `.tmp` + rename **原子写**并带 CRC32（注释原文："a crash or a write failure can therefore never leave a torn index"），所以不存在"磁盘上的半成品产物"；`AddChunks` 不写盘 ⇒ 也不会刷新 mtime，判据扫不到东西。真正的残留是 ①被放弃的 `BUILDING` 索引对象（**在 vecstore 内存**，重启 / `Build` 覆盖 / §10.6(4) 的 `Discard` 都能清）②`Save` 中途崩溃留下的 `.tmp` 垃圾（下次 `Save` 覆盖，且不影响 `EnforceDiskRetention`）。**若要兜底，唯一"有对象"的做法是按 mtime 清孤立 `.tmp`**；否则本节可暂不做。
  4. ✅ **§7.13 其余部分已全部落地**：① 上报的 leader 发现——`sync.DataVersionReporter` 每周期重新解析 leader、不缓存；② 周期上报聚合——`plane.DataVersionRegistry`（leader 内存、整份替换）+ `ReportDataVersions` RPC（非 leader 回 `accepted=false` 而非报错）+ `plane.LeaderGate`（follower→leader 跃迁时清空）+ 查询入口 `LocalControlPlane.DataVersionHolders`（`ok=false` 表示**不可用**，≠"没人持有"）；③ 局部健康视图按 §7.13.5 的设计约定保留（不设权威维护者）。**⚠️ 术语陷阱**：这里的 `ReportEpoch` 聚合与代码里**同名**的一次性重启对账 `ControlPlane.ReportEpoch` 不是一回事（v13 §7.13.4 已加注记）。
  5. ✅ **协调者模型（§7.13.2）已完成并落地**。1a：`CreateVersion` 回 `internal/router` 的 `writeMethods`。1b：`ExecuteVersionWrite` RPC + 候选侧 handler（`WithVersionWriteExecutor`）+ `CoordinatorDispatcher`（稳定顺序 try 候选、本地兜底、"接受但未完成"算失败）+ `RaftNodeImpl.IsLeader()` + apply 时现查 leader 的派活钩子 + `Execute` 拆为"只提议 + 登记 + 失败 Forget"（`Dispatch == nil` 时保留旧的本地写回退）+ 准入拒绝（直连 follower ⇒ `kvraft.ErrNotLeader`）。集成用例已改写为 `TestRealStack_ThreeNodeCluster_NonLeaderRefusesTheWrite` 并通过；**全量 23 包全绿**。**未做的端到端验证**：派活路径需要一个"写路径经由 `plane.LocalDataPlane`"的测试栈（本栈仍直接写存储），所以"leader 派出去、候选写下来"目前只由单测覆盖（dispatcher 4 例、handler 3 例、登记表 5 例、WAL `ChangesFor` 4 例）。
  6. **`changes` 的持久化与回收（新设计，已并入 v13 §7.5）**：`changes` 不能只是 fan-out 的一次性 payload——§7.5 的"区间 `(localVersion, V-1]` 增量补齐"要求能拿出"这段区间改了什么"。落点是**复用现有 WAL**（`WAL.WriteBegin` 的 `encodeBeginPayload` **已经**把 changes 编码进 BEGIN 记录，原本服务本机崩溃重放），把读取面从"仅本机重启时读"扩到"响应对等节点追链"；回收判据 = **全部副本经 `ReportEpoch` 报过 `localVersion ≥ V`**（复用 §7.13.4 聚合，顺带解决 WAL 只增不减）；区间已被回收则退化为全量状态传输（§6.4）。
     **进展**：读取面第一步已完成——`WAL.ChangesFor(ctx, kbID, versionID)` 已实现（`FileWAL`/`MockWAL`，4 例测试含"重启后仍可读"），并顺带修掉一个真缺陷：`FileWAL.beginDataByVersion` 原先只在 `Open` 的重建里填充、**运行时写入不维护**，导致"本进程刚写完的版本"查不到（`MockWAL` 才是对的）；现在 `WriteBegin`/`WriteVersionID` 维护它，运行时的 `Recover()` 也因此更完整。
     **剩余**：**已全部完成**（原计划的 ①②③ 全部落地，含物理回收）。① 区间请求 RPC + 服务端 handler（阶段 B：`PullVersionChanges` 流式、按版本升序、缺口可见；服务端未装配 reader 报 `FailedPrecondition`，**绝不返回空区间**）+ 客户端 `sync.VersionChangesPuller`；`backfillTo` 先试增量（`backfillByChanges`）、缺口或失败退全量；重放走 `ApplyBackfillChanges`（只做本地事务，跳过 fan-out / digest 上报 / 索引调度 / 失败上报）。② 可回收水位 **+ 物理回收**（阶段 C 聚合 + D1：`DataVersionRegistry` + `ReclaimableChangesThrough`；后续补齐 `FileWAL.Compact`（整文件重写 + 原子 rename + 崩溃安全）、`LocalDataPlane.ReclaimChanges`、后台 `plane.WALReclaimer`，以及让"写数据但不是 leader"的节点也能拿到水位的**水位回传**——`ReportDataVersionsResponse.reclaimable` 复用既有上报通道，不新增 RPC）。③ 全量传输兜底（阶段 D2：`transferFullState`，仅在**元数据确认版本已删除**时跳 **`versionID`**（不是 `versionID-1`——后者可能正是被删的那个）的快照，且判定在拉取之前；顺带修掉"成功但无记录 → 静默推进游标"的既有隐患，判据走 `VersionExistenceChecker`/`LocalControlPlane.ExistingVersions`，一次取全表）。
     **1b 的修正顺序**：① WAL 按版本/区间读 changes → ② 追链 RPC + handler → ③ apply 阶段派活 → ④ 回收（依赖 §7.13.4 聚合）→ ⑤ 全量传输兜底。（历史记录：该顺序已全部走完。）
     **更正（本轮核实）**：③ 的派活**并不需要**从 WAL 读 changes —— 受理者就是 leader，请求里的 changes 此刻就在它手上，派活时随 RPC 发给候选即可；WAL 读取面真正服务的是**追链**（对等节点请求区间变更）。而 `FileWAL.beginDataByVersion`（`internal/wal/file.go:223`，由 `rebuildIndex` 在 `Open` 时从文件重放构建、每次 `Write*` 保持最新）**已经保留了全部 BEGIN 记录**，所以第 ① 步只是把它**导出成只读查询**（按 versionID 取 `changes`，并校验 kbID 一致）+ 区间取用，成本很小。另：`Recover()` 的注释正好印证了新模型的一个事实——"a VERSION_ID without bound BEGIN data … a follower applying leader's log"，即 follower 的 WAL 本来就没有 BEGIN；派活后**写数据的那台机器**才会写自己的 BEGIN，它的 WAL 因此成为追链服务端，与 §7.13.4 的"数据在哪"聚合口径一致。
     另：**"直连非 leader 的 `CreateVersion` 一律拒绝"已由用户确认**（返回 `NotLeader`）——理由与 §7.13.1 的 leader 发现同一条：它本质是一次 Raft propose，只有 leader 能写进日志，让 follower 自行转发等于在客户端逻辑之外多长一条重复转发路径。`CreateVersion` 归入 `isWriteMethod` 统一经 Router 到 leader（1a 已完成）。**连带约束（已并入 v13 §8.7）**：将来若恢复攒批，"攒批节点"这个角色被钉成"当前 leader"，缓冲只能建在 leader 进程内——落在半路 follower 上会凭空多一个"转发前崩溃"的丢失窗口。

---

## 4. 约束与教训（踩过的坑，别重踩）

| # | 约束 | 出处 |
|---|---|---|
| 1 | **`SourceResolver` 必须是廉价、不探测的查找**。它跑在 **Raft apply 路径**上（重放历史日志时每条 `CreateVersion` 都触发），任何阻塞式 RPC 都会拖住后续所有日志。实测反例：在它里面逐 peer 探测，`integration` 从 26s 恶化到 **462s 超时** | `local_data_plane.go` 的 `SourceResolver` 注释、v13 §8.5 |
| 2 | **新增一条"读取数据的路径"时，原路径上的守卫都要复制**。漏了删除墓碑检查 → 已删除的 KB 变得可查 | `brute_force.go` 的 `tryBruteForce`、v13 §8.6(b) |
| 3 | **状态由 `Save` 封印**（`BUILDING → READY`）。任何索引形态都必须 `Save` 之后才可查询 | `hnsw_index.cpp` 的 "Save seals the build" |
| 4 | **转发的等待语义**：`proposeAndWait` 等的是**应用该命令那台机器**的 apply。转发之后那是 **leader 的 apply**，所以"提议成功"与"本节点可见"之间多了一个异步间隔 | v13 §7.3 |
| 5 | **错误跨进程要传哨兵名**（`errors.Name`/`ByName`），否则转发后 `errors.Is` 失效 | `internal/errors` |
| 6 | **多副本同时上报无害，不需要选举代表**：提议幂等 + 状态机忽略"已定局"的迟到上报。这是"存储层不必长出共识协议"的关键 | v13 §7.3 |
| 7 | **`dataPlane` 晚于构建回调注册创建**时，用闭包变量延迟绑定 | `cmd/stratum/main.go` 的 `distributeIndex` |
| 8 | **`ExistsIndex` RPC 在部分 vecstore 构建里返回 `Unimplemented`** —— 别用它做测试判据，直接看文件 | `integration` 的 `indexFileOf` |
| 9 | **"提案者"不等于"数据的写者"**。`onVersionCreated` 曾只在非提案者 apply 时触发，隐含"提案者已就地写完数据"——这只在**协调者 = leader** 时成立。转发让 leader 也成为提议的中转方（于是被跳过，而它没有数据）⇒ 非 leader 协调者时数据永不收敛（实测 25s 超时）。**谁需要拉数据是数据面的问题，不是 Raft 的**：现在每个 applier 都触发，数据面按游标判断 | `internal/raft/impl.go` 的 `onVersionCreated`、v13 §8.5 |
| 10 | **"我是 writer"不能由"`resolve()` 说没有源"表达**。`EnsureIndex`/`FetchVersionData` 原用它表示"数据已在本机"，那同样只对协调者 = leader 成立。判据要直接问事实：**本机连续游标**（`localVersionOf ≥ versionID`），并且在重试循环里每轮复查（协调者自己的 apply 会早于它自己写完数据） | `internal/plane/local_data_plane.go` |
| 11 | **"源就是我自己"必须显式短路**。通知每个 applier 之后，协调者会收到**自己那条版本**的通知——而它的 apply 早于它自己的存储写入完成。此时若照常去"拉取"，就会从自己读回**写了一半的数据并据此建索引**（实测：`FaultTolerance` 基线一致性阶段 5/5 失败、报 `index is still building` / 查询空）。生产侧本来就有这条规则（`main.go` 的 `Resolve`：我是 leader ⇒ `("", false, nil)`），测试栈的 `sourceAddrFor` 也补上了；**新增"读数据的路径"时，这类自我短路要跟着一起确认**（呼应第 2 条） | `cmd/stratum/main.go` 的 `Resolve`、`integration` 的 `sourceAddrFor` |

---

## 5. 已知 flaky 测试（**不是**你的改动造成的，先单跑确认）

| 测试 | 位置 | 现状（最近一次核实） |
|---|---|---|
| `TestRealStack_ThreeNodeCluster_SnapshotPipeline` | `integration` | 仍偶发 |
| `TestRealStack_ThreeNodeCluster_NonLeaderCanPropose` | `integration`（未跟踪文件 `non_leader_propose_test.go`） | **已修正，不再是偶发**：失败根因不是功能坏了，而是提议那一瞬间该节点还没学到 leader（选举/心跳窗口）⇒ 转发没有目标 ⇒ `kvraft: not leader`。现在提议前 `waitForLeaderView` 等视图稳定、且只在 `ErrNotLeader` 上短退避重试（`proposeFromNonLeader`）；40 次连续运行全过 |
| `TestRealStack_ThreeNodeCluster_FaultTolerance` | `integration` | 曾 10 次里失败 9 次的"稳定失败"，根因已查明并修掉（**空版本构建死循环**，见下方"已修根因"）；修后仍**偶发**（近期 10 轮里 3~4 次失败，症状仍是 `index load timeout`）——属既有时序脆弱，不是那个死循环了 |
| `TestRealStack_TwoNodeReplication` | `integration` | 同一根因（空版本构建死循环）已修；但**仍会偶发**：`go test ./...` 并发跑时观测到 1 次失败（`hnsw_index: search: index is still building`，即 follower 20s 内索引没建完），而**单跑 2 次 + 单包跑 1 次均通过**（后者 28/28 全绿）。与 `FaultTolerance` 同类的时序脆弱，**并发跑时更明显** |
| `TestCluster_SnapshotCatchesUpLaggingFollower` | `internal/kvraft` | 仍偶发（单跑通过） |
| `TestRealStack_ThreeNodeCluster_NonLeaderRefusesTheWrite` | `integration`（`non_leader_coordinator_test.go`） | **偶发**：整包并发跑时观测到 1 次失败，症状是 `version is PENDING`——即 leader 写入成功但索引 20s 内没到 READY，查询超时。**单跑 3 次全过**（2.1s），与 `FaultTolerance`/`TwoNodeReplication` 同类的"索引构建时序"脆弱，**并发时更明显**。注意：该用例的**功能断言**（follower 被拒 `NotLeader`）在失败那次**是通过的**，失败发生在后续的查询等待上 |
| `LifecycleStateTest.ConcurrentSearchResetLoadSmoke` | vecstore（ctest） | 仍偶发（`ctest --rerun-failed` 重跑即通过；整轮 ctest 45/45） |

**已修根因（别当成 flaky 再放过去）**：`internal/index/impl.go` 的空版本构建原来调 `Build(empty)` + `Save`，而 vecstore 对空 batch 不创建索引、`Save` 必然失败 → 被判为可重试 → 5 分钟重试窗口占住 `loading`，该版本的查询全部 `index load timeout`。现在空版本构建在 Go 侧直接成功返回。相关单测已改写为 `TestIndexManager_BuildEmptyVersionDoesNotCallVecstore`。

**一条与 §8.6(c) 有关的时序观察**：纯追加复用（`LoadForAppend`）会先**把起点产物从磁盘读回来**再追加，因此版本的 vecstore 条目在 **BUILDING** 停留得比纯 `Build` 更久；多副本并发构建/安装同一版本时，更长的窗口理论上更容易撞上上面那条 `index load timeout`。10 轮 A/B：启用增量 4/10 失败、不启用 3/10 —— 在该用例的噪声内，所以 `realNode` 照常装配 `SetVersionParentGetter`（`integration/e2e_test.go` 的装配点旁也写了这段结论）。若哪天这条 flaky 变得频繁，先看这个窗口。

**修正记录（这里原先有两处不准确的记录）**：曾写"`TestRealStack_ThreeNodeCluster_NonLeaderCanPropose` 随『任意节点当协调者』回退一并删除"，两点都不对——① 工作区里那个**未跟踪**的 `integration/non_leader_propose_test.go` 一直在，且每次 `go test ./...` 都会跑到它；② 它测的是**转发**（§7.3 的 propose 通道，生产装配里由 `cmd/stratum/main.go` 的 `SetForwarder` 启用），**不是**已回退的动态指派（回退的是"谁当写协调者"）。已按"先修测试内容、再修竞态"处理：提议前用 `waitForLeaderView` 等 leader 视图稳定、只在 `ErrNotLeader` 上退避重试（`proposeFromNonLeader`）；并把同文件 `ForwardedErrorIsFaithful` 的**弱断言**加强——它原来只要求 `err != nil`，转发完全坏掉也会"通过"，现在必须拿到状态机哨兵 `ErrVersionNotFound`（日志里能看到 `version not found` 真的穿过了 gRPC 转发）。40 次连续运行全过；§3.3 重做动态指派时这两个用例可以直接复用。

---

## 6. 环境与命令

```bash
# Go 侧
go build ./... && go vet ./...
go test ./... -count=1 -timeout 420s

# vecstore（C++）—— 改了 vecstore/ 下的任何源码后必须重建两个构建目录：
#   * vecstore/build  ：ctest 用的（HNSWVectorIndex 单测）
#   * build/          ：integration 的 vecstore_server 子进程用的（top-level CMake）
cmake --build vecstore/build -j4 && (cd vecstore/build && ctest)   # 基线 45/45
cmake --build build --target vecstore_server -j4                   # 供 integration 使用

# proto 重新生成
protoc -I api/proto --go_out=. --go_opt=module=stratum \
       --go-grpc_out=. --go-grpc_opt=module=stratum api/proto/<file>.proto
protoc -I vecstore/proto --go_out=. --go_opt=module=stratum \
       --go-grpc_out=. --go-grpc_opt=module=stratum vecstore/proto/vecstore.proto
```

**vecstore 构建**：`vecstore/CMakeLists.txt` 里有一段 **RocksDB shim**（`if(NOT TARGET ...)`），是为绕开 Ubuntu `librocksdb-dev` 的 `RocksDBTargets.cmake` 引用自身未定义目标而加的。缺了它链接会报 `-lgflags::gflags_shared` 之类。

**⚠️ 两个构建目录都要重建**：`integration` 用的是 `build/vecstore/vecstore_server`（顶层 CMake 输出），`ctest` 用的是 `vecstore/build/`。只重建后者会让 `integration` 继续跑**过期的 vecstore 二进制**——本次实测就踩到：过期二进制不认识 `QUANTIZER_*_FLAT`（枚举落到 `default` → 全精度 HNSW），于是冷热转换"执行了但产物一个字节没变"，排查了很久。用 `STRATUM_VECSTORE_SERVER_BIN=<path>` 可以临时指向别的二进制做对照实验。

**测试用的 vecstore 子进程**由 `integration` 自己拉起（`startVecstoreServerForTest`）；真实多节点装配见 `integration/e2e_test.go` 的 `realNode`。

**测试里的异步路径不要用 `t.Logf`/`t.Errorf`**：§8.6a 的冷评估器让"构建在测试结束后才完成"变成常态（构建回调会调 `t.Logf`，在已结束的 `*testing.T` 上记录日志会让整个测试进程 panic）。`realNode` 现有一个 `logger`（默认 `zap.NewNop()`），分发/拉取/构建的异步日志都走它；新增后台路径时沿用。


---

## 7. 工作方式（沿用即可，非强制）

- 全程简体中文；代码/标识符/路径/命令保留原文
- 大改动**分阶段 + 每阶段跑全量**；触及 `main.go` 装配或 Raft apply 路径时，**必须**单独复跑可疑的 `RealStack_*` 用例
- 阶段性完成后**回填 v13**（各节都带状态标注与"实现进展"表）
- 临时脚本放 `/tmp`，用完即删，不在仓库留 scratch

---

## 8. 一行总结

**§8 整节完成、§9 未开。** §8.4 分发、§8.6(a) 免图分层与冷热转换、§8.6(b) 惰性构建、§8.6(c) 增量复用（纯追加 + 删除场景：免图 `RemoveChunks` 真删、带图墓碑阈值与延迟全量重建）均已落地；`ctest` **50/50**，Go 侧除 §5 清单里的偶发用例外全绿。下一步可选：按 §3.3 的正确形态重做"任意节点当协调者"（解决单点压力）、或（用户明确排最后）§9 服务站；§8.6(c) 剩下的只有"已封印索引的事后清理"（见 §3.2 末）。
