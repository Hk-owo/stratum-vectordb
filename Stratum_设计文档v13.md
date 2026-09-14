# Stratum 设计文档 v13 —— 存储层协调协议、读路径服务站与版本链线性化

> 面向 RAG 场景的分布式知识库存储系统
> 本文档记录系统的设计方向、数据模型、模块职责和关键流程，不包含代码层面的实现细节。
> **本文档（v13）在 v12 之上整合 v1（`control-data-separation-design.md`）与三份补充设计稿（见 §12.1）。** v12 的两段式检索与分级存储已落地，其余「背景与现状」与当前代码对齐。v13 新增的 §6–§11 中，**已实现**：§6（版本链线性化）、§7 的写事务方案（§7.12）、quorum 判定与 fan-out（§7.1/§7.2）、游标与追链（§7.5）、无数据 version 检测（§7.12 ①/②）、§8.3 的前置（索引原子写 + checksum）；**§7 已整节实现（§7.1–§7.12 全部 ✅）**；**§8.4 的索引分发已实现**（"建一次、分发 N 份"，含跨节点传输通道）。仍为目标设计：§8.5–§8.7、§9、§10 的其余部分（其中 §10.1 的 `FAILED_PERMANENT` 判定与暴露已实现）。各节均带状态标注，§11 给出阶段级进度。

---

## 版本变更记录

### v13（设计稿）：存储层协调协议 / 索引多副本分发 / 读路径服务站 / 版本链线性化

v13 把 `control-data-separation-design.md`（v1，前置基准）与三份补充设计稿（`storage-coordination-and-service-station-design.md`、`unresolved-issues-resolution-final.md`、`version-linearization-decision.md`）整合进正文，并登记它们与 v11 / v12 / 当前实现之间的冲突。阶段 ⓪ 的决策补齐已完成（三条结论见 §11）；同时回填了 v12 的两处过时状态标注（v12 的量化两段式并非"尚未实现"，见 §12.3 冲突 #4）。

**实现进展（正文各节带状态标注，§11 给出阶段级进度）**：§6 版本链线性化、§7.12 写事务方案、§7 的 push/fan-out/quorum（§7.1/§7.2）、游标与追链（§7.5）与游标交换（§7.6）、无数据 version 检测（§7.12 ①②）、§8.3 前置的索引原子写与 checksum、§7.3 的协调者崩溃接管（含前置的 propose 通道）、§7.8 的恢复时安全 durable version（quorum 最小值）、§7.9 的 `ReportEpoch` payload 修订、§7.7 的 per-KB 在飞写入上限与排队（至此 **§7 整节闭合**）、§10.1 的 `FAILED_PERMANENT` 判定与暴露（含 `FailureFatalGlobal` 短路与 per-KB 失败预算）、§10.6 的判死后清理（含失败重试队列）与迟到上报校验均**已实现**；**§8.6(a) 的免图分层已实现**（vecstore 可建免图索引并跨节点装载；存储层可请求免图形态；冷热转换策略——访问跟踪 + 后台冷阈评估 + 形态记账——已落地，形态变化经既有 §8.4 回调链重新分发）；**§8.6(b) 的惰性构建已实现**（活跃版本预建 + 数据/索引 READY 分离 + 查询时按需构建与暴力扫描兜底）；**§8.4 的"建一次、分发 N 份"已实现并端到端验证**（含索引文件跨节点传输通道、真实双节点栈上"副本安装的正是构建者的产物"）；§8.5–§8.7、§9 与 §10 的其余部分仍为目标设计。"收线头"清单中剩下两项均为显式结论而非欠缺：`SourceResolver` 回退 leader 是**必要设计**（节点自身为 leader 时阻塞式拉取会卡住 apply 循环），`sync.LeaderHandler` 的改名是纯清理。另：**§8.5 的"任意节点当协调者"经过一次尝试后回退**——真实前置是不阻塞 Raft apply 的数据源发现（详见 §8.5 的实现进展），代码注释中已留约束。**§8.7 写入攒批已撤销**——目标写入模式是一次调用提交一批变更，攒批在该模式下无收益而代价照付。

1. **版本链线性化（§6）**：`CreateVersion` 强制父版本最多一个子版本，丢弃版本树分叉。这让实现与 v12 / `改动内容.md` 早已采用的"线性版本链"表述收敛一致（此前 `README.md:96` 与代码均允许分叉）。
2. **存储层协调协议（§7）**：写路径由现状"leader 本地落盘 + follower 主动 `PullVersionData` 拉取"改为"任意节点临时担任协调者 → fan-out 到 N 副本 → 各自确认 → 达 quorum 上报"；新增数据游标 `localVersion[kbID]` 与追链不变式、`ReportEpoch` payload 修订。§7.0 固定了 v1 的契约基准（`DataPlane` / `ControlPlane`、可用性模型、epoch 恢复协议、演进路径）。
3. **索引构建与多副本分发（§8）**：以"建一次、分发 N 份"取代 v1 §9 #6 与 v11「多副本构建」的"每副本独立构建"（阶段 ⓪ 已定，见 §8.4），并前置补齐索引文件的原子写与 checksum（当前 `vecstore/src/hnsw_index.cpp:382` 为裸 `faiss::write_index`）；另含免图分层、惰性构建、增量复用三层成本优化（写入攒批已撤销，见 §8.7）。
4. **读路径服务站（§9）**：把现状 `internal/router`（读 round-robin + 写转发 leader + 故障转移）升级为带路由表缓存、新鲜度凭证校验、鉴权与熔断的服务站。
5. **遗留事项收敛（§10）**：Saga 永久失败终态 `FAILED_PERMANENT`（**判定与暴露已实现**，见 §10.1）、索引服务能力阈值归属、4.4 节四个工程细节、一致性窗口占位值（`ConsistencyBudget`）。
6. **实施路线（§11）与文档对齐（§12）**：按依赖给出阶段 ⓪–⑦（含 v1 阶段 1 的契约抽取）；登记 v1 引述核对结果（4 处偏差已更正）、术语映射表、与 v11 / v12 / README 的冲突清单。

### v12（已落地）：内存量化 HNSW 粗筛 + 磁盘全精度 rerank + IndexManager 分级存储

v12 相对 v11（与实现全面对齐）提出以下增量设计：

1. **检索架构变化（两段式检索）**：从"整份全精度 HNSW 索引驻留内存、单段搜索直接出 top-k"，改为"内存量化粗筛器出候选 top-N → 从磁盘全精度原向量（复用现有 RocksDB chunk store）批量读取 → 精确距离 rerank 出最终 top-k"。
2. **索引后端变化**：内存索引后端由 `IndexHNSWFlat`（float32 原向量）按 KB 配置切换为 Faiss 量化变体 `IndexHNSWSQ` / `IndexHNSWPQ`（图结构保留、向量载荷压缩为量化码）；量化器仅承担召回，不承担最终评分。
3. **索引管理变化（LRU → 分级存储）**：Go 侧 IndexManager 内存驻留对象从"完整索引副本"改为"每版本量化粗筛器"；全精度向量永驻磁盘；LRU 淘汰语义保留，但换出对象与内存记账口径（`.index.mem`）随之改变。
4. **兼容要求**：未开启量化的 KB 保持现状路径不变；存量 `.index`（Flat 格式）文件可继续 Load；磁盘上的索引文件与全精度向量均不压缩。

> **状态回填（v13）**：本条已落地。`api/proto/knowledgebase.proto:44` 有 `QuantizerType` 枚举，KB 元数据字段（`:97`）与创建请求字段（`:120`）均已就位；`configs/config1.yaml:46` 已按"图边 + 量化码"粗筛器口径说明内存记账；附录 D 的召回与延迟基准已产出（含 `vecstore/test/latency_bench_test.cpp`）。因此 v12 头部原有的"尚未实现"与本节原有的"均未实现"均不再成立。

---

## 1 背景与现状

### 1.1 动机与目标

现状下每个版本的向量索引是**全精度 HNSW**（`faiss::IndexHNSWFlat`，float32 原向量 + HNSW 图），整份驻留内存。单版本内存占用按向量载荷估算为 `4 字节 × 维度 × chunk 数`（internal/index/impl.go:569-594 的 `sizeBytes` 口径），随版本数据量线性膨胀；而每个知识库可同时存在多个版本（保留窗口内几十上百个），索引管理器用 LRU 容量 + 内存字节阈值约束内存（internal/index/impl.go:674-703），超限即换出整份索引，冷版本查询时再整份 Load 回内存。

设计目标：

- **降低单版本索引的内存占用**：内存中不再保留任何全精度向量；每版本内存驻留物缩小为"量化粗筛器"（HNSW 图边 + 量化码 + 码本/量化状态）。
- **磁盘向量不压缩、分片可读**：全精度 float32 原向量继续以现有 RocksDB chunk store 为权威存储（内容寻址、按 key 定位），不做压缩、不重建新存储。
- **召回精度不因量化而损失在最终结果上**：量化只用于快速召回候选，最终 top-k 由磁盘上的全精度原向量精确 rerank 得出。
- **索引管理分级化**：内存 = 热层（粗筛器缓存）；磁盘 = 冷层（索引文件）+ 永久层（全精度向量）。全精度向量永不整体进入内存。

非目标（明确不做）：

- 不把磁盘上的原向量/索引文件压缩（不引入 PQ-on-disk、不做向量文件整体压缩）。
- 不引入新的独立开源库（选型结论见 1.3）。
- 不新增独立的磁盘向量存储层（复用现有 RocksDB chunk store）。
- 不改变对外 gRPC/Query 语义（结果仍是"chunk_id + score"的 top-k，阈值/评分语义统一到 rerank 之后，见 2.2）。

### 1.2 现状盘点

以下事实均核对自当前代码（行号以 2026-09 工作区为准）。

#### 1.2.1 C++ vecstore：全精度 chunk 向量 + 单段全精度索引

**全精度向量权威存储（RocksDB）**（vecstore/include/chunk_storage.h、vecstore/src/rocksdb_storage.h）：

- `ChunkStorage` 接口：`Write/Read/Exists/Delete/DeleteByPrefix/DiskUsage`，按**不透明 key**存取**一条 float32 向量**（chunk_storage.h:32-58）。
- `RocksDBChunkStorage`：每个 chunk 一条 RocksDB value = 小端 float32 数组；**key-format-agnostic**——key 由调用方（Go 侧）按 `kbID + chunkID` 语义拼好，C++ 存储层原样透传（rocksdb_storage.h:19-24）。Go 侧 key 编码见 1.2.2。
- chunk 内容寻址、跨版本去重：相同文本 + 相同 embed 配置切出的 chunk 在同一知识库只存一份（Stratum_设计文档v11.md:207-212）。

**向量索引（Faiss HNSW，单段全精度）**（vecstore/include/vector_index.h、vecstore/src/hnsw_index.cpp）：

- 接口：`Build / AddChunks / Search / Save / Load / Reset`（vector_index.h:33-75），按 `(kb_id, version_id)` 一一对应一份索引。
- 实现 `HNSWVectorIndex`：构造 `faiss::IndexHNSWFlat(dim, M=32, metric)`（hnsw_index.cpp:84，常量 :29-31）；COSINE 以 L2 归一化 + `METRIC_INNER_PRODUCT` 实现（:33-53, :98-101, :124-127）；EUCLIDEAN 距离取负以统一"分高者更相似"（:140-145）。
- 训练语义：Flat 无训练。构建 = 分批 `Build` + `AddChunks`（gRPC 单消息 ≤ 4 MiB 预算，Go 侧切批，impl.go:554-594）。
- 持久化：`Save` = `faiss::write_index` 主文件 + `.ids` 边车（首行 dim、次行 metric，之后每行一个 chunk_id，即 faiss 插入序 ↔ chunk_id 映射）（hnsw_index.cpp:151-175）；`Load` = `faiss::read_index` + `dynamic_cast<IndexHNSWFlat*>`（:177-213）——**当前 Load 强绑定 Flat 类型**。

**gRPC service 组合**（vecstore/src/grpc_service.h、vecstore/src/grpc_service.cpp）：

- `ChunkStorageServiceImpl` 与 `VectorIndexServiceImpl` 是**两个独立 service**：前者持有 `ChunkStorage*`（grpc_service.h:28-53），后者**不持有** ChunkStorage，只持有 `map<(kb_id, version_id), unique_ptr<VectorIndex>>`（grpc_service.h:63-103，实现统一 `GetOrCreateLocked` 创建 `HNSWVectorIndex`，grpc_service.cpp:118-125）。
- `VecstoreGrpcServer` 分别持有 `storage_ / chunk_service_ / index_service_`（grpc_service.cpp:252-255 附近）——**两个 service 之间目前没有任何共享引用**。这是"Search 内部 rerank 需访问磁盘原向量"时首先要打破的隔离（见 2.3.4）。
- `vecstore.proto`：`VectorIndexService` 提供 `Build/AddChunks/Search/Save/Load/ExistsIndex/Reset`（vecstore.proto:21-40）；`BuildIndexRequest` 现仅携带 `kb_id / version_id / chunks / metric`（:74-79），**无量化配置通道**。

#### 1.2.2 Go 侧：key 语义与 IndexManager LRU

**ChunkStore key 编码**（internal/chunkstore/grpc_client.go:68-98）：

- `encodeKey(kbID, chunkID)` = `encodeKBPrefix(kbID) + chunkID`，其中 `encodeKBPrefix` 为 4 字节大端 kbID 长度前缀 + kbID 原字节。长度前缀保证 `DeleteByPrefix(kbID 前缀)` 不会误删"以相同字符开头但更长"的其它 kbID。
- 索引构建数据源与索引内 `chunk_id`（纯 ID）↔ RocksDB key（`encodeKey`）的换算**发生在 Go 侧**（IndexManager 的回调与 `EncodeKey`，grpc_client.go:78-84、cmd/stratum/main.go 接线）。C++ 侧索引只存 `chunk_id` 字符串，**不知道 key 规则**。

**IndexManager（internal/index/impl.go）**：

- 配置：`LRUCapacity`（条目数上限）、`MemoryThresholdMB`（字节阈值，impl.go:62-68，语义按向量载荷 `4B×d×n` 记账）、`IndexRetentionCount`（磁盘保留版本数）、`IndexDataDir`（impl.go:49-60）。
- 内存条目：`loaded map[indexKey]*loadedIndex`，`loadedIndex{refCount, lastAccess}`（impl.go:97-108, 126-129）；`sizeByKey/loadedBytes` 记账（impl.go:102-108）；删除墓碑 `deletedKBs/deletedVersions` 防 Load 复活（impl.go:110-119）。
- 查询：`Search` 未命中先 `loadFromDisk`（整份索引 Load 回内存）→ `acquire`（refcount++、记 lastAccess）→ vecstore `Search` RPC → `release`（impl.go:200-244）。
- 加载：`loadFromDisk` = vecstore `Load` RPC + `makeRoomLocked` + 读 `.index.mem` 尺寸边车入账（impl.go:516-552）。
- 淘汰：`makeRoomLocked` 在条目数 ≥ `LRUCapacity` 或 `loadedBytes > MemoryThresholdMB` 时，按 `lastAccess` 淘汰 `refCount == 0` 的最久未用条目（impl.go:674-703）。
- 尺寸边车：构建完成时写 `<IndexDataDir>/index/<kbID>/<versionID>.index.mem`（尺寸 = `4×d×n`，collectChunkBatches 的 `sizeBytes`），重启后 `loadFromDisk` 读取恢复记账（impl.go:569-594, 883-899）。
- 删除：`Evict`（清单个版本内存）、`EvictByKB`（清整 KB 内存）、`Discard`（清内存 + 墓碑 + vecstore Reset + 删磁盘文件）、`DeleteFilesByKB`（整 KB 磁盘索引目录 + 墓碑），均幂等（impl.go:740-766, 906+, 775+）。
- 接口契约见 internal/index/index.go（Search 语义、Evict/Discard/EvictByKB/DeleteFilesByKB 等，index.go:53-102）；LRU 相关行为测试见 internal/index/index_test.go（含字节阈值触发与 LRU 淘汰用例）。

#### 1.2.3 版本删除语义：三种删除模式

原 `DeleteVersion` 只能「删除某版本及其全部后代」——对线性版本链而言等价于只能截断链尾，既无法单独删掉一个中间版本，也无法把历史上的某个版本「提升」为新的基线。现扩展为按 `mode` 选择删除范围（`api/proto/knowledgebase.proto` 的 `VersionDeleteMode`，内部对应 `internal/types.VersionDeleteMode`）。三种模式的待删集合全部在 Raft 状态机 apply 阶段基于当前快照确定性计算（`internal/raft/state_machine.go` 的 `versionDeleteTargets`），保证各副本结论一致：

| mode | 待删集合 | 结构变更 |
|---|---|---|
| `SUBTREE`（默认） | 目标版本 + 全部后代 | 无 |
| `SINGLE` | 仅目标版本 | 目标的直接子版本改挂到目标的父版本（链表 splice），目标之下的分支结构保留 |
| `ANCESTORS` | 目标的全部前置版本 + 这些祖先上挂着的旁支 | 目标的 `parent_version_id` 置 0，成为版本链新的基底 |

**不变量**：

- **校验先于变更**：整个待删集合先做「无活跃版本 / 无 PENDING」检查，任一违规则整体拒绝，不会留下半删除的版本树（`ErrVersionIsActive` / `ErrVersionPending`）。
- **标记与清理分离**：标记 `Deleting` 只改状态机；索引丢弃、`VersionDocList`/`DocStore` 清理与元数据移除仍由 `DeleteVersionCoordinator` 异步、幂等地完成，与既有流程一致。
- **版本数据互不影响**：每个版本持有独立的 MVCC 文档记录与 HNSW 索引，因此删除任一版本（中间版本或前置版本）都不会改变其余版本的查询结果。
- **幂等**：对已 `Deleting` 的集合重复调用是空操作；`ANCESTORS` 作用于已是基底的版本返回空列表。
- **fail-closed**：无法识别的 `mode` 返回 `INVALID_ARGUMENT`，不静默降级为 `SUBTREE`。
- **无悬空父指针**：`SINGLE` 重接时若目标版本的父版本已不存在或正在删除，子版本直接成为根（而非挂到一个即将消失的版本上）；`ANCESTORS` 遇到断链也会把目标版本的 parent 归零。
- **幸存者不得为 PENDING**：`SINGLE` 被重挂的子版本、`ANCESTORS` 中被保留的目标版本，若其父版本在待删集合中且自身仍 PENDING，则整体拒绝——它的存储写入与崩溃恢复都要读父版本的 `VersionDocList`，而父版本正被异步清理，否则会静默丢失继承来的文档集。
- **按可见性回收 MVCC 记录**：文档记录是增量写入、按版本回溯读取的，因此清理版本时只回收**不再被任何存活版本读取**的记录（锚点是最小的存活更高版本）。这是 `SINGLE` / `ANCESTORS` 保留后代的前提：否则存活版本会静默丢文档或读到回退值。代价是删除中间/前置版本不会立即释放这些文档的空间（它们仍是活跃数据），完全释放发生在删除无存活更高版本的链尾时。
- **版本自身数据随删随清**：索引（内存条目 + 墓碑、vecstore 侧对象、磁盘 `.index`/`.ids`/`.mem`）与每版本文档布隆过滤器（`bloom-version/<kb>/<version>.bloom`）都随版本删除回收；chunk 向量（L2）不随版本立即删除，由 chunk GC 按"最新版本是否引用"周期性回收。

响应回传本次实际标记删除的版本 ID 清单（`deleted_version_ids`），供控制台提示被波及的版本。控制台（`web/app.js`）在版本节点上提供「设为基底」按钮，并在删除弹窗中显式选择上述范围。

### 1.3 方案调研与选型（含决策记录）

#### 1.3.1 候选量化能力（本地代码核实）

项目已集成 Faiss **1.9.0**（faiss/faiss/Index.h:19-21）。其 HNSW 家族在 `faiss/faiss/IndexHNSW.h` 声明了四种存储变体（:122-155）：

| 变体 | 载荷存储 | 训练需求 | 说明 |
|---|---|---|---|
| `IndexHNSWFlat` | float32 原向量 | 无 | 现状 |
| `IndexHNSWSQ` | 标量量化码 | `QT_8bit` 等固定位宽需一次 `train`；`QT_fp16`/`QT_bf16` 免训练 | 构造 `(d, QuantizerType, M, metric)`（IndexHNSW.h:144-151） |
| `IndexHNSWPQ` | 乘积量化码 | 需一次 `train`（学码本） | 构造 `(d, pq_m, M, pq_nbits=8, metric)` + `train` override（:130-139） |
| `IndexHNSW2Level` | 两级码 | 需训练 | 见 :155-169，暂不采用 |

`ScalarQuantizer::QuantizerType` 提供 `QT_8bit`（1 字节/维）、`QT_8bit_uniform`、`QT_fp16`（2 字节）、`QT_bf16`、`QT_8bit_direct(_signed)` 等（faiss/faiss/impl/ScalarQuantizer.h:27-40）。**关键特性：量化状态（码本/逐维范围）随 `write_index`/`read_index` 单文件持久化，Load 即恢复、无需外部重训**（与现有 `faiss::write_index/read_index` 序列化格式一致）。

#### 1.3.2 独立开源库调研结论（外部事实，2026-09 调研快照）

用户要求评估"引入独立开源库做 HNSW 量化"的可行性。逐项结论（仓库页/发布元数据核证，URL 见附录）：

| 库 | 许可证 | 维护状态 | 量化机制 | 对本项目适配性 |
|---|---|---|---|---|
| hnswlib（nmslib） | Apache-2.0（仓库页） | v0.9.0（2026-03 发布），header-only | **上游无量化**，仅原始 float32 距离 | 不满足需求 |
| DiskANN（microsoft） | MIT | 经典 C++（Vamana+PQ）在 `cpp_main` 分支且 README 声明**不再积极维护**；主线已转 Rust（DiskANN3），图为 Vamana 非 HNSW | PQ/OPQ 压缩强 | 图算法与维护状态双不匹配 |
| USearch（unum-cloud） | Apache-2.0 | v2.26.2（2026-08-31），活跃 | **仅免训练标量降位**（bf16/f16/i8/Float8-MX），无 PQ/码本；自有二进制格式 + mmap | 需整体替换 HNSW 层与序列化，迁移成本高、且无码本级压缩 |
| Faiss 内建 `IndexHNSWSQ/PQ` | MIT | 随 Faiss 1.9.0（已集成） | SQ（1-2 字节/维，可免训练）+ PQ（码本，训练一次）+ 与现有 `write_index/read_index` 同格式 | **零新依赖、序列化与边车格式不变** |

> 注：上表中第三方维护状态/版本号来自公开仓库页与 release 元数据（2026-09 快照），属厂商/仓库自述，未跑基准验证；本地代码核证部分见 1.3.1。

#### 1.3.3 选型判定与决策记录

**决策 1（路线）**：量化粗筛采用 **Faiss 内建 `IndexHNSWSQ` / `IndexHNSWPQ`**，不引入独立开源库。

- 理由：hnswlib 无量化；DiskANN C++ 分支停维护且图为 Vamana；USearch 只有标量降位、且需整体替换后端与序列化；Faiss 内建方案零新依赖、与现有 `write_index/read_index`/`.ids` 边车/分批 AddChunks 流程完全兼容，改动集中在 `hnsw_index.cpp` 与配置通道。
- 权衡（显式说明）：用户原倾向"允许引入新独立库"，经调研其选项均不划算（见 1.3.2 表），已与用户确认改为主推 Faiss 内建方案；若未来需要比 Faiss 更轻量的部署形态，可再评估 USearch，属后续独立议题。

**决策 2（磁盘向量）**：复用现有 RocksDB chunk store 作为全精度原向量来源，**不压缩、不新建磁盘存储**。

- 理由：它已是权威全精度存储（1.2.1），rerank 只需按候选 `chunk_id` 批量读取；避免新存储的格式、GC、生命周期与删除一致性问题。
- 代价（写入设计）：LSM 随机读多条候选的读放大与延迟预算（见 2.3.5、2.5）。

**决策 3（量化形态）**：量化用于**内存粗筛召回**，不出最终分；最终 top-k 由磁盘全精度 rerank 产出。量化后的内存索引是"有损召回器"，与"无损 Flat 直接出结果"的本质区别见 1.4。

**决策 4（索引管理）**：LRU 改为分级存储——内存只放每版本**量化粗筛器**，淘汰语义保留 LRU（lastAccess + refcount 保护），换出对象与记账口径改为粗筛器（见第 3 章）。

### 1.4 目标架构总览

```
                         ┌──────────────────────────────┐
                         │      IndexManager (Go)        │
                         │  分级存储管理(第3章)            │
                         │ 内存层: 每版本量化粗筛器缓存      │
                         │ 磁盘层: <versionID>.index 文件  │
                         └──────────────┬───────────────┘
                                        │ Search(kb,ver,vec,k)
                         ┌──────────────▼───────────────┐
                         │     vecstore Search 内部       │
                         │ ① 内存量化HNSW粗筛 → top-N候选  │
                         │ ② 按候选 chunk_id 批量读原向量   │
                         │ ③ 精确距离 rerank → 最终 top-k  │
                         └───────┬──────────────┬───────┘
                                 │              │
                  ┌──────────────▼─────┐   ┌────▼──────────────────┐
                  │ 内存: 量化粗筛器      │   │ 磁盘 RocksDB chunk store│
                  │ (IndexHNSWSQ/PQ:    │   │ (全精度 float32,分key,  │
                  │  图边+量化码+码本)    │   │  内容寻址,不压缩)        │
                  └────────────────────┘   └───────────────────────┘
```

系统将并存两条检索路径（按 KB 量化配置切换，见 2.1.1、2.4）：

| 维度 | OFF（现状，默认/兼容路径） | ON（新路径） |
|---|---|---|
| 内存驻留物 | 完整全精度 HNSW（现状条目） | 量化粗筛器（图边 + 量化码） |
| 查询 | 单段：内存索引直接出 top-k | 两段：粗筛 → 磁盘原向量 rerank |
| 内存记账 | `4B×d×n`（现状） | 粗筛器字节口径（新，第 3 章） |
| 换出 | 丢整份索引（现状语义） | 丢粗筛器（小、重载成本低） |
| 磁盘精排 | 不参与 | RocksDB 全精度批量读取 |
| 存量 `.index`(Flat) Load | 兼容（现状） | 兼容（`Load` 动态识别类型） |

---

## 2 两段式检索设计

本章设计第 1.4 节新路径的三个环节：内存量化粗筛（2.1-2.2）、磁盘全精度读取（2.3）、以及打通两者的配置/接口通道（2.4）与规模估算（2.5）。

### 2.1 量化粗筛器设计

#### 2.1.1 量化配置模型

量化配置为 **KB 级、创建后不可变**属性（与 `index_type`/`similarity` 同语义，见 Stratum_设计文档v11.md:229-244：变更是换 embed 配置级别的操作，需新建 KB 迁移）。配置模型草案：

```text
QuantizerConfig {
  type: OFF | SQ8 | SQ_BF16 | SQ_FP16 | PQ
  pq_m:      int    // PQ 子空间数（子向量数），type=PQ 时生效
  pq_nbits:  int    // PQ 每子空间码位数，默认 8
}
```

| type | 载荷字节/维 | 训练需求 | Faiss 构造要点 | 备注 |
|---|---|---|---|---|
| `OFF` | 4（现状） | 无 | `IndexHNSWFlat` | 默认；行为=现状单段 |
| `SQ8` | 1 | 一次（首包） | `IndexHNSWSQ(d, QT_8bit, M, metric)`；COSINE 场景实现时验证 `QT_8bit_direct_signed` | 压缩 4×，误差中等 |
| `SQ_BF16` | 2 | 免训练 | `IndexHNSWSQ(d, QT_bf16, M, metric)` | 压缩 2×，误差小 |
| `SQ_FP16` | 2 | 免训练 | `IndexHNSWSQ(d, QT_fp16, M, metric)` | 压缩 2×，误差小 |
| `PQ` | `pq_m×nbits/8` | 一次（首包，学码本） | `IndexHNSWPQ(d, pq_m, M, pq_nbits, metric)` | 压缩幅度最大；码本质量依赖训练样本（2.1.2） |

**构造替换**：`HNSWVectorIndex::index_` 的创建点（现为 `make_unique<faiss::IndexHNSWFlat>(dim, kM, metric)`，vecstore/src/hnsw_index.cpp:84）改为按配置分支构造；类成员类型放宽为 `std::unique_ptr<faiss::Index>`（或 `faiss::IndexHNSW`），Load 时按文件实际类型恢复（2.1.3）。量化类型与 metric 的匹配原则：EUCLIDEAN 对 SQ 最友好；COSINE/IP 用 8bit 时召回损失更大，必要时以 `efSearch` 或候选数补偿（2.2），**具体匹配由实现阶段基准验证后定**（faiss 源码为权威）。

#### 2.1.2 训练时机与批量 AddChunks 的衔接

现有构建是"一次 `Build` 调用 + 一次或多次 `AddChunks`"的分批追加（gRPC 单消息 ≤ 4 MiB 预算，internal/index/impl.go:554-594）。量化训练必须发生在首次 add 之前：

- **免训练类型**（`SQ_BF16`/`SQ_FP16`）：无需改动时序，首个批次直接 add。
- **需训练类型**（`SQ8`/`PQ`）：**首个批次到达时用该批向量训练一次（全量传入，faiss 的 `train` 内部决定是否抽样），此后所有批次只 add 不重训**。每个版本的量化器独立（版本级索引本来就各自全量重建），不存在跨版本共享码本。
  - 实现验证项：`IndexHNSWPQ` 显式声明 `train` override（faiss/faiss/IndexHNSW.h:138）；`IndexHNSWSQ` 的 8bit 训练经基类路径完成——具体是否/如何显式调用 `train` 以 faiss 1.9 源码与最小单测为准（属实现阶段第一件事，见第 4 章路线 ①）。
- **首包训练的代表性局限**：PQ 码本质量依赖训练样本覆盖真实分布；分批场景下首个 gRPC 批次（≈2 MiB 载荷）通常已含数千向量，对 8bit SQ 的逐维范围统计足够；对 PQ，若基准显示召回不足，对策为（按序选一）：
  1. 增大首批切分（训练批 ≥ 配置的 `train_min_batch`，实现阶段基准后定默认值）；
  2. 退级建议：该 KB 用 `SQ_BF16`/`SQ_FP16`（免训练、无此问题）；
  3. 未来如需独立训练批，扩 `TriggerBuild` 流程"先训练后添加"，本轮不引入（避免改动构建协议）。

#### 2.1.3 Load 兼容与粗筛分语义

- **Load 放宽类型检查**：现有 `dynamic_cast<faiss::IndexHNSWFlat*>` 失败即报错（vecstore/src/hnsw_index.cpp:197-205）。改为按 `read_index` 返回对象的实际类型识别并恢复为对应的运行态（Flat → 单段精确路径；SQ/PQ → 两段粗筛路径）。
- **磁盘文件自描述**：码本/量化状态随 `write_index`/`read_index` 持久化，`Load` **不依赖外部量化配置**即可恢复——因此加载路径天然兼容存量 Flat 文件与将来不同量化类型的文件。
- **配置与文件类型不一致**：KB 量化配置不可变，理论上不存在；若磁盘文件被手工替换导致类型与 KB 配置不符，Load 以文件实际类型为准并记告警日志（防御性，不阻塞）。
- **粗筛分语义**：量化粗筛的距离/分数**仅用于候选排序**，不再作为对外分数返回；对外分数一律来自 rerank 精确计算（2.2），与现状 Flat 路径的分数口径一致。

### 2.2 候选召回与 rerank 参数

**候选数 top-N**（粗筛输出）与最终 `top_k`（对外返回）解耦：

```text
N = clamp(ceil(top_k × candidate_multiplier), min_candidates, max_candidates)
默认: candidate_multiplier = 8, min_candidates = 16, max_candidates = 4096
```

- N 由查询路径携带（`SearchIndexRequest` 扩展，见 2.4），默认值可被 KB/请求覆盖；上限 `max_candidates` 同时是磁盘读预算闸门（2.3.5）。
- 召回兜底逻辑：粗筛是近似排序，真近邻可能落在 N 之外——**N 是召回与磁盘 IO 的唯一旋钮**，数值由实现阶段离线基准（真实 embed 分布）标定，本节不给虚构数字，只定机制。

**评分与阈值语义**：最终 `score` = rerank 的**精确距离分**，沿用现状口径（COSINE/IP = 归一化后内积，EUCLIDEAN = 负平方 L2，见 vecstore/src/hnsw_index.cpp:33-53, 140-145）。Go Query 链路的 topK×N 过滤/聚合与相似度阈值过滤（若有）位于 `IndexManager.Search` 之上（internal/index/index.go:53-59），语义不变——只是分数来源从（现状的）精确/（量化路径的）近似统一为精确，与现状 Flat 行为一致。

**归一化归属**：

- 构建侧（现状逻辑保留）：COSINE 时向量在入索引/入量化器前先 L2 归一化（hnsw_index.cpp:98-101），即**量化码对应归一化向量**；
- 磁盘侧：RocksDB 存的是 embed 服务原始输出（**未归一化**）。因此 rerank 计算 COSINE 时需对磁盘原向量先归一化再求内积；EUCLIDEAN 直接用原向量（负平方 L2）。实现为读盘后按 metric 批量归一化一次。

### 2.3 磁盘读取与编排

#### 2.3.1 候选 `chunk_id → RocksDB key` 映射

候选是 C++ 索引内存的 `chunk_id`（`id_to_chunk_id_`），而 RocksDB key = `encodeKey(kb_id, chunk_id)`（4B 大端 kbID 长度前缀 + kbID + chunk_id，internal/chunkstore/grpc_client.go:68-98），编码规则**目前只在 Go 侧实现**。方案对比：

| 方案 | 做法 | 评价 |
|---|---|---|
| (a) C++ 侧复制编码规则（推荐） | Search 已带 `kb_id`，C++ 内实现同款 `encodeKey`，候选即算即读 | 规则简单固定；风险是双实现漂移 → 用跨语言一致性单测锁定（见第 4 章路线 ①） |
| (b) 索引内持久化 key 映射 | 构建时把完整 key 一并写入索引/边车，候选直接用 | 每 chunk 冗余存储完整 key；边车格式变复杂；收益低 |
| (c) 构建消息传 key | `BuildIndexRequest` 的 chunk 增带 key | 消息体积 +，且构建与查询解耦性变差 |
| (d) Go 侧两段编排 | 候选经 RPC 回 Go，Go 算 key 再读向量 | 属"显式两阶段 RPC"备选方案，本轮未采用（用户已选 Search 内部自动） |

**采用 (a)**，并在实现阶段以 Go `EncodeKey` 与 C++ `encodeKey` 的交叉用例防止漂移。

#### 2.3.2 批量读取接口

`ChunkStorage` 新增批量读：

```text
ReadMulti(keys) → { found: key → 全精度 vector; missing: [key] }
```

`RocksDBChunkStorage` 以 RocksDB `MultiGet` 实现（单次往返、内部并行）；排序语义由调用方（rerank 按候选序）负责。该接口进程内被 rerank 使用，**不新增 gRPC RPC**（rerank 在 vecstore 内部完成，见 2.3.4）。

#### 2.3.3 缺失容错

正常生命周期下索引内的 chunk 在构建后不会被单删（GC 只删"无版本引用"的孤儿 chunk，KB/版本删除会先 Evict/Discard 对应内存索引并置墓碑，查询不会命中已删数据，见 internal/index/impl.go:110-119 的墓碑机制与 Stratum_设计文档v11.md 第 226 行附近）。防御性策略：`ReadMulti` 的 `missing` 项跳过并计指标/告警日志，不阻断其余候选的 rerank。

#### 2.3.4 编排落点（Search 内部自动完成）与改动清单

用户已确认编排在 **vecstore `Search` 内部自动完成**（粗筛 → 读盘 → rerank → 返回最终 top-k），Go 侧调用链与接口不变。这要求打破 1.2.1 记录的 service 隔离，改动清单：

1. **注入 ChunkStorage**：`VectorIndexServiceImpl` 与 `VecstoreGrpcServer` 装配处（grpc_service.h:63-103、grpc_service.cpp:252-255）让索引侧获得 `ChunkStorage*`；`GetOrCreateLocked` 创建索引实例时传入（grpc_service.cpp:118-125）。
2. **Search 的锁模型**：现状 Search 在索引表上查 `indexes_`（grpc_service.cpp:163-192 区域）。两段式 Search 含磁盘 IO，**不得在 `mu_` 持锁期间执行**——设计：锁内取索引（生命周期保护，见下），锁外执行粗筛+读盘+rerank。并发删除（Evict/Reset/Discard 对应 RPC）与在途 Search 的竞态，由索引引用的生命周期保护解决（实现建议：`indexes_` 改存 `shared_ptr<VectorIndex>`，Search 持临时 `shared_ptr` 短引用；与 Go 侧 refcount 保护是同一思路的两端实现）。
3. **Rerank 引擎归属**：放在索引实现之外的可复用单元（读盘批量取数 + 精确距离排序），由 Search 编排；`HNSWVectorIndex` 保持"只懂 faiss 索引"，避免与 ChunkStorage 耦合过深——或由实现评估直接内聚亦可，约束是**接口签名不变、对外语义不变**。

**备选方案（仅记录，不采用）**：显式两阶段 RPC（`SearchCandidates` → Go 算 key → `FetchVectors`+Go 侧 rerank）：灵活但多一跳、Go 侧要复制候选语义与批量读，改动面大，本轮不做。

#### 2.3.5 磁盘读预算

单查询磁盘读量 ≈ `N × 4 × d` 字节（N 条 float32 向量）。示例见 2.5；`max_candidates = 4096` 即单查询最多读 `4096 × 4 × d`（d=768 时 ≈ 12.6 MB，极端上限）。`candidate_multiplier` 的默认标定目标：让 N 落在"召回充分且磁盘读量可控"区间，由基准定（第 4 章路线 ④）。

### 2.4 配置与接口通道

**vecstore.proto 扩展点**（草案字段名，以接口评审为准）：

```protobuf
// BuildIndexRequest 新增
enum QuantizerTypeProto { QUANTIZER_OFF = 0; QUANTIZER_SQ8 = 1;
                          QUANTIZER_SQ_BF16 = 2; QUANTIZER_SQ_FP16 = 3;
                          QUANTIZER_PQ = 4; }
QuantizerTypeProto quantizer = 5;   // 默认 QUANTIZER_OFF
int32 pq_m = 6;                     // PQ 时生效
int32 pq_nbits = 7;                 // PQ 时生效，默认 8

// SearchIndexRequest 新增
int32 candidate_n = 5;              // 粗筛候选数；0 表示用服务端默认(2.2)
```

- **透传路径**：KB 元数据（Go 侧 `knowledgebase.proto` 的 KB 配置）新增 KB 级不可变量化配置 → `CreateKnowledgeBase` 建库时写入 → `IndexManager.TriggerBuild` 构建时读取并随 `BuildIndexRequest` 下发（cmd/stratum/main.go 数据源接线处）。`Load` **不需要**量化配置（文件自描述，2.1.3），因此磁盘恢复/启动 reconcile 路径零改动。
- **默认路径兼容**：未配置（`OFF`）时构造 `IndexHNSWFlat`，行为与现状完全一致；存量 KB、存量 `.index` 文件不受影响。
- **候选数默认值归属**：`candidate_multiplier`/`min`/`max` 作为 vecstore 服务端可配置默认（对标现有 `kEfConstruction` 等硬编码常量的处理方式，hnsw_index.cpp:29-31），并允许 `SearchIndexRequest.candidate_n` 覆盖。

### 2.5 内存 / 磁盘 / 延迟 / 召回估算

示例参数：`d = 768`（常见 embedding 维度）、HNSW `M = 32`（hnsw_index.cpp:31）。

**单版本载荷内存（每向量字节）**：

| 配置 | 载荷字节/向量 | n=10 万 | n=100 万 | 相对现状 |
|---|---|---|---|---|
| 现状（Flat float32） | 3,072（4×768） | ≈ 307 MB | ≈ 3.07 GB | 1× |
| `SQ_BF16`/`SQ_FP16` | 1,536 | ≈ 154 MB | ≈ 1.54 GB | 0.5× |
| `SQ8` | 768 | ≈ 77 MB | ≈ 768 MB | 0.25× |
| `PQ`（`pq_m=96`, 8bit） | 96 | ≈ 9.6 MB | ≈ 96 MB | ≈ 1/32 |

**图边开销（与量化无关，两路径相同）**：HNSW 图每个节点的邻居表随度数动态增长，经验上约数百字节/节点（`M=32` 时通常 ~200-600 B，与实现与数据维度相关）。n=100 万时约 0.2-0.6 GB。量化后**图边成为内存主导项**——分级存储的记账口径必须同时计入图边与量化码（第 3 章），不能只看载荷。

**磁盘读（rerank 一次查询）**：设 `top_k=10`、默认 `candidate_multiplier=8` → N=80：读 `80 × 3072 B ≈ 246 KB`（d=768）。RocksDB LSM 随机读 + block cache 命中率决定实际 IO；命中热缓存时近零，未命中时按盘型（NVMe/HDD）数 ms 级。这构成与现状"纯内存单段"的延迟增量，是两段式的主要代价。

**召回(阶段④ 实测回填,2026-09;方法与局限见附录 D)**：合成数据下量化类型 × 候选 N 的 top-10 召回(对照暴力精确,rerank 用全精度):

| 索引 | cand=32 | 64 | 128 | 256 | 平均搜索耗时(top128) | 载荷/图边(8000 点) |
|---|---|---|---|---|---|---|
| HNSW Flat | 0.860 | 0.860 | 0.860 | 0.860 | 0.22 ms | 2.0 MB / 2.2 MB |
| SQ8 | 0.878 | 0.878 | 0.878 | 0.878 | 0.30 ms | 0.5 MB / 2.2 MB |
| SQ_BF16 | 0.873 | 0.873 | 0.873 | 0.873 | 0.39 ms | 1.0 MB / 2.2 MB |
| SQ_FP16 | 0.895 | 0.895 | 0.895 | 0.895 | 0.34 ms | 1.0 MB / 2.2 MB |
| PQ(m16,8bit) | 0.559 | 0.694 | 0.742 | 0.753 | 0.28 ms | 0.13 MB / 2.2 MB |

方向性结论(合成数据,勿外推为生产值):

- **SQ 类(8bit/bf16/fp16)召回与全精度 HNSW 相当**(此数据集上差异为数据噪声级),且候选 N ≥ 32 即进入平坦区——SQ 用默认 `candidate_multiplier=8`(top10 → N≈80)已足够;
- **PQ 召回最低且对 N 敏感**(0.559@32 → 0.753@256):选 PQ 的用户应上调候选数(如 multiplier≥16 或按 KB 配 `candidate_n`)换取召回,或以 `pq_nbits`/`pq_m` 增大码本表达力;
- **图边开销(实测 2.2MB)超过量化载荷**(SQ8 0.5MB、PQ 0.13MB)——实测印证"量化后图边成为内存主导项"(本节省略号前论断),分级记账必须含图边;
- 平均搜索耗时各类型同量级(0.2-0.4 ms/查询,top128,d=64),量化未带来延迟红利(图遍历仍是主导),两段式延迟主要来自磁盘读原向量(见上文"磁盘读"段)。

> 局限：上述数字来自**合成 24-簇高斯数据**(d=64、簇内 σ=0.3、n=8000、200 查询、seed 固定、EUCLIDEAN),数据"偏易";真实 embed 分布下的绝对数值与类型排序需按附录 D 的固定方法以生产数据复跑标定。

**端到端查询延迟(状态机与两段式落地后实测,2026-09;合成数据 d=64、n=6000、300 查询、top10、候选 N=80、RocksDB 热 cache、单线程串行)**：

| 路径 | avg/查询 | 相对 OFF-Flat |
|---|---|---|
| OFF-Flat(量化前基线,单段全精度) | 0.27 ms | 1.0× |
| SQ8(两段式) | 0.71 ms | 2.6× |
| SQ_FP16(两段式) | 0.71 ms | 2.6× |
| PQ(m16,b8)(两段式) | 1.00 ms | 3.7× |
| SQ_BF16(两段式) | 1.93 ms | 7.2× |

解读(与量化前对比):

- 两段式相对 OFF 的延迟增量来自**候选放大(N=80)的磁盘全精度读 + 精确 rerank**,而非粗筛本身——粗筛(图导航+量化距离)与 Flat 同量级(见附录 D 的 search_ms);
- **SQ_BF16 在本机走软件 bf16 慢路径,延迟异常高**:召回与 SQ_FP16 相当而延迟明显更差,**选型建议回避 SQ_BF16**,优先 SQ8 / SQ_FP16 / PQ;
- 每实例生命周期读锁开销可忽略(OFF 基线同样持锁,已含该成本);
- 口径为热 cache、单线程;生产绝对值需复测,方法固定于 `vecstore/test/latency_bench_test.cpp`(DISABLED,显式运行)。

### 2.6 索引对象生命周期状态机与并发模型

量化器的写（构建/加载/删除）与读（查询）处于不同生命周期阶段；原实现无显式状态与对象级互斥，Search 锁外执行、Reset/Load/AddChunks 锁内改写同一对象，存在竞态（第 5 章风险 #8）。本方案在 **C++ 每索引实例**显式化生命周期、把读/写按状态分门，并配**每实例读写锁**根除竞态。

**状态集**：

| 状态 | 含义 | 可读（Search） | 可写 |
|---|---|---|---|
| `EMPTY` | 构造后 / Reset 后，无内容 | 返回空结果（兼容现状） | Build、Load |
| `BUILDING` | 首个 Build 开始 → Save 成功 | **拒绝**（"index building"，区别于空） | AddChunks、Save、Reset |
| `READY` | Save 成功或 Load 完成 | 执行（含两段式 rerank） | Save（幂等）、Load（替换）、Reset；**不可 AddChunks** |

**转换表**（转换与操作在每实例**写锁**内完成）：

| 现态 | 操作 | 新态 |
|---|---|---|
| EMPTY | Build（首包）/ Load（成功）→ | BUILDING / READY |
| BUILDING | AddChunks | BUILDING |
| BUILDING | Save（成功，构建完成信号）| READY |
| BUILDING | Reset（中止） | EMPTY |
| READY | Build（重建）→（内部 Reset）| EMPTY → BUILDING |
| READY | Save / Load（成功，整体替换）| READY |
| EMPTY/READY | Reset | EMPTY（幂等） |
| EMPTY | Save | 拒绝（无索引，现状语义）|
| READY | AddChunks | 拒绝（构建已关闭）|
| BUILDING | Load | 拒绝（先 Reset）|

**锁模型**：每实例一把 `std::shared_mutex`——读操作（`Search`/`SearchCandidates`/`SearchWithRerank`/`EstimatedMemoryBytes`）持**读锁贯穿**（含 rerank 的磁盘读段）；写操作（`Build`/`AddChunks`/`Save`/`Load`/`Reset`）持**写锁**并在锁内完成状态转换。service 的 `mu_` 仅保护 `indexes_` map——索引包装器实例进程级驻留（C++ 侧无 RPC 删除 map 条目），裸指针安全；并发保护全部下沉实例内，锁与状态同层，无 TOCTOU。

**错误映射**：service 找不到 `(kb,version)` → `NOT_FOUND`（现状）；`EMPTY` 的 Search → 空结果；`BUILDING` 的 Search → `FAILED_PRECONDITION("index building")`。Go 查询路径由 READY 门卫驱动（正常流程只会命中 READY）；直连/竞态窗口命中 `BUILDING` 时，该错误经 `IndexManager.Search` 包装后按查询失败返回调用方——**Go 侧无需改动**（C++ 由崩溃/错位结果变为确定性错误，是纯收益；若将来需要可把该错误映射为可重试语义，属后续增强）。

**与 Go 门卫分工**：C++ 状态机保证**对象内**读/写合法性；Go IndexManager 的 `loaded/loading` + 版本 READY 状态管**逻辑可用性与 LRU**；两端独立、语义互补。构建完成信号统一为 **Save**（Go 流程在构建结束落盘后报 READY，与 C++ `BUILDING→READY` 对齐）。

---

## 3 分级存储管理设计（IndexManager 内存管理层改造）

第 2 章把"单版本索引"的内存形态从完整全精度索引改成了量化粗筛器。本章把这一变化落到 Go 侧索引管理器：LRU 淘汰机制改造为**分级存储**——内存层只驻留各版本粗筛器，全精度向量永驻磁盘，淘汰语义保留 LRU。

### 3.1 现状 LRU 机制梳理

按 internal/index/impl.go 与接口 internal/index/index.go 的现状（2026-09 工作区）：

| 机制 | 现状实现 | 位置 |
|---|---|---|
| 内存条目 | `loaded map[(kb_id,version_id)]*loadedIndex{refCount,lastAccess}`；条目 = 一份完整全精度索引（vecstore 侧内存） | impl.go:97-108, 126-129 |
| 容量阈值 | 条目数 ≥ `LRUCapacity` **或** 字节账 `loadedBytes` > `MemoryThresholdMB`（MiB）即触发淘汰 | impl.go:29-31, 62-68, 674-680 |
| 淘汰算法 | `makeRoomLocked`：循环找 `refCount==0` 中 `lastAccess` 最旧者，删条目并扣账 | impl.go:674-703 |
| 记账口径 | 向量载荷字节 `4×d×n`（构建时 `collectChunkBatches` 累计），**不含 HNSW 图边** | impl.go:569-594 |
| 尺寸边车 | `<dataDir>/index/<kbID>/<versionID>.index.mem` 构建完成时写、Load 时读回恢复账本 | impl.go:883-899, 516-552 |
| 查询加载 | `Search` 未命中 → `loadFromDisk` 整份 Load（vecstore `Load` RPC）→ `acquire`(refcount++) → RPC Search → `release` | impl.go:200-244, 516-552 |
| 并发等待 | `loading` 单飞 + 有界轮询等待（load_wait_timeout） | impl.go:599-656 |
| 删除 | `Evict`/`EvictByKB`/`Discard`/`DeleteFilesByKB` + `deletedKBs/deletedVersions` 墓碑防 Load 复活，均幂等 | impl.go:740-766, 906+, 775+ |

现状的语义要点（与第 2 章新模型的关键差异）：

1. **内存条目 = 精确结果的全部依赖**：查询质量完全依赖内存条目（全精度索引）本身；换出 = 丢掉精确检索能力，冷查询要整份 Load 一个大文件才能恢复精确检索。
2. **记账是载荷口径**：`4×d×n` 只算向量载荷，不算 HNSW 图边与结构开销，是"下限估算"而非真实内存。
3. **换出重载成本高**：Load 一份全精度 HNSW 文件 = 读入 n×4×d 字节原向量 + 重建图结构所需反序列化。

### 3.2 分级内存模型（目标）

| 层 | 内容 | 生命周期 | 查询角色 |
|---|---|---|---|
| L0 内存热层 | 每 `(kb, version)` 一份**量化粗筛器**（图边 + 量化码 + 码本/SQ 状态，常驻 vecstore 进程内存） | LRU 准入/淘汰（3.3） | ① 粗筛出候选 |
| L1 磁盘冷层 | `<versionID>.index`（量化 HNSW，含码本）+ `.ids`/`.mem` 边车 | 磁盘保留策略（`IndexRetentionCount`）裁剪；被裁版本 `RebuildIndex` 重建 | 粗筛器换出后的来源（Load 重载） |
| L2 永久层 | RocksDB chunk store：全精度 float32 原向量（内容寻址、按 key） | KB/版本删除时清理 | ② 按候选批量读原向量 → ③ rerank |

模型要点：

- **任何版本都不再把全精度向量整体驻留内存**（现状是索引=原向量+图，全精度在内存；新模型把原向量从内存索引中剥离到 L2）。
- **内存压力构成改变**：单版本内存从 `4×d×n`（载荷）变为"图边 + 量化码 + 码本常数"。d=768、n=100 万时载荷从约 3 GB（Flat）降到约 0.1-0.8 GB（量化，2.5 节），而图边约 0.2-0.6 GB 成为主导项——**同样字节预算下可驻留的版本数显著上升**。
- **查询质量与内存驻留解耦**：换出粗筛器只影响"候选召回是否走冷路径（多一次磁盘 Load）"，不影响最终结果精度（rerank 用 L2 全精度向量）——这是与现状最本质的行为差异。

### 3.3 LRU 调度改造

目标：**保留 LRU 语义与现有代码骨架，只改"条目内容物"与"记账口径"**，最小化对 `makeRoomLocked`/`acquire`/`loadFromDisk`/删除流程的改动。

1. **淘汰语义不变**：`lastAccess` + `refCount==0` 的最久未用淘汰算法（impl.go:674-703）原样保留。`Search`/`Warmup` 命中刷新 `lastAccess`、refcount 保护的语义（index.go:54-59）不变。
2. **换出对象解释变化**：`loaded` 条目对应的 vecstore 侧对象从"完整全精度索引"变为"量化粗筛器"。Go 侧代码结构（map、删除、扣账）无需改动——差异在条目大小与重载成本。
3. **记账口径改造**（重点）：
   - 现状：`sizeBytes = Σ 4×len(v)`（impl.go:569-594），写 `.index.mem`，Load 读回（impl.go:883-899）。
   - 新口径：**粗筛器内存 = HNSW 图边 + 量化码载荷 + 码本/SQ 状态常数**。数据来源二选一：
     - (a) 推荐：**vecstore 侧报告**——构建完成/`Load` 时由 C++ 按实际索引结构估算内存字节，经 RPC/响应带出或落盘，Go 侧直接记账（避免双端公式漂移；C++ 内估算函数与 faiss 内部结构对齐，实现阶段校准）；
     - (b) 回退：Go 侧公式估算（`n ×(码字节+每节点图边经验值)+ 常数`），与 2.5 节口径一致，精度低于 (a)。
   - `.index.mem` 边车机制（写入时机、Load 恢复）不变，只换数值来源。**OFF 路径沿用旧口径 `4×d×n`**（存量 KB 账本与行为不变）；ON 路径写新口径——边车值与文件实际类型天然对应（写入方知道量化配置），无歧义。
4. **容量配置语义再标定**：`LRUCapacity`（条目数）含义不变；`MemoryThresholdMB` 单位不变但单条目数值大降 → **默认阈值需按图边主导的新口径重新标定**（实现阶段任务，给配置迁移说明与示例值，见第 5 章风险）。
5. **加载路径代价下降**：冷查询 `loadFromDisk` 读的是量化粗筛器文件（远小于全精度文件），重载成本低——这使 LRU 换出更"便宜"，是分级模型可行性的支撑（2.1.3：Load 免外部配置、文件自描述）。
6. **无需改动的部分**：`loading` 单飞与有界等待、`load_wait_timeout_ms` 超时语义、`ErrIndexNotReady`/`ErrIndexLoadTimeout` 错误映射、启动 reconcile 的磁盘事实判定。

### 3.4 交互与兼容

- **删除路径**：`EvictByKB`/`Discard`/`DeleteFilesByKB` 与墓碑（impl.go:740-766, 906+, 775+）操作对象泛化为"内存条目"，删除语义、幂等性、防复活约束**不变**；KB/版本删除同时清理 L2（chunk store）的既有流程（Stratum_设计文档v11.md:226, 445）不变。
- **启动恢复**：`IndexExists`/reconcile 以磁盘文件事实判定 READY（vecstore.proto ExistsIndex 语义），不依赖量化配置——新文件、旧 Flat 文件一视同仁。
- **WarmupVersion**：语义"Load 进内存但不切活跃"不变；预热代价因粗筛器变小而下降。
- **旧索引文件兼容**：存量 Flat `.index` + 旧口径 `.index.mem` 在 OFF 路径完全按现状加载与记账；若 ON 路径 KB 误遇 Flat 文件，按 2.1.3 以文件实际类型运行并告警。
- **磁盘保留与重建**：`IndexRetentionCount` 裁剪与 `RebuildIndex`（设计文档v11:237-242）机制不变；被裁版本重建 = 重新 train + add + save（含 2.1.2 的训练时序）——重建回归测试列入第 4 章路线 ⑤。
- **升级/迁移路径**：配置按 KB 新增（新建 KB 生效），存量 KB 保持 OFF → **无存量数据迁移**。存量数据均为测试数据，可删除重建；不对旧文件/旧边车设兼容验收（Load 对旧 Flat 的读取仅为文件自描述机制的零成本防御）。

---

## 4 实施路线

按依赖顺序分五个阶段，每阶段给出验证方式与完成标准。本文档为设计稿，实施从阶段 ① 开始。

### 阶段 ①：C++ 最小闭环（量化粗筛 + 候选批量读 + rerank）

改动面：vecstore 内部（含单测扩展），Go 侧不动、proto 不动（配置暂用内部常量/测试直连）。

- 验证 faiss 量化基础：写最小用例确认 `IndexHNSWSQ`（`QT_8bit`/`QT_bf16`）与 `IndexHNSWPQ` 在**本仓库 metric 映射与分批 AddChunks 时序**下的 train/add/search 行为与免训练类型是否可跳过显式 train（以 faiss 1.9 源码与实测为准，2.1.2 实现验证项）。
- C++ 侧实现 `encodeKey`（与 Go `EncodeKey` 同规则）并加**跨语言一致性测试**（同一组 (kbID, chunkID) 双方产出的 key 字节一致）。
- `ChunkStorage` 新增 `ReadMulti`，`RocksDBChunkStorage` 以 `MultiGet` 实现。
- service 装配改造（2.3.4 改动清单）：`VectorIndexServiceImpl` 注入 `ChunkStorage*`；Search 改为锁外执行、索引实例以生命周期保护持有。
- `HNSWVectorIndex` 支持按量化类型构造与 `Load` 类型放宽；`Search` 内部完成 粗筛(top-N) → `ReadMulti` 取原向量 → rerank。
- **验证**：扩展 `vecstore/test/hnsw_index_test.cpp`（量化构造/训练/加载/兼容）与 `vecstore/test/grpc_service_test.cpp`（两段式 e2e：Build → Search 返回精确 top-k）；`ctest`/CMake 构建通过；小规模内存实测对比（量化 vs Flat）。
- **完成标准**：未配置量化时行为与现状一致（回归）；配置量化时 Search 结果与"全精度精确 top-k"在候选覆盖内一致。

### 阶段 ②：proto 与 Go 配置通道

改动面：vecstore.proto、Go KB 元数据、`IndexManager.TriggerBuild` 接线。

- `BuildIndexRequest` 增量化字段、`SearchIndexRequest` 增 `candidate_n`（2.4 草案）；regenerate proto。
- Go 侧 KB 配置新增 KB 级量化配置（建库写入、创建后不可变）；`TriggerBuild` 读取并随 Build RPC 下发。
- **验证**：e2e 测试（Go 建库选量化 → 构建 → 查询，确认两段式路径生效且 OFF 默认不受影响）；`go test ./internal/...` 回归。
- **完成标准**：一条 CreateKnowledgeBase(quantizer=…) → CreateVersion → Query 全链路通过。

### 阶段 ③：IndexManager 分级存储改造

改动面：internal/index（记账口径、`.index.mem` 内容、配置再标定）。

- 记账口径切换（3.3）：ON 路径尺寸来源改为 vecstore 侧报告（先以 RPC 返回/边车携带落地；若评估成本高则先用 Go 侧公式估算的 (b) 方案）——推荐先 (a) 的简化版：构建完成响应带 size、写入 `.index.mem`。
- `MemoryThresholdMB` 默认值按"图边 + 量化码"口径再标定，写配置说明。
- **验证**：`go test ./internal/index/...`（含现有 LRU/字节阈值/加载测试）全绿；新增"量化 KB 记账口径"测试；集成测试覆盖冷查询（换出后 Load 粗筛器 → 磁盘 rerank 结果不变）。
- **完成标准**：OFF 路径账本与行为零变化；ON 路径换出/重载/记账正确。

### 阶段 ④：参数与基准

- 离线基准产出「候选 N / 量化类型 / `efSearch` → 召回与延迟」曲线（真实 embed 分布；对照全精度 Flat 精确 top-k 的召回）。
- 标定 2.2 默认值（`candidate_multiplier`/`min`/`max`）与 2.1 量化类型推荐（metric 匹配、8bit vs fp16/bf16 vs PQ 的取舍），**回填 2.5 节估算表与 2.2 节参数默认值**。
- **验证**：基准脚本产物 + 结果表入库文档。

### 阶段 ⑤：兼容性回归与压测

- GC/版本删除路径、启动 reconcile、`RebuildIndex`（删除重建含训练时序）、删除 tombstone 竞态。**不做**存量 Flat 文件/旧口径 `.index.mem` 的迁移兼容回归——存量数据均为测试数据，可删除重建；Load 对旧 Flat 的读取仅为文件自描述机制的零成本防御（2.1.3），非验收目标。
- 压测：单版本大 n 构建/查询延迟（含磁盘读）、多版本分级换出稳定性。
- **验证**：既有 `go test ./...`、vecstore `ctest`、integration/docker 回归全绿；压测报告。

---

## 5 风险与开放问题清单

| # | 风险/开放问题 | 影响 | 缓解/归属 |
|---|---|---|---|
| 1 | PQ/8bit 训练样本代表性不足（首批切分） | 码本质量 → 粗筛召回 | 2.1.2 对策；阶段 ④ 基准标定 |
| 2 | faiss `IndexHNSWSQ` 8bit 与 `IndexHNSWPQ` 在 IP/COSINE metric 下的行为未验证（调研遗留项） | 召回与正确性 | 阶段 ① 最小用例优先验证；必要时 metric 匹配矩阵入档 |
| 3 | RocksDB 随机读延迟与 block cache 命中率 | 查询延迟增量（两段式主要代价） | 2.3.5/2.5 预算；阶段 ④ 延迟基准；未来可评估候选批量预取 |
| 4 | 粗筛候选召回上限依赖真实数据分布 | 召回不达标 | N 可调（2.2）；阶段 ④ 曲线标定 |
| 5 | 分数语义变化：对外分数从近似（现状 Flat 为精确；量化路径原会近似）统一为精确 | Query 层阈值/过滤行为微变 | 2.2 已统一口径；阶段 ⑤ 回归调用方 |
| 6 | 内存记账口径：图边估算准确性、vecstore 尺寸报告机制未设计 | 分级换出误判 | 3.3 记账口径 (a) 优先；阶段 ③ |
| 7 | `MemoryThresholdMB` 默认值按新口径需再标定 | 过早/过晚换出 | 阶段 ③ 配置说明 |
| 8 | (已解决)C++ service 并发竞态：Search 锁外执行与 Reset/Load/AddChunks 锁内改写同一索引对象 | 查询/删除竞态、use-after-free | **2.6 状态机 + 每实例 shared_mutex 方案已落地**：读/写按状态分门、写锁内转换、读锁贯穿；Phase 3 并发单测（Search∥Reset/Load/AddChunks）锁定 |
| 9 | `encodeKey` 双实现漂移 | 候选读盘错 key | 跨语言一致性测试（2.3.1/阶段 ①） |
| 10 | COSINE 磁盘原向量读后需归一化（磁盘未归一化、量化码对应归一化向量） | rerank 分数错误 | 2.2 归一化归属；阶段 ① 单测锁定 |
| 11 | 免训练 SQ 是否在所有 metric 下可跳过 train | 构建时序错误 | 阶段 ① faiss 行为验证（2.1.2） |
| 12 | (无需处理)量化后构建/加载的「存量旧数据兼容」：存量数据均为**测试数据、可删除重建**，不做旧文件/旧边车的迁移与兼容专项；构建/加载回归以「删除重建」为准。Load 对旧 Flat 文件的读取只是文件自描述机制的零成本防御（2.1.3），不作验收目标 | 兼容性 | 无（删除重建即可） |

---

## 6 版本链线性化

**状态**：**已实现**（阶段 ②，见 §11）。来源：`version-linearization-decision.md`（已整合）。

### 6.1 决策

`CreateVersion` 在 apply 阶段新增一条校验：**父版本已存在子版本时拒绝**。现有校验（`internal/raft/state_machine.go` 的 `applyCreateVersion`）为：父版本存在、与该 KB 同属、非 `PENDING`、非 `Deleting` —— **不含"父已有子版本"**，因此当前实现允许分叉。

### 6.2 依据：数值序与祖先序在分叉下不再等价

`PebbleDocStore.ReadAt(kbID, docID, maxVersionID)`（`internal/docstore/pebble.go:125`）取该 docID 下"版本号 ≤ maxVersionID 的最大一条"，是**纯数值比较**，不判祖先关系。该替代关系仅在线性（全序）链上与"沿祖先链能读到的最新版本"等价。

复现路径：公共祖先 v5 分叉出兄弟分支 B、C；C 在 v15 独立改写文档 D；B 从未再碰 D、推进到 v20；查询"B@20"的 D 时，`ReadAt` 全局扫描 D 的条目、命中 v15（C 写入，不在 B 的祖先链上），而非 B 血缘上该读到的 v5。

`versiondoc.VersionDocList.ListDocIDs` 一侧按 `ParentVersionID` 真祖先链计算，结论正确 —— **问题在于 membership 检查与内容读取用了两套不同的"谁在先"标准**。现有 `TestIntegration_ForkedVersions`（`integration/integration_test.go:547`）两分支写的是不同 docID（`doc-a` / `doc-b`），未覆盖"同一 docID 在两分支各自独立改写"，故一直未被发现。

线性化之后版本号数值顺序与祖先关系重新画等号，该问题不需要专门修复。

### 6.3 连带影响

1. **README 卖点调整**：`README.md:96`「父版本须已 READY，**允许分叉**」、`:5` 与 `:13` 的"并行比对(A/B)""多个版本并存、可被并行查询"依赖分叉，需拿掉或改写；"查询任意历史版本做审计/对比"不受影响。
2. **`DeleteVersion` 三模式语义退化**（逻辑本身不必推翻，注释/文档需重写）：
   - `SUBTREE`（目标 + 全部后代）→ 退化为"删除该版本及其之后的全部"，即截断链尾；
   - `SINGLE`（仅目标自身，子版本改挂父版本）→ 线性链下最多一个后继要重挂，本质是链表删除中间节点，逻辑不变，但注释中"子版本"（复数）应改为"唯一后继"；
   - `ANCESTORS`（目标全部祖先 + 兄弟分支）→ 退化为"删除该版本及其之前的全部"，即删前缀，"兄弟分支一并删除"这句要去掉。
3. **产品定位不受影响**：既定的"可追溯到任意历史版本，不做破坏性分叉"本就按线性化语义制定。

> **【对齐】** v12 §1.2 与 `改动内容.md:1283` **已按"线性版本链"表述**（`DeleteVersion` 三模式的动机原文即"对线性版本链而言，这等价于只能截断链尾"），而代码与 `README.md:96` 仍允许分叉。**本版是让实现与 README 收敛到 v12 早已假定的前提**，不是新增能力 —— 属文档↔实现三方不一致的清除，见 §12.3 冲突 #2。

### 6.4 删除中间版本：允许

线性化之后删除中间版本是常规能力，无需新设计一套删除逻辑：

- **元数据层**：`SINGLE` 模式即"删除目标自身、后继重挂父版本"，线性链下就是标准的链表删除中间节点，直接复用，只改注释（§6.3 第 2 点）。
- **MVCC 数据回收层**：`DeleteByVersionExceptVisibleFrom` 的 anchor 语义（"大于 versionID 的最小幸存版本"，见 `internal/docstore/pebble.go:235-251` 注释）在线性化后不再有歧义 —— 数值序等价于祖先序，anchor 找到的一定是真正的直系后继，不会像分叉场景那样可能是不相关的兄弟分支。原先"anchor 回收逻辑复用了同一个有问题的替代关系"这条顾虑自动解除。
- **幸存者保护**（`validateSurvivorsNotPending`）：判断对象从"多个子版本"收窄成"至多一个后继"，逻辑不变。

**唯一真正悬空、需额外设计的一环**：被删除中间版本的**原始增量记录**（不是 docstore 里保留的 MVCC 内容，而是"该版本相对上一版本改了什么"这份增量）若被 §7.5 的追链机制依赖，区间 `(localVersion, V-1]` 可能盖住一个已被删除、只保留 MVCC 快照的中间版本，追链就补不出来。方案：**命中已删除区间时退化为全量状态传输**（类似 Raft `InstallSnapshot`），落后节点直接把 `localVersion` 跳到全量传输对应的版本号。该方案因"删除中间版本"被确认为常规能力（不只是删尾巴），已从可选项变为分布式多节点场景的必做兜底，需在 §7 一并实现。

> **【2026-09 落地】** 该兜底已按本节方案实现，见 §7.5 第 5 条：`LocalDataPlane.transferFullState` 在**元数据确认某版本已删除**时，拉取 **`versionID`**（本次要 apply 的那个版本）的整份状态并把 `localVersion` 直接跳到它。取 `versionID` 而不是 `versionID-1` 是先实现后才想清的一点：这条路存在的意义就是"缺口里有版本被删了"，而 `versionID-1` 可能**正是那个被删的版本**——把已知不存在的版本当快照，兜底就会恰好在该用它时失败。两个与前文设想不同、值得记录的实现要点：① 触发信号**不能**由数据面自己判断——`LeaderHandler` 只有 Pebble 句柄与 vecstore 客户端，"空版本 / 已删除 / 从未存在"在存储层是同一种状态（没有行），所以判据必须来自控制层元数据（`VersionExistenceChecker` → `LocalControlPlane.ExistingVersions`，只读本地副本、且一次取全表）；② 该判定**只对"确认删除"放行**，元数据读失败与传输失败仍然中止 apply —— §6.4 的"不报错、不卡死"针对的是"记录确实不在了"，不能扩展到"我暂时拿不到"，否则一次瞬时故障会静默跳过某个版本。判定发生在**拉取之前**，所以已删版本根本不会被拉取（既有优化，也让"确实走了快照"可被外部观察）。另外，一个版本的全量是**自包含**的（`versiondoc` 存整份 docID 列表，不是相对父版本的增量），这才使"不拖父链地跳到快照"成立。

### 6.5 验证计划（TDD，未执行）

1. 先写失败测试：父版本已有子版本时 `CreateVersion` 必须返回错误（对应 `applyCreateVersion` 新增校验点）；
2. 改写 `TestIntegration_ForkedVersions` 中验证"分叉成功"的断言（`integration/integration_test.go:547`）；
3. 校验补齐后重跑全量测试确认无回归。

---

## 7 存储层协调协议：写路径 quorum 与节点间协调

**状态**：**部分实现**（2026-09）。来源：`storage-coordination-and-service-station-design.md` §1–§2 + `control-data-separation-design.md`（v1）§4–§5、§7、§9（已整合）。

| 小节 | 状态 | 落地位置 |
|---|---|---|
| §7.1 quorum 判定与三态 | ✅ 已实现 | `plane.QuorumSize`（⌈(n+1)/2⌉，`internal/plane/local_data_plane.go`） |
| §7.2 协调者 + fan-out | ✅ 已实现（协调者＝写路径所在节点，**临时形态**） | `LocalDataPlane.fanOut` + `sync.Pusher` / `sync.PushHandler`；解开"协调者＝leader"需要不阻塞 apply 的数据源发现，见 §8.5 |
| §7.3 协调者崩溃接管 | ✅ 已实现 | 副本 `WatchVersionWrite`（超时）→ `attemptTakeover`（问 peer 数 quorum，复用 `VersionPresence`）→ 代 `ReportDataDurable`；协调者成功后广播确认（`ConfirmVersionWrite`）让副本偃旗息鼓。propose 通道见下一行 |
| §7.3 前置：propose 通道 | ✅ 已实现并端到端验证 | `InternalService.Propose`（`api/proto/internal.proto`）+ `RaftNodeImpl.proposeAndWait` 的转发分支 + `GRPCProposeForwarder`；错误以哨兵线名跨进程（`errors.Name`/`ByName`） |
| §7.4 客户端 at-least-once | ✅ 前提已满足 | §10.5 的 ack 约束（`Execute` 在 WAL COMMIT 之后才返回） |
| §7.5 游标 + 追链不变式 | ✅ 已实现（游标为内存态，补齐源＝leader） | `LocalDataPlane.localVersion` / `advanceLocalVersion` / `backfillTo` |
| §7.6 节点间交换 `localVersion` | ✅ 已实现 | `LocalVersion` RPC + `sync.LocalVersionQuerier` + `LocalDataPlane.pickBackfillSource`（追链按 peer 游标选源；`SourceResolver` 返回的 leader 只是**保底默认源**，不是唯一源——节点自身为 leader 时拉取会阻塞 apply 循环，所以那个回退是必要的） |
| §7.7 控制层并发调度与有界窗口 | ✅ 已实现 | `LocalDataPlane.limiter`（`internal/plane/write_gate.go`）：per-KB 在飞计数 + 达上限排队（阻塞、honour ctx）；上限来自 `DurabilityPolicy.MaxInFlightWrites`，默认 `DefaultMaxInFlightWrites = 8` |
| §7.8 恢复时的临时协调者 | ✅ 已实现 | `LocalDataPlane.SafeDurableVersion`（对话 peer → quorum 最小值），由 `cmd/stratum` 的 `reconcileIndexStatus` 在启动恢复时过滤 durable 集合 |
| §7.9 `ReportEpoch` payload | ✅ 已实现（修订后的两侧形态） | `ControlPlane.ReportEpoch(epoch, dataVersions map[string]int64, indexReadyVersions map[string][]int64)`；payload 由 `cmd/stratum` 的 `reconcileIndexStatus` 构造（数据游标取自 §7.8 的 `SafeDurableVersion`） |
| §7.10 与现状实现的关系 | 见下方「实现进展」 | — |
| §7.11 边界 | ✅ 已定 | — |
| §7.12 写事务跨层方案 | ✅ 已实现（④a）+ 已接入 fan-out（④b） | 见 §11 阶段 ④ |

### 7.0 v1 基准：契约、恢复协议与演进路径

> 本节把 v1 中与本章直接相关的内容固定为基准，避免只按补充文档转述而产生偏差（偏差清单见 §12.1）。

**两个契约**（v1 §4.1 / §4.2，只含**逻辑对象**，不出现副本 / EC / 路径 / 节点等物理概念）：

- `DataPlane`（控制 → 存储）：`WriteVersionData(kbID, versionID, changes)` / `EnsureIndex(kbID, versionID)` / `DropVersionData` / `Search` / `SetDurabilityPolicy(kbID, policy)`；
- `ControlPlane`（存储 → 控制）：`ReportDataDurable(kbID, versionID, digest)` / `ReportIndexReady` / `ReportAvailability` / `ReportEpoch(epoch, durableVersions)`。

**可用性模型**（v1 §4.3）：控制层只保存抽象可用性 `AVAILABLE` / `DEGRADED` / `UNAVAILABLE`，**不区分降级原因**（副本不足？EC 解码？）—— "副本水位"被上移一档抽象、降为存储层内部概念。

**epoch + block report 恢复协议**（v1 §5.3）：不变式 `控制层状态 ≤ 存储层实际数据`；存储集群每次启动 / 恢复完成生成单调递增的 epoch 并上报 durableVersions，控制层作废所有 `epoch < E` 的历史报告，并把不在集合中的版本降级。

**v1 演进路径**（v1 §7）：阶段 1 抽出契约（**同进程实现**，零分布式代价，先把边界划死）→ 阶段 2 存储集群独立进程（存储层内部重复制，**现有 sync 模型搬家**）→ 阶段 3 epoch + block report（v1 称"生产分水岭"）→ 阶段 4 自治（冷热分层 / 修复 / EC）→ 阶段 5 分片。本版阶段 ⓪ 的三条结论即建立在该路径之上（见 §11）。

**EC 的定位**（v1 §6.3 / §6.4）：EC 只作用于冷数据的部分（L2 冷向量 + 冷版本文档），**不适用于 HNSW 索引**（需随机读、需常驻内存）；且 EC 与 chunk 内容寻址 / 去重、MVCC 零拷贝存在实质冲突（解码单 chunk 需读整条带）—— v1 的结论是 **EC 排在路线最后**，本版沿用。

### 7.1 quorum 判定与三态

存储层内部以**多数派确认**作为"durable"的判定标准，由 `DurabilityPolicy` 表达（`Replicas=N` 时 quorum = ⌈(N+1)/2⌉；EC 场景对应"k 个可解码分片"）。

| 状态 | 条件 |
|---|---|
| `AVAILABLE` | ≥ quorum 确认 durable |
| `DEGRADED` | 有副本但不足 quorum |
| `UNAVAILABLE` | 零副本 |

### 7.2 协调者：临时角色，不是 leader

`DataPlane.WriteVersionData` 打到的**那一个**存储节点，临时充当这次写入的协调者：

1. 把 `changes` fan-out 给该 version 的全部 N 个目标副本（含自己），**同时把"目标副本列表 + quorum 阈值"一并发给每个副本**（原设计 4.1 节的 `DataPlane` 接口需为此新增字段 —— 现状只发数据、不发副本拓扑）；
2. 各副本落盘后各自确认；
3. 协调者数到 quorum 个确认，调用 `ReportDataDurable`。

协调者随请求产生、随请求结束，不跨请求持久化、不需要选举。与 leader 的本质区别：每个 version 的 `changes` 内容在 Raft commit 时已经确定、不需要跨 version 维护顺序，因此不需要一个长期占据的协调者维持全局日志顺序。

### 7.3 协调者崩溃后的接管

**问题**：协调者收齐 quorum 确认后、上报 `ReportDataDurable` 前崩溃 —— 数据本身没丢（quorum 个副本都有），但"已达 quorum"这个事实只存在于协调者内存里。

**机制**：收到写入的每个副本设一个短暂本地超时（毫秒级，副本间直连、局域网延迟）；超时内未收到协调者的"quorum 已达成"收尾信号，副本互相核对各自的确认状态，任一副本数出已达 quorum 后，代替协调者调用 `ReportDataDurable`。这套接管顶替的不是数据传输（副本已有数据，无需重发），只是"发现 quorum 已凑够并负责上报"这最后一步。

**实现进展（前置已完成，接管本体未做）**：

这一节原本卡在一个比它更基础的问题上：**"上报"是一次 Raft 提议，而 Raft 只在 leader 上追加日志**（`kvraft.Raft.Propose` 对非 leader 直接返回 `ErrNotLeader`）。一份"我持有 V"若要由副本说出来，就必须有一条把提议送到 leader 的路——而当时那条路不存在。

| 子项 | 状态 | 落地位置 |
|---|---|---|
| **propose 通道**（前置） | ✅ | `InternalService.Propose`：调用方交出编码后的 command，收到的一方**若自己是 leader 就跑，若不是则回 `leader_id` 重定向而不接力转发**（防环）；接入层 `RaftNodeImpl.proposeAndWait` 在 `ErrNotLeader` 时转发，`kvraft` 核心一行未动 |
| **错误跨进程保真** | ✅ | 结果里的哨兵以稳定线名传输（`errors.Name` / `ByName`），使转发后 `errors.Is` 仍然成立；未知线名退化为消息而非臆造分类 |
| 协调者收尾信号 | ✅ | `ConfirmVersionWrite` RPC；`WriteVersionData` 成功后**异步**广播给全部候选（best-effort：漏掉的副本只是自己多查一次，方向是安全的） |
| 副本本地超时 + 互相核对 | ✅ | `takeoverTimeout`（200ms，§10.4 占位值）+ `WatchVersionWrite` / `ConfirmVersionWrite`；核对用 `VersionPresence` 逐 peer 问"你是否持有 V"，自己算一份 ack |
| 代替协调者上报 | ✅ | `attemptTakeover` → 达 quorum 才 `ReportDataDurable`（digest 由 `Follower.DigestOf` 从本节点数据算出，不能臆造）；**凑不够 quorum 就不宣布**，让版本按 §10.1 走到终态 |
| 一个随之修好的缺陷 | ✅ | `cmd/stratum` 的 `reconcileIndexStatus` 在**每个节点**启动时都跑，此前只有 leader 那次真正生效（其余静默失败）。现在非 leader 的 propose 会转发，该对账在所有节点上都生效 |

- **验证**：单测 11 例（哨兵线名往返与唯一性、命令字节保真、哨兵重建、未知哨兵退化为消息、重定向上报、无地址表报错）；**端到端 2 例**（真实三节点栈：非 leader 进程内提议 → 三节点都可见；转发回来的哨兵仍是原哨兵）。
- **接管本体的验证**：`internal/plane` 7 例（达 quorum 才宣布、少数派保持沉默、不可达 peer 不参与计数、确认后不再宣布、对未监视版本确认是无害 no-op、未接线时不起计时器、同一版本重复投递只产生一次宣布）。
- **一个测试自身的竞态（已记录）**：端到端用例先读"当前 leader"再挑一个非 leader 去提议；若在该读取之后又发生一次选举，转发路径就变了，用例会偶发失败（`-count=3` 稳定通过）。这是测试的时序问题，不是通道的问题。
- **注意（转发引入的语义间隔）**：`proposeAndWait` 等的是**应用这条命令的那台机器**的 apply。转发之后那是 **leader** 的 apply，因此调用返回时 **leader 已应用、proposer 自己还在等日志复制**。"提议成功"与"本节点可见"之间从此多一个异步间隔，读方必须容忍（端到端测试即以此改为限时轮询）。

### 7.4 客户端 at-least-once 保留，作为最终兜底

若协调者所在节点整体丢失（连磁盘都没了），§7.3 的副本互相接管仍能工作（数据在其余副本上）；但若**这一批**（协调者 + 其 fan-out 到的全部目标节点）同时丢失，只能靠调用方（网关/SDK）保留原始数据、在未收到 quorum 确认前不得丢弃，视为 at-least-once 语义。得益于 chunk 内容寻址，重试是幂等的（已收到的 chunk 直接跳过），代价很低。

需要把这一条显式写进 Saga 描述的前提条件里（原设计隐含该假设，但从未言明）。

### 7.5 数据游标：严格线性，标量表示

每个存储节点本地维护 `localVersion map[kbID]int64`（该 KB 已完整应用到的 version 号）。

**强制不变式**：节点收到 version V 的 `changes` 时，若 `localVersion[kb] < V-1`，不得直接应用 —— 必须先向任意一个 `localVersion[kb] >= V-1` 的 peer 请求区间 `(localVersion[kb], V-1]` 的变更补齐，按序应用后才应用 V。此规则堵住断链风险，是"版本号比较即可代表完整性"这一简化成立的前提。

**`changes` 必须持久化，且不只服务本机恢复**：上一条要求**能拿出"版本 N−1 到 N 之间到底改了什么"这份原始记录**。因此 `changes` 不能只是协调者 fan-out 时的一次性字节流、应用完 docstore/versiondoc 就丢弃 —— 一旦丢弃，追链就只剩全量状态传输一条路，正常情况下的增量追赶完全用不了。

**落点：复用现有 WAL，不新建持久化子系统**（一份数据两个用途）：

- `WAL.WriteBegin(ctx, kbID, parentVersionID, changes)` **已经**把 `changes` 编码进 BEGIN 记录（`internal/wal/file.go` 的 `encodeBeginPayload`）；它原本的用途是**本机崩溃后重放存储写入**（`ReplayVersionStorageWrites` 的 replay 输入正来自这条记录）。
- 新用途：把 WAL 的**读取面**从"仅本节点重启时读"扩展到"也能响应**对等节点的追链请求**" —— 落后节点请求的"区间变更"直接就是这段记录，不必另维护一份"变更历史归档"。

**回收规则：跟着"还有没有人可能没追上"走，不跟版本删除走**。这条要单独定，不能套用版本本身的删除节奏（"删除中间版本"讨论的是 MVCC 内容与 versiondoc 集合的可达性，与 WAL 变更记录能否删除是两件事）：

```
当且仅当：当前已知的全部副本（DurabilityPolicy 拓扑范围内）
          都已通过 ReportEpoch 报过 localVersion ≥ V
才允许回收该段 changes / WAL 记录
```

判据复用 §7.13.4 的**周期上报聚合**（"数据在哪"那套），不需要为它单独设计依据。**这同时顺手解决了 WAL 只增不减**：现状 WAL 除 `Truncate` 截尾部垃圾外没有任何回收，是长期无界增长的。

**与全量状态传输兜底的关系**：若落后节点要追的区间**已被回收**（正常回收，或判据出错），直接触发**全量状态传输**（类似 Raft `InstallSnapshot`，见 §6.4），不报错、不卡死 —— 这条兜底本来就是为"增量记录不在了"准备的。

**实现进展与剩余缺口（2026-09 更新）**：

1. **读取面已落地**：`WAL.ChangesFor(kbID, versionID)`（单版本）与 `WAL.ChangesInRange(kbID, from, to)`（区间，返回 `map[int64]VersionDelta`，**带权威 parentID**——一个版本的文档集合由其父版本集合加上自身变更推出，所以 parent 不能由版本链推断）。顺带修掉一个真缺陷：`FileWAL.beginDataByVersion` 原先只在 `Open` 的重建里填充、**运行时写入不维护**，导致"本进程刚写完的版本"查不到（`MockWAL` 才是对的）。
2. **通道已打通**：`DataSyncService.PullVersionChanges`（流式、按版本升序、缺口跳过）+ 服务端 `PushHandler.PullVersionChanges`（未装配 reader 时报 `FailedPrecondition`，**绝不返回空区间**——那会被对端读成"这段没有变更"）+ 客户端 `sync.VersionChangesPuller`。
3. **落后节点侧已接入**：`backfillTo` 先试增量（`backfillByChanges`），**缺口或传输失败都退回**现有逐版本全量拉；重放走轻量入口 `LocalDataPlane.ApplyBackfillChanges`，它只做本地事务，**刻意跳过** fan-out（不能反过来通知自己正在学习的那些 peer）、digest 上报（durable 早已确立）、索引调度（历史版本按 §8.6b 惰性）与**失败上报**——最后一条尤其重要：`reportFailure` 是协调者的通道、能走到 §10.1 的终局裁决，让"只是没取到别人历史"的节点有权判版本永久失败，等于开了一条删除健康版本的路径。
4. **可回收水位 + 物理回收均已落地**：`LocalControlPlane.ReclaimableChangesThrough(kbID)` 按"**全部必须副本都报过 ≥ V**"算出可安全丢弃的水位——取**最慢副本**的游标，任一副本未上报即返回**未知**（＝不回收）；且"必须副本"取自**静态拓扑**（`cfg.Peers`）而非聚合，否则一个只是沉默的节点会悄悄退出要求集合，判据就变成"拿报表校验报表自己"。它依赖的 §7.13.4 聚合**已落地**：`plane.DataVersionRegistry`（leader 内存、按任期清空）+ `ReportDataVersions` 上报 RPC + 节点侧 `sync.DataVersionReporter`（每周期重解析 leader、空视图仍发）。
   **物理回收**：`FileWAL.Compact(ctx, keepThrough)` 整文件重写 + 原子 `rename` + `fsync(dir)`，记录**逐字节复制**（不重编码）；只丢"≤ 水位**且**已提交"的 BEGIN/VERSION_ID，**未完成流程与尾部 in-flight BEGIN 一律保留**（那是 `Recover` 的输入，丢了就等于把崩溃恢复变成崩溃的洞）。保留全部 COMMIT 是刻意的：COMMIT 只带 versionID，按它做回收判断会在多 KB 同号时误丢活记录，而它才九字节、`Recover` 还需要。
   **接线（这个方向值得记住）**：判据是**全局**的（只有 leader 持有上报），而回收动作**只能是存储层的**（WAL 是它的文件）。所以 `ReclaimableChangesThrough` 被提到 `ControlPlane` 接口上，让 `LocalDataPlane.ReclaimChanges` 自己取水位、自己压缩——`LocalDataPlane` 本来就持有 `control`，**不需要新查询 RPC**。但**写数据的那台机器不一定是 leader**（§7.13.2 的候选），它的 WAL 才是增长的那台；补法是**水位回传**：`ReportDataVersionsResponse` 增加 `reclaimable`，leader 只回填"上报者刚提到的那些 KB"的水位，节点侧**仅在报告被接受时**存入（`SetLeaderWatermarks`），`ReclaimableChangesThrough` 在非 leader 时回退到它。陈旧水位是**单向安全**的——游标单调不减，所以陈旧值绝不高于当前真相，只会少回收。后台由 `plane.WALReclaimer` 周期驱动（`DefaultReclaimInterval`；压缩重写文件，绝不能挂在 apply 路径上）。
5. **全量传输兜底已落地**（§6.4）：`backfillTo` 在**元数据确认某个缺口版本已删除**时，退化为"拉 `versionID`（本次要 apply 的那个版本）的整份状态 + 游标跳到它"（`transferFullState`）。**快照取 `versionID` 而不是 `versionID-1`**：这条路存在的意义就是"gap 里有版本被删了"，而 `versionID-1` 完全可能**正是那个被删的版本**——拿一个已知不存在的版本当快照，会让兜底恰好在该用它的时候失败。触发条件被刻意收窄——**只有元数据确认删除才跳**；元数据读失败与传输失败**仍中止 apply**，否则一次网络抖动会静默丢掉一个版本的可见性。判定发生在**拉取之前**（先判定、再 pull），于是已删版本**根本不会被拉**：既省掉无谓传输，也让"确实走了全量快照"成为外部可观察的事实（端到端用例正是靠它取证）。配套修掉一个既有隐患：此前"成功但无记录"的拉取会让游标**越过一个从未获取的版本**；现在由 `VersionExistenceChecker` 先行判定，而 `LocalControlPlane.ExistingVersions` 明确只读**本地**元数据副本、不发 Raft 往返（它跑在 apply 路径上），且**一次取全表**（否则一个长缺口会变成 O(缺口 × 版本数)）。

### 7.6 节点间同步

节点间定期（或写入 fan-out 时顺带）交换 `localVersion[kb]`，落后方按 §7.5 主动追平。因为是标量整数比较，不存在"半新不旧"的中间损坏态，不需要像 chunk 级 gossip 那样处理误传播窗口问题。

### 7.7 控制层调度：内容与应用顺序解耦

`changes` 在 Raft commit 时内容已确定（不依赖存储层此刻进度），因此控制层可以**并发**向多个协调者发起多个 version 的写入，不必为保证顺序而串行等待 —— 顺序由 §7.5 的不变式在存储节点侧保证。

**有界并发窗口**：需对同一 KB 设置"允许同时在飞的未完成 Saga 数"上限，防止某个 version 卡住导致后续无限堆积在存储节点暂存区。达上限后新写入排队；卡住的 version 应转入 §10.1 的"永久失败"终态处理，而不是无限等待。

**实现进展**：`LocalDataPlane.limiter`（`internal/plane/write_gate.go`）。

- **落点**：`WriteVersionData` 的入口 —— 一次写入就是一个未完成 Saga，而堆积发生的正是存储层这一侧。进入前 `Acquire`，退出 `Release`（`defer`，失败路径也归还）。
- **行为**：达上限**排队**而不是失败。这是刻意的：排队的是一个完全合法的写入，快速失败只会把背压推给客户端，变成一堆本不该出现的错误。
- **per-KB**：计数按知识库分桶（一个 KB 的积压不拖住另一个），上限来自该 KB 的 `DurabilityPolicy.MaxInFlightWrites`（`SetDurabilityPolicy` → `SetLimit`），未声明时用 `DefaultMaxInFlightWrites = 8`（§10.4 占位值）。
- **等待可取消**：排队期间 honour `ctx`，调用方放弃就停止等待并返回包装了 ctx 错误的 error，且**不占槽位**。
- **卡住的 version 怎么办**：不在这里处理 —— 由重试预算与终态兜底（§10.1）收尾。本节只保证它不会拖出无限积压。
- **实现细节**：用"计数 + 每次 Release 关闭并换新一个 `wait` channel"而非带缓冲 channel —— 前者让上限**可动态调整**（配置可改），后者会在创建时把容量冻死。

- **验证**：`internal/plane` 10 例（上限内不阻塞、越限排队且 Release 唤醒、排队中 ctx 结束返回错误且不占槽、per-KB 互不影响、`SetLimit` 覆盖与清除、空 Release 不使计数变负、`WriteVersionData` 真的排队、排队后被取消返回错误）。

### 7.8 恢复时的临时协调者

任意存活节点重启后，向其余节点询问 `localVersion[kb]`，凑够 quorum 个报告里的**最小值**，即为可安全上报给控制层的 durable version。

**实现进展**：`LocalDataPlane.SafeDurableVersion(ctx, kbID) (version int64, ok bool, err error)`。

- 向**全部候选 peer** 询问游标（本节点自己的报告也算一份，哪怕它是 0 —— 它丢掉的进度是事实的一部分，不是"没有进度"）；
- 凑够 `QuorumSize(1+len(peers))` 个报告后，取**第 quorum 大**的值，即"至少 quorum 个节点都达到"的最大版本（等价于 sorted-descending 后取第 quorum 个）；这样答案永远不会高估进度；
- 报告不足 quorum → 返回错误（**沉默不等于同意**，不可达的 peer 不参与凑数）；无副本集或未接线 → 返回 `ok=false`，调用方沿用本地视图（单节点没有可分歧的对象）。

**接入点**：`cmd/stratum` 的 `reconcileIndexStatus`。启动恢复时用它过滤 `ReconcileIndexes` 返回的 durable 集合，只上报 `versionID <= safe` 的版本；某个 KB 凑不齐 quorum 时**整个 KB 跳过**（宁可不上报，也不让控制层状态跑到数据前面）。每 KB 只查一次游标，不按版本重复问。

- **验证**：`internal/plane` 6 例（quorum 最小值、报告不足 quorum、无副本集、无查询器、本地报告参与、resolve 错误上抛）。

### 7.9 ReportEpoch payload【修订】

原设计 `ReportEpoch(epoch, durableVersions []VersionRef)` 修订为：

```go
ReportEpoch(epoch uint64,
    dataVersions map[kbID]int64,          // 数据游标，标量：线性链保证完整性
    indexReadyVersions map[kbID][]int64,  // 索引就绪版本集合，不能压成标量（见 §8.1）
)
```

数据侧压缩成标量游标是合法的（线性链的完整性由顺序应用保证）；索引侧不能这么做，见 §8.1。

**实现进展**：契约已按上式修订（`ControlPlane.ReportEpoch` 接收两个 map）。

- **索引侧**才是提升 READY 的依据 —— `IndexStatus.READY` 的含义是"索引可查"，而数据游标只说明"数据都在"，两者不能互相替代，这正是本节要把 payload 拆开的原因。实现上按报告里的**显式集合**匹配（不是区间），对应 §8.1 的"就绪不是版本序的单调函数"。
- **数据侧游标**目前只被记录（日志），尚无消费点：数据侧拥有独立于索引侧的状态属于 §10.1 的"数据侧/索引侧各自独立拥有终态"，那项仍未做；届时游标才真正被用上。
- **payload 的来源**：`cmd/stratum` 的 `reconcileIndexStatus` —— 数据游标直接取自 §7.8 的 `SafeDurableVersion`（quorum 最小值），索引侧集合取自 `ReconcileIndexes` 并按该游标过滤。§7.8 与 §7.9 本就是同一件事的两半。
- **验证**：`internal/plane` 5 例（含"只有数据游标不足以提升 READY"、"索引侧按显式集合而非区间提升"、"未出现在索引侧报告里的 KB 完全不动"）。

### 7.10 【对齐】与现状实现的关系

| 维度 | 现状实现 | 本方案 |
|---|---|---|
| 写路径落点 | 命中 Raft leader，leader 本地落盘（`internal/coordinator/write_impl.go`；入口 `service/knowledgebase.go:139`） | 任一节点临时充当协调者 + fan-out |
| 数据到达副本 | follower 主动 `PullVersionData` 拉全量（`internal/sync/follower.go:55`，装配于 `cmd/stratum/main.go:354-422`） | 协调者 push fan-out + 副本确认 |
| durable 判定 | Raft 元数据 commit + leader 本地落盘 | 数据面 quorum 确认 + `ReportDataDurable` |
| 副本拓扑 | **无**（`DurabilityPolicy` / `Replicas` 在 Go 代码与 proto 中均不存在） | 由 `DurabilityPolicy` 显式表达 |
| 落后检测 | 无数据面游标（`kvraft` 的 applied index 是元数据日志位置） | `localVersion[kbID]` + 追链不变式 |

> **【冲突】** v11「Leader→Follower 数据同步」称 follower 的存储层数据由 Leader 经 `DataSyncService` **推送**，而实现是 follower **主动拉取**（`PullVersionData` 由 follower 发起）。措辞与实现相反，见 §12.3 冲突 #3。

> **【实现进展（2026-09）】** 上表「本方案」一列已部分落地：写路径的 **fan-out 与 quorum 确认**已实现（§7.1/§7.2 —— `LocalDataPlane.WriteVersionData` 写本地后并发推给其余副本、不足 quorum 即失败、达标才上报 durable）、**标量游标与追链不变式**已实现（§7.5）。**推送方向**因此也真实存在了（`PushVersionData`），且与拉取方向共用同一份导出逻辑（`LeaderHandler.ExportVersion`）。**游标交换也已实现**（§7.6）：节点可被问及"你的连续历史到哪个版本"（`LocalVersion` RPC），追链据此选源而不是固定问 leader。尚未实现的是 §7.3 的副本互相接管与 epoch 作废语义（§7.9 的 payload 形态已落地）。

### 7.11 边界【阶段 ⓪ 已定】

1. **数据面独立于 Raft**：本协议是**独立于 Raft 的第二套机制** —— 存储层内部自协调，不承载 Raft 元数据（控制层的 KB / 版本链 / 活跃版本仍走现有 `internal/kvraft` 单组）。依据：v1 §9 #1「存储集群内部用什么协调？→ **独立**协议（否则又不解耦）」。
2. **"网状拓扑"前提在 v1 中不存在**：§8.2 曾引用"节点间不能直连的前提已被网状拓扑推翻"，但 v1 全文没有"网状拓扑"、也没有"节点间不能直连"的表述（v1 §2.2 只说 `PullVersion` 假设"数据源是 leader"）。**该前提需重新论证**，不得作为既定事实使用。实现侧现状是：节点间通路只有三条 —— Raft transport（`internal/kvraft/transport.go:73`）、follower→leader 的 pull（`internal/sync/follower.go:63`）、router→节点（`internal/router/router.go:46`），**没有横向数据面直连**。

### 7.12 写事务跨层方案【阶段 ④ 前置，已定】

**问题**：现状 `WriteCoordinatorImpl.Execute`（`internal/coordinator/write_impl.go:97`）把下列步骤串成一个**持锁的单一事务**（`txnMu`，覆盖 BEGIN 到 COMMIT）：

```
txnMu.Lock()
  Step 1   WAL.WriteBegin(kbID, parentVersionID, changes)     ← 持久化本次写入的重放输入
  Step 2   RaftNode.ProposeCreateVersion(...) → versionID     ← Raft apply 内部写 WAL.WriteVersionID
           GetKB
  Steps 3-6 writeVersionStorage(...)（Step 6 = WAL.WriteCommit）
  Step 6.5 ProposeUpdateVersionSummary(versionID, docIDsHash)  ← 非致命
  Step 7   IndexManager.TriggerBuild                            ← 非致命
```

而 v1 §5.1 的 Saga 是「① 控制层分配 versionID → ② `DataPlane.WriteVersionData(versionID, changes)`」。**差别就在 WAL**：现状的 WAL 同时被控制层（Raft apply 写 `VERSION_ID`）与存储层（协调器写 BEGIN/COMMIT）使用，`FileWAL.rebuildIndex`（`internal/wal/file.go:264-308`）靠**同一文件内的记录顺序** `BEGIN → VERSION_ID` 把"重放输入"与"版本号"绑定起来；拆成两个进程后，这个顺序保证不复存在。

**三条硬约束**（划定候选方案的边界）：

1. **changes 不能进 Raft 日志**：现状刻意把重放输入放在 WAL 而不是 Raft 命令（`ProposeCreateVersion` 只带 kbID / parentVersionID）。Raft 日志会复制到每个节点，带上 MB 级 changes 会让网络与磁盘成本成倍放大。
2. **现状 `BEGIN` 先于 `ProposeCreateVersion` 是有意的**：它保证"已分配的 version 一定有重放输入"。崩溃恢复时 `len(rec.Changes) == 0` 的记录只能跳过（`cmd/stratum/main.go:558-570`），靠 Raft 日志重放 + DataSync 收敛。
3. **恢复语义是"重放存储写入"**：`ReplayVersionStorageWrites` 不写 BEGIN、不 propose version，只重放 steps 3-6（全部幂等），补上崩溃前未写完的数据。

**候选方案**：

| 方案 | 做法 | 优点 | 代价 |
|---|---|---|---|
| **A. 双 WAL + 关联键** | 控制层侧记"versionID + changes 引用"，存储层 WAL 记"为 V 写 changes C"；恢复时存储层扫自己的 WAL，再向控制层查 changes | 两层各自自足，无共享文件 | 控制层必须持久化 changes，否则"控制层崩溃 + 存储层未完成"会丢重放输入；而现状 Raft 日志不带 changes → 要么改 Raft 命令格式，要么另设一个控制层 WAL |
| **B. 存储层自持事务** | 控制层只分配 ID（Raft 日志，已有能力）；`WriteVersionData(kbID, versionID, changes)` 内部 = `BEGIN(changes)` → 写数据 → `COMMIT`，全部落存储层 WAL | 贴合 v1 §5.1 的 Saga 形态；事务原子性完整留在存储层（正是"存储集群自治"的语义）；Raft 日志不膨胀 | 约束 2 的保证消失 → 出现新窗口（见下） |
| **C. changes 进 Raft 日志** | `cmdCreateVersion` 携带 changes，存储层直接从中取重放输入 | 崩溃后任何节点都能重建 | **违反约束 1**（日志膨胀）；折中是 Raft 只存 digest + 客户端重传 |
| **D. B + 显式缺口修复** | 同 B，另外为"propose 成功后、存储层 BEGIN 前崩溃"留下的**无数据 version** 定义检测与补齐路径 | 不引入新协议，复用既有机制 | 需要显式定义检测口径（现状只是被动靠 DataSync 收敛） |

**决策（阶段 ④ 前置，已定）：采用 B + D，且"无数据 version"必须显式补齐** —— 不接受"propose 后、write 前崩溃 → 该 version 可能永远无数据"。

理由：B 是唯一同时满足 v1 §5.1 形态、又不膨胀 Raft 日志的方案，且把事务原子性完整留在存储层；"显式补齐"这条附加要求把约束 2 消失后的窗口变成有明确收敛路径的状态，而不是一个静默的数据缺口。

**新窗口的补齐链路**（必须实现，不能只在文档里声明）：

```
① 检测：block report 的并集（§5.3 ReportEpoch）
   控制层知道该 version 的候选副本集合（§7.1 的 fan-out 拓扑），
   若全部候选都未报告它 durable → 判定为"无数据 version"
② 暴露：保持 PENDING，并在 GetSystemStatus 可见（stuck），不静默
③ 触发补齐：要求写入方按 at-least-once 重传 changes
   （§10.5 的 ack 约束保证客户端此时仍持有原始数据）
④ 兜底：超过重试预算仍未补齐 → FAILED_PERMANENT（§10.1）+ 清理广播（§10.6）
```

**由此推导出一条必要设计（阶段 ④ 必须一并做）**：③ 要真正补上数据，重传**必须复用同一个 version** —— 否则重试会分配新的 versionID，旧的那个仍然无数据，只剩终态清理一条路，等于没补齐。因此 `CreateVersion` 需要**客户端请求幂等键**：

- **契约上**：`CreateVersionRequest` 增加 `client_request_id`（客户端生成，重试保持不变）；
- **行为上**：同一 `client_request_id` 的重试复用已分配的 version，走 `WriteVersionData(versionID, changes)` 重放 —— 现状 `ReplayVersionStorageWrites` 已经是幂等重放，可直接复用；
- **幂等窗口**：与 §10.1 的重试预算 / 超时对齐（具体值待定，见下）。

> **【对齐】** 幂等键是**新增能力**：现状 `CreateVersion` 每次调用都分配新 version，客户端重试会产生一个新版本（旧版本成为孤儿）。这也是自 v11 以来"客户端重试"一直只能靠 DataSync 被动收敛、而无法真正补齐特定 version 的原因。

**剩余待定**：

1. 幂等键的形态与保留窗口（客户端 UUID + 服务端保留多久）——建议与 §10.1 的重试预算同寿命。
2. 检测口径的归属：判定"全部候选都缺"需要 fan-out 拓扑知识，这在 §7.1 落地前不可用；在那之前该窗口只能靠客户端重试 + 终态兜底覆盖。

> **【对齐】** 阶段 ①（已完成）刻意**没有**碰这里：`LocalDataPlane.WriteVersionData` / `DropVersionData` 目前明确返回"未接入"，就是把这个决策留给阶段 ④（见 §11 阶段 ① 的改动面说明）。

### 7.13 协调者选择、数据位置查询与局部健康视图【并入自 `coordinator-selection-and-node-liveness-design.md`】

本节整合该稿 §1–§5。**状态：除 7.13.3 的边界说明外，全部未实现**；与已实现的 §8.5 存在一处模型冲突（7.13.2，已登记为 §12.3 #8）。

#### 7.13.1 存储节点上报时的 leader 发现：复用 router 的动态发现，不新增接口

存储节点向控制层上报（`ReportDataDurable` / `ReportEpoch` / 迟到重传）需要"当前 leader 是谁"，而发起时刻的 leader 可能已经切换。做法：把这些上报调用归入 `isWriteMethod` 判定集合，复用 `internal/router` 的 `forwardWrite` —— `LeaderNow` **每次现查、不缓存**（多数节点认可的 leader 胜出）、无结果时 `tryAll` 逐个试、收到 `NotLeader` 后重新 `LeaderNow` 再打一次。

**理由**：不给"静态/缓存的 leader 查询接口"开口子 —— 查完到实际调用之间仍有窗口，leader 可能又变了，等于把问题往前挪一步而不是解决。（现状：节点内用自己那份 `GetClusterStatus` 视图单点判断，即 §8.5 的 `Resolve` fallback。）

**同一条理由适用于 `CreateVersion` 本身**：它本质上是一次 Raft propose，只有 leader 能真正把它写进日志。因此客户端**不应**绕过 Router 直连某个 follower —— 对方要么直接拒绝（`NotLeader`），要么就得自己再转发一次，等于在客户端逻辑之外又长出一条重复的转发路径。落地方式：`CreateVersion` 与 `ReportDataDurable` / `ReportEpoch` 一样归入 `isWriteMethod`，统一先经 Router 路由到当前 leader；**受理者必须是 leader**，直连 follower 的请求被拒绝（客户端重试时由 Router 重新发现新 leader，与上面的 leader 发现共用同一套逻辑）。

#### 7.13.2 协调者选择：按副本拓扑"试了再说"

- 候选列表 = 该 KB 的副本拓扑（`DurabilityPolicy`），按**稳定顺序**排列（如副本 ID，避免随机顺序让日志难以关联）；
- 依次尝试发起 `WriteVersionData`：成功者即本次协调者，进入既有写入腿流程；全部失败则记 transient、按重试预算/超时判定；
- **不建控制层自己的健康检查子系统**：会与服务站的健康视图独立漂移（两套流量、两套判断标准，故障排查更难）。每次选择自带验证，与服务站熔断同一种"试了再说"的思路，只是不为写路径重复建一套。
- **fan-out 必须保持网状直连**，不插代理层：协调者需要精确知道"是哪几个副本确认了"（同 Raft 需要精确的 `matchIndex`），代理会模糊这个信号；且 §7.4 的 at-least-once 兜底、§7.3 的接管、§10.6 的清理广播都建立在"协调者与副本直连"这一前提上，插一层就要重新审查它们的正确性前提。

> **【冲突 §12.3 #8】** 本节把协调者选择放在**控制层**（apply 时由 leader 按拓扑指派），而 §8.5 **已实现**的是"受理 `CreateVersion` 的节点就地当协调者"（入口/客户端落到谁即谁）。这是两种不同的模型，需二选一或明确分工。

#### 7.13.3 apply 阶段谁能触发外部副作用【边界】

`Apply()` 在**每个**节点（leader + 全部 follower）都会跑，用于保证本地状态机与日志一致。**外部副作用（协调者选择 + fan-out）只能由"apply 这一刻确实是 leader"的节点触发** —— 是 apply 时现查（如 `rf.GetState() == Leader`），不是"发起 propose 时是不是 leader"。否则一次写入会被 N 个节点各自触发一遍（内容寻址天然幂等，不是正确性错误，但会产生不必要的资源浪费与重复 quorum 链路）。

接受的窗口：leader 切换恰好落在"旧 leader 尚未跑到这条日志、新 leader 已跑到"之间时，理论上两边都判断自己是 leader —— 靠幂等吸收，不用分布式锁去堵（堵这个窗口的复杂度收益远不如"允许极小概率重复 + 幂等兜底"）。

> **【别混】本条约束的是写方向**（选协调者 + fan-out）。**读/同步方向必须每个节点都触发** —— 即 §8.5 的 `onVersionCreated` 通知（每个 apply 过的节点各自补齐自己的副本）。两者方向不同，不冲突。

#### 7.13.4 写完成后"数据最终在哪"的查询：不记快照，用 `ReportEpoch` 聚合

- **不记录"写完成时的确认副本清单"**：那份清单从记下的那一刻就开始过期（副本可能被换掉、可能落后需补链、也可能后来才追上）。
- **改为持续刷新的聚合**：每个存储节点周期性上报自己每个 KB 的 `dataVersions`；查询"版本 V 现在活在哪几个节点" = "**哪些节点最近一次上报的 `dataVersions[kb] ≥ V`**"。节点掉队/追上都会自动反映。
- **维护位置**：**当前 leader 的内存**（`nodeID → kbID → dataVersions`），**不进 Raft、不持久化**。它是高频软状态、允许数秒滞后；真正需要强一致的是"数据/索引 READY"（决定查询可见性），两者不能混同。
- **leader 切换**：新 leader 上任时该 map 为空，**不做任何迁移**；存储节点下一轮上报（走 `NotLeader` 重试）自然会打到新 leader，一个上报周期内信息自己补齐。软状态允许在故障切换时清零重建。
- **用途**：① 为"永久失效副本换新节点补数据"挑数据源；② 指导落后节点该找谁补链（§7.5）。

> **【硬边界】** 这份聚合是**允许滞后的软状态**，只能用于**运维决策与优化**（挑数据源、指导补链），**不得接入正确性判定**。反例：若把"数据是否从未落地"的判定换成查这份聚合 —— 现状 `service/datamissing.go` 的 `dataMissingVersions` 是**实时逐个探测**且"不可达算未知、不算缺失"；换掉之后，刚写完尚未到上报周期的版本、或聚合因 leader 切换刚清空的时段，都会被读成"没有任何副本持有"，而该判定一旦接到"数据全局不可用"乃至 `FAILED_PERMANENT`，就会触发 §10.6 的清理广播，**删掉真实存在的数据**（不可逆）。

> **【实现（2026-09）与一个改名提醒】** 本节已落地：`plane.DataVersionRegistry`（leader 内存聚合，`Record` 整份替换）、`ReportDataVersions` 上报 RPC（非 leader 回 `accepted=false` 而不是报错——"打到了错节点"不是传输失败）、节点侧 `sync.DataVersionReporter`（每周期重新解析 leader，不缓存；**空视图仍然上报**，否则一个丢了数据的节点会永远留在别人的旧视图里）、以及 `plane.LeaderGate`（follower→leader 跃迁时清空聚合，保证新任期从零开始）。查询入口是 `LocalControlPlane.DataVersionHolders(kbID, versionID) ([]int64, bool)`——`ok=false` 表示**答案不可用**（非 leader 或未装配），**不等于"没人持有"**：两者导致相反的行动，后者会为删除健康数据背书。
>
> **⚠️ 名字陷阱**：本节标题里的 `ReportEpoch` 指的是**上面这套周期上报聚合**；而代码里另有一个**同名**的 `ControlPlane.ReportEpoch(ctx, epoch, dataVersions, indexReadyVersions)`，它是**重启后的一次性对账**（把元数据与存储层事实对齐、把存储层缺失的版本降级，不变式是"控制层状态不超前于存储层数据"），**与本节的周期聚合毫无关系**。按名字去找实现会找错地方。

#### 7.13.5 存储节点之间的局部健康视图

- **不设权威维护者**（否则凭空造出一个新的共识问题：维护者本身要选、崩了要重选），而健康状态天然不需要共识 —— 每个节点维护**自己**对其它节点的局部认知，互不对齐、互不共享。
- **维护方式**：顺着真实流量捎带（fan-out、§7.3 的互相核对代为上报，调用成败顺手更新），**不单开心跳/ping 子系统**；对长期无往来的节点对补一个低频、尽力而为的心跳兜底。
- **只用于优化，不用于正确性判断**：协调者选择时把"最近能通"的候选排前面、复用 §9 的熔断骨架短暂降低某副本优先级；排错也无妨，最终仍由"试了再说 + quorum"兜底。
- **不出节点边界**：不进 `ReportEpoch`、不上报控制层、不给服务站共享（服务站有自己独立的健康检查，两边探测频率/阈值不对齐会加剧视图分歧，共享反而让故障排查更难）。

---

## 8 索引构建与多副本分发

**状态**：草案、未实现。来源：`storage-coordination-and-service-station-design.md` §3（已整合）。

### 8.1 索引游标不能是标量

数据必须按链严格顺序应用，一个整数游标天然蕴含"之前全部完整"。索引构建**没有这条约束** —— v12 §2.1.2 已确立"每个版本的量化器独立（版本级索引本来就各自全量重建），**不存在跨版本共享码本**"（`Stratum_设计文档v12.md:223`），version V+1 的索引可能先于 V 建好（V 卡在重试、V+1 后来居上）。因此索引侧状态必须是 `map[int64]bool`（每个 version 各自的 READY / 未 READY），且只需覆盖 `IndexRetentionCount` 保留窗口内的 version，窗口外按需走 `RebuildIndex`。

### 8.2 索引 quorum 的含义与数据 quorum 完全不同

- **索引从不需要持久性保护** —— 它是派生物，L2 层（RocksDB 全精度向量，受数据 quorum 保护）不丢，索引随时可重算，不存在不可逆损失。
- 索引侧的"quorum"（多少个节点 READY 才不算 `DEGRADED`）保护的是**读路径的服务能力分布**，不是持久性 —— 是一个负载/容量问题。`DEGRADED` 的准确含义应改为"当前能扛读流量的节点数低于预期，读请求有较大概率打到需要现场触发构建的节点、延迟劣化"，而不是"有丢失风险"。
- 该阈值应是**独立于** `DurabilityPolicy.Replicas` 的配置项，二者维度不同、不该共用一个数字。其归属见 §10.2。

### 8.3 构建也走"建一次、分发 N 份"，复用写路径的协调者模式

同一个 version 的索引没必要在 N 个副本节点各自重复训练 + 构建（重复计算，且并发构建可能因插入顺序不同产生略有差异的码本）。改为：

1. `EnsureIndex` 打到的存储节点临时充当"构建协调者"，实际执行训练 + 构建 + Save；
2. 建好后把索引文件（含码本/量化状态，随 `write_index` 一并序列化）分发给其余副本，副本 `Load` 即可，不重新构建；
3. 副本确认 Load 成功、凑够索引服务能力阈值后，协调者上报 `ReportIndexReady`。

**[前置依赖 —— 阶段 ③ 已补齐]**：索引写入原为非原子、无 checksum —— `vecstore/src/hnsw_index.cpp:382` 直接 `faiss::write_index(index_.get(), path.c_str())`，既无 `.tmp + rename`、也无校验；`Load` 不校验 `ids.size() == ntotal`。本地磁盘 corruption 尚可接受（重建即可，损失可控）；一旦引入跨网络分发，网络传输本身可能损坏文件而现有 `Load` 无法感知 —— **必须在文件分发之前补齐原子写与 checksum**，否则会引入一个比本地偶发 corruption 更易触发、且是静默的正确性风险。

### 8.4 推翻 v1 / v11 的「多副本构建」【阶段 ⓪ 已定】

v1 §9 #6 的建议是「索引是否参与跨节点复制？→ 建议**否**（索引是派生物，从原始数据本地重算）」，v11 据此写成（`Stratum_设计文档v11.md:244`）：「**多副本构建**：每个副本独立异步构建自己的 HNSW 索引。follower 的存储层数据由 Leader 经 `DataSyncService` 推送，落盘后独立构建；HNSW 构建的随机性导致不同副本图结构可能不完全相同，但召回质量等价，无需同步。」

该决策有实现背景：`改动内容.md:1173` 记录缺陷 6「**共享 vecstore 多节点并发 build 竞态**（偏离设计）：3 节点共享宿主单 vecstore，leader 与各 follower 各自 `TriggerBuild` 同一版本，vecstore `Build = Reset + AddChunks` 互相清空 → 偶发 `Save: no index has been built`」，其修复方式即**按 v11「多副本构建」改为每节点独立 vecstore**（`scripts/docker-cluster.sh` 新增 `vecstore start/stop/status`，第 N 节点连宿主 `:710N`，RocksDB 数据目录各自独立）。

**结论（阶段 ⓪）：改为"建一次、分发 N 份"**，撤销 v1 §9 #6 与 v11:244。

**实现进展（已实现）**：

| 子项 | 状态 | 落地位置 |
|---|---|---|
| 索引文件传输通道 | ✅ | `DataSyncService.PushIndexData`（流式）：**跨节点**那段由 Go 存储层承担——它已握着副本集合；vecstore 仍只按调用方给的 `Path` 读写本地文件，**C++ 一行未改、也不需要改** |
| 传送顺序 | ✅ | **sidecar 先、索引后**。传输中断时副本留下的是一个校验文件加一个残缺索引（下次attempt 覆盖即可），而不是一个**无法认证**的索引 |
| 分块 | ✅ | `indexChunkSize = 1 MiB`，远在 gRPC 默认 4 MiB 报文上限之下，大索引无需调参 |
| 接收与安装 | ✅ | `IndexManagerImpl.InstallIndex`：两个文件都走 `.installing` 临时名 + `fsync` + `rename`（与本地构建同一条纪律），再 `Load`。写入者是构建者之外的新写入者，这是"安装"与"构建"来源不同所要求的 |
| 触发 | ✅ | 构建完成回调里分发（`distributeIndex`，因为 `dataPlane` 晚于回调注册创建，用闭包变量延迟绑定） |
| 分发失败 | ✅ 回退自建 | 逐副本 best-effort 并记 warn，**绝不让分发失败导致版本不可服务**——最坏退化为分发存在之前的行为。这是阶段 ⓪ 时按"先依赖、再能测"定下的取舍 |
| 构建者 | ✅ | 写路径所在节点（当前即协调者），它同时握着副本集合与"我构建完了"这个事实 |

- **验证**：`internal/sync` 5 例（两份文件跨块重组逐字节相等、sidecar 确实先行、空索引本地拒绝、未装 installer 时 node 明确报错而非假装成功、安装失败上抛）、`internal/plane` 4 例（覆盖全部候选、单副本失败不中断、未接线为 no-op、本地读失败上抛且不外发）。
- **端到端验证**：`integration` 的真实双节点栈 —— leader 建版本（真实 changes）→ 索引落盘 → 副本经 `PushIndexData` 安装 → **断言两边索引文件与 sidecar 逐字节相同**。这个断言才是本节的核心证据：HNSW 构建是随机的，两个节点各自构建不可能产出相同的图文件，所以"相同"只可能来自"其中一方收到的是另一方的产物"。同时覆盖 `InstallIndex` 的真实写盘与后续 `Load`。
  - 为此需要让真实节点装配（`integration` 的 `realNode`）显式接上分发链路，并**补上它原本缺失的 `IndexDataDir`** —— 没有它，测试栈的索引只留在内存、根本不落盘，§8.4 在那里无货可发。
- **一条踩过的弯路（结论仍成立）**：曾试图用"两个 vecstore + 两个 IndexManager"轻量构造端到端，不成立 —— 真实 `IndexManagerImpl` 的构建需要**完整数据面**（docstore / chunkdoc / chunkstore / versiondoc，才能列出并读取该版本的 chunk 向量），而且当时 `realNode` 连 `IndexDataDir` 都没设。结论：**索引分发的端到端验证必须跑在完整节点上**。
- **连带需要（仍待做）**：

- ~~新增**索引文件跨节点传输通道**~~ ✅ 已做（见上表）；
- **改写 v11:244 与 `scripts/docker-cluster.sh` 的"每节点独立 vecstore"前提** —— 每副本仍可有独立 vecstore 进程（分发后各自 `Load` 到自己的目录），但索引不再各自构建。**文档与脚本的这一处尚未同步**。

一条附带收益：该结论同时消掉了 `改动内容.md:1173` 那个缺陷的成因（多个节点对同一 version 各自 `TriggerBuild`，`Build = Reset + AddChunks` 互相清空），因为构建收敛到单个协调者。

### 8.5 崩溃接管：不预留 standby，动态指派

**结论**：不为每个构建任务固定绑定 standby 节点，6 个节点可同时开 6 个独立构建任务，最大化正常态并行度。理由：

- 构建失败的代价只是"这个 version 多在 `DEGRADED` 里待一会儿"，不是数据风险（§8.2 已确认索引可随时重算）；
- 预留 standby 换来的收益（缩短故障后的调度延迟）只在低概率崩溃时刻兑现；不预留换来的收益（吞吐翻倍）持续存在；
- standby 节点即使预留也没有断点续传能力（构建中间态未落盘），一样要从头重建 —— 预留省下的只是"发现崩溃到指派接手者"这一小段调度延迟。

**例外**：若未来业务提出"version 多久内必须可查询"的硬性 SLA，需重新评估该取舍（当前没有这样的约束）。

**实现进展（模型已按 §7.13.2 收敛：协调者由控制层指派，不由受理者自任）**：

> **【2026-09 更新：下面"两步"曾按"受理者就地当协调者"落地，随后按 §7.13.2 收敛】** 现行为是：`CreateVersion` 回到 `internal/router` 的 `writeMethods`（leader-bound），**受理者必须是 leader —— 直连 follower 一律拒绝（`NotLeader`）**；由 **apply 那一刻现查 `IsLeader()`** 的节点按副本拓扑挑一个候选、派它执行写（`ExecuteVersionWrite`），数据落在候选身上、fan-out 从候选发起。理由见 §7.13.1：`CreateVersion` 本质是一次 Raft propose，只有 leader 能把它写进日志，让 follower 自己再转发一次等于在客户端逻辑之外又长出一条重复的转发路径。
>
> **这两步的工作没有白做**：① 数据源注册表仍是"数据在哪"的推送式提示（§8.5 第一步）；② 第二步 `CreateVersion` 期间顺带修掉的三处"协调者 = leader"隐含等式——`EnsureIndex`/`FetchVersionData` 的游标判据、`onVersionCreated` 每个 applier 都通知、`sourceAddrFor`/`Resolve` 的自我源短路——**全部继续有效**（详见 §7.13 与 §12.3 #8）。

以下为当时（"受理者就地当协调者"模型）的记录，保留以备追溯：

本节要的"动态指派"，在实现上等于**解开"协调者 = leader"的绑定**（§7.2 的当前形态）——让写请求可以由任意节点受理、由受理者就地当协调者。它的动机不只是并行度，还有**压力分布**：现在控制层 leader、数据 fan-out、索引构建、索引分发常常叠在同一台机器上。

2026-09 试过一小步（把 `CreateVersion` 移出 `internal/router` 的 leader-bound 名单），**结论是不成立、已回退**，原因值得记下：

- **真实前置不是路由名单，而是"数据源发现"**。副本靠 `SourceResolver` 找数据，而它只会答"leader"。协调者一旦可以是 follower，leader 就未必持有该版本的数据，副本会永远拉不到。
- **而修法不能是"在 `SourceResolver` 里逐 peer 探测"**。这个回调跑在 **Raft apply 路径**上（重放历史日志时每条 `CreateVersion` 都会触发它），任何阻塞式 RPC 都会拖住后续所有日志的应用——这正是原实现只做一次 `GetClusterStatus` 的原因。实测后果：`integration` 从 26s 恶化到 **462s 并超时**。
- **正确形态**：协调者写完数据后**主动告知**副本"数据在我这儿"（与 §7.3 的收尾信号同一路数），把"找源"从**拉取方探测**变成**推送方的已知事实**。这样 `SourceResolver` 可以继续保持不探测。

`SourceResolver` 与 `internal/router/classify.go` 的相关注释里都留了这条约束，避免以后无声地重踩。

**第一步已落地：把"源"从探测变成可查表的事实**

| 子项 | 状态 | 落地位置与形态 |
|---|---|---|
| 数据源注册表 | ✅ | `plane.DataSourceRegistry`（`internal/plane/data_source_registry.go`）：`(kb, version) → DataSyncService 地址`。**刻意是内存表**——丢了没代价（回退 leader，下一次确认重新学到），持久化反而多一个真值来源 |
| 协调者主动告知 | ✅ | **复用既有的 §7.3 收尾信号**：`ConfirmVersionWriteRequest` 加了 `source_addr`（协调者填自己的地址；装配点 `cmd/stratum/main.go` 的 `SelfDataSyncAddr`，与 DataSyncService 同端点）。接收侧 `sync.PushHandler.ConfirmVersionWrite` 把它记进注册表；`DeleteVersionData` 回收数据时 `ForgetVersion` 掉，避免把后来的拉取指向"已经没有该版本"的节点 |
| 查找路径 | ✅ | `SourceResolver` 签名补上 `(kbID, versionID)`（原来只有 `ctx`，因而只能答"leader"）；`plane.ResolverWithRegistry(reg, fallback)` 先查表、未命中回退 leader。**行为与第一步之前完全一致**（当前协调者就是 leader），且仍是"廉价、不探测"的查找——上面那条实测约束没有被破坏 |
| 验证 | ✅ | `internal/plane/data_source_registry_test.go`：注册/覆盖/空地址忽略/按 KB 与版本隔离/并发；resolver 的"注册优先、未注册回退、回退错误透传、nil registry 安全"。全量 `go test ./...` 除 §5 既有的偶发用例外全绿 |
| 装配边界 | ⚠️ | `cmd/stratum/main.go` 已装注册表、`SelfDataSyncAddr` 与 `WithDataSourceRegistry`；`integration` 的 `realNode` **没装**（它不走 plane 的拉取路径，也没配 `Confirmer`，所以那条通路在测试栈里本来就不活动）——端到端验证留给第二步 |

**第二步已落地：任意节点受理 `CreateVersion`（受理者就地当协调者）**

| 子项 | 状态 | 落地位置与形态 |
|---|---|---|
| 路由不再重定向 | ✅ | `CreateVersion` 移出 `internal/router/classify.go` 的 `writeMethods`：受理它的节点成为**该版本的写协调者**，就地跑写路径（落数据 → 扇出副本 → 凑 quorum → 提议，提议经既有转发到 leader）。压力因此从"一台机器"变成"受理请求的那台机器"（control leader + 数据扇出 + 索引构建 + 索引分发） |
| 写路径的 leader 依赖核查 | ✅ | `WriteCoordinatorImpl.Execute` 只做两件事：`RaftNode.ProposeCreateVersion`（转发已就位）与 `DataPlane.WriteVersionData`（本地存储事务 + 扇出 + `reportAndSchedule`：digest 上报经转发、索引构建本地触发）。**没有任何一处要求"我是 leader"** |
| "我是 writer"不再等于"我是 leader" | ✅ | 原来 `EnsureIndex`/`FetchVersionData` 把 `resolve() → ok=false` 读作"我是 writer"，这只在 writer == leader 时成立。现在判据是**本节点连续游标**（`localVersionOf(kbID) ≥ versionID`）：数据已在本机（写路径 / `PullVersion` / backfill 任一来源）就不拉取。重试循环里**每轮重新检查游标 + 重新解析源**——前者覆盖"协调者自己的 apply 早于它自己写完数据"，后者覆盖"**声明晚到**"：非 leader 协调者要等自己的写完成才宣告自己，所以第一次解析到的 leader 根本不持有该版本，守着它只会白拉到超时（回归测试 `TestLocalDataPlane_EnsureIndex_ReResolvesSourceEachAttempt`） |
| **`onVersionCreated` 的旧假设（关键）** | ✅ | 该回调过去只在**非提案者** apply 时触发（`internal/raft/impl.go`），编码的正是"提案者 = 写者"。转发形态下这条假设两头都错：**leader 也是提议的中转方**（它有本地 waiter，于是被跳过，而它恰恰不持有数据），而非 leader 协调者持有数据却无人知晓。改为**每个 applier 都触发**，"谁需要拉取"交给数据面按游标判断——Raft 层本来也不该知道数据在哪。固化旧假设的单测 `…_NoopForProposer` 已改名为 `…_FiresForProposerToo` |
| 端到端验证 | ✅ | `integration/non_leader_coordinator_test.go`：三节点、协调者取**非 leader**，在该节点上 `CreateVersion`；断言 ① 调用成功 ② 数据确实落在协调者（`versionDoc.ListDocIDs` 非空）③ **leader 不持有该版本**（0 docs——这条对照使后面的收敛只能来自跨节点搬运）④ 每个节点都收敛到数据且索引可查。`-count=5` 连跑通过 |
| 装配边界（本栈） | ⚠️ | `integration` 的 `realNode` 写路径是旧形态（不经过 plane），所以 §8.5 的**声明广播不会自然发生**：测试显式写入每个**读者**的声明表来替代（生产由 plane 的 §7.3 确认携带地址发往所有候选）。读取侧 `sourceAddrFor` 与生产同构：先查声明、再回退 leader、指向自己则忽略 |
| 仍是 leader-bound | ⚠️ | `CreateKnowledgeBase` / `DeleteKnowledgeBase` / `RollbackVersion` / `DeleteVersion` / `RebuildIndex` / `WarmupVersion` 未动。版本删除类操作同样存在"协调者 ≠ leader"的数据问题，应作为后续独立一项（各自先确认其数据面语义） |

验证口径就是上面那条用例：**"非 leader 当协调者后，副本仍能找到数据源并收敛"**。

### 8.6 构建成本的三层优化（按效果从大到小）

**(a) 免图分层（与 v11 的"冷热分层"结合）【已实现】**

**实现进展**：

| 子项 | 状态 | 落地位置 |
|---|---|---|
| 免图形态的枚举 | ✅ | `QuantizerTypeProto` 加 `QUANTIZER_{OFF,SQ8,SQ_BF16,SQ_FP16,PQ}_FLAT`；C++ 侧 `QuantizerType` 加对应的 `kOffFlat`/`kSQ8Flat`/… 及 `isGraphFree()` / `isQuantized()` |
| 按请求建免图索引 | ✅ | `hnsw_index.cpp` 构造分支加免图路径（`IndexFlat` / `IndexScalarQuantizer` / `IndexPQ`）；proto ↔ C++ 映射同步 |
| 检索语义 | ✅ | `quantized_` 由 `isQuantized()` 判定——免图变体量化方式相同，rerank 语义随之相同 |
| **冷热转换策略** | ✅ | 见下 |

**三处"不做就会坏"的埋点（能力要成立必须一起改）**：

1. `index_` 的类型原是 `std::unique_ptr<faiss::IndexHNSW>` —— **HNSW 被写死在类型里**，免图索引根本放不进去。已放宽为 `faiss::Index`（其余 7 处用法都走基类接口，只有两个调优旋钮需要 cast）。
2. `index_->hnsw.efConstruction/efSearch` —— 无守卫，免图索引没有 `hnsw` 成员，**这一行会崩**。已改为 `dynamic_cast` 守卫。
3. **`Load` 的 `dynamic_cast<IndexHNSW*>`** —— 它原本**明确拒绝**非 HNSW 文件。不改的话免图索引**存得下、读不回**，而 §8.4 的分发恰好依赖"存盘 + 在别的节点 Load"。已放宽为接受任意 `faiss::Index`，`ntotal` 与检索模式的恢复改用基类/多态判定。

**一条被免图测试重新确认的既有语义**：状态由 **`Save`** 封印（`BUILDING → READY`，见 `hnsw_index.cpp` 的 "Save seals the build"），任何形态都必须 `Save` 之后才可查询。免图不改变这条语义。

- **验证**：`ctest` **40/40 通过**（当时的基线 37 + 3 个免图用例：免图索引构建/查询/**Save→Load 往返**；全精度免图的答案与暴力扫描逐位相同；免图形态的内存估计不含图项。整轮 ctest 在 §8.6(c) 落地后为 **50/50**）。

**冷热转换策略（Go 侧地基 + 策略本体）**

起因是一处结构性限制：`QuantizerConfig` 是 **KB 级、创建时固定**（`vecstore/include/types.h`：*"fixed at knowledge-base creation time and never changes"*），而本节要的是**逐版本**的形态差异。

**地基已完成**：

| 改动 | 内容 |
|---|---|
| vecstore proto 的 Go 侧 | 重新生成，5 个免图枚举可用 |
| `quantizerForKB(kb, graphFree)` | 加免图维度；新 `graphFreeVariant()` 做量化→免图映射 |
| `buildQuantizerFromKB` | 改为复用 `quantizerForKB`（原先自己重复了一遍 switch） |
| `TriggerBuildGraphFree` | 新入口，存储层可显式请求"把某版本建为免图" |
| `doBuild` / `build` / `buildWithRetry` | 贯通 `graphFree` 到 `BuildIndexRequest.Quantizer` |

链路已通：**存储层能请求免图形态，vecstore 能照做**。

**转换策略（原"剩余三件"）——已实现**：

| 子项 | 状态 | 落地位置与形态 |
|---|---|---|
| 访问跟踪 | ✅ | `IndexManagerImpl.lastSearch`（`internal/index/impl.go`）。刻意**不复用** `loadedIndex.lastAccess`：后者是内存 LRU 提示、随换出丢失，而"这个版本冷不冷"是查询流量问题，必须跨换出存活，并覆盖从未加载过的版本（暴力扫描应答的小版本、被磁盘保留策略删掉的版本）。`Search` 入口即记（在任何 load/build/暴力扫描决策之前）；构建成功会为尚无记录的版本种下"构建时间"基准，于是**从未被查询的版本也会随阈值老化**。`InstallIndex`（副本装载 §8.4 分发来的产物）也走同一个 `seedAccessLocked`——否则"只接收过产物、自己从未构建"的副本永远不进访问表，评估器看不见它，分发来的形态就永久固化在那里。`LastAccess(kbID, versionID)` 读之。清理语义：`Evict` 保留（换出是内存决策），`Discard` 清该版本，`DeleteFilesByKB` 清整 KB |
| 后台定时评估 + 冷阈配置 | ✅ | 形态即设计所定的"**自动、存储层自己判、阈值做成配置项**"：`IndexManagerConfig.ColdThreshold` / `ColdSweepInterval`（`<= 0` = 关闭，保持历史行为），yaml 为 `index_manager.cold_threshold_ms` / `cold_sweep_interval_ms`；`StartColdPolicy()` 起后台评估器（`DefaultColdSweepInterval` = 1 分钟），`Close()` 停之。评估器只读本节点访问表：不参与共识、不跨节点通信，因此两副本的转换时机可以不同。构建进行中的版本被跳过（在飞的构建自己决定形态），下一轮再评估 |
| 形态记账（防重复重建） | ✅ | `builtGraphFree map[indexKey]bool`：构建成功时记下本次形态；已是免图的版本不再在下一轮被选中，避免"每轮都重建一遍"。随版本/KB 删除而清 |
| **vecstore 侧形态切换（实测补上的必需一环）** | ✅ | 只有"请求免图"的能力还不够：`VectorIndexServiceImpl::GetOrCreateLocked` 原来对已存在的 (kb,version) **直接复用对象、忽略新 config**，而形态（Faiss 索引类型）是构造时定死的——于是免图 Build 会静默地重建出**带图**索引，产物字节与原来完全相同（这正是真机实测里"重建发生了、文件却一个字节没变"的原因）。现改为：`VectorIndex` 加 `MatchesConfig()`，Build 走 `GetOrCreateForShapeLocked()`，config 不匹配就**替换索引对象**；`AddChunks`/`Load` 仍走不替换的 `GetOrCreateLocked()`（否则分批构建的第二批会把第一批请求的形态改回默认） |
| 重建后经 §8.4 分发 | ✅ **复用既有链路，未加新代码** | 免图重建走的是同一个 `doBuild`，因此同样触发 `BuildCompleteCallback(READY)`；`cmd/stratum/main.go`（及 `integration` 的 `realNode` 装配）在 `IndexStatusReady` 时**无条件**调 `distributeIndex` → `LocalDataPlane.PushIndexToReplicas` → 副本 `IndexManager.InstallIndex` + `Load`。分发本身形态无关（纯文件搬运 + 副本 `Load`，而 `Load` 已放宽为接受任意 `faiss::Index`），所以"形态变了产物就变了"这件事无需额外通道 |
| 内存记账按形态 | ✅ | `HNSWVectorIndex::EstimatedMemoryBytes()` 不再给免图形态叠加图项（详见下方"内存记账按形态区分"）：否则冷重建省下的正是这块内存，而 Go 侧字节预算（`MemoryThresholdMB`）读的恰是它 |

**证据（单元 + 集群两层）**：

- 单元：`TestIndexManager_ColdRebuildReportsThroughBuildCallback` 断言冷重建确实以 `READY` 回报（分发正是由该回调驱动）；`TestIndexManager_ColdPolicyPicksOnlyColdVersions` / `RunsInBackground` / `Lifecycle` 覆盖选取、自动通路与开关；`TestIndexManager_InstallIndexSeedsColdPolicyBaseline` 覆盖"只接收过产物"的副本也能老化。
- 集群：**`TestRealStack_ColdRebuildRedistributesTheArtifact`**（`integration/cold_reshape_test.go`）——真实双节点栈（真 vecstore 子进程、真 Raft、真 gRPC），只有 node 1 启用评估器、node 2 只做数据拉取（`wireSyncPullDataOnly`，§8.6b 的 data-only pull）因而**永不自己重建**。实测：带图产物 **6866 字节** → 冷重建后 **1325 字节**（图边被去掉），**两节点字节相等**，且重建后两节点仍能查到结果（top doc `doc-0`）。node 2 的字节相等只能来自 node 1 的推送——与 §8.4 首次分发用例同一论证。

**内存记账按形态区分（已修）**：`HNSWVectorIndex::EstimatedMemoryBytes()` 原先只认带图的 `IndexHNSWSQ` / `IndexHNSWPQ`，免图对象落到 `else` 分支（`n×dim×4` 再叠加图开销）——即把"图边"算进了**没有图**的形态里，正好抵消了冷热转换想省下的那块内存。现改为：载荷先按量化码（`IndexScalarQuantizer` / `IndexPQ` 直接持有码表；带图的 twins 从 `storage` 取）或 float32 原向量计，**图项只对 `dynamic_cast<const faiss::IndexHNSW*>` 成立的对象叠加**（按实际驻留对象判断，而不是按 `config_`——`Load` 之后两者未必一致）。验证：`HNSWVectorIndexTest.GraphFreeEstimateOmitsTheGraph` 断言全精度免图的估计**精确等于** `n×dim×4`，且全精度对 / SQ8 对两种形态的"带图 − 免图"差值**完全相等**（差的正是图项）。

**一条已知小限制（记录，不修）**：副本 `InstallIndex` 只搬运 `.index` 与 `.ids`，不搬字节侧车 `.index.mem`，且 `loadFromDisk` 在内存里已有该版本条目时保留原有 `sizeByKey` 记账。于是"副本把带图产物换成免图产物"之后，其内存占用估算仍按旧（带图）值——**偏高**，只让该副本略微更早换出，是保守方向，不影响正确性。（注意：这条影响的是**原地换产物**的副本；本节点自己冷重建时走的 `doBuild` 会用 vecstore 回报的新 `mem_bytes` 更新记账。）

原（设计）描述：

**(a) 免图分层（与 v11 的"冷热分层"结合）**

图边开销（约 0.2–0.6 GB/百万点，与量化无关）在量化后成为内存主导项；构建耗时的大头也在"图插入"这一步（O(n log n) 带较大常数）。冷版本（非活跃、长时间无查询）不值得建图：直接使用 Faiss 的免图量化索引（`IndexPQ` / `IndexScalarQuantizer`，暴力扫描量化码，PQ 的 ADC 可查表 + SIMD 加速），构建耗时因省掉图插入而大幅下降；查询退化为 O(n) 扫描，n 不大时完全可接受，且量化损失同样有 rerank 兜底，不引入新的精度风险。热版本仍正常建图，冷热转换时按需重建对应形态。

**(b) 按需触发（惰性构建）【已实现】**

**实现进展**：

| 子项 | 状态 | 落地位置 |
|---|---|---|
| 只在活跃版本预建 | ✅ | `cmd/stratum` 的 `onVersionCreated`（`main.go`）：读复制元数据里的 `ActiveVersionID`，相等才 `EnsureIndex`；否则走"只拉数据" |
| 数据 READY 与索引 READY 分离 | ✅ | `DataPlane.FetchVersionData`（契约新增）：拉数据但**不建索引**；`sync.Follower.PullVersionData` 实现之（`PullVersion` 保留原行为）。数据仍必须到位——"所有副本持有数据"才是版本 durable 的依据 |
| 查询时的按需构建 | ✅ | `IndexManagerImpl.Search` 在"磁盘上无索引"时不再直接失败，改为分流；`tryBruteForce`（`internal/index/brute_force.go`）判定并处理 |
| 暴力扫描兜底 | ✅ | `bruteForceSearch` + `similarityScore`：COSINE / EUCLIDEAN / INNER_PRODUCT 三种度量统一成"越大越好"，小版本直接扫描返回、同时调度构建 |
| 大版本改为等待 | ✅ | 超过 `BruteForceMaxChunks`（默认 50 000，见 §10.4）时不扫描，调度构建后由 `acquire` 等待 |
| 删除墓碑 | ✅ | `tryBruteForce` 第一行做与 `loadFromDisk` 相同的 KB/version 删除检查——**新读取路径必须复制原路径的守卫**，否则删除会被绕过 |
| 空版本不碰 vecstore | ✅ **（实测修掉的死循环）** | 空版本（无 chunk，典型如 KB 初始版本 v1）原实现会调一次 `Build(empty)` + `Save`，期望"在 vecstore 侧建立索引条目"；但 vecstore 的 `AddChunksLocked` 对空 batch 直接返回而**不创建 Faiss 索引**，`Save` 于是永远报 `Save: no index has been built or loaded`（FailedPrecondition）。该错误被判为可重试 → 构建进入 **5 分钟重试窗口**，`loading[key]` 长期为真 → 期间该版本上的查询全部以 `index load timeout` 失败。空版本没有可检索内容，构建在 Go 侧即算完成（`build()` 直接返回成功），查询由 `tryBruteForce` 回答空结果。修复后 `integration` 的 `TestRealStack_TwoNodeReplication`（原先 10 次里失败 9 次、30s 超时仍失败）**连续 5 次通过**，`TestRealStack_ThreeNodeCluster_FaultTolerance` 连续 3 次通过 |

- **验证**：`internal/index` 4 例（未建索引改为扫描返回、空版本仍报 `ErrIndexNotReady`、超阈值不扫描而是等待、删除后仍被拒），全量 **23 个包 ok**。
- **一处实现教训**：最初漏了删除墓碑检查，导致"已删除的 KB 仍可查"（`TestIndexManager_DeletePreventsResurrection` 抓到）。新增一条读取数据的路径时，原路径上的守卫要一并复制——它们重要，而不是只属于原路径。

原（设计）描述：

**(b) 按需触发（惰性构建）**

现状是每个被创建的 version 无条件触发索引构建（`cmd/stratum/main.go:374` 的 `onVersionCreated` 拉到数据后经 `IndexManager.TriggerBuild` 构建，`internal/index/impl.go:253`；v1 §7 的表格即把 `onVersionCreated` 列为**现状**、把声明式 `EnsureIndex` 列为**目标**）。改为只在 version 成为 `ActiveVersionID` 或被显式访问（历史查询 / rollback）时才触发 —— 这样能过滤掉大量"创建后很快被取代、从未被查询"的中间 version 的构建开销。（原设计此处还提到"配合写入攒批"，但攒批已撤销，见 §8.7。）

需要拆分 Saga 完成判定：**数据 READY**（quorum 个副本 durable，可随时按需构建索引）与**索引 READY**（真正建完）分离。非活跃 version 只需数据 READY 即可视为 Saga 完成；活跃 version 需等到索引 READY。

首次查询延迟量级（未构建索引、临时暴力扫描 n 个 chunk 的代价；**数量级推理，非实测数字**）：

| n | 计算成本 | 磁盘读成本（主导项，取决于缓存命中） |
|---|---|---|
| 1 万 | < 1ms | 热缓存几十 ms；冷缓存 SSD 几十到几百 ms |
| 10 万 | 个位数 ms | 热缓存百 ms 级；冷缓存 SSD 秒级 |
| 100 万 | 十几 ms | 热缓存秒级；冷缓存 SSD 十几秒到 1 分钟 |

n 到十万级以上时暴力扫描兜底的意义已不大（与"等免图索引建完"同一量级），此时应直接短暂阻塞/拒绝查询；n 较小时可用"首次查询暴力扫描返回结果，同时异步后台补建索引"作为过渡。该临界 n 的占位值见 §10.4。

**(c) 增量复用（仅适用于纯追加场景）【已实现】**

**实现进展**：

| 子项 | 状态 | 落地位置与形态 |
|---|---|---|
| vecstore 侧"从产物继续构建" | ✅ | 新增 RPC `LoadForAppend`（`vecstore/proto/vecstore.proto`）+ `VectorIndex::LoadForAppend` + `HNSWVectorIndex::LoadLocked(path, op, final_state)`：与 `Load` 同一套读取/校验（sidecar、CRC、`ntotal`↔id 数一致），唯一区别是**终态**——`Load` 落 READY，`LoadForAppend` 落 **BUILDING**，于是后续 `AddChunks` 不再被"Save 封印"挡住，最后由 `Save` 封印。对象属于 `(kb_id, version_id)`（本版本），`path` 指向的**起点产物**（通常是父版本）由请求携带；响应回报 `base_ntotal`（起点产物有多少向量），供判墓碑 |
| Go 侧复用判定 | ✅ | `IndexManagerImpl.appendBase()`（`internal/index/impl.go`）：有父版本（版本链线性，§6）；**父产物仍在本节点磁盘**（`.index` + `.ids`，直接 stat 文件——不用 `ExistsIndex`，它在部分构建里返回 `Unimplemented`）；**父形态已知且与本次目标一致**（`builtGraphFree` 记录；重启后无记录 → 保守重建，避免"拿免图产物续出带图索引"）；**至少有一个新 chunk 可追加**（delta 空时复用只是拷贝）。**删除不再是否决条件**——见下一行 |
| 删除场景：免图真删 + 带图墓碑 | ✅ | 被删文档的向量对索引来说不再需要。**免图形态可以真删**：`buildFromBase()` 在 `LoadForAppend` 之后、`AddChunks` 之前调新 RPC `RemoveChunks`（`VectorIndex::RemoveChunks`：建 chunk→位置反查 → faiss `IDSelectorBatch` + `remove_ids` → **按压缩后的顺序重建 chunk-id 映射**，否则后续命中与 `Save` 都会指错 chunk），被删向量当场回收。**带图形态不能真删**（faiss 对 HNSW 没有 `remove_ids`），其向量只能留成**墓碑**：查不到（见下面"墓碑过滤"行），但仍占内存与 top-K 候选名额。带图形态、以及免图形态里"说不清 id 的祖先遗留墓碑"，统一由阈值兜底：`LoadForAppend` 回报的 `base_ntotal`（免图时再减去本次真删掉的）让 `buildFromBase()` 算出死向量占比 `≈ base_ntotal − (totalChunks − len(delta))`（**上界**，宁可多重建一次），超过 `AppendMaxDeadRatio`（默认 `DefaultAppendMaxDeadRatio` = **0.2**；yaml `index_manager.append_max_dead_ratio`，`1.0` = 永不因墓碑而重建）就放弃增量、改**全量重建**——顺带把墓碑清掉。这样"延迟全量重建"由**阈值**触发，无需为每个版本持久化墓碑计数 |
| `RemoveChunks` 的两条前置（vecstore 侧强制） | ✅ | ①**只对免图形态**：`HNSWVectorIndex::RemoveChunks` 先 `dynamic_cast<faiss::IndexHNSW*>` 判定，带图直接返回 `FailedPrecondition`（既不尝试真删，也不让 faiss 的异常穿过 gRPC 边界）；②**只在构建仍开着（BUILDING）时**：已 `Save` 封印（READY）的索引必须先 `LoadForAppend` 重开——与 `AddChunks` 同一套"Save 封印"纪律 |
| 重复 chunk 的防御（增量特有的） | ✅ | `delta` 是**集合差**（本版本 − 父版本），不是"产物内容差"，所以它可能指到一个 base 产物里已存在的 chunk——典型路径：某代删了 chunk（带图形态只能留成墓碑）→ 后代又把它加回来。chunk id 是**内容寻址**的（`SHA-256(chunk 文本 + embedConfigID)`，`internal/splitter/sliding_window.go:77`）⇒ 同一 id 必然同一向量 ⇒ `AddChunksLocked` 对已在索引里的 id **直接跳过**（不重复插入、也无须覆盖写），并在 `Reset`/`Load`/`RemoveChunks` 处同步维护 `known_chunk_ids_` 镜像（所以"删掉再加回来"仍能插入）；对称地，`RemoveChunks` 删的是该 chunk 的**每一份**向量（老产物可能列了两次），不是只删第一个匹配位置 |
| 墓碑过滤由谁负责 | ✅（既有路径，非本轮新增） | **查询路径本来就挡住了墓碑**（`service/query.go`）：每个命中先 `chunkDocMapper.ListDocIDs(kbID, chunkID)` 反查文档 → per-version bloom + `versionDocList` 权威确认 → `docStore.ReadAt(kbID, docID, versionID)` 读内容；而 `chunkdoc.DeleteByDoc` 在文档被删时清掉反查映射。所以墓碑只花内存与召回，不会漏出错误结果——这也正是"墓碑过滤"那一半**无需新代码**的原因 |
| Go 侧增量构建 | ✅ | `buildFromBase()`：`LoadForAppend(起点产物路径)` → **（免图）`RemoveChunks(本步删除的 chunk)`** → 判剩余死向量占比 → 只对 **delta** 分批 `AddChunks` → `Save` 到本版本路径。产物与全量构建**完全同构**：回调、§8.4 分发、磁盘保留策略都分辨不出它是怎么来的 |
| 失败回退 | ✅ | 增量只是优化：起点加载失败、追加失败、**真删失败** → 记 WARN 并**回退到全量构建**（`Build` 自身会 Reset 覆盖）；"墓碑过多"走哨兵 `errAppendTooManyTombstones`，记 **Info**（那是策略选择，不是故障）。都不会让版本构建失败 |
| 装配 | ✅ | `cmd/stratum/main.go`：`indexMgr.SetVersionParentGetter(...)` 从 Raft 版本元数据读 `ParentVersionID`（与 `onVersionCreated` 判活跃版本同一只读来源）；`integration` 的 `realNode` 同样装配，集群层真的走这条路径。未装配时行为与从前一致（每次全量构建） |
| 估值口径 | ✅ | 增量产物的内存记账仍优先用 vecstore 回报的 `mem_bytes`（覆盖起点产物 + delta 的驻留结构），取不到才退化为"父版本 sidecar + delta 载荷" |
| 一处需知道的权衡 | ⚠️ | 增量把版本的 vecstore 条目留在 **BUILDING** 的时间比纯 `Build` 更长（要先把起点产物从磁盘读回来再追加）。多副本并发构建/安装同一版本的场景下，这个更长的窗口理论上更容易撞上既有的 `index load timeout` 时序脆弱点。一次 10 轮 A/B 实测：`TestRealStack_ThreeNodeCluster_FaultTolerance` 在启用增量时 4/10 失败、不启用时 3/10 —— 差异在该用例的噪声范围内，所以装配照常保留；但这也正是"增量必须是纯优化、且带全量回退"的另一个理由 |

**测试**：

- `internal/index`：`BuildReusesParentArtifactOnPureAppend`（`LoadForAppend` 命中父产物、`Build` 只调一次、`AddChunks` 载荷**恰好是 delta**）；`BuildRebuildsWhenThereIsNothingToAppend`（无新增 → 不复用）；`BuildReusesParentArtifactWhenDeletionsAreSmall`（删 1 / 加 1 → 仍走增量，墓碑 ≈0.17 < 0.2）；`BuildRebuildsWhenTombstonesExceedRatio`（墓碑 4/8 = 0.5 → 先加载再判定，随后全量重建）；`BuildReusesDespiteTombstonesWhenRatioDisabled`（阈值 1.0 → 墓碑再多也增量）；**`BuildRemovesDeadChunksFromGraphFreeIndex`**（免图形态：`RemoveChunks` 载荷**恰好是本步删除的 chunk**，追加载荷恰好是 delta）；**`BuildDoesNotRemoveChunksForGraphedIndex`**（带图形态：同场景不调 `RemoveChunks`，墓碑留着由阈值兜底）；**`BuildFallsBackWhenRemoveChunksFails`**（真删失败 → 回退全量、版本仍 READY）；`BuildFallsBackWhenAppendReuseFails`（起点加载失败 → 版本仍 READY）；`BuildSkipsReuseWhenParentShapeIsUnknown`（重启后无形态记录 → 不复用）。
- `vecstore`：`LoadForAppendContinuesABuildFromAnArtifact`（追加前后两批点都可检索；`TotalVectors()` 在加载后 = 起点产物的向量数、追加后 = 起点 + delta；产物可被独立 `Load` 回来，满足跨节点分发前提）；**`RemoveChunksCompactsGraphFreeIndexAndKeepsTheMapping`**（免图真删：被删的查不到、留下的仍以**自己的 id** 命中、`TotalVectors` 减少、`Save → Load` 往返后映射依然对得上——这条专门盯 remove_ids 的 id 重排）；**`RemoveChunksRejectsSealedAndGraphedIndexes`**（已封印的索引与带图 HNSW 都被拒、索引内容不变）；**`AddChunksSkipsChunksAlreadyInTheIndex`**（重复 id 直接跳过：`TotalVectors` 只增加新 chunk 的数量，`Save → Load` 后仍如此，且被重复提供的 chunk 仍以自己的 id 命中）；**`RemovedChunksBecomeAddableAgain`**（删掉的 chunk 能重新加回来——盯 `known_chunk_ids_` 镜像与删除的同步）；**`RemoveChunksDropsEveryCopyOfAChunk`**（手工造"同一 id 列两次"的 legacy 产物：删除返回 2 而非 1，`TotalVectors` 归零）。

**Faiss API 核实结论（已在 vendored faiss 1.9.0 上用最小用例实测，用例留在 `vecstore/test/hnsw_index_test.cpp` 长期回归）**：

| 核实项 | 实测结论 | 用例 |
|---|---|---|
| HNSW 能否对**已写盘、再读回**的索引继续 `add` | ✅ 支持（新点接旧 id 续排，新旧点都可检索）→ "以 version N-1 的产物为起点、只对 delta 增量 `add`"的**前提成立** | `FaissHNSWAppendOnLoadedIndexWorks` |
| HNSW 能否 `remove_ids` | ❌ 不支持：`IndexHNSW` 未覆写该方法，落到 `Index::remove_ids` 抛 `remove_ids not implemented`，索引内容不被改动。原因与原推测一致：HNSW 图里节点被邻居表引用，删除需修复连通性 | `FaissHNSWRemoveIdsIsUnsupported` |
| 免图形态能否 `remove_ids` | ✅ **支持且压缩存储**（`IndexFlatCodes` 家族：`IndexFlat` / `IndexScalarQuantizer` / `IndexPQ`，即 §8.6(a) 的 `*_FLAT` 形态） | `FaissFlatCodesRemoveIdsCompacts` |
| 本项目封装层能否"从产物继续构建" | ⚠️ 核实当时**不能**（`Load` 后是 READY，`AddChunks` 被拒）；这正是上面 `LoadForAppend` 补上的那一环，用例保留作对照 | `LoadedIndexRejectsAppendUntilReset` |

**两处对原设计的修正（实测才发现）**：

1. **免图版本可以真删**，不必只靠墓碑。原设计"含删除只能墓碑 + 延迟全量重建"的结论只对**带图 HNSW** 成立；免图形态走 `remove_ids` 是更干净的路径（删除即压缩存储）。于是"删除怎么处理"取决于该版本当时的形态：带图 → 墓碑 + 阈值重建；免图 → 真删。统一走墓碑反而更不精确。
2. **实现的第一处阻塞不在 faiss，而在封装层**——已按此落地：没有改 faiss 用法，而是给封装层加了 `LoadForAppend`（见上表）。

**一条容易被问到的交互（删除中间版本 × 增量复用）**：SINGLE 模式删除中间版本时，Raft 状态机会把它的子版本**重新接到祖父**上（`internal/raft/state_machine.go:264` 的 `reparentChildren(..., spliceParent(...))`；祖父不存在或也在删除中时 `spliceParent` 返回 0，被删者的子版本变成**根**）。因为 delta 每次都按**当前** `ParentVersionID` 现算（无缓存），被删版本自己那部分变更会**自动并入新的 delta**（本版本 − 祖父），不会漏算；而仍被本版本引用的 chunk 也不是孤儿（`internal/coordinator/gc.go:41` 的孤儿判据是"它映射到的文档是否全部消失"），不会被回收。**但"跨度变大、仍是增量"并非无条件**——四种情况会退回全量重建（结果不变，只是失去增量收益）：①祖父不存在或也在删除中（新父被置 0）⇒ 走 `appendBase` 的 `parent <= 0` 分支；②**祖父的产物不在本节点磁盘**（§8.6(b) 惰性构建下非活跃版本通常根本没有索引文件，这条最常见）；③祖父的形态记录缺失或不匹配（重启后无记录，或免图/带图不一致）；④本版本相对祖父没有新增 chunk（`delta` 为空）。另外跨度变大也会让 `dead` 相对祖父变大，更容易撞上 `AppendMaxDeadRatio`（默认 0.2）而触发重建。

**仍属未做**：

- **量化码本失配**：需训练的类型（SQ8 / PQ）在增量场景下新增 chunk 可能使旧码本略有偏移，免训练类型（SQ_BF16 / SQ_FP16）无此问题；可接受码本轻微失配，或隔几个 version 重新训练刷新。另注意：KB 的 quantizer 是**创建时固定**的（§8.6(a) 提到的结构性限制），所以跨版本复用必须同形态（父子版本同为带图或同为免图）——`appendBase` 正是照这条判定的。
- **已封印（READY）索引的事后清理**：`RemoveChunks` 只在构建开着（BUILDING）时可用，所以"某个已就绪的免图索引积了墓碑、想后台清理一次"目前走不通——需要先 `LoadForAppend` 重开、删完再 `Save`（会短暂进入 BUILDING）。当前靠阈值触发重建兜底；若将来真要做后台清理，得先想清楚"重开期间该版本的查询怎么办"。

### 8.7 写入攒批（debounce）【已撤销】

**撤销理由**：目标写入模式是**一次调用提交一批变更**（批量一次写），不是很多次小写入。攒批是为"窗口内到达的多次写入"准备的机制——在批量一次写的模式下，它**收益趋近于零，而可见延迟的代价照付**。因此这一层不再实现。

原设计（保留以备追溯）：现状每次写入调用各自触发一整套流程（Raft 共识往返、数据确认、控制层状态机记录、`ReportEpoch` payload 增长、chunk 去重检查），成本按"写入调用次数"计费；引入短暂攒批窗口（毫秒到秒级，或按累积 changes 大小触发），窗口内到达的多次写入合并为一次 `ProposeCreateVersion`，以降 version 产生频率、代价是可见延迟。

**连带影响**：

- §10.4 的"写入攒批窗口"占位值随之失效（已标注）。
- §10.5 记录的 ack 约束**不再有实施对象**，但它描述的风险模式（内存缓冲 + 提前 ack = 永久丢数据）以及"ack 必须卡在 commit 之后"这条要求**依然成立**——现状本就满足，故该节保留。
- 早先被否掉的"自适应窗口"（§10.5 提到）同样不实现，两者一并作废。
- 控制层 propose 压力因此失去了攒批这一条缓解途径；若将来写入调用频率成为瓶颈，应重新评估本节的撤销。

> **【若将来恢复攒批，有一条硬约束】**：攒批缓冲**只能建在当前 leader 进程里**（在 Router 把请求路由到 leader 之后那一层），不能落在"半路上先接到请求的某个 follower"上。否则该 follower 必须在窗口关闭时把攒好的一批 changes 转发给 leader 才能真正 propose —— 这会凭空多引入一个**丢失窗口**：follower 在转发前崩溃，这批 changes 就没了，而这次丢失是"直连非 leader"这个选择自己造出来的，不是 §10.5 原本认下的那个风险。缓冲钉死在 leader 之后，风险模型与 §10.5 保持一致：攒批期间 leader 崩了 → 客户端没收到 ack → 重试 → Router 重新发现新 leader → 新 leader 的缓冲区从空开始重新攒。**"攒批节点"这个抽象角色因此被钉成"当前 leader"，而不是"随便哪个先接到请求的节点"**（与 §7.13.1 同一条理由：`CreateVersion` 只有 leader 能真正 propose）。

### 8.8 索引构建失败产物的回收【并入自 `coordinator-selection-and-node-liveness-design.md` §6】

> **⚠️ 核实结论先说**：本节设计稿的**前提与当前实现不符**，方案没有作用对象（详见下方"核实结论"）。保留在此是为了记住这条待办与它的真实形态，而不是照抄一个扫不到东西的后台任务。

**要解决的问题**：候选选择是"按顺序试候选，失败换下一个"。用在索引构建（`EnsureIndex`）上：候选 A 被打中、本地切到 `BUILDING`、构建中途崩溃或超时 → 控制层换候选 B → B 成功、该 version 索引最终 READY。**整个过程 version 从未进入过 `FAILED_PERMANENT`**，§10.6（4）的清理广播不会触发，A 那份"没建完的东西"此后没有任何机制会碰它。这比"整个 version 判死"频繁得多——普通瞬时故障/超时重试即可触发。

**不采用的方案**：让控制层记录"曾在哪些候选上尝试过" —— 会重蹈 §7.13.4 记快照、§7.13.5 建权威健康表所拒绝的模式（记录本身会过期，等于给软状态强上一致性 bookkeeping，收益不成比例）。

**设计稿的方案（自超时兜底）**：后台定期扫描本地磁盘上处于 `BUILDING` 状态的索引产物（未被 `Save` 封印，即无 READY 标记/sidecar），若 `now − mtime > BuildAbandonTimeout`（占位值 **30 分钟**，依据"10 万 chunk 构建 5 分钟内完成"留 6 倍余量）则删除该半成品、并把本地状态复位为"无此 version 的构建记录"。**判据必须是磁盘文件 mtime，不能是进程内计时器**——候选节点自身崩溃重启后内存计时器随之消失，mtime 天然跨重启成立。其哲学是"不确定时保守地自己收拾，不指望一个可能根本不会发出的外部通知"，与 §7.13.5、§7.13.4 一致。

**核实结论：该方案在当前实现下没有作用对象。**

1. **构建全程在内存**：`HNSWVectorIndex` 的 `Build`/`AddChunks` 不写任何文件（源码里只有一句 "sealed by Save" 的注释）；`grpc_service.cpp` 的 `Build`/`AddChunks` 同样不落盘，只有 `LoadForAppend` 会读 `request->path()`。
2. **唯一的落盘是 `Save`，且已是原子写**：`Save` 先写 `path + ".tmp"` 与 `ids_path + ".tmp"`、算 CRC32，再 rename 就位——注释原文："a crash or a write failure can therefore never leave a torn index"。因此**不存在**"磁盘上那份没建完的半成品索引"（§8.3 曾记的"索引写入非原子、无 checksum"已被阶段 ③ 消除）。
3. **`AddChunks` 不刷新任何产物文件 mtime**（它不写盘），所以"构建中持续活动 ⇒ mtime 刷新"不成立，上述 mtime 判据扫不到东西。

**真正存在的残留**（按危害排序）：

| 残留 | 位置 | 何时发生 | 现有处理 |
|---|---|---|---|
| 被放弃的 `BUILDING` 索引对象 | vecstore **内存** | 候选被静默放弃（version 在别处成功） | 进程重启即清；该 version 若再收到合法构建请求，`Build` 自身会 `Reset` 覆盖；若整个 version 判死，由 §10.6（4）的 `Discard` 清理 |
| `.tmp` 垃圾文件 | 磁盘 | 仅在 `Save` 执行**中途**崩溃 | 下次 `Save` 覆盖同名 `.tmp`；不影响 `EnforceDiskRetention`（它只统计 `.index`/`.index.ids`/`.index.mem`） |

**若仍要兜底**：唯一"有对象"的是 `.tmp` 垃圾（按 mtime 清理孤立 `.tmp` 可行）；"内存残留"由重启与 `Build` 覆盖天然自愈，专门加扫描器收益有限。**结论：本节的落地应改为"清 `.tmp` 垃圾"或暂不做**，而不是扫描 BUILDING 产物。

---

## 9 读路径服务站

**状态**：草案、未实现。来源：`storage-coordination-and-service-station-design.md` §4（已整合）。

### 9.1 为什么要引入该层

读路径绕开中转层的三个风险：

1. **返回过时结果而不自知** —— 某节点因分区/暂时落后，`localVersion[kb]` 落后于当前活跃版本，但本地数据完整、会正常应答查询，调用方无法感知返回的是旧版本结果，直接损害 MVCC 版本语义最看重的 as-of 精确性；
2. **鉴权 / 多租户隔离缺失** —— 数据面 `Search` 接口若不涉及调用方权限校验，任何能直连存储节点的调用方即可查询任意 kbID；
3. **恢复窗口期间读的语义未定义** —— 节点重启后到 `ReportEpoch` 之间的窗口，本地状态尚未完成自我校验，缺少显式的"暂不可信"标记。

### 9.2 定位

独立于控制集群之外的一层：**无状态、可水平扩展、查询结果经过它中转**。

数据经过它不违反"控制层轻量"的前提 —— 它根本不是控制集群的一部分，不需要参与 Raft 共识，可以像标准无状态反向代理（Nginx / Envoy 模式）一样线性扩容应对 QPS 和带宽。

### 9.3 职责

1. **路由表缓存**：本地缓存"`KB + version` → 可服务节点列表"，从控制层（通过类似 `ReportEpoch` 的机制）异步刷新，不是每次查询都问控制层；
2. **新鲜度凭证校验**：转发查询前附上"当前应看到的版本号 / epoch"，存储节点核对本地 `localVersion[kb]` 是否达标，不够则拒绝 —— 把 §9.1 风险 1 从"悄悄发生"变成可检测、可拒绝的显式条件；
3. **负载均衡**：一个 version 有多个 READY 副本时，决定发给哪一个；
4. **故障转移**：某节点因凭证不达标或不可达而拒绝，服务站在同一客户端连接内部换下一个候选节点重试，客户端无感；
5. **鉴权**：租户 / 权限校验收敛到这一个关卡，存储集群可以完全不对外暴露，只有服务站能访问；
6. **健康检查 / 熔断**：探活存储节点，路由表标记不健康节点。

### 9.4 【对齐】现状 `internal/router` 已有的骨架

现有路由层（`internal/router/` 目录共 835 行含测试，其中 `router.go` 161 行；入口 `cmd/stratum-router`）已实现下表前三行：

| 职责 | 现状 |
|---|---|
| 写转发 leader | ✅ `forwardWrite`（`router.go:89`）：每次 `LeaderNow` 重新发现、不缓存；被拒或节点不可达则换 leader 重试 |
| 读负载均衡 | ✅ `forwardRead`（`router.go:111`）：round-robin |
| 故障转移 | ✅ `tryAll`（`router.go:130`）+ `isRetryableErr`（`router.go:152`） |
| 路由表缓存（`KB + version` → 可服务节点） | ❌ 无（只按方法类型分流，不按版本/副本就绪度路由） |
| 新鲜度凭证校验 | ❌ 无 |
| 鉴权 | ❌ 无（三个外部服务直接暴露，鉴权靠部署隔离） |
| 健康检查 / 熔断 | ❌ 无（仅靠转发失败重试） |

因此本章是**升级既有组件**，而非从零新建。

### 9.5 待细化的工程点（已确认要做，细节未定）

- **连接管理**：每个存储节点一条共享 gRPC channel（HTTP/2 多路复用，不是连接池开多条）；建立于节点首次进路由表、关闭于节点被移除路由表；健康检查心跳复用同一 channel；
- **超时预算传播**：客户端 deadline 直接透传（gRPC context deadline），服务站计算 `remaining = deadline - now()`，转发时设为下游 RPC 的 deadline，节点内部处理（含暴力扫描兜底）不得超出；重试链条用**次数上限 + 剩余预算**双重限制，谁先耗尽就停；
- **延迟预算核算**：调用链 客户端→服务站→存储节点→服务站→客户端，公式骨架 = 服务站路由决策开销（可忽略）+ 网络往返 ×2 + 存储节点检索耗时（命中索引 vs 暴力扫描，见 §8.6b）+ 故障转移预留（至少一次重试的时间片）；具体数字待真实环境测；
- **背压 / 熔断**：每节点维护在途请求数；标准三态熔断（`closed` / `open` / `half-open`），慢请求比例或错误率超阈值触发 `open`，冷却后 `half-open` 试探性放量；熔断状态是服务站**本地**状态、实例间不同步（服务站无状态水平扩展的前提要求如此）。

---

## 10 遗留事项收敛

**状态**：已定稿（**v2** —— 2026-09 补全写事务相关的两条）、未实现。来源：`unresolved-issues-resolution-final.md`（收敛 `storage-coordination-and-service-station-design.md` §5）。

| 原遗留事项 | 结论位置 |
|---|---|
| Saga 永久失败终态 | §10.1 |
| 索引服务能力阈值归属 | §10.2 |
| 4.4 节四个工程细节 | §10.3（内容并入 §9.5） |
| 一致性窗口量级约束 | §10.4（占位值） |
| 写入攒批的隐含风险（v2 新增） | §10.5 |
| 判死后的物理数据清理与迟到上报（v2 新增） | §10.6 |

### 10.1 Saga 永久失败终态

**判定权**：控制层拥有终态判定权（重试次数 + 超时，与它一直在管的 Saga 编排逻辑一致）。存储层每次失败上报时附一个原因分类：

```go
type FailureReason int
const (
    FailureTransient   FailureReason = iota // 网络/节点重启/磁盘满——换节点大概率成功
    FailureFatalGlobal                      // 数据全局不可用/KB已删除/配额拒绝——重试无意义
)
```

- `FailureFatalGlobal`：不等重试预算耗尽，直接短路进终态；
- `FailureTransient`：计入重试预算，按次数 / 超时逻辑处理。

两条路径通向同一个终态，只是谁先触发的问题。

**终态定义**：新增 `FAILED_PERMANENT`，数据侧和索引侧各自独立拥有（索引失败不代表数据不可用）。

> **【对齐·已定】** `FAILED_PERMANENT` **取"新增枚举值"**：`types.IndexStatusFailedPermanent`（proto `INDEX_STATUS_FAILED_PERMANENT = 3`），与 `Failed` 并存（`FAILED` = 可重试，`FAILED_PERMANENT` = 终态）。见 §12.3 冲突 #5。

**人工介入的最小闭环**：`FAILED_PERMANENT` 必须记录失败原因链（现在就要留的字段）。预留但暂不实现：`ListFailedVersions(kbID)` 只读查询、`ForceRetryVersion` / `ForceAbandonVersion` 运维操作接口。

**实现进展（本版已完成判定与暴露，人工介入接口按要求预留）**：

| 子项 | 状态 | 落地位置 |
|---|---|---|
| `FAILED_PERMANENT` 枚举 | ✅ | `types.IndexStatusFailedPermanent`、proto `INDEX_STATUS_FAILED_PERMANENT = 3`、`String()` → `FAILED_PERMANENT` |
| 原因链字段 | ✅ | `VersionMeta.FailureReason` + `FailureCount`（随 Raft 快照往返） |
| 终态命令 | ✅ | `cmdMarkVersionFailedPermanent` → `applyMarkVersionFailedPermanent`（幂等、校验 KB 归属）；`RaftNode.ProposeMarkVersionFailedPermanent` |
| 判定权（计数 + 预算） | ✅ | `ControlPlane.ReportVersionFailure` → `LocalControlPlane`：按 `kbID\x00versionID` 计数，`DefaultFailureBudget = 5`（可 `WithFailureBudget` 覆盖），**成功上报即清零** |
| 失败上报入口 | ✅ | `LocalDataPlane.WriteVersionData` 的两个失败点各上报一次，原因区分阶段（`local write failed` / `replication failed`） |
| 运维可见 | ✅ | `GetSystemStatus.failed_permanent_versions`（`FailedVersion{kb_id, version_id, reason, failure_count}`），在既有版本遍历中收集 |
| `FailureFatalGlobal` 短路 | ✅ | `types.FailureClass`（`FailureTransient` / `FailureFatalGlobal`）；`LocalControlPlane` 对致命类**跳过预算直接判死**；`classifyLocalWriteFailure` 把"KB 已删 / 输入被拒 / 版本不存在"归为致命，"磁盘满 / 复制不足"仍是瞬态 |
| 按 KB 的重试预算 | ✅ | `ControlPlane.SetFailureBudget(ctx, kbID, n)`；`DataPlane.SetDurabilityPolicy` 把 `DurabilityPolicy.MaxFailures` 转给控制层（`LocalControlPlane.kbBudgets`），非正值表示"取消覆盖、回到默认" |
| 人工介入接口 | ❌ | 按本节要求预留（`ListFailedVersions` / `ForceRetryVersion` / `ForceAbandonVersion`） |

- **验证**：`internal/plane` 21 例（预算边界、成功清零、按版本/按 KB 隔离、两个阶段各自上报、无 control 时写入照常失败、per-KB 预算覆盖与清除、`SetDurabilityPolicy` 的转发与 no-op、致命类短路预算、瞬态类仍按预算、分类函数逐例、致命/瞬态分类端到端上报）、`internal/raft` 4 例（终态、幂等、拒绝未知与跨 KB、快照往返）、`service` 2 例（status 可见性、可重试的 `FAILED` 不算终态）、`integration` 1 例端到端（上报 → 判死 → `GetSystemStatus` 可见 → 状态机一致）。
- **取舍**：计数是**进程内**状态，重启即清零 —— 宁可多 retry 也不早判死（重试是可恢复的方向，预算本身仍是 §10.4 待标定数字）。真正需要持久化计数的是"判死后的清理"（§10.6），那是另一件事。

### 10.2 索引服务能力阈值归属

**结论**：声明式配置管告警，实际路由完全不查它。

- **声明式部分（控制层，KB 级配置）**：`IndexServingReplicaMin`，与 `DurabilityPolicy.Replicas` 同级但独立；唯一用途是当 `READY` 副本数 < 该值时上报 `DEGRADED`。这个基线只能来自声明式配置（存储层不知道业务对该 KB 的读 SLA 预期）；
- **动态部分（服务站，运行时）**：实际路由决策永远基于自己实时观测到的健康状态，与该阈值无关。

二者管不同的事，不互相覆盖。

### 10.3 4.4 节四个工程细节

连接管理 / 超时预算传播 / 延迟预算核算 / 背压熔断 —— 已并入 §9.5，此处不重复。

### 10.4 一致性窗口量级约束【占位默认值，待压测替换】

以下数字全部是占位值、不是最终结论，标准是"能把机制跑通、量级合理"，不是精确计算：

| 窗口 | 占位值 | 依据 |
|---|---|---|
| ~~写入攒批窗口（§8.7）~~ | ~~200ms 或累计 4MB~~ | **已撤销**：攒批不实现（§8.7），该占位值失效 |
| per-KB 在飞写入上限（§7.7） | 8（`DefaultMaxInFlightWrites`；可按 KB 用 `DurabilityPolicy.MaxInFlightWrites` 覆盖） | 取值足以保留常规写并发，又能在某个 version 卡住时及早形成背压而不是无限积压 |
| 暴力扫描阈值（§8.6b） | 5 万 chunk（`DefaultBruteForceMaxChunks`；可按 KB 用 `IndexManagerConfig.BruteForceMaxChunks` 覆盖） | 与"等免图索引建完"同量级的临界点，取其偏保守一侧：超过就不扫描而等待构建 |
| 判死重试预算（§10.1） | 5 次失败（`DefaultFailureBudget`；可按 KB 用 `DurabilityPolicy.MaxFailures` 覆盖，或进程级用 `WithFailureBudget`） | 进程内计数、成功即清零，取小值以尽早暴露真故障而不是无限重试；致命类失败不走这个预算 |
| 暴力扫描临界 n（§8.6b） | n ≤ 5 万走暴力扫描 + 异步补建；n > 5 万直接阻塞 / 拒绝 | n=10 万已与"等免图索引建完"同量级，5 万取在可接受与不划算之间偏保守 |
| quorum 确认延迟 / 协调者接管超时（§7.3） | 副本本地超时 200ms | 局域网直连场景，覆盖正常 RTT 抖动 |
| epoch 恢复窗口（§7.8） | 5 秒 | 节点重启后拉取 peer `localVersion` 并凑 quorum 的往返 + 重试余量 |
| 冷版本阈值 / 评估周期（§8.6a） | 默认**关闭**（`cold_threshold_ms = 0`）；一旦启用，评估周期默认 1 分钟（`DefaultColdSweepInterval`） | 这是**策略开关**而非性能常数：开启前的历史行为是"每个版本都建完整 HNSW 图"，故不作为默认值强加给既有部署；评估周期只决定"变冷→换形态"的最大滞后，且扫描成本仅为一次内存 map 遍历 |

这四个数字集中放进一个配置结构体（如 `ConsistencyBudget`），不散落成代码里的魔法数字，方便以后用真实压测数据一次性替换。

> **【对齐】** 现有配置结构（`cmd/stratum/main.go` 的 `appConfig` 与 `loadConfig`）只有 node / raft / storage / vecstore / write_coordinator / delete_coordinator / index_manager / bloom_filter / gc 等字段（见 `configs/config1.yaml`），**没有任何副本、quorum、consistency 相关字段** —— `ConsistencyBudget`、`IndexServingReplicaMin` 仍是全新配置项。`DurabilityPolicy` 已作为类型存在（`internal/plane`，含 `Replicas` 与 `MaxFailures`），但尚无配置来源与读取方；`DefaultFailureBudget` 目前是代码常量而非配置。

### 10.5 写入攒批的 ack 约束【v2 新增】

**风险**：攒批（§8.7）发生在 Raft propose **之前**，缓冲区全在内存里，没有 WAL、没有持久化。攒批节点在窗口关闭、真正调用 `ProposeCreateVersion` 之前崩溃，缓冲区内容直接丢失。

**这不是新增风险**，而是 §7.4 的客户端 at-least-once 兜底本该覆盖、此前却没显式言明的场景。

**强制约束**：攒批节点在窗口关闭、Raft commit 成功之前，**绝不能**向客户端返回任何"写入成功"确认 —— ack 必须卡在 version 真正 commit 之后才发出。

在该约束下：客户端在收到确认前不会丢弃原始数据，按 at-least-once 持续重试，而内容寻址让重试天然幂等、代价很低。攒批窗口只是把 §7.4 那段"客户端在 durable 确认前不得丢弃数据"的责任窗口，从"写入请求 → quorum 确认"往前延长到"写入请求 → 攒批窗口关闭 → Raft commit"——**责任模型没变，只是变长了**。

**若违反**（提前 ack）：客户端会认为写入已成功并丢弃本地副本，一旦攒批节点此时崩溃，这批数据永久丢失，无法挽回 —— §7.4 整套兜底都建立在"客户端还没被告知成功"这个前提上。

> **【对齐】** 现状 `WriteCoordinator.Execute` 在 WAL COMMIT 之后才返回 versionID，service 层随后才回 ack，因此现状（尚无攒批）**已满足**该约束；这条约束是给将来实现攒批时立的门槛。

**窗口策略**：**固定窗口，不做自适应**（v2 §4.2）。讨论过的"短探测期未命中并发写入则提前关闭"方案**决定暂不实现**；当前固定 200ms / 4MB（§10.4）保持不变，待写入并发密度摸清后再评估。

### 10.6 判死后的物理数据清理与迟到上报【v2 新增】

**背景**：控制层判定写入进入 `FAILED_PERMANENT` 时，物理数据可能已经真的写成功（某个副本实际落盘了，只是确认消息丢失、超时导致控制层以为失败）。这份数据不会被任何查询路径读到（可见性由控制层状态驱动，与数据是否物理存在无关），但也没有人会去回收它。需要补两处动作：

**（1）进入 `FAILED_PERMANENT` 必须触发清理广播**：

```
判定进入 FAILED_PERMANENT
  → 记录失败原因链（§10.1 原有逻辑）
  → 新增：对该 version 当初 fan-out 的全部候选副本节点广播 DeleteByVersion
```

必须覆盖"**全部候选节点**"，而不是"确认写成功的那几个" —— 控制层判定失败时并不知道数据实际落到了哪些节点，清理指令宁可对没收到数据的节点发一次空操作，也不能漏掉真正落盘的那个。

**（2）迟到的 `ReportDataDurable` 必须做状态校验后再决定是否应用**：

```
收到 ReportDataDurable(versionID, …)
  → 检查 version 当前状态
    → 仍在"等待数据确认"：正常应用，标记 READY
    → 已是 FAILED_PERMANENT（已判死 + 已清理）：丢弃，不做任何状态变更
    → 已是 READY（如换协调者重试后成功）：丢弃，避免重复处理
```

不能省 —— 迟到确认若被当成"现在写成功了"直接应用，会出现比原问题更糟的语义错乱：客户端可能早已判定写入失败并走了补偿逻辑，系统却让这份数据突然变得可查。

**（3）分类澄清**：控制层收到写入超时、无法确定是否真的写成功时，**不需要**给 `FailureReason` 新增分类，按 `FailureTransient` 处理即可 —— 数据内容寻址天然幂等（§7.4 的 at-least-once 兜底），重试代价低，不确定时默认按"可能是暂时性问题"处理更安全。真正的 `FAILED_PERMANENT` 判定是重试预算 / 超时耗尽之后的事，本节两条改动处理的正是"判死之后"这个时间点遗留的物理数据与迟到消息。

**（4）清理广播需覆盖索引产物，不止 docstore 数据**：广播内容需加一项 —— 候选节点收到后，除了清理 docstore 数据，也要检查并清掉自己本地任何跟该 version 相关的**索引构建残留**（不论当前是 `BUILDING` 还是其他残留状态）。

索引构建的候选选择本身还有一层"版本从未判死、但候选被静默放弃"的常规情况（构建到一半失败、控制层换了下一个候选成功）：这种情况**不会**触发本节的终态清理广播，需要候选节点自己靠**本地文件 mtime 自超时**兜底 —— 该兜底与本节同步启用，其设计写在 `coordinator-selection-and-node-liveness-design.md` 第 6 节，**但该节目前尚未写出**（见下表）。

**实现进展（本节两条均已落地）**：

| 子项 | 状态 | 落地位置 |
|---|---|---|
| （1）清理广播 | ✅ | `DeleteVersionData` RPC + `PushHandler.DeleteVersionData`（`WithVersionDataDropper`）+ `sync.VersionDataCleaner` + `LocalDataPlane.DropVersionData`：**覆盖全部候选副本 + 本地**，不区分"是否确认过" |
| （1）触发点 | ✅ | `ControlPlane.ReportVersionFailure` 返回 `terminal bool`；`LocalDataPlane.reportFailure` 收到终态即执行清理 |
| （1）本地回收 | ✅ | `WriteCoordinatorImpl.DropVersionStorage`（`DocStore.DeleteByVersion` + `VersionDocList.DeleteByVersion`，两者都是幂等前缀删） |
| （1）游标 | ✅ | 清理后把 `localVersion` 推过该版本：它永远不会有数据，否则那个空洞会被后续 backfill 当成待补 |
| （2）迟到上报校验 | ✅ | `applyUpdateVersionSummary`：版本已是 `READY` / `FAILED_PERMANENT` 时**丢弃、不改状态**。放在状态机而非报告路径，因为"已定局"是复制状态里的确定性事实，且写路径无需额外读 |
| 清理的重试 | ✅ | `cleanupQueue` + `StartCleanupRetries`：广播或本地删除失败即入队（按 `kbID+versionID` 去重，重复失败不堆叠），后台每 `cleanupRetryInterval`（30s）重试一次；超过 `cleanupRetryAttempts`（5）次**放弃并记 error**——队列有界，否则它自己就变成一个新的泄漏 |
| 清理队列的持久化 | ❌ | 待重试项在内存里，进程重启即丢；漏掉的那份孤儿数据只能靠下一次判死或 GC 兜底 |
| 控制层主动重传 | ❌ | 仍受 §7.12 ③ 限制（控制层不能主动动作，只能暴露状态） |
| **（4）索引产物清理** | ✅ | `WriteCoordinatorImpl.DropVersionStorage` 现在除 docstore 两处外，还会调 `IndexManager.Discard`：evict 内存条目 + 置版本墓碑（关闭 Load-RPC 复活竞态）+ 调 `Reset` RPC 清 vecstore 侧索引对象 + 删除 `<IndexDataDir>/index/<kbID>/<versionID>.index` 及其 `.ids`、`.index.mem` sidecar（全部幂等；对从未构建过的索引，vecstore 侧是 no-op）。**无需新增 proto、无需改 C++**。原本的缺口是"能力已有、终态清理路径没接"——`Discard` 此前只被 `delete_version_impl.go`（删除版本路径）调用。验证：`internal/coordinator` 2 例（Discard 被调用且重试幂等；未装配 `IndexManager` 时仍完成其余清理） |
| **（4）候选被静默放弃的自兜底** | ⚠️ | 设计见 **§8.8**（并入自 `coordinator-selection-and-node-liveness-design.md` §6）。**核实结论：该方案在现状下没有作用对象** —— 构建全程在内存（`Build`/`AddChunks` 不写盘）、唯一的落盘 `Save` 已是 `.tmp`+rename 原子写且带 CRC32，因此不存在"磁盘上的半成品产物"；`AddChunks` 也不刷新任何产物文件 mtime，故 mtime 判据扫不到东西。真正的残留是"被放弃的 `BUILDING` 索引对象"（在 vecstore 内存，重启 / `Build` 覆盖 / 本节（4）的 `Discard` 均可清）与 `Save` 中途崩溃留下的 `.tmp` 垃圾（会被下次 `Save` 覆盖）。若仍要兜底，唯一"有对象"的是按 mtime 清理孤立 `.tmp` |

- **验证**：`internal/plane` 6 例（覆盖全部候选、广播失败仍完成本地清理、游标推进、未接线时显式报错而非静默、终态触发清理、非终态**不**清理）、`internal/sync` 3 例（本地回收与 `dropped` 标志、无接线时不算错误、drop 失败上抛给广播方）。
- **注意**：`DropVersionData` 未接线时返回 "not wired" 错误而不是静默成功 —— 一个悄悄什么都不做的清理，正是孤儿数据活下来的方式。

---

## 11 实施路线（v13 增量）

承接 v12 §4 的形态：按依赖顺序分阶段，每阶段给出验证方式与完成标准。**已完成：阶段 ⓪（决策补齐）、阶段 ①（契约抽取）、阶段 ②（版本链线性化）、阶段 ③（索引文件原子写 + checksum）、阶段 ④a（写事务拆分 + 幂等键）、阶段 ④b（push 通道 / fan-out + quorum / 游标与追链 / 游标交换与选源 / 无数据 version 检测），§7.7 的 per-KB 在飞写入上限与排队（§7 至此整节闭合）、§7.3 的协调者崩溃接管（含"任何节点都能发起提议"的 propose 通道）、§7.8 的恢复时安全 durable version、§7.9 的 `ReportEpoch` payload 修订、§10.1 的 Saga 永久失败终态判定与暴露、§10.6 的判死后清理；其余未开始。**

### 阶段 ⓪：决策补齐【已完成】

| 事项 | 结论 | 依据 |
|---|---|---|
| v1 文档处理 | 已补入 `control-data-separation-design.md`；本文 §12.1 逐条核对，4 处引述偏差已更正 | 会话确认 |
| 数据面 / Raft 边界 | **独立于 Raft 的协议**：存储层内部自协调，不承载 Raft 元数据；具体见 §7（临时协调者 + fan-out + quorum） | v1 §9 #1「独立协议（否则又不解耦）」 |
| 索引构建拓扑 | **改为"建一次、分发 N 份"**，撤销 v1 §9 #6 与 v11:244 的"每副本独立构建" | 会话确认，见 §8.4 |
| 目标层级 | **集群级分离**（控制集群 + 自治存储集群，即 v1 §1.3/§3.1 的原意），不是仅"Go 控制 / C++ 数据"的代码分层——后者当前已存在。v13 §11 的阶段 ①④⑤⑥ 均按此方向保留 | 会话确认 |

### 阶段 ①：契约抽取（同进程实现）【已完成】

**v1 演进路径的阶段 1，必须先做**：抽出 `DataPlane` / `ControlPlane` 两个契约（§7.0），**先以同进程实现**，让控制层不再直接探磁盘。v1 §7 的原话是"零分布式代价，先把边界划死（此后控制层不再直接探磁盘）"，并警告跳过这步会让"现有物理假设在拆分时反噬"。

v1 §7 给出的接口抽取清单（现状 → 目标）：

| 调用点 | 现状 | 目标 |
|---|---|---|
| `internal/index/impl.go:573` `IndexExists` | 直接探本地磁盘 | 改查存储层逻辑状态 |
| `internal/index/impl.go:592` `loadFromDisk` | 直接读本地文件 | 移入存储层内部 |
| `cmd/stratum/main.go:374` `onVersionCreated` | 触发本节点全量拉取 | 改声明式 `EnsureIndex` |
| `internal/sync/follower.go:55` `PullVersion(leaderAddr,…)` | 固定从 leader 拉 | 移入存储层内部复制 |
| `cmd/stratum/main.go:237` 构建回调 | 直接 propose 状态 | 改 `ReportIndexReady` |
| `cmd/stratum/main.go:612` `reconcileIndexStatus` | 用本地磁盘反推 | 改 `ReportEpoch` 对账 |

- **验证结果**：`go build ./...`、`go vet ./...` 全过；`go test ./...` 20 个包 ok（integration 里 2 个 `TestRealStack_*` 失败经单独重跑与 `-count=3` 确认是 flaky，与本阶段无关）。
- **完成标准**：✅ v1 §7 的 6 项全部改走契约 —— ① `IndexExists`（调用方不再探盘，降为存储层内部能力）② `loadFromDisk`（本已是私有方法）③ `onVersionCreated` → `DataPlane.EnsureIndex` ④ `PullVersion`（leader 解析、拉取、digest 校验退避全部内移）⑤ 构建回调 → `ControlPlane` ⑥ `reconcileIndexStatus` → `DataPlane.ReconcileIndexes` + `ControlPlane.ReportEpoch`。
- **实际改动**：新增 `internal/plane/`（`plane.go` 契约、`local_control_plane.go`、`local_data_plane.go`）；`cmd/stratum/main.go` 装配改为经契约（新增 `controlPlane` / `dataPlane`，启动 reconcile 移到 `dataPlane` 就绪后）；`cmd/stratum/main_test.go` 的 reconcile 决策表测试改走契约（断言未改，行为等价）。
- **收尾（已完成）**：v1 §7 清单之外同类的一处也已内移 —— `enforceRetentionAtStartup` 的磁盘保留策略成为 `LocalDataPlane.EnforceRetention`，`cmd/stratum` 不再持有 `EnforceDiskRetention` 调用；两个 Local 实现改用**窄接口**（`LocalControlPlane` 只需 `MetadataProposer`、`LocalDataPlane` 只需 `IndexStore`），并补上 `internal/plane` 的单测（契约转发与边界、`EnsureIndex` 的拉取/重试/初版本单次拉取、`ReconcileIndexes` 决策表与保留窗口跳过、`EnforceRetention` 对 active 版本的保护）。

### 阶段 ②：版本链线性化（§6）【已完成】

改动面（实际）：`internal/raft/state_machine.go`（`applyCreateVersion` 新增校验）、`internal/raft/mock.go`（`MockRaftNode` 同逻辑，防止测试替身与真实实现漂移）、`internal/raft/{impl,mock,state_machine}_test.go`（15 处断言分叉成功的用例改为严格链，含删除模式的全部 fixture）、`internal/coordinator/delete_version_test.go`（`KeepsRecordsForForkedDescendant` → `...ForLinearDeletion`）、`integration/integration_test.go`（`ForkedVersions` → `ForkRejected`；`ConcurrentCreateVersion` 期望改为"恰好 1 成功、4 拒绝"）、`README.md`（`:5`/`:7`/`:13`/`:96`/`:97` 与配套文档指针）、`state_machine.go` 的注释。

- **验证结果**：`go test ./internal/raft/...`、`./internal/coordinator/...` 全绿；`go test ./...` 全量 21 个包通过（首次跑出现 3 个快照/容错类失败，经 `-count=3` 复跑与单独重跑确认是 flaky，与本改动无关）；`go vet ./...` 通过；`gofmt -l` 干净；CI 的 `-race` 门禁（kvraft / raft / index）通过。
- **完成标准**：✅ 父版本已有子版本时 `CreateVersion` 返回 `ErrInvalidParentVersion`；README 与实现一致。

### 阶段 ③：索引文件原子写 + checksum（§8.3 前置）【已完成】

改动面（实际）：`vecstore/src/hnsw_index.cpp`（Save / Load 重写 + CRC-32 / fsync helper）、`vecstore/test/hnsw_index_test.cpp`（5 个新用例）、`vecstore/CMakeLists.txt`（RocksDB 传递依赖 shim，见下）。

- **Save**：两个文件先写 `*.tmp` → `fsync` → `rename` 就位（index 先、sidecar 后）→ `fsync` 目录；失败路径清理 tmp。两次 rename 之间崩溃会留下"新 index + 旧 sidecar"，由 checksum 检出。
- **sidecar 格式**：首行 magic `stratum-index-1` + dim + metric + **CRC-32** + chunk ids；旧格式（首行即 dim、无 checksum）仍可 Load。
- **Load**：校验 index 文件 CRC-32；`read_index` 后校验 `ids.size() == ntotal`（不匹配即拒，避免 chunk id 与位置错配）。
- **连带**：本地 vecstore 构建原本不可用（CMakeCache 指向已失效的 `/tmp/vecstore_cmake_aliases.cmake`；清掉后链接报 `-lgflags::gflags_shared` 等）——根因是 Ubuntu `librocksdb-dev` 的 `RocksDBTargets.cmake` 点名了自身未定义的 `X::y` 目标。已把 shim 固化进 `vecstore/CMakeLists.txt`（`if(NOT TARGET …)` 幂等，对 CI 无害）。
- **验证结果**：`cmake --build vecstore/build --target vecstore_tests` 通过；`ctest` **37/37 通过**（含 5 个新用例）。改动前基线为 31/32，唯一失败 `LifecycleStateTest.ConcurrentSearchResetLoadSmoke` 在改动后复跑中通过，确认是依赖调度机会的 flaky，非稳定失败。
- **注意**：vecstore 仍不在常规 CI（`.github/workflows/vecstore-cpp.yml` 需手动触发），无自动门禁。

### 阶段 ④：存储集群独立进程 + 副本拓扑 + 写路径 fan-out + quorum（§7.1–§7.4、§7.10）【部分完成】

对应 v1 阶段 2（存储集群独立进程、存储层内部重复制 —— "现有 sync 模型搬家"）+ 补充文档 §1–§2 的协调协议。

**实施时拆成两段（依赖顺序）**：

#### ④a：写事务拆分 + 幂等键【已完成】

- **写事务归存储层**：`Execute` 瘦身为「控制层 `ProposeCreateVersion` → `DataPlane.WriteVersionData`（BEGIN → 存储写入 → COMMIT）」，契约新增 `ResumeVersionWrite` 承接崩溃重放；`WAL.rebuildIndex` 改为**双向配对**（`BEGIN→VERSION_ID` 与反转顺序都能绑定），以支持"先分配 ID 再写 BEGIN"。
- **幂等键**：`CreateVersionRequest.client_request_id` → `command.ClientRequestID` → 状态机 `versionsByRequest`（命中即复用原 version、不分配；快照往返携带；版本/KB 元数据删除时清理）。这是 §7.12「无数据 version 必须显式补齐」的前提。
- **验证**：`internal/raft` 的 5 个幂等用例 + `integration` 的端到端用例（proto→service→coordinator→raft 全链路）。

#### ④b：push / fan-out / quorum / 游标 / 检测【部分完成】

| 子项 | 状态 | 落地位置 |
|---|---|---|
| push 通道 | ✅ | `PushVersionData`（proto）、`sync.PushHandler`、`sync.Pusher`、共享导出逻辑 `LeaderHandler.ExportVersion` |
| fan-out + quorum | ✅ | `LocalDataPlane.fanOut`（并发推送 + ack 计数，不足 quorum 不上报 durable）、`QuorumSize`；节点装配用 Raft 成员表作副本集 |
| 游标 + 追链 | ✅ | `localVersion`（只前进）、`backfillTo`（应用 V 前补 `(cursor, V)`） |
| 无数据 version 检测 | ✅ | `VersionPresence`（proto + `PushHandler`）、`PresenceChecker`、`service/dataMissingVersions`、`GetSystemStatus.data_missing_versions` |
| 协调者崩溃接管（§7.3） | ❌ | 已由「客户端重试 + ④a 的幂等复用」覆盖；副本互相接管是第二条路径 |
| `localVersion` 交换（§7.6） | ✅ | `LocalVersion` RPC + `LocalVersionQuerier`；追链改为按 peer 游标选源（缺口用持有缺口的 peer，目标版本仍用 resolver 的源） |
| §7.12 的 ③ 触发重传 | 🟡 | 控制层无法主动执行，只能暴露状态；前提（幂等键）已就绪 |
| §7.12 的 ④ 终态兜底 | ✅ | 依赖的 §10.1 `FAILED_PERMANENT` 已实现（判定 → 终态 → `GetSystemStatus.failed_permanent_versions`），§10.6 的**判死后清理**也已落地（广播 + 本地回收 + 迟到上报校验）；仍缺的是**控制层主动触发重传**（受 §7.12 ③ 限制） |

- **前置**：§7.12 的写事务跨层方案（已定，见上）。

- **验证**：三节点 Docker 集群测试（现 `integration/docker/`）需同步重写；单测覆盖 quorum 达成 / 不足 / 协调者崩溃接管。
- **完成标准**：quorum 达标才上报 `ReportDataDurable`；协调者崩溃后副本能接管上报。
- **规模**：大。

### 阶段 ⑤：`ReportEpoch` + `localVersion` 追链 + Saga 终态（§7.5–§7.9、§10.1）

对应 v1 阶段 3（epoch + block report，v1 称"生产分水岭"）。

改动面：proto 新增 `epoch` 通道；节点本地游标与 peer 互查；追链命中已删区间时的全量传输兜底（§6.4）。

> **注**：本阶段原列的 `FAILED_PERMANENT` 与原因链字段**已提前完成**（见 §10.1 的实现进展），阶段 ⑤ 因此只剩 epoch 追链与自愈广播。

- **规模**：大。

### 阶段 ⑥：索引分发 + 惰性构建 + 免图分层 + 增量复用（§8.3、§8.6）

改动面：索引文件传输 RPC + 副本 `Load`（**§8.4 已完成**）；Saga 完成判定拆分（数据 READY / 索引 READY）（**§8.6b 已完成**）；免图分层（§8.6a，**已完成**：C++ 侧免图量化形态 + 存储层冷热转换策略）；增量复用（§8.6c，**已完成**：`LoadForAppend` + 复用判定 + 失败回退 + 删除场景——免图 `RemoveChunks` 真删、带图墓碑阈值与延迟全量重建）。写入攒批（§8.7）已撤销，不在序列内。

- **注意**：同样属 C++ 改动，无常规 CI 门禁。
- **规模**：大。

### 阶段 ⑦：服务站扩展 + 工程细节（§9、§10.3）

改动面：`internal/router`（路由表缓存、新鲜度凭证、鉴权、熔断、连接管理、超时预算）。

- **规模**：中。

### 未排入本路线的 v1 后续阶段

- v1 阶段 4 的**冷热分层**已部分进入 §8.6(a)（免图分层）；**数据修复 / scrub** 与 **EC** 未排 —— v1 §6.4 明确 EC 与 chunk 内容寻址 / 去重、MVCC 零拷贝冲突，结论是"EC 排在路线最后"（见 §7.0）。
- v1 阶段 5（存储层分片）未排；v1 §9 #5 自己标注"阶段 5 再定；分片直接挑战'每版本完整索引'模型"。

### 门禁与回归约束

- 阶段 ④–⑦ 会大面积打破现有测试基线（373 个测试函数、26 个包、T4 三节点 Docker 集成测试），需同步维护；
- 阶段 ③⑥ 是 C++ 改动，而 vecstore 不在常规 CI 内（`.github/workflows/vecstore-cpp.yml` 手动触发），只能靠本地构建（`faiss` 为指向 `/home/lacas/lerning/faiss` 的符号链接）验证 —— 静默回归风险最高。

---

## 12 文档体系对齐、术语映射与冲突登记

### 12.1 文档族与 v1 引述核对

| 文档 | 定位 | 状态 |
|---|---|---|
| `Stratum_设计文档v11.md` | 与实现全面对齐的系统设计（基线） | 已落地 |
| `Stratum_设计文档v12.md` | 两段式检索 + 分级存储 | 已落地（状态标注过时，见 §12.3 #4） |
| `Stratum_设计文档v13.md`（本文） | v12 + 三份补充设计稿的整合 | 本稿 |
| `storage-coordination-and-service-station-design.md` | 存储协调 / 索引加速 / 读路径服务站的补充设计 | 草案，已整合进本文 §7–§9 |
| `unresolved-issues-resolution-final.md` | 上述草案 §5 遗留事项的收敛（**定稿 v2**：补 §1.4 判死后清理与迟到上报校验、§4.1 攒批 ack 约束、§4.2 固定窗口） | 已整合进本文 §10；**其中 §4.1/§4.2 的攒批部分随 §8.7 撤销而失效**（以本文为准），§10.5 保留的 ack 约束继续有效 |
| `version-linearization-decision.md` | 版本链线性化决策 | 已整合进本文 §6（**已实现**：`applyCreateVersion` 与 `MockRaftNode` 均含线性链校验） |
| `coordinator-selection-and-node-liveness-design.md` | 协调者选择 / 写完成后数据位置查询（`ReportEpoch` 聚合）/ 存储节点间局部健康视图 | 已整合进本文 §7.13；**除 7.13.3 的边界说明外全部未实现**，与 §8.5 有一处模型冲突（§12.3 #8） |
| `Stratum_接口设计v9.md` | gRPC / 内部接口与语义 | 与实现对齐 |
| `Stratum_设计目标.md` / `Stratum_测试顺序.md` / `Stratum_实现顺序.md` / `Stratum_代码风格.md` | 目标、测试顺序、实现顺序、代码风格 | — |
| `改动内容.md` | 开发日志 | — |
| `control-data-separation-design.md`（v1） | **前置基准**：控制层 / 数据层分离设计（契约、可用性模型、epoch 恢复协议、演进路径） | 已补入仓库；关键内容见 §7.0 |

**v1 引述核对【阶段 ⓪ 完成】**：三份补充文档对 v1 的引述大部分准确。

核对**准确**的引述：

- v1 §9 #6「索引是否参与跨节点复制」= 建议**否** → 补充文档 §3.2 转述一致（本文 §8.4 已按阶段 ⓪ 结论推翻）；
- v1 §9 #1「存储集群内部用什么协调」= **独立**协议 → 补充文档 §2 一致（本文 §7.11 采用）；
- v1 §5.2「控制层不参与读路径」→ 补充文档 §4 一致（本文 §9 推翻，改为经服务站中转）；
- v1 §4.2 `ReportEpoch(epoch, durableVersions []VersionRef)` → 补充文档 §2.5 引用的签名一致；
- v1 §4.1 `DataPlane` 不含副本拓扑 → 补充文档 §1.2「需新增该字段」一致。

**4 处引述偏差（已更正）**：

| 补充文档的引述 | v1 原文 | 处置 |
|---|---|---|
| 待决 **#4**「一致性窗口的量级约束」 | v1 §9 #4 =「存储层如何回答'哪些版本 durable'」（manifest + 节点核对）；v1 **没有**"一致性窗口量级"这一待决项，它只是 §8.1 风险表里的"中"级风险 | 本文 §10.4 保留占位值，但不再声称出自 v1 #4 |
| 「**方案甲**（把数据代理塞进控制集群）」 | v1 全文无"方案甲" | 本文 §9.2 已改为不依赖该提法 |
| 「该结论建立在'**节点间不能直连**'前提上；**网状拓扑**确立后需重新论证」 | v1 全文无"网状拓扑"、无"不能直连"（v1 §2.2 只说 `PullVersion` 假设"数据源是 leader"） | 本文 §7.11 已标注该前提需重新论证 |
| 「**现状**（v1 5.1 节⑤）是每个 version 无条件触发 `EnsureIndex`」 | v1 §5.1 ⑤ 是**目标 Saga 的一步**（未实现）；v1 §7 表格自己把 `onVersionCreated` 列为现状、`EnsureIndex` 列为目标 | 本文 §8.6(b) 已改为按现状描述 |

### 12.2 术语映射（补充文档用语 → 现有代码 / proto 实际名称）

| 补充文档用语 | 现有对应物 | 性质 |
|---|---|---|
| `DataPlane.WriteVersionData` | **已实现**：`plane.DataPlane` 契约 + `LocalDataPlane`（写本地事务 → fan-out → quorum → 上报）；入口 `KnowledgeBaseService.CreateVersion`（`service/knowledgebase.go`）→ `WriteCoordinator.Execute`（`internal/coordinator/write_impl.go`） | v1 契约 + 本版实现 |
| `DataPlane.Search` | `QueryService.Query`（`service/query.go`）+ `IndexManager.Search`（`internal/index/impl.go:206`） | 改名 |
| `EnsureIndex` | **已实现**：`DataPlane.EnsureIndex` → `LocalDataPlane.EnsureIndex`（数据源解析 → 拉取 → digest 校验退避 → 追链补缺）；底层仍是 `IndexManager.TriggerBuild`（`internal/index/impl.go:253`） | 改名 + 语义扩展（§8.6b 的惰性化尚未做） |
| `ReportDataDurable` / `ReportEpoch` / `ReportIndexReady` | **已实现**（本节点对账）：`LocalControlPlane` → `ProposeUpdateVersionSummary` / `ProposeUpdateVersionStatus`；跨副本汇聚（block report 并集）尚未实现 | 新概念 |
| `DurabilityPolicy.Replicas` | 无 | 新概念 |
| `localVersion[kbID]int64` | 无（`kvraft` 的 applied index 是元数据日志位置，不是数据面游标） | 新概念 |
| `FAILED_PERMANENT` | **已实现**：`IndexStatusFailedPermanent`（`internal/types/types.go`）+ `VersionMeta.FailureReason` / `FailureCount` | 新增枚举值，与可重试的 `FAILED` 语义分开 |
| `sync.LeaderHandler` | 名字是历史遗留：它其实是节点的**导出端**（把本节点数据交给 peer），与"本节点是否 leader"无关。已在类型注释中澄清 | 彻底改名是纯清理（影响 20+ 调用点，含 4 个测试文件），记为将来 |
| `IndexServingReplicaMin` / `ConsistencyBudget` | 无（`appConfig` 无对应字段） | 新配置项 |
| `Saga` | `WriteCoordinator` / `DeleteCoordinatorImpl` / `DeleteVersionCoordinatorImpl` 的 `Execute` + `retry`（`internal/coordinator/*_impl.go`） | 既有机制的重新命名 |
| `服务站` | `internal/router`（入口 `cmd/stratum-router`） | 升级既有组件 |
| `chunk 存在性 / 去重` | chunk 内容寻址 + `internal/bloom` + `internal/chunkstore` | 已存在 |

### 12.3 冲突登记

| # | 冲突 | 出处对比 | 处置 |
|---|---|---|---|
| 1 | **索引多副本：每副本独立构建 vs 建一次分发 N 份** | `control-data-separation-design.md` §9 #6「建议**否**（从原始数据本地重算）」+ `Stratum_设计文档v11.md:244`「每个副本独立构建」+ `改动内容.md:1173`（据此修复"共享 vecstore 并发 build 竞态"）**vs** 本文 §8.3 | **阶段 ⓪ 已定：采用 §8.3（建一次、分发 N 份）**，撤销 v1 §9 #6 与 v11:244；连带需新增索引文件传输 RPC、改写 `docker-cluster.sh` 前提 |
| 2 | **版本链：严格线性 vs 允许分叉** | v12 §1.2 / `改动内容.md:1283` 按"线性版本链"表述 **vs** `README.md:96`「允许分叉」+ `README.md:5`/`:13` 的 A/B 卖点 **vs** 代码 `applyCreateVersion` 无分叉校验、`TestIntegration_ForkedVersions`（`integration/integration_test.go:547`）断言分叉成功 | **已解决（阶段 ②）**：`applyCreateVersion` 与 `MockRaftNode` 均新增线性链校验，测试与 README 已同步（§11 阶段 ②） |
| 3 | **数据到达副本：推送 vs 拉取** | v11「Leader→Follower 数据同步」称经 `DataSyncService` **推送** **vs** 实现 `sync.Follower.PullVersion`（`internal/sync/follower.go:55`，follower 主动拉） | 措辞以实现为准；§7 将整条路径替换为 fan-out push |
| 4 | **v12 状态标注过时** | v12 头部"本文档为设计稿，**尚未实现**"、变更记录"**均未实现**" **vs** 代码已含 `QuantizerType`（`api/proto/knowledgebase.proto:44`、KB 字段 `:97`、请求字段 `:120`）、`configs/config1.yaml:46` 已按粗筛器口径记账、附录 D 基准与 `vecstore/test/latency_bench_test.cpp` 已产出 | 本文已在头部与 v12 条目回填实际状态 |
| 5 | **终态命名：`FAILED_PERMANENT` vs `IndexStatusFailed`** | 本文 §10.1 新增 `FAILED_PERMANENT`**vs** `internal/types/types.go` 的 `IndexStatus{Pending, Ready, Failed, FailedPermanent}` | **已解决**：取"扩枚举"（两者并存，语义分开：`FAILED` 可重试、`FAILED_PERMANENT` 终态）。**并且终态只由控制层单侧持有**（Raft 状态机字段，与"数据/索引 READY"同类的强一致状态）：存储层只上报 `FailureReason`、只执行"清理这个 version 的数据/索引残留"的指令，**不维护自己的 `FAILED_PERMANENT`** —— 两方各守一份本该单一权威的状态迟早漂移；§7.13.5 的"靠本地文件 mtime 自超时"正是这条界限的正面例子 |
| 6 | **`RollbackVersion` 是否产生分叉** | §7 的"无 rollback 分叉"**vs** `README.md:96`「`RollbackVersion` 无停机切换活跃版本」 | 二者一致（rollback 只切换 active 指针、不产生新版本）；实现时以测试锁定 |
| 7 | **一致性窗口全部为占位值** | §10.4 的四个数字 | 明确标注为占位、待压测替换（原文档亦如此声明） |
| 8 | **协调者由谁选：控制层指派 vs 受理者就地当** | `coordinator-selection-and-node-liveness-design.md` §2（apply 时由 leader 按副本拓扑"试了再说"挑出协调者）**vs** 本文 §8.5 **已实现**的"受理 `CreateVersion` 的节点就地当协调者" | **未决**，两者是不同的模型：前者由控制层统一指派（口径统一、可借 §7.13.5 的局部视图避开不健康节点），后者入口即分散（无额外指派往返，压力随受理节点分布）。`CreateVersion` 当前**已不在** `internal/router` 的 `writeMethods` 中，若选前者即改回该名单并相应调整"谁触发 `WriteVersionData`"。**注意 §7.13.3 的边界与二者正交**（它约束的是"谁能触发写方向副作用"），不构成对 §8.5 读/同步方向通知的否定 |

### 12.4 本文与 v12 的关系

v13 = v12 全文（§1–§5 与附录 A–D 正文未改动，仅回填两处状态标注）+ 新增 §6–§12（三份补充设计稿的整合、v1 基准的纳入与对齐）。**本文未修改任何代码**；四份来源文档（v1 与三份补充稿）均保留原样作为决策记录，其内容已被本文吸收。

---

## 13 附录

### 附录 A：代码引用索引（2026-09 工作区）

| 引用 | 位置 |
|---|---|
| VectorIndex 接口（Build/AddChunks/Search/Save/Load/Reset） | vecstore/include/vector_index.h:33-75 |
| HNSWVectorIndex 实现与常量（M=32/ef） | vecstore/src/hnsw_index.cpp:29-31, 57-213 |
| RocksDB key-agnostic 全精度存储 | vecstore/src/rocksdb_storage.h:19-24；vecstore/include/chunk_storage.h:32-58 |
| gRPC service 组合（两 service 隔离） | vecstore/src/grpc_service.h:28-103, 133-136；vecstore/src/grpc_service.cpp:118-125, 163-192, 252-255 |
| vecstore.proto VectorIndexService/BuildIndexRequest | vecstore/proto/vecstore.proto:21-40, 74-79 |
| Faiss 量化 HNSW 变体 | faiss/faiss/IndexHNSW.h:122-169（Flat:122 / PQ:130-139 / SQ:144-151 / 2Level:155-169） |
| ScalarQuantizer 类型 | faiss/faiss/impl/ScalarQuantizer.h:27-40 |
| Go chunk store key 编码 | internal/chunkstore/grpc_client.go:68-98 |
| IndexManager 配置与内存账本 | internal/index/impl.go:29-69, 97-129 |
| Search/loadFromDisk/acquire | internal/index/impl.go:200-244, 516-552, 599-656 |
| 记账口径与 .index.mem | internal/index/impl.go:569-594, 883-899 |
| makeRoomLocked（LRU 淘汰） | internal/index/impl.go:674-703 |
| Evict/EvictByKB/Discard/DeleteFilesByKB/墓碑 | internal/index/impl.go:740-766, 775+, 906+ |
| IndexManager 接口与 Search 语义 | internal/index/index.go:53-102 |
| LRU/字节阈值行为测试 | internal/index/index_test.go（TestIndexManager_LRUEviction 等） |
| chunk store 全精度权威存储（设计） | Stratum_设计文档v11.md:207-212 |

### 附录 B：外部调研快照（2026-09，仓库页/release 元数据，未经本地基准验证）

| 来源 | URL | 快照要点 |
|---|---|---|
| hnswlib | https://github.com/nmslib/hnswlib | Apache-2.0；v0.9.0（2026-03-28）；header-only；上游无量化 |
| DiskANN | https://github.com/microsoft/DiskANN | MIT；经典 C++（Vamana+PQ）在 `cpp_main` 且官方声明不再积极维护；主线转 Rust（DiskANN3，Vamana 图） |
| USearch | https://github.com/unum-cloud/USearch | Apache-2.0；v2.26.2（2026-08-31）；training-free 标量降位（bf16/f16/i8/Float8-MX），无 PQ；自有格式 + mmap |
| Faiss | https://github.com/facebookresearch/faiss | 本地 vendored v1.9.0（faiss/faiss/Index.h:19-21） |

### 附录 C：术语

- **粗筛器（coarse retriever）**：内存中负责快速产出候选集的近似索引；本文指量化 HNSW（`IndexHNSWSQ/PQ`）。
- **rerank（精排）**：对候选集合用全精度原向量精确计算相似度并重排序。
- **分级存储**：L0 内存热层（粗筛器缓存）/ L1 磁盘冷层（索引文件）/ L2 永久层（RocksDB 全精度向量）。
- **KB 级不可变配置**：量化类型等建库时确定、创建后不可变更的属性（变更 = 新建 KB 迁移）。

### 附录 D：量化离线基准（阶段④，2026-09，合成数据）

**方法（可复现）**：

- 数据：24 个高斯簇中心（均匀 ±8/维），每簇成员 = 中心 + N(0, σ=0.3)；n=8000 条、d=64、EUCLIDEAN；查询 = 200 条随机语料向量；随机 seed 固定。
- 索引：Faiss 1.9.0 HNSW（M=32、efConstruction=200、efSearch=128）五种存储：Flat、`IndexHNSWSQ`(QT_8bit / QT_bf16 / QT_fp16)、`IndexHNSWPQ`(m=16、8bit；train 用全量 8000 条，k-means 打印"样本<9984"警告属正常提示)。
- 指标：recall@10 = |粗筛 N → 全精度 rerank 后 top10 ∩ 暴力精确 top10| / 10，取 200 查询均值；平均搜索耗时 = 每查询 faiss search(top128) 毫秒（单线程墙钟，含噪声，仅量级参考）；载荷 = n×code_size、图边 = n×(2·M·4B+16B) 估算。
- 对照：暴力精确 top10（全量扫描原向量）为召回真值。

**原始 CSV**（与 2.5 节表同源）：

```text
type,cand32,cand64,cand128,cand256,search_ms,payload_bytes,graph_bytes
Flat(HNSW),0.8600,0.8600,0.8600,0.8600,0.2206,2048000,2176000
SQ8,0.8780,0.8780,0.8780,0.8780,0.3038,512000,2176000
SQ_BF16,0.8725,0.8725,0.8725,0.8725,0.3936,1024000,2176000
SQ_FP16,0.8945,0.8945,0.8945,0.8945,0.3364,1024000,2176000
PQ(m16,b8),0.5590,0.6940,0.7420,0.7525,0.2818,128000,2176000
```

**局限**：合成簇数据偏易（类内紧致），不能代表真实 embed 分布（如长尾、各向异性）；真实分布标定需以同样方法（seed 数据替换为生产 embed 样本）复跑后再回填 2.2 参数默认值。基准工具为独立 faiss-only C++ 程序（未入库，见阶段④执行记录）。

**端到端延迟补充（2026-09，状态机与两段式落地后）**：方法 = 真实 `HNSWVectorIndex::SearchWithRerank`（含生命周期读锁贯穿、RocksDB 热 cache、top10、候选 N=80、d=64、n=6000、300 查询、单线程），程序 `vecstore/test/latency_bench_test.cpp`（DISABLED，显式运行）。原始输出：

```text
latency,OFF-Flat,0.2672,300,0
latency,SQ8,0.7050,300,0
latency,SQ_BF16,1.9283,300,0
latency,SQ_FP16,0.7100,300,0
latency,PQ(m16,b8),0.9992,300,0
```

该口径同时暴露 **SQ_BF16 软件慢路径**问题（本机无硬件 bf16 支持时 faiss 距离走软件转换，延迟 ≈7× OFF）——选型建议回避 SQ_BF16。
