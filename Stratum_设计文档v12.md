# Stratum 设计文档 v12 —— 向量检索两段式架构与分级存储设计

> 面向 RAG 场景的分布式知识库存储系统
> 本文档记录系统的设计方向、数据模型、模块职责和关键流程，不包含代码层面的实现细节。
> **本文档（v12）为设计稿，尚未实现。**「背景与现状」部分与 v11/当前代码实现对齐；其余部分为目标设计，待评审后进入实施。

---

## 版本变更记录

### v12（设计稿）：内存量化 HNSW 粗筛 + 磁盘全精度 rerank + IndexManager 分级存储

v12 相对 v11（与实现全面对齐）提出以下增量设计，**均未实现**：

1. **检索架构变化（两段式检索）**：从"整份全精度 HNSW 索引驻留内存、单段搜索直接出 top-k"，改为"内存量化粗筛器出候选 top-N → 从磁盘全精度原向量（复用现有 RocksDB chunk store）批量读取 → 精确距离 rerank 出最终 top-k"。
2. **索引后端变化**：内存索引后端由 `IndexHNSWFlat`（float32 原向量）按 KB 配置切换为 Faiss 量化变体 `IndexHNSWSQ` / `IndexHNSWPQ`（图结构保留、向量载荷压缩为量化码）；量化器仅承担召回，不承担最终评分。
3. **索引管理变化（LRU → 分级存储）**：Go 侧 IndexManager 内存驻留对象从"完整索引副本"改为"每版本量化粗筛器"；全精度向量永驻磁盘；LRU 淘汰语义保留，但换出对象与内存记账口径（`.index.mem`）随之改变。
4. **兼容要求**：未开启量化的 KB 保持现状路径不变；存量 `.index`（Flat 格式）文件可继续 Load；磁盘上的索引文件与全精度向量均不压缩。

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

## 6 附录

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
