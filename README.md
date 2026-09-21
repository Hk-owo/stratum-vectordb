# Stratum

**分布式版本化向量检索引擎 —— 为 RAG 构建的可回滚、可审计知识库存储层。**

文档经 embed 服务向量化后，以 **MVCC 版本** 为单位组织、索引与查询：一次写入产出独立新版本，查询永远落在明确的版本上，回滚 / 版本对比 / 审计都是一等公民。文档按**内容定义分块**切成语义完整的 chunk，元数据由 **Raft** 在节点间强一致，向量由 **C++ Faiss HNSW** 索引；存储按 **L0 内存热层 / L1 磁盘冷层 / L2 永久层** 分层，大知识库可选**量化两段式检索**，内存载荷压缩 4–32× 而精度不变。

名字取自"地层"：文档的每次变更形成一层可独立回滚、可逐版本比对的版本；存储亦分热、冷、永久三层。

## 亮点

- **MVCC 版本化知识库** —— 版本链 + 活跃版本随时切换：
  - **回滚**：一次有问题的更新，一条调用即可撤销，不停服；
  - **版本对比**：任意历史版本都可独立查询、逐个比对；
  - **审计**：每个查询结果都追溯到产生它的具体版本。
- **内容定义分块（默认）** —— 边界来自文档字节上的滚动 Rabin 指纹，而非"从头数偏移"，因此中部插入/删除只扰动编辑点附近的 chunk：同一批真实语料实测，编辑点之后的 chunk **保住 ChunkID 的比例从 0% 升到约 95%**，需要重新 embed 的文本从 64% 降到 10%。边界还会吸附到句末标点、并对齐到 rune 起点（后者是正确性红线，不是调参）。
- **内容寻址 chunk** —— `ChunkID = SHA-256(text + model ID)` 天然去重，无需跨节点协调；写入路径在 **embed 之前**按 ChunkID 过滤已存在的 chunk，复用因此真的省下 embedding 调用，并把复用率与 embed 调用量落进日志（`chunks_already_present` / `embed_skipped`）。
- **每版本独立 HNSW 索引** —— 版本之间零干扰；索引按 LRU 驻留内存，换出/重载廉价，不会影响查询结果精度。
- **量化两段式检索（可选，默认全精度）** —— 开启后内存只驻留量化粗筛器（HNSW 图 + SQ/PQ 码），查询先粗筛出候选、再按候选读 L2 全精度向量做精确 rerank：精度由全精度 rerank 决定，量化只影响候选覆盖。量化后端为 Faiss 内建 `IndexHNSWSQ`/`IndexHNSWPQ`——评估 hnswlib（无量化）、DiskANN（C++ 分支已停止维护）、USearch（仅免训练标量降位、需整体换后端）后选定：零新依赖，且与现有 `write_index`/`read_index` 序列化格式完全兼容。此外**码本能被重新训练**：累积新增向量比或追加次数越界时自动全量重建，避免"只 append 不删除"的库量化误差随分布漂移静默上升。
- **索引对象生命周期状态机** —— 每个 vecstore 索引实例显式区分 `EMPTY`/`BUILDING`/`READY` 三态，读锁贯穿查询（含 rerank 磁盘 IO）、写锁保护构建/加载/重置，杜绝 Search 与并发 Reset/Load/AddChunks 之间的 use-after-free 与错位结果。
- **Raft 强一致 + 崩溃一致性** —— 元数据写操作经 leader 并受 WAL 保护；查询可负载均衡到任意节点；快照不阻塞心跳与写入。存储节点的**数据游标同样落在 WAL 里**，重启后立即准确可用，不再靠扫产物的结论推断。
- **控制面 / 存储面契约分离** —— `internal/plane` 定义两层之间的契约（`ControlPlane` / `DataPlane`），只传逻辑对象（知识库、版本、抽象可用性），**从不暴露副本数、纠删码、文件路径或节点身份**，因此两层可以拆成独立进程/集群而控制层无需知道数据放在哪。`node.role` 支持 `all`（默认，两层同进程）、`control`（只跑控制面：Raft 日志与元数据，不建数据目录、不建索引）与 `storage`（只跑存储层，不留 Raft 日志，元数据经 `RemoteRaftNode` 走 gRPC 读取），`storage.nodes` 声明存储组。
- **任何节点都能发起写** —— Raft 只在 leader 追加日志，但"刚写完一个版本""启动 reconcile 有结论"这类事实可能发生在任何节点，所以非 leader 把提案经内部 `InternalService.Propose` 转给 leader（转发不成链）；错误以稳定 wire name 跨进程传递，转发后 `errors.Is` 依然成立。
- **写幂等 + 卡住的版本有明确归宿** —— `CreateVersion` 的 `client_request_id` 让重试复用首次分配的版本而非另分配一个；PENDING 不再等同于"永远在构建"：**DATA_MISSING**（没有任何候选副本持有该版本的数据，写入方可按同一 key 重发，或用 `DiscardVersion` 放弃）与 **FAILED_PERMANENT**（重试预算耗尽或不可恢复，只有运维能重试或放弃）都会在 `GetSystemStatus` 里显式列出。**数据与索引各有自己的状态与终态**（`DataStatus` / `IndexStatus`，判死时标明落在哪一侧），数据写失败不再被记成索引失败。
- **落后副本自己追上来** —— 存储节点每 5 秒上报一次自己的连续游标，leader 在响应里捎带每个知识库的**链尾**；发现自己落后的节点随即在后台补齐数据、再触发索引（可能直接装载分发来的产物）。**没有开关**：落后自愈是常态，能调的只是节奏（滞后阈值、抖动窗口、并发上限）。
- **数据源解析有四层** —— 本地注册表 → leader 回退 → 游标探测 → **控制层 holders 镜像**（心跳响应捎带、本地缓存，查表即可，绝不在 Raft apply 路径上发 RPC）。一个错过 push 广播的副本仍能自己找到持有者。
- **存储层退化是显式信号** —— 存活副本低于 quorum 时，**写被明确拒绝**（`kb_storage_degraded` / `storage_unavailable`，可重试），而**读照常服务**：判据来自已有的周期上报聚合，未知态一律放行；服务站转发前拦一道（省掉注定失败的往返），控制层在提交 Raft 之前再拦一道（不浪费版本号与日志）。**只读调用方也能发现它**：查询响应上的 `storage_degraded` 由服务站填，不必先撞上一次写失败。
- **自动存储卫生** —— 周期 chunk GC、每版本布隆过滤器、磁盘保留策略的**访问保护**（还在被读的老版本不会被当成死版本删掉）、构建残留的自超时回收、以及可选的墓碑回收；启动时从磁盘事实与持久化游标推导版本可服务状态，不依赖构建回调确实送达。
- **开箱可运维** —— 三态健康检查；Prometheus `/metrics` 端点（`node.metrics_addr`）；HTTP 网关 + Web 控制台（同源提供、免 CORS）；`scripts/gateway.sh --with-db` 一条命令拉起完整链路。

Stratum 是 RAG 管线的**存储与检索层**：不处理聊天历史、用户会话或 prompt 构造——这些属于其上方的应用层。

## 快速开始

```bash
# 构建
go build ./cmd/stratum/

# 运行全部测试(24 个包含测试,共 29 个 Go 包)
go test ./... -timeout 180s -count=1

# 竞态检测(共识与索引核心)
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# 单节点服务
go run ./cmd/stratum/

# 一键拉起完整链路:vecstore(C++) → stratum(gRPC) → 服务站 → gateway(HTTP) + Web UI
scripts/gateway.sh --with-db        # 然后打开 http://localhost:8081
```

`cmd/stratum` 接受可选 YAML 配置用于多节点部署，命令行 flag 优先于文件：

```bash
go run ./cmd/stratum/ -config integration/docker/config1.yaml
```

**3 节点 Docker 集群**（CI 同款，`integration/docker` + `docker` 构建标签）：

```bash
scripts/cluster.sh up 3 --with-embed
go test ./integration/docker/... -tags=docker -timeout 300s
scripts/cluster.sh down
```

**两层拓扑（控制组 + 存储组）**同样一键起停，细节见脚本头部：

```bash
scripts/cluster.sh --topology two-tier build   # 两个镜像;存储镜像自带 vecstore
scripts/cluster.sh --topology two-tier up      # 控制组 1..3(17000+) + 存储组 11..13(17100+) + mock-embed
STRATUM_T4_NODE_SERVICES=stratum-node-control1,stratum-node-control2,stratum-node-control3 \
  go test ./integration/docker/... -tags=docker -v -timeout 900s
scripts/cluster.sh --topology two-tier status  # 每容器状态与控制组 leader
```

## 架构

```
                    外部客户端:gRPC SDK · HTTP 网关 / Web 控制台
                                    │
┌───────────────────────────────────▼───────────────────────────────────┐
│        服务站 station(stratum-router):检索入口 + 鉴权闸门             │
│   按 min_version 取副本 · 副本不健康换候选 · ErrIndexMaintenance 重试  │
│   存储层退化时拒绝写入(读照常)                                         │
└───────┬───────────────────────────────────────────────┬───────────────┘
        │ gRPC(控制面)                                  │ gRPC(存储面)
┌───────▼──────────────────────┐   ┌────────────────────▼───────────────┐
│ 控制面节点(role=control)     │   │ 存储面节点(role=storage)           │
│  只拥有 Raft 复制的元数据     │──►│  拥有全部物理事实:文档 · chunk     │
│  KB · 版本链 · 活跃版本       │   │  · 向量索引 · 复制 · 放置与修复    │
│  抽象可用性 · READY 副本计数  │   │                                    │
│  Raft 共识:日志 · 快照        │   │  IndexManager:每版本索引 LRU 缓存  │
│  (不写本地存储)               │   │  + 引用计数 + 异步构建 + §8.6 策略 │
└──────────────────────────────┘   │  PebbleDB:DocStore(MVCC) ·         │
                                   │  ChunkDoc 双向映射 · VersionDoc    │
                                   ├────────────────────────────────────┤
                                   │  §8.6a 冷版本免图分层               │
                                   │  §8.6c 纯追加复用(墓碑 + 比例重建)  │
                                   │  §8.6d 墓碑回收(滚动 + 副本数前置)  │
                                   └────────────────────┬───────────────┘
                                                        │ 内部 gRPC
┌───────────────────────────────────────────────────────▼───────────────┐
│                    C++ vecstore(向量存储与检索)                        │
│                                                                        │
│  L0 内存:每 (kb, version) 一份 HNSW 索引 / 量化粗筛器                  │
│          生命周期状态机 EMPTY → BUILDING → READY + 每实例读写锁         │
│  量化路径:① 粗筛 top-N → ② 按候选读 L2 全精度原向量 → ③ 精确 rerank    │
│  L1 磁盘:<versionID>.index(.ids/.mem 边车)冷层,Load 即恢复             │
│  L2 永久:RocksDB 全精度向量(内容寻址、不压缩,权威数据)                 │
└─────────────────────────────────────────────────────────────────────────┘
```

三层职责的边界是这套设计的核心：**服务站**只做"选谁、怎么重试、要不要拒",不持有任何状态——它坏掉只影响路由,不影响正确性;**控制面**只拥有 Raft 复制的元数据,回答"哪些版本存在、谁是活跃版本、有多少副本 READY、存储层是否还有足够冗余";**存储面**拥有全部物理事实——文档、chunk、向量索引、复制、放置与修复。向量计算全部下沉到 C++ vecstore,Go 层只负责编排。

两段检索的并发语义(Search 持读锁贯穿,含 rerank 磁盘 IO)由索引对象的显式状态机 + 每实例锁保证,杜绝 use-after-free 与错位结果。

控制面与存储面之间是一条**只传逻辑对象、不传放置细节**的契约(`internal/plane`)。契约的同进程实现(`LocalControlPlane` / `LocalDataPlane`,连同写门、leader 门、数据版本注册表、holders 镜像、回收水位、追链 backfill)已落地,它也是把两层拆成独立进程的前提;`internal/wire` 负责 gRPC proto 与领域类型的**双向**映射,避免某个字段只在一侧被加上而另一侧静默丢弃。

两层既能在同一进程里跑,也能分开部署:`role=control` 的节点不写本地存储(只有 Raft 日志与快照),`role=storage` 的节点不参与选举(它从控制面读元数据、向控制面报告进度)。上面的服务站是**独立形态**——`scripts/cluster.sh --topology two-tier up --with-station` 一次拉起"控制面 + 存储面 + 服务站 + embed"的完整拓扑。

**数据传输有闸门,且不推无用之物**。三条会成规模消耗资源的路径各自有上限:增量恢复的文档写(`maxConcurrentDocumentWrites`,8 路)、索引本地重建(`index_manager.build_concurrency`,构建池同时是两级优先队列)、索引分发(`index_manager.push_concurrency`,默认 4)。分发前还会**先探测对端是否已持有该版本产物**——已持有则一个字节都不传,也不占分发额度。

## 核心概念:版本化文档库

- **版本链严格线性**:`CreateVersion` 一次调用应用任意数量的文档变更(ADD / DELETE / UPDATE),产出新版本并异步构建索引;父版本须已 READY,且**最多只能有一个子版本**(分叉被拒,返回 `invalid_parent_version`)——文档读取要沿祖先链解析,两条分支会让某条分支读到兄弟分支的写入。`RollbackVersion` 无停机切换活跃版本。
- **惰性根版本**:`CreateKnowledgeBase` **不再**创建版本(`initial_version_id` 恒为 0)。版本文档集从父版本继承,所以"空的 changes 列表"意思是"与父版本相同",而不是"空集"——这两者只在链的根部才是同一件事。因此**空的 changes 被拒**(`empty_changes` → `InvalidArgument`),首个版本在真正有东西要写时出现。
- **版本删除三种模式**:`DeleteVersion` 由 `mode` 决定删除范围——`SUBTREE`(默认,删目标版本及其全部后代)、`SINGLE`(只删目标版本,其唯一后继自动改挂到它的父版本上,因此任意"中间版本"都能单独删掉)、`ANCESTORS`(删目标版本的全部前置版本,使其成为版本链新的基底)。响应回传本次实际标记删除的版本清单;活跃版本与 PENDING 版本始终不可删。
- **MVCC 零成本快照**:基于 PebbleDB 前缀编码,未变更文档在新版本中零拷贝;文档历史被压缩保存。
- **布隆过滤器**:每个版本一份完整文档 ID 集合的布隆过滤器,成员检查开销极低;磁盘副本缺失时自动从 `VersionDocList` 重建。
- **垃圾回收**:`ChunkGarbageCollector` 周期性(默认 5 分钟,`gc.sweep_interval_s`)清扫不再被任何版本引用的 chunk。sweep 两遍:先无锁枚举孤儿候选,再持写锁按 Raft **当前**版本复查后删除,与并发写入互斥、不依赖过期快照(stale-snapshot race 免疫),锁粒度为一个 chunk,阻塞毫秒级。
- **写幂等**:`CreateVersion.client_request_id` 是可选幂等键——同一知识库上重发同一 key 会复用首次分配的版本,而不是再分配一个。这是"数据没落地的版本"能被客户端救回的**唯一**途径(§7.12);省略则保持历史语义,每次调用分配新版本。
- **任何节点都能写**:非 leader 节点把已编码的提案经内部 `InternalService.Propose` 交给 leader 执行;接收方不是 leader 时回传 `leader_id` 让调用方改问,转发不会成链。错误以稳定 wire name(`internal/errors.Name` / `ByName`)跨进程传递,认不出某个名字的一方只保留 message,不臆造 sentinel。
- **卡住的版本有明确归宿**:PENDING 的版本若没有任何候选副本持有其数据,即为 **DATA_MISSING**(写入方在分配版本后、数据落盘前死亡,或每次都推送失败),客户端可以 `AwaitVersion` 看到 `data_missing` 后按同一 `client_request_id` 重发,或用 **`DiscardVersion`** 放弃;失败尝试耗尽重试预算或撞上不可恢复错误时,控制面判定 **FAILED_PERMANENT**,不再自动重试,只有运维能重试或放弃——重试是 `ForceRetryVersion`(只针对索引侧:数据侧的终态没有可重试的写入),放弃是 `ForceAbandonVersion`(只接受判死版本,走 `DeleteVersion` 的 SINGLE 语义);要看清队列里的全部判死版本用 `ListFailedVersions`。放弃会向**所有**候选副本广播物理回收,因为控制面并不知道数据实际落在哪几个副本上(§10.1 / §10.6)。
- **数据侧与索引侧各有终态**(§10.1b):`DataStatus`(PENDING / DURABLE / FAILED_PERMANENT)与 `IndexStatus`(PENDING / READY / FAILED / FAILED_PERMANENT)互相独立——一个有持久数据却没有可用索引的版本,与一个索引从未构建的版本,是两件不同的事;判死时 `FailureSide` 说明原因链描述的是哪一侧,运维据此决定"重发写入"还是"重建索引"。版本元数据还携带文档集摘要 `doc_id_set_hash`(§7.9),让存储层能区分"空版本"与"摘要从未提交"。

## 文档切分与向量检索

### 内容定义分块(默认)

切块在 Stratum 内部完成(送进来的是文档全文),算法按知识库固定、创建后不可变——同一个知识库的每个版本必须切得一样,否则同一段文本产出不同 ChunkID,什么都复用不了。

| 模式 | 边界怎么来 | 状态 |
|---|---|---|
| **CDC**(默认,也是元数据的零值) | 文档字节上的滚动 Rabin 指纹,再吸附到句末标点、对齐到 rune 起点 | API 不做选择:每个知识库都按内容定义切块 |
| 滑动窗口 | 从头数固定的 rune 偏移(窗口 / 重叠) | 保留为对照实现,只有代码能选中 |

CDC 的默认参数:最小 256 B、最大 1536 B、期望平均 2^9 = 512 B。实测平均 **738 B / chunk**(滑窗为 766 B),因此检索粒度不变,而复用率不是。

由此得到两条实测收益(同一批真实语料,见 `docs/content-defined-chunking-plan.md` §1.2b):编辑点之后保住 ChunkID 的 chunk 比例 **0% → 约 95%**,需要重新 embed 的文本 **64% → 10%**。

写路径还会在 **embed 之前**按 ChunkID 过滤掉本节点已持有的 chunk(Bloom + Exists),于是"chunk 复用"真的省下 embedding 调用;每篇文档的 `chunks` / `chunks_already_present` / `embed_skipped` 都会落进日志,复用率与 embed 调用量因此可观测。

### 全精度与量化两段式

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

**码本会漂移,所以它能被重新训练**。对学习码本的类型(`SQ8` / `PQ`),索引构建默认走"纯追加复用"(见下节),于是**几乎不会**重训码本,量化误差随数据分布漂移上升、召回静默退化。因此全量重建有第二个独立触发条件:自上次训练以来累积新增向量超过基线的一定比例(`index_manager.max_codebook_drift_ratio`,默认 0.25),或追加次数越过兜底(`index_manager.max_codebook_appends`,默认 50);基线太小(低于 `min_codebook_baseline_vectors`)则忽略比例,免得小库每个版本都重建。两个数都随产物持久化,重启后依然有效;免训练类型(`OFF` / `SQ_FP16` / `SQ_BF16`)不参与判定,默认部署不会平白多出周期性重建。

**实测**(2026-09,合成数据;方法见 `Stratum_设计文档v13.md` §2.5 与附录 D):

- 召回:SQ8 / SQ_BF16 / SQ_FP16 ≈ Flat HNSW;PQ 随候选 N 提升(0.56@32 → 0.75@256);
- 端到端(top-10、候选 N=80、热 cache、单线程):OFF ≈ 0.27 ms,SQ8/FP16 ≈ 0.71 ms,PQ ≈ 1.0 ms——增量来自候选放大读盘 + rerank。候选数默认由**服务端策略**决定(`clamp(top_k × 8, 16, 4096)`);节点可用 `index_manager.candidate_n` 覆盖它(0 = 用服务端默认),这是"量化召回 ↔ 延迟"之间唯一的旋钮。

## 索引生命周期:构建、保留与回收

**构建**:每个版本一份产物,构建只跑在一个候选节点上,完成后按 §8.4 "建一次、分发 N 份"推给副本(副本装载而不再各自重建)。构建有**两级优先级**:`EnsureIndex` 触发的实时构建(有人在等)永远优先于 reconcile 的批量补建(没人等)——worker 空闲时先取高优先队列,空了才看低优先,因此**补建积压再多,也不会挡住一次实时写入**;全局并发上限为 `index_manager.build_concurrency`(0 = 按 CPU 核数),避免一批补建把 CPU、磁盘与 vecstore 一起占满。启动时 reconcile 只补 `PENDING` 与活跃版本,不做全历史重建。

**分发**:`index_manager.push_concurrency`(默认 4)限制同时在跑的分发数;发送端在传输前**探测对端是否已持有该版本产物**,命中则一个字节都不传、不读本地产物、不占额度;分发两端的产物都会拿到保留盾,不会在传输途中被保留策略删掉。

**保留**:磁盘上每 KB 保留 `gc.version_retention_count`(默认 50)个最新版本的产物——这个数同时是"分发还能修复多深的落后"。**访问保护**在其上再叠一层:窗口内(`index_manager.retention_protect_window_ms`,默认 24h)被查询过的版本额外保留,上限 `index_manager.retention_protect_max`(默认取 `version_retention_count`),因此磁盘最多保留 2×。访问时间写在 `<versionID>.index.used` 旁车里,所以进程重启后(启动时那一遍删得最多)依然有效;`RebuildIndex` / `WarmupVersion` 也登记同一份兴趣,运维手动重建的产物不会被随后的保留清掉。负值即关闭该保护,回到"只保新"。

**形态分层(§8.6a)与纯追加复用(§8.6c)**:某版本连续 `cold_threshold_ms` 没被查询,后台评估器把它重塑为"免图"形态(量化码 + 暴力扫描,省掉图边的构建耗时与常驻内存),检索语义不变;它**是双向的**——免图版本在冷阈的一半内被查询过就重建回带图,两个阈值之间是迟滞,避免边界抖动来回重建。评估器枚举**权威版本集合**(控制层元数据)而非本地内存访问表,因此重启后立即生效,并且只塑形本节点真正持有的版本。形态随产物 sidecar 一起走(`graph_free` 行),接收分发来的产物与重启后都能正确识别,不会重复重塑。若版本 N 只是 N-1 的纯追加且父产物还在本节点,构建就以其产物为起点只追加新增 chunk;因删除文档而留在产物里的"墓碑"占比超过 `index_manager.append_max_dead_ratio`(默认 0.2,1.0 = 不检查)时改用全量重建。

**墓碑回收(§8.6d)**:扫面默认就在跑(只读本地产物与当前文档集,不动任何东西),把墓碑占比超过 `index_manager.gc_ratio_threshold`(默认 0.2)的"活跃且长期没有后继版本"记进日志与 `GetSystemStatus` 的 `gc_blocked_versions`;真正**重写正在对外服务的产物**是运维显式打开的开关(`index_manager.gc_enabled`,默认关)。收集前先问控制层"除我之外还有几个副本在服务这个版本",不足 `serving_replica_min`(默认 2)就放弃——滚动回收的前提是"我暂时下线之后仍有人能服务";若总副本数恰好等于该值,收集永远无法启动,此时以 `gc_blocked_versions` 上报(不会静默卡死),处置办法是调大副本数。带图形态没有"原地删向量"这条路(Faiss 的 HNSW 不支持 `RemoveIds`),只能整图重建,所以它的阈值 `graph_rebuild_ratio`(默认 0.5)比追加路径高得多。这里有一个**已知的待改进点,而不是有意决策**:报告门槛(`gc_ratio_threshold`)与追加路径的重建门槛(`append_max_dead_ratio`)默认值目前相同(都是 0.2),同值时 §8.6c 恰好会在 §8.6d 开始关心的那一刻重建,于是 (d) 在压测里永远看不到候选。三个阈值现在都能在配置里分开调(`index_manager.append_max_dead_ratio` / `gc_ratio_threshold` / `graph_rebuild_ratio`),**怎么定值需要真实部署数据**,所以在拿到之前只把旋钮交出来、不改默认值——"先给个可在配置文件里覆盖的默认值"正是这条路径的形态。

**残留回收**:构建在候选节点上失败或被放弃时留下的半成品(`.index.tmp` / `.index.ids.tmp`,或"有 `.index` 无 `.index.ids`")由后台扫描按 mtime 超过 `build_abandon_timeout_ms`(默认 30 分钟)回收;已封印的一对永不触碰,那是保留策略的职责。同一版本产物的读写另有版本分片锁,避免"一边重写、一边装载"。

## 一致性与恢复:游标、追赶与数据源

**连续游标是本地事实,且是持久的**。每个存储节点为每个知识库维护一个"完全持有的最高版本"游标,推进时同步落进 WAL(`CURSOR` 记录,幂等且单调;Compact 只保留每 KB 最新一条)。重启后游标**立即准确可用**,不再靠"扫磁盘上有哪些产物"推断——后者既慢,又会与保留策略耦合(产物被删 ⇒ 游标偏低)。没有记录的旧库才回退到推断,并把结论回写。

**落后副本自己追上来**。节点每 5 秒向控制 leader 上报游标,leader 在响应里捎带每个 KB 的**链尾**(已提交的最新版本——刻意不用 ActiveVersionID:落后是"数据链没跟上",而 active 只是"读路由选谁",回滚会让后者抖动)。本节点若发现 `本地游标 < 链尾` 且滞后达到 `lag_catchup.min_lag_versions`(默认 1),就在后台补齐:先走既有的数据拉取路径把数据追到链尾,再触发索引构建(若构建者仍持有产物,直接装载分发来的那份,沿用分发闸门)。追链是**预热性质**的,绝不与实时查询抢构建队列;同时最多追 `lag_catchup.max_concurrent_kbs`(默认 2)个知识库,触发时带 `lag_catchup.jitter_ms` 的随机延迟把"同时发现"摊平。这里没有开关——把 SLO 上的节点关成"不可服务"不是选项,能调的只是节奏。补齐分两档:缺口里每个版本都能**逐版本重放**时走 WAL 记下的变更增量;缺口里含**已被删除**的版本时整段退回**全量状态传输**(取本次要 apply 的那个版本的整份状态)。判据来自控制层元数据——**只有确认删除才退,元数据读不到不退**;跳过被删版本继续增量重放是错的,那些版本的文档集要按父版本集合推出,而父版本已经不在了(`ListDocIDs` 对不存在的父版本返回空表且不报错,会静默丢文档)。

**数据源解析有四层**,`resolve` 依次尝试:① §8.5 本地注册表(谁把版本推给了我);② 回退 leader;③ 游标探测(向对等节点问"你到哪了");④ **控制层 holders 镜像**——节点在自己的 5 秒上报心跳里收到 leader 的聚合视图(哪些节点报过持有该版本及其自报地址),存成本地缓存,查表即得。第四层补的是这样一个环:一个副本既没收到 push、也没收到确认广播(广播是有界的一次性 fire-and-forget),本地表里就不会有源;而它不查询、也就不会 miss、也就永远找不回来。缓存条目会自行过期,且"问不到"从不写进缓存——"我不知道"与"没人有"导向相反的动作。

**读的新鲜度凭证**:服务站转发查询前附上"当前应看到的版本号",存储节点核对本地连续游标,不够就拒——"悄悄返回过时结果"因此变成显式失败,服务站再换一个达标候选。

**数据侧终态由 Epoch 上报推进**:存储节点的数据游标随 `ReportEpoch` 上行,控制层据此把版本从 PENDING 提升为 DURABLE(只提升 PENDING、只提升仍然存在且非删除中的版本)。

## 存储层退化信号

副本不足时,系统的行为**有名字**,而不是"写入一直 PENDING 然后消失":

- **写被明确拒绝**:该 KB 的存活副本低于 quorum 时返回 `kb_storage_degraded`;一个必需副本都不活时返回 `storage_unavailable`。两者都是 `codes.Unavailable`——**可重试**,因为判据是软状态(周期上报的聚合),它也可能偏乐观,调用方必须能回来拿权威结论。
- **读不受影响,但读能看到它**:低于 quorum 时,手里有数据且索引就绪的副本照样服务——这是明确的取舍,不是遗漏。而查询响应上的 `storage_degraded` 位由**服务站**填(判定在控制 leader 的聚合里,存储节点没有),所以**只读客户端不必先撞上一次写失败**才知道存储层已经降级。这一位只有一 bit,`false` 同时表示"健康"与"未知":判据是换届即清空的软状态,绝不能把换届读成故障。
- **判据不新建采集**:用已有的 §7.13.4 周期上报聚合,分母取副本拓扑(不是"谁报了",否则沉默的节点会悄悄退出要求),阈值是 quorum,新鲜度用上报时间窗(`-storage-silence-window`,默认 3 个上报周期 = 15 s)。窗口本身就是滞回,单次缺失不翻转。
- **三态而不是两态**:`HEALTHY` / `DEGRADED`(有人活但不够 quorum) / `UNAVAILABLE`(**一个必需副本都不活**,即 `live == 0`)。前两档决定写拒绝的 sentinel 名,所以"存储层没了"和"还差一个副本"在读错误的人眼里是两句不同的话。当前只有一份集群级副本拓扑,所以第三档只在极端处触发;per-KB 副本集到位后,这两档的边界才会真正按 KB 划分。
- **未知一律放行**:判不出有四种来源(本节点不是 leader、聚合为空或换届刚清空、拓扑未装配、拓扑读不到),全部 fail-open——换届瞬间变成写熔断是不可接受的。
- **两道门控**:服务站刷新路由表时把判定存进快照,转发写之前先拦(提前返回,省掉一次注定失败的控制层往返);控制层 `CreateVersion` 在提交 Raft **之前**再判一次(请求可能绕过服务站直达),否则会先花掉版本号与日志条目。两处的拒绝都带诊断:缺几个副本、最后上报多久;两处报的**是同一个 sentinel 名**,同一个故障不会因经过哪道门而换名字。
- **运维可见**:`HealthCheck` / `GetSystemStatus` 把诊断放进 `Details`,不改状态位——低于 quorum 时读仍在服务,探针报 UNHEALTHY 会把流量从一个正常干活的节点上摘走。这一行由**服务站**补:判定在 leader 手里,而答 HealthCheck 的存储节点不是 leader(`RemoteRaftNode.IsLeader()` 恒为 false),自己永远填不出这一行。

## 部署形态

- **单节点**:`cmd/stratum`(gRPC,默认 `127.0.0.1:7000`)+ 外部 C++ vecstore 进程。
- **多节点集群**:多个 stratum 节点组成 Raft 集群,共享一套元数据。
- **角色分离**:`node.role` 选择进程承担哪一半——`all`(默认,控制面 + 存储面同进程)、`control`(只跑控制面:Raft 日志与元数据,不持有任何 data plane,连 vecstore 数据目录都不创建)、`storage`(只跑存储层:文档 / chunk / 索引 / vecstore,不留 Raft 日志,元数据经 `RemoteRaftNode` 读取,也从不参与选举)。存储组由 `storage.nodes` 声明;省略即"每个 Raft 成员都持有数据",与拆分前的部署完全一致,老配置无需改动。
  - **两层拓扑的两条硬约束**:① 控制层 ID(`1..N`,即 Raft member ID)与存储层 ID(`11..1N`,同时是 `storage.nodes` 的键)**不得重叠**,否则控制节点会把存储节点的地址认成自己的;② 写入由控制层在 apply 时按副本拓扑**指派**给某个存储节点执行(`ExecuteVersionWrite`,那个节点作为该次写入的协调者再 fan-out),读由服务站路由到持有该版本的存储节点。

```yaml
# 一个只跑存储层的节点:数据在这里,元数据在控制集群
node:
  node_id: 4
  role: storage            # all(默认) | control | storage
raft:
  peers:                   # 控制集群成员:谁答元数据读取、谁接受转发的提案
    - id: 1
      addr: "node1:8000"
      service_addr: "node1:7000"
    - id: 2
      addr: "node2:8000"
      service_addr: "node2:7000"
storage:
  nodes:                   # 存储组:持有数据、构建索引的成员
    - id: 4
      addr: "node4:7000"
```

- **服务站 `cmd/stratum-router`**:集群**唯一**的对外入口。七项职责:① 路由表缓存(`KB + version → 可服务节点`,从控制层聚合**异步刷新**,不是自己逐节点探测);② **新鲜度凭证**(转发前附上"当前应看到的版本号",存储节点核对本地连续游标,不够就拒——把"悄悄返回过时结果"变成显式失败,服务站再换一个达标候选);③ 读负载均衡;④ 故障转移(同一客户端连接内换候选,客户端无感);⑤ 鉴权(token 表 → 租户/权限,数据面完全不必对外);⑥ 健康检查/熔断(closed / open / half-open 三态,状态是服务站**本地**态、实例间不同步,这是它能无状态水平扩展的前提);⑦ **写入门控**(存储层退化时直接拒绝写入,不必等到 fan-out 才失败)。另有 **超时预算传播**(按剩余候选数切分,避免一个慢节点吃掉整个预算)、背压,以及 leader 发现时**给每个节点的探测单独设预算**——`GetClusterStatus` 问的是"你认为谁是 leader",节点从自己的 Raft 状态就能答,答不出来更可能是它没了而不是它慢,于是一个不响应的节点不会把整次发现钉死到调用方 deadline。
  - 节点侧还有一道闸门:`service/authgate.go` 要求三个**客户端可见**的 service 必须带服务站的信任标记(开关 `require_authenticated`);节点间协作(`DataSyncService` / `InternalService`)不受影响、也不得要求。信任标记刻意不携带身份——接收方只需知道"有权限提问的东西替这次调用背了书",安全性建立在"集群从外部不可达"之上。
- **网关 `cmd/stratum-gateway`**:把三个外部 gRPC 服务暴露为 REST/JSON,并从同源提供 Web 控制台静态资源(`web/`),因此无需 CORS。内部服务(`DataSyncService`、`InternalService`)有意不对外。

```
Web UI ⇄ gateway(:8081) ⇄ station(:7009) ⇄ 存储节点(读) / 控制节点(写)
```

**为什么是两层,而不是一个入口**:两者解决的问题不同,合起来会让每一层都背着不属于自己的知识。

| | `stratum-router`(服务站,`:7009`) | `stratum-gateway`(网关,`:8081`) |
|---|---|---|
| 协议 | **gRPC** | **HTTP/JSON** + 前端静态资源(同源) |
| 面向 | 程序客户端 / SDK | 浏览器 / Web 控制台 |
| 职责 | leader 发现、写转发、读均衡、副本选择与换候选、熔断、鉴权闸门、写入门控 | HTTP→gRPC 转换(`protojson`)、静态资源托管、`/ops/` 运维台 |
| 知道集群拓扑吗 | **知道**——要选副本、要把写转发给 leader | **不知道,也不关心**——只连服务站一个地址 |

关键在最后一行:**leader 会换、副本会增减、某个副本会短暂不可用**,这些都该由服务站吸收,而不是泄漏到边缘。所以拓扑变化只动服务站;换前端、加 HTTP 鉴权、把网关放到 DMZ 多开几个实例,都不碰集群。鉴权闸门也只在服务站一处(`-tokens`),SDK 与浏览器过的是同一道闸。

**只要程序接入(用 SDK 直连 `:7009`)就完全不需要网关**——它是可选的纯适配层,不装它集群照常工作,只是没有浏览器入口。

> 浏览器打不开 `http://localhost:7009` 是**正常的**:那是 gRPC 端口,HTTP/1.1 请求会得到 `Received HTTP/0.9`,HTTP/2 请求得到 `415`(只接受 `application/grpc`)。要看界面请用网关的 `:8081`。

```bash
scripts/gateway.sh [up|stop|status|logs|build|db|router]   # 服务站 + 控制台(本地入口)
scripts/gateway.sh --with-db             # 连数据库三件套一起拉起(原来 start.sh 的一键)
scripts/gateway.sh --in-docker           # 控制台跑进集群容器网络(容器名形式的 embed 地址可达)
scripts/gateway.sh router status|stop    # 只操作服务站(两层拓扑下自动派生 -storage-nodes)
scripts/cluster.sh [--topology single|two-tier] <命令>      # Docker 集群编排

# 手动等价:先起服务站,再起 gateway(始终指向服务站)
./run/bin/stratum-router  -listen 0.0.0.0:7009 -nodes 127.0.0.1:7000
./run/bin/stratum-gateway -grpc-addr 127.0.0.1:7009
```

环境变量可覆盖默认:`STRATUM_HTTP_ADDR`(网关监听,默认 `0.0.0.0:8081`)、`STRATUM_ROUTER_ADDR`(默认 `127.0.0.1:7009`)、`STRATUM_GRPC_ADDR`(单机模式下服务站应连的节点,默认 `127.0.0.1:7000`)。

`scripts/gateway.sh --with-db` 一键构建并启动完整链路:服务站与控制台先行,数据库服务经控制台 `/ops/start` 端点拉起——Web UI(默认 `http://localhost:8081`,含「运维」页)在数据库未运行时也可用;Ctrl+C 干净停止,日志在 `run/log/`。仅需运维:直接运行 `./run/bin/stratum-gateway`,在「运维」页编辑 `run/console.yaml` 的启动参数并启停服务。

> Docker 集群模式下 vecstore 是宿主机上的外部依赖(`vecstore.grpc_addr: host.docker.internal:7100`),必须监听宿主机的**对外接口**(`--grpc_addr=0.0.0.0:7100`);只绑 `127.0.0.1` 时容器内无法访问,会导致索引构建失败、删除报错等连锁问题。

## gRPC API

Protobuf 定义在 `api/proto/`:三个外部服务(下面三节)加三个内部服务(`DataSyncService`、`InternalService`、C++ 端的 vecstore 服务)。所谓内部,是指有意不向集群外暴露,只供节点之间使用。

**KnowledgeBaseService**(知识库与版本生命周期)

| RPC | 说明 |
|---|---|
| `CreateKnowledgeBase` | 创建知识库(embed 配置、切块参数、索引类型、量化类型);**不创建版本** |
| `DeleteKnowledgeBase` | 标记删除,清理异步执行 |
| `CreateVersion` | 应用文档变更(ADD / DELETE / UPDATE)并产出新版本;可带 `client_request_id` 幂等键(没带时服务端生成并**随响应回传**);空 changes 被拒 |
| `ListVersions` | 返回知识库版本链(含 `IndexStatus` 与 `DataStatus` 两侧状态) |
| `RollbackVersion` | 切换活跃版本,无停机 |
| `ListKnowledgeBases` / `GetKnowledgeBase` | 列出 / 查询知识库及其活跃版本 |
| `DeleteVersion` | 按 `mode` 删除版本:`SUBTREE`(默认,含后代)/ `SINGLE`(仅该版本,子版本改挂其父)/ `ANCESTORS`(删前置版本,使其成为新基底);清理异步执行 |
| `AwaitVersion` | **等一个版本到达目标状态**(`DATA_DURABLE` 或 `INDEX_READY`,后者也是激活的前置);"还没好"**正常返回**(`stage` + `retry_after_ms`),不是错误;事件驱动(apply 后广播,不靠轮询等),没有 watcher 时退回 200 ms 轮询;响应还带 `data_missing`(可达副本里没有任何一份数据,先问控制 leader 的聚合,聚合答不了才探副本) |
| `DiscardVersion` | **客户端放弃一个从未落地的版本**:只接受仍是 PENDING 的版本(已落地的回 `version_not_pending`,那属于 `DeleteVersion`);幂等(版本已不在时回 `discarded=false`);会清掉幂等键映射,因此同 key 重发分配**新**版本 |
| `GetDataVersionHolders` | "哪些节点报过持有该 KB 的这个版本",读控制 leader 的内存聚合(**软状态**:空答案只意味着"我没听到过",不是"没人有");同一响应回带该 KB 的存储层退化判定(`degraded` / `storage_unavailable` / `degradation_known`)与诊断(`degradation_detail`);服务站据此拦写,并把它变成下面 `Query` 响应上的可见标记 |

**QueryService**

| RPC | 说明 |
|---|---|
| `Query` | 向量相似度检索(阈值、top-k、聚合);响应带回 `storage_degraded`("存储层现在低于 quorum"),由**服务站**填(判定在控制 leader 手里,存储节点没有)——**只读客户端靠它就能发现退化**,不必先撞上一次写失败 |

**AdminService**

| RPC | 说明 |
|---|---|
| `HealthCheck` | 三态健康检查(HEALTHY / DEGRADED / UNHEALTHY);存储层退化**只写进 `Details`、不改状态位**——低于 quorum 时读仍在服务,报 UNHEALTHY 会把流量从一个正常干活的节点上摘走。这一行由**服务站**补:答话的存储节点不是 leader(`RemoteRaftNode.IsLeader()` 恒 false),自己填不出来 |
| `GetSystemStatus` | 卡住版本、**数据缺失版本**(DATA_MISSING)、**永久失败版本**(FAILED_PERMANENT,带 `side` 说明哪一侧)、**GC 受阻版本**(`gc_blocked_versions`)、删除失败的知识库、删除中的版本、WAL 告警、资源占用 |
| `GetClusterStatus` | 节点 Raft 视图(node_id / leader_id / member_count),供服务站发现 leader |
| `RebuildIndex` / `WarmupVersion` | 重试失败版本的索引构建 / 预热版本索引入内存(不切换活跃版本);两者都会登记"被需要",产物受保留策略保护 |
| `ListFailedVersions` | **运维的工作队列**:列出被判定 FAILED_PERMANENT 的版本(两侧任一,带原因链与 `side`),可按知识库或全集群(`knowledge_base_id` 留空)。与 `GetSystemStatus.failed_permanent_versions` 同一个收集函数,两者不会各说各话 |
| `ForceRetryVersion` | **运维撤销索引侧的终态**:清掉终态与原因链、触发重建(登记"被需要")。只接受索引侧——数据侧的终态意味着数据永远不会到,没有可重试的写入,那种版本要 `ForceAbandonVersion`;拒绝时明说这一点,不假装成功 |
| `ForceAbandonVersion` | **放弃一个判死版本**:走 `DeleteVersion` 的 SINGLE 语义(版本离链,子版本改挂其父),清理异步执行。只接受带终态裁决的版本——"放弃"是对失败的操作,名字里带 `DeleteVersion` 的别名会招来误用 |

**InternalService**(节点间控制面流量,不对外)

| RPC | 说明 |
|---|---|
| `Propose` | 把一条已编码的 Raft 命令交给本节点(须是 leader)执行;接收方不是 leader 时回传 `leader_id` 让调用方改问,因此转发不成链 |

**DataSyncService**(存储面节点间流量,不对外)

| RPC | 说明 |
|---|---|
| `ExecuteVersionWrite` | 请本节点执行某个**已提交**版本的存储层写事务(切分 / embed / 本地落盘 / 扇出),按 (kb, version) 幂等 |
| `PushVersionData` / `PullVersionData` | 流式推送 / 拉取某版本的全部记录,按 (kb, version, doc/chunk) 幂等 |
| `PullVersionChanges` | 流式拉取一段版本区间的**变更记录**,让落后节点重放增量而非逐版本拉全量;区间内缺记录是可见的缺口,调用方须退化为全量传输 |
| `LocalVersion` | 本节点对某知识库的连续数据游标(完全持有的最高版本),供落后节点找 peer 补齐 |
| `VersionPresence` | 本节点是否持有 (kb, version) 的数据——控制面据此把"数据从未落地"与"只是索引没建"区分开 |
| `ReportDataVersions` | 存储层周期性向控制 leader 上报数据游标(默认每 5 秒);响应里带回可回收的 changes 水位、每 KB 的**链尾**(落后据此开始追赶),以及 leader 的 **holders 聚合**镜像(数据源解析的第四层) |
| `DeleteVersionData` | 回收本节点上某版本的物理数据(FAILED_PERMANENT 时向所有候选副本广播) |
| `ConfirmVersionWrite` | 告知副本它收到的版本已达 quorum,stand down 其接管计时器 |
| `PushIndexData` | 流式**推送已构建的索引**,副本直接加载而不再各自重建("建一次、分发 N 份");发送端先探测对端是否已持有,端到端受 `push_concurrency` 限流 |

## 工程与测试

| 批次 | 范围 | 状态 |
|---|---|---|
| T1 | 单模块契约(8 个模块) | ✅ |
| T2 | 跨模块集成(4 组) | ✅ |
| T3 | 单节点全链路(15 个场景) | ✅ |
| T4 | 3 节点 Raft 集群(进程内 + Docker 集群 + 数据量 + 压测 + 故障注入) | ✅ |
| T5 | 真实栈 e2e(真实 Pebble/WAL/Raft/IndexManager + vecstore 子进程,零 mock) | ✅ |

```bash
go test ./integration/... -run TestRealStack -v   # 全真实栈端到端(需 C++ 二进制,缺失时跳过)
go test ./integration/... -run TestMultiNode -v   # 3 节点进程内集群:选主、复制 KB + 版本元数据
STRATUM_STRESS_DOCS=20000 go test ./integration/docker/ -tags=docker -count=1 -run TestT4_QueryLatency -v

scripts/t4-integration.sh                      # T4 整套(默认 both 拓扑,自动起集群)
scripts/t4-integration.sh -t all-in-one        # 换 CI 用的那种拓扑
scripts/t4-integration.sh -r 'TestT4_Await'    # 只跑某一组用例
```

全量单测(24 个测试包)与 3 节点 Docker 集群(T4)命令见[快速开始](#快速开始)。

CI(`.github/workflows/ci.yml`,push main 与 PR):gofmt + `go vet` + `go build` + 24 个测试包的单测 + raft/kvraft/index 竞态检测 + 3 节点 Docker 集群(T4)容错运行;C++ vecstore 由手动触发的工作流(`vecstore-cpp.yml`)覆盖。

**T4 集群套件覆盖的场景**(`integration/docker`,两层拓扑需 `STRATUM_T4_NODE_SERVICES` 指向控制组容器):

| 用例 | 覆盖 |
|---|---|
| `TestT4_MultiNode_Consistency` | 选主、元数据复制、版本一致性 |
| `TestT4_MinorityFaultTolerance` / `TestT4_LeaderFailover` / `TestT4_NodeRestartRecovery` | 少数派故障、换届、节点重启后的恢复 |
| `TestT4_QueryLatency` / `TestT4_DataVolume` / `TestT4_MultiVersionEviction` / `TestT4_GCPressure` | 延迟、写入量、多版本换出、墓碑回收 |
| `TestTwoTier_EveryStorageNodeServesTheWrittenVersion` / `TestTwoTier_StorageGroupToleratesOneNodeDown` | 存储组每个副本都能服务写过的版本;挂一个副本仍可用 |
| `TestT4_StorageDegradationRefusesWritesButStillServesReads` | 副本低于 quorum:写被明确拒绝、读照常;恢复一个副本后门控消失 |
| `TestT4_StorageUnavailabilityIsNamedDistinctly` | 一个必需副本都不活:改报 `storage_unavailable`——"存储层整体没了"与"还差一个副本"是两句不同的话,且都要能重试 |
| `TestT4_ActiveLagCatchupCatchesUpWithoutAQuery` / `TestT4_LagCatchupRetentionWindow` | 落后副本不靠查询自己追上;分发能修复的落后深度 |
| `TestT4_HoldersFallbackPullsTheVersionItMissed` | 错过 push 的副本经控制层 holders 兜底找到数据源 |
| `TestT4_AwaitVersion_*` / `TestT4_CreateVersion_ReturnsTheKeyThatMakesAResendIdempotent` | 续等:`AwaitVersion` 收敛、"还没好"正常返回 stage 而不是错误、只有真的不存在才 `NotFound`;响应回传的 `client_request_id` 能用来重发并复用同一版本 |
| `TestT4_AwaitVersion_EveryControlNodeAnswersOnItsOwn` | 绕过服务站直连每个控制节点:等待的锚点是复制状态,不是连接(服务站每次重选后端,会把这条性质藏起来) |
| `TestT4_DiscardVersion_*` | 放弃从未落地的版本:拒绝已落地(`version_not_pending`)与激活版本(`version_is_active`);embedder 停机时版本停在 PENDING、越过探测门槛后 `data_missing=true`,放弃成功后同 key 重发拿到**新**版本 |
| `TestT4_ClientSDK_*` | `client/` 包端到端:先落盘再提交、同 key 重发幂等、丢掉本地 changes 后只剩放弃;以及等就绪的完整流程(需要索引构建可用) |
| `TestT4_UnconvergedReplicaDoesNotAnswerEmpty` | **绕过服务站**直连一个刚重新加入、仍在追赶的副本:它在收到该版本之前只能**可重试地拒绝**,绝不能返回 `results=0, err=nil`——那种答案与"确实没有相关文档"不可区分,且不可重试,服务站也没理由换候选(见 `docs/stress-test-report.md` §7.4) |

## 性能实测

> 以下为 **2026-09-20** 在两层拓扑(控制组 3 + 存储组 3,每个存储容器自带真实 Faiss HNSW + RocksDB,768 维)上的实测,由 `scripts/cluster.sh --topology two-tier` 起集群、经服务站测量。宿主 **12 核 / 15 GB**,容器与压测进程共享这台机器。**完整口径、原始数据与四条发现(四条本轮都已修)见 `docs/stress-test-report.md`**,本节只是它的摘要。

### 查询延迟

`TestT4_QueryLatency` —— 2,000 / 8,000 / 20,000 篇文档,各 200 次查询,**冷热分开**报:

| 指标 | 2,000 篇 / 200 次查询 | 8,000 篇 / 200 次查询 | 20,000 篇 / 200 次查询 |
|---|---|---|---|
| 冷查询(重启副本后的首次,口径见下) | **8.62 ms** | **18.48 ms** | **34.50 ms** |
| p50(热) | **4.24 ms** | **8.18 ms** | **18.45 ms** |
| p95 | **6.19 ms** | **10.04 ms** | **22.81 ms** |
| p99 | **10.69 ms** | **11.77 ms** | **26.26 ms** |
| 平均 | 4.52 ms | 8.41 ms | 18.65 ms |
| 全零向量对照 p50 | 3.86 ms | 7.72 ms | 16.95 ms |

查询向量与 embedder 同卦限(`queryVector(768)`,固定种子、分量非负),所以 HNSW 的剪枝强度与真实调用方一致;全零向量那行是同一次运行内的对照——它与所有文档等距,贪心遍历无从剪枝,是"测了一个没人会发的请求"的代价。热 p50 与文档数**近似线性**(4.24 → 8.18 → 18.45;文档数比 1:4:10,延迟比 1:1.93:4.35):本套语料经内容定义分块去重后唯一 chunk 极少,`matched_docs` 恒等于全部文档数,所以这条路径量的是"候选映射到多少个文档"(构成见下)。

**量化对照** —— 同一个用例、同一台机器,只把知识库创建时的量化类型换成 `SQ8`(创建后不可变):

| 指标 | 2,000 篇 `OFF` | 2,000 篇 `SQ8` | 8,000 篇 `OFF` | 8,000 篇 `SQ8` | 20,000 篇 `OFF` | 20,000 篇 `SQ8` |
|---|---|---|---|---|---|---|
| 冷查询 | 8.62 ms | 7.53 ms | 18.48 ms | 11.98 ms | 34.50 ms | 32.33 ms |
| p50(热) | 4.24 ms | 5.41 ms | 8.18 ms | 8.71 ms | 18.45 ms | 22.53 ms |
| p95 | 6.19 ms | 7.46 ms | 10.04 ms | 11.21 ms | 22.81 ms | 28.48 ms |
| p99 | 10.69 ms | 12.14 ms | 11.77 ms | 13.89 ms | 26.26 ms | 32.99 ms |
| 平均 | 4.52 ms | 5.77 ms | 8.41 ms | 9.10 ms | 18.65 ms | 23.51 ms |
| 全零向量对照 p50 | 3.86 ms | 4.84 ms | 7.72 ms | 7.44 ms | 16.95 ms | 21.28 ms |

**量化买到的是内存,不是延迟**:`SQ8` 把每向量的内存载荷从 4 B/维压到 1 B/维,而这条路径的延迟**不降**——两段式的粗筛省下的时间,被"按候选回读全精度向量做 rerank"吃掉了;规模放大到 20,000 篇后,量化的净效应从噪声变成可见的负担(+4.1 ms p50)。要不要开量化,取决于部署的内存压力(以及 §2.4 里那张载荷表),而不是指望它降低查询延迟。写入路径同理:量化只影响索引构建(见下),不影响写入事务。

**为什么冷热必须分开**:重启一个副本后,它的第一次查询要把产物从磁盘 `Load` 回内存,之后都在内存里——两者成本不可比,取平均得到的数字既不描述常见情况也不描述最坏情况。这正是设计文档阶段⑤"含磁盘读"的落点;尾部分位同理,一个检索服务是被它最慢的查询定义的。

**但 `cold` 这一列不是副本级测量**:用例经**服务站**发查询(那是部署形态),而服务站按自己的健康视图选副本——它量的是"重启之后服务站把这一跳路由给该副本时的首次查询",不保证落在被重启的那一台,也可能把刚重启、尚未上报健康的副本整个排除在外。`docs/stress-test-report.md` §3.4 记了这条口径问题与一次正面旁证。要测真正的冷加载,得像 `TestProbeChainTiming` 那样直连副本。

### 查询成本的构成

分段计时只在 `logging.level: debug` 下产生(`query: stage timings`),日志本身会给延迟加一点开销,所以**绝对值以 info 级别的冷热分位为准**;这里的用途是看构成与占比。

**全精度(`OFF`)** —— 各 225 / 248 个查询,p50 / p95(µs):

| 段 | 做什么 | 2,000 篇 p50 | 2,000 篇 p95 | 8,000 篇 p50 | 8,000 篇 p95 |
|---|---|---|---|---|---|
| `filter_us` | 候选的版本过滤 + 聚合(`chunkmap_us` 是它的子项) | **1672** | 2504 | **5080** | 6765 |
| ↳ `chunkmap_us` | 每个候选 chunk 一次 `ListDocIDs`(chunk → 文档) | 853 | 1485 | 1971 | 3369 |
| `bloom_us` | bloom 过滤器 + 该版本的文档 ID 集合(各一次本地读) | 199 | 302 | 985 | 1243 |
| `meta_us` | 向控制面读版本元数据(`ListVersions`,连接已复用) | 218 | 311 | 243 | 332 |
| `search_us` | vecstore 的 HNSW 检索 | 192 | 291 | 227 | 316 |
| `read_us` | 读命中文档的正文 | 119 | 217 | 170 | 223 |
| **`total_us`** | 节点内合计 | **2639** | 3708 | **7564** | 9480 |

**量化(`SQ8`)** —— 同口径:

| 段 | 2,000 篇 p50 | 2,000 篇 p95 | 8,000 篇 p50 | 8,000 篇 p95 |
|---|---|---|---|---|
| `filter_us` | **1298** | 2082 | **5238** | 6872 |
| ↳ `chunkmap_us` | 719 | 1049 | 2279 | 2857 |
| `bloom_us` | 220 | 274 | 685 | 948 |
| `meta_us` | 187 | 266 | 246 | 337 |
| `search_us` | 209 | 294 | 290 | 406 |
| `read_us` | 135 | 193 | 157 | 209 |
| **`total_us`** | **2218** | 3269 | **7361** | 9122 |

读法:各段 p50 之和看起来大于 `total_us`,因为 `chunkmap_us` 是 `filter_us` 的子项(它在过滤循环内部累加)。真正的大头是**与候选规模成正比**的那两项(`chunkmap_us` + 过滤本身),而不是检索;`search_us` 占比始终是个位数百分比(全精度 2,000 篇 7.3%、8,000 篇 3.0%)——这条路径对 top-k 与候选数敏感,对文档总量只通过"候选映射到多少文档"间接敏感。两轮语料都是合成的高重复语料(6 句循环拼接),唯一 chunk 极少,所以候选数恒为 11、`matched_docs` 等于全部文档数;量化的两段式在这里也体现不出粗筛的规模优势,真实语料下量化的收益是**内存**(§2.4 的载荷口径),不是这条路径的延迟。

### 写入量

`TestT4_DataVolume` —— 真实栈上采样(CDC 默认切块;每批 1,000 篇,`STRATUM_VOLUME_DOCS=10000|20000`):

| 指标 | 10,000 篇 `OFF` | 10,000 篇 `SQ8` | 20,000 篇 `OFF` | 20,000 篇 `SQ8` |
|---|---|---|---|---|
| 写入(`CreateVersion` × 10 / 20 批,仅 API 用时) | **647.79 ms** | **651.46 ms** | **1.318 s** | **1.328 s** |
| 累计索引构建(10 / 20 批串行,每批等前一批 READY) | **2 m 10.9 s** | **1 m 43.6 s** | **5 m 33.8 s** | **5 m 16.1 s** |
| 端到端 | **132.9 s** | **105.8 s** | **337.3 s** | **319.6 s** |
| 单版本索引产物(同一份数据、同一组 chunk) | 36,966 B | **17,802 B** | 36,966 B | **17,802 B** |
| 查询 | top-k=10 返回 10 条结果 | 同左 | 同左 | 同左 |

四列来自同一天、同一集群,差别只在规模与知识库创建时的量化类型(创建后不可变)。**索引产物**是量化在这里唯一干净的可比量:`.index` 同时装 HNSW 图与向量载荷,同一份数据下 SQ8 的产物约为 `OFF` 的 48%。

两件从这张表里读出来、且不能只读数字的事:**写入 API 的用时与规模几乎无关**(10,000 篇 648 ms、20,000 篇 1.32 s,摊到每批都 ~65 ms)——版本提交在批量固定时是常数级操作,数据落盘与索引构建都在响应返回之后;**累计构建则超线性**(10 批 2.18 m → 20 批 5.56 m,规模翻倍 2.5×),因为每个版本都要重写完整的 doc-ID 集并重建整个索引,这是"每版本独立索引"模型的固有开销,不是回归。此外 **10,000 与 20,000 篇的产物字节完全相同,不是笔误**:本套语料高度重复,内容定义分块去重后两个规模的唯一 chunk 集一样大,而 `.index` 装的是 chunk(内容寻址)而不是文档——这也正是"产物大小不能用来衡量文档规模"的原因。

存储节点数据目录总量**不可跨轮比较**——它统计的是整个节点的数据目录,含该轮之前历次压测留下的所有知识库,所以这一列只会随轮次单调变大,不作为口径(本轮 20,000 篇读到 8.47 / 8.80 GB,其中本轮新增只占很小一部分)。

要点(与数字无关):写入须分批——单条 `CreateVersion` 受 4 MiB gRPC 消息上限约束(约 1,400 篇),每批成一版本且前一批 READY 后才链接;耗时随版本号递增(每版本重写完整 doc-ID 集并重建索引),这是"每版本独立索引"模型的固有开销。Raft 快照(`max_log_length`)在这一轮里被反复触发:日志即时 trim、写入与心跳不停摆——快照在 RLock 下深拷贝并异步持久化,apply 与心跳永不被阻塞。

### 写入成本的构成

**一条写链路有六个分段,横跨三个进程。** 只有前两段是**客户端在等**的:版本号提交后响应就返回,数据落盘(段 5)与索引构建(段 6)都在后台继续——所以"`CreateVersion` 慢"和"版本迟迟不可查"是两个问题,各有各的段。全部段都在 debug 级别产生(节点: `logging.level: debug`;服务站: `-log-level debug`),跨节点用 `kb_id` + `version_id` 拼接:同一知识库的每个写都有独立版本号,所以并发写不会串线。

| # | 段(它做什么) | 日志 | 字段 | 产生在 |
|---|---|---|---|---|
| 1 | 服务站转发:鉴权 + 存储门 → 找 leader → 转发 RPC | `router: write forward timings` | `admission_us` · `leader_lookup_us` · `attempt_us` · `attempts` · `leader_index` | 服务站 |
| 2 | 控制面入口:存储门 → proto 转换 → 调协调器 | `service: create version timings` | `gate_us` · `convert_us` · `execute_us` | 控制节点 |
| 3 | 控制面事务:等 `txnMu` → 注册变更 → Raft 提交 → 交接 dispatch | `coordinator: write execute timings` | `txn_wait_us` · `register_us` · `propose_us` · `handoff_us` · `storage_us` · `dispatched` | 控制节点 |
| 4 | 分发:解析候选(µs 级) → **把写交给入选者执行并等它做完**(这一层包着段 5) | `plane: dispatch timings` | `resolve_us` · `attempts_us` · `tried` / `tried_us` · `chosen` · `budget_ms` | 控制节点(后台) |
| 5 | 存储节点写事务 | `write: stage timings` | `acquire_us` · `local_us` · `fanout_us` · `report_us` · `confirm_us` | 存储节点 |
| 6 | 索引构建:排队 → 构建 | `index: build timings` | `queue_us` · `build_us` · `size_bytes` · `status` | 存储节点(后台) |

嵌套关系(按定义成立,不是巧合):段 1 的 `attempt_us` ⊇ 段 2 + 段 3 + 网络;段 2 的 `execute_us` ⊇ 段 3 的 `total_us`(只差函数进出的几十 µs);段 4 的 `tried_us` 里成功那一次的耗时 ⊇ 段 5 的 `total_us`(候选是本地时就是它自己);段 5 的 `report_us` 只覆盖"入队",段 6 才是构建本身。**读法是各段对同一 `(kb_id, version_id)` 的 `*_us` 纵向对比**,而不是把跨节点的绝对时刻相减——不同进程的时钟不做假设。

实测一例(3+3 容器集群,一批 1,000 篇;前三列是同一写法的三次独立测量,第四列是单篇探测写入):

| 段 | 1,000 篇(批 1) | 1,000 篇(批 2) | 1,000 篇(批 3) | 1 篇 |
|---|---|---|---|---|
| 段 1 `total_us`(其中 `leader_lookup_us` / `attempt_us`) | 23.6 ms(2.1 / 21.5) | 25.3 ms(1.9 / 23.4) | 16.7 ms(1.7 / 14.9) | 12.0 ms(1.8 / 10.2) |
| 段 2 `total_us`(其中 `gate_us` / `convert_us` / `execute_us`) | 7.1 ms(0.01 / 0.01 / 7.1) | 12.3 ms(0.01 / 0.01 / 12.2) | 5.9 ms(0.01 / 0.01 / 5.9) | 9.2 ms(0.01 / 0 / 9.2) |
| 段 3 `total_us`(其中 `txn_wait_us` / `propose_us` / `handoff_us`) | 7.1 ms(0 / 7.1 / 0) | 12.2 ms(0 / 12.2 / 0) | 5.8 ms(0 / 5.8 / 0) | 8.9 ms(0 / 8.9 / 0) |
| 段 4 `total_us`(其中 `tried_us` 唯一那次) | **4,649.9 ms**(4,649.9) | 4,131.6 ms(4,131.6) | 7,830.1 ms(7,830.1) | 148.1 ms(148.1) |
| 段 5 `total_us`(其中 `local_us` / `fanout_us` / `report_us`) | **4,536.7 ms**(1,324.9 / 3,207.8 / 4.0) | 4,015.1 ms(1,233.1 / 2,773.5 / 8.5) | 7,724.6 ms(1,499.1 / 6,214.1 / 11.3) | 44.9 ms(26.6 / 6.7 / 11.6) |
| 段 6 `total_us`(其中 `queue_us` / `build_us`) | **21.2 ms**(0.10 / 10.5) | 21.2 ms(0.03 / 10.0) | 31.7 ms(0.04 / 19.8) | 10.6 ms(0.09 / 3.8) |

四条从这张表里读出来、别处看不到的事:

- **客户端等的时间主要不是节点算的。** 段 1 的 23.6 ms 里,节点内部只占 7.1 ms(段 2),其余 ~16 ms 是把这 1,000 篇搬过服务站→控制节点那条 wire(收发 + 序列化)。单篇时同一差额只有 ~2.8 ms,可见它随 payload 而不是随请求数走。
- **批量写的成本在复制,不在本地处理。** 段 5 的 4.54 s 里 `fanout_us` 3.21 s、`local_us` 1.32 s(切分 + embed + 落盘)。要压批量写入的墙钟时间,该看的是副本侧,不是本机——第三列就是证据:同一批 1,000 篇,`fanout_us` 从 2.77 s 抖到 6.21 s,整批墙钟随之翻倍。
- **段 4 量的不是"挑候选",是"等候选干完"。** 挑候选只花 `resolve_us`(读副本拓扑 + 排序 + 健康排序,本例 **2 µs**);`tried_us` 里那 4,649.9 ms 是**候选节点执行写事务**的墙钟时间,它 ⊇ 段 5 的 4,536.7 ms。要问"轮询了几个候选",看的是 `tried` 的长度:长度为 1 且其值 ≈ 段 5 ⇒ 第一个候选就接下来了;多个元素、每个都逼近 `budget_ms`(1,000 篇时 65 s)⇒ 前面的候选不可达、各自吃满了预算(§7.13.2 的候选轮询),那才是段 4 真正会爆的时候。
- **"等 READY"和"构建索引"不是一回事。** 测试用例报的 `build-accum`(从 `CreateVersion` 返回到 READY)在 1,000 篇时是 5.0 s,而段 6 的 `build_us` 只有 10.5 ms——两者差近 500 倍,因为 `CreateVersion` 在**版本提交**时就返回了,之后的 5.0 s 是段 4 的数据落盘,不是构建。拿 `build-accum` 当"索引构建耗时"会把它高估两个数量级。

各段为什么存在:`txn_wait_us` 是唯一能看见 §7.7 串行化的地方(同一知识库的写排成一队,突发的并发版本只在这里显形);段 6 的 `queue_us` / `build_us` 必须分开——同样的 90 s 排在队列里是并发度问题,花在 `build()` 里是 vecstore / 批次问题,而下游只能看到"PENDING 了 90 s";段 4 的 `tried_us` 分开记每个候选,是因为"第一个候选已失联、吃掉了整个预算"和"候选真的在写 1,000 篇"总耗时相同。

存储节点内的分段计时(`write: stage timings`)——上表的段 5:

| 段 | 做什么 | 单篇(`changes=1`) | 1,000 篇(全精度) | 1,000 篇(SQ8) |
|---|---|---|---|---|
| `acquire_us` | 该知识库在途写入的限流(§7.7) | 1 µs | 1 µs | 1 µs |
| `local_us` | 本地事务:切分 → embed → 落盘 | 26.6 ms | **1,324.9 ms** | **891.2 ms** |
| `fanout_us` | 复制到副本(quorum;对端各自拉取并落盘) | 6.7 ms | **3,207.8 ms** | **770.0 ms** |
| `report_us` | 上报"数据已可持久" + 触发索引构建 | 11.6 ms | 4.0 ms | 8.1 ms |
| `confirm_us` | 广播确认(异步触发,基本不计入) | 1 µs | 1 µs | 3 µs |
| **`total_us`** | 节点内合计 | **44.9 ms** | **4,536.7 ms** | **1,669.3 ms** |

后两列**不来自同一时刻**(全精度那列在 `debug` 轮早期、SQ8 那列在更晚的一次单独运行),所以 `local_us` / `fanout_us` 的差异是环境负载而不是量化造成的,不可逐格比较。可比的是**量与构成**:量化的影响只在段 6(见下),不在这条事务里。`report_us` 很小,却是最不能少的一步:它同时是"数据已可持久"的上报与索引构建的触发点。单篇那列的样本数少(来自探测写入),且 `report_us` 偶尔会因同时触发的构建而抬到百毫秒级,故这里报的是 p50 而非平均。

同一规模(1,000 篇)下 SQ8 对**索引构建**的影响看段 6:`build_us` 从 **10.5 ms** 升到 **41.6 ms**(训练码本),`size_bytes` 从 **33,792** 降到 **11,440**——这就是量化买到的内存,与写入事务无关。

### 压力测试

压测覆盖设计文档阶段⑤ 要求的单版本大 n 构建/查询延迟(含重启后的磁盘读)与多版本分级换出稳定性。规模用环境变量控制(`STRATUM_STRESS_DOCS` / `_QUERIES` / `_VERSIONS` / `_TIMEOUT`),默认值小到能进 CI;量化对照用 `STRATUM_T4_QUANTIZER=off|sq8|sq_fp16|sq_bf16|pq` 切换,测量用例会为它单独建一个知识库(量化类型创建后不可变)。

**定位:写入与读取分别称量。** 每个用例都是"写完并 READY 之后才开始读",两者从不并发——这不是漏测,而是刻意的:混跑会让写入侧的构建/IO/CPU 摊进查询的分位里,得到的数字既不是写入成本也不是读取成本。

| 用例 | 测什么 | 实测 |
|---|---|---|
| `TestT4_QueryLatency` | 单版本查询延迟,**冷热分开报** | 2,000 篇:cold **8.62 ms** · p50 **4.24 ms** · p95 6.19 ms · p99 10.69 ms;8,000 篇:cold **18.48 ms** · p50 **8.18 ms** · p95 10.04 ms · p99 11.77 ms;20,000 篇:cold **34.50 ms** · p50 **18.45 ms** · p95 22.81 ms · p99 26.26 ms |
| `TestT4_DataVolume` | 写入量:写 / 构建 / 占用 | 10,000 篇(10 批 × 1,000):写入 API **648 ms** · 累计索引构建 **2 m 11 s** · 单版本产物 **36.97 KB**;20,000 篇(20 批):写入 **1.32 s** · 构建 **5 m 34 s**;量化(SQ8)同口径:10,000 篇 **651 ms / 1 m 44 s / 17.80 KB**,20,000 篇 **1.33 s / 5 m 16 s** |
| `TestTwoTier_EveryStorageNodeServesTheWrittenVersion` | 控制组提交的版本在每个存储副本上都能读到 | 每个存储副本都能服务写过的版本 |
| `TestTwoTier_StorageGroupToleratesOneNodeDown` | 一个存储副本挂掉后写入与读取仍可用 | 通过 |
| `TestT4_MultiVersionEviction` | 多版本分级换出的稳定性 | 8 版本 × 3 轮轮转,每个版本始终应答 |
| `TestT4_GCPressure` | 墓碑回收的端到端可见性(写入 → 删除 → 观察产物 → 查询仍正确) | **跑通了**:产物 99,194,878 B → **49,282,907 B(回收 50.3%)**,收集后查询仍返回结果;用时 43.7 s。这条路以前从来没真正跑通过——它被四层套着的因果挡着(delta 为空导致 §8.6(c) 重建、判据无日志、分发获得的副本不上报 READY、`IndexReadyNodes` 不过 wire),见 `docs/stress-test-report.md` §6。**2026-09 补记**:该用例随后变成 flaky(实测 8 次里 3 次失败)——§8.6(d) 补上了第二个扫描来源"链尾"后,刚写完的版本 5 s 内就被回收,而这条用例用"产物比父的干净产物大"来推断"复用了、有墓碑",测量晚于收集时读到的是回收后的产物,于是把"墓碑刚被回收"误判成"重建了、没有墓碑"(报错文案写的正是后者,会把人带偏)。已改为**用日志证据断言**(`built by appending to the parent version's artifact (§8.6c)` + `deleted_chunks > 0`),收集后的断言对"复用产物"与"父的干净产物"两个参照都成立;`-count=10` 全过,且覆盖了测量与收集的两种先后 |

测量类用例的规模都可由环境变量放大,默认值小到能进 CI;放大时记得同时放大 `STRATUM_STRESS_TIMEOUT`(20,000 篇建议 `90m`)。**这一轮压测抓到四条缺陷,四条都已修**(详见 `docs/stress-test-report.md`):① 落后追赶的数据源解析会落到控制节点 ⇒ 现在只接受存储层地址(实测 `this node exports no data` 从每知识库上百次降到 0);② 大版本的拉取窗口是固定墙钟(15 s 盖住握手+整条流、30 s 盖住整个拉取循环且重试沿用同一窗口) ⇒ 改成**按无进展计时**+绝对上界,20,000 篇的副本补齐从"永不收敛"变成 28 s 追上,`DeadlineExceeded` 64 → 0;③ §8.6(d) 的收集链路被四层因果挡死 ⇒ 拆到底后首次跑通,回收 50.3%(其中两个是生产缺陷:分发获得产物的副本**不上报 READY**、`IndexReadyNodes` **不过 wire**);④ 未收敛的副本把"我还不能服务"答成"这里没有匹配的文档" ⇒ 两处"空"都改判成可重试的 `index_not_ready`。每条都带"撤掉即变红"的单测。

```bash
STRATUM_STRESS_DOCS=20000 STRATUM_STRESS_TIMEOUT=90m STRATUM_INDEX_BUILD_TIMEOUT=25m \
  go test ./integration/docker/ -tags=docker -count=1 -run TestT4_QueryLatency -v
STRATUM_VOLUME_DOCS=20000 STRATUM_VOLUME_TIMEOUT=150m \
  go test ./integration/docker/ -tags=docker -count=1 -run TestT4_DataVolume -v

# 量化对照:同一个用例、同一台机器,只换知识库的量化类型
STRATUM_T4_QUANTIZER=sq8 STRATUM_STRESS_DOCS=20000 STRATUM_STRESS_TIMEOUT=90m \
  go test ./integration/docker/ -tags=docker -count=1 -run TestT4_QueryLatency -v
```

## 项目结构

```
api/proto/           # Protobuf 定义(.proto)→ 生成代码在 api/proto/stratum/
internal/            # Go 核心
  docstore/          #   MVCC 文档存储(PebbleDB)
  versiondoc/        #   版本元数据与文档集映射(含 doc_id_set_hash)
  chunkdoc/          #   chunk ↔ 文档双向映射
  chunkstore/        #   chunk 正文存储(内容寻址)
  bloom/             #   chunk 存在性 + 每版本文档布隆过滤器
  splitter/          #   切块:内容定义分块(默认)+ 滑动窗口(对照)
  embed/             #   embed 服务客户端(HTTP)
  index/             #   IndexManager(LRU + 引用计数 + 构建池 + 保留/回收 + 分发)
  kvraft/            #   Raft 共识(选主 / 日志复制 / 快照)
  raft/              #   Stratum Raft 状态机(KB + 版本元数据)+ 节点间转发
  plane/             #   控制面 ↔ 存储面契约 + 同进程实现(写门 / 游标 / holders / 追链)
  wire/              #   gRPC proto ↔ 领域类型映射
  wal/               #   崩溃一致性写前日志(含游标记录与按水位重写)
  sync/              #   Leader→Follower 数据同步 + 索引分发 + 上报(游标 / 链尾 / holders)
  coordinator/       #   Write / Delete / DeleteVersion / chunk-GC 编排
  router/            #   写 → leader、读负载均衡、路由表与写入门控
  kvstorage/ pebbleutil/ authmeta/ types/ errors/
service/             # gRPC 服务实现(含测试)
cmd/stratum/         # 节点入口(-config YAML / flags)
cmd/stratum-router/  # 服务站:单地址接入 Raft 集群,集群唯一对外入口
cmd/stratum-gateway/ # HTTP/JSON 网关 + /ops 控制台控制面
vecstore/            # C++ 向量存储:Faiss HNSW + RocksDB,支持量化两段式检索
web/                 # Web 控制台前端(HTML/CSS/JS)
integration/         # mock 集成 + 真实栈 e2e + docker/(T4 集群测试)
scripts/             # cluster.sh(集群编排) / gateway.sh(本地入口) / t4-integration.sh / update-all.sh / ops/(运维 CLI)
                     #   用法与选项见 scripts/README.md(脚本文档,随脚本一起纳入版本控制)
configs/             # 示例配置文件
```

## 配套文档

与代码同目录维护一套随实现演进的中文设计文档——**不纳入版本控制**(`.gitignore` 忽略根目录除本 README 外的全部 Markdown),仅供本地阅读:

- `Stratum_设计文档v13.md` —— 最新版设计总纲(控制面 / 存储面契约 §7.0、节点间转发 §7.3、游标与追链 §7.5、数据侧独立终态 §10.1b、数据缺失与幂等重发 §7.12、存储层状态上报 §7.13、索引分发 §8.4、索引形态与回收 §8.6、FAILED_PERMANENT §10.1、存储集群独立进程 §11 阶段 ④;含量化两段式 §2.5/§2.6、附录 D 实测方法)
- `Stratum_接口设计v9.md` —— gRPC/内部接口与语义
- `Stratum_设计目标.md` —— 功能目标、性能指标与验收标准
- `Stratum_测试顺序.md` / `Stratum_实现顺序.md` / `Stratum_代码风格.md`
- `client-integration-guide.md` —— **客户端接入指南**(调用方视角:数据模型、怎么选择文档提交、REST / gRPC 调用全流程、幂等与责任边界)
- 专题计划与落地记录:`content-defined-chunking-plan.md`、`codebook-refresh-plan.md`、`cursor-persistence-plan.md`、`active-lag-detection-design.md`、`data-source-holders-fallback-plan.md`、`index-distribution-backpressure-plan.md`、`index-push-probe-plan.md`、`storage-degradation-signal-plan.md`
- `HANDOFF.md` —— 故障史与排查记录;`改动内容.md` —— 开发日志
- `stress-test-report.md` —— **压测报告**:逐轮的口径、原始数据与发现(README「性能实测」是它的摘要,冲突时以报告为准)

## 前置依赖

- **Go 1.24**(依赖 `restic/chunker` 做滚动指纹分块)
- **C++17**(vecstore):Faiss ≥ 1.9.0、RocksDB、gRPC、Protobuf、BLAS/LAPACK、OpenMP
- C++ 构建仅在需要 Faiss HNSW 后端时必需;Go 单测使用进程内 mock。
