# Stratum

[![CI](https://github.com/Hk-owo/stratum-vectordb/actions/workflows/ci.yml/badge.svg)](https://github.com/Hk-owo/stratum-vectordb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Hk-owo/stratum-vectordb/branch/main/graph/badge.svg)](https://codecov.io/gh/Hk-owo/stratum-vectordb)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/go1.24)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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
- **每版本独立 HNSW 索引，且不跨节点复制** —— 版本之间零干扰；每个版本只落在**该版本的 owner 集**上（服务站按 (知识库, 版本) 确定性算出：活跃版本全体副本都是 owner，非活跃版本按“最近是否被查询”加档），需要它的副本按需向 owner 拉取、或自己建；索引按 LRU 驻留内存，换出/重载廉价，不会影响查询结果精度。
- **量化两段式检索（可选，默认全精度）** —— 开启后内存只驻留量化粗筛器（HNSW 图 + SQ/PQ 码），查询先粗筛出候选、再按候选读 L2 全精度向量做精确 rerank：精度由全精度 rerank 决定，量化只影响候选覆盖。量化后端为 Faiss 内建 `IndexHNSWSQ`/`IndexHNSWPQ`——评估 hnswlib（无量化）、DiskANN（C++ 分支已停止维护）、USearch（仅免训练标量降位、需整体换后端）后选定：零新依赖，且与现有 `write_index`/`read_index` 序列化格式完全兼容。此外**码本能被重新训练**：累积新增向量比或追加次数越界时自动全量重建，避免"只 append 不删除"的库量化误差随分布漂移静默上升。
- **索引对象生命周期状态机** —— 每个 vecstore 索引实例显式区分 `EMPTY`/`BUILDING`/`READY` 三态，读锁贯穿查询（含 rerank 磁盘 IO）、写锁保护构建/加载/重置，杜绝 Search 与并发 Reset/Load/AddChunks 之间的 use-after-free 与错位结果。
- **Raft 强一致 + 崩溃一致性** —— 元数据写操作经 leader 并受 WAL 保护；查询可负载均衡到任意节点；快照不阻塞心跳与写入。存储节点的**数据游标同样落在 WAL 里**，重启后立即准确可用，不再靠扫产物的结论推断。
- **控制面 / 存储面契约分离** —— `internal/plane` 定义两层之间的契约（`ControlPlane` / `DataPlane`），只传逻辑对象（知识库、版本、抽象可用性），**从不暴露副本数、纠删码、文件路径或节点身份**，因此两层可以拆成独立进程/集群而控制层无需知道数据放在哪。`node.role` 支持 `all`（默认，两层同进程）、`control`（只跑控制面：Raft 日志与元数据，不建数据目录、不建索引）与 `storage`（只跑存储层，不留 Raft 日志，元数据经 `RemoteRaftNode` 走 gRPC 读取），`storage.nodes` 声明存储组。
- **任何节点都能发起写** —— Raft 只在 leader 追加日志，但"刚写完一个版本""启动 reconcile 有结论"这类事实可能发生在任何节点，所以非 leader 把提案经内部 `InternalService.Propose` 转给 leader（转发不成链）；错误以稳定 wire name 跨进程传递，转发后 `errors.Is` 依然成立。
- **写幂等 + 卡住的版本有明确归宿** —— `CreateVersion` 的 `client_request_id` 让重试复用首次分配的版本而非另分配一个；PENDING 不再等同于"永远在构建"：**DATA_MISSING**（没有任何候选副本持有该版本的数据，写入方可按同一 key 重发，或用 `DiscardVersion` 放弃）与 **FAILED_PERMANENT**（重试预算耗尽或不可恢复，只有运维能重试或放弃）都会在 `GetSystemStatus` 里显式列出。**数据与索引各有自己的状态与终态**（`DataStatus` / `IndexStatus`，判死时标明落在哪一侧），数据写失败不再被记成索引失败;只有**数据侧**的终态才触发物理回收——索引侧判死说的是"构建失败",不代表数据没了。
- **落后副本自己追上来** —— 存储节点每 5 秒上报一次自己的连续游标，leader 在响应里捎带每个知识库的**链尾**；发现自己落后的节点随即在后台补齐数据、再触发索引（owner 仍持有产物时直接按需拉取装载）。**没有开关**：落后自愈是常态，能调的只是节奏（滞后阈值、抖动窗口、并发上限）。
- **数据源解析有四层** —— 本地注册表 → leader 回退 → 游标探测 → **控制层 holders 镜像**（心跳响应捎带、本地缓存，查表即可，绝不在 Raft apply 路径上发 RPC）。一个错过 push 广播的副本仍能自己找到持有者。
- **存储层退化是显式信号** —— 存活副本低于 quorum 时，**写被明确拒绝**（`kb_storage_degraded` / `storage_unavailable`，可重试），而**读照常服务**：判据来自已有的周期上报聚合，未知态一律放行；服务站转发前拦一道（省掉注定失败的往返），控制层在提交 Raft 之前再拦一道（不浪费版本号与日志）。**只读调用方也能发现它**：查询响应上的 `storage_degraded` 由服务站填，不必先撞上一次写失败。
- **自动存储卫生** —— 周期 chunk GC、每版本布隆过滤器、磁盘保留策略的**访问保护**（还在被读的老版本不会被当成死版本删掉）、构建残留的自超时回收、以及**默认开启**的墓碑回收（可显式关闭）；启动时从磁盘事实与持久化游标推导版本可服务状态，不依赖构建回调确实送达。
- **开箱可运维** —— 三态健康检查；Prometheus `/metrics` 端点（`node.metrics_addr`）；HTTP 网关 + Web 控制台（同源提供、免 CORS）；`scripts/gateway.sh --with-db` 一条命令拉起完整链路。

Stratum 是 RAG 管线的**存储与检索层**：不处理聊天历史、用户会话或 prompt 构造——这些属于其上方的应用层。

## 快速开始

```bash
# 构建
go build ./cmd/stratum/

# 运行全部测试(25 个包含测试,共 31 个 Go 包)
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

![Stratum 架构:客户端 → 服务站 → 控制面 / 存储面 → C++ vecstore](assets/architecture.svg)

同一拓扑的文字版(关键信息在下面这份 ASCII 图里重复一遍,渲染 SVG 的环境缺中文字体时以它为准):

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

**数据传输有闸门,且只传真正被需要的产物**。两条会成规模消耗资源的路径各自有上限:增量恢复的文档写(`maxConcurrentDocumentWrites`,8 路)、索引本地重建(`index_manager.build_concurrency`,构建池同时是两级优先队列)。索引产物的分发已改为**按需拉取**:接收方向持有者索要,对端没有就改问别人或自己建,因此不存在"推了一堆对方用不上的产物"这回事;提供传输的一侧另有 `index_manager.fetch_concurrency` 闸门(默认不限,用于压住同一版本被 N−1 个副本同时索要的突发);同一版本的并发索要还会**合并成一次读盘**(字节多路复用,源节点内存峰值从 N 份整产物降到 1 份;不是缓存——读一结束就重建,重建后的下一次读仍读新产物)。

## 核心概念:版本化文档库

- **版本链严格线性**:`CreateVersion` 一次调用应用任意数量的文档变更(ADD / DELETE / UPDATE),产出新版本并异步构建索引;父版本须已 READY,且**最多只能有一个子版本**(分叉被拒,返回 `invalid_parent_version`)——文档读取要沿祖先链解析,两条分支会让某条分支读到兄弟分支的写入。`RollbackVersion` 无停机切换活跃版本。
- **惰性根版本**:`CreateKnowledgeBase` **不再**创建版本(`initial_version_id` 恒为 0)。版本文档集从父版本继承,所以"空的 changes 列表"意思是"与父版本相同",而不是"空集"——这两者只在链的根部才是同一件事。因此**空的 changes 被拒**(`empty_changes` → `InvalidArgument`),首个版本在真正有东西要写时出现。
- **版本删除三种模式**:`DeleteVersion` 由 `mode` 决定删除范围——`SUBTREE`(默认,删目标版本及其全部后代)、`SINGLE`(只删目标版本,其唯一后继自动改挂到它的父版本上,因此任意"中间版本"都能单独删掉)、`ANCESTORS`(删目标版本的全部前置版本,使其成为版本链新的基底)。响应回传本次实际标记删除的版本清单;活跃版本与 PENDING 版本始终不可删。
- **"已删"只问当前状态**:判定某版本是否已被删除,依据是两件**当前**事实——该版本号是否已被分配(分配计数器单调递增、随快照走),以及它是否仍在存活版本集合里;两者同时成立即为"已删或已弃"。判定因此**不依赖任何需要保留的历史记录**,版本链的 backfill(缺口里的版本是空的还是已删的)与本地残留的对账(启动对账)共用同一判据;控制节点读自己的状态机,纯存储节点(`role=storage`)经内部读拿到同样的答案,两种形态一致。被删版本对客户端始终只是"不存在"。
- **MVCC 零成本快照**:基于 PebbleDB 前缀编码,未变更文档在新版本中零拷贝;文档历史被压缩保存。
- **布隆过滤器**:每个版本一份完整文档 ID 集合的布隆过滤器,成员检查开销极低;磁盘副本缺失时自动从 `VersionDocList` 重建。
- **垃圾回收**:`ChunkGarbageCollector` 周期性(默认 5 分钟,`gc.sweep_interval_s`)清扫不再被任何版本引用的 chunk。sweep 两遍:先无锁枚举孤儿候选,再按 Raft **当前**版本复查后删除,该复查与删除**按知识库**与并发写入互斥(与写路径共用同一套 per-KB 写锁),不依赖过期快照(stale-snapshot race 免疫),锁粒度为一个 chunk、一次知识库,阻塞毫秒级。
- **写幂等**:`CreateVersion.client_request_id` 是可选幂等键——同一知识库上重发同一 key 会复用首次分配的版本,而不是再分配一个。这是"数据没落地的版本"能被客户端救回的**唯一**途径(§7.12);省略则保持历史语义,每次调用分配新版本。
- **任何节点都能写**:非 leader 节点把已编码的提案经内部 `InternalService.Propose` 交给 leader 执行;接收方不是 leader 时回传 `leader_id` 让调用方改问,转发不会成链。错误以稳定 wire name(`internal/errors.Name` / `ByName`)跨进程传递,认不出某个名字的一方只保留 message,不臆造 sentinel。
- **卡住的版本有明确归宿**:PENDING 的版本若没有任何候选副本持有其数据,即为 **DATA_MISSING**(写入方在分配版本后、数据落盘前死亡,或每次都推送失败),客户端可以 `AwaitVersion` 看到 `data_missing` 后按同一 `client_request_id` 重发,或用 **`DiscardVersion`** 放弃;失败尝试耗尽重试预算或撞上不可恢复错误时,控制面判定 **FAILED_PERMANENT**,不再自动重试,只有运维能重试或放弃——重试是 `ForceRetryVersion`(只针对索引侧:数据侧的终态没有可重试的写入),放弃是 `ForceAbandonVersion`(只接受判死版本,走 `DeleteVersion` 的 SINGLE 语义);要看清队列里的全部判死版本用 `ListFailedVersions`。放弃会向**所有**候选副本广播物理回收,因为控制面并不知道数据实际落在哪几个副本上(§10.1 / §10.6)。
- **判死后的自动回收不靠广播**:每个 **Raft 成员**(leader 与全部 follower)从自己的 apply 得知终态后**本地**清(广播是尽力而为的一次性通知,分区或正在重启的副本收不到,而它恰恰最可能留着残留;启动时再扫一次状态机补回),检测到失败的那个节点还会提前清一次。**只有数据侧终态触发回收**——索引侧判死只说明构建失败,数据是好的,删掉它等于把 `ForceRetryVersion` 之后重建要读的东西毁掉(§10.1b)。**两层部署下的纯存储节点(`role=storage`)不参与 Raft**,所以它拿不到这条 apply 通知:只有"自己是检测者时就地清"与"重启时扫一次元数据"两条,残留只占空间而不影响正确性(幽灵数据不可达,可见性由控制层状态决定)。
- **数据侧与索引侧各有终态**(§10.1b):`DataStatus`(PENDING / DURABLE / FAILED_PERMANENT)与 `IndexStatus`(PENDING / READY / FAILED / FAILED_PERMANENT)互相独立——一个有持久数据却没有可用索引的版本,与一个索引从未构建的版本,是两件不同的事;判死时 `FailureSide` 说明原因链描述的是哪一侧,运维据此决定"重发写入"还是"重建索引"。版本元数据还携带文档集摘要 `doc_id_set_hash`(§7.9),让存储层能区分"空版本"与"摘要从未提交"。

## 文档切分与向量检索

### 内容定义分块(默认)

切块在 Stratum 内部完成(送进来的是文档全文),算法按知识库固定、创建后不可变——同一个知识库的每个版本必须切得一样,否则同一段文本产出不同 ChunkID,什么都复用不了。

| 模式 | 边界怎么来 | 状态 |
|---|---|---|
| **CDC**(默认,也是元数据的零值) | 文档字节上的滚动 Rabin 指纹,再吸附到句末标点、对齐到 rune 起点 | API 不做选择:每个知识库都按内容定义切块 |
| 滑动窗口 | 从头数固定的 rune 偏移(窗口 / 重叠) | 保留为对照实现,只有代码能选中 |

CDC 的默认参数:最小 256 B、最大 1536 B、期望平均 2^9 = 512 B。实测平均 **738 B / chunk**(滑窗为 766 B),因此检索粒度不变,而复用率不是。

由此得到两条实测收益(同一批真实语料,见 `docs/Stratum_设计文档v14.md` §6.1 与附录 C.3):编辑点之后保住 ChunkID 的 chunk 比例 **0% → 约 95%**,需要重新 embed 的文本 **64% → 10%**。

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

**实测**(2026-09,合成数据;方法见 `docs/Stratum_设计文档v14.md` §5.1 与附录 C.2):

- 召回:SQ8 / SQ_BF16 / SQ_FP16 ≈ Flat HNSW;PQ 随候选 N 提升(0.56@32 → 0.75@256);
- 端到端(top-10、候选 N=80、热 cache、单线程):OFF ≈ 0.27 ms,SQ8/FP16 ≈ 0.71 ms,PQ ≈ 1.0 ms——增量来自候选放大读盘 + rerank。候选数默认由**服务端策略**决定(`clamp(top_k × 8, 16, 4096)`);节点可用 `index_manager.candidate_n` 覆盖它(0 = 用服务端默认),这是"量化召回 ↔ 延迟"之间唯一的旋钮。

## 索引生命周期:构建、保留与回收

**构建**:每个版本一份产物,索引**不跨节点复制**——按 (知识库, 版本) 算出的 **owner 集**决定谁持有它,读只路由到 owner,于是构建只发生在**被请求到的那个 owner** 上(`-index-owner-inactive-k`,默认 2:活跃版本全体副本都是 owner,非活跃版本按"最近是否被查询"的优先级加档)。产物由需要它的一方**按需拉取**(`FetchIndexData`,装到本地即用,不必各自重建)。构建有**两级优先级**:`EnsureIndex` 触发的实时构建(有人在等)永远优先于 reconcile 的批量补建(没人等)——worker 空闲时先取高优先队列,空了才看低优先,因此**补建积压再多,也不会挡住一次实时写入**;全局并发上限为 `index_manager.build_concurrency`(0 = 按 CPU 核数),避免一批补建把 CPU、磁盘与 vecstore 一起占满。启动时 reconcile 只补 `PENDING` 与活跃版本,不做全历史重建。

**索引传输**:自 §8.4 改为**按需拉取**后,传输由需要产物的节点发起(`FetchIndexData`),接收方拿到产物直接加载、不必各自重建;提供传输的一侧受 `index_manager.fetch_concurrency` 限流(默认不限——闸门是给"同一版本被 N−1 个副本同时索要"这种突发准备的)。两端的产物都会拿到保留盾,不会在传输途中被保留策略删掉。

**保留**:磁盘上每 KB 保留 `gc.version_retention_count`(默认 50)个最新版本的产物——这个数同时是"分发还能修复多深的落后"。**访问保护**在其上再叠一层:窗口内(`index_manager.retention_protect_window_ms`,默认 24h)被查询过的版本额外保留,上限 `index_manager.retention_protect_max`(默认取 `version_retention_count`),因此磁盘最多保留 2×。访问时间写在 `<versionID>.index.used` 旁车里,所以进程重启后(启动时那一遍删得最多)依然有效;`RebuildIndex` / `WarmupVersion` 也登记同一份兴趣,运维手动重建的产物不会被随后的保留清掉。负值即关闭该保护,回到"只保新"。

**形态分层(§8.6a)与纯追加复用(§8.6c)**:某版本连续 `cold_threshold_ms`(默认 0 = 关闭)没被查询,它的索引就建成"免图"形态(量化码 + 暴力扫描,省掉图边的构建耗时与常驻内存),检索语义不变;它**是双向的**——免图版本在冷阈的一半内被查询过就重建回带图,两个阈值之间是迟滞,避免边界抖动来回重建。**形态在构建那一刻按本节点自己的访问记录决定**(没有后台扫描周期可调),所以只塑形真正被请求到、且本节点持有的版本。形态随产物 sidecar 一起走(`graph_free` 行),被按需拉去别的副本、或重启后都能正确识别,不会重复重塑。**它的收益与「每版本唯一向量数」成正比**(HNSW 图边 ≈ 0.26 KB/向量,M=32):压测用的那份**重复**语料(`genDataVolumeDocs`,6 句话循环拼接)每版本只有约 11 个唯一 chunk,实测只省 3 KB/版本(8.5%),构建时间与查询延迟都无可测差异;每版本上万点(≥ 2.6 MB/版本)时才值得开,百万点级别才是它的主场。阈值取该知识库的版本复核周期的 2–3 倍以上——判定的迟滞带是 `[T/2, T]`,阈值落在查询节奏附近会让形态来回翻转,而每次翻转都是一次全量重建。完整读数见 `docs/stress-test-report.md` §D(`COLD_THRESHOLD_MS=5000` 的 A/B)。若版本 N 只是 N-1 的纯追加且父产物还在本节点,构建就以其产物为起点只追加新增 chunk;因删除文档而留在产物里的"墓碑"占比超过 `index_manager.append_max_dead_ratio`(默认 0.2,1.0 = 不检查)时改用全量重建。

**墓碑回收(§8.6d)**:扫面默认就在跑(只读本地产物与当前文档集,不动任何东西),把墓碑占比超过 `index_manager.gc_ratio_threshold`(默认 0.3)的"活跃且长期没有后继版本"记进日志与 `GetSystemStatus` 的 `gc_blocked_versions`;真正**重写正在对外服务的产物**是 `index_manager.gc_enabled`,**2026-10 起默认开启**——它是滚动回收:一次只动一个副本,该副本临时退出服务池(查询返回可重试哨兵,服务站换候选),写完再回来。要回到"只报告不收集"就写 `false`。收集前先问控制层"除我之外还有几个副本在服务这个版本",不足 `serving_replica_min`(**默认 1**)就放弃——滚动回收的前提是"我暂时下线之后仍有人能服务"。**默认曾是 2,2026-10 降到 1**:索引是按需构建的,一个没被路由到的副本不会持有该版本的索引,于是 3 副本下"另有 2 个在服务"实际要求三个副本全都持有,收集永远启不动(实测 `others_serving=1 < minimum_required=2`)。仍不满足时以 `gc_blocked_versions` 上报(不会静默卡死),处置办法是调大副本数或调小这个门槛。带图形态没有"原地删向量"这条路(Faiss 的 HNSW 不支持 `RemoveIds`),只能整图重建,所以它的阈值 `graph_rebuild_ratio`(默认 0.5)比追加路径高得多。这里曾有一个**已知的待改进点**:报告门槛(`gc_ratio_threshold`)与追加路径的重建门槛(`append_max_dead_ratio`)默认值相同(都是 0.2),同值时 §8.6c 恰好会在 §8.6d 开始关心的那一刻重建,于是 (d) 在压测里永远看不到候选。**2026-10 已解开**:报告门槛默认抬到 0.3(复用门槛仍 0.2),`internal/index/gc_thresholds_test.go` 用一条断言把"回收门槛必须高于复用门槛"钉住。三个阈值仍都能在配置里分开调(`index_manager.append_max_dead_ratio` / `gc_ratio_threshold` / `graph_rebuild_ratio`)。

**残留回收**:构建在候选节点上失败或被放弃时留下的半成品(`.index.tmp` / `.index.ids.tmp`,或"有 `.index` 无 `.index.ids`")由后台扫描按 mtime 超过 `build_abandon_timeout_ms`(默认 30 分钟)回收;已封印的一对永不触碰,那是保留策略的职责。同一版本产物的读写另有版本分片锁,避免"一边重写、一边装载"。

## 一致性与恢复:游标、追赶与数据源

**连续游标是本地事实,且是持久的**。每个存储节点为每个知识库维护一个"完全持有的最高版本"游标,推进时同步落进 WAL(`CURSOR` 记录,幂等且单调;Compact 只保留每 KB 最新一条)。重启后游标**立即准确可用**,不再靠"扫磁盘上有哪些产物"推断——后者既慢,又会与保留策略耦合(产物被删 ⇒ 游标偏低)。没有记录的旧库才回退到推断,并把结论回写。

**落后副本自己追上来**。节点每 5 秒向控制 leader 上报游标,leader 在响应里捎带每个 KB 的**链尾**(已提交的最新版本——刻意不用 ActiveVersionID:落后是"数据链没跟上",而 active 只是"读路由选谁",回滚会让后者抖动)。本节点若发现 `本地游标 < 链尾` 且滞后达到 `lag_catchup.min_lag_versions`(默认 1),就在后台补齐:先走既有的数据拉取路径把数据追到链尾,再触发索引构建(若 owner 仍持有产物,直接按需拉取装载,沿用 `index_manager.fetch_concurrency` 闸门)。追链是**预热性质**的,绝不与实时查询抢构建队列;同时最多追 `lag_catchup.max_concurrent_kbs`(默认 2)个知识库,触发时带 `lag_catchup.jitter_ms` 的随机延迟把"同时发现"摊平。这里没有开关——把 SLO 上的节点关成"不可服务"不是选项,能调的只是节奏。补齐分两档:缺口里每个版本都能**逐版本重放**时走 WAL 记下的变更增量;缺口里含**已被删除**的版本时整段退回**全量状态传输**(取本次要 apply 的那个版本的整份状态)。判据来自控制层元数据——**只有确认删除才退,元数据读不到不退**;跳过被删版本继续增量重放是错的,那些版本的文档集要按父版本集合推出,而父版本已经不在了(`ListDocIDs` 对不存在的父版本返回空表且不报错,会静默丢文档)。

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

- **单节点**:`cmd/stratum`(gRPC,默认 `127.0.0.1:7000`)+ 外部 C++ vecstore 进程。vecstore 的写/读索引 RPC(`Save`/`Load`/`LoadForAppend`/`ExistsIndex`)接受调用方给的文件路径,所以它只允许在 `--index_dir` 指定的目录里读写;不传该参数时退化为 `--rocksdb_path` 的父目录,而部署脚本与网关 supervisor 显式传节点的 data_dir。少了这道闸门,能连上 vecstore 端口的一方就能以该进程的身份读写任意文件(`docs/code-review-2026-09-24.md` M4)。
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
  - 节点侧还有一道闸门:`service/authgate.go` 要求三个**客户端可见**的 service 必须带服务站的信任标记(开关 `require_authenticated`);节点间协作(`DataSyncService` / `InternalService`)不受影响、也不得要求。信任标记刻意不携带身份——接收方只需知道"有权限提问的东西替这次调用背了书"。标记本身是 **HMAC-SHA256(时间戳)**(`internal/authmeta`),密钥由服务站与节点共享(`-station-secret` / `node.station_secret`,脚本写在 `run/station-secret`),所以它**不可伪造**、且观察到的标记过了 TTL 就失效;`require_authenticated` 在配置了共享密钥时**默认开启**,显式写 `false` 才关。注意这两件事必须**成对配置**:只有一边有密钥时,节点会一律拒绝而不是退化成"接受任何标记"。详见 `docs/code-review-2026-09-24.md` 的 H4。
  - 网关自身的 `/ops` 运维台只绑回环(`-http-addr`,默认 `127.0.0.1:8081`),并对写请求做同源/Sec-Fetch-Site 校验;`bin_dir`、`cluster`、`docker.script`、`station_secret` 这些**决定执行什么**的字段不接受 HTTP 改写,服务二进制必须落在 `bin_dir` 内、编排脚本必须落在 `scripts/` 内(H2)。
  - 客户端凭据只在服务站一处校验(`-tokens`);`kb_ids: ["*"]` 表示"所有知识库,含将来创建的"(知识库 ID 由服务端随机生成,静态表列不出来)。控制台自己的凭据由网关以 `-station-token` 注入(`run/console-token`)。
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

环境变量可覆盖默认:`STRATUM_HTTP_ADDR`(网关监听,默认 `127.0.0.1:8081`——只绑回环,因为 `/ops` 能启停服务;要对外提供控制台就显式改它,并想清楚谁能访问)、`STRATUM_ROUTER_ADDR`(默认 `127.0.0.1:7009`)、`STRATUM_GRPC_ADDR`(单机模式下服务站应连的节点,默认 `127.0.0.1:7000`)、`STRATUM_STATION_SECRET`(覆盖 `run/station-secret`)、`STRATUM_STATION_TOKENS`(覆盖 `run/tokens.yaml`)。

`scripts/gateway.sh --with-db` 一键构建并启动完整链路:服务站与控制台先行,数据库服务经控制台 `/ops/start` 端点拉起——Web UI(默认 `http://localhost:8081`,含「运维」页)在数据库未运行时也可用;Ctrl+C 干净停止,日志在 `run/log/`。仅需运维:直接运行 `./run/bin/stratum-gateway`,在「运维」页编辑 `run/console.yaml` 的启动参数并启停服务。

> Docker 集群模式下 vecstore 是宿主机上的外部依赖(`vecstore.grpc_addr: host.docker.internal:7100`),必须监听宿主机的**对外接口**(`--grpc_addr=0.0.0.0:7100`);只绑 `127.0.0.1` 时容器内无法访问,会导致索引构建失败、删除报错等连锁问题。

## 性能实测

> 以下为 **2026-10-16** 在两层拓扑(控制组 3 + 存储组 3,每个存储容器自带真实 Faiss HNSW + RocksDB,768 维)上的实测,经服务站测量。宿主 **7.2 GiB 内存**。集群按当前代码 `clean` 后 `build` + `up` 重建,数据卷全新。口径:写入与读取分开称量。**逐轮口径、原始读数与所有发现见 `docs/stress-test-report.md` §D/§E**,本节是它的摘要(冲突时以报告为准)。

**这一轮测的就是默认配置**:索引 owner 分片开启(服务站 `-index-owner-inactive-k=2`)、§8.6(d) 墓碑回收默认开启(`gc_enabled=true` / `serving_replica_min=1` / `gc_ratio_threshold=0.3`)、冷阈 `cold_threshold_ms` 仍默认关闭。

### 查询延迟(全精度 `OFF`)

`TestT4_QueryLatency` —— 2,000 / 8,000 / 20,000 篇文档,各 200 次热查询 + 20 次全零向量对照,**冷热分开**报:

| 指标 | 2,000 篇 | 8,000 篇 | 20,000 篇 |
|---|---|---|---|
| 冷查询(重启副本后的首次) | 7.67 ms | 18.23 ms | 39.95 ms |
| p50(热) | **3.78 ms** | **9.78 ms** | **22.13 ms** |
| p95 | 5.41 ms | 13.27 ms | 31.00 ms |
| p99 | 6.37 ms | 17.62 ms | 37.83 ms |
| 平均 | 3.98 ms | 10.02 ms | 23.13 ms |
| 全零向量对照 p50 | 3.61 ms | 9.57 ms | 22.14 ms |

热 p50 随文档数近似线性(1 : 2.59 : 5.85,文档数 1 : 4 : 10)。**为什么冷热必须分开**:重启一个副本后,它的第一次查询要把产物从磁盘 `Load` 回内存,之后都在内存里——两者成本不可比,取平均既不描述常见情况也不描述最坏情况。

### 量化对照(`SQ8`,2,000 档)

同一个用例、同一台机器,只把知识库创建时的量化类型换成 `SQ8`(创建后不可变):

| 指标 | 2,000 篇 `OFF` | 2,000 篇 `SQ8` | 8,000 篇 `OFF` | 8,000 篇 `SQ8` |
|---|---|---|---|---|
| 冷查询 | 7.67 ms | 58.27 ms | 18.23 ms | 16.40 ms |
| p50(热) | 3.78 ms | **3.55 ms** | 9.78 ms | 11.82 ms |
| p95 | 5.41 ms | 4.80 ms | 13.27 ms | 15.32 ms |
| p99 | 6.37 ms | 5.23 ms | 17.62 ms | 17.59 ms |
| 平均 | 3.98 ms | 4.04 ms | 10.02 ms | 13.30 ms |
| 全零向量对照 p50 | 3.61 ms | 3.26 ms | 9.57 ms | 9.91 ms |

**量化买到的是内存,不是延迟**:`SQ8` 把每向量的内存载荷从 4 B/维压到 1 B/维,而热分位**两个方向都有**:2,000 档比全精度快 0.23 ms,8,000 档慢 2.04 ms(报告 §C.3 同档记的也是 +2.04 ms,同一方向、同一量级)。两个方向的绝对值都小于"宿主/页缓存"这类轮间差异,能确定的仍然是"没有收益"。`cold` 那一格是**单样本**(每轮只重启一个副本、只测一次首查询,且服务站可能把这一跳给别的副本),不据此下结论。写入侧 `SQ8` 的构建更快(2,000 档 4.04 s vs 9.90 s),但同为单次测量,**不做因果归因**。

> `SQ8` 取 2,000 与 8,000 档;20,000 档未跑。

### 查询成本的构成

分段计时只在存储节点日志级别为 `debug` 时产生(`query: stage timings`),日志本身会给延迟加一点开销,所以**绝对值以 `info` 级为准**,这里看的是**构成与占比**(经服务站、经 owner 分片;单位 µs,`chunkmap_us` 是 `filter_us` 的子项):

| 段 | 做什么 | 2,000 篇 p50 | 2,000 篇 p95 | 8,000 篇 p50 | 8,000 篇 p95 |
|---|---|---|---|---|---|
| `filter_us` | 候选的版本过滤 + 聚合(`chunkmap_us` 是它的子项) | **2061** | 3224 | **6722** | 9516 |
| ↳ `chunkmap_us` | 每个候选 chunk 一次 `ListDocIDs`(chunk → 文档) | 1138 | 2072 | 2903 | 4221 |
| `bloom_us` | bloom 过滤器 + 该版本的文档 ID 集合 | 37 | 53 | 164 | 245 |
| `meta_us` | 向控制面读版本元数据(连接已复用) | 295 | 661 | 345 | 685 |
| `search_us` | vecstore 的 HNSW 检索 | 324 | 719 | 381 | 670 |
| `read_us` | 读命中文档的正文 | 87 | 123 | 157 | 244 |
| **`total_us`** | 节点内合计 | **3026** | 5448 | **9005** | 23352 |

**大头不是向量检索**:`search_us` 只占 `total_us` 的 **4–11%**;主导项是 `filter_us`——把候选 chunk 映射回文档的那几次本地读,它随"候选数 × 文档数"一起涨。这与更早的 debug 轮(报告 §A.3)构成一致,而绝对值更低(2,000 档 `total` p50 4558 → 3026 µs;8,000 档 14247 → 9005 µs)——**不归因**:宿主、页缓存与数据卷状态都不同。

### 写入量

`TestT4_DataVolume` —— 每批 1,000 篇:

| 规模 | 写入(`CreateVersion` × N 批) | 累计索引构建 | 端到端 | 数据目录(3 节点)¹ |
|---|---|---|---|---|
| 2,000 篇(2 批) | **52.8 ms** | 9.90 s | 10.7 s | 62.8 MB |
| 8,000 篇(8 批) | **200.6 ms** | 48.27 s | 49.5 s | 438.6 MB |
| 20,000 篇(20 批) | **626.9 ms** | **4 m 25.6 s** | 267.8 s | 964.9 MB |

¹ **这一列是累积值**:三档连续跑、数据卷未清,后一档包含前面几档的知识库。**干净的单档占用**测过一次(测量前 `clean`,只跑 20,000 篇):**346.19 MiB**(3 节点;storage1 136.7 / storage2 108.9 / storage3 100.5 MiB),同一次端到端 373.5 s、构建 **6 m 11.4 s**——比上表热身后的 4 m 25.6 s 慢,因为冷启动时后台 reconcile 正与写入的索引构建争资源。

**写入 API 与规模几乎无关**(20,000 篇 20 批合计 0.63 s);代价全在索引构建,且随**版本数**超线性(8 批 → 20 批:48 s → 266 s,批次 2.5×、耗时 5.5×)。这是"每个版本一份产物"的固有开销——owner 分片减少的是**持有份数**(每版本只有 owner 集持有),不是每一份的构建成本。

**owner 分片对构建次数的效果**(报告 §C.6 留的待办,第一次读数):同一批写入、同一集群,只换服务站的 `-index-owner-inactive-k` —— 8,000 篇(8 个版本)时 `k=0`(关闭)产生 **21** 次索引构建,`k=2`(默认开)**16** 次(= 8 版本 × 2 个 owner,一个不多);2,000 篇(2 版本)时是 4 vs 3。也就是说**默认配置下的索引构建次数比"每个副本各建"低约 24%**,而降幅随非活跃版本的比例增长。

### 多版本换出与墓碑回收(2,000 档)

- **多版本换出**(`TestT4_MultiVersionEviction`,6 版本 × 3 轮 = 18 次查询):每个版本在轮换中**始终应答**,12.7 s;
- **墓碑回收**(`TestT4_GCPressure`,2,000 篇删 80%):产物 **33,064,960 → 16,217,948 B**(回收 **51%**),收集后查询正常,40.0 s。这是 `gc_enabled` 默认开启 + `serving_replica_min=1` 之后第一次端到端确认(默认还是 2 时同一用例失败并打印 `others_serving=1 < minimum_required=2`,见报告 §E.4)。

### 冷版本免图分层(§8.6a)的标定

同一个套件、只换 `cold_threshold_ms`(报告 §D 的 A/B;`5000` ms 是**加速因子**,把"多久没被查算冷"压到秒级;判定逻辑与时间尺度无关,所以形状可比、绝对值不可搬):

| 组 | `cold_threshold_ms` | reshape 次数 | 形态(带图 / 免图) |
|---|---|---|---|
| 基线 | 0 | **0** | 27 / 0 |
| 开启 | 5000 | 16 | 19 / 9 |

逐产物对照:带图 **36,966 B** vs 免图 **33,837 B** → **省 3,129 B / 产物(8.5%)**。这 3 KB 就是 HNSW 的图结构:3,129 ÷ 11 个 chunk ≈ 284 B/chunk,与 M=32 邻居表(≈256 B/点)吻合。

**结论:保持默认关**。收益 ≈ **0.26 KB/向量**;而压测语料每版本只有约 11 个唯一 chunk(`genDataVolumeDocs`,6 句话循环拼接),所以只省 8.5% × 37 KB,构建与延迟都无可测差异。每版本上万点(≥ 2.6 MB/版本)才值得开,且阈值要 ≥ 该知识库版本复核周期的 2–3 倍——判定的迟滞带是 `[T/2, T]`,阈值落在查询节奏附近会让形态来回翻转,而每次翻转是一次全量重建。

### 资源占用

宿主只有 **7.2 GiB**,而两个最重的档各开着一整套两层 3+3 集群(每个存储容器内含 vecstore + RocksDB)。实测内存轨迹(压测期间每 20 s 采样):

| 档 | used | available |
|---|---|---|
| `QueryLatency` 20,000 | 4.17–4.28 GB | 3.1–3.2 GB |
| `DataVolume` 20,000 | 3.99–4.42 GB | 2.96–3.39 GB |

**全程没有接近吃紧**——7.2 GiB 上跑这两档是可行的。(压测期间有一次桌面被压垮,原因是"集群 + `docker build` + Go 构建同时开",不是压测本身。)

### 与更早轮次的对照(热 p50,全精度)

| 规模 | §0(09-20) | §A(09-21) | §C(10-15) | **§E(本轮)** |
|---|---|---|---|---|
| 该轮的 owner 分片 | 不存在 | 不存在 | 已落地但**关闭**(`=0`) | **默认开**(`=2`) |
| 2,000 | 4.24 ms | 4.85 ms | 4.79 ms | **3.78 ms** |
| 8,000 | 8.18 ms | 10.81 ms | 9.99 ms | **9.78 ms** |
| 20,000 | 18.45 ms | 35.50 ms | 30.61 ms | **22.13 ms** |

三档都落在同一散布内或更低。**这不构成"变快了"的结论**:轮间差异的主要来源是宿主、页缓存与数据卷状态(报告 §B 论证过);而且 §C 之前 owner 分片还是**关闭**的,不能与 §E 直接比较。

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
| `DeleteVersionData` | 回收本节点上某版本的物理数据(**删除版本**时向所有候选副本广播:控制面不知道字节在哪几个节点上;判死后不再走这条广播,改为每个 Raft 成员从自己的 apply 得知后本地回收(不参与 Raft 的存储节点只有"检测者就地清"与"重启扫一次"两条);**接收端无法执行清理时以错误回答**,不静默成功——否则控制面会先删掉元数据行,这里的残骸再也没人能命名) |
| `ConfirmVersionWrite` | 告知副本它收到的版本已达 quorum,stand down 其接管计时器 |
| `FetchIndexData` | 流式**拉取**已构建的索引产物,接收方直接加载而不再各自重建(索引唯一的跨节点传输方向;推送/分发侧已整体移除):按 (kb, version) 幂等,对端不持有该版本时以 `ErrIndexNotExported` 回答,调用方据此改问其他副本或自建。提供传输的一侧受 `index_manager.fetch_concurrency` 限流(<= 0 不限) |