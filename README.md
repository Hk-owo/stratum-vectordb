# Stratum

**分布式版本化向量检索引擎 —— 为 RAG 构建的可回滚、可审计知识库存储层。**

文档经 embed 服务向量化后,以 **MVCC 版本** 为单位组织、索引与查询:一次写入产出独立新版本,查询永远落在明确的版本上,回滚 / 版本对比 / 审计都是一等公民。元数据由 **Raft** 在节点间强一致,向量由 **C++ Faiss HNSW** 索引;存储按 **L0 内存热层 / L1 磁盘冷层 / L2 永久层** 分层,大知识库可选**量化两段式检索**,内存载荷压缩 4–32× 而精度不变。

名字取自"地层":文档的每次变更形成一层可独立回滚、可逐版本比对的版本;存储亦分热、冷、永久三层。

## 亮点

- **MVCC 版本化知识库** —— 版本链 + 活跃版本随时切换:
  - **回滚**:一次有问题的更新,一条调用即可撤销,不停服;
  - **版本对比**:任意历史版本都可独立查询、逐个比对;
  - **审计**:每个查询结果都追溯到产生它的具体版本。
- **内容寻址 chunk** —— `ChunkID = SHA-256(text + model ID)` 天然去重,无需跨节点协调。
- **每版本独立 HNSW 索引** —— 版本之间零干扰;索引按 LRU 驻留内存,换出/重载廉价,不会影响查询结果精度。
- **量化两段式检索(可选,默认全精度)** —— 开启后内存只驻留量化粗筛器(HNSW 图 + SQ/PQ 码),查询先粗筛出候选、再按候选读 L2 全精度向量做精确 rerank:精度由全精度 rerank 决定,量化只影响候选覆盖。量化后端为 Faiss 内建 `IndexHNSWSQ`/`IndexHNSWPQ`——评估 hnswlib(无量化)、DiskANN(C++ 分支已停止维护)、USearch(仅免训练标量降位、需整体换后端)后选定:零新依赖,且与现有 `write_index`/`read_index` 序列化格式完全兼容。
- **索引对象生命周期状态机** —— 每个 vecstore 索引实例显式区分 `EMPTY`/`BUILDING`/`READY` 三态,读锁贯穿查询(含 rerank 磁盘 IO)、写锁保护构建/加载/重置,杜绝 Search 与并发 Reset/Load/AddChunks 之间的 use-after-free 与错位结果。
- **Raft 强一致 + 崩溃一致性** —— 元数据写操作经 leader 并受 WAL 保护;查询可负载均衡到任意节点;快照不阻塞心跳与写入。
- **控制面 / 存储面契约分离** —— `internal/plane` 定义两层之间的契约(`ControlPlane` / `DataPlane`),只传逻辑对象(知识库、版本、抽象可用性),**从不暴露副本数、纠删码、文件路径或节点身份**,因此两层可以拆成独立进程/集群而控制层无需知道数据放在哪。`node.role` 已支持 `all`(默认,两层同进程)与 `storage`(只跑存储层,不留 Raft 日志,元数据经 `RemoteRaftNode` 走 gRPC 读取),`storage.nodes` 声明存储组;纯控制面(`control`)角色尚未实现。
- **任何节点都能发起写** —— Raft 只在 leader 追加日志,但"刚写完一个版本""启动 reconcile 有结论"这类事实可能发生在任何节点,所以非 leader 把提案经内部 `InternalService.Propose` 转给 leader(转发不成链);错误以稳定 wire name 跨进程传递,转发后 `errors.Is` 依然成立。
- **写幂等 + 卡住的版本有明确归宿** —— `CreateVersion` 的 `client_request_id` 让重试复用首次分配的版本而非另分配一个;PENDING 不再等同于"永远在构建":**DATA_MISSING**(没有任何候选副本持有该版本的数据,只能由写入方按同一 key 重发救回)与 **FAILED_PERMANENT**(重试预算耗尽或不可恢复,只有运维能重试或放弃)都会在 `GetSystemStatus` 里显式列出。
- **自动存储卫生** —— 周期 chunk GC、每版本布隆过滤器、启动时从磁盘事实推导版本 READY 状态(不依赖构建回调确实送达)。
- **开箱可运维** —— 三态健康检查;HTTP 网关 + Web 控制台(同源提供、免 CORS);`start.sh` 一条命令拉起完整链路。

Stratum 是 RAG 管线的**存储与检索层**:不处理聊天历史、用户会话或 prompt 构造——这些属于其上方的应用层。

## 快速开始

```bash
# 构建
go build ./cmd/stratum/

# 运行全部测试(23 个包含测试,共 29 个 Go 包)
go test ./... -timeout 180s -count=1

# 竞态检测(共识与索引核心)
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# 单节点服务
go run ./cmd/stratum/

# 一键拉起完整链路:vecstore(C++) → stratum(gRPC) → 服务站 → gateway(HTTP) + Web UI
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
│        服务站 station(stratum-router):检索入口 + 鉴权闸门             │
│   按 min_version 取副本 · 副本不健康换候选 · ErrIndexMaintenance 重试  │
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

三层职责的边界是这套设计的核心:**服务站**只做"选谁、怎么重试",不持有任何状态——它坏掉只影响路由,不影响正确性;**控制面**只拥有 Raft 复制的元数据,回答"哪些版本存在、谁是活跃版本、有多少副本 READY";**存储面**拥有全部物理事实——文档、chunk、向量索引、复制、放置与修复。向量计算全部下沉到 C++ vecstore,Go 层只负责编排。

两段检索的并发语义(Search 持读锁贯穿,含 rerank 磁盘 IO)由索引对象的显式状态机 + 每实例锁保证,杜绝 use-after-free 与错位结果。

控制面与存储面之间是一条**只传逻辑对象、不传放置细节**的契约(`internal/plane`)。契约的同进程实现(`LocalControlPlane` / `LocalDataPlane`,连同写门、leader 门、数据版本注册表、回收水位、追链 backfill)已落地,它也是把两层拆成独立进程的前提;`internal/wire` 负责 gRPC proto 与领域类型的**双向**映射,避免某个字段只在一侧被加上而另一侧静默丢弃。

两层既能在同一进程里跑,也能分开部署:`role=control` 的节点不写本地存储(只有 Raft 日志与快照),`role=storage` 的节点不参与选举(它从控制面读元数据、向控制面报告进度)。上面的服务站是**独立形态**——`scripts/docker-cluster-both.sh up --with-station` 一次拉起"控制面 + 存储面 + 服务站 + embed"的完整拓扑。

## 核心概念:版本化文档库

- **版本链**:`CreateVersion` 一次调用应用任意数量的文档变更(ADD / DELETE / UPDATE),产出新版本并异步构建索引;父版本须已 READY,且最多只能有一个子版本(版本链严格线性,不支持分叉)。`RollbackVersion` 无停机切换活跃版本。
- **版本删除三种模式**:`DeleteVersion` 由 `mode` 决定删除范围——`SUBTREE`(默认,删目标版本及其全部后代)、`SINGLE`(只删目标版本,其唯一后继自动改挂到它的父版本上,因此任意"中间版本"都能单独删掉)、`ANCESTORS`(删目标版本的全部前置版本,使其成为版本链新的基底)。响应回传本次实际标记删除的版本清单;活跃版本与 PENDING 版本始终不可删。
- **MVCC 零成本快照**:基于 PebbleDB 前缀编码,未变更文档在新版本中零拷贝;文档历史被压缩保存。
- **布隆过滤器**:每个版本一份完整文档 ID 集合的布隆过滤器,成员检查开销极低;磁盘副本缺失时自动从 `VersionDocList` 重建。
- **垃圾回收**:`ChunkGarbageCollector` 周期性(默认 5 分钟,`chunk_gc.sweep_interval_sec`)清扫不再被任何版本引用的 chunk。sweep 两遍:先无锁枚举孤儿候选,再持写锁按 Raft **当前**版本复查后删除,与并发写入互斥、不依赖过期快照(stale-snapshot race 免疫),锁粒度为一个 chunk,阻塞毫秒级。
- **写幂等**:`CreateVersion.client_request_id` 是可选幂等键——同一知识库上重发同一 key 会复用首次分配的版本,而不是再分配一个。这是"数据没落地的版本"能被客户端救回的**唯一**途径(§7.12);省略则保持历史语义,每次调用分配新版本。
- **任何节点都能写**:非 leader 节点把已编码的提案经内部 `InternalService.Propose` 交给 leader 执行;接收方不是 leader 时回传 `leader_id` 让调用方改问,转发不会成链。错误以稳定 wire name(`internal/errors.Name` / `ByName`)跨进程传递,认不出某个名字的一方只保留 message,不臆造 sentinel。
- **卡住的版本有两种终点**:PENDING 的版本若没有任何候选副本持有其数据,即为 **DATA_MISSING**(写入方在分配版本后、数据落盘前死亡,或每次都推送失败),只能靠客户端按同一 `client_request_id` 重发恢复;失败尝试耗尽重试预算或撞上不可恢复错误时,控制面判定 **FAILED_PERMANENT**,不再自动重试,只有运维能重试或放弃——放弃会向**所有**候选副本广播物理回收,因为控制面并不知道数据实际落在哪几个副本上(§10.1 / §10.6)。
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

**实测**(2026-09,合成数据;方法见 `Stratum_设计文档v13.md` §2.5 与附录 D):

- 召回:SQ8 / SQ_BF16 / SQ_FP16 ≈ Flat HNSW;PQ 随候选 N 提升(0.56@32 → 0.75@256);
- 端到端(top-10、候选 N=80、热 cache、单线程):OFF ≈ 0.27 ms,SQ8/FP16 ≈ 0.71 ms,PQ ≈ 1.0 ms——增量来自候选放大读盘 + rerank,可按需调小 `candidate_n`。

## 部署形态

- **单节点**:`cmd/stratum`(gRPC,默认 `127.0.0.1:7000`)+ 外部 C++ vecstore 进程。
- **多节点集群**:多个 stratum 节点组成 Raft 集群,共享一套元数据。
- **角色分离**:`node.role` 选择进程承担哪一半——`all`(默认,控制面 + 存储面同进程,即原有形态)、`control`(**只跑控制面**:Raft 日志与元数据,不持有任何 data plane,连 vecstore 数据目录都不创建)、`storage`(只跑存储层:文档 / chunk / 索引 / vecstore,不留 Raft 日志,元数据经 `RemoteRaftNode` 读取,也从不参与选举)。存储组由 `storage.nodes` 声明;省略即"每个 Raft 成员都持有数据",与拆分前的部署完全一致,老配置无需改动。
  - **两层拓扑的两条硬约束**:① 控制层 ID(`1..N`,即 Raft member ID)与存储层 ID(`11..1N`,同时是 `storage.nodes` 的键)**不得重叠**,否则控制节点会把存储节点的地址认成自己的;② 写入由控制层在 apply 时按副本拓扑**指派**给某个存储节点执行(`ExecuteVersionWrite`,那个节点作为该次写入的协调者再 fan-out),读由服务站路由到持有该版本的存储节点。

```yaml
# 一个只跑存储层的节点:数据在这里,元数据在控制集群
node:
  node_id: 4
  role: storage            # all(默认) | storage | control(未实现)
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

- **服务站 `cmd/stratum-router`**:集群**唯一**的对外入口。六项职责:① 路由表缓存(`KB + version → 可服务节点`,从控制层聚合**异步刷新**,不是自己逐节点探测);② **新鲜度凭证**(转发前附上"当前应看到的版本号",存储节点核对本地连续游标,不够就拒——把"悄悄返回过时结果"变成显式失败,服务站再换一个达标候选);③ 读负载均衡;④ 故障转移(同一客户端连接内换候选,客户端无感);⑤ 鉴权(token 表 → 租户/权限,数据面完全不必对外);⑥ 健康检查/熔断(closed / open / half-open 三态,状态是服务站**本地**态、实例间不同步,这是它能无状态水平扩展的前提)。另有 **超时预算传播**(按剩余候选数切分,避免一个慢节点吃掉整个预算)与背压。
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
| 职责 | leader 发现、写转发、读均衡、副本选择与换候选、熔断、鉴权闸门 | HTTP→gRPC 转换(`protojson`)、静态资源托管、`/ops/` 运维台 |
| 知道集群拓扑吗 | **知道**——要选副本、要把写转发给 leader | **不知道,也不关心**——只连服务站一个地址 |

关键在最后一行:**leader 会换、副本会增减、某个副本会短暂不可用**,这些都该由服务站吸收,而不是泄漏到边缘。所以拓扑变化只动服务站;换前端、加 HTTP 鉴权、把网关放到 DMZ 多开几个实例,都不碰集群。鉴权闸门也只在服务站一处(`-tokens`),SDK 与浏览器过的是同一道闸。

**只要程序接入(用 SDK 直连 `:7009`)就完全不需要网关**——它是可选的纯适配层,不装它集群照常工作,只是没有浏览器入口。

> 浏览器打不开 `http://localhost:7009` 是**正常的**:那是 gRPC 端口,HTTP/1.1 请求会得到 `Received HTTP/0.9`,HTTP/2 请求得到 `415`(只接受 `application/grpc`)。要看界面请用网关的 `:8081`。

```bash
scripts/gateway.sh [single|build|stop]   # 默认:构建后起 Docker 集群模式;single=连 127.0.0.1:7000
scripts/router.sh status|stop            # 单独管理服务站(两层拓扑下会从 run/console.yaml 自动派生 -storage-nodes)

# 两层集群(控制组 + 存储组)一键起停,细节见脚本头部
scripts/docker-cluster-both.sh build     # 两个镜像;存储镜像自带 vecstore(scripts/build-storage-image.sh)
scripts/docker-cluster-both.sh up        # 控制组 1..3(17000+) + 存储组 11..13(17100+) + mock-embed
scripts/docker-cluster-both.sh status    # 每容器状态与控制组 leader

# 手动等价:先起服务站,再起 gateway(始终指向服务站)
./run/bin/stratum-router  -listen 0.0.0.0:7009 -nodes 127.0.0.1:7000
./run/bin/stratum-gateway -grpc-addr 127.0.0.1:7009
```

环境变量可覆盖默认:`STRATUM_HTTP_ADDR`(网关监听,默认 `0.0.0.0:8081`)、`STRATUM_ROUTER_ADDR`(默认 `127.0.0.1:7009`)、`STRATUM_GRPC_ADDR`(单机模式下服务站应连的节点,默认 `127.0.0.1:7000`)。

`start.sh` 一键构建并启动完整链路:服务站与控制台先行,数据库服务经控制台 `/ops/start` 端点拉起——Web UI(默认 `http://localhost:8081`,含「运维」页)在数据库未运行时也可用;Ctrl+C 干净停止,日志在 `run/log/`。仅需运维:直接运行 `./run/bin/stratum-gateway`,在「运维」页编辑 `run/console.yaml` 的启动参数并启停服务。

> Docker 集群模式下 vecstore 是宿主机上的外部依赖(`vecstore.grpc_addr: host.docker.internal:7100`),必须监听宿主机的**对外接口**(`--grpc_addr=0.0.0.0:7100`);只绑 `127.0.0.1` 时容器内无法访问,会导致索引构建失败、删除报错等连锁问题。

## gRPC API

Protobuf 定义在 `api/proto/`:三个外部服务(下面三节)加三个内部服务(`DataSyncService`、`InternalService`、C++ 端的 vecstore 服务)。所谓内部,是指有意不向集群外暴露,只供节点之间使用。

**KnowledgeBaseService**(知识库与版本生命周期)

| RPC | 说明 |
|---|---|
| `CreateKnowledgeBase` | 创建知识库(embed 配置、chunk 窗口、索引类型) |
| `DeleteKnowledgeBase` | 标记删除,清理异步执行 |
| `CreateVersion` | 应用文档变更(ADD / DELETE / UPDATE)并产出新版本;可带 `client_request_id` 幂等键 |
| `ListVersions` | 返回知识库版本链 |
| `RollbackVersion` | 切换活跃版本,无停机 |
| `ListKnowledgeBases` / `GetKnowledgeBase` | 列出 / 查询知识库及其活跃版本 |
| `DeleteVersion` | 按 `mode` 删除版本:`SUBTREE`(默认,含后代)/ `SINGLE`(仅该版本,子版本改挂其父)/ `ANCESTORS`(删前置版本,使其成为新基底);清理异步执行 |

**QueryService**

| RPC | 说明 |
|---|---|
| `Query` | 向量相似度检索(阈值、top-k、聚合) |

**AdminService**

| RPC | 说明 |
|---|---|
| `HealthCheck` | 三态健康检查(HEALTHY / DEGRADED / UNHEALTHY) |
| `GetSystemStatus` | 卡住版本、**数据缺失版本**(DATA_MISSING)、**永久失败版本**(FAILED_PERMANENT)、删除失败的知识库、WAL 告警、资源占用 |
| `GetClusterStatus` | 节点 Raft 视图(node_id / leader_id / member_count),供服务站发现 leader |
| `RebuildIndex` / `WarmupVersion` | 重试失败版本的索引构建 / 预热版本索引入内存(不切换活跃版本) |

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
| `ReportDataVersions` | 存储层周期性向控制 leader 上报数据游标;"谁持有版本 V"由此是一份定期刷新的权威视图,而非一次性快照 |
| `DeleteVersionData` | 回收本节点上某版本的物理数据(FAILED_PERMANENT 时向所有候选副本广播) |
| `ConfirmVersionWrite` | 告知副本它收到的版本已达 quorum,stand down 其接管计时器 |
| `PushIndexData` | 流式**推送已构建的索引**,副本直接加载而不再各自重建("建一次、分发 N 份") |

## 工程与测试

| 批次 | 范围 | 状态 |
|---|---|---|
| T1 | 单模块契约(8 个模块) | ✅ |
| T2 | 跨模块集成(4 组) | ✅ |
| T3 | 单节点全链路(15 个场景) | ✅ |
| T4 | 3 节点 Raft 集群(进程内 + Docker 集群 + 数据量 + 压测) | ✅ |
| T5 | 真实栈 e2e(真实 Pebble/WAL/Raft/IndexManager + vecstore 子进程,零 mock) | ✅ |

```bash
go test ./integration/... -run TestRealStack -v   # 全真实栈端到端(需 C++ 二进制,缺失时跳过)
go test ./integration/... -run TestMultiNode -v   # 3 节点进程内集群:选主、复制 KB + 版本元数据
STRATUM_STRESS_DOCS=20000 go test ./integration/docker/ -tags=docker -count=1 -run TestT4_QueryLatency -v
```

全量单测(23 个测试包)与 3 节点 Docker 集群(T4)命令见[快速开始](#快速开始)。

CI(`.github/workflows/ci.yml`,push main 与 PR):gofmt + `go vet` + `go build` + 23 个测试包的单测 + raft/kvraft/index 竞态检测 + 3 节点 Docker 集群(T4)容错运行;C++ vecstore 由手动触发的工作流(`vecstore-cpp.yml`)覆盖。

### 索引构建调度(2026-09)

索引构建不是"每个请求起一个 goroutine"就完事：重启后要对多个知识库补建索引，而**实时写入的构建不能排在补建后面等**。当前实现有两条约束：

- **有界并发**：全局 worker 池限制同时在跑的构建数（`index_manager.build_concurrency`，0 = 按 CPU 核数），避免一批补建把 CPU、磁盘与 vecstore 一起占满；
- **两级优先级**：`EnsureIndex` 触发的实时构建（有人在等）永远优先于 reconcile 的批量补建（抢跑，没人等）——worker 空闲时先取高优先队列，空了才看低优先，因此**补建积压再多，也不会挡住一次实时写入**。

顺序语义没有变化：构建仍只跑在一个候选节点上，完成后按 §8.4 分发给副本。

### 性能实测(3 节点 Docker 集群,真实 Faiss HNSW + RocksDB,768 维)

拓扑是 `scripts/docker-cluster-both.sh` 的两层形态:3 个控制节点 + 3 个存储节点,每个存储容器自带 vecstore(真实 Faiss HNSW + RocksDB);宿主 8 核 / 7.2 GB,容器与压测进程共享这台机器。

**查询延迟**(`TestT4_QueryLatency`)——2,000 篇文档、200 次查询,**冷热分开**报:

| 指标 | 值 |
|---|---|
| 冷查询(重启副本后的首次,含磁盘 `Load`) | **18 ms** |
| p50(热) | **15 ms** |
| p95 | **27 ms** |
| p99 | **34 ms** |

**同一台机器、同一口径下的三轮对比**(2026-09,见下"查询路径的两处 O(n) 修复"):

| | cold | p50 | p95 | p99 | 用例时长 |
|---|---|---|---|---|---|
| 最初 | 769 ms | 679 ms | 771 ms | 847 ms | 185 s |
| 修掉 O(候选 × 文档) 后 | 65 ms | 42 ms | 56 ms | 66 ms | 55 s |
| **再修掉"读完再截断"后** | **18 ms** | **15 ms** | 27 ms | 34 ms | 52 s |

单次查询的成本合计降了 **约 44×**。**cold − warm ≈ 3 ms** 才是从磁盘 `Load` 产物本身的价格,其余是检索、结果组装与一次 gRPC 往返。

**为什么冷热必须分开**:重启一个副本后,它的第一次查询要把产物从磁盘 `Load` 回内存,之后都在内存里——两者成本不可比,取平均得到的数字既不描述常见情况也不描述最坏情况。这正是设计文档阶段⑤"含磁盘读"的落点;尾部分位同理,一个检索服务是被它最慢的查询定义的。

**查询向量的口径**:套件**现在发确定性非零向量**(`queryVector(768)`,固定种子),与真实调用方一致。曾经每个查询都发全零向量 —— 它与所有文档等距,HNSW 贪心遍历无从剪枝、退化为近似全图扫描;那是真实存在的放大效应,但只是**放大器**:修掉下面两处 O(n) 之后,`TestT4_QueryLatency` 里保留的零向量对照与随机向量已经**几乎无差别**(p50 15.4 ms vs 15.3 ms),因为检索本身只占总延迟的一小块了。

**查询路径的两处 O(n) 修复(2026-09)**:两处都在 `service/query.go`,共同点是把"与命中规模成正比"的工作变成了"与需要的结果成正比"。

1. **每个候选重扫一遍版本的完整 doc-ID 列表** —— 对每个候选 chunk(及其映射到的每个 docID)都做一次前缀扫描取回全部文档 ID、再线性查找成员 ⇒ 单次查询 O(候选数 × 文档数)。改成"每次查询取一次 + 集合查找"。定位时**先排除了 vecstore**:它内部 faiss HNSW 搜索只花 **16 µs**(临时探针 `VECPROBE search ntotal=18 took_us=16`)。
2. **读完再截断** —— 对**每个**命中文档读一遍正文,再排序、截到 top-k,绝大部分正文直接丢掉。chunk 是内容寻址的,一个 chunk 可属于成千上万个文档(近似重复的语料塌缩成少量 chunk),实测该版本 `matched_docs: 1000`、`top_k=5`。改成先聚合打分、再排序、只为中选的 top-k 读正文。

| | 修复前 p50 | 修复后 p50 |
|---|---|---|
| 2,000 篇,随机向量 | 207 ms | **23 ms** |
| 8,000 篇,随机向量 | 2.73 s | **49 ms**(56×) |
| 8,000 篇,全零向量 | 10.23 s | **90 ms**(114×) |

同时"4× 数据 ⇒ 12–13× 延迟"的超线性也消失了(变成约 2×)。第二处的效果由**新的分段耗时日志**当场指出:打开 debug 后 `query: stage timings` 显示 `read_us: 8684` 占了 `total_us: 22909` 的 38%,而 `search_us` 只有 389。

**服务端现在有分段耗时了**:`logging.level: debug` 即生效(此前这个配置项**没有任何代码读取**,节点永远是 info 级),`Query` 会打一行 `query: stage timings`(search / filter / read / total + 候选数与命中数)。这是定位上面两处问题时缺的那件工具 —— 当时只能靠外部探针加临时 C++ 插桩。定位过程见 HANDOFF「链上各部分耗时」。

规模可放大,但**时间预算要一起放大**:`STRATUM_STRESS_TIMEOUT` 默认 10 分钟在 8,000 篇下不够(200 次 warm 只跑完 166 次就撞上 deadline)。

```bash
STRATUM_STRESS_DOCS=8000 STRATUM_STRESS_TIMEOUT=35m \
  go test ./integration/docker/ -tags=docker -count=1 -run TestT4_QueryLatency -v
```


**写入量**(`TestT4_DataVolume`)——真实栈上采样(window=512 切分、mock embed 10 ms/chunk;每个存储容器运行自己的 vecstore,共享会引发并发 `Build = Reset + AddChunks` 竞态)。本轮实测(两层 3+3,宿主 8 核 / 7.2 GB,`STRATUM_VOLUME_DOCS=10000`,每批 1,000 篇):

| 指标 | 值 |
|---|---|
| 写入(`CreateVersion` × 10 批,仅 API 用时) | **2.55 s** |
| 累计索引构建(10 批串行,每批等前一批 READY) | **4 m 2 s** |
| 端到端 | **4 m 5 s** |
| 存储占用(存储节点 1 / 2 / 3) | **306.8 / 157.4 / 155.1 MiB**,合计 **619 MiB** |
| 查询 | top-k=10 返回 10 条结果 |

```bash
STRATUM_VOLUME_DOCS=10000 go test ./integration/docker/ -tags=docker -count=1 -run TestT4_DataVolume -v
```

**读法**:写入 API 只花 2.55 s,而 4 m 2 s 全在索引构建上——"每版本独立索引"模型下,吞吐由构建而非写路径决定。占用分布不均(第一个存储节点多出约一倍)是构建者与副本的差别,不是三份完整拷贝。

要点(与数字无关):写入须分批——单条 `CreateVersion` 受 4 MiB gRPC 消息上限约束(约 1,400 篇),每批成一版本且前一批 READY 后才链接;耗时随版本号递增(每版本重写完整 doc-ID 集并重建索引),这是"每版本独立索引"模型的固有开销。100k 轮还验证了 raft 快照(`max_log_length=150` 触发 2 次):日志即时 trim、写入与心跳不停摆——快照在 RLock 下深拷贝并异步持久化,apply 与心跳永不被阻塞。

### 压力测试(3 节点 Docker 集群,真实 Faiss HNSW + RocksDB,768 维)

压测填补的是设计文档阶段⑤ 要求、此前**没有任何实现**的两项——`TestT4_PerformanceBaseline` 与 `TestT4_StorageEfficiency` 都只是 `t.Skip` 占位。规模用环境变量控制(`STRATUM_STRESS_DOCS` / `_QUERIES` / `_VERSIONS` / `_TIMEOUT`),默认值小到能进 CI。

**定位:写入与读取分别称量。** 每个用例都是"写完并 READY 之后才开始读",两者从不并发——这不是漏测,而是刻意的:混跑会让写入侧的构建/IO/CPU 摊进查询的分位里,得到的数字既不是写入成本也不是读取成本。写入的账由 `TestT4_DataVolume` 给(写/构建/占用),读取的账由 `TestT4_QueryLatency` 给(冷热分位)。

本轮(2026-09,两层 3+3 集群,宿主 8 核 / 7.2 GB)实测:

| 用例 | 测什么 | 实测 |
|---|---|---|
| `TestT4_QueryLatency` | 单版本查询延迟,**冷热分开报** | 2,000 篇 / 200 次查询:cold **18 ms** · p50 **15 ms** · p95 **27 ms** · p99 **34 ms**(确定性非零查询向量;同口径最初为 cold 769 ms / p50 679 ms) |
| `TestT4_MultiVersionEviction` | 多版本分级换出的稳定性 | 6 版本 × 3 轮轮转(18 次查询),每个版本始终应答 |
| `TestT4_GCPressure` | 墓碑回收的端到端可见性(写入 → 删除 → 观察产物 → 查询仍正确) | **SKIP**:扫描器在跑,但没有候选——默认 `GCRatioThreshold` 与 §8.6(c) 的 `AppendMaxDeadRatio` 都是 0.2,两者互抵;诊断信息会指明卡在哪条门 |

**这轮压测顺手修掉的一个真缺陷**:重启一个存储副本后,它的内存游标(`localVersion`,§7.8)从 0 开始,磁盘数据完好却被 §9.3(2) 的新鲜度检查拒服务(`local history reaches version 0, below the required N`)。现在启动时从**本节点自己的事实**重建连续游标(`plane.RecoverLocalCursors`),同一场景由稳定失败转为通过——故障史与边界见 HANDOFF 与设计文档 §5 第 16 条。

**为什么冷热必须分开报**:重启一个副本后,它的第一次查询要把产物从磁盘 `Load` 回来,之后都在内存里——两者成本不可比,取平均得到的数字既不描述常见情况,也不描述最坏情况。这正是阶段⑤ 那句"含磁盘读"的落点;尾部分位(p95/p99)同理,一个检索服务是被它最慢的查询定义的。

三个用例的规模都可由环境变量放大,默认值小到能进 CI;放大时记得同时放大 `STRATUM_STRESS_TIMEOUT`。

## 项目结构

```
api/proto/           # Protobuf 定义(.proto)→ 生成代码在 api/proto/stratum/
internal/            # Go 核心
  docstore/          #   MVCC 文档存储(PebbleDB)
  chunkdoc/          #   chunk ↔ 文档双向映射
  bloom/             #   chunk 存在性 + 每版本文档布隆过滤器
  index/             #   IndexManager(LRU + 引用计数 + 异步构建)
  kvraft/            #   Raft 共识(选主 / 日志复制 / 快照)
  raft/              #   Stratum Raft 状态机(KB + 版本元数据)+ 节点间转发
  plane/             #   控制面 ↔ 存储面契约 + 同进程实现(写门 / 回收水位 / 追链)
  wire/              #   gRPC proto ↔ 领域类型映射
  wal/               #   崩溃一致性写前日志(含按水位重写日志)
  sync/              #   Leader→Follower 数据同步(DataSync)
  coordinator/       #   Write / Delete / DeleteVersion / chunk-GC 编排
  router/            #   写 → leader、读负载均衡的前端
  splitter/ embed/ chunkstore/ kvstorage/ pebbleutil/ types/ errors/
service/             # gRPC 服务实现(含测试)
cmd/stratum/         # 节点入口(-config YAML / flags)
cmd/stratum-router/  # 服务站:单地址接入 Raft 集群,集群唯一对外入口
cmd/stratum-gateway/ # HTTP/JSON 网关 + /ops 控制台控制面
vecstore/            # C++ 向量存储:Faiss HNSW + RocksDB,支持量化两段式检索
web/                 # Web 控制台前端(HTML/CSS/JS)
integration/         # mock 集成 + 真实栈 e2e + docker/(T4 集群测试)
scripts/             # docker-cluster.sh / gateway.sh / router.sh 等
configs/             # 示例配置文件
```

## 配套文档

与代码同目录维护一套随实现演进的中文设计文档——**不纳入版本控制**(`.gitignore` 忽略根目录除本 README 外的全部 Markdown),仅供本地阅读:

- `Stratum_设计文档v13.md` —— 最新版设计总纲(控制面 / 存储面契约 §7.0、节点间转发 §7.3、数据缺失与幂等重发 §7.12、存储层状态上报 §7.13、索引分发 §8.4、FAILED_PERMANENT §10.1、存储集群独立进程 §11 阶段 ④;含量化两段式 §2.5/§2.6、附录 D 实测方法)
- `Stratum_接口设计v9.md` —— gRPC/内部接口与语义
- `Stratum_设计目标.md` —— 功能目标、性能指标与验收标准
- `Stratum_测试顺序.md` / `Stratum_实现顺序.md` / `Stratum_代码风格.md`
- `改动内容.md` —— 开发日志

## 前置依赖

- **Go 1.24**
- **C++17**(vecstore):Faiss ≥ 1.9.0、RocksDB、gRPC、Protobuf、BLAS/LAPACK、OpenMP
- C++ 构建仅在需要 Faiss HNSW 后端时必需;Go 单测使用进程内 mock。
