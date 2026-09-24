# Stratum 脚本目录

本目录是 Stratum 的**操作入口**：构建、启动、Docker 集群编排、集成测试，以及一套面向
运维的 REST 命令行。日常使用不需要读代码；需要原理时，各脚本头部注释与本文档都写了
「为什么这么设计」。

## 目录导览

| 脚本 | 用途 | 典型用法 |
|---|---|---|
| `cluster.sh` | Docker 集群编排（单层 / 两层一个入口） | `scripts/cluster.sh --topology two-tier up` |
| `gateway.sh` | 本地入口：服务站 + 控制台（+ 可选数据库三件套） | `scripts/gateway.sh --with-db` |
| `update-all.sh` | 全量更新：Go 二进制 + 前端 + 可选 docker 集群 + 重启 | `scripts/update-all.sh --two-tier 3` |
| `t4-integration.sh` | 跑 T4 Docker 集群集成套件 | `scripts/t4-integration.sh -r 'TestT4_Await'` |
| `latency-baseline.sh` | 重复测量建基线：查询延迟跑 N 轮，报告分布与「多少轮才分得开」 | `scripts/latency-baseline.sh -n 20` |
| `ops/*.sh` | 运维 CLI（走控制台的 REST API） | `scripts/ops/kb-list.sh` |
| `ops/gen-config.py` | 生成/调整节点 YAML 配置（含两层拓扑） | `scripts/ops/gen-config.py --role storage …` |

> 架构：控制台（`stratum-gateway`）只连**服务站**（`stratum-router`），服务站负责 leader
> 发现、写转发与读均衡；控制台不需要知道集群拓扑。数据库三件套（vecstore / embed /
> stratum）由控制台的「运维」页或 `/ops/*` 接口启停。

## 一、快速开始

```bash
# 单机全套（vecstore + mock-embed + stratum + 服务站 + 控制台），Ctrl+C 全停
scripts/gateway.sh --with-db            # 浏览器打开 http://localhost:8081

# 只起服务站 + 控制台（数据库三件套自己管，或在「运维」页里启停）
scripts/gateway.sh

# 3 节点单层 Docker 集群（宿主 vecstore，节点数据 bind mount 到 /var/lib/stratum/nodeN）
scripts/cluster.sh up 3 --with-embed

# 两层 Docker 集群（控制组 3 + 存储组 3，每个容器自带 vecstore）
scripts/cluster.sh --topology two-tier build
scripts/cluster.sh --topology two-tier up
```

## 二、`cluster.sh` —— Docker 集群编排

两种拓扑由**同一个脚本**执行，用 `--topology` 区分。这也是控制台「运维」页驱动集群的
方式：`cmd/stratum-gateway` 转调的就是这个脚本，参数形状与本文一致。

### 拓扑

| | `--topology single`（默认） | `--topology two-tier` |
|---|---|---|
| 节点 | N 个同构节点，`role=all` | 控制组 N（`role=control`，零数据层）+ 存储组 M（`role=storage`，不参与选举） |
| vecstore | **宿主进程** `:710N`（每节点一个） | 每个容器**自带**（`127.0.0.1:7100`） |
| 节点数据 | bind mount 到宿主 `/var/lib/stratum/nodeN` | Docker 命名卷 `stratum-controlN-data` / `stratum-storage-containerN-data` |
| 镜像 | `stratum-node:latest`（`Dockerfile`） | `stratum-storage:latest`（`Dockerfile.storage`，含 C++ vecstore 与它的库） |
| 节点 id | `1..N` | 控制 `1..N`，存储 `11..1M`（两段不能重叠） |
| 端口 | gRPC `P+i-1`、raft `+1000`、metrics `+2000` | 只映射 gRPC：控制 `17000+i-1`、存储 `17100+i-1` |

存储 id 从 11 起是硬约定：它同时是 `storage.nodes` 的键，节点按自己的 `node_id` 在那张
表里解析自身地址（见 `scripts/cluster.sh` 的 `storage_id`）。控制台的 `/ops/docker/*`
也按这个 id 空间寻址（控制 1-N、存储 11-1M）。

### 命令

```bash
scripts/cluster.sh [--topology single|two-tier] <命令> [选项]

build                构建二进制与镜像；--only node|storage 可只建一半
init [N]             生成节点配置与网络（幂等，不启动）
up [N]               init + 启动（幂等；两层不接位置参数，节点数用选项给）
update [N]           重编 → 重建镜像 → --force 重建容器（数据卷保留）
start|stop|restart [id...]   单个/一批节点（缺省全部）
status               集群状态；--json 输出控制台读的那种机器可读形态
logs [id] [--lines N] [-f]
down                 停止并删除容器（保留数据卷）
clean                连数据卷/网络/配置一起删（不可逆）
embed start|stop|status      可选依赖 mock-embed 容器
vecstore start|stop|status   单层拓扑的宿主 vecstore（:710N；两层下会说明它不需要）
station up|down|status|logs  服务站（宿主进程）
```

常用选项：`--nodes N`、`--control-nodes N`、`--storage-nodes N`、`--base-port P`、
`--control-base-port P`、`--storage-base-port P`、`--network N`、`--image IMG`、
`--force`、`--with-embed` / `--no-embed`、`--with-station`、`--json`。

环境变量：`STRATUM_CONTROL_COUNT`、`STRATUM_STORAGE_COUNT`、`STRATUM_CONTROL_BASE_PORT`、
`STRATUM_STORAGE_BASE_PORT`、`STRATUM_STATION_ADDR`、`STRATUM_REQUIRE_AUTH`、
`STRATUM_STATION_SECRET`、`STRATUM_STATION_TOKENS`、`STRATUM_RAFT_MAX_LOG_LENGTH`、
`LAG_CATCHUP_MIN_LAG` / `LAG_CATCHUP_JITTER_MS` / `LAG_CATCHUP_MAX_KBS`、`LOG_LEVEL`。

### 两层拓扑的几个要点

- **`require_authenticated: true` 是默认**：节点只服务带服务站信任标记的调用，
  也就是说客户端只能经服务站进来（§9.3(5)）。要直连节点端口（例如自带的测试 harness），
  用 `STRATUM_REQUIRE_AUTH=false`。
- **信任标记现在是签名的**：`init`/`up` 会在 `run/station-secret` 生成（或复用）一个共享密钥，
  写进每个节点配置的 `node.station_secret`，服务站以 `-station-secret` 用同一份签名。
  节点校验的是 HMAC，所以「带一个 `1`」不再能冒充服务站（`docs/code-review-2026-09-24.md` H4）。
  直连节点端口的工具/测试要拿到同一份密钥（`STRATUM_STATION_SECRET`，或读那个文件）。
  **两半必须一起配**：只有节点有密钥、服务站没有时，节点会拒掉全部转发——这是安全失败，
  但集群会不可用，日志里是 `unauthenticated`。
- **凭据表默认生成并启用**：`run/tokens.yaml` 里有一条 `kb_ids: ["*"]` 的凭据（明文在
  `run/console-token`），服务站以 `-tokens` 加载它。所以经服务站的调用要带
  `Authorization: Bearer $(cat run/console-token)`；控制台不用带——网关启动时用
  `-station-token` 注入（H3）。不需要鉴权就删掉该文件，或指向自己的表：
  `STRATUM_STATION_TOKENS=/path/to/tokens.yaml`。
- **`up` 会一并启动 mock-embed 容器**：生成的配置把 embed 指向
  `http://stratum-embed:8080`，少了它每次写入都会以 `lookup stratum-embed` 失败——
  这个 DNS 错误只说症状不说原因，所以默认起它（`--no-embed` 用于自带 embed 服务的场景）。
- **服务站**（`station` 子命令）是宿主进程，不是容器：`down` 会显式停它，否则下次 `up`
  会留下一个服务于「已被重建过的集群」的孤儿。`--with-station` 可让 `up` 顺手拉起它。
- 存储侧生成的配置里 `gc_enabled: true` + `gc_sweep_interval_ms: 5000` +
  `append_max_dead_ratio: 0.95` 是**测试夹具**取值（见脚本内注释）：生产值需要另行决定。
- **宿主 vecstore 只被允许碰节点的数据目录**：单层拓扑下它由 `vecstore start` 以
  `--index_dir="$VECSTORE_INDEX_DIR"`（默认 `/var/lib/stratum`，即节点配置里
  `storage.data_dir` 的父目录）启动，容器的存储半边同理（`STRATUM_VECSTORE_INDEX_DIR`
  可覆盖）。vecstore 的 `Save`/`Load` 等 RPC 直接用调用方给的路径，没有这道闸门就是
  "能连端口就能读写任意文件"（`docs/code-review-2026-09-24.md` M4）。

## 三、`gateway.sh` —— 本地入口（服务站 + 控制台）

```bash
scripts/gateway.sh [up] [选项]                  # 默认命令 up
scripts/gateway.sh stop                         # 停止控制台、本脚本拉起的服务站、容器形态的控制台
scripts/gateway.sh status                       # 控制台 / 服务站 / 数据库三件套
scripts/gateway.sh logs [gateway|router|vecstore|embed|stratum] [--lines N] [-f]
scripts/gateway.sh build [--no-frontend]        # 强制重建控制台与服务站（默认也重建前端）
scripts/gateway.sh db <start|stop|restart|status>   # 经 /ops 管理数据库三件套
scripts/gateway.sh router <up|stop|status|logs>     # 只操作服务站
```

`up` 的模式：

| 选项 | 作用 |
|---|---|
| （无） | 自动判断：`run/console.yaml` 有 `docker` 段就按集群形态派生地址，否则单机 |
| `--single` | 服务站只连 `127.0.0.1:7000`（`STRATUM_GRPC_ADDR` 可覆盖） |
| `--cluster` | 服务站地址从 `run/console.yaml` 的 `docker` 段派生（控制组 17000+，两层再加存储组 17100+） |
| `--in-docker` | 控制台跑进集群容器网络（`stratum-net`）；服务站监听 `0.0.0.0:7009` |
| `--with-db` | 经 `/ops/start` 一并拉起数据库三件套；退出时经 `/ops/stop` 停掉 |
| `--detach` | 前台是默认（Ctrl+C 全停），加它则后台运行（`run/.gateway.pid`） |

环境变量：`STRATUM_HTTP_ADDR`、`STRATUM_ROUTER_ADDR`、`STRATUM_GRPC_ADDR`、
`STRATUM_STORAGE_NODES`、`STRATUM_HTTP_PORT`（`--in-docker` 的宿主端口）、
`STRATUM_IMAGE_TAG`、`STRATUM_NETWORK`、`STRATUM_VECSTORE_ADDR`（首次生成 `run/console.yaml` 用）。

几点说明：

- **`--with-db` 会补齐缺失的二进制**：`stratum`、`mock-embed`，以及 C++ 的
  `vecstore_server`（首次 cmake 构建较慢）。前端产物缺失时也会构建（`--no-frontend` 跳过）。
- **`--in-docker` 与 `--with-db` 不能同用**：数据库三件套是**宿主**进程，容器里的控制台
  管不到它们。`--in-docker` 解决的是另一件事——集群里 embed 的地址是容器名
  （`http://stratum-embed:8080`），宿主进程解析不了，只有把控制台放进同一个网络，
  `POST /api/query-text`（服务端 embed 的文本检索）才够得着它。
- **不会静默连错地址**：`run/console.yaml` 里没有 `docker` 段时按单机形态连接并说明；
  不会像以前那样在单机配置下悄悄去连 `17000-17002` 那些根本没起的端口。
- **服务站二进制过旧会自动重建**：`run/bin/stratum-router` 不认识 `-storage-nodes` 时
  直接重建——否则要么启动就报 `flag provided but not defined`，要么操作者把两层参数去掉，
  服务站退回「全部节点同址」的单层假设，读请求悄悄走错节点。
- **控制台默认只绑回环**（`STRATUM_HTTP_ADDR`，默认 `127.0.0.1:8081`）：`/ops` 能改启动参数、
  启停服务，把它挂到 `0.0.0.0` 就等于把运维接口对全网开放（`docs/code-review-2026-09-24.md` H2）。
  要远程访问就显式设置该变量，并用网络边界或反向代理的另一层鉴权护住它。容器形态（`--in-docker`）
  在容器里绑 `0.0.0.0:8081`，由 `-p` 映射决定宿主侧的暴露面。
- **凭据与信任标记由脚本生成**（`run/station-secret`、`run/tokens.yaml`、`run/console-token`）：
  服务站以 `-tokens` + `-station-secret` 启动，控制台凭据由网关以 `-station-token` 注入。
  所以经服务站的调用要带 `Authorization: Bearer $(cat run/console-token)`，直连节点端口
  （`STRATUM_REQUIRE_AUTH=false` 之外的情况）的脚本要从 `run/station-secret` 取密钥签标记。
  换自己的凭据表：`STRATUM_STATION_TOKENS=/path/to/tokens.yaml`。

## 四、`update-all.sh` —— 全量更新

```bash
scripts/update-all.sh                  # Go 二进制 + 前端,并按原参数重启 gateway/router
scripts/update-all.sh --docker 3       # 再更新 3 节点单层集群(= cluster.sh update 3)
scripts/update-all.sh --two-tier 3     # 再更新两层集群(控制 3 + 存储 3)
scripts/update-all.sh --vecstore       # 额外(重)构建 C++ vecstore_server
scripts/update-all.sh --no-frontend    # 只构建 Go 侧
scripts/update-all.sh --no-restart     # 只构建,不重启
```

重启用的是进程的**原始启动参数**（从 `/proc/<pid>/cmdline` 读），所以之前是
`scripts/gateway.sh` 还是手工 `nohup` 起的都不影响。改完 `web/src` 记得让它重建前端，
否则控制台页面还是旧产物（gateway 的 `-static` 指向 `web/dist`）。

## 五、`t4-integration.sh` —— T4 集成套件

```bash
scripts/t4-integration.sh                       # 两层拓扑，整套（自动起集群）
scripts/t4-integration.sh -t all-in-one         # 换单层拓扑
scripts/t4-integration.sh -r 'TestT4_Await'     # 只跑匹配的用例
scripts/t4-integration.sh --no-up               # 集群已经起着，别再动它
scripts/t4-integration.sh --down                # 跑完把集群停掉（默认不停）
```

已知问题（2025-09 实测，未修）：**单层**拓扑下宿主 vecstore 的 `Save` 会失败
（节点日志 `index: Save RPC: ... Unexpected error in RPC handling`），于是没有版本能变成
READY，需要 READY 的用例会红。两层拓扑不受影响，CI 也因此只用两层。

## 六、运维 CLI（`ops/`）

面向运维的命令行：日常操作走控制台的 REST API，不需要读代码。依赖 `curl`、`jq`
（`kb-version-create.sh`/`query.sh` 等用到 `jq`；`gen-config.py` 需要 PyYAML）。
所有脚本默认连 `http://127.0.0.1:8081`，可用 `--api http://主机:端口` 或
`STRATUM_HTTP_ADDR` 指向别的节点。

### 谁管谁：`gateway.sh stop` 与 `ops/stop.sh`

| 场景 | 用什么 |
|---|---|
| 控制台还能用，想干净停掉它管的东西 | `scripts/gateway.sh stop`（走 `/ops/stop`，再停控制台与服务站） |
| 控制台已挂 / 进程是别的方式起的 | `scripts/ops/stop.sh`（只按 `run/bin` 下的进程名停，含 `cluster.sh` 的宿主 vecstore） |
| 容器形态的节点 | `scripts/cluster.sh down`（`ops/stop.sh` 不碰容器） |

### 运行时操作

| 脚本 | 作用 | 典型用法 |
|---|---|---|
| `health.sh` | 健康检查（三态） | `health.sh`（退出码 0=HEALTHY）；`--quiet` 输出单个词，适合监控 |
| `status.sh` | 系统状态：卡住/永久失败/数据缺失/删除中/删除失败/WAL/回收受阻/资源 | `status.sh`、`status.sh --json` |
| `kb-list.sh` | 列出全部知识库 | `kb-list.sh` |
| `kb-get.sh` | 单个知识库的配置与激活版本 | `kb-get.sh <KB-ID>` |
| `kb-create.sh` | 创建知识库 | `kb-create.sh --name 手册 [--index-type IVF --similarity EUCLIDEAN --quantizer SQ8]` |
| `kb-delete.sh` | 删除知识库（异步，自动重试 DELETE_FAILED） | `kb-delete.sh <KB-ID> --yes` |
| `kb-versions.sh` | 版本链（索引状态 + 数据状态 + 是否删除中） | `kb-versions.sh <KB-ID>` |
| `kb-version-create.sh` | **改内容**：新增/更新/删除文档，生成新版本 | 见下 |
| `kb-await.sh` | 等版本到 READY / DURABLE（写入后确认落地） | `kb-await.sh <KB-ID> <版本> --timeout 300` |
| `kb-rollback.sh` | 切换激活版本（发布/回滚，无停机） | `kb-rollback.sh <KB-ID> <版本>` |
| `kb-version-delete.sh` | 删除版本（subtree / single / ancestors） | `kb-version-delete.sh <KB-ID> <版本> --mode single` |
| `kb-discard-version.sh` | 放弃 PENDING 版本（元数据移除，无数据可回收） | `kb-discard-version.sh <KB-ID> <版本>` |
| `kb-rebuild.sh` | 重建 FAILED 版本的索引 | `kb-rebuild.sh <KB-ID> <版本>` |
| `kb-warmup.sh` | 预热版本索引到内存（不切换激活版本） | `kb-warmup.sh <KB-ID> <版本>` |
| `query.sh` | 检索：`--text` 走服务端 embed，`--vector` 直接给向量 | `query.sh <KB-ID> --text "…" --top-k 5` |

### 改内容的推荐流程

写入是异步的：接口返回只是「版本已分配」。**确认落地**再发布：

```bash
# 1. 改内容（产生新版本；parent 默认是当前激活版本）
scripts/ops/kb-version-create.sh kb-xxx --add d1 --content "…"
# 2. 等它到 READY（失败终态会直接告诉你该怎么处置）
scripts/ops/kb-await.sh kb-xxx <版本> --timeout 300
# 3. 发布
scripts/ops/kb-rollback.sh kb-xxx <版本>
# 4. 验证
scripts/ops/query.sh kb-xxx --text "…" --top-k 5
```

`--changes` 文件里可以一次写多条变更：

```json
{"changes": [
  {"op": "ADD",    "doc_id": "d1", "content": "新文档"},
  {"op": "UPDATE", "doc_id": "d2", "content": "修改后的全文"},
  {"op": "DELETE", "doc_id": "d3"}
]}
```

**版本链语义**：不写 `--parent` 时，新版本的父版本是**当前激活版本**——它决定新版本继承
哪一份文档集。所以连续写入而不发布，每个版本都基于同一个激活版本（而不是上一个新版本）；
要串成线性链，就「写入 → await → rollback 发布 → 再写下一批」，或显式 `--parent <版本>`。

### 卡住的版本怎么处置

`status.sh` / `kb-await.sh` 会把两类终态报出来，它们都不会自愈：

| 信号 | 含义 | 处置 |
|---|---|---|
| `data_missing`（PENDING 且无副本持有数据） | 写入方在数据落盘前死了，或每次推送都失败 | 用**同一幂等键**重发（`kb-version-create.sh --client-request-id <KEY>`，键在提交时打印/返回），或 `kb-discard-version.sh` 放弃 |
| `FAILED_PERMANENT`（`side=data` / `side=index`） | 控制层耗尽重试预算，不再自动重试 | 数据侧 → 重发；索引侧 → `kb-rebuild.sh`；都不要 → `kb-discard-version.sh` |
| `INDEX_FAILED` | 索引构建失败（可重试） | `kb-rebuild.sh <KB-ID> <版本>` |

### 枚举与字段：为什么要发 proto 全名

控制台用 **protojson（且 `DiscardUnknown`）** 解析请求体，枚举字段只认 proto 全名
（`CHANGE_OP_UPDATE`、`AGGREGATION_METHOD_MAX`）或数字。写简名（`UPDATE`、`MAX`）**不会
报错，而是被静默丢弃、退回零值**——于是 `UPDATE` 变成 `ADD`、`MAX` 变成 `MEDIAN`，接口
还返回成功。所以：

- 各脚本接受简名，但一律用 `scripts/ops/lib.sh` 的 `proto_enum` 归一化后再发；写错的
  取值会**本地报错退出**，不会发出去变成另一个语义。
- `/api/query-text` 的 `version_id` 是 JSON **字符串**（protojson 把 int64 编码成字符串），
  而 `/api/query` 里是数字——两个端点的脚本分支不同，改脚本时别混。
- `kb-create.sh --index-type/--similarity/--quantizer`、`query.sh --aggregation`、
  `kb-version-create.sh` 的 `op`、`kb-version-delete.sh --mode` 都按上面的规则处理。

### 生成/调整节点配置：`gen-config.py`

```bash
# 单节点
scripts/ops/gen-config.py --node-id 1 --peers "1=localhost:8000=localhost:7000" \
  --out run/configs/node1.yaml

# 两层拓扑：控制节点（跑 Raft，零数据层）与存储节点（从控制组读元数据）
scripts/ops/gen-config.py --role control --node-id 1 \
  --peers "1=c1:8000=c1:7000,2=c2:8000=c2:7000,3=c3:8000=c3:7000" \
  --storage-nodes "11=s1:7000,12=s2:7000,13=s3:7000" --require-auth true \
  --out run/configs/control1.yaml
scripts/ops/gen-config.py --role storage --node-id 11 \
  --peers "1=c1:8000=c1:7000,2=c2:8000=c2:7000,3=c3:8000=c3:7000" \
  --storage-nodes "11=s1:7000,12=s2:7000,13=s3:7000" --require-auth true \
  --out run/configs/storage1.yaml

# 在现有配置上调：--set 任意层级（值自动识别数字/布尔/字符串）
scripts/ops/gen-config.py --base configs/config1.yaml \
  --set index_manager.gc_enabled=true --set gc.version_retention_count=10 \
  --out configs/my-node1.yaml
```

工具只列**真正会被解析的键**（写进来而不被解析的键是静默失效，比不写更糟），并做两条
硬校验：多节点时每个 peer 必须带 service 地址；`role=storage` 时 `node_id` 必须出现在
`--storage-nodes` 里（存储节点从那张表解析自身地址）。`0` / `false` 一律表示「用代码里的
默认值」，默认值与含义见 `configs/config1.yaml` 的注释。

### 给监控用

- `health.sh --quiet`：输出 `HEALTHY` / `DEGRADED` / `UNHEALTHY`，退出码 0=健康。
- `status.sh`：存在 FAILED 版本、永久失败版本、数据缺失版本或 DELETE_FAILED 知识库时退出码非 0。
- 失败时向 stderr 打印后端返回的错误 JSON（含 `grpc_code`），便于告警定位。

## 七、与 Go 侧「运维」页的关系

控制台（`stratum-gateway`）不直接调 docker CLI，而是转调 `scripts/cluster.sh`；页面上的
参数（节点数、端口、网络、镜像、embed）与本文的选项一一对应，`status --json` 的字段也
就是页面渲染用的那份。所以：**命令行怎么操作，页面就怎么操作**，两边不会各自长出一套行为。

它读取的集群级配置在 `run/console.yaml`（首次由 `scripts/gateway.sh` 生成）。旧的
`docker-cluster.sh` / `docker-cluster-both.sh` 脚本已经合并进 `cluster.sh`，老配置里的
脚本名会在加载时就地迁移到 `scripts/cluster.sh`（否则页面按钮会去执行一个已删除的文件）。

## 八、注意

- `run/` 是本地的运行时目录（二进制、日志、控制台配置、数据），不进版本库。
  **彻底清空数据**：停服后删 `run/data/stratum`（Go 侧 Pebble + WAL + Raft）与
  `run/data/vecstore_rocksdb`（C++ RocksDB + Faiss 索引）；容器形态用
  `scripts/cluster.sh --topology two-tier clean`（会删数据卷与网络，不可逆）。
- 两层拓扑下 `POST /api/query-text` 需要控制台能解析**容器名**（`stratum-embed`）：
  宿主进程用 `scripts/gateway.sh --in-docker`，或自己算好向量调 `/api/query`。
- 修改本目录脚本后，控制台正在用的那份不会自动重建：`scripts/gateway.sh build`（或
  `update-all.sh`）。
