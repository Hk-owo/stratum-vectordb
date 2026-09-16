# resolve 的数据源第四层：控制层 holders 兜底 —— 实施计划

> 状态：**方案待实施**（代码尚未落盘；撰写环境无 Go 工具链、无 protoc，本文代码未编译、未运行）
> 范围：`internal/plane`（`resolve` 组装 + 缓存）、`internal/sync`（刷新客户端）、`cmd/stratum`（组装）、测试
> 关联：`Stratum_设计文档v13.md` §7.13.4（数据版本聚合）、§8.5（数据源注册表）、§7.5（游标与追链）、§9.3(2)（新鲜度检查）；`docs/index-distribution-backpressure-plan.md`、`docs/active-lag-detection-design.md`、`docs/index-push-probe-plan.md`

> 约定：本文行号以撰写时的代码为准，落地后需重新校正。

---

## 1. 背景：一个会永久落后的组合

### 1.1 现象

一个存储副本可能同时错过两件事，然后**再也找不到这个版本的数据源**：

1. **没收到 push** → 它没有该版本的数据（可能整体不可达）；
2. **没收到 `ConfirmVersionWrite` 广播** → 它本地的 §8.5 `DataSourceRegistry` 里没有 `(kbID, versionID) → addr` 这一条。

而广播是**有界之前的 fire-and-forget**：`broadcastConfirmation`（`internal/plane/local_data_plane.go:1074-1101`）用 `takeoverTimeout*5`（默认 200 ms × 5 = **1 秒**）的窗口发一轮，失败只 `logger.Warn`，**没有重试**。

于是后续任何拉取都只能走 `resolve`（`internal/plane/local_data_plane.go:69`，组装处 `cmd/stratum/main.go:676-705`）：

```
① §8.5 表 Lookup        → miss（这正是问题的起点）
② 回退 leader           → 分层部署下指向 control leader，而它明确拒绝：
                          "sync: PullVersionData: this node exports no data"（codes.Unimplemented）
③ 游标探测（§7.6）       → 只在"有缺口"时参与：
                          backfillTo 的 `if local == 0 || local >= versionID-1 { return nil }`
                          恰好只缺目标版本时**不探测**
```

### 1.2 为什么是"永久"

- 拉不到 → `EnsureIndex` 的 30 s 循环每轮重新 `resolve`，得到**同一个错答案** → 放弃；外层的查询自愈重试也一样。
- 而服务站选副本用的是 `GetDataVersionHolders`（只挑持有者），它不在列表里 → **永远不会被路由选中** → 也就永远不会再触发 `EnsureIndex`。"越落后越没人问"的正反馈，正是 `docs/active-lag-detection-design.md` §1 记的代价之一。
- 纯 `storage` 角色没有 raft、不注册 `OnVersionCreated`（`cmd/stratum/main.go:832` 要求 `storageLocal && raftNode != nil`），所以它连"自己发现自己落后"的机会都没有。

结论：**数据没丢（别的节点持有），但这个节点对该版本永久不可用。**

## 2. 目标 / 非目标

### 目标

1. 在 §8.5 表 miss 时，给 `resolve` 增加一层**基于控制层聚合**的数据源答案，让"错过 confirm 但已过一个上报周期"的副本能自己找到源。
2. **不改 `.proto`、不重新生成 pb.go**（撰写环境无 protoc）。
3. **绝不把 RPC 放进 Raft apply 路径**——这是 §8.5 当初的硬约束（首次在 apply 路径上探测 peers，把 `integration` 从 26 s 拖到 462 s）。

### 非目标（本轮不做，记录理由）

- **改写 `ReportDataVersionsResponse` 捎带 holders / `chain_tails`**（即"B2"形态）：最干净，但需要改 `.proto` 并重新生成 `sync.pb.go`；与 `docs/active-lag-detection-design.md` §3 一起做更合适（见 §10）。
- **放宽 `backfillTo` 的选源条件**（即"A"形态）：它是实时的，但会在 `all` 角色下把 peer 轮询带进 apply 回路（每个版本的 apply 都探测一圈）——若做，必须配"表优先 + 探测回填 + 不在 apply 路径探测"三道护栏。
- **修正 confirm 广播本身**（加有界重试）：它治的是根因的另一半，改动小、可独立做（见 `docs/index-push-probe-plan.md` 的姐妹项与 §10）。
- 改变失败语义：拉不到仍然是失败/推迟，本方案只增加"能查到"的机会。

---

## 3. 现成通道盘点（这是本方案可行的关键）

**`GetDataVersionHolders` 已经存在，而且是照着这个用途写的。**（`api/proto/knowledgebase.proto:136-145`）：

```proto
// GetDataVersionHolders answers "which nodes reported holding kbID at or past
// versionID" — the §3.1 question the service station's route table asks. It
// reads the control leader's in-memory §7.13.4 aggregate, which is SOFT state:
// an empty answer means "no node I have heard from", never "no node has it".
//
// It belongs on this control-layer service rather than on DataSyncService
// because of who asks and who answers: the station already holds a control
// connection (ListKnowledgeBases is how it learns the target version), while
// the reporters are the data nodes.
rpc GetDataVersionHolders(GetDataVersionHoldersRequest) returns (GetDataVersionHoldersResponse);
```

| 要素 | 位置 | 说明 |
|---|---|---|
| 请求/响应 | `knowledgebase.proto:259-278` | `(knowledge_base_id, version_id)` → `holders[]{node_id, address}` + `known`；holders 按 node_id **升序**（"so a caller's choice is reproducible"） |
| 服务端 | `service/knowledgebase.go:72` | 转 `LocalControlPlane.DataVersionHolders` |
| 聚合实现 | `internal/plane/local_control_plane.go:433-443` | LeaderGate 检查；`ok=false` = "问不到"，**不是**"没人有" |
| 聚合本体 | `internal/plane/data_version_registry.go` | 控制 leader 的内存聚合；**soft state**：never written to Raft、never feeds a correctness decision、**按 leadership term 作用域**（新 leader 从空开始） |
| 聚合来源 | `internal/sync/data_version_reporter.go` | 存储节点周期上报游标 + 自身地址；**`DefaultDataVersionReportInterval = 5 * time.Second`** |
| 现有唯一消费者 | `internal/router/router.go:272`（§3.1 路由表） | 本方案是它的**第二个**消费者 |

两条对方案至关重要的结论：

1. **滞后只有一个上报周期（5 s）**，而不是分钟级——因为聚合每 5 s 就被刷新一次，且注释明确"a shorter interval only costs heartbeats"。
2. **语义正好**：proto 与 `DataVersionHolders` 的注释都把它定义为"a routing hint: **fetch from one of them** rather than from a peer that never reached V"。本方案就是把这句 hint 用在**拉取**路径上（此前只用在路由上）。

---

## 4. 关键约束：不能在 `resolve` 里直接发 RPC

`resolve` 的调用方包括 `FetchVersionData`，而它的调用者是 `OnVersionCreated`——**跑在 Raft apply 回调里**（`all` 角色）。`internal/plane/data_source_registry.go` 的注释把教训写得很明确：

> Why a table instead of a probe: the source lookup runs on the **Raft apply path** — replaying history re-runs every CreateVersion through it — so it may never dial anyone (the first attempt at §8.5 probed peers there and drove `integration` from 26s to 462s and a timeout).

所以第四层**不能**是一次 `GetDataVersionHolders` 调用。两种形态：

| 形态 | 做法 | 是否需要改 proto | 是否碰 apply 路径 |
|---|---|---|---|
| **B1′（本方案主推）** | 存储节点在**已有的 5 s 上报心跳**里顺带刷新一份本地 holders 缓存；`resolve` 第四层只读缓存（纯查表） | **否** | 否 |
| **B2** | 让 `ReportDataVersionsResponse` 直接捎带 holders/链尾（照 `reclaimable` 的先例） | 是（需 protoc） | 否 |

B1′ 之所以成立：刷新由**存储节点自己发起**，走的是现成的周期心跳（`DataVersionReporter.Run`），而不是被动接收捎带——所以不需要协议改动，也不需要 `resolve` 阻塞。

---

## 5. 改动方案

### Step 1 —— 本地 holders 缓存（`internal/plane`）

```go
// holdersCache remembers, per knowledge base, which nodes the control leader's
// §7.13.4 aggregate reported holding a version — and the address each of them
// reported for itself.
//
// The entries are a MIRROR of soft state: they expire on their own (the aggregate
// keeps changing as nodes report and restart), and `known=false` is never cached,
// because "I could not find out" must not be frozen into "nobody has it" — the two
// lead to opposite actions.
type holdersCache struct {
    mu      sync.RWMutex
    entries map[string]holdersEntry // key = kbID：每个知识库一条就够
    ttl     time.Duration           // 略大于上报周期：默认 15s（3 个周期）
    limit   int                     // 上限（按 KB 计），满了按最旧淘汰（同 DataSourceRegistry 的小表取向）
}

// holdersEntry is ONE successful answer for a knowledge base, plus the version it
// was fetched with.
//
// queriedVersion >= v is what makes the entry reusable for v: a node whose cursor
// reached queriedVersion holds every version below it too (the cursor is a
// contiguous prefix, and every replica holds the whole dataset), so
// P(queriedVersion) ⊆ P(v) is a valid — if narrower — source set for v. The reverse
// does NOT hold, which is why the entry never answers a query for a HIGHER version
// than the one it was fetched with.
type holdersEntry struct {
    queriedVersion int64      // 这次答案是用哪个版本问的（= P 的下界）
    addrs          []string   // 升序（服务端已按 node_id 排序），第一个即首选源
    fetchedAt      time.Time
}

// Lookup answers "which nodes can serve versionID" from the last successful answer
// for that knowledge base.
// ok=false means "no usable entry": never fetched, expired, evicted, or the entry was
// fetched with a LOWER version than the one being asked about (a narrower node set is
// not evidence about a higher version).
func (c *holdersCache) Lookup(kbID string, versionID int64) ([]string, bool)

// Store records a successful, truthy answer together with the version it was fetched
// with. An empty/unknown answer must NOT be stored: see the type comment. A new answer
// replaces the previous one for that knowledge base — it is the answer to the most
// recent question, and keeping an older, higher-versioned entry would silently shrink
// the candidate set for later, higher queries.
func (c *holdersCache) Store(kbID string, versionID int64, addrs []string)
```

要点：

- **只在"拿到了非空 holders 且 `known=true`"时 Store**；`known=false` / 空列表 / 出错都不写缓存（否则会把"不知道"固化）。
- **查询按当前需要的版本发起，缓存负责向下复用**：`P(v)` 在 v 越小时集合越大，所以**不能只查链尾/最新版本**——`P(链尾) ⊆ P(v)`，在"还没人报到达链尾、却有人到达 v"的窗口里会返回空集，把本来能拉的版本判成无源。查询用 v（集合最大、最可能非空），条目则用它回答所有 `≤ queriedVersion` 的查询。
- **TTL 略大于上报周期**（默认 15 s ≈ 3 × 5 s）：既覆盖心跳抖动，又保证不会长期用过期视图。
- **容量上限**：每个 KB 只保留最近一次成功答案（`limit` 按 KB 数计，按 `fetchedAt` 淘汰最旧的条目），避免长跑时无界增长。

### Step 2 —— 刷新路径（`internal/sync` 新增一个只读客户端）

照 `PresenceChecker` / `DataVersionReporter` 的形状写一个小客户端（dial + resolve leader + 调用 + 失败即"不可用"）：

```go
// HoldersClient asks the control leader which nodes reported holding a version
// (KnowledgeBaseService.GetDataVersionHolders, §7.13.4).
//
// ok=false carries the RPC's own "known" semantics and is NOT "nobody has it":
// the answering node may not be the leader, or may simply never have folded a
// report for this version yet.
type HoldersClient interface {
    HoldersOf(ctx context.Context, kbID string, versionID int64) (addrs []string, ok bool, err error)
}
```

刷新触发点（两种，可只做前者）：

1. **顺带刷新**：在 `DataVersionReporter` 的每轮心跳里，对"最近 miss 过的那几个 `(kbID, versionID)`"刷新一次——即缓存 miss 时把该键记进一个小的"待刷新"集合，下一个心跳周期处理。
2. **异步预取**：`resolve` 第四层 miss 时，起一个 `go` 去刷新（有界并发、带超时），本次请求仍按旧行为返回，下一次请求就命中。

无论哪种，**都不在调用者线程上发 RPC**——这是 §4 约束的落点。

### Step 3 —— 接进 `resolve`（第四层）

`internal/plane/data_source_registry.go` 的组合函数扩成三层查找：

```go
// ResolverWithRegistry answers with, in order:
//   1. the announced holder of (kbID, versionID) — a writer said so (§8.5);
//   2. the control leader's aggregate, read from the LOCAL mirror (never an RPC:
//      this runs on the Raft apply path, see §8.5's history);
//   3. the pre-§8.5 answer — the leader.
func ResolverWithRegistry(reg *DataSourceRegistry, holders *holdersCache, fallback SourceResolver) SourceResolver {
    return func(ctx context.Context, kbID string, versionID int64) (string, bool, error) {
        if addr, ok := reg.Lookup(kbID, versionID); ok {
            return addr, true, nil
        }
        if addrs, ok := holders.Lookup(kbID, versionID); ok && len(addrs) > 0 {
            return addrs[0], true, nil // ascending ⇒ reproducible; the rest are retry candidates
        }
        holders.ScheduleRefresh(kbID, versionID) // cheap, bounded, asynchronous
        return fallback(ctx, kbID, versionID)
    }
}
```

组装处（`cmd/stratum/main.go:676`）多传一个参数即可；控制节点不构造缓存（它没有拉取路径）。

### Step 4 —— 可观测

- 命中第四层时记一条 debug（`kb_id` / `version_id` / `holders` / 距上报的时间）；
- 缓存 miss 触发的刷新次数与被丢弃的过期条目计数（便于标定 TTL）；
- `known=false` 单独计数——它表示"问不到"，与"问了但没人持有"是不同的问题。

---

## 6. 涉及文件清单

| 文件 | 改动 |
|---|---|
| `internal/plane/holders_cache.go`（新） | `holdersCache`：`Lookup` / `Store` / `ScheduleRefresh` / TTL / 上限 |
| `internal/plane/data_source_registry.go` | `ResolverWithRegistry` 增加 holders 层（第三参） |
| `internal/plane/local_data_plane.go` | 配置字段 + 组装缓存；`ScheduleRefresh` 的异步执行点在 plane 侧（它已有 `resolveReplicas` / leader 解析能力） |
| `internal/sync/holders_client.go`（新） | `HoldersClient`：dial + 调用 `GetDataVersionHolders` + `known` 语义 |
| `internal/sync/data_version_reporter.go` | （可选，Step 2 形态 1）心跳里顺带刷新待刷新集合 |
| `cmd/stratum/main.go` | 组装：构造缓存与客户端，传给 `ResolverWithRegistry`；控制节点不构造 |
| 测试 | `internal/plane/holders_cache_test.go`、`internal/plane/data_source_registry_test.go` 扩充、`internal/sync/holders_client_test.go` |

---

## 7. 测试计划

| 用例 | 断言 |
|---|---|
| 表命中 | 不查 holders、不触发刷新（零额外成本） |
| 表 miss + 缓存命中 | `resolve` 返回缓存里的地址；**不调用 fallback** |
| 表 miss + 缓存 miss | 返回 fallback 的结果（行为与今天一致）；**刷新被排队**（异步，不阻塞本次调用） |
| `known=false` | **不写缓存**；行为与 miss 相同 |
| 空 holders 列表 | 同上（"没人持有"不是可缓存的事实） |
| TTL 过期 | 过期后 Lookup 返回 miss，并触发刷新 |
| **低版本查询命中高版本条目** | 先以 v=10 填充缓存，再查 v=6 → **命中且不重查**（`P(10) ⊆ P(6)`，高版本的持有者必然持有低版本） |
| **高版本查询不吃低版本条目** | 先以 v=6 填充，再查 v=10 → **不命中**、触发刷新（"到 6"的节点集合不能证明"到 10"可达） |
| 容量上限 | 超限（按 KB 计）时按最旧淘汰，且不 panic |
| 升序取首 | 多持有者时取 node_id 最小的地址（可复现） |
| 缓存不存在（控制节点 / 未组装） | 直接退化为今天的两层行为 |

---

## 8. 风险与边界

1. **滞后窗口仍在**：如果版本刚写完不到一个上报周期、或控制 leader 刚换（聚合按 term 归零），holders 里就是没有它 → 第四层无解。**本方案缩小窗口，不消除窗口**；覆盖的是"错过 confirm 但已过一个心跳周期"这一段——而这恰好是网络抖动/对端重启（秒到分钟级）最常见的形态。
2. **`known=false` 的语义必须照抄**：不缓存、不当作"没人有"。这是 proto 注释与 `DataVersionHolders` 注释反复强调的一点。
3. **不在 apply 路径发 RPC**：所有 RPC 都发生在刷新路径（后台/心跳），`resolve` 只读内存。落地时要在 `ResolverWithRegistry` 的注释里写明这条，防止后来者"顺手"把 Lookup 换成一次调用。
4. **多持有者与陈旧地址**：升序第一个未必可达 → 失败仍走既有重试；`address` 是节点自报的、注册表"vouches for none of it beyond that"（见 `data_version_registry.go` 的 `Holder` 注释），所以它只当候选、不当事实。
5. **容量与内存**：缓存必须有限（本方案给上限 + 淘汰）；否则长跑下它是又一张无界的表。
6. **与 `GetDataVersionHolders` 其他消费者的关系**：服务站路由已在用同一个 RPC；多一个消费者只会增加控制层的读负载（每次刷新一个 KB 一个版本一次调用，5 s 一次，量级可忽略，但仍应计数观测）。
7. **不能只查"最新版本 / 链尾"**：持有链尾的节点确实持有全部历史（cursor 是连续前缀，且每个副本持全量数据集），所以"链尾的持有者"对任何更低版本都是有效源——但它是 `P(v)` 的**子集**（`P(链尾) ⊆ P(v)`）：在"还没人报到达链尾、却有人到达 v"的窗口里会返回空集，把本来能拉的版本判成无源。因此查询按**目标版本**发起（集合最大、最可能非空），只让**缓存**向下复用（命中条件 `queriedVersion >= v`）；反过来，条目**永不**回答比它更高的版本。

---

## 9. 验证方式

撰写与实现环境（本机）**没有 Go 工具链，也没有 protoc**：

- 无法 `go build` / `go vet` / `go test`，本文代码**未经编译与运行**；
- 本方案**不需要 protoc**（复用已生成的 `GetDataVersionHolders`），这是刻意的设计约束。

落地后需要在有工具链的机器上执行：

```bash
go build ./...
go vet ./internal/plane/ ./internal/sync/ ./cmd/stratum/
go test ./internal/plane/ -run 'Holders|ResolverWithRegistry' -race
go test ./internal/sync/ -run HoldersClient
go test ./internal/... ./service/... ./cmd/stratum/...
```

手工确认点：

- 人为掐断一个存储节点到协调者的网络（`ConfirmVersionWrite` 收不到），恢复后它的日志里应出现第四层命中，并随后成功拉起数据；
- 控制节点上不应构造 holders 缓存（它没有拉取路径）。

---

## 10. 后续（本轮之外）

1. **B2 形态（响应捎带）**：给 `ReportDataVersionsResponse` 加 holders / `chain_tails`，与 `docs/active-lag-detection-design.md` §3 一起落地（同一手法：`reclaimable` 就是先例，注释写着 "Piggy-backing on the report every node already sends avoids inventing an RPC for it"）。这能省掉本方案的刷新调用。
2. **confirm 广播的有界重试**：治根因的另一半——让 §8.5 表本身不再永久缺失（`broadcastConfirmation` 现在只有 1 秒窗口、不重试）。与 `docs/index-push-probe-plan.md` 属于同一轮基建。
3. **A 形态（放宽 `backfillTo` 选源）**：实时探测，若能配上"表优先 + 探测回填 + 不在 apply 路径探测"三道护栏，可与本方案互补（本方案覆盖 5 s 之后的窗口，A 覆盖 5 s 之内）。
4. **让服务站把"落后但健康"的节点纳入候选**：现在只挑持有者，落后节点永远不被选中、也就永远没有机会自愈——这正是主动追赶设计稿要解的第二个代价。
5. **把 holders 缓存接到 `pickBackfillSource`（本方案之后收益最大的一处）**：那是现在唯一还会"遍历 peers"的地方——串行地问 storage group 里每个 peer 的 `CursorQuerier.LocalVersionOf`（每 peer 带 `peerCursorTimeout`），只为挑一个能覆盖缺口的源。而 `P(need)` 缓存**就是那个答案**：命中时直接从 `addrs ∩ storageGroup` 里挑（升序第一个），整轮游标 RPC 归零——这同时把"缺口场景的探测风暴"风险一并消掉（见 §8 第 7 条与 `docs/index-push-probe-plan.md` 的讨论）。配套两点：**重试时轮换候选**（条目保存整个 `addrs` 列表，`EnsureIndex` 的 30 s 循环让第 n 次尝试用 `addrs[n mod len]`，避免反复撞同一个刚重启的节点）；以及**探测结果回填**（任何一次成功的实时探测都写回 §8.5 表 / holders 缓存，让一次探测被后续版本与后续节点复用）。
6. **可选的收敛：把"选源"抽成一个组件**。现在它散在三处——`resolve`（`SourceResolver`）、`pickBackfillSource`（§7.6）、`transferFullState`（§6.4 传入的 source）。统一成 `SourcePicker(kbID, versionID, need)`，内部优先级固定为「§8.5 表 → holders 缓存 → 实时探测（有闸门 + 回填） → leader 兜底」，三处共用同一份缓存、同一道闸门、同一套观测——"什么时候允许探测"这条规则也就只有一个落点。
7. **两条红线（不要顺手优化掉）**：① `SafeDurableVersion`（§7.8）**必须实时**逐个问 peers 的连续游标并取 quorum 最小值——它喂的是**正确性**的界（"我可以声称 durable 到哪"），而 holders 是控制 leader 的软状态、按 term、有滞后，用它替代直接违反它自己写下的硬边界（*never feeds a correctness decision*）；② **leader 兜底要保留**——它在分层部署下虽然无效，却是让拉取循环与 `backfillTo` 得以启动的入口（`ok=false` 会让 `EnsureIndex` 静默 `return nil`，比一个会失败的地址更糟）。
