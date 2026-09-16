# 主动落后检测与激活 —— 设计稿

> 状态：**已实现**（2026-09，核心链路；集成与压测标定见下）
> 范围：`api/proto/sync.proto`、`internal/sync`、`internal/plane`、`cmd/stratum`、`configs/config1.yaml`
> 关联：`Stratum_设计文档v13.md` §7.5（游标与追链）、§7.8（恢复时的连续游标）、§7.9（epoch payload）、§8.4（建一次、分发 N 份）、§8.6(b)（惰性构建）；`docs/index-distribution-backpressure-plan.md` §7

**实现进展**：

| 组件 | 位置 | 状态 |
|---|---|---|
| 响应字段 `chain_tails` | `api/proto/sync.proto` 的 `ReportDataVersionsResponse` | ✅ |
| leader 侧回填 | `internal/sync/push.go`（`WithChainTails`）+ `LocalControlPlane.ChainTail` | ✅ |
| 上报侧投递 | `internal/sync/data_version_reporter.go`（只在 `accepted` 时交给 sink） | ✅ |
| 节点侧判定与调度 | `internal/plane/lag_catchup.go`（判据 / 每 KB 幂等 / jitter / 并发上限） | ✅ |
| 配置 | `lag_catchup.*`（`configs/config1.yaml`），**默认关闭** | ✅ |
| 单测 | `internal/sync` 3 例（含真 gRPC 往返）、`internal/plane` 7 例 | ✅ |
| 集成（§8 的双节点自愈） | `integration/docker/lag_catchup_test.go` 的 `TestT4_ActiveLagCatchupCatchesUpWithoutAQuery`；集群开关由 `scripts/docker-cluster-both.sh` 的 `LAG_CATCHUP_ENABLED` / `LOG_LEVEL` 提供 | ⚠️ **用例在，但跳过**：真集群上跑不通，卡在两个**前置**（见下），不是卡在追赶本身 |
| 压测标定（§9 的 `jitter_ms` / `max_concurrent_kbs`） | —— | ❌ 未做 |

### 集成验证暴露的两个前置（都在这条链路之外）

1. **`RecoverLocalCursors` 会把落后节点看成不落后 —— 根因已定位。** 实测：一个离线了 55 个版本的存储节点，回来时上报的游标是 **405 / 405**（＝链尾）。
   - **直接证据**：`holdsVersionLocally` 的"停在哪"日志（`stopped at a version this node does not hold`）**一次都没有**——也就是每个 KB 的**所有**版本都被判成"本节点持有"，游标自然推到该 KB 的最大版本。
   - **命中哪条判据**：不是"本地产物"（离线的节点没有），而是 `internal/plane/local_data_plane.go:1805` 那条 —— `v.DocIDSetHash == "" && IndexStatus == READY` 就当作"持有"。它不依赖任何本节点的事实，只依赖**复制来的元数据**。
   - **它为什么会被普遍命中**：这套集群里大量版本的 `DocIDSetHash` 是空的（digest 从未提交）。digest 由 `reportAndSchedule`（`local_data_plane.go:1380`）在写事务后提交，而那里的注释写明它是**quorum 主张**——"only the caller that got enough replica acknowledgements may make it"。3 个存储节点、`serving_replica_min=2`、还杀掉一个的场景下，quorum 常常不足，于是 digest 缺失。
   - **注释里的假设不成立**：那条判据的注释说这种读法"fails in the direction of a too-high cursor **by one version**"。实际不是——判据是**逐版本**的，每个没有 digest 的 READY 版本都会"持有"，于是游标**一路推到链尾**，而不是高出 1。
   - **两个修法方向**：① 收窄这条判据——不要用"digest 为空"去推断"这是个空版本"，而应让控制层**显式**回答"这个版本有没有文档集"（§7.9 把数据侧与索引侧拆开就是这个路子）；② 先查 digest 为什么缺失——若正常 quorum 下它本该提交，那真正要修的是那条路径，而不是这条判据。
   - **影响面比追赶大**：服务站的新鲜度检查（§9.3(2)）正是按这个游标判断副本够不够新。游标被虚高，意味着服务站会**误信**一个其实数据不全的副本——这比"落后检测不触发"严重。
2. **链尾没有到达 reporter。** 实测：leader 上 `ChainTail` 被调用 **0 次**，storage 侧的 sink 收到 **0 次信号**（`lag catch-up: signal carried no chain tails` 一次都没打）。组件本身有单测覆盖（含真 gRPC 往返），所以剩下的问题是**真集群里的接线**——单元测试把它 stub 掉了，看不出。

两条都记在这里而不是留在对话里：谁接手任何一条，用例与集群开关都已经是现成的。

---

## 1. 问题：恢复是惰性的，成本被推给第一个撞上的请求

索引恢复目前是**懒**的：数据到得晚时本节点按需构建（`EnsureIndex`，`internal/plane/local_data_plane.go:378`），reconcile 只给两个版本"起步优势"（`PENDING` 与 active，见 `ReconcileIndexes` 的决策表）。这个取向是对的——它避免了重启后的重建风暴（实测：41 个历史产物抢在新写入之前重建，新写入 601 秒拿不到 READY）。

但它留下两个代价：

1. **可能长期落后**：读路由若一直没有把某个 KB 的查询发给这个节点，`EnsureIndex` 就永远不被触发，它可以一直停在后面。落后本身不报错，只在 §9.3(2) 的新鲜度检查把它读成"不够新"时才显形。
2. **第一个撞上的查询付冷启动**：路由一旦选中它，那次查询要等一次数据拉取加一次索引构建——而这两件事本可以在它空闲时做完。

两者都是"把恢复的成本推给某个倒霉的请求"。本设计把它改成**后台主动追赶**。

## 2. 决策：谁来触发

| | A. 节点自发现（pull） | B. leader 推送激活（push） |
|---|---|---|
| 触发点 | 每个节点自己的上报周期 | leader，一对多 |
| 通道 | 复用 `ReportDataVersions` 的**响应**（已在跑） | 需要新的下行通道 |
| leader 需要知道 | 不需要——"我落后了吗"用节点自己的事实就能答 | 需要（谁落后、按什么节奏放行） |
| 同步性 | 上报周期若对齐，则各节点同时发现 | 广播天然同步 |
| 限速落点 | 节点侧自限速 + jitter | leader 侧"待恢复队列" |

**结论：选 A（节点自发现）。** 理由：

- 复用一条已经在跑的通道：每个存储节点周期性上报游标（`internal/sync/data_version_reporter.go:117` 的 `Run` → `:136` 的 `ReportOnce`），而**响应里捎带 leader 的全局视图**这个手法已经有先例——`reclaimable` 水位就是这么回传的（`api/proto/sync.proto:252`，消费点在 `data_version_reporter.go:174`）。
- "我落后了吗"用节点自己的事实就能回答：本地连续游标（§7.8 的 `localVersion`，也就是上报里 `data_versions` 的那份数据）与链尾一比即可。不需要 leader 维护逐节点状态，符合 §7.8/§7.9 "本地事实优先"的取向。
- B 的收益是全局节奏控制；在 A 上，节点侧自限速加 jitter 能拿到大部分（§5）。

**A 的代价要认下来**：leader 只捎带一个事实，**限速无从施力**。所以防护必须全部落在节点侧，这也是 §5 存在的原因。

## 3. 字段设计

`api/proto/sync.proto` 的 `ReportDataVersionsResponse` 增加一个字段：

```proto
message ReportDataVersionsResponse {
  bool accepted = 1;
  int64 node_id = 2;
  map<string, int64> reclaimable = 3;   // 已有：可回收的 changes 水位（§7.5）

  // chain_tails carries, per knowledge base, the version at the TAIL of the
  // replicated chain: the newest version the control layer has accepted. A node
  // whose own contiguous cursor is behind it has fallen behind and MAY start
  // catching up — whether, when and how fast are the node's own decisions (§5),
  // which is what keeps this from becoming a broadcast storm.
  //
  // Deliberately the chain tail, NOT ActiveVersionID: a node is not "behind"
  // merely because it has not switched to the newest version. Being on an older
  // version is a routing choice; missing the chain is a missing data fact. (The
  // same distinction §7.9 draws between the data cursor and the index-ready set.)
  //
  // A knowledge base absent from this map means "unknown" — no signal — not
  // "nothing to catch up".
  map<string, int64> chain_tails = 4;
}
```

**为什么用链尾而不是 `ActiveVersionID`**：落后是"数据链没跟上"，而 active 是"读路由选谁"，两者独立（§7.9 特意把数据游标与索引就绪集合拆开就是这个道理）。若用 active 版本当信号，回滚或快速切换会让"落后"的判定随路由策略抖动。

**写入侧（leader）**：`PushHandler.ReportDataVersions`（`internal/sync/push.go:471`）已经握有控制层状态，填这个 map 是顺手的（`reclaimable` 已是同样的写法）。

**读取侧（上报节点）**：`ReportOnce` 在 `resp.GetReclaimable()` 旁边把 `resp.GetChainTails()` 交给一个新的 sink（与 `LeaderWatermarkSink` 同形，例如 `LagSignalSink`），由 plane 决定是否、何时开始追赶。**只在 `resp.GetAccepted()` 为真时消费**——与水位同理，过期或被拒的响应不得触发动作。

## 4. 节点侧：发现自己落后之后做什么

**判据**：`localVersion[kb]`（§7.8 的内存连续游标）< `chain_tails[kb]`，且滞后达到 `min_lag_versions`（默认 1；设成 2 以上可以把"刚好差一个版本"这种正常写入竞态排除在外）。

**动作**（顺序固定，索引在数据之后）：

1. 数据侧：走 §7.5 的既有路径补齐——`FetchVersionData`（`internal/plane/local_data_plane.go:521`）按版本拉到 `chain_tails[kb]`，重放由 `maxConcurrentDocumentWrites`（`internal/coordinator/write_impl.go:511`）限流；
2. 索引侧：数据就位后再 `EnsureIndex`（`:378`）；若产物能由 §8.4 分发而来（构建者仍持有），副本 `InstallIndex` 直接装载，沿用本轮的 `indexPushSem` 闸门；
3. 沿途的构建走 `TriggerBuildBackfill`（`internal/index/impl.go`）——**预热性质，绝不与实时查询抢队**。

**幂等**：每个 KB 同时只允许一个追赶在跑（一个 in-flight 标记 + 版本目标）。上报表是每个周期重发的完整声明，所以"重复发现"是常态；追赶中的 KB 收到新链尾时只更新目标，不重开一趟。

**不要**在追赶路径里调用 `EnsureIndex` 之外的构建入口：`EnsureIndex` 自带"数据缺失先拉"的语义，重复实现会分叉。

## 5. 防护：为什么不能只靠 jitter

触发时机被同步是 thundering herd 的根因，不是"需要恢复"本身。三种防护叠加：

1. **jitter（必须）**：收到链尾信号后本地先睡一个 `0..jitter_ms` 的随机区间再开始。最标准也最便宜；代价是追赶完成时间的确定性变差——而它本来就是后台预热，没有时间承诺。
2. **节点侧自限速（必须）**：同时最多 `max_concurrent_kbs` 个 KB 在追赶。没有这一条，一个刚从长时间离线回归的节点会一次性把所有 KB 都点起来。
3. **优先级兜底（已有）**：追赶排 `BuildPriorityBackfill`（`internal/index/build_queue.go:21` 起），实时查询触发的构建永远排在它前面。

**单独 jitter 不够**：当落后的 KB 数量远超预期（整节点长时间离线后回归），抖动窗口未必够宽；自限速才是真正的上界，jitter 只是把峰值摊平。

**注意执行侧闸门已经就位**：数据段是 `maxConcurrentDocumentWrites`，索引构建段是 `buildPool`，分发段是本轮的 `indexPushSem`（`DefaultMaxConcurrentIndexPush`）。本设计**只负责"何时开始"**，不重复造限流器。

## 6. 与既有机制的分工

| 机制 | 回答的问题 | 本设计的关系 |
|---|---|---|
| §7.9 `ReportEpoch` | 节点 → 控制层："我有什么" | 方向相反，互补；两者都不新增 RPC |
| §7.5 追链 / `FetchVersionData` | "怎么补" | 被本设计复用，不改 |
| §8.4 分发 | "产物怎么到副本" | 被复用；其闸门是本轮补的 |
| §8.6(b) 惰性构建 | "没人问就先不建" | 本设计**不推翻**它：追赶仍是后台预热，缺失产物照旧可按需重建 |
| reconcile 的 head start | "启动时先补哪些" | 本设计是它的持续版本：reconcile 一次性、启动时；本设计周期性、稳态 |

**与 reconcile 的边界要写清**：reconcile 在启动时对 `PENDING` 与 active 给 head start；本设计在稳态下对"落后于链尾"的 KB 给 head start。两者共用 `TriggerBuildBackfill`，不产生新的构建入口。

## 7. 配置

新增一段（**默认关闭**，与 `gc_enabled` 同风格：新机制先观测再开）：

```yaml
lag_catchup:
  # 主动落后追赶（§7 设计稿）。0/false = 关闭，行为与历史一致：
  # 只在查询撞上或 reconcile 时恢复。
  enabled: false
  # 收到链尾信号后的随机延迟上界，把同时发现打散成一个窗口。
  jitter_ms: 0
  # 同时最多几个知识库在追赶。<= 0 = 用默认值。
  max_concurrent_kbs: 0
  # 滞后多少版本才算落后。1 = 任何落后都算；调大可以避开正常写入的竞态。
  min_lag_versions: 1
```

## 8. 验证

- **单测（`internal/sync`）**：响应捎带 `chain_tails` 时 sink 收到；`accepted=false` 时不投递；缺席的 KB 不被当作"无需追赶"。
- **单测（`internal/plane`）**：给定（本地游标, 链尾, `min_lag_versions`）判定落后；同一 KB 不重复起两趟追赶；jitter 可注入（测试里设为 0）。
- **集成（`integration/`）**：双节点，停掉一个节点一段时间，恢复后**它自己**追上链尾——断言不依赖任何查询触发。
- **压测（本设计之外但必须做）**：多节点同时回归，观察追赶不再产生同步风暴，并据此标定 `jitter_ms` / `max_concurrent_kbs` 与 `IndexRetentionCount` 的默认值（见 §9）。

## 9. 非目标

- **leader 侧待恢复队列**：只有 A 方案被证明节奏不够可控时才引入（那时才需要 B 的下行通道）。
- **源节点侧限流**：当前恢复以 push 分发为主（源节点＝构建者自己，已被 `indexPushSem` 覆盖）。若恢复路径改成"落后节点主动 pull 产物"，源节点侧需要另设闸门——这是 `docs/index-distribution-backpressure-plan.md` §6 风险第 6 条记着的那笔账。
- **改 `ActiveVersionID` 的推进或路由策略**：本设计只处理数据/索引的"跟不上"，不碰"切到哪个版本服务"。
