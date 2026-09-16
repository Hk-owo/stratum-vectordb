# 索引分发背压（§8.4 补闸门）——实施计划

> 状态：**已实现**（Step 1–4 已落地；验证结果见 §5）
> 范围：`internal/plane`、`internal/index`、`cmd/stratum`、`configs/config1.yaml`
> 关联：README「三条路径两个有闸门、一个没有」、Stratum_设计文档v13.md §8.4

---

## 1. 背景

存储层有三条会**成规模消耗资源**的路径，其中两条已有闸门，一条没有：

| 路径 | 瓶颈 | 现有闸门 |
|---|---|---|
| 增量恢复（重放，重新 embed） | CPU / 网络 | `maxConcurrentDocumentWrites = 8`（`internal/coordinator/write_impl.go`） |
| 索引本地重建 | CPU / 磁盘 | `buildPool`：有界并发 + 两级优先级（`internal/index/build_queue.go`） |
| **索引分发（§8.4）** | **网络 / IO / 内存** | **无** |

第三条就是本计划要补的闸门。

它为什么现在就要补（而不是等某个新功能）：

- 缺限速是**独立于任何新功能的既有隐患**。哪怕不做"主动推送激活"，正常的版本切换、写入侧批量推进版本时，多个版本几乎同时构建完成就会同时进入分发（重启 reconcile 为各 KB 补建 active 版本也会叠加），而当前分发是**逐副本 best-effort、无限速**。
- `PushIndexToReplicas` 每次调用都**把整个索引文件读进内存**（`ReadIndexFiles` → `os.ReadFile`），再推给 N 个副本——所以"无限速"同时意味着**内存峰值不受控**。
- 先补裂缝、再叠新功能，比反过来稳。

> 注：`build_queue.go` 的 "hundreds of builds" 与 `IndexStore.TriggerBuildBackfill` 的 "a sweep over historical versions" 曾把**已被撤销**的行为写成现状——这两句已在本轮一并校正（见 §7「顺带清理」）。reconcile 现在只补 `PENDING` 与 active。

### 一个必须同时修的连带问题

改动前，分发是**同步跑在 build worker 里**的（Step 1 已把它异步化）：

```
buildPool worker                              internal/index/impl.go:1075, 1082
  └─ doBuild(...)
       └─ invokeCallback(cb, ...)             internal/index/impl.go:1150   ← 同步
            └─ cb = RegisterBuildCallback 的闭包    cmd/stratum/main.go:298
                 └─ distributeIndex(...)      cmd/stratum/main.go:775-790
                      └─ PushIndexToReplicas(...)              ← 改动前：同步、逐副本、读大文件
```

**因此"只加限流"是错的第一步**：拿不到信号量的分发会占住 build worker 在那里排队，极端情况下所有 worker 卡在分发上，构建池（写给查询/写入用的紧急路径）彻底停滞——等于用一个更坏的问题替换原来的问题。

正确的顺序是：**先异步化（还原 `distributeIndex` 本就该有的 fire-and-forget 语义），再加限流。**

（Step 1 落地时在新 goroutine 里加了 `defer/recover`：离开 build worker 之后，`doBuild` 那层的 recover 不再覆盖分发路径，一次 panic 会终结进程——见 §6 风险第 9 条。）

### 恢复路径是两段：闸门也各管一段

"落后"的恢复不是一步，闸门也不止一道。分清这一点，才知道将来的"主动激活"（§7）该防哪一段、本轮这道闸门又盖住了多少：

| 段 | 路径 | 瓶颈 | 现有闸门 |
|---|---|---|---|
| 数据段 | §7.5 backfill 拉数据 → 重放、重新 embed | CPU / 网络 | `maxConcurrentDocumentWrites = 8` |
| 索引段 · 前半 | 本地重建 | CPU / 磁盘 | `buildPool`：有界并发 + 两级优先级 |
| 索引段 · 后半 | 自建完成后由构建者 push 产物给副本 | 网络 / IO / 内存 | **无 → 本计划补** |

所以本轮补的闸门只覆盖**索引段的后半**。将来"主动激活"触发的风暴：前段（回放 / 重建）由既有调度器挡，后段（分发）由本轮闸门挡；而"群体动作的瓶颈可能是网络与**源节点**磁盘 IO"这一层，本轮的闸门（推送方**自己**的信号量）**管不到**——那要等恢复路径 pull 化之后在源节点侧另设，见 §6 风险第 6 条。

### 追赶的有效窗口 = retention 窗口

分发送的是**产物**，而产物受磁盘保留策略支配，所以 §8.4 能覆盖的追赶深度有一个隐含上限：**只有落在 retention 窗口内的版本才可能被分发**。

窗口由三样东西决定（`internal/index/impl.go`）：

| 依据 | 来源 | 说明 |
|---|---|---|
| 最新 `IndexRetentionCount` 个 | `gc.version_retention_count`（默认 50） | 按 `versionID` 保留最大的 N 个 |
| `RetentionProtectWindow` 内被访问过的版本 | `<versionID>.index.used` sidecar | 默认 24h（`DefaultRetentionProtectWindow`） |
| 名额上限 `RetentionProtectMax` | 默认 = `IndexRetentionCount` | 最近访问保护最多占这么多，避免磁盘无界增长 |

而"这个版本在这里被需要"的登记，只有一部分入口会落到磁盘，且**磁盘盾与内存基线不同步**：

| 位置 | 内存基线 | 磁盘盾（`.used`） |
|---|---|---|
| `Search` 查询 | ✅ | ✅ `recordSearch` |
| `RebuildIndex` / `WarmupVersion` | ✅ | ✅ `RecordInterest` |
| **build 完成**（`impl.go` 的 `seedAccessLocked`） | ✅ | ❌ |
| **`InstallIndex` 装入分发产物**（`install.go`） | ✅ | ❌ |
| reconcile 的 `TriggerBuildBackfill` 补建 | 走 build 完成那条 | ❌ |

两个后果：

1. **窗口内的版本**：产物靠"最新 N"活着，分发可行；但在分发的**排队期间**仍可能被下一次 retention 收走——因为 build 完成没给产物留下磁盘盾（见 §6 风险第 7 条）。
2. **窗口外的版本**：装了又删。`InstallIndex` 把产物写盘后，副本侧的 retention 不认识它（无 `.used`、不在最新 N、非 active），下一个 build 或重启时的 `EnforceRetention` 就会删掉它——带宽白花，副本退回按需重建。

所以 `IndexRetentionCount` 不只是磁盘配额，它**同时是"落后到多少以内，追赶还能靠分发修复"的上限**；超出部分退回"按需重建"——`ReconcileIndexes` 的 `retentionCutoff` 分支就是这条的显式实现：窗口外且产物缺失的版本**故意不补建**，只记 Info、留待按需重建。

> 另有一处口径不一致：`retentionCutoff` 只按 `versionID` 排序算，不看 active 与 `.used` 盾；而 `EnforceDiskRetention` 是**剔除**盾之后才取最新 N。两者对"窗口内"的定义不同，见 §6 风险第 8 条。

---

## 2. 目标 / 非目标

### 目标
1. 让索引分发**不再占住 build worker**。
2. 给索引分发一个**有界并发上限**，同时约束网络、源节点 IO 与内存峰值。
3. 上限可通过配置调整，默认值合理，行为对既有部署向后兼容。

### 非目标（本轮不做，记录理由）
- **leader 侧节流广播**（分批发送"激活"信号）：属于"主动推送激活"这个**尚未存在**的新功能，本轮不实现。
- **分发路径的二级优先级**：目前分发的所有调用都是**后台补齐**性质，没有"用户在等"的实时路径，统一进一个有界池即可；等将来出现"副本被选中要立刻服务查询但版本落后"这种急切分发时再引入优先级。
- 不改 `PushIndexToReplicas` 的**分发逻辑本身**（逐副本、best-effort、失败回退自建）。

---

## 3. 改动方案

### Step 1 —— 异步化 `distributeIndex`

**文件**：`cmd/stratum/main.go`（`assign` 于数据平面就绪处，现在是约 775-791）

改动前（同步）：

```go
distributeIndex = func(kbID string, versionID int64) {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    if err := dataPlane.PushIndexToReplicas(ctx, kbID, versionID); err != nil {
        logger.Warn("index distribution failed", ...)
    }
}
```

改为脱离调用方后台执行（已落地；实际代码里还多了一层 `defer/recover`）：

```go
distributeIndex = func(kbID string, versionID int64) {
    // 分发是 §8.4 的优化（副本 load 现成产物而非自建），不是 build 的一部分：
    // 它跑在 build 回调里，而回调又跑在 buildPool 的 worker 上——同步分发会占住
    // 一个 worker 整个推送期间（还含一次整文件 ReadIndexFiles）。这里的 2 分钟
    // 超时 + context.Background() 本就表明它是 fire-and-forget，此改动只是把它
    // 落成它一直声明的样子。
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
        defer cancel()
        if err := dataPlane.PushIndexToReplicas(ctx, kbID, versionID); err != nil {
            logger.Warn("index distribution failed",
                zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
        }
    }()
}
```

**已核对的安全性**：

- `distributeIndex` 无返回值，`cb` 返回的是 `reportIndexStatus` 的错误，所以异步化**不影响** `invokeCallback` 的重试语义（重试针对状态上报，不是分发）。
- 顺序：`EnforceDiskRetention` 先于分发执行，且它把 `versionID` 作为 shield 传入（`[]int64{kb.ActiveVersionID, versionID}`），**刚 build 的产物不会被同一次 retention 删掉**——异步分发读它时是安全的。

### Step 2 —— `PushIndexToReplicas` 入口加有界信号量

**文件**：`internal/plane/local_data_plane.go`

照抄 `maxConcurrentDocumentWrites` 的信号量模式，但注意本次的语义不同（见下）。

在 `LocalDataPlane` 结构体新增字段：

```go
// indexPushSem bounds how many PushIndexToReplicas runs may be in flight at once.
// 一次调用 = 读一整份索引文件进内存 + 推给 N 个副本，所以它同时是
// 网络/带宽与内存峰值的上限（不是"同时推给多少副本"，那种更细的粒度在本轮不做）。
indexPushSem chan struct{}
```

在 `NewLocalDataPlane` 中初始化（`<=0` 取默认）：

```go
pushLimit := cfg.MaxConcurrentIndexPush
if pushLimit <= 0 {
    pushLimit = DefaultMaxConcurrentIndexPush
}
// 沿用 MaxInFlightWrites 的约定：<=0（含未配置）走默认，不额外暴露一个字段给调用方。
d.indexPushSem = make(chan struct{}, pushLimit)
```

在 `PushIndexToReplicas` 入口 acquire、出口 release，且**尊重 ctx**（否则后台 goroutine 会无限排队）：

```go
func (d *LocalDataPlane) PushIndexToReplicas(ctx context.Context, kbID string, versionID int64) error {
    if d.indexReader == nil || d.indexShipper == nil || d.resolveReplicas == nil {
        return nil
    }
    // §8.4 背压：分发风暴（重启后 reconcile、批量版本切换）会让多个节点同时拉同一份
    // 大产物。有界并发把"同时炸开"降级为"排队推进"。ctx 感知是必须的——调用方是
    // 后台 goroutine，不感知 ctx 会在这里无限等待。
    select {
    case d.indexPushSem <- struct{}{}:
        defer func() { <-d.indexPushSem }()
    case <-ctx.Done():
        return ctx.Err()
    }
    ... // 其余不变
}
```

新增默认值常量——**导出**，与同包的 `DefaultMaxInFlightWrites`（`write_gate.go`）对齐（`build_queue.go` 的 `defaultBuildConcurrency` 属于另一个包，不是参照物）：

```go
// DefaultMaxConcurrentIndexPush 是一次"建一次、分发 N 份"的并发分发数上限。
// 取值偏保守：一次分发 = 一次整文件读入内存 + N 份网络推流，约束的是内存与源节点
// 出带宽；具体值属 §10.4 的 placeholder 约定，待实测再调。
const DefaultMaxConcurrentIndexPush = 4
```

**为什么这里可以用 buffered channel，而 `writeLimiter` 刻意不用**：`write_gate.go:20` 写着 "a buffered channel would freeze the limit at creation time"——那条闸门要支持 per-KB **动态**调限，所以不能用 buffered channel。分发上限是**全局、创建期固定**的（一个 `index_manager.push_concurrency` 值），没有动态调整需求，`chan struct{}` 是合适的。这个取舍要写进注释，否则读者会拿 `writeLimiter` 那段话来质疑。

**为什么语义是"并发分发调用数"而不是"并发副本推送数"**：后者要改 `for peer` 循环的内部结构，收益有限（一次分发的 N 通常等于副本数，很小）。先做粗粒度即可，且粗粒度天然限制内存峰值。

### Step 3 —— 配置接线

**文件**：`configs/config1.yaml`、`cmd/stratum/main.go`

放在 `index_manager` 段（与 `build_concurrency` 并列，因为同为索引相关资源的闸门），命名 `push_concurrency`：

```yaml
  # §8.4 索引分发的并发上限：同时最多几个"建一次、分发 N 份"在跑。
  # 它约束的是分发风暴（重启 reconcile / 批量版本切换）对**网络与内存**的冲击——
  # 一次分发要把整份索引文件读进内存再推给 N 个副本。有界之外，分发本身已从
  # build 回调里异步化，不再占 build worker。
  # 0 = 用默认值（4）。
  push_concurrency: 0
```

`main.go`：

- `appConfig` 增加字段 `IndexPushConcurrency int`（照 `IndexBuildConcurrency` 的位置；现在在 `main.go:1533`）。
- yaml 解码结构体 `IndexManager` 增加 `PushConcurrency int \`yaml:"push_concurrency"\``（现在在 `main.go:1658`）。
- 在读取处（`if fc.IndexManager.BuildConcurrency != 0` 附近，现在在 `main.go:1816`）赋值给 `cfg.IndexPushConcurrency`。
- 构造 `LocalDataPlaneConfig` 时传入：

  ```go
  MaxConcurrentIndexPush: cfg.IndexPushConcurrency,
  ```

### Step 4 —— 让分发两端的产物都拿到磁盘盾（补 `.used` 登记）

**文件**：`internal/index/impl.go`、`internal/index/install.go`

**问题**（见 §1「追赶的有效窗口」）：build 完成（`impl.go` 的 `seedAccessLocked`）与 `InstallIndex`（`install.go`）都只写**内存**访问表，不写磁盘的 `<versionID>.index.used`，于是产物只剩"最新 N 个"这一层保护：

- 构建者侧：分发的**排队期间**，产物可能被下一次 retention 收走（`PushIndexToReplicas` 读盘时 ENOENT）；
- 副本侧：`InstallIndex` 装下的**窗口外**产物在下一次 retention（或重启时的 `EnforceRetention`）被删——装了又删。

**改动**：把"这个版本在这里被需要"在**这两个入口**也落到磁盘，即在这两处调用 `persistAccessTime`（与 `recordAccess` 共用的那条路径；它自带的节流足以避免每次 build / 每次安装都写文件）。

- `impl.go`：`doBuild` 的 defer 里，`seedAccessLocked` 之后补一次——**注意锁**：`persistAccessTime` 是文件写，必须在 `im.mu.Unlock()` 之后调用，与 `recordAccess` 的写法一致。
- `install.go`：`InstallIndex` 里同样放在 `im.mu.Unlock()` 之后。

**为什么复用 `.used` 而不是"传参式 shield"**：`EnforceDiskRetention` 的 `protectedIDs` 是**每次现传**的，只在当次调用有效；而分发是**滞后**的——滞后多久由闸门深度决定。`.used` 是持久事实（重启也在），且正好复用 `RetentionProtectWindow` 与 `RetentionProtectMax` 两道既有约束，不需要新概念。

> 已核对：这条路已经在走——`RecordInterest`（`RebuildIndex` / `WarmupVersion`，`service/admin.go`）在触发构建**之前**登记，注释写明是为了"消掉构建先完成的竞态窗口"。本 Step 只是把它推广到分发两端。

---

## 4. 涉及文件清单

| 文件 | 改动 |
|---|---|
| `cmd/stratum/main.go` | ① `distributeIndex` 异步化；② `appConfig.IndexPushConcurrency`；③ yaml 结构体字段；④ 配置读取；⑤ 传给 `LocalDataPlaneConfig` |
| `internal/plane/local_data_plane.go` | ① 新增 `indexPushSem` 字段与 `DefaultMaxConcurrentIndexPush` 常量；② `NewLocalDataPlane` 初始化；③ `PushIndexToReplicas` 加 acquire/release；④ `LocalDataPlaneConfig` 新增 `MaxConcurrentIndexPush int` |
| `configs/config1.yaml` | `index_manager.push_concurrency` |
| `internal/index/impl.go` | Step 4：`doBuild` 的 defer 补 `persistAccessTime`（构建者侧的 `.used`） |
| `internal/index/install.go` | Step 4：`InstallIndex` 补 `persistAccessTime`（副本侧的 `.used`） |
| `internal/plane/index_share_test.go` | 新增并发上限的测试（见下） |
| `internal/index/index_test.go` | Step 4：断言 build 完成 / `InstallIndex` 之后 `.used` 存在 |

---

## 5. 验证

### 单元测试（`internal/plane/index_share_test.go`）

现有脚手架（`stubIndexReader` / `stubIndexShipper` / `newIndexPlane`）可直接复用。新增用例：

- **`TestLocalDataPlane_PushIndexToReplicasRespectsConcurrencyLimit`**：
  - 用一个会在内部阻塞的 `stubIndexShipper`（新增一个 `block chan struct{}` 字段，或一个可注入的 hook），记录"同时在跑的分发数"的峰值。
  - **注意**：现有 `stubIndexShipper.PushIndex` 是无锁 `append`，并发用例必须给它加 mutex，否则是 data race。
  - 上限要**注入**（`newIndexPlane` 目前不接受它——加个参数，或直接用 `LocalDataPlaneConfig{MaxConcurrentIndexPush: 2}` 构造），不要拿默认值 4 当被测上限。并发发起 `limit + k` 次，断言**峰值 ≤ limit**，且全部最终完成。
- **`TestLocalDataPlane_PushIndexToReplicasHonoursContextWhileQueued`**：
  - 先占满全部名额，再发起一次但传一个已取消的 ctx，断言立即返回 `ctx.Err()` 而不是阻塞。
- **回归**：现有 `TestLocalDataPlane_PushIndexToReplicasCoversEveryCandidate` 等应保持通过（语义未变）。

### Step 4 的测试（`internal/index/index_test.go`）

- build 成功之后，`<versionID>.index.used` 存在且时间戳落在当前；
- `InstallIndex` 之后同样存在；
- 构造"窗口外的版本 + 一次 retention"，断言产物**存活**（即装了不会被删）。

### 构建 / 全量测试

```bash
go build ./...
go vet ./internal/plane/ ./internal/index/ ./cmd/stratum/
go test ./internal/plane/... ./internal/index/... ./cmd/stratum/...
go test ./internal/plane/ -run PushIndexToReplicas -race   # 并发闸门的用例
```

> 注：本机有 Go 工具链（`go version` = go1.24.4）。**Step 1–4 落地后实测**：`go build ./...` 与 `go vet`（上述三个包）通过；`go test` 覆盖 `internal/plane`（两个新用例在 `-race` 下）、`internal/index`（含两个新用例）、`cmd/stratum`、`internal/coordinator`、`internal/sync`，全部通过。

### 手工确认点

- `PushIndexToReplicas` 的调用栈里**不再出现 build worker**（`doBuild` 立即返回，分发在独立 goroutine）。
- 配置 `push_concurrency: 1` 时，多个版本同时构建完成后，分发呈串行推进（日志时间戳可辨）。
- build 完成后 `ls <IndexDataDir>/index/<kbID>/` 能看到对应的 `.index.used`。

---

## 6. 风险与注意点

1. **异步化的错误可见性**：异步后分发的失败只进日志（现状本就如此，`PushIndexToReplicas` 对单副本失败只 warn 且总返回 `nil`）。这是可接受的——§8.4 是优化，失败的副本会**自己构建**（注释原文："that replica will build its own"），不损正确性。
2. **goroutine 泄漏**：分发 goroutine 有 2 分钟超时兜底；信号量 acquire 尊重 ctx。两者叠加可保证 goroutine 不会无限存活。
3. **上限过小会拖慢恢复**：`push_concurrency` 太低会让副本更久地停留在"自建"路径（比 load 现成产物贵）。默认 4 是起点，需实测标定（§10.4 placeholder 约定）。
4. **不要在本轮顺带改分发逻辑**（如把逐副本改并发）：那会改变 `PushIndexToReplicas` 的语义与内存特征，超出"补闸门"的范围，应单独评估。
5. **与 `buildPool` 优先级的关系**：本改动**不**引入优先级。若将来出现急切分发场景，再复用 `BuildPriorityInteractive/Backfill` 那套模式。
6. **源节点 / 接收侧仍是敞口**：本轮闸门限的是"本节点同时推几路"——推送方**自己**的全局信号量，管不到"多个节点同时打**同一个源节点**"。当前 push 模型下源节点就是构建者自己，所以这一道够用；但若将来恢复路径改成叶子节点主动 pull（见 §7），源节点的出带宽与磁盘 IO 会在它**不知情**时被多个拉取者同时打满，那时必须在源节点侧（对并发的 `PushIndexData` 服务）另设限流。这条不解决，本节闸门的效果会被下一阶段的 pull 化吃掉一半。
7. **窗口内版本的排队竞态**：`protectedIDs` 是每次现传的，所以"刚 build 的产物"只在**当次** retention 里受保护；异步 + 排队之后，读盘被推迟到"拿到信号量之后"，期间每一次 build 完成都会跑一次 retention 并换掉 shield。若排队跨过 `IndexRetentionCount`（默认 50）个更高 `versionID` 的构建，产物就被收走，`ReadIndexFiles` 报 `ENOENT`（后果只是该副本自建 + 一条 warn，不损正确性）。**Step 4 的 `.used` 登记就是这一条的修法**；不做 Step 4 的话，至少要把它当已知边界记下来。
8. **两处"窗口内"口径不一致**：`ReconcileIndexes` 的 `retentionCutoff`（`internal/plane/local_data_plane.go`）只按 `versionID` 排序取第 `len-N` 个，不看 active 与 `.used` 盾；而 `EnforceDiskRetention` 是**剔除**盾之后才取最新 N。于是"磁盘上因盾尚存的版本"与 reconcile 眼中的窗口边界可能错开——表现为 reconcile 对某些其实还活着的版本记 `skipping rebuild of retention-dropped index`，或反之。属既有行为，建议单独立项统一口径（见 §7）。
9. **异步化改了 panic 的兜底路径**：`doBuild` 的 defer 里有 `recover`（`internal/index/impl.go:1082` 起），它把 panic 记成 `FAILED` 并照常上报状态——同步分发时，`PushIndexToReplicas` 里的 panic 也被它兜住。改成独立 goroutine 之后，这个 `recover` **不再覆盖分发路径**：一次 panic 会直接终结进程。风险低（`PushIndexToReplicas` 入口有 nil 检查），但建议在新 goroutine 里加一层 `defer/recover` 记日志，或在文档里明确接受"分发路径的 panic 不再被兜住"。

---

## 7. 后续（本轮之外）

### 主动落后检测 / 激活：先定"谁来触发"

> **设计稿已出**：`docs/active-lag-detection-design.md`——模型选型（结论：节点自发现）、响应侧 `chain_tails` 字段、节点侧动作与三种防护、验证方式都在那里；下面只保留与本计划交叉的部分。

目标是不让"落后"长期只靠查询撞上才被修复——惰性恢复的问题是路由可能一直不选中落后节点，或者第一个撞上它的查询要付冷启动代价。

**第一步是定触发模型，两种模型的防护手段不同：**

| 模型 | 触发点 | 同步性 | 防护落点 |
|---|---|---|---|
| **节点自发现**（pull）：`ReportDataVersions` 的响应里捎带每 KB 的**版本链尾**（不是 `ActiveVersionID`），节点自己发现落后 | 各节点自己的上报周期 | 天然分散；上报周期若对齐则不分散 | 只能**节点侧**自限速 + jitter |
| **leader 推送激活**（push）：leader 主动把"去恢复"的信号下发给落后节点 | leader，一对多 | **触发时机被同步**——落后节点在同一心跳周期几乎同时开始动作 | **leader 侧**限速推送 + 节点侧 jitter |

前一种 leader 侧限速**无从施力**，后一种才是"leader 侧待恢复队列"的适用场景。本文档 §1 里那条"节点主动发现落后"属于前者，所以"leader 推送"这个说法要么改掉、要么补上后者的机制。**二选一未定之前，下面三条都只是候选。**

### thundering herd 的根因是"触发时机被同步"，不是"需要恢复"

所有落后节点在同一时刻收到信号、几乎同时开始动作，才把问题从"个别请求偶尔踩雷"变成"全体节点同一时刻扎堆拉取"。三种防护可叠加：

1. **触发端 jitter**：节点收到信号后本地睡一个 `0..N` 秒的随机延迟再开始。最标准也最简单，把"精确同一时刻"打散成"一个窗口内均匀分布"；代价是恢复完成时间点的确定性变差（原本惰性、本就不保证时间，通常不重要）。
2. **leader 侧限速推送**（仅 push 模型适用）：leader 维护"待恢复队列"，按固定速率（每周期只放行 K 个）逐步下发，而不是一次性覆盖全部落后节点。这与 `buildPool` 的"有界并发 worker 池"是同一类思路，只是限流点在 leader 推送侧，而不是存储节点本地。
3. **复用两级优先级兜底**：主动激活的预热恢复放 `BuildPriorityBackfill` 这一档（或更低），即使信号打散得不够好、瞬间涌入一批任务，实时查询触发的构建也**不会**被预热任务挡住。这条防护本来就该有，与触发源是被动查询还是主动推送无关。

**单独 jitter 可能不够**：落后节点数量远超预期（批量版本变更、节点离线后集中回归）时，抖动窗口未必够宽。三者叠加才能把"同步风暴"降级为"平滑的背景流量"。

### 与恢复路径的关系（见 §1 的两段辨析）

关键在于先确认恢复走哪条路，因为它决定瓶颈与限流点：

- **从已有 READY 副本复制现成产物**（当前 §8.4 的 push 模型，以及将来可能出现的 pull）：风暴是**网络 / IO** 型的——多个落后节点同时向同一个源节点（builder 节点）发起大文件搬运，抢的是带宽和**源节点磁盘 IO**。这一类比"重复重建"轻，且更适合限速：直接对"分发"这个动作加并发上限，**与它走不走 leader 触发无关**——那是分发路径本身该有的保护（本轮做的就是这一条）。
- **独立重建**（每个落后节点从头 embed → 建 HNSW）：风暴是 **CPU / 构建**型的，多个节点同时抢本机 CPU 与磁盘 IO。这一档由 `buildPool` 挡。

两种路径在本轮各自只被一道闸门覆盖；"源节点被多方同时打"这一层仍未覆盖，见 §6 风险第 6 条。

### 分发路径的细粒度限流

- **并发副本推送数**（把 `for peer` 循环也纳入闸门）与二级优先级：待出现"副本被选中要立刻服务查询但版本落后"这种急切分发需求时再评估。
- **源节点 / 接收侧背压**：见 §6 风险第 6 条，pull 化之前必须单独立项。

### retention 口径统一（✅ 已做）

- `ReconcileIndexes` 的"窗口"改为按**磁盘上实际有产物的版本集合**计算（新增纯函数 `retentionCutoffOf`），与 `EnforceDiskRetention`（数 `.index` 文件）同口径。此前它按控制层版本集合算，会把从未落盘的版本误判成"策略丢弃"——而 retention 分支排在 `PENDING`/active 分支**之前**，误判意味着**拒绝补建**（老 PENDING 写入、回滚后的 active 版本都会中招，而 `EnforceDiskRetention` 在磁盘上恰恰把 active 盾住）。
- 测试：`TestReconcileIndexes_WindowFollowsArtifactsNotTheVersionSet`（带一条"前提守卫"，防止它退化成恒真测试）、`TestRetentionCutoffOf`。
- **孤儿 sidecar 也一并清了**：`EnforceDiskRetention` 的删除循环是**从 `.index` 出发**的，所以一个产物已丢失的版本，它的 `<v>.index.used` / `<v>.index.mem` 永远进不了那个循环——实测两轮 retention 都不动它。现在同一趟先扫一遍这两类孤儿（有 `.index` 就留，没有就删）。**只清这两种后缀**：`saveToDisk` 与 `recordInterestNow` 都在 `.index` 之后写它们，而 `InstallIndex` 是**先写 `.ids` 后写 `.index`**，所以"有 `.ids` 没 `.index`"是正常的安装中间态，不能当垃圾（那属于 §8.8）。测试：`TestIndexManager_RetentionClearsOrphanedSidecars`。
- 注意一条**反直觉的结论**（核实过）：`retentionCutoffOf` 并不需要把 `.used` 盾算进去。盾保护的产物留在磁盘上 → 产物集合更大 → cutoff 更小 → 判定反而更宽松；盾的效果已经通过"数产物"间接进入了窗口。而孤儿 `.used` 也不占 shield 名额（`accessProtectedIDs` 只遍历磁盘上的候选）。

### `IndexRetentionCount` 默认值怎么标定

它既是磁盘配额，也是**可被分发修复的追赶深度上限**（§1「追赶的有效窗口」）。标定要同时满足两头：

- **下界**：不小于"一次批量版本推进的幅度"。否则刚构建出来的产物会在分发读盘之前被下一次 retention 收走——除非 `.used` 盾先兜住（Step 4 做的正是这个）。所以标定问的其实是"盾与窗口，谁先接住"。
- **上界**：磁盘。上界 ≈ `IndexRetentionCount × (1 + RetentionProtectMax 的相对倍数)` × 单版本产物大小 × 每节点 KB 数。产物大小可参照已有实测的小样本（免图 1325 B / 带图 6866 B），生产量级要按文档集规模外推。

**三个可观测信号**（都已在代码里，不需要新埋点）：

| 信号 | 来源 | 期望 |
|---|---|---|
| `index distribution failed` 的 warn 计数 | `PushIndexToReplicas` 读盘失败 | 接了 Step 4 之后应 ≈ 0；不为 0 说明排队跨过了盾窗口 |
| `skipping rebuild of retention-dropped index` 的 Info 计数 | `ReconcileIndexes`（口径已统一） | 应只对应真正被策略丢弃的版本 |
| 副本侧自建次数 | `buildPool` 的 backfill 条目 | 越少越好——每次自建都是一次本该省下的构建 |

**测法（已落地为 `integration/docker/lag_catchup_test.go`）**：

1. 集群就绪——用例自己先等存储组 healthy 再写第一版，否则首次 `CreateVersion` 会因 dispatch 找不到候选被当作 TRANSIENT 放弃，之后的 READY 永远等不到（第一次跑就是这么超时的）；
2. 在**同一个 KB** 上串行推进 M 个版本（每版等 READY，父版本不能是 PENDING）；
3. `STRATUM_LAG_OFFLINE=1` 时先把一个 storage 节点杀掉再推进，随后拉起它，看它的产物数；
4. 用例打印每个 storage 的产物数与该 KB 上的三类分发失败，而不是断言阈值——阈值取决于部署的写入速率。

**首次实测（2026-09-16，3 控制 + 3 存储，默认配置 `gc.version_retention_count=50` / `push_concurrency=4`，M=55）**：

| 读数 | 值 |
|---|---|
| 推进耗时 | 36 s（55 版，每版等到 READY） |
| 产物数（三个 storage） | 批前 `[1 1 1]` → 批后 `[56 56 56]` |
| `read local index`（读盘时产物已不在） | **1** |
| `checksum mismatch`（收到的对在接收方校验失败） | **7** |
| 副本不可达 | 2 |
| `retention-dropped` | 0 |

两条结论：

1. **窗口 50 对这个写入速率够用**。56 个产物一个没被淘汰——`50 ≤ 56` 是靠 `.used` 盾兜住的（若没有 Step 4 的盾，retention 会把最老的 6 个删掉，而其中可能正有排队待发的）；55 次分发里只有 1 次撞上"产物已不在"。这印证了"下界 ≥ 一次批量推进的幅度"这条：**先把盾接上，窗口才有资格只当配额看**。
2. **但暴露出一个独立的交错竞态**：`checksum mismatch` 7 次（前一次跑 8 次，稳定复现）。接收方 `Load` 时报 `<v>.index` 的 checksum 与 sidecar 记录的不符，说明它落盘的 `<v>.index` + `<v>.index.ids` **不是同一代**。**已修并复验**（同一用例、同一参数：`checksum` 回到 **0**）。根因是**同一版本那一对产物文件被并发读写**——本地构建 `saveToDisk`（经 vecstore 进程写盘）、`InstallIndex`（接收分发）、`ReadIndexFiles`（分发读盘）三者之间没有任何互斥，而 Step 1 的异步化把"同一次构建完成的多次分发"从串行变成了并发（`invokeCallback` 对回调带重试，最多 4 次，每次都会走 `distributeIndex`）。修法三件：① 一张按版本分片的锁（`IndexManagerImpl.installShards`，FNV-1a 取模 64，固定表无需回收）；② `InstallIndex`、`saveToDisk`、`ReadIndexFiles` 全部取该锁，让同一版本的写-写与读-写都串行（锁序统一为 shard → `im.mu`，三个入口的调用方都不持 `im.mu`）；③ `distributeIndex` 用 `sync.Map` 对同一 `(kbID, versionID)` 做 in-flight 去重。`read local index` 仍是 1——那一条与窗口有关，与本次修复无关。

**用例位置与开关**：`integration/docker/lag_catchup_test.go`（`//go:build docker`）；`STRATUM_LAG_VERSIONS`、`STRATUM_LAG_OFFLINE`、`STRATUM_LAG_SETTLE`、`STRATUM_LAG_TIMEOUT` 可调。本仓库的集群由 `scripts/docker-cluster-both.sh`（控制组 + 存储组）管理，不是 `docker-cluster.sh`。

### 顺带清理：过时的注释（✅ 已做）

- `internal/plane/local_data_plane.go` 的 `IndexStore.TriggerBuildBackfill` 注释与 `internal/index/build_queue.go` 的 "a restart over a populated volume … hundreds of builds" 都改成了"历史动因 + 现状"的写法：reconcile 现在只补 `PENDING` 与 active，而 `buildPool` 的上限仍为版本切换 / 冷重建 / 追赶批次保留。
- §3 的行号引用已按落地后的代码校正（见 Step 1 与 Step 3）。
