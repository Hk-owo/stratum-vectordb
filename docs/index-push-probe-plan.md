# 索引分发发送端探测（§8.4 跳过已持有产物的副本）——实施计划

> 状态：**方案待实施**（代码尚未落盘；撰写环境无 Go 工具链、无 protoc，方案中的代码未编译、未运行）
> 范围：`internal/sync`、`internal/plane`、`internal/index`（只读方法）、`internal/plane/index_share_test.go`、`internal/sync/push_test.go`
> 关联：`Stratum_设计文档v13.md` §8.4（建一次、分发 N 份）；`docs/index-distribution-backpressure-plan.md` §1（追赶的有效窗口）、§6 风险 9；`docs/active-lag-detection-design.md` §9

> 约定：本文行号以撰写时的代码为准（`docs/index-distribution-backpressure-plan.md` §3 末尾有同样的说明）；落地后需要重新校正。

---

## 1. 背景与动机

### 1.1 现状：装的不会推，建的一定会推

- **装**（`IndexManagerImpl.InstallIndex`，`internal/index/install.go:49`）**不触发任何回调** —— `install.go` 里没有 `invokeCallback`，所以"收到推送"不会引发再分发。
- **建**（`doBuild` 完成且 READY）**一定会推** —— `invokeCallback`（`internal/index/impl.go:1155`）→ `RegisterBuildCallback`（`cmd/stratum/main.go:296-320`）→ `distributeIndex`（`main.go:784-793`）→ `PushIndexToReplicas`（`internal/plane/local_data_plane.go:1007`）。回调里唯一的条件是 `status == types.IndexStatusReady`，**没有"我是不是原始构建者"这种判断**。

于是当一次推送失败（接收端自建）时，会形成一次反向扇出：

```
A 建完 v7 ──推──▶ B   ✗ 失败（warn: "that replica will build its own"）
A 建完 v7 ──推──▶ C   ✓ 装上，结束（不再转发）
B 被查询 / active 触发 → 自建 v7 → READY
B ──推──▶ A、C       ← 又一次全量扇出；A 收到的是它自己已经持有的那份
```

`PushIndexToReplicas` 的目标是 `resolveReplicas(ctx)` 返回的**全部**其它副本（`local_data_plane.go:1027-1040`），且循环内**串行**逐个推。所以"回推"不是链式放大，但**总会出现"对端已持有"的冗余推送**：多构建者（推送失败后自建、§8.6a 冷重塑、§8.6(d) GC 重建、副本按需自建）每多一个，就多扇出一圈。

### 1.2 一次冗余推送的实际成本

接收方为一份"它已经有的产物"实际付出四项：

| 成本 | 依据 |
|---|---|
| 网络：整份产物（tens of MB） | `IndexPusher.PushIndex` 按 `indexChunkSize = 1 MiB` 分片流式发送（`internal/sync/index_push.go`） |
| 内存：整条流缓冲进内存，且在锁外 | `PushHandler.PushIndexData`（`internal/sync/push.go:577-580` 注释："The stream is buffered in memory rather than written through… Indexes are tens of megabytes"） |
| 磁盘：两份文件（临时名 + rename），覆盖已有的那份 | `installFile`（`internal/index/install.go:63-69`） |
| vecstore：一次 `Load` | `loadFromDisk` 的顺序是**先发 Load RPC（`impl.go:1550`）、再检查内存表已加载则返回 nil（`impl.go:1561-1563`）**，所以重复安装必然打到 vecstore |

### 1.3 为什么现在省不掉

1. **接收端判断得太晚**：`PushIndexData` 只检查"有 storage / 流里有 kbID / installer 已接线"，然后**先把流缓冲完**，才轮到 `InstallIndex`（那里才有 `installShards` 串行）。流量已经花掉了。
2. **发送端不知道对端有没有**：`resolveReplicas` 是"其它副本"的集合，"构建者"不是一个可查询的身份（没有构建者选举，也没有持有者索引）。
3. **去重只管自己这一侧**：发送端的 `sync.Map`（`main.go:784-793`）防的是 `invokeCallback` 重试导致的**同节点**重复推，管不到"别的节点推给我的重复"。

---

## 2. 目标 / 非目标

### 目标

1. **发送端在传输之前探测**：对端已持有该版本产物时，**一个字节的产物都不传**，且不读本地产物、不占分发额度。
2. **不改 `.proto`**：用 `PushIndexData` 现有字段表达探测，避免重新生成 `sync.pb.go`（撰写环境无 protoc）。
3. **滚动升级安全**：新老组合双向都能正常工作，无需版本协商。

### 非目标（本轮不做，记录理由）

- **校验内容一致**：探测答的是"文件是否存在"，不做 checksum / 形态比对（见 §3 的取舍说明）。
- **把控制层的 index-ready 集接进分发器**：§7.9 的 `indexReady`（`ReportEpoch`）原则上可以裁剪推送目标，但它目前只喂 epoch 与服务站路由，且是周期上报、有滞后；接入是另一件事（见 §10）。
- **改 §7.5 的数据拉取路径**：那条路的多对一压力与源节点侧限流是 `docs/index-distribution-backpressure-plan.md` §6 风险 6 记着的另一笔账。
- **分发内部把 `for peer` 改成并发**：会改变 `PushIndexToReplicas` 的语义与内存特征，超出本方案范围。

---

## 3. 判据（已定）

**对端已有产物 → 跳过本次推送。**

- 语义 = `IndexExists`（磁盘上有该版本的 `.index`），**不**校验 checksum、**不**比对形态。
- 正确性依据：同一版本的产物是自洽且可服务的；§8.6a 保证"带图 / 免图"两种形态**检索语义等价**，所以跳过不会让对端退化成答不出结果，只可能让它保留一份**资源形态次优**的产物（例如保留带图版本、多占内存）。
- **已知取舍**：对端产物若**磁盘损坏或半装**，探测会永久跳过它。若日后要收紧，最便宜的加固是让探测携带 sidecar 的 checksum（几十 KB 级，见 §10）。

---

## 4. 协议约定（不改 `.proto`）

`PushIndexChunk` 现有字段（`knowledge_base_id`、`version_id`、`sidecar`、`data`、`last`）足以表达探测：

```
探测帧 = { kb_id, version_id, sidecar=false, data=<空>, last=false }
```

**为什么这是无歧义的哨兵**：

- `IndexPusher.PushIndex` 开头就拒绝空的 `indexData`（`internal/sync/index_push.go:37-39`）；
- 真实传输的**首帧永远是 sidecar 分片**（`sidecar=true`）。

所以"首帧且 `sidecar=false` 且非 `last`"在现有协议里不可能出现，可以作为探测帧的判据（不必依赖"data 为空"这一条，虽然它同时成立）。

**应答 = gRPC status `codes.AlreadyExists`**：接收端 `return status.Error(codes.AlreadyExists, ...)` 直接终止流；发送端在 `Send` 或 `CloseAndRecv`/`SendAndClose` 处拿到该 code。

**兼容性论证（滚动升级）**：

| 组合 | 行为 |
|---|---|
| 新发送端 → 老接收端 | 老接收端把零长度帧当作空分片 `sidecarBuf.Write(nil)`（无副作用），继续正常收 → 安装照旧成功 |
| 老发送端 → 新接收端 | 首帧是真实 sidecar 分片（`sidecar=true`）→ 不被识别为探测帧 → 照旧 |
| 新 ↔ 新 | 探测生效 |

---

## 5. 改动方案

### Step 1 —— 接收端识别探测帧并应答

**文件**：`internal/sync/push.go`

`PushHandler.PushIndexData`（当前 `:580-620`）的 `Recv` 循环里，在分派到 `sidecarBuf`/`indexBuf` 之前插入：

```go
var probeHandled bool
...
    // §8.4(a) probe: the sender asks "do you already hold this artifact?" with a
    // zero-length frame before spending tens of megabytes. Answering with
    // AlreadyExists closes the stream, so the transfer never starts.
    //
    // Only the FIRST frame can be a probe: a real transfer opens with a sidecar
    // chunk (sidecar=true), and an empty index payload is refused on the sending
    // side, so this combination cannot occur in the normal flow. Treating later
    // frames as probes would race with the data.
    if !probeHandled && len(chunk.GetData()) == 0 && !chunk.GetSidecar() && !chunk.GetLast() {
        probeHandled = true
        if h.installer == nil {
            return status.Errorf(codes.Unimplemented, "sync: PushIndexData: this node holds no storage")
        }
        held, err := h.installer.HasIndex(stream.Context(), kbID, versionID)
        if err != nil {
            // "I could not find out" is not "it is absent": accept the push
            // rather than silently skipping a ship that was actually needed.
            h.logger.Warn("sync: PushIndexData: presence probe failed; accepting the push",
                zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
        } else if held {
            return status.Errorf(codes.AlreadyExists,
                "sync: PushIndexData: %s v%d already holds an artifact; skipping the ship", kbID, versionID)
        }
        continue
    }
```

同时在 `internal/sync/push.go` 约 `:270` 的 installer 接口上加一个**只读**方法：

```go
type IndexInstaller interface {
    InstallIndex(ctx context.Context, kbID string, versionID int64, indexData, sidecarData []byte) error
    // HasIndex reports whether this node already holds an on-disk artifact for
    // the version — the fact the §8.4(a) probe answers with. Read-only: it must
    // not build, load or fetch anything.
    HasIndex(ctx context.Context, kbID string, versionID int64) (bool, error)
}
```

> **落地前需确认**：该接口的确切名字，以及实现侧是否已有等价方法。`index.IndexManagerImpl` 已有语义等价的能力（`ReconcileIndexes` 用的 `IndexExists(ctx, kbID, versionID) (bool, error)`，`internal/plane` 的 `IndexStore` 也把它列在接口里），因此实现侧大概率**只需对齐方法名/签名或加一层薄适配**，不应引入新逻辑。

### Step 2 —— 发送端发探测帧，并把"已存在"变成一种身份

**文件**：`internal/sync/index_push.go`

在 `PushIndex` 内、任何真实分片之前：

```go
// Probe first: one zero-length frame asks the receiver whether it already holds
// this artifact. AlreadyExists means "skip" — the receiver answered before a
// single megabyte moved. Any other error is a normal transfer failure.
if err := stream.Send(&pb.PushIndexChunk{
    KnowledgeBaseId: kbID, VersionId: versionID, Sidecar: false, Data: nil, Last: false,
}); err != nil {
    return fmt.Errorf("sync: PushIndex(%s v%d) to %s: probe: %w", kbID, versionID, targetAddr, err)
}
```

错误身份的传达有两种做法：

- **A（推荐）**：`PushIndex` 把 `status.Code(err) == codes.AlreadyExists` 解包成哨兵（沿用本项目"错误身份结构化"的做法，`internal/errors` 已有 `ToGRPCStatus` / `ErrorInfo` 的成例）：

  ```go
  // ErrIndexAlreadyPresent reports that the target already holds this version's
  // artifact, so the ship was skipped rather than lost. Its own error identity
  // exists so the caller can tell "not needed" apart from "failed".
  var ErrIndexAlreadyPresent = errors.New("sync: the target already holds this version's index")
  ```

- **B（更松）**：调用方直接判 `status.Code(err) == codes.AlreadyExists`。

**必须在两处检查**：接收端是在 `Recv` 之后 `return status.Error(...)` 的，发送端不一定能在 `Send` 处立刻看到它——可能要到 `CloseAndRecv` / `SendAndClose` 才拿到。**两处都要判定**，否则会退化成 `io.EOF` 并被误当作成功。

### Step 3 —— 调用方区分"跳过"与"失败"，并让它可观测

**文件**：`internal/plane/local_data_plane.go`（`PushIndexToReplicas`，当前 `:1007-1040`）

```go
var skipped int
for _, peer := range peers {
    err := d.indexShipper.PushIndex(ctx, peer, kbID, versionID, indexData, sidecarData)
    switch {
    case err == nil:
    case errors.Is(err, sync.ErrIndexAlreadyPresent):
        skipped++ // 对端已持有：本次传输没有发生
    default:
        d.logger.Warn("plane: index distribution failed; that replica will build its own",
            zap.String("peer", peer), zap.String("kb_id", kbID),
            zap.Int64("version_id", versionID), zap.Error(err))
    }
}
if skipped > 0 {
    d.logger.Info("plane: index distribution skipped replicas that already hold the artifact",
        zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Int("skipped", skipped))
}
```

理由：没有这条 Info，这次优化在日志里**完全不可见**，也就无法判断它值不值得（对照 `docs/index-distribution-backpressure-plan.md` §7 的"三个可观测信号"写法）。

### Step 4（推荐，收益最大）—— 把探测前置到读盘之前

探测只需要 `kbID`/`versionID`，不需要 `ReadIndexFiles`——而**读盘正是内存峰值所在**（`local_data_plane.go:1016-1021` 的注释："Acquire before the read, not before the ship: the read is what makes the memory peak"）。所以更划算的顺序是：

```
① 对每个 peer 发探测（便宜，1 个小 RPC，带短超时——可复用 peerCursorTimeout 那一档）；
② 需要推的 peer 集合为空 → 直接返回：一次 read 都不做，连 indexPushSem 都不必取；
③ 否则取 indexPushSem → ReadIndexFiles → 只对"需要推"的 peer 逐个推。
```

这样"多构建者互推"这类场景退化成 N 次几十字节的 RTT：**不占 `push_concurrency`、不读整份文件、不占出带宽**。

取舍要说清：探测落在 `indexPushSem` 之外，意味着"同时探测的路数"没有节点级上限。单个探测是一个小 RPC 且带超时，风险低；若要收紧，应为探测**另设**一个更宽松的信号量，而**不要复用 `indexPushSem`**（它的语义是"读一份大产物 + 推 N 份"，混在一起会让探测排队，失去前置的意义）。

---

## 6. 涉及文件清单

| 文件 | 改动 |
|---|---|
| `internal/sync/push.go` | ① `PushHandler.PushIndexData` 识别探测帧、查产物、回 `AlreadyExists`；② installer 接口新增只读 `HasIndex` |
| `internal/sync/index_push.go` | ① 发送探测帧；② `AlreadyExists` → `ErrIndexAlreadyPresent`；③ `Send` 与 `CloseAndRecv`/`SendAndClose` 两处判定 |
| `internal/plane/local_data_plane.go` | `PushIndexToReplicas` 区分"跳过 / 失败"，记 Info；Step 4：探测前置到读盘之前 |
| （实现侧，待确认） | 让已注入的 index manager 满足 `HasIndex`（对齐现有 `IndexExists` 或加薄适配） |
| `internal/sync/push_test.go` | 接收端用例（见 §7） |
| `internal/plane/index_share_test.go` | 发送端用例（见 §7）；`stubIndexShipper`（`:30`）需能按 peer 返回"已存在" |

---

## 7. 测试计划

### 接收端（`internal/sync/push_test.go`）

| 用例 | 断言 |
|---|---|
| 探测帧 + 本地已有产物 | 返回 `codes.AlreadyExists`；**installer 的 `InstallIndex` 未被调用**（关键：证明没有缓冲几十 MB 就退出） |
| 探测帧 + 本地无产物 | 正常收完并 `InstallIndex`（探测帧无副作用） |
| 无探测帧的旧序列（兼容） | 首帧是 sidecar 分片 → 正常安装成功 |
| 探测查询报错 | **不**跳过，照常接收并安装 |
| 非首帧的零长度帧 | 不被识别为探测帧（防与数据竞争） |

### 发送端（`internal/plane/index_share_test.go`）

| 用例 | 断言 |
|---|---|
| 某 peer 回"已存在" | 该 peer 被跳过（不记 warn），**后续 peers 继续被推** |
| 全部 peer 都跳过 | 返回成功；Step 4 后**`stubIndexReader` 未被调用**（证明省下读盘与内存峰值） |
| 混用：一个跳过 + 一个失败 + 一个成功 | 三条路径各自被正确计数/记录 |
| 现有回归用例 | 全部保持通过（语义未变） |

---

## 8. 风险与注意点

1. **跳过的是"存在"，不是"正确"**：对端产物损坏/形态次优时会被永久跳过（§3 已确认的取舍）。
2. **错误路径要两处检查**：接收端在 `Recv` 之后返回 `AlreadyExists`，发送端可能直到 `CloseAndRecv`/`SendAndClose` 才拿到；只查一处会把"跳过"误判为 `io.EOF` 成功。
3. **兼容性已论证**（§4 表），但需要在用例里钉住"老序列仍成功"这一条。
4. **探测失败一律照推**：查不到 ≠ 没有，宁可多传一份。
5. **探测不进 `indexPushSem`**（Step 4）：否则探测会与大产物传输互相排队，前置的意义消失。
6. **收益依赖场景**：正常路径（1 个构建者、推送成功）只多付 N 次 RTT（LAN 上可忽略）；收益出现在"多构建者互推 / 重复推送"这些**当前确实存在**的路径上（§1.1）。
7. **零长度帧的判据要认"首帧"**：仅凭"`sidecar=false` 且非 `last`"不够稳，必须叠加"尚未处理过任何帧"，否则一个真实传输中途的异常帧会被误判。

---

## 9. 验证方式

撰写环境（本机）**没有 Go 工具链，也没有 protoc**：

- 无法 `go build` / `go vet` / `go test`，因此本文所有代码**未经编译与运行**；
- 本方案**不需要 protoc**（不改 `.proto`），这一点是刻意的设计约束。

落地后需要在有工具链的机器上执行：

```bash
go build ./...
go vet ./internal/sync/ ./internal/plane/ ./internal/index/
go test ./internal/sync/ -run PushIndexData -race
go test ./internal/plane/ -run PushIndexToReplicas -race
go test ./internal/... ./service/... ./cmd/stratum/...
```

> 手工确认点：收到回推的节点，其日志里应出现 `skipped replicas that already hold the artifact`，且**不再**出现该 peer 的安装（对照发送端的 `indexPushDistributions` 计数与接收端的 `PushIndexData` 日志）。

---

## 10. 后续（本轮之外）

1. **判据收紧到内容级**：让探测携带 sidecar 的 checksum（或形态标记），只跳过"内容一致"的副本——把 §3 的取舍补上，代价是每次探测多传几十 KB。
2. **用控制层的 index-ready 集裁剪推送目标**：§7.9 的 `indexReady` 上报已经存在（`ReportEpoch`），但只喂 epoch 与服务站路由；接进分发器即可在**选择目标**这一层省掉探测本身。注意它是周期上报、有滞后，需要用"探测兜底 + 集合加速"的组合。
3. **源节点侧的背压**：本方案解决的是"已知对端已有时不推"，不解决"多个节点同时向同一个源节点拉"——后者见 `docs/index-distribution-backpressure-plan.md` §6 风险 6 与 `docs/active-lag-detection-design.md` §9。
4. **细粒度分发限流**：把 `for peer` 循环纳入闸门 + 二级优先级，待出现"副本被选中要立刻服务查询但版本落后"这种急切分发需求再评估。
