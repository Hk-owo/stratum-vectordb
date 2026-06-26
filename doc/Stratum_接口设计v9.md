# Stratum 接口设计文档

> 本文档记录 Stratum 的接口设计，包括内部模块接口、函数签名和逻辑链路。
> 预留接口以 `[预留]` 标注，暂不实现，占位供未来扩展。

---

## 数据类型定义

### Go 侧

```go
// IndexStatus 索引构建状态
type IndexStatus int

const (
    IndexStatusPending IndexStatus = iota // 元数据已分配，存储层写入进行中，不可查询
    IndexStatusReady                      // 索引构建完成，可查询
    IndexStatusFailed                     // 构建失败，RollbackVersion 拒绝切换；RebuildIndex 可重新触发
)

// KBStatus 知识库状态
type KBStatus int

const (
    KBStatusActive KBStatus = iota
    KBStatusDeleting
    KBStatusDeleteFailed
)

// Chunk 文档切割后的单个片段
type Chunk struct {
    ChunkID string // SHA-256(chunk文本 + embed配置ID)
    Content string // chunk 原始文本
}

// SearchResult 向量搜索单条结果
type SearchResult struct {
    ChunkID string  // chunk ID
    Score   float32 // 相似度分数
}

// AggregationMethod chunk 分数聚合方式，查询级别可配置
type AggregationMethod int

const (
    AggregationMethodMedian AggregationMethod = iota // 默认
    AggregationMethodMax
    AggregationMethodMean
)

// EmbedConfig embed 服务配置，知识库级别固定属性，创建后不可变
type EmbedConfig struct {
    ServiceAddr string // embed 服务地址
    ModelID     string // embed 模型 ID，参与 chunk ID 计算：SHA-256(chunk文本 + ModelID)
}

// KnowledgeBaseMeta 知识库元数据，存储在 Raft 状态机
type KnowledgeBaseMeta struct {
    KBID             string
    Name             string
    ChunkWindowSize  int
    ChunkOverlapSize int
    IndexType        string     // HNSW / IVF / FLAT，创建后不可变，当前仅 HNSW 真实实现
    Similarity       string     // COSINE / EUCLIDEAN / INNER_PRODUCT，创建后不可变，默认 COSINE
    EmbedConfig      EmbedConfig
    ActiveVersionID  int64
    Status           KBStatus
}

// VersionMeta 版本元数据，存储在 Raft 状态机
type VersionMeta struct {
    VersionID       int64
    ParentVersionID int64
    KBID            string
    CreatedAt       int64      // Unix 时间戳，leader apply 时本地时钟，不要求跨节点单调
    IndexStatus     IndexStatus
}

// ChangeOp 文档变更操作类型
type ChangeOp int

const (
    ChangeOpAdd ChangeOp = iota
    ChangeOpDelete
    ChangeOpUpdate
)

// DocChange 文档变更记录
type DocChange struct {
    Op      ChangeOp
    DocID   string
    Content string // ADD / UPDATE 必填；向量由 Stratum 内部调用 embed 服务生成，调用方不传
}

// PendingRecord WAL 崩溃恢复时的待处理记录
type PendingRecord struct {
    Type PendingRecordType
    KBID string // 知识库 ID，DELETE_MARK 类型使用
}

// ClusterStatus Raft 集群连通性状态，不依赖具体知识库
type ClusterStatus struct {
    HasLeader   bool
    MemberCount int
    LeaderID    int64
}

// ReplayCounter WAL 重放失败计数，内存态，不持久化
type ReplayCounter struct {
    Record     PendingRecord
    RetryCount int
}

// PendingRecordType WAL 待处理记录类型
type PendingRecordType int

const (
    PendingRecordTypeDeleteMark PendingRecordType = iota // 知识库删除未完成
)
```

### C++ 侧

```cpp
// MetricType 向量距离度量方式
enum class MetricType {
    COSINE,
    EUCLIDEAN,
    INNER_PRODUCT
};

// ChunkVector 用于构建索引的 chunk 数据
struct ChunkVector {
    std::string chunk_id;
    std::vector<float> vector;
};

// SearchResult 向量搜索单条结果
struct SearchResult {
    std::string chunk_id;
    float score;
};
```

---

## 逻辑链路总览

### CreateKnowledgeBase
```
CreateKnowledgeBase
  → RaftNode.ProposeCreateKB
  → 初始化 chunk 存在性 BloomFilter
  → 返回 knowledge_base_id + initial_version_id
```

### DeleteKnowledgeBase
```
DeleteKnowledgeBase
  → WAL.WriteDeleteMark
  → RaftNode.ProposeMarkKBDeleting（标记删除中）
  → [异步] DeleteCoordinator.Execute
      → IndexManager.EvictByKB
      → DocStore.DeleteByKB
      → ChunkStore.DeleteByKB
      → ChunkDocMapper.DeleteByKB
      → VersionDocList.DeleteByKB
      → RaftNode.ProposeRemoveKBMeta（ErrKnowledgeBaseNotFound 视为成功）
      → WAL.WriteDeleteComplete
      → [失败超过重试上限] RaftNode.ProposeMarkKBDeleteFailed
```

### CreateVersion
```
CreateVersion
  → WriteCoordinator.Execute
      → WAL.WriteBegin
      → RaftNode.ProposeCreateVersion
          （Raft apply 阶段：先写 WAL.WriteVersionID(versionID)，再写状态机；
            校验父版本约束；分配新版本 ID，初始状态 PENDING）
      → 对每个变更文档：
          → ChunkSplitter.Split（计算 ChunkID = SHA-256(chunk文本 + embed配置ID)）
          → EmbedClient.Embed（调用 embed 服务生成每个 chunk 的向量）
          → 对每个 chunk：
              → BloomFilter.Test
              → ChunkStore.Exists（假阳性确认）
              → ChunkStore.Write + BloomFilter.Add
              → ChunkDocMapper.Write（正向+反向）
          → DocStore.Write（key 含新分配的版本 ID）
      → VersionDocList.Write
          （从父版本前缀扫描全量文档 ID，应用变更，写入新版本全量文档 ID 集合；
            key 含新分配的版本 ID）
      → BloomFilter.Serialize（版本文档布隆过滤器持久化）
      → WAL.WriteCommit(versionID)
      → IndexManager.TriggerBuild（异步）
  → 返回 version_id
```

### ListVersions
```
ListVersions
  → RaftNode.ListVersions
  → 返回版本列表
```

### RollbackVersion
```
RollbackVersion
  → RaftNode.GetKB（确认目标版本存在且索引状态为 READY）
  → RaftNode.ProposeRollback
  → 返回成功
```

### Query
```
Query
  → 若 version_id 未传：RaftNode.GetKB(kbID) → 取 ActiveVersionID
  → RaftNode.ListVersions → 检查目标版本 IndexStatus
      → PENDING → 返回 ErrVersionPending（gRPC FAILED_PRECONDITION）
      → FAILED  → 返回 ErrVersionFailed（gRPC FAILED_PRECONDITION）
      → READY   → 继续
  → IndexManager.Search
  → 过滤低于 threshold 的结果
  → ChunkDocMapper.ListDocIDs（对每个 chunk ID）
  → 按文档 ID 分组去重，按 aggregation 指定方式（MAX/MEDIAN/MEAN，默认 MEDIAN）计算分数
  → BloomFilter.Test（版本文档布隆过滤器）
  → VersionDocList.ListDocIDs（假阳性确认）
  → 按分数排序，取前 top_k
  → DocStore.ReadAt（对每个文档 ID）
  → 异步写入审计日志
  → 返回文档列表
```

### HealthCheck
```
HealthCheck
  → 并行：
      → RaftNode.GetClusterStatus（Raft 连通性探针，不依赖具体知识库）
      → DocStore / ChunkStore 连接状态探针
      → IndexManager.Ping（轻量探针，不触发冷加载）
  → 汇总状态 → 返回 HEALTHY / DEGRADED / UNHEALTHY
```

### GetSystemStatus
```
GetSystemStatus
  → HealthCheck（复用三态汇总）
  → RaftNode.ListVersions（遍历所有知识库，筛选长期 FAILED 版本）
  → WAL.GetReplayCounters（超阈值的重放失败记录）
  → 资源使用快照
  → 返回诊断信息
```

### RebuildIndex
```
RebuildIndex
  → RaftNode.ProposeUpdateVersionStatus(PENDING)
  → IndexManager.TriggerBuild
  → 返回触发成功（异步）
  → [异步] 构建完成或失败后 IndexManager 回调
      → 成功 → RaftNode.ProposeUpdateVersionStatus(READY)
      → 失败 → RaftNode.ProposeUpdateVersionStatus(FAILED)，记录 ERROR 日志 + counter
```

---

## 内部模块接口（Go）

每个模块实现前必须先定义接口，调用方依赖接口而非具体实现，模块可独立替换。

---

### ChunkSplitter

文档切割策略接口。默认实现为滑动窗口，未来可替换为语义切割等策略。切割并计算 `ChunkID = SHA-256(chunk文本 + embed配置ID)`，返回的 `Chunk` 直接可用。文档长度小于窗口大小时，整篇文档作为一个 chunk。

**逻辑链路**：`CreateVersion → ChunkSplitter.Split`

```go
type ChunkSplitter interface {
    Split(content string, windowSize int, overlapSize int, embedConfigID string) []Chunk
}

// 默认实现
func (s *SlidingWindowSplitter) Split(content string, windowSize int, overlapSize int, embedConfigID string) []Chunk
```

---

### EmbedClient

embed 服务客户端接口，调用外部 embed 服务生成 chunk 向量。知识库级别绑定，创建后不可变。

**逻辑链路**：`CreateVersion → EmbedClient.Embed`

```go
type EmbedClient interface {
    Embed(ctx context.Context, chunks []Chunk) (map[string][]float32, error) // chunkID → vector
}

// HTTP 客户端实现
func (c *HTTPEmbedClient) Embed(ctx context.Context, chunks []Chunk) (map[string][]float32, error)
```

---

### DocStore

文档原始文本存储接口。MVCC 语义，key 为 `知识库 ID + 文档 ID + 版本 ID`。

**逻辑链路**：
- `CreateVersion → DocStore.Write`
- `Query → DocStore.ReadAt`
- `DeleteKnowledgeBase → DocStore.DeleteByKB`

```go
type DocStore interface {
    Write(ctx context.Context, kbID, docID string, versionID int64, value []byte) error
    ReadAt(ctx context.Context, kbID, docID string, maxVersionID int64) ([]byte, error)
    DeleteByKB(ctx context.Context, kbID string) error
}

// PebbleDB 实现
func (s *PebbleDocStore) Write(ctx context.Context, kbID, docID string, versionID int64, value []byte) error
func (s *PebbleDocStore) ReadAt(ctx context.Context, kbID, docID string, maxVersionID int64) ([]byte, error)
func (s *PebbleDocStore) DeleteByKB(ctx context.Context, kbID string) error
```

---

### ChunkStore

chunk 向量存储接口。权威存储在 C++ vecstore 的 RocksDB（`ChunkStorage`），Go 侧 `ChunkStore` 是 vecstore 内部 gRPC 的客户端封装，不在 PebbleDB 中存储向量数据，无数据冗余。key 为 `知识库 ID + chunk ID`。

**逻辑链路**：
- `CreateVersion → BloomFilter.Test → ChunkStore.Exists → [gRPC] → ChunkStorage.Exists`
- `CreateVersion → ChunkStore.Write → [gRPC] → ChunkStorage.Write`
- `DeleteKnowledgeBase → ChunkStore.DeleteByKB → [gRPC] → ChunkStorage.DeleteByPrefix`

```go
type ChunkStore interface {
    Write(ctx context.Context, kbID, chunkID string, vector []float32) error
    Exists(ctx context.Context, kbID, chunkID string) (bool, error)
    Delete(ctx context.Context, kbID, chunkID string) error
    DeleteByKB(ctx context.Context, kbID string) error
}

// vecstore gRPC 客户端实现
func (s *VecstoreChunkStore) Write(ctx context.Context, kbID, chunkID string, vector []float32) error
func (s *VecstoreChunkStore) Exists(ctx context.Context, kbID, chunkID string) (bool, error)
func (s *VecstoreChunkStore) Delete(ctx context.Context, kbID, chunkID string) error
func (s *VecstoreChunkStore) DeleteByKB(ctx context.Context, kbID string) error
```

---

### ChunkDocMapper

chunk 到文档 ID 的双向映射接口。维护两个方向的 key：
- 正向 `知识库 ID + chunk ID + 文档 ID`（chunk → 文档，供 Query 读路径使用）
- 反向 `知识库 ID + 文档 ID + chunk ID`（文档 → chunk，供 IndexManager 异步构建批量反查使用）

两个方向在同一次 `Write` 调用中一起写入，保持一致。

**逻辑链路**：
- `CreateVersion → ChunkDocMapper.Write`
- `Query → ChunkDocMapper.ListDocIDs`
- `IndexManager.TriggerBuild → ChunkDocMapper.ListChunkIDsByDocs`
- `DeleteKnowledgeBase → ChunkDocMapper.DeleteByKB`
- `[异步 GC] → ChunkDocMapper.DeleteByDoc`

```go
type ChunkDocMapper interface {
    Write(ctx context.Context, kbID, chunkID, docID string) error
    ListDocIDs(ctx context.Context, kbID, chunkID string) ([]string, error)
    ListChunkIDsByDocs(ctx context.Context, kbID string, docIDs []string) ([]string, error) // 批量反查，合并去重
    DeleteByKB(ctx context.Context, kbID string) error
    DeleteByDoc(ctx context.Context, kbID, docID string) error
}

// PebbleDB 实现
func (m *PebbleChunkDocMapper) Write(ctx context.Context, kbID, chunkID, docID string) error
func (m *PebbleChunkDocMapper) ListDocIDs(ctx context.Context, kbID, chunkID string) ([]string, error)
func (m *PebbleChunkDocMapper) ListChunkIDsByDocs(ctx context.Context, kbID string, docIDs []string) ([]string, error)
func (m *PebbleChunkDocMapper) DeleteByKB(ctx context.Context, kbID string) error
func (m *PebbleChunkDocMapper) DeleteByDoc(ctx context.Context, kbID, docID string) error
```

---

### VersionDocList

版本文档列表接口。key 为 `知识库 ID + 版本 ID + 文档 ID`，记录每个版本的全量文档 ID 集合。

**逻辑链路**：
- `CreateVersion → VersionDocList.Write`
- `Query → BloomFilter.Test → VersionDocList.ListDocIDs（假阳性确认）`
- `IndexManager.TriggerBuild → VersionDocList.ListDocIDs`
- `DeleteKnowledgeBase → VersionDocList.DeleteByKB`

```go
type VersionDocList interface {
    Write(ctx context.Context, kbID string, versionID int64, docID string) error
    ListDocIDs(ctx context.Context, kbID string, versionID int64) ([]string, error)
    DeleteByVersion(ctx context.Context, kbID string, versionID int64) error
    DeleteByKB(ctx context.Context, kbID string) error
}

// PebbleDB 实现
func (v *PebbleVersionDocList) Write(ctx context.Context, kbID string, versionID int64, docID string) error
func (v *PebbleVersionDocList) ListDocIDs(ctx context.Context, kbID string, versionID int64) ([]string, error)
func (v *PebbleVersionDocList) DeleteByVersion(ctx context.Context, kbID string, versionID int64) error
func (v *PebbleVersionDocList) DeleteByKB(ctx context.Context, kbID string) error
```

---

### BloomFilter

布隆过滤器接口，支持持久化和重建。

**逻辑链路**：
- `CreateVersion → BloomFilter.Test → BloomFilter.Add`（chunk 存在性）
- `CreateVersion → BloomFilter.Serialize`（版本文档布隆过滤器持久化）
- `Query → BloomFilter.Test`（版本文档过滤）
- `DeleteKnowledgeBase → BloomFilter.Reset`

```go
type BloomFilter interface {
    Add(key string)
    Test(key string) bool
    Serialize() ([]byte, error)
    Deserialize(data []byte) error
    Reset()
}

// bits-and-blooms 实现
func (b *BloomFilterImpl) Add(key string)
func (b *BloomFilterImpl) Test(key string) bool
func (b *BloomFilterImpl) Serialize() ([]byte, error)
func (b *BloomFilterImpl) Deserialize(data []byte) error
func (b *BloomFilterImpl) Reset()
```

---

### IndexManager

向量索引管理器接口，负责索引的加载、换入换出和查询。

**逻辑链路**：
- `CreateVersion → IndexManager.TriggerBuild`
- `Query → IndexManager.Search`
- `RebuildIndex → IndexManager.TriggerBuild`
- `DeleteKnowledgeBase → IndexManager.EvictByKB`
- `[构建完成或失败] → BuildCompleteCallback → RaftNode.ProposeUpdateVersionStatus`

**构建数据来源**：`TriggerBuild` 异步执行时按以下流程获取该版本的全部 chunk 向量：
```
VersionDocList.ListDocIDs(kbID, versionID) → 该版本全量文档 ID
  → ChunkDocMapper.ListChunkIDsByDocs(kbID, docIDs) → 全量 chunk ID（批量反查，合并去重）
  → 批量 ChunkStorage.Read（经 ChunkStore，内部 gRPC）→ chunk 向量
  → VectorIndex.Build(chunks, metric)
```

**类型定义**：
```go
type BuildCompleteCallback func(kbID string, versionID int64, status IndexStatus) error
```

**说明**：构建成功时 `status` 为 `IndexStatusReady`；构建失败时为 `IndexStatusFailed`，记录 ERROR 日志和构建失败 counter。处于 `IndexStatusFailed` 的版本可通过 `RebuildIndex` 重新触发构建。

```go
type IndexManager interface {
    Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]SearchResult, error)
    TriggerBuild(ctx context.Context, kbID string, versionID int64) error
    RegisterBuildCallback(cb BuildCompleteCallback)
    Evict(ctx context.Context, kbID string, versionID int64) error
    EvictByKB(ctx context.Context, kbID string) error
    Ping(ctx context.Context) error
}

// 实现
func (m *IndexManagerImpl) Search(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) ([]SearchResult, error)
func (m *IndexManagerImpl) TriggerBuild(ctx context.Context, kbID string, versionID int64) error
func (m *IndexManagerImpl) RegisterBuildCallback(cb BuildCompleteCallback)
func (m *IndexManagerImpl) Evict(ctx context.Context, kbID string, versionID int64) error
func (m *IndexManagerImpl) EvictByKB(ctx context.Context, kbID string) error
func (m *IndexManagerImpl) Ping(ctx context.Context) error
```

**并发加载等待超时**：若内存中所有索引引用计数均不为 0，`Search` 触发的新索引加载请求等待直到有索引可换出；等待时间超过 `load_wait_timeout_ms` 后返回 `ErrIndexLoadTimeout`，映射为 gRPC `DEADLINE_EXCEEDED`。`Search` 内部同时监听 `ctx.Done()`，gRPC 层超时时立即终止等待。

**Ping**：轻量健康探针，检查 `IndexManager` 自身是否正常运行，不触发任何索引加载操作，不占用引用计数。供 `HealthCheck` 使用。

**回调重试与后台修复**：`BuildCompleteCallback` 调用 `RaftNode.ProposeUpdateVersionStatus` 失败时，内部做指数退避重试；重试耗尽后加入内存"待修复集合"，后台协程定期扫描重试。进程重启后崩溃恢复发现 WAL 有 COMMIT 记录且版本仍为 PENDING：若索引文件已存在则直接 propose READY，不存在则触发重建。

---

### RaftNode

Raft 操作接口，封装版本元数据的强一致读写。

**逻辑链路**：
- `CreateKnowledgeBase → RaftNode.ProposeCreateKB`
- `DeleteKnowledgeBase → RaftNode.ProposeMarkKBDeleting`
- `[异步] DeleteCoordinator → RaftNode.ProposeRemoveKBMeta`
- `[删除失败] DeleteCoordinator → RaftNode.ProposeMarkKBDeleteFailed`
- `CreateVersion → RaftNode.ProposeCreateVersion`
- `RollbackVersion → RaftNode.GetKB → RaftNode.ProposeRollback`
- `ListVersions → RaftNode.ListVersions`
- `RebuildIndex → RaftNode.ProposeUpdateVersionStatus`
- `[构建完成回调] → RaftNode.ProposeUpdateVersionStatus`
- `HealthCheck → RaftNode.GetClusterStatus`

```go
type RaftNode interface {
    ProposeCreateKB(ctx context.Context, kb KnowledgeBaseMeta) error
    ProposeMarkKBDeleting(ctx context.Context, kbID string) error
    ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error
    ProposeRemoveKBMeta(ctx context.Context, kbID string) error
    ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64) (int64, error)
    ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status IndexStatus) error
    ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error
    GetKB(ctx context.Context, kbID string) (KnowledgeBaseMeta, error)
    ListVersions(ctx context.Context, kbID string) ([]VersionMeta, error)
    GetClusterStatus(ctx context.Context) (ClusterStatus, error)
}

// 实现
func (r *RaftNodeImpl) ProposeCreateKB(ctx context.Context, kb KnowledgeBaseMeta) error
func (r *RaftNodeImpl) ProposeMarkKBDeleting(ctx context.Context, kbID string) error
func (r *RaftNodeImpl) ProposeMarkKBDeleteFailed(ctx context.Context, kbID string) error
func (r *RaftNodeImpl) ProposeRemoveKBMeta(ctx context.Context, kbID string) error
func (r *RaftNodeImpl) ProposeCreateVersion(ctx context.Context, kbID string, parentVersionID int64) (int64, error)
func (r *RaftNodeImpl) ProposeUpdateVersionStatus(ctx context.Context, versionID int64, status IndexStatus) error
func (r *RaftNodeImpl) ProposeRollback(ctx context.Context, kbID string, targetVersionID int64) error
func (r *RaftNodeImpl) GetKB(ctx context.Context, kbID string) (KnowledgeBaseMeta, error)
func (r *RaftNodeImpl) ListVersions(ctx context.Context, kbID string) ([]VersionMeta, error)
func (r *RaftNodeImpl) GetClusterStatus(ctx context.Context) (ClusterStatus, error)
```

**版本 ID 分配**：`ProposeCreateVersion` 不接收 `VersionID`。Raft 状态机 apply 阶段执行顺序：先同步调用 `WAL.WriteVersionID(versionID)`（幂等，重复写入直接返回成功），成功后再分配版本 ID 写入状态机。此顺序保证 WAL 有 VERSION_ID 记录时状态机里一定有对应版本，消除孤儿版本。

**父版本约束**：`ProposeCreateVersion` 在状态机 apply 阶段校验：父版本必须属于同一知识库，且不能处于 PENDING 状态。允许分叉（同一父版本可以有多个子版本）。

**ProposeRemoveKBMeta 幂等**：若知识库元数据已不存在，返回成功而非 `ErrKnowledgeBaseNotFound`，保证删除流程崩溃恢复时可安全重复执行。

**知识库删除三阶段**：`ProposeMarkKBDeleting` 置为 `Deleting`；`ProposeRemoveKBMeta` 清理完成后移除元数据；`ProposeMarkKBDeleteFailed` 在重试耗尽后置为 `DeleteFailed`，暴露给运维。

**GetClusterStatus**：返回集群连通性信息，不依赖任何具体知识库，用于 `HealthCheck` 的 Raft 连通性探针。

---

### WAL

WAL 接口，保证写路径和删除路径的崩溃一致性。

**逻辑链路**：
- `CreateVersion → WAL.WriteBegin → [Raft apply 内] WAL.WriteVersionID → ... → WAL.WriteCommit`
- `DeleteKnowledgeBase → WAL.WriteDeleteMark → ... → WAL.WriteDeleteComplete`
- `[启动恢复] → WAL.Recover`
- `GetSystemStatus → WAL.GetReplayCounters`

```go
type WAL interface {
    WriteBegin(ctx context.Context) error
    WriteVersionID(ctx context.Context, versionID int64) error // 幂等，同一 versionID 重复写入返回成功
    WriteCommit(ctx context.Context, versionID int64) error
    WriteDeleteMark(ctx context.Context, kbID string) error
    WriteDeleteComplete(ctx context.Context, kbID string) error
    Recover(ctx context.Context) ([]PendingRecord, error)
    GetReplayCounters() []ReplayCounter
}

// 实现
func (w *WALImpl) WriteBegin(ctx context.Context) error
func (w *WALImpl) WriteVersionID(ctx context.Context, versionID int64) error
func (w *WALImpl) WriteCommit(ctx context.Context, versionID int64) error
func (w *WALImpl) WriteDeleteMark(ctx context.Context, kbID string) error
func (w *WALImpl) WriteDeleteComplete(ctx context.Context, kbID string) error
func (w *WALImpl) Recover(ctx context.Context) ([]PendingRecord, error)
func (w *WALImpl) GetReplayCounters() []ReplayCounter
```

**WAL 记录语义**：
- `WriteBegin`：事务开始标记，无参数
- `WriteVersionID`：由 Raft 状态机 apply 阶段调用，在分配版本 ID 前写入；同一 versionID 重复写入幂等；重放时发现有 VERSION_ID 记录，跳过 Raft propose，直接用记录的 versionID 继续后续存储写入
- `WriteCommit`：存储层写入完成标记，含 versionID

**崩溃恢复逻辑**：
- 有 BEGIN 但无 VERSION_ID：Raft apply 未完成，状态机里没有对应版本，重新 propose 即可
- 有 VERSION_ID 但无 COMMIT：跳过 Raft propose，使用 WAL 记录的 versionID 从头重放存储写入，每步幂等可安全重放
- 有 COMMIT 但版本仍为 PENDING：检查索引文件是否存在，存在则 propose READY，不存在则触发异步构建

**ReplayCounter**：内存态，进程重启归零；重放失败超过阈值（`wal.replay_retry_threshold`）后标记需人工介入，暴露给 `GetSystemStatus`。

---

### WriteCoordinator

写路径编排接口，承担 `CreateVersion` 的内部编排逻辑。`Service.CreateVersion` 只做参数校验和响应转换，编排逻辑全部交给此接口，依赖通过构造函数注入。

**逻辑链路**：`Service.CreateVersion → WriteCoordinator.Execute`

```go
type WriteCoordinator interface {
    Execute(ctx context.Context, kbID string, parentVersionID int64, changes []DocChange) (int64, error)
}

// 实现
func (c *WriteCoordinatorImpl) Execute(ctx context.Context, kbID string, parentVersionID int64, changes []DocChange) (int64, error)
```

**IO 错误自动重试**：存储层写入遇到非永久性错误时，内部做指数退避重试（次数和间隔见配置 `write_coordinator.max_retries` / `write_coordinator.retry_base_interval_ms`）。重试耗尽后返回错误，调用方重新发起 `CreateVersion` 即可；崩溃恢复由 WAL 自动处理，调用方无需感知 PENDING 状态。

---

### DeleteCoordinator

删除路径编排接口，承担 `DeleteKnowledgeBase` 标记删除后的异步清理编排。

**逻辑链路**：`Service.DeleteKnowledgeBase → [异步] DeleteCoordinator.Execute`

```go
type DeleteCoordinator interface {
    Execute(ctx context.Context, kbID string) error
}

// 实现，持有 IndexManager / DocStore / ChunkStore / ChunkDocMapper /
// VersionDocList / RaftNode / WAL 的接口引用
func (c *DeleteCoordinatorImpl) Execute(ctx context.Context, kbID string) error
```

**幂等保证**：磁盘文件删除忽略 `ErrNotExist`；`ProposeRemoveKBMeta` 收到 `ErrKnowledgeBaseNotFound` 视为成功；其余步骤前缀扫描删除天然幂等。

**永久错误处理**：每步失败后指数退避重试，超过 `delete_coordinator.max_retries` 后调用 `ProposeMarkKBDeleteFailed`，停止重试并暴露给运维。

---

## 内部模块接口（C++）

---

### VectorIndex

向量索引接口，HNSW / IVF / FLAT 各自实现此接口。

**逻辑链路**：
- `IndexManager.TriggerBuild → VectorIndex.Build → VectorIndex.Save`
- `IndexManager.Search → VectorIndex.Load → VectorIndex.Search`
- `IndexManager.EvictByKB → VectorIndex.Reset`

```cpp
class VectorIndex {
public:
    virtual absl::Status Build(const std::vector<ChunkVector>& chunks, MetricType metric) = 0;
    virtual absl::StatusOr<std::vector<SearchResult>> Search(const std::vector<float>& vector, int topK) = 0;
    virtual absl::Status Save(const std::string& path) = 0;
    virtual absl::Status Load(const std::string& path) = 0;
    virtual absl::Status Reset() = 0;
};
```

**说明**：`metric` 由知识库元数据中的 `Similarity` 决定，创建后不可变，默认 COSINE。当前仅 HNSW 有真实实现。IVF / FLAT 留待后续迭代。

---

### ChunkStorage

chunk 持久化存储接口，RocksDB 实现。

**逻辑链路**：
- `IndexManager.TriggerBuild → ChunkStorage.Read`
- `ChunkStore.Write → ChunkStorage.Write`
- `ChunkStore.Exists → ChunkStorage.Exists`
- `ChunkStore.DeleteByKB → ChunkStorage.DeleteByPrefix`

```cpp
class ChunkStorage {
public:
    virtual absl::Status Write(const std::string& key, const std::vector<float>& vector) = 0;
    virtual absl::StatusOr<std::vector<float>> Read(const std::string& key) = 0;
    virtual absl::StatusOr<bool> Exists(const std::string& key) = 0;
    virtual absl::Status Delete(const std::string& key) = 0;
    virtual absl::Status DeleteByPrefix(const std::string& prefix) = 0;
};
```

---

### vecstore 内部 gRPC（vecstore.proto）

Go 侧 `ChunkStore` 和 `IndexManager` 通过内部 gRPC 调用 C++ vecstore。地址见配置文件 `vecstore.grpc_addr`。

```protobuf
syntax = "proto3";
package vecstore;

service ChunkStorageService {
  rpc Write(WriteChunkRequest) returns (WriteChunkResponse);
  rpc Read(ReadChunkRequest) returns (ReadChunkResponse);
  rpc Exists(ExistsChunkRequest) returns (ExistsChunkResponse);
  rpc Delete(DeleteChunkRequest) returns (DeleteChunkResponse);
  rpc DeleteByPrefix(DeleteByPrefixRequest) returns (DeleteByPrefixResponse);
}

service VectorIndexService {
  rpc Build(BuildIndexRequest) returns (BuildIndexResponse);
  rpc Search(SearchIndexRequest) returns (SearchIndexResponse);
  rpc Save(SaveIndexRequest) returns (SaveIndexResponse);
  rpc Load(LoadIndexRequest) returns (LoadIndexResponse);
  rpc Reset(ResetIndexRequest) returns (ResetIndexResponse);
}

message WriteChunkRequest {
  string key = 1;            // 知识库 ID + chunk ID
  repeated float vector = 2;
}
message WriteChunkResponse {}

message ReadChunkRequest { string key = 1; }
message ReadChunkResponse { repeated float vector = 1; }

message ExistsChunkRequest { string key = 1; }
message ExistsChunkResponse { bool exists = 1; }

message DeleteChunkRequest { string key = 1; }
message DeleteChunkResponse {}

message DeleteByPrefixRequest { string prefix = 1; }
message DeleteByPrefixResponse {}

message ChunkVectorProto {
  string chunk_id = 1;
  repeated float vector = 2;
}

enum MetricTypeProto {
  COSINE = 0;
  EUCLIDEAN = 1;
  INNER_PRODUCT = 2;
}

message BuildIndexRequest {
  string kb_id = 1;
  int64 version_id = 2;
  repeated ChunkVectorProto chunks = 3;
  MetricTypeProto metric = 4;
}
message BuildIndexResponse {}

message SearchIndexRequest {
  string kb_id = 1;
  int64 version_id = 2;
  repeated float vector = 3;
  int32 top_k = 4;
}
message SearchResultProto {
  string chunk_id = 1;
  float score = 2;
}
message SearchIndexResponse { repeated SearchResultProto results = 1; }

message SaveIndexRequest { string kb_id = 1; int64 version_id = 2; string path = 3; }
message SaveIndexResponse {}

message LoadIndexRequest { string kb_id = 1; int64 version_id = 2; string path = 3; }
message LoadIndexResponse {}

message ResetIndexRequest { string kb_id = 1; int64 version_id = 2; }
message ResetIndexResponse {}
```

---

## 配置默认值

| 配置项 | 默认值 |
|---|---|
| 索引类型 | HNSW |
| 相似度计算方式 | 余弦相似度 |
| 查询历史审计粒度 | 轻量（入参 + 版本 ID） |
| 指标上报方式 | 拉模式（`/metrics` 端点） |

---

## KnowledgeBaseService

### CreateKnowledgeBase

```go
func (s *Service) CreateKnowledgeBase(ctx context.Context, req *CreateKnowledgeBaseRequest) (*CreateKnowledgeBaseResponse, error)
```

**入参**：
- `name`：知识库名称
- `chunk_window_size`：切割窗口大小
- `chunk_overlap_size`：切割重叠大小
- `index_type`（可选）：索引类型，枚举值 HNSW / IVF / FLAT，默认 HNSW
- `similarity`（可选）：相似度计算方式，枚举值 COSINE / EUCLIDEAN / INNER_PRODUCT，默认 COSINE
- `embed_config`：embed 服务配置，包含服务地址和模型 ID

**返回**：
- `knowledge_base_id`：知识库 ID
- `initial_version_id`：初始版本 ID

---

### DeleteKnowledgeBase

```go
func (s *Service) DeleteKnowledgeBase(ctx context.Context, req *DeleteKnowledgeBaseRequest) error
```

**入参**：`knowledge_base_id`

**返回**：`success`（标记删除后立即返回，后台异步清理）

---

### CreateVersion

```go
func (s *Service) CreateVersion(ctx context.Context, req *CreateVersionRequest) (*CreateVersionResponse, error)
```

**入参**：
- `knowledge_base_id`：知识库 ID
- `parent_version_id`：父版本 ID
- `changes`：文档变更列表，每条记录包含：
  - `op`：操作类型，枚举值 ADD / DELETE / UPDATE
  - `doc_id`：文档 ID
  - `content`（ADD / UPDATE 时必填）：文档原始文本

**返回**：
- `version_id`：新版本 ID（PENDING 状态，索引异步构建中）

---

### ListVersions

```go
func (s *Service) ListVersions(ctx context.Context, req *ListVersionsRequest) (*ListVersionsResponse, error)
```

**入参**：`knowledge_base_id`

**返回**：版本列表，每条记录包含 `version_id`、`parent_version_id`、`created_at`、`index_status`（PENDING / READY / FAILED）

---

### RollbackVersion

```go
func (s *Service) RollbackVersion(ctx context.Context, req *RollbackVersionRequest) error
```

**入参**：`knowledge_base_id`、`target_version_id`

**返回**：`success`

---

### GetVersion `[预留]`
### DiffVersions `[预留]`
### TagVersion `[预留]`

---

## QueryService

### Query

```go
func (s *Service) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error)
```

**入参**：
- `knowledge_base_id`：知识库 ID
- `version_id`（可选）：不传时使用活跃版本
- `vector`：查询向量，由调用方使用 embed 模型生成
- `top_k`：返回文档数量上限
- `threshold`（可选）：相似度阈值
- `aggregation`（可选）：MAX / MEDIAN / MEAN，默认 MEDIAN

**返回**：
- `results`：文档列表（`doc_id`、`content`、`score`）
- `version_id`：实际查询使用的版本 ID

---

### BatchQuery `[预留]`
### HybridQuery `[预留]`

---

## AdminService

### HealthCheck

```go
func (s *Service) HealthCheck(ctx context.Context) (*HealthCheckResponse, error)
```

**返回**：`status`（HEALTHY / DEGRADED / UNHEALTHY）、`details`

---

### GetSystemStatus

```go
func (s *Service) GetSystemStatus(ctx context.Context) (*GetSystemStatusResponse, error)
```

**返回**：
- `health`：三态汇总
- `stuck_versions`：长期 FAILED 的版本列表（`kb_id`、`version_id`、`index_status`、`updated_at`）
- `delete_failed_kbs`：DeleteFailed 状态的知识库列表
- `wal_alerts`：WAL 重放计数超阈值的记录列表
- `resource_usage`：资源使用快照

---

### RebuildIndex

触发后调用方通过轮询 `GetSystemStatus` 或 `ListVersions` 的 `index_status` 感知结果，不提供推送机制。

```go
func (s *Service) RebuildIndex(ctx context.Context, req *RebuildIndexRequest) error
```

**入参**：`knowledge_base_id`、`version_id`

**返回**：`success`（异步执行，不等待构建完成）

---

### WarmupVersion

预热指定版本的索引，不切换活跃版本。判断顺序：先检查磁盘索引文件是否存在，文件存在且状态为 READY 则直接返回；否则触发构建。

```go
func (s *Service) WarmupVersion(ctx context.Context, req *WarmupVersionRequest) error
```

**入参**：`knowledge_base_id`、`version_id`

**返回**：`success`（异步执行）

---

### GetMetrics `[预留]`
### GetAuditLog `[预留]`
