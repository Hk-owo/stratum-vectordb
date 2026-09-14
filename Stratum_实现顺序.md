# Stratum 实现顺序文档

> 本文档定义各模块的实现顺序、依赖关系和阶段性测试节点。每个阶段完成后执行对应的批量测试，不等全部实现完再验收。

---

## 实现原则

- 按依赖关系从底向上实现，被依赖的模块先实现
- 每个模块实现前先定义接口，其他模块可以用 mock 继续并行
- 每个阶段结束有明确的测试节点，测试通过后进入下一阶段
- 并行任务在同一阶段内可以同时进行，互不阻塞

---

## 阶段划分

### 阶段 0：接口定义与项目骨架（并行，1 人）

不写任何业务逻辑，只建立项目结构和接口定义，为后续并行开发提供契约。

**任务**：
- 初始化 Go module，建立 `internal/` 包结构
- 定义所有 Go 接口：`DocStore` / `ChunkStore` / `ChunkDocMapper` / `VersionDocList` / `BloomFilter` / `ChunkSplitter` / `EmbedClient` / `IndexManager` / `RaftNode` / `WAL` / `WriteCoordinator` / `DeleteCoordinator`
- 定义所有数据类型：`DocChange` / `Chunk` / `VersionMeta` / `KnowledgeBaseMeta` / `IndexStatus` / `KBStatus` / `EmbedConfig` / `PendingRecord` / `ReplayCounter` / `SearchResult`
- 定义具名错误类型和 `ToGRPCStatus` 映射表（`internal/errors`）
- 初始化 C++ vecstore 目录结构，定义 `VectorIndex` 和 `ChunkStorage` 纯虚类头文件
- 编写 `vecstore.proto` 和三个对外 proto 文件（knowledgebase / query / admin）
- 生成 proto 代码
- 编写每个接口的 mock 实现（`internal/*/mock.go`），供其他模块测试使用

**产出**：所有接口定义、mock 实现、proto 生成代码、项目骨架

**测试节点**：无（仅编译通过即可）

---

### 阶段 1：存储层基础模块（可并行，2 人）

实现独立的存储模块，互不依赖，可以完全并行。

#### 任务 A（1 人）：Go 侧 PebbleDB 存储模块

按以下顺序实现，每个模块独立可测：

**1-A-1 DocStore**
- PebbleDB 实现，key = `kbID + docID + versionID`
- `Write`：写入文档内容或墓碑
- `ReadAt`：前缀扫描取 versionID ≤ maxVersionID 的最大条目
- `DeleteByKB`：前缀扫描清理

**1-A-2 ChunkDocMapper**
- PebbleDB 实现，正向 + 反向双 key
- `Write`：同时写正向和反向
- `ListDocIDs`：正向前缀扫描
- `ListChunkIDsByDocs`：对 docIDs 逐个反向前缀扫描，合并去重
- `DeleteByKB` / `DeleteByDoc`

**1-A-3 VersionDocList**
- PebbleDB 实现，key = `kbID + versionID + docID`
- `Write`：写入单条记录
- `ListDocIDs`：前缀扫描
- `DeleteByVersion` / `DeleteByKB`

**1-A-4 BloomFilter**
- `bits-and-blooms/bloom` 实现
- `Add` / `Test` / `Serialize` / `Deserialize` / `Reset`

**1-A-5 ChunkSplitter**
- 滑动窗口实现
- `Split`：切割并计算 `ChunkID = SHA-256(chunk文本 + embedConfigID)`
- 短文档处理：长度 < windowSize 时整体作为一个 chunk

**测试节点（对应第一批验收测试 A 部分）**：完成 1-A-1 到 1-A-5 后执行：
- DocStore MVCC 读写正确性
- ChunkDocMapper 双向映射正确性
- VersionDocList 全量文档正确性
- BloomFilter 假阳性率
- ChunkSplitter 切割正确性和 ChunkID 一致性

#### 任务 B（1 人）：C++ vecstore 存储模块

**1-B-1 ChunkStorage（RocksDB）**
- `Write` / `Read` / `Exists` / `Delete` / `DeleteByPrefix`
- key = `kbID + chunkID`

**1-B-2 VectorIndex（HNSW）**
- Faiss HNSW 实现
- `Build`：批量输入 ChunkVector，构建 HNSW 图
- `Search`：向量近似搜索，返回 top_k 结果
- `Save` / `Load` / `Reset`

**1-B-3 vecstore gRPC 服务**
- 实现 `ChunkStorageService` 和 `VectorIndexService`
- 接入 ChunkStorage 和 VectorIndex 实现
- 启动 gRPC server

**测试节点（对应第一批验收测试 B 部分）**：完成 1-B-1 到 1-B-3 后执行：
- ChunkStorage 写入/读取/删除/前缀删除正确性
- VectorIndex 构建后搜索召回率（与暴力搜索对比，召回率 > 95%）
- gRPC 接口正常通信

---

### 阶段 2：WAL 和 ChunkStore 客户端（可并行，2 人）

依赖阶段 0 的接口定义，不依赖阶段 1 的具体实现（可用 mock）。

#### 任务 A（1 人）：WAL

**2-A WAL 实现**
- `WriteBegin`：写 BEGIN 记录
- `WriteVersionID`：写 VERSION_ID 记录，幂等（同一 versionID 重复写入返回成功）
- `WriteCommit`：写 COMMIT 记录（含 versionID）
- `WriteDeleteMark` / `WriteDeleteComplete`
- `Recover`：启动时扫描 WAL，返回 `[]PendingRecord`
- `GetReplayCounters`：内存态重放失败计数

**测试节点（对应第一批验收测试 WAL 部分）**：
- WriteVersionID 幂等写入
- 各阶段截断后 Recover 返回正确 PendingRecord
- BEGIN 无 VERSION_ID → 重新 propose 路径
- VERSION_ID 无 COMMIT → 续传路径
- COMMIT 存在 → 触发索引构建路径

#### 任务 B（1 人）：ChunkStore（vecstore gRPC 客户端）

**2-B ChunkStore 实现**
- vecstore gRPC 客户端封装
- `Write` / `Exists` / `Delete` / `DeleteByKB`
- 连接管理、重试、超时

**测试节点**：
- 连接真实 vecstore gRPC server（阶段 1-B-3）
- Write + Exists + Delete 正确性
- DeleteByKB 前缀删除正确性

---

### 阶段 3：Raft 状态机（1 人）

依赖现有 kvserver Raft，扩展状态机支持知识库和版本元数据。

**3 RaftNode 实现**
- `ProposeCreateKB` / `ProposeMarkKBDeleting` / `ProposeMarkKBDeleteFailed` / `ProposeRemoveKBMeta`（幂等，NotFound 视为成功）
- `ProposeCreateVersion`：apply 阶段先调 `WAL.WriteVersionID(versionID)`（幂等），再分配版本 ID 写状态机；校验父版本约束（同知识库，不能是 PENDING，允许分叉）
- `ProposeUpdateVersionStatus`
- `ProposeRollback`
- `GetKB` / `ListVersions` / `GetClusterStatus`

**测试节点（对应第一批验收测试 Raft 部分）**：
- ProposeCreateKB + GetKB 正确性
- ProposeCreateVersion 版本 ID 单调递增
- 父版本约束校验（PENDING 父版本拒绝，跨知识库拒绝，分叉允许）
- ProposeRemoveKBMeta 幂等（NotFound 返回成功）
- 单节点 leader 切换后元数据不丢失

---

### 阶段 4：EmbedClient 和 IndexManager（可并行，2 人）

#### 任务 A（1 人）：EmbedClient

**4-A EmbedClient 实现**
- HTTP 客户端，调用外部 embed 服务
- `Embed(chunks []Chunk) (map[string][]float32, error)`
- 超时、重试

**测试节点**：
- 连接 mock embed HTTP server，验证请求格式和响应解析

#### 任务 B（1 人）：IndexManager

**4-B IndexManager 实现**
- LRU 换入换出，引用计数
- `TriggerBuild`：异步构建，VersionDocList → ChunkDocMapper.ListChunkIDsByDocs → ChunkStore 批量读向量 → VectorIndex.Build → Save
- `Search`：加载索引（LRU），引用计数 +1，搜索，引用计数 -1
- 并发加载等待（`load_wait_timeout_ms`，同时监听 `ctx.Done()`）
- `RegisterBuildCallback`：构建完成后回调，指数退避重试
- `Evict` / `EvictByKB` / `Ping`

**测试节点（对应第二批验收测试 IndexManager 部分）**：
- TriggerBuild 后 Search 返回正确结果（使用真实 ChunkStore + 真实 VectorIndex）
- LRU 换出：超过 lru_capacity 后最久未访问的索引被换出
- 引用计数保护：查询进行中的索引不被换出
- 并发加载：多个并发请求只触发一次构建
- 构建失败回调：回调失败重试，重试耗尽后状态正确

---

### 阶段 5：Coordinator 层（可并行，2 人）

依赖阶段 1-4 的全部模块，此阶段开始集成真实实现。

#### 任务 A（1 人）：WriteCoordinator

**5-A WriteCoordinator 实现**
- 编排 CreateVersion 完整写路径：
  1. WAL.WriteBegin
  2. RaftNode.ProposeCreateVersion（内部先写 WAL.WriteVersionID）
  3. 对每个变更文档：ChunkSplitter.Split → EmbedClient.Embed → 对每个 chunk：BloomFilter.Test → ChunkStore.Exists → ChunkStore.Write + BloomFilter.Add → ChunkDocMapper.Write → DocStore.Write
  4. VersionDocList.Write（从父版本扫描全量，应用变更）
  5. BloomFilter.Serialize
  6. WAL.WriteCommit
  7. IndexManager.TriggerBuild（异步）
- IO 错误指数退避重试

**测试节点（对应第二批验收测试 WriteCoordinator 部分）**：
- 完整写路径：写入后各模块数据一致，WAL COMMIT 存在
- 各阶段崩溃恢复：数据最终一致，无孤儿版本
- 写入 100 篇文档 P99 < 5s（mock embed）

#### 任务 B（1 人）：DeleteCoordinator

**5-B DeleteCoordinator 实现**
- 编排 DeleteKnowledgeBase 异步清理：
  1. IndexManager.EvictByKB
  2. 清理磁盘索引文件（ErrNotExist 忽略）
  3. DocStore.DeleteByKB
  4. ChunkStore.DeleteByKB
  5. ChunkDocMapper.DeleteByKB
  6. VersionDocList.DeleteByKB
  7. RaftNode.ProposeRemoveKBMeta（ErrKnowledgeBaseNotFound 视为成功）
  8. WAL.WriteDeleteComplete
- 每步失败指数退避重试，耗尽后调 ProposeMarkKBDeleteFailed

**测试节点（对应第二批验收测试 DeleteCoordinator 部分）**：
- 完整删除后各模块数据归零
- 各步骤崩溃恢复：重启后续传，最终完成清理
- ProposeRemoveKBMeta 幂等（已删再删不报错）

---

### 阶段 6：Service 层（1 人）

依赖阶段 5，接入所有 Coordinator 和 RaftNode，实现对外 gRPC 接口。

**6 Service 实现**
- `KnowledgeBaseService`：CreateKnowledgeBase / DeleteKnowledgeBase / CreateVersion / ListVersions / RollbackVersion
- `QueryService`：Query（含版本状态检查，ErrVersionPending / ErrVersionFailed 区分）
- `AdminService`：HealthCheck / GetSystemStatus / RebuildIndex / WarmupVersion
- 统一通过 `ToGRPCStatus` 转换错误码
- 配置文件加载（YAML，`${data_dir}` 变量替换）
- 启动入口 `cmd/stratum/main.go`

**测试节点（对应第三批验收测试）**：
- 单节点完整链路：CreateKnowledgeBase + CreateVersion + Query
- RollbackVersion 不停服（并发查询不中断）
- 版本链分叉：两个子版本查询结果互不干扰
- 崩溃恢复全链路（各阶段 kill 进程后重启）
- HealthCheck / GetSystemStatus 正确性
- 查询性能：P50 < 50ms，P99 < 200ms（索引在内存）
- CreateVersion 1000 篇文档 P99 < 30s

---

### 阶段 7：三节点集群与集成测试（1 人）

**7 集群部署与集成测试**
- 编写 `docker-compose.yml`（三节点 stratum + vecstore + mock embed）
- 配置文件：`config1.yaml` / `config2.yaml` / `config3.yaml`
- 集成测试：`integration/write_read_test.go` / `rollback_test.go` / `fault_injection_test.go`

**测试节点（对应第四批验收测试）**：
- 多副本一致性
- leader 切换恢复 < 10s
- 少数派故障（1 节点宕机）下服务继续
- 100 QPS 并发查询 P99 < 500ms
- 存储占用 100 万 chunk < 10GB
- 版本存储放大 < 1.1x
- 崩溃恢复 WAL 重放 < 30s

---

## 并行任务汇总

| 阶段 | 并行任务 | 最少人数 |
|---|---|---|
| 阶段 0 | 接口定义（单人，其他等待） | 1 |
| 阶段 1 | 1-A（Go 存储）+ 1-B（C++ vecstore）并行 | 2 |
| 阶段 2 | 2-A（WAL）+ 2-B（ChunkStore 客户端）并行 | 2 |
| 阶段 3 | RaftNode（依赖 WAL，串行） | 1 |
| 阶段 4 | 4-A（EmbedClient）+ 4-B（IndexManager）并行 | 2 |
| 阶段 5 | 5-A（WriteCoordinator）+ 5-B（DeleteCoordinator）并行 | 2 |
| 阶段 6 | Service 层（串行） | 1 |
| 阶段 7 | 集群集成测试（串行） | 1 |

**最大并行人数**：2 人（阶段 1/2/4/5 均可双线并行）

**单人串行估算顺序**：0 → 1-A → 1-B → 2-A → 2-B → 3 → 4-A → 4-B → 5-A → 5-B → 6 → 7

---

## 阶段依赖图

```
阶段 0（接口 + mock）
    ↓
阶段 1-A（Go 存储）    阶段 1-B（C++ vecstore）
    ↓                       ↓
阶段 2-A（WAL）        阶段 2-B（ChunkStore 客户端）
    ↓                       ↓
阶段 3（RaftNode，依赖 WAL）
    ↓
阶段 4-A（EmbedClient）    阶段 4-B（IndexManager）
    ↓                           ↓
阶段 5-A（WriteCoordinator）    阶段 5-B（DeleteCoordinator）
    ↓                               ↓
阶段 6（Service 层）
    ↓
阶段 7（集群集成测试）
```
