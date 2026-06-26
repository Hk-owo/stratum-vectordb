# Stratum 测试顺序文档

> 本文档定义各阶段的测试范围、测试方法和验收标准，与实现顺序文档对应。测试分四批执行，每批在小范围内尽早发现问题，不做一次性全量验收。

---

## 测试原则

- 每个模块实现完立即测试，不等其他模块就绪
- 优先用 mock 隔离外部依赖，单独验证被测模块
- 每批测试有明确的通过/失败标准，失败则阻塞下一阶段
- 性能测试从第二批开始采集基准数据，逐批收紧标准

---

## 第一批：单模块契约验证

**触发时机**：阶段 1 各子任务完成后立即执行，不等其他模块  
**环境**：单进程，mock 所有外部依赖，不启动真实服务  
**工具**：Go `testing` 包，table-driven 风格；C++ Google Test

---

### T1-1 DocStore

```
测试文件：internal/docstore/docstore_test.go
```

| 用例 | 输入 | 期望输出 |
|---|---|---|
| 写入后正确读回 | Write(kbID, docID, v=1, "content") → ReadAt(kbID, docID, maxV=1) | "content" |
| MVCC：读取旧版本 | Write(v=1, "v1") + Write(v=2, "v2") → ReadAt(maxV=1) | "v1" |
| MVCC：读取最新版本 | Write(v=1) + Write(v=2) → ReadAt(maxV=5) | "v2" |
| 墓碑标记 | Write(v=1, "content") + Write(v=2, tombstone) → ReadAt(maxV=2) | 空/not found |
| 墓碑后不影响旧版本 | Write(v=1, "content") + Write(v=2, tombstone) → ReadAt(maxV=1) | "content" |
| DeleteByKB 清理 | Write 多条 → DeleteByKB → ReadAt | not found |
| 幂等写入 | Write 同一 key 两次 → ReadAt | 最后一次写入值 |

**通过标准**：所有用例通过，无 panic，边界条件正确处理

---

### T1-2 ChunkDocMapper

```
测试文件：internal/chunkdoc/chunkdoc_test.go
```

| 用例 | 输入 | 期望输出 |
|---|---|---|
| 正向写入后正确读回 | Write(kbID, chunkID, docID) → ListDocIDs(kbID, chunkID) | [docID] |
| 一个 chunk 对应多个 doc | Write(chunk1, doc1) + Write(chunk1, doc2) → ListDocIDs(chunk1) | [doc1, doc2] |
| 反向批量查询 | Write(chunk1, doc1) + Write(chunk2, doc1) → ListChunkIDsByDocs([doc1]) | [chunk1, chunk2] |
| 多文档批量反查去重 | Write(chunk1, doc1) + Write(chunk1, doc2) → ListChunkIDsByDocs([doc1, doc2]) | [chunk1]（去重） |
| 幂等写入 | Write 同一三元组两次 → ListDocIDs | 结果不重复 |
| DeleteByDoc | Write 后 DeleteByDoc → ListDocIDs + ListChunkIDsByDocs | 均为空 |
| DeleteByKB | Write 多条 → DeleteByKB → ListDocIDs | 均为空 |

**通过标准**：所有用例通过，去重正确，双向一致

---

### T1-3 VersionDocList

```
测试文件：internal/versiondoc/versiondoc_test.go
```

| 用例 | 输入 | 期望输出 |
|---|---|---|
| 写入后正确读回 | Write(kbID, v=1, docID) → ListDocIDs(kbID, v=1) | [docID] |
| 版本隔离 | Write(v=1, doc1) + Write(v=2, doc2) → ListDocIDs(v=1) | [doc1] |
| 全量文档集合 | Write(v=2, doc1) + Write(v=2, doc2) → ListDocIDs(v=2) | [doc1, doc2] |
| DeleteByVersion | Write 后 DeleteByVersion → ListDocIDs | 空 |
| DeleteByKB | Write 多版本 → DeleteByKB → 所有版本 ListDocIDs | 均为空 |
| 幂等写入 | Write 同一 (kbID, versionID, docID) 两次 → ListDocIDs | 不重复 |

**通过标准**：所有用例通过，版本隔离正确

---

### T1-4 BloomFilter

```
测试文件：internal/bloom/bloom_test.go
```

| 用例 | 输入 | 期望输出 |
|---|---|---|
| Add 后 Test 命中 | Add("key1") → Test("key1") | true |
| 未 Add 的 key | Test("not_added") | false（概率性，99% 场景下） |
| 假阳性率 | 插入 10 万 key，对 10 万未插入 key 测试 | 假阳性率 ≤ 0.01 |
| Serialize + Deserialize | Add 后 Serialize → 新实例 Deserialize → Test | true |
| Reset 后清空 | Add("key1") → Reset → Test("key1") | false |

**通过标准**：功能用例全部通过，假阳性率满足配置值

---

### T1-5 ChunkSplitter

```
测试文件：internal/splitter/splitter_test.go
```

| 用例 | 输入 | 期望输出 |
|---|---|---|
| 正常切割 | content=1000字，windowSize=200，overlap=50 | chunk 数量正确，边界正确 |
| 短文档整体 chunk | content=50字，windowSize=200 | 1 个 chunk，内容为整篇文档 |
| ChunkID 一致性 | 相同 content + 相同 embedConfigID → Split 两次 | ChunkID 完全相同 |
| 不同 embedConfigID | 相同 content，不同 embedConfigID → Split | ChunkID 不同 |
| 重叠正确 | windowSize=100，overlap=20 → 相邻 chunk | 后一个 chunk 头部含前一个 chunk 尾部 20 字 |
| 空文档 | content="" | 返回空或单个空 chunk（不 panic） |

**通过标准**：所有用例通过，ChunkID 计算公式 `SHA-256(chunk文本 + embedConfigID)` 实现正确

---

### T1-6 WAL

```
测试文件：internal/wal/wal_test.go
```

| 用例 | 输入 | 期望输出 |
|---|---|---|
| WriteVersionID 幂等 | WriteVersionID(v=5) 两次 | 第二次返回成功，不报错 |
| Recover：只有 BEGIN | WriteBegin → 截断 → Recover | PendingRecord 为空（无 VERSION_ID 说明 Raft propose 未完成，由调用方处理） |
| Recover：BEGIN + VERSION_ID | WriteBegin + WriteVersionID(v=5) → 截断 → Recover | 返回需续传的 versionID=5 |
| Recover：完整事务 | WriteBegin + WriteVersionID + WriteCommit → Recover | 无 PendingRecord |
| Recover：DELETE_MARK | WriteDeleteMark(kbID) → 截断 → Recover | PendingRecord{Type: DeleteMark, KBID: kbID} |
| Recover：DELETE_COMPLETE | WriteDeleteMark + WriteDeleteComplete → Recover | 无 PendingRecord |
| ReplayCounter 累计 | 模拟重放失败 3 次 → GetReplayCounters | RetryCount=3 |
| ReplayCounter 不持久化 | 重放失败后重启 → GetReplayCounters | RetryCount=0 |

**通过标准**：所有用例通过，幂等和崩溃场景正确处理

---

### T1-7 错误码映射

```
测试文件：internal/errors/errors_test.go
```

| 用例 | 输入 | 期望 gRPC code |
|---|---|---|
| ErrVersionNotFound | ToGRPCStatus(ErrVersionNotFound) | codes.NotFound |
| ErrVersionPending | ToGRPCStatus(ErrVersionPending) | codes.FailedPrecondition |
| ErrVersionFailed | ToGRPCStatus(ErrVersionFailed) | codes.FailedPrecondition |
| ErrKnowledgeBaseNotFound | ToGRPCStatus(ErrKnowledgeBaseNotFound) | codes.NotFound |
| ErrIndexLoadTimeout | ToGRPCStatus(ErrIndexLoadTimeout) | codes.DeadlineExceeded |
| ErrInvalidParentVersion | ToGRPCStatus(ErrInvalidParentVersion) | codes.InvalidArgument |
| 未知错误 | ToGRPCStatus(errors.New("unknown")) | codes.Internal |
| nil | ToGRPCStatus(nil) | nil |
| Wrap 后的错误 | ToGRPCStatus(fmt.Errorf("wrap: %w", ErrVersionPending)) | codes.FailedPrecondition |

**通过标准**：所有映射正确，`errors.Is` 穿透 wrap 正确

---

### T1-8 C++ ChunkStorage 和 VectorIndex

```
测试文件：vecstore/test/rocksdb_storage_test.cpp
          vecstore/test/hnsw_index_test.cpp
```

| 用例 | 期望输出 |
|---|---|
| ChunkStorage Write + Read | 读回向量与写入一致 |
| ChunkStorage Exists | 写入后 true，删除后 false |
| ChunkStorage DeleteByPrefix | 前缀删除后前缀扫描为空 |
| VectorIndex Build + Search | 搜索结果包含最近邻（与暴力搜索对比召回率 > 95%） |
| VectorIndex Save + Load | Save 后重新 Load，搜索结果一致 |
| VectorIndex Reset | Reset 后 Search 返回空 |

**通过标准**：所有用例通过，HNSW 召回率 > 95%

---

## 第二批：模块间配合验证

**触发时机**：阶段 2-5 各子任务完成后执行  
**环境**：单进程，部分模块用真实实现，部分用 mock；不启动完整集群  
**工具**：Go `testing` 包集成测试，启动真实 PebbleDB 和 vecstore gRPC server

---

### T2-1 WriteCoordinator 写路径

```
测试文件：internal/coordinator/write_test.go
依赖：真实 DocStore + ChunkDocMapper + VersionDocList + BloomFilter + WAL
      mock ChunkStore + mock EmbedClient + mock RaftNode + mock IndexManager
```

| 用例 | 验收标准 |
|---|---|
| 完整写路径 ADD | 写入后 DocStore / ChunkDocMapper / VersionDocList / WAL 数据一致 |
| 完整写路径 DELETE | 写入文档后删除，doc store 有墓碑，VersionDocList 移除该文档 |
| 完整写路径 UPDATE | 更新后 DocStore 有新版本内容，ChunkID 可能变化 |
| chunk 去重 | 同内容文档写两次，ChunkStore.Write 只调一次 |
| BloomFilter 命中 | 写入后再次写相同 chunk，走 Exists 确认路径而非直接写入 |
| WAL COMMIT 存在 | 写入完成后 WAL 有 COMMIT 记录 |
| IndexManager.TriggerBuild 被调用 | 写入完成后 mock IndexManager 收到 TriggerBuild 调用 |

**崩溃恢复用例**：

| 用例 | 方法 | 验收标准 |
|---|---|---|
| BEGIN 无 VERSION_ID | 截断 WAL，Recover 后重新 propose | 新版本写入成功，旧 PENDING 不存在 |
| VERSION_ID 无 COMMIT | 截断 WAL，Recover 后续传 | 使用相同 versionID 续传，数据一致 |
| COMMIT 后未构建 | mock IndexManager 未收到 TriggerBuild → 重启 | 重启后 TriggerBuild 被调用 |

**性能基准**：
- 写入 100 篇文档（mock embed 返回固定向量），端到端 P99 < 5s

---

### T2-2 IndexManager 构建和查询

```
测试文件：internal/index/indexmanager_test.go
依赖：真实 vecstore gRPC server（ChunkStorage + VectorIndex）
      真实 VersionDocList + ChunkDocMapper
      mock RaftNode（构建完成回调）
```

| 用例 | 验收标准 |
|---|---|
| TriggerBuild 后 Search | 写入 chunk 向量后构建，Search 返回最近邻文档 |
| LRU 换出 | 加载超过 lru_capacity 个版本后，最久未访问的被换出 |
| 引用计数保护 | 查询进行中的索引引用计数 > 0，不被换出 |
| 并发加载 | 100 个并发 Search 同一冷版本，只触发一次 Build |
| 构建失败回调 | mock RaftNode.ProposeUpdateVersionStatus 失败，回调重试后成功 |
| 构建失败状态 | 回调重试耗尽后，状态标记为 FAILED |
| Ping | 返回成功，不触发索引加载 |

---

### T2-3 Query 读路径

```
测试文件：internal/query_integration_test.go（或 service/ 下）
依赖：真实 DocStore + ChunkDocMapper + BloomFilter + VersionDocList + IndexManager
      真实 vecstore gRPC server
```

| 用例 | 验收标准 |
|---|---|
| 写入后查询返回正确文档 | Query 返回写入文档的内容，score > 0 |
| 版本隔离 | v1 写 doc1，v2 写 doc2，Query(v=1) 不返回 doc2 |
| 相似度阈值过滤 | threshold=0.9，低分结果不返回 |
| chunk 聚合 | 同一文档多 chunk 命中，按 MEDIAN 聚合后去重，只返回一条 |
| top_k 限制 | top_k=3，最多返回 3 篇文档 |
| 布隆过滤器假阳性处理 | 假阳性 key 经 VersionDocList 确认后正确过滤 |
| 版本 PENDING 状态 | Query(PENDING 版本) → ErrVersionPending |
| 版本 FAILED 状态 | Query(FAILED 版本) → ErrVersionFailed |

---

### T2-4 DeleteCoordinator 清理

```
测试文件：internal/coordinator/delete_test.go
依赖：真实 DocStore + ChunkDocMapper + VersionDocList
      mock ChunkStore + mock IndexManager + mock RaftNode + 真实 WAL
```

| 用例 | 验收标准 |
|---|---|
| 完整清理 | 清理后 DocStore / ChunkDocMapper / VersionDocList 前缀扫描为空 |
| WAL DeleteComplete 存在 | 清理完成后 WAL 有 DeleteComplete 记录 |
| 各步崩溃恢复 | 模拟步骤 3-8 各位置截断，重启后续传，最终清理完成 |
| ProposeRemoveKBMeta 幂等 | mock RaftNode 返回 ErrKnowledgeBaseNotFound，DeleteCoordinator 视为成功 |
| 磁盘文件不存在 | 磁盘索引文件已删，清理步骤忽略 ErrNotExist 继续 |
| 重试耗尽 → DeleteFailed | mock 某步骤持续失败，耗尽 max_retries 后调 ProposeMarkKBDeleteFailed |

---

## 第三批：单节点集成验证

**触发时机**：阶段 6 完成后执行  
**环境**：单节点完整服务（单节点 Raft + 真实 vecstore + mock embed HTTP server），通过 gRPC 接口调用  
**工具**：Go 集成测试，通过 gRPC 客户端调用

---

### T3-1 完整链路正确性

| 用例 | 验收标准 |
|---|---|
| CreateKB + CreateVersion + Query | 写入文档后查询返回正确内容 |
| ListVersions | 返回完整版本列表，状态正确（PENDING → READY） |
| RollbackVersion | 回滚后 Query 使用目标版本，并发查询不中断 |
| 版本链分叉 | 同一父版本两个子版本，各自 Query 结果互不干扰 |
| DeleteKnowledgeBase | 删除后 Query / ListVersions 返回 NotFound |
| 父版本约束 | PENDING 父版本 → ErrInvalidParentVersion |

---

### T3-2 崩溃恢复全链路

| 用例 | 方法 | 验收标准 |
|---|---|---|
| BEGIN 阶段 kill | 写入途中 kill 进程 → 重启 | 重启后自动续传，最终版本 READY |
| VERSION_ID 后 kill | Raft apply 后、存储写入前 kill → 重启 | 重启后用相同 versionID 续传 |
| COMMIT 后 kill | COMMIT 写入后、索引构建前 kill → 重启 | 重启后触发索引构建，版本 READY |
| 删除途中 kill | DeleteKnowledgeBase 清理任意步骤 kill → 重启 | 重启后续传清理，最终彻底清除 |

---

### T3-3 运维接口

| 用例 | 验收标准 |
|---|---|
| HealthCheck 正常 | 返回 HEALTHY |
| GetSystemStatus 暴露 FAILED | 构建失败版本出现在 stuck_versions |
| GetSystemStatus 暴露 DeleteFailed | 删除失败知识库出现在 delete_failed_kbs |
| RebuildIndex | 触发后 ListVersions 最终返回 READY |
| WarmupVersion | 预热后 Query 延迟符合内存命中标准 |

---

### T3-4 单节点性能基准

| 指标 | 目标值 | 测试方法 |
|---|---|---|
| 查询延迟 P50 | < 50ms | 索引在内存，top_k=10，单并发 100 次采样 |
| 查询延迟 P99 | < 200ms | 同上 |
| CreateVersion 1000 篇文档 P99 | < 30s | mock embed 固定延迟 10ms/chunk |
| HealthCheck 延迟 | < 100ms | 单次调用 |
| Raft propose 延迟 P99 | < 500ms | 单节点（无网络） |

---

## 第四批：三节点集群验证

**触发时机**：阶段 7 完成后执行  
**环境**：Docker Compose 三节点集群（三个 stratum 节点 + 三个 vecstore 进程 + mock embed HTTP server）  
**工具**：Go 集成测试，`docker-compose up/down`，`docker kill` 模拟故障

---

### T4-1 分布式正确性

| 用例 | 验收标准 |
|---|---|
| 多副本一致性 | 向 leader 写入，从任意节点查询，结果一致 |
| 并发写入 | 10 个并发 CreateVersion，版本 ID 单调递增，无冲突 |
| leader 切换不丢数据 | kill leader → 等待新 leader 选出 → 查询历史数据完整 |

---

### T4-2 容错

| 用例 | 方法 | 验收标准 |
|---|---|---|
| 少数派故障 | docker kill 1 个 follower → 写入 + 查询 | 服务继续正常 |
| leader 宕机恢复 | docker kill leader → 等待 → docker start | 10s 内新 leader 选出，服务恢复 |
| 节点重启恢复 | docker kill 任意节点 → docker start | 节点追上日志，数据一致 |
| 网络分区（模拟） | iptables 隔离 1 节点 → 写入 → 恢复网络 | 分区节点恢复后数据同步 |

---

### T4-3 集群性能

| 指标 | 目标值 | 测试方法 |
|---|---|---|
| 查询延迟 P50 | < 50ms | 100 QPS 并发，索引在内存 |
| 查询延迟 P99（高并发） | < 500ms | 100 QPS 并发 |
| leader 切换恢复时间 | < 10s | kill leader 到首次成功写入的时间 |
| 索引构建时间 | < 5min | 10 万 chunk，768 维，HNSW |
| WAL 重放完成时间 | < 30s | 写入 1000 篇文档后 kill 重启 |

---

### T4-4 存储效率

| 指标 | 目标值 | 测试方法 |
|---|---|---|
| 单知识库存储占用 | < 10GB | 写入 100 万 chunk，768 维向量，du -sh 采样 |
| 版本存储放大系数 | < 1.1x | 基准版本 10 万文档，10 个版本各变更 1%，对比存储总量与单版本全量备份 |

---

## 测试批次与阶段对应关系

| 测试批次 | 对应实现阶段 | 阻塞关系 |
|---|---|---|
| 第一批 | 阶段 1 完成后 | 第一批通过后才进入阶段 2/3 |
| 第二批 | 阶段 2-5 各子任务完成后 | 第二批通过后才进入阶段 6 |
| 第三批 | 阶段 6 完成后 | 第三批通过后才进入阶段 7 |
| 第四批 | 阶段 7 完成后 | 第四批通过即为项目验收 |

---

## 测试环境配置

### mock embed HTTP server

```go
// 测试用 embed server，固定返回随机向量，延迟可配置
func StartMockEmbedServer(addr string, vectorDim int, latencyMs int) *http.Server
```

### 性能测试工具

```go
// 延迟采样工具，计算 P50/P99
func MeasureLatency(n int, fn func() error) (p50, p99 time.Duration)
```

### Docker Compose 故障注入

```bash
# kill leader
docker kill stratum-node1

# 恢复节点
docker start stratum-node1

# 模拟网络分区（iptables）
docker exec stratum-node3 iptables -A INPUT -s stratum-node1 -j DROP
docker exec stratum-node3 iptables -A INPUT -s stratum-node2 -j DROP
```
