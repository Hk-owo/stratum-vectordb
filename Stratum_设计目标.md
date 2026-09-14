# Stratum 设计目标文档

> 本文档定义 Stratum 的功能完整性目标、性能指标和验收标准。每个目标有对应的验收测试，测试分批次执行，在小范围尽早发现问题。

---

## 功能目标

### F1 知识库生命周期管理

| 目标 | 标准 |
|---|---|
| 创建知识库 | 指定 embed 配置、切割参数、索引类型，返回知识库 ID |
| 删除知识库 | 标记删除后异步清理所有存储层数据，最终彻底移除元数据 |
| 删除崩溃恢复 | 进程在删除任意步骤崩溃重启后，可自动续传，最终完成清理 |

### F2 版本管理

| 目标 | 标准 |
|---|---|
| 创建版本 | 一次调用包含任意数量文档变更，产生新版本，索引异步构建 |
| 版本链约束 | 父版本必须属于同一知识库且不能是 PENDING 状态，允许分叉 |
| 回滚版本 | 切换活跃版本不停服，正在进行的查询不中断 |
| 版本列表 | 返回完整版本链，含状态和创建时间 |

### F3 写入正确性

| 目标 | 标准 |
|---|---|
| 文档切割 | 滑动窗口切割，chunk 边界正确，文档短于窗口时整体作为一个 chunk |
| chunk 去重 | 同一知识库相同内容 + 相同 embed 配置的 chunk 只存一份 |
| MVCC 正确性 | 读取指定版本的文档，返回该版本时刻的内容，不受后续变更影响 |
| 版本文档列表正确性 | 每个版本的全量文档 ID 集合正确（继承父版本 + 应用变更） |

### F4 崩溃一致性

| 目标 | 标准 |
|---|---|
| BEGIN 无 VERSION_ID | 重启后重新 propose，不产生孤儿版本 |
| VERSION_ID 无 COMMIT | 重启后用记录的 versionID 幂等续传存储写入 |
| COMMIT 后未构建索引 | 重启后自动触发索引构建或 propose READY |
| WAL apply 顺序 | Raft apply 阶段先写 WAL VERSION_ID 再更新状态机，消除孤儿版本 |

### F5 查询正确性

| 目标 | 标准 |
|---|---|
| 向量查询 | 返回目标版本内 top_k 篇文档，相似度分数正确 |
| 版本隔离 | 不同版本查询结果互不干扰 |
| 活跃版本默认查询 | 不传 version_id 时查询活跃版本 |
| 相似度阈值过滤 | 低于阈值的结果不返回 |
| chunk 聚合 | 同一文档多 chunk 命中时按指定方式聚合，正确去重 |

### F6 Raft 强一致

| 目标 | 标准 |
|---|---|
| 版本元数据强一致 | 多副本下读取版本状态一致，无脏读 |
| leader 切换 | 切换后元数据不丢失，服务恢复正常 |
| 少数派故障 | 3 节点集群容忍 1 节点故障继续提供服务 |

### F7 运维接口

| 目标 | 标准 |
|---|---|
| HealthCheck | 返回三态（HEALTHY / DEGRADED / UNHEALTHY），低延迟 |
| GetSystemStatus | 暴露 FAILED 版本、DeleteFailed 知识库、WAL 告警、资源使用 |
| RebuildIndex | 手动触发索引重建，异步执行，结果可轮询 |
| WarmupVersion | 预热目标版本索引，不切换活跃版本 |

---

## 性能目标

### 写入性能

| 指标 | 目标值 | 测试条件 |
|---|---|---|
| CreateVersion 端到端延迟（P99） | < 30s | 单次 1000 篇文档，含 embed 调用，不含索引构建 |
| 索引构建时间 | < 5min | 单知识库 10 万 chunk，768 维向量，HNSW |
| Raft propose 延迟（P99） | < 500ms | 3 节点集群，局域网 |

### 查询性能

| 指标 | 目标值 | 测试条件 |
|---|---|---|
| 查询延迟 P50 | < 50ms | 索引已在内存，top_k=10 |
| 查询延迟 P99 | < 200ms | 索引已在内存，top_k=10 |
| 查询延迟 P99（高并发） | < 500ms | 100 QPS 并发，索引已在内存 |
| 冷加载延迟（索引不在内存） | < 10s | 单版本索引 10 万 chunk |

### 可用性

| 指标 | 目标值 |
|---|---|
| leader 切换后恢复服务时间 | < 10s |
| 崩溃恢复（WAL 重放）完成时间 | < 30s |
| 删除知识库异步清理完成时间 | < 60s（单知识库 10 万 chunk） |

### 存储效率

| 指标 | 目标值 | 测试条件 |
|---|---|---|
| 单知识库存储占用 | < 10GB | 100 万 chunk，768 维向量 |
| 版本存储放大系数 | < 1.1x | 每版本变更 1% 文档，10 个版本 |

---

## 验收测试分批次计划

测试分四批，每批在前一批通过后执行，不做一次性全量验收。

---

### 第一批：单模块契约验证

**范围**：单个模块独立验证，mock 所有外部依赖，不启动真实服务。

**目标**：验证每个模块的接口契约正确，边界条件处理正确。

| 测试项 | 验收标准 |
|---|---|
| DocStore MVCC 读写 | 写入多版本后，按版本 ID 读取返回正确内容；墓碑标记正确处理 |
| ChunkDocMapper 双向映射 | 正向/反向写入后，前缀扫描返回正确结果；幂等写入不产生重复 |
| VersionDocList 全量文档 | 从父版本复制 + 应用变更后，列表内容正确 |
| BloomFilter 假阳性率 | 插入 10 万 key 后，假阳性率 ≤ 配置值（0.01） |
| ChunkSplitter 切割正确性 | 窗口/重叠参数正确；短文档整体作为一个 chunk；ChunkID 计算一致 |
| WAL 幂等写入 | WriteVersionID 重复调用同一 versionID 返回成功 |
| WAL 崩溃恢复 | 模拟 BEGIN/VERSION_ID/COMMIT 各阶段截断，Recover 返回正确 PendingRecord |
| 错误码映射 | 每个具名 error 映射到正确的 gRPC status code |

**不覆盖**：跨模块调用、真实 Raft、真实 embed 服务、真实 vecstore。

---

### 第二批：模块间配合验证

**范围**：2-3 个模块联合测试，使用真实实现替换 mock，不启动完整集群。

**目标**：验证模块间接口契约对齐，数据在模块间流转正确。

| 测试项 | 涉及模块 | 验收标准 |
|---|---|---|
| WriteCoordinator 写路径 | WAL + DocStore + ChunkDocMapper + VersionDocList + BloomFilter + mock ChunkStore + mock EmbedClient | 写入后各模块数据一致；WAL COMMIT 记录存在 |
| WriteCoordinator 崩溃恢复 | WAL + 上述模块 | 模拟各阶段崩溃后重启，数据最终一致，无孤儿版本 |
| IndexManager 构建 | IndexManager + mock ChunkStore + VersionDocList + ChunkDocMapper | TriggerBuild 后索引可查询，回调触发正确 |
| Query 读路径 | IndexManager + ChunkDocMapper + BloomFilter + VersionDocList + DocStore | 写入后查询返回正确文档，版本隔离正确 |
| DeleteCoordinator 清理 | DeleteCoordinator + DocStore + ChunkDocMapper + VersionDocList + mock ChunkStore + mock IndexManager | 清理后各模块数据归零；崩溃恢复后续传完成 |

**性能验收**（本批次开始采集）：
- WriteCoordinator 写入 100 篇文档，端到端延迟 P99 < 5s（mock embed，不含真实网络）

---

### 第三批：单节点集成验证

**范围**：启动单节点完整服务（含真实 Raft 单节点、真实 vecstore、mock embed），通过 gRPC 接口调用。

**目标**：验证完整链路正确，功能目标 F1-F7 全部可覆盖。

| 测试项 | 验收标准 |
|---|---|
| CreateKnowledgeBase + CreateVersion + Query 完整链路 | 写入文档后查询返回正确结果 |
| RollbackVersion 不停服 | 回滚期间并发查询不中断，切换后查询打到目标版本 |
| 版本链分叉 | 同一父版本创建两个子版本，各自查询结果互不干扰 |
| DeleteKnowledgeBase 完整清理 | 删除后所有存储层数据归零，GetSystemStatus 无残留 |
| HealthCheck / GetSystemStatus | 返回正确状态，FAILED 版本和 WAL 告警正确暴露 |
| RebuildIndex | 触发后状态从 FAILED 转 READY，可查询 |
| WarmupVersion | 预热后索引在内存，查询延迟符合内存命中标准 |
| 崩溃恢复全链路 | 在写路径各阶段强制 kill 进程，重启后数据一致 |

**性能验收**：
- 查询延迟 P50 < 50ms，P99 < 200ms（索引在内存，top_k=10）
- CreateVersion 1000 篇文档 P99 < 30s（含 mock embed）
- HealthCheck 延迟 < 100ms

---

### 第四批：三节点集群验证

**范围**：Docker Compose 启动三节点集群，含真实 Raft、真实 vecstore、mock embed，通过 gRPC 接口调用。

**目标**：验证分布式场景下的正确性、容错和性能。

| 测试项 | 验收标准 |
|---|---|
| 多副本一致性 | 写入后从任意节点查询结果一致 |
| leader 切换 | 停止 leader 节点，10s 内新 leader 选出，服务恢复 |
| 少数派故障 | 停止 1 个 follower，写入和查询继续正常 |
| 并发写入 | 多个 CreateVersion 并发提交，版本 ID 单调递增，无冲突 |
| 并发查询 | 100 QPS 并发查询，P99 < 500ms |
| 索引存储占用 | 100 万 chunk（768 维），存储占用 < 10GB |
| 版本存储放大 | 10 个版本每版本变更 1% 文档，放大系数 < 1.1x |
| 崩溃恢复（集群） | 任意节点 kill 重启后，集群数据一致，WAL 重放完成 < 30s |

**性能验收**：
- 所有第三批性能指标在三节点集群下同样满足
- leader 切换恢复时间 < 10s
- 索引构建 10 万 chunk < 5min
