# Stratum — 进度包（阶段 0-7 全部完成）

## 当前状态

- **阶段 0（接口定义与项目骨架）：已完成**
- **阶段 1（存储层基础模块）：已完成**
- **阶段 2（WAL + ChunkStore 客户端）：已完成**
- **阶段 3（Raft 状态机）：已完成**
- **阶段 4-A（EmbedClient HTTP 实现）：已完成**
- **阶段 4-B（IndexManager 实现）：已完成**
- **阶段 5-A（WriteCoordinator 实现）：已完成**
- **阶段 5-B（DeleteCoordinator 实现）：已完成**
- **阶段 6（Service 层 + main.go + configs）：已完成**
- **阶段 7（集成测试）：已完成**

---

## 验证方式

### Go 侧
```bash
go build ./...
go vet ./...
go test ./... -timeout 180s -count=1     # 17 个有测试的包，全部 PASS
go test -race ./... -timeout 180s          # 无数据竞争
```

### C++ 侧
```bash
# 先按 vecstore/CMakeLists.txt 说明编译 Faiss（同级目录）
mkdir build && cd build
cmake .. -DFAISS_SOURCE_DIR=/path/to/faiss -DFAISS_BUILD_DIR=/path/to/faiss/build
make -j$(nproc)
ctest --output-on-failure    # 18 个测试全部 PASS
```

### 3 节点集群测试
```bash
# 进程内（无需 Docker）
go test ./integration/... -run TestMultiNode -v

# Docker（需要 Docker 环境）
cd integration/docker
docker compose up -d --wait
go test ./integration/docker/... -tags=docker -v
docker compose down
```

---

## 阶段 4-6 新增内容

### Phase 4-A: EmbedClient
```
internal/embed/
├── embed.go               # EmbedClient 接口（已有）
├── mock.go                # MockEmbedClient（已有）
├── http_client.go          # HTTPEmbedClient 实现
└── http_client_test.go     # 6 个测试
```

### Phase 4-B: IndexManager
```
internal/index/
├── index.go               # IndexManager 接口（已有）
├── mock.go                # MockIndexManager（已有）
├── impl.go                 # IndexManagerImpl（LRU + 引用计数 + 并发加载）
└── index_test.go           # 11 个测试
```

### Phase 5-A: WriteCoordinator
```
internal/coordinator/
├── write.go                # WriteCoordinator 接口（已有）
├── mock.go                 # MockWriteCoordinator（已有）
├── write_impl.go            # WriteCoordinatorImpl
└── write_test.go            # 8 个测试（ADD/DELETE/UPDATE/去重/WAL/崩溃恢复）
```

### Phase 5-B: DeleteCoordinator
```
internal/coordinator/
├── delete.go               # DeleteCoordinator 接口（已有）
├── delete_impl.go           # DeleteCoordinatorImpl（幂等 + 崩溃恢复）
└── delete_test.go           # 6 个测试
```

### Phase 6: Service 层
```
service/
├── knowledgebase.go         # KnowledgeBaseService 实现
├── knowledgebase_test.go    # 7 个测试
├── query.go                 # QueryService 实现（聚合/阈值/版本隔离）
├── query_test.go            # 8 个测试
├── admin.go                 # AdminService 实现
cmd/stratum/main.go          # 启动入口
configs/config1.yaml         # 单节点配置
```

### Phase 7: 集成测试
```
integration/
├── integration_test.go      # 15 个单节点集成测试
├── cluster_test.go          # 3 节点集群测试
└── docker/                  # Docker 3 节点配置
    ├── docker-compose.yml
    ├── Dockerfile
    ├── mock_embed_server.go
    ├── docker_test.go
    └── config*.yaml
```

---

## 阶段 3 修复的真实 Bug（TDD 过程中捕获，原 KVServer 中均存在）

| # | Bug | 影响 | 修复方式 |
|---|---|---|---|
| 1 | 单节点集群永远选不出 leader | 多数票判断只在 peer 响应 handler 里，0 个 peer 时永不触发 | `SendVoteRequest` 在广播前立即用自投票检查是否已达多数 |
| 2 | `AppendEntries` 心跳路径跳过日志一致性检查 | follower 可能在日志分叉的情况下错误推进 commitIndex | 心跳和带日志条目的两条路径合并，统一走一致性检查 |
| 3 | `InstallSnapshot` 持锁发送 channel 消息 | 潜在死锁：若 channel 满而消费方同时需要锁 | 释锁后再发送 |
| 4 | `RequestVote` 未检查 `killed()` | Stop() 期间的节点仍可投票，导致少数派错误当选 | 在函数入口加 `killed()` 检查 |
| **5** | **无 no-op-on-election 提案** | **新当选 leader 永远无法使旧日志条目对状态机可见** | **当选时立即追加一条空 Data 的日志条目** |
| **6** | **RaftNodeImpl 把自己注册为 peer，leader 被自己心跳踢下台** | **3 节点集群永远选不出稳定 leader** | **AddPeer 时 skip self** |

Bug 6 是集成测试阶段发现的——所有节点 `GetClusterStatus` 都报告有 leader，但 `Propose` 始终返回 `ErrNotLeader`。根因是 leader 通过 transport 给自己发心跳时，`AppendEntries` handler 无条件将 `state` 设为 `Follower`。

---

## 关键设计决策

- **proposeAndWait**：`kvraft.Raft.Propose` 非阻塞，返回分配的 (index, term)；`RaftNodeImpl` 用 pending map（以 index 为键）挂等待，apply 分发循环收到对应 ApplyMsg 时校验 term 后唤醒调用方。
- **WAL-before-state-machine**：`applyCreateVersion` 在把新版本写入内存状态机之前先调 `WAL.WriteVersionID(versionID)`，所有节点都走这条路径。
- **命令编码用 JSON**：Raft 日志的控制面命令量小、频率低，JSON 可直接 `jq` 调试。
- **chunk 内容寻址**：ChunkID = SHA-256(chunk 文本 + embed 配置 ID)，天然去重。
- **Cos = L2 归一化 + 内积**：Faiss 不原生支持余弦，通过归一化后用内积等价计算。
- **统一"越高越相似"**：三种距离度量统一为分数越高越相似。

---

## 当前测试总览

```
Go:   17 个有测试的包，全部 PASS
      - internal/kvraft:      15 个测试（9 单节点 + 6 集群），-race 稳定
      - internal/raft:        11 个测试，-race 稳定
      - internal/coordinator: 17 个测试（Write + Delete + Mock）
      - internal/index:       21 个测试（Impl 11 + Mock 10）
      - internal/embed:       11 个测试（HTTP 6 + Mock 5）
      - service:              15 个测试（KB 7 + Query 8）
      - integration:          15 个测试 + 1 个 3 节点集群测试
      其余各存储模块同上一版

C++:  18 个 GoogleTest 用例，全部 PASS
```

---

## 已知遗留问题

| 问题 | 说明 |
|---|---|
| `go.mod` 中的 `replace` 指令 | 沙箱网络限制，非设计选择 |
| Faiss 编译产物不在仓库 | 需自行按 vecstore/CMakeLists.txt 说明编译 |
| `health_service.cpp` 未实现 | 实现顺序文档未分配具体阶段 |
| `internal/kvraft` 来源 | 改写自 https://github.com/Hk-owo/KVServer，已修复 6 个真实 bug |
| 生产级 YAML 配置加载 | main.go 目前使用 hardcoded defaults，需接入 YAML 解析 |
| IVF / FLAT 索引 | 接口预留，仅 HNSW 有真实实现 |
| 审计日志 | 接口预留，Query 链路中有占位步骤 |
| GC（孤儿 chunk 清理） | 设计文档中有完整方案，待实现 |
