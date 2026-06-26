# Stratum — 分布式向量搜索知识库引擎

面向 RAG 场景的分布式知识库存储系统，提供带版本管理的文档集合、向量索引和相似度查询服务。

## 架构

```
Go 侧（编排 / 元数据 / Raft 共识）    C++ 侧（向量存储 / HNSW 索引）
         │                                        │
         ├─ gRPC API ─────────────────────────    │
         │  KnowledgeBaseService                   │
         │  QueryService                           │
         │  AdminService                           │
         │                                        │
         ├─ Coordinator ──────┐                    │
         │  WriteCoordinator   │                    │
         │  DeleteCoordinator  │                    │
         │                     │                    │
         ├─ Storage ───────────┤   内部 gRPC       │
         │  DocStore (PebbleDB) │◄──────────►  ChunkStorage (RocksDB)
         │  ChunkDocMapper      │               VectorIndex (Faiss HNSW)
         │  VersionDocList      │                    │
         │  BloomFilter         │                    │
         │                     │                    │
         ├─ IndexManager ───────┘                    │
         │                                         │
         └─ Raft (kvraft) ──── 强一致副本 ────      │
```

## 快速开始

```bash
# 编译
go build ./cmd/stratum/

# 测试
go test ./... -timeout 180s -count=1

# Race 检测
go test -race ./internal/kvraft/... ./internal/raft/... ./internal/index/...

# 单节点启动
go run ./cmd/stratum/
```

## 项目结构

```
stratum/
├── api/proto/              # Protobuf 定义
│   ├── knowledgebase.proto # KnowledgeBaseService
│   ├── query.proto         # QueryService
│   ├── admin.proto         # AdminService
│   ├── kvraft/             # Raft RPC（内部）
│   └── vecstore/           # Go↔C++ 内部 gRPC
│
├── internal/
│   ├── types/              # 共享数据类型
│   ├── errors/             # 错误码映射 (→ gRPC status)
│   ├── pebbleutil/         # PebbleDB key 编码
│   ├── docstore/           # MVCC 文档存储
│   ├── chunkdoc/           # chunk↔doc 双向映射
│   ├── versiondoc/         # 版本文档列表
│   ├── bloom/              # 布隆过滤器
│   ├── splitter/           # 滑动窗口文档切割
│   ├── embed/              # 外部 Embed 服务客户端
│   ├── chunkstore/         # vecstore gRPC 客户端
│   ├── index/              # IndexManager（LRU + 引用计数）
│   ├── raft/               # RaftNode（知识库/版本强一致）
│   ├── kvraft/             # Raft 共识库（leader 选举/日志复制/快照）
│   ├── kvstorage/          # Raft 持久化
│   ├── wal/                # 写前日志（崩溃一致性）
│   └── coordinator/        # WriteCoordinator + DeleteCoordinator
│
├── service/                # gRPC Service 实现
│   ├── knowledgebase.go
│   ├── query.go
│   └── admin.go
│
├── integration/            # 集成测试 + T4 多节点测试
│   └── docker/             # Docker 3 节点集群配置
│
├── cmd/stratum/main.go     # 启动入口
├── configs/                # 配置文件
├── doc/                    # 设计文档
├── vecstore/               # C++ 向量存储（Faiss + RocksDB）
└── go.mod
```

## 实现进度

| 阶段 | 说明 | 状态 |
|------|------|------|
| 0 | 接口定义与项目骨架 | ✅ |
| 1 | 存储层基础模块（PebbleDB + C++ vecstore） | ✅ |
| 2 | WAL + ChunkStore 客户端 | ✅ |
| 3 | Raft 状态机（kvraft + RaftNodeImpl） | ✅ |
| 4-A | EmbedClient（HTTP 实现） | ✅ |
| 4-B | IndexManager（LRU + 引用计数 + 异步构建） | ✅ |
| 5-A | WriteCoordinator（完整 7 步写路径编排） | ✅ |
| 5-B | DeleteCoordinator（异步清理 + 崩溃恢复） | ✅ |
| 6 | Service 层 + main.go + 配置 | ✅ |
| 7 | 集成测试（单节点 + 3 节点集群） | ✅ |

## 测试覆盖

```
17 个包全部 PASS（race 干净）:

单模块测试（T1）： bloom, chunkdoc, chunkstore, docstore, embed, errors,
                   pebbleutil, splitter, versiondoc, wal, kvstorage
模块配合测试（T2）： coordinator, index, service
集成测试（T3）：     integration（15 个端到端场景）
集群测试（T4）：     integration（3 节点 Raft 集群）
Raft 测试：          kvraft（15 个），raft（11 个）
```

## kvraft Raft 共识库

从 [KVServer](https://github.com/Hk-owo/KVServer)（MIT 6.5840 教学实现）改写而来，修复了 6 个真实 bug：

| # | Bug | 修复 |
|---|-----|------|
| 1 | 单节点集群永远选不出 leader | 自投票立即检查多数 |
| 2 | AppendEntries 心跳跳过日志一致性检查 | 统一走一致性检查 |
| 3 | InstallSnapshot 持锁发送 channel → 死锁 | 释锁后发送 |
| 4 | RequestVote 未检查 killed() | 入口加 killed() 检查 |
| 5 | 无 no-op-on-election 提案 | 当选时立即追加空日志 |
| 6 | 自己作为 peer 导致 leader 被自己心跳踢下台 | skip self in AddPeer |

## 相关文档

- `doc/Stratum_设计文档v10.md` — 系统架构与数据流
- `doc/Stratum_设计目标.md` — 功能与性能目标
- `doc/Stratum_接口设计v9.md` — 模块接口定义
- `doc/Stratum_实现顺序.md` — 分阶段实现计划
- `doc/Stratum_测试顺序.md` — 四批测试计划
- `doc/Stratum_代码风格v2.md` — 编码规范
