# Stratum 代码风格

> 本文档规定 Stratum 项目的代码风格、接口设计原则和工程约定，所有模块统一遵守。

---

## 语言分工

- **Go**：上层协调逻辑，包括版本管理、读写路径、索引管理器、知识库管理
- **C++**：底层存储和向量搜索，包括 chunk store、向量索引

---

## 错误处理

### Go

- 统一使用 error 返回值，不使用 panic/recover 处理业务错误
- 定义具名业务错误类型，集中放在 `internal/errors` 包：

```go
var (
    ErrVersionNotFound       = errors.New("version not found")
    ErrVersionPending        = errors.New("version is pending")        // 版本存储写入中，不可查询
    ErrVersionFailed         = errors.New("version index failed")      // 索引构建失败，不可查询
    ErrKnowledgeBaseNotFound = errors.New("knowledge base not found")
    ErrKnowledgeBaseDeleted  = errors.New("knowledge base is deleted")
    ErrIndexNotReady         = errors.New("index not ready")
    ErrInvalidArgument       = errors.New("invalid argument")
    ErrIndexLoadTimeout      = errors.New("index load timeout")
    ErrInvalidParentVersion  = errors.New("invalid parent version")
)
```

- 错误向上传递时用 `fmt.Errorf("...: %w", err)` 包装，保留调用链
- 内部模块（DocStore、ChunkStore、Coordinator 等）只返回上述具名 error，不感知 gRPC

**gRPC 错误码映射**：Service 层统一通过 `ToGRPCStatus(err error) error` 转换业务 error 为 gRPC status，集中维护映射表：

```go
var grpcCodeMap = map[error]codes.Code{
    ErrVersionNotFound:       codes.NotFound,
    ErrVersionPending:        codes.FailedPrecondition,
    ErrVersionFailed:         codes.FailedPrecondition,
    ErrKnowledgeBaseNotFound: codes.NotFound,
    ErrKnowledgeBaseDeleted:  codes.FailedPrecondition,
    ErrIndexNotReady:         codes.FailedPrecondition,
    ErrInvalidArgument:       codes.InvalidArgument,
    ErrIndexLoadTimeout:      codes.DeadlineExceeded,
    ErrInvalidParentVersion:  codes.InvalidArgument,
    // 未匹配的错误兜底为 codes.Internal
}

func ToGRPCStatus(err error) error {
    if err == nil {
        return nil
    }
    for sentinel, code := range grpcCodeMap {
        if errors.Is(err, sentinel) {
            return status.Error(code, err.Error())
        }
    }
    return status.Error(codes.Internal, err.Error())
}
```

每个 gRPC 方法实现的最外层统一调用 `ToGRPCStatus`，新增错误类型时只需在映射表加一行。

**进程级不可恢复错误**：遇到无法通过业务逻辑恢复的错误（配置错误、存储引擎初始化失败等），使用 `log.Fatal` 或显式 `os.Exit(非零)` 退出，确保退出码非零。不可恢复错误不能被 panic+recover 吞掉——容器编排（k8s 等）依赖非零退出码识别崩溃并记录重启次数，这是 Stratum 进程级存活监控的基础，Stratum 自身不需要也不应该实现"自我崩溃检测"。

**context 使用约定**：所有涉及等待的操作（索引加载、Raft propose、gRPC 调用）必须同时监听 `ctx.Done()` 和自身的超时定时器，哪个先触发就先返回。不能只依赖自身超时而忽略 context 取消——gRPC 层超时时调用方 context 已取消，内部等待必须立即终止，不继续占用资源。示例：

```go
select {
case <-loadDone:
    // 加载完成
case <-time.After(loadWaitTimeout):
    return nil, ErrIndexLoadTimeout
case <-ctx.Done():
    return nil, ctx.Err()
}
```

### C++

- 使用 abseil 的 `absl::Status` / `absl::StatusOr<T>`，不使用异常
- 风格与 gRPC Status 保持一致（abseil status code 与 gRPC status code 一一对应）
- 函数签名示例：

```cpp
absl::Status WriteChunk(const ChunkID& id, const Vector& vec);
absl::StatusOr<Vector> ReadChunk(const ChunkID& id);
```

---

## 日志

### Go（zap）

- 使用结构化日志，字段明确：

```go
logger.Info("version created",
    zap.String("knowledge_base_id", kbID),
    zap.Int64("version_id", versionID),
)
```

- 日志级别规范：
  - `INFO`：正常业务流程关键节点
  - `DEBUG`：调试信息，默认关闭
  - `ERROR`：错误，需要关注
  - 不滥用 `WARN`

### C++（spdlog）

- 使用结构化格式，字段明确：

```cpp
spdlog::info("chunk written, knowledge_base_id={}, chunk_id={}", kb_id, chunk_id);
```

- 日志级别规范与 Go 侧保持一致

---

## 封装边界

- 对外只暴露顶层接口（写入、查询、版本管理、知识库管理）
- 内部模块（doc store、chunk store、chunk-doc 映射、版本文档列表、布隆过滤器、索引管理器、切割模块、embed 客户端）全部放在 `internal/` 下，不对外暴露
- 外部调用者只能通过顶层接口操作，不能绕过逻辑链路直接访问中间层
- C++ 内部实现的头文件不对外暴露，顶层服务头文件只暴露入口接口

---

## 模块接口规定

每个模块在实现之前必须先定义对外接口，实现依赖接口而不是具体类型。模块替换时只需提供新的实现，调用方无感知。接口定义写入接口设计文档，实现放在对应的 `internal/` 子包下。

**Go 侧需提前定义接口的模块**：
- `ChunkSplitter`：切割策略，默认实现为滑动窗口，未来可替换为语义切割
- `EmbedClient`：embed 服务客户端，调用外部 embed 服务生成 chunk 向量
- `DocStore`：文档存储
- `ChunkStore`：chunk 存储（vecstore gRPC 客户端封装）
- `ChunkDocMapper`：chunk-doc 双向映射
- `VersionDocList`：版本文档列表
- `BloomFilter`：布隆过滤器
- `IndexManager`：索引管理器
- `RaftNode`：Raft 操作
- `WriteCoordinator`：写路径编排，承担 CreateVersion 的内部编排逻辑
- `DeleteCoordinator`：删除路径编排，承担 DeleteKnowledgeBase 的异步清理编排
- `WAL`：写路径和删除路径的崩溃一致性保证

**C++ 侧需提前定义接口的模块**：
- `VectorIndex`：向量索引，HNSW / IVF / FLAT 各自实现此接口
- `ChunkStorage`：chunk 持久化存储

---

## 可测试性

- 依赖通过接口注入，不在模块内部直接构造依赖，方便 mock：

```go
type DocStore interface {
    Write(ctx context.Context, key DocKey, value []byte) error
    Scan(ctx context.Context, prefix DocPrefix) ([]DocEntry, error)
}

type Service struct {
    docStore DocStore
    // ...
}
```

- 避免全局状态，测试之间互不影响
- 必要时暴露内部状态的 getter 供测试使用

---

## 测试约定

- 每个模块写完必须配套单元测试
- Go 使用白盒测试（`_test.go` 与被测包同包），直接访问内部实现
- C++ 使用独立测试编译单元，必要时用 `friend class` 访问内部状态
- 测试用例使用 table-driven 风格（Go）：

```go
tests := []struct {
    name    string
    input   Input
    want    Output
    wantErr error
}{
    {"normal case", ...},
    {"version not found", ...},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        // ...
    })
}
```

---

## 包结构（Go）

```
stratum/
├── api/              # 对外暴露的顶层接口定义（proto 生成代码）
├── internal/
│   ├── docstore/
│   ├── chunkstore/
│   ├── chunkdoc/
│   ├── versiondoc/
│   ├── bloom/
│   ├── splitter/         # 切割策略，默认滑动窗口
│   ├── embed/            # embed 服务客户端
│   ├── index/
│   ├── raft/
│   ├── wal/
│   ├── coordinator/
│   │   ├── write.go       # WriteCoordinator
│   │   └── delete.go      # DeleteCoordinator
│   └── errors/            # 具名业务错误类型 + gRPC 错误码映射表（ToGRPCStatus）
└── service/
    ├── knowledgebase.go    # KnowledgeBaseService 实现
    ├── query.go            # QueryService 实现
    └── admin.go            # AdminService 实现（含 HealthCheck / GetSystemStatus）
```

---

## 配置文件 Schema

集群启动通过配置文件 + 命令行参数（沿用 kvserver 方式），成员变更走"改配置 + 重启"，不提供在线成员变更接口。每个节点一份 `configN.yaml`。

```yaml
# config.yaml 示例

node:
  node_id: 1
  grpc_addr: "0.0.0.0:7000"      # KnowledgeBaseService / QueryService / AdminService
  raft_addr: "0.0.0.0:8000"      # Raft 内部通信
  metrics_addr: "0.0.0.0:9000"   # Prometheus /metrics

raft:
  peers:
    - id: 1
      addr: "node1:8000"
    - id: 2
      addr: "node2:8000"
    - id: 3
      addr: "node3:8000"

storage:
  data_dir: "/var/lib/stratum/node1"
  doc_store_path: "${data_dir}/docstore"            # PebbleDB
  chunkdoc_path: "${data_dir}/chunkdoc"             # PebbleDB
  versiondoc_path: "${data_dir}/versiondoc"         # PebbleDB
  wal_path: "${data_dir}/wal"
  index_path: "${data_dir}/index"                   # HNSW 索引磁盘文件

vecstore:
  rocksdb_path: "${data_dir}/vecstore_rocksdb"      # C++ 侧 chunk store（RocksDB）
  grpc_addr: "127.0.0.1:7100"                       # Go ↔ C++ vecstore 内部通信
  health_addr: "127.0.0.1:7101"                     # vecstore HTTP 健康端点（/health、/metrics）

write_coordinator:
  max_retries: 3                # 存储层 IO 错误自动重试次数
  retry_base_interval_ms: 100  # 指数退避基础间隔

delete_coordinator:
  max_retries: 5                # 删除步骤失败最大重试次数，耗尽后标记 DeleteFailed
  retry_base_interval_ms: 500

index_manager:
  lru_capacity: 16              # 内存中最多保留的活跃索引数量
  memory_threshold_mb: 4096     # 超过后触发 LRU 换出标志位
  load_wait_timeout_ms: 5000    # 并发加载等待超时，超时返回 DEADLINE_EXCEEDED；建议设置为调用方 gRPC 超时的 70%~80%
  callback_max_retries: 3           # 构建完成回调 Raft propose 失败重试次数
  callback_retry_base_interval_ms: 200  # 回调重试指数退避基础间隔

bloom_filter:
  false_positive_rate: 0.01     # chunk 存在性 / 版本文档布隆过滤器误判率

knowledge_base_defaults:
  chunk_window_size: 512        # 调用方未指定时的默认切割窗口
  chunk_overlap_size: 64        # 调用方未指定时的默认重叠
  index_type: "HNSW"            # 当前仅支持 HNSW
  similarity: "COSINE"          # 默认余弦相似度

wal:
  replay_retry_threshold: 3     # 重放失败超过该次数标记为需人工介入（内存计数，不持久化）

gc:
  version_retention_count: 50   # 每个知识库保留的最近版本数量
  gc_interval_s: 3600           # 后台 GC 任务间隔
  audit_log_retention_days: 30  # 审计日志保留天数

logging:
  level: "info"                 # debug / info / error
```

**字段说明**：

- `node.*`：节点身份和监听地址，三个端口分别对应业务 gRPC、Raft 内部通信、Prometheus 指标
- `raft.peers`：集群成员列表，扩缩容需要修改所有节点的配置并重启
- `storage.*`：Go 侧 PebbleDB 模块（doc store/chunk-doc 映射/版本文档列表）和 WAL、HNSW 索引文件的磁盘路径
- `vecstore.*`：C++ 侧 chunk store（RocksDB）路径和 Go↔C++ 的内部 gRPC 地址
- `index_manager.*`：对应设计文档中索引管理器的 LRU 换入换出、并发加载超时、异步构建回调重试
- `bloom_filter.false_positive_rate`：两类布隆过滤器（chunk 存在性、版本文档）共用该误判率配置
- `knowledge_base_defaults.*`：`CreateKnowledgeBase` 未传参数时的默认值
- `wal.replay_retry_threshold`：对应 `ReplayCounter` 的告警阈值，内存态不持久化
- `gc.*`：版本文档列表 / doc store 的 GC 触发间隔和保留数量，审计日志保留策略
- `delete_coordinator.*`：删除流程每步最大重试次数，耗尽后置 `DeleteFailed` 状态
- `logging.level`：全局日志级别，DEBUG 默认关闭

**变量替换机制**：`${data_dir}` 等占位符由 Stratum 启动代码在加载配置时替换（简单字符串替换，不依赖 Helm/consul-template 等外部工具），替换后的绝对路径用于初始化各存储引擎。配置文件本身不要求外部模板引擎预处理。

---

## 库依赖

### Go 侧

| 库 | 用途 |
|---|---|
| PebbleDB | LSM-Tree 存储引擎（doc store、chunk-doc 映射、版本文档列表、审计日志） |
| Bleve | 全文搜索，用于混合查询的关键词过滤 |
| `bits-and-blooms/bloom` | 布隆过滤器，chunk 存在性检查和版本文档过滤 |
| `prometheus/client_golang` | 指标上报，暴露 `/metrics` 端点 |
| `go.uber.org/zap` | 结构化日志 |
| gRPC + protobuf | 对外接口 |
| 现有 kvserver Raft | 版本元数据强一致复制 |

### C++ 侧

| 库 | 用途 |
|---|---|
| RocksDB | chunk store 持久化存储 |
| Faiss | 向量索引，统一支持 HNSW / IVF / FLAT |
| abseil | `absl::Status` / `absl::StatusOr<T>`，统一错误处理 |
| spdlog | 结构化日志 |
| gRPC + protobuf | 对外接口 |
| prometheus-cpp | vecstore 侧 `/metrics` 端点和 `/health` 端点，暴露 Faiss 内存、RocksDB 读写延迟等指标 |

---

## 包结构（C++ vecstore）

```
vecstore/
├── include/
│   ├── vector_index.h    # VectorIndex 纯虚类
│   └── chunk_storage.h   # ChunkStorage 纯虚类
├── src/
│   ├── hnsw_index.cpp    # Faiss HNSW 实现
│   ├── rocksdb_storage.cpp
│   ├── grpc_service.cpp  # Go ↔ C++ 内部 gRPC 服务入口
│   └── health_service.cpp # HTTP /health 和 /metrics 端点（prometheus-cpp）
├── proto/
│   └── vecstore.proto    # vecstore 内部 gRPC 接口
├── test/
│   ├── hnsw_index_test.cpp
│   └── rocksdb_storage_test.cpp
└── CMakeLists.txt
```

IVF / FLAT 的实现文件（`ivf_index.cpp`/`flat_index.cpp`）留待后续迭代时新增，不在当前结构中创建空文件。

---

## 可观测性

### 指标埋点

**latency histogram**：
- 写入端到端延迟
- Raft 提交延迟
- embed 服务调用延迟
- HNSW 搜索延迟
- doc store 扫描延迟
- chunk-doc 映射扫描延迟
- 布隆过滤器查询延迟

**counter**：
- 写入成功 / 失败次数
- 查询成功 / 失败次数
- embed 服务调用成功 / 失败次数
- 索引构建触发次数
- 索引构建成功 / 失败次数
- 版本回滚次数
- 布隆过滤器假阳性命中次数
- WAL 重放失败次数

**gauge**：
- 内存中活跃索引数量
- 每个知识库的版本数
- chunk store 大小
- doc store 大小
- 处于 FAILED 状态的版本数量
- 处于 DeleteFailed 状态的知识库数量
- WAL 重放计数超阈值（需人工介入）的记录数量

### 分布式追踪

暂不引入，作为预留方向，接口设计时 context 链路保持完整，后续引入 OpenTelemetry 时可直接在 context 上注入 span。

---

## 测试规划

### 单元测试

- 每个模块写完必须配套单元测试
- 依赖通过接口注入，使用 mock 隔离外部依赖
- Go 使用白盒测试，C++ 使用独立测试编译单元

### 集成测试

使用 Docker Compose 起完整环境，覆盖：
- 写入后正确读回
- 版本回滚后查询切换到目标版本
- 多版本并存查询正确性

### 故障注入测试

- **崩溃恢复**：在 WAL BEGIN 和 COMMIT 之间强制终止进程，重启后验证数据一致性
- **Raft 故障**：网络分区（leader 和 follower 隔离）、leader 宕机重新选举，验证元数据一致性
- **磁盘故障**：Go 侧 mock 存储接口返回错误，C++ 侧使用 RocksDB 原生 fault injection，验证错误处理路径正确

---

## 完整项目结构

```
stratum/
├── api/
│   └── proto/
│       ├── knowledgebase.proto   # KnowledgeBaseService
│       ├── query.proto           # QueryService
│       └── admin.proto           # AdminService
│
├── internal/
│   ├── docstore/
│   ├── chunkstore/
│   ├── chunkdoc/
│   ├── versiondoc/
│   ├── bloom/
│   ├── splitter/
│   ├── embed/
│   ├── index/
│   ├── raft/
│   ├── wal/
│   ├── coordinator/
│   │   ├── write.go
│   │   └── delete.go
│   └── errors/
│
├── service/
│   ├── knowledgebase.go
│   ├── query.go
│   └── admin.go
│
├── vecstore/
│   ├── include/
│   │   ├── vector_index.h
│   │   └── chunk_storage.h
│   ├── src/
│   │   ├── hnsw_index.cpp
│   │   ├── rocksdb_storage.cpp
│   │   ├── grpc_service.cpp
│   │   └── health_service.cpp
│   ├── proto/
│   │   └── vecstore.proto
│   ├── test/
│   │   ├── hnsw_index_test.cpp
│   │   └── rocksdb_storage_test.cpp
│   └── CMakeLists.txt
│
├── integration/
│   ├── docker-compose.yml
│   ├── write_read_test.go
│   ├── rollback_test.go
│   └── fault_injection_test.go
│
├── cmd/
│   └── stratum/
│       └── main.go
│
├── configs/
│   ├── config1.yaml
│   ├── config2.yaml
│   └── config3.yaml
│
├── go.mod
├── go.sum
└── CMakeLists.txt
```
