# Stratum 运维脚本手册

本目录是一套面向运维的命令行脚本：日常操作通过 REST API 完成，
**不需要阅读或修改代码**。所有脚本依赖 `curl`、`jq`（查询脚本另需
`python3`，配置工具另需 PyYAML）。

## 快速上手

```bash
# 1. 启动整套服务（已有 start.sh，保持原样）
./start.sh

# 2. 看系统是否健康、有哪些知识库
scripts/ops/health.sh
scripts/ops/status.sh
scripts/ops/kb-list.sh

# 3. 创建一个知识库并写入文档
scripts/ops/kb-create.sh --name 产品手册
scripts/ops/kb-version-create.sh <知识库ID> \
  --changes <(echo '[{"op":"ADD","doc_id":"d1","content":"Stratum 支持版本回滚"}]')

# 4. 查询 / 发布新版本 / 回滚
scripts/ops/query.sh <知识库ID> --text "什么是回滚" --top-k 3
scripts/ops/kb-versions.sh <知识库ID>
scripts/ops/kb-rollback.sh <知识库ID> 2
```

## 一、运行时操作（改内容 / 改状态）

所有脚本默认连 `http://127.0.0.1:8081`（与 `../../start.sh` 的 gateway 端口一致），
可用 `--api http://主机:端口` 或环境变量 `STRATUM_HTTP_ADDR` 指向其它节点。

| 脚本 | 作用 | 典型用法 |
|---|---|---|
| `health.sh` | 健康检查（三态） | `health.sh`（退出码 0=健康，可接监控） |
| `status.sh` | 系统状态：卡住的版本、删除失败、WAL 告警、资源 | `status.sh --json` |
| `kb-list.sh` | 列出全部知识库 | `kb-list.sh` |
| `kb-get.sh` | 查看单个知识库配置与激活版本 | `kb-get.sh <KB-ID>` |
| `kb-create.sh` | 创建知识库 | `kb-create.sh --name 手册` |
| `kb-delete.sh` | 删除知识库（异步，需确认） | `kb-delete.sh <KB-ID> --yes` |
| `kb-versions.sh` | 查看版本链（含索引状态） | `kb-versions.sh <KB-ID>` |
| `kb-version-create.sh` | **改内容**：新增/更新/删除文档，生成新版本 | 见下 |
| `kb-rollback.sh` | 切换激活版本（发布/回滚，无停机） | `kb-rollback.sh <KB-ID> <版本>` |
| `kb-rebuild.sh` | 重建 FAILED 版本的索引 | `kb-rebuild.sh <KB-ID> <版本>` |
| `kb-warmup.sh` | 预热版本索引到内存（不切换激活版本） | `kb-warmup.sh <KB-ID> <版本>` |
| `query.sh` | 向量检索 | `query.sh <KB-ID> --text "…" --top-k 5` |

### 修改文档内容（核心操作）

知识库的内容按"版本"管理，**任何修改都产生一个新版本**，不影响当前
激活版本；确认无误后再用 `kb-rollback.sh` 发布，随时可以回退：

```bash
# 方式一：变更文件（推荐，支持批量）
cat > /tmp/changes.json <<'EOF'
{
  "changes": [
    {"op": "ADD",    "doc_id": "d1", "content": "新文档全文…"},
    {"op": "UPDATE", "doc_id": "d2", "content": "修改后的全文…"},
    {"op": "DELETE", "doc_id": "d3"}
  ]
}
EOF
scripts/ops/kb-version-create.sh kb-xxx --changes /tmp/changes.json

# 方式二：单条快捷写法
scripts/ops/kb-version-create.sh kb-xxx --add d4 --content "一句话文档"
scripts/ops/kb-version-create.sh kb-xxx --delete d5
```

`op` 不区分大小写；`DELETE` 不需要 `content`。

### 版本发布流程（灰度 / 回滚）

```bash
scripts/ops/kb-versions.sh kb-xxx      # 1. 看版本链与索引状态（READY 才能发布）
scripts/ops/kb-rollback.sh kb-xxx 3    # 2. 发布版本 3 为激活版本
scripts/ops/query.sh kb-xxx --text "…" # 3. 验证
scripts/ops/kb-rollback.sh kb-xxx 2    # 4. 出问题？一键切回版本 2
```

索引构建失败的版本：`status.sh` 会列出，用 `kb-rebuild.sh` 重建。

### 查询向量从哪来

`query.sh --text` 使用 **mock embed 的确定性算法**由文本直接生成向量，
只在开发/演示环境（`mock_embed_server`）有效。生产环境请调用你的 embed
服务拿到向量后传入：

```bash
scripts/ops/query.sh kb-xxx --vector "0.123,0.456,…" --top-k 10 --threshold 0.5
```

## 二、运行前参数（配置 / 启停）

### 生成与调整配置文件 `gen-config.py`

`cmd/stratum` 支持 YAML 配置（`-config` 参数），本工具生成/调整该文件，
覆盖的字段见 `cmd/stratum/main.go` 的 `fileConfig`（node、raft.peers、
storage、vecstore、embed、index_manager、write_coordinator、
delete_coordinator）。

```bash
# 从零生成单节点配置
scripts/ops/gen-config.py \
  --node-id 1 \
  --grpc-addr 0.0.0.0:7000 --raft-addr 0.0.0.0:8000 \
  --data-dir /var/lib/stratum/node1 \
  --out configs/my-node1.yaml

# 生成三节点集群配置（每个节点一份，各自改 node_id/地址）
scripts/ops/gen-config.py --node-id 1 --data-dir /var/lib/stratum/node1 \
  --peers "1=10.0.0.1:8000=10.0.0.1:7000,2=10.0.0.2:8000=10.0.0.2:7000,3=10.0.0.3:8000=10.0.0.3:7000" \
  --out configs/cluster-node1.yaml

# 在现有配置上调整个别参数（--set 任意层级，自动识别数字/布尔）
scripts/ops/gen-config.py --base configs/config1.yaml \
  --set index_manager.lru_capacity=32 \
  --set write_coordinator.max_retries=5 \
  --set storage.data_dir=/data/stratum/node1 \
  --out configs/my-node1.yaml

# 启动时应用（命令行 flag 可再覆盖文件值）
./stratum -config configs/my-node1.yaml
```

注意：`--peers` 的 `id=raft地址=service地址`，多节点时 service 地址
（数据同步用）必填；单节点可以只写 `id=地址`。

### 启动 / 停止

```bash
./start.sh                 # 一键启动（前台，Ctrl+C 停止；支持 STRATUM_* 环境变量覆盖端口）
scripts/ops/stop.sh        # 停止全部服务（优雅退出，--force 直接杀）
scripts/ops/stop.sh --dry-run   # 只列出将停止的进程
```

数据目录在 `run/data/`；**彻底清空数据**请先停服，再用
`scripts/delete_test_db.py`（开发环境重置）。

## 三、给监控 / 自动化使用

- `health.sh --quiet`：输出 `HEALTHY` / `DEGRADED` / `UNHEALTHY`，退出码
  0=健康，适合 crontab / Prometheus 文本采集器。
- `status.sh`：存在 FAILED 版本或删除失败的知识库时退出码非 0。
- 所有脚本失败时向 stderr 打印后端返回的错误 JSON（含 grpc_code），
  可用于告警定位。
