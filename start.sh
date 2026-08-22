#!/usr/bin/env bash
# start.sh — Stratum 控制台一键启动脚本
#
# 构建 vecstore(C++) / stratum / stratum-gateway / mock-embed，然后只启动
# stratum-gateway（控制台进程）；数据库三件套（vecstore / embed / stratum）
# 由控制台通过 /ops/start 统一拉起。因此：
#   - Web 控制台始终可用——数据库未启动时也能改启动参数、看日志；
#   - 「运维」页看到的状态与脚本一致，Ctrl+C 退出时控制台会一并停止它
#     管理的所有服务（不会残留孤儿进程）。
#
# 前置依赖：
#   - Go 1.24+
#   - C++ 工具链 + Faiss / RocksDB / gRPC / protobuf / BLAS / LAPACK / OpenMP
#     （仅首次构建 vecstore_server 时需要；见 vecstore/CMakeLists.txt 顶部说明）
#
# 可用环境变量覆盖端口/地址（仅首次生成 run/console.yaml 时生效，之后以
# 运维页保存的配置为准；如需重置配置请删除 run/console.yaml 后重跑）：
#   STRATUM_VECSTORE_ADDR  默认 127.0.0.1:7100
#   STRATUM_GRPC_ADDR      默认 127.0.0.1:7000
#   STRATUM_HTTP_ADDR      默认 0.0.0.0:8081
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

RUN="$ROOT/run"
BIN="$RUN/bin"
LOG="$RUN/log"
DATA="$RUN/data"
CONSOLE_YAML="$RUN/console.yaml"
mkdir -p "$BIN" "$LOG" "$DATA"

# 构建缓存落在 run/ 下，不依赖全局 GOCACHE（某些环境全局缓存只读）。
export GOCACHE="$RUN/gocache"
export GOTMPDIR="$RUN/gotmp"
mkdir -p "$GOCACHE" "$GOTMPDIR"

VECSTORE_ADDR="${STRATUM_VECSTORE_ADDR:-127.0.0.1:7100}"
GRPC_ADDR="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"
HTTP_ADDR="${STRATUM_HTTP_ADDR:-0.0.0.0:8081}"
HTTP_PORT="${HTTP_ADDR##*:}"
GRPC_PORT="${GRPC_ADDR##*:}"

# ---------- 1. 构建 vecstore_server（C++） ----------
VECSTORE_BIN="$BIN/vecstore_server"
if [ ! -x "$VECSTORE_BIN" ]; then
  echo "==> [1/4] 构建 vecstore_server（C++，首次较慢）…"
  cmake -S . -B "$RUN/cmake" >/dev/null
  cmake --build "$RUN/cmake" --target vecstore_server -j"$(nproc)" >/dev/null
  cp "$RUN/cmake/vecstore/vecstore_server" "$VECSTORE_BIN"
else
  echo "==> [1/4] vecstore_server 已存在，跳过构建"
fi

# ---------- 2. 构建 Go 二进制 ----------
echo "==> [2/4] 构建 Go 二进制（stratum / stratum-gateway / stratum-router / mock-embed）…"
go build -o "$BIN/stratum" ./cmd/stratum/
go build -o "$BIN/stratum-gateway" ./cmd/stratum-gateway/
go build -o "$BIN/stratum-router" ./cmd/stratum-router/
go build -o "$BIN/mock-embed" ./integration/docker/mock_embed_server.go

# ---------- 3. 控制台配置 ----------
# 首次启动按脚本默认值生成 run/console.yaml（与脚本的目录/端口一致）；
# 若已存在（例如已通过 Web「运维」页改过参数）则保留用户配置，不覆盖。
if [ ! -f "$CONSOLE_YAML" ]; then
  echo "==> [3/4] 生成控制台配置 $CONSOLE_YAML"
  cat > "$CONSOLE_YAML" <<EOF
node_id: 1
bin_dir: "$BIN"
log_dir: "$LOG"
config_dir: "$RUN/configs"
cluster:
  - id: 1
    gateway_addr: "http://127.0.0.1:$HTTP_PORT"
services:
  vecstore:
    bin: "vecstore_server"
    grpc_addr: "$VECSTORE_ADDR"
    rocksdb_path: "$DATA/vecstore_rocksdb"
  embed:
    bin: "mock-embed"
    service_addr: "http://localhost:8080"
  stratum:
    bin: "stratum"
    node_id: 1
    data_dir: "$DATA/stratum"
    grpc_addr: "$GRPC_ADDR"
    raft_addr: "0.0.0.0:8000"
    peers:
      - id: 1
        addr: "localhost:8000"
        service_addr: "127.0.0.1:$GRPC_PORT"
    vecstore_addr: "$VECSTORE_ADDR"
    embed_addr: "http://localhost:8080"
EOF
else
  echo "==> [3/4] 控制台配置已存在，保留 $CONSOLE_YAML（如需重置请删除后重跑）"
fi

# ---------- 4. 启动路由层、控制台并拉起服务 ----------
# 拓扑：gateway → stratum-router（路由层）→ 集群节点。router 负责 leader
# 发现与写转发/读均衡，gateway 只连 router 一个地址。
echo "==> [4/4] 启动路由层（stratum-router）与控制台（stratum-gateway）…"
ROUTER_ADDR="${STRATUM_ROUTER_ADDR:-127.0.0.1:7009}"
PIDS=()
cleanup() {
  echo
  echo "==> 停止服务（/ops/stop）并退出路由层与控制台…"
  curl -sf -X POST "http://127.0.0.1:${HTTP_PORT}/ops/stop" -H 'Content-Type: application/json' -d '{}' >/dev/null 2>&1 || true
  sleep 1
  kill "${PIDS[@]:-}" 2>/dev/null || true
  wait "${PIDS[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

"$BIN/stratum-router" \
  -listen "$ROUTER_ADDR" \
  -nodes "127.0.0.1:$GRPC_PORT" \
  >"$LOG/router.log" 2>&1 &
PIDS+=($!)

"$BIN/stratum-gateway" \
  -grpc-addr "$ROUTER_ADDR" \
  -http-addr "$HTTP_ADDR" \
  -static "$ROOT/web" \
  -ops-config "$CONSOLE_YAML" \
  >"$LOG/gateway.log" 2>&1 &
PIDS+=($!)

# 等待控制台就绪
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${HTTP_PORT}/ops/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# 通过控制台拉起 vecstore / embed / stratum
echo "==> 通过 /ops/start 启动数据库服务…"
curl -sf -X POST "http://127.0.0.1:${HTTP_PORT}/ops/start" -H 'Content-Type: application/json' -d '{}' >/dev/null 2>&1 || \
  echo "  （启动请求失败，可到「运维」页手动操作，或查看日志 $LOG/gateway.log）"

# 等待数据库就绪
ready=0
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${HTTP_PORT}/api/health" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

echo
echo "=============================================="
echo "  Stratum 控制台已启动"
echo "  浏览器访问：http://localhost:${HTTP_PORT}/"
echo "  「运维」页：服务启停、启动参数、日志"
if [ "$ready" -ne 1 ]; then
  echo "  （数据库尚未就绪，可查看日志：$LOG/，或在「运维」页手动启动）"
fi
echo "  日志目录：$LOG/"
echo "  数据目录：$DATA/"
echo "  按 Ctrl+C 停止全部服务"
echo "=============================================="
echo

wait
