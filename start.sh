#!/usr/bin/env bash
# start.sh — Stratum 控制台一键启动脚本
#
# 依次拉起：vecstore(C++) → mock embed(Go) → stratum(Go) → gateway(Go)
# 然后输出浏览器访问地址。按 Ctrl+C 停止全部进程。
#
# 前置依赖：
#   - Go 1.24+
#   - C++ 工具链 + Faiss / RocksDB / gRPC / protobuf / BLAS / LAPACK / OpenMP
#     （仅首次构建 vecstore_server 时需要；见 vecstore/CMakeLists.txt 顶部说明）
#
# 可用环境变量覆盖端口/地址：
#   STRATUM_VECSTORE_ADDR  默认 127.0.0.1:7100
#   STRATUM_GRPC_ADDR      默认 127.0.0.1:7000
#   STRATUM_HTTP_ADDR      默认 0.0.0.0:8081
#   （mock embed 固定监听 :8080，与 Stratum 默认 embed 地址一致）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

RUN="$ROOT/run"
BIN="$RUN/bin"
LOG="$RUN/log"
DATA="$RUN/data"
mkdir -p "$BIN" "$LOG" "$DATA"

# 构建缓存落在 run/ 下，不依赖全局 GOCACHE（某些环境全局缓存只读）。
export GOCACHE="$RUN/gocache"
export GOTMPDIR="$RUN/gotmp"
mkdir -p "$GOCACHE" "$GOTMPDIR"

VECSTORE_ADDR="${STRATUM_VECSTORE_ADDR:-127.0.0.1:7100}"
GRPC_ADDR="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"
HTTP_ADDR="${STRATUM_HTTP_ADDR:-0.0.0.0:8081}"
HTTP_PORT="${HTTP_ADDR##*:}"

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
echo "==> [2/4] 构建 Go 二进制（stratum / stratum-gateway / mock-embed）…"
go build -o "$BIN/stratum" ./cmd/stratum/
go build -o "$BIN/stratum-gateway" ./cmd/stratum-gateway/
go build -o "$BIN/mock-embed" ./integration/docker/mock_embed_server.go

# ---------- 3. 启动四个进程 ----------
echo "==> [3/4] 启动服务…"
PIDS=()
cleanup() {
  echo
  echo "==> 停止所有服务…"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

"$BIN/vecstore_server" --rocksdb_path="$DATA/vecstore_rocksdb" --grpc_addr="$VECSTORE_ADDR" \
  >"$LOG/vecstore.log" 2>&1 &
PIDS+=($!)

"$BIN/mock-embed" >"$LOG/embed.log" 2>&1 &
PIDS+=($!)

"$BIN/stratum" \
  -data-dir "$DATA/stratum" \
  -grpc-addr "$GRPC_ADDR" \
  -vecstore-addr "$VECSTORE_ADDR" \
  -embed-addr "http://localhost:8080" \
  >"$LOG/stratum.log" 2>&1 &
PIDS+=($!)

"$BIN/stratum-gateway" \
  -grpc-addr "$GRPC_ADDR" \
  -http-addr "$HTTP_ADDR" \
  -static "$ROOT/web" \
  >"$LOG/gateway.log" 2>&1 &
PIDS+=($!)

# ---------- 4. 等待就绪 ----------
echo "==> [4/4] 等待服务就绪…"
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
if [ "$ready" -ne 1 ]; then
  echo "  （健康检查尚未就绪，可查看日志：$LOG/）"
fi
echo "  日志目录：$LOG/"
echo "  数据目录：$DATA/"
echo "  按 Ctrl+C 停止全部服务"
echo "=============================================="
echo

wait
