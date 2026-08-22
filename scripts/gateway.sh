#!/usr/bin/env bash
# gateway.sh — stratum-gateway 快捷启动（两种模式，默认零参数）。
#
# 模式：
#   cluster（默认）  Docker 集群模式：自动从 run/console.yaml 的 docker 段
#                    读取节点数/基础端口，拼接多节点 gRPC 地址并启动
#                    （写操作自动跟随当前 leader，leader 宕机自动轮换）。
#   single           单机模式：-grpc-addr 127.0.0.1:7000。
#
# 用法：
#   scripts/gateway.sh              集群模式启动（零参数快捷启动）
#   scripts/gateway.sh single       单机模式启动
#   scripts/gateway.sh build        强制重新构建二进制后（默认模式）启动
#   scripts/gateway.sh stop         停止正在运行的 gateway
#
# 环境变量：
#   STRATUM_HTTP_ADDR  控制台监听地址（默认 0.0.0.0:8081）
#   STRATUM_GRPC_ADDR  单机模式 gRPC 地址（默认 127.0.0.1:7000）

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN="$ROOT/run/bin/stratum-gateway"
OPS_CONFIG="$ROOT/run/console.yaml"
STATIC="$ROOT/web"
HTTP_ADDR="${STRATUM_HTTP_ADDR:-0.0.0.0:8081}"
SINGLE_GRPC="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"

# ---------- 停止 ----------
if [[ "${1:-}" == "stop" ]]; then
  pids=$(pgrep -f "^$BIN " || true)
  if [[ -z "$pids" ]]; then
    echo "gateway 未在运行"
  else
    kill $pids
    echo "已停止 gateway（PID: $(echo "$pids" | tr '\n' ' ')）"
  fi
  exit 0
fi

# ---------- 构建 ----------
if [[ "${1:-}" == "build" ]]; then
  shift
fi
if [[ ! -x "$BIN" ]] || [[ "${1:-}" == "build" ]]; then
  echo "==> 构建 stratum-gateway …"
  export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
  mkdir -p "$ROOT/run/bin" "$ROOT/run/gocache" "$ROOT/run/gotmp"
  go build -o "$BIN" ./cmd/stratum-gateway/
fi

# ---------- 模式解析 ----------
MODE="${1:-cluster}"
GRPC_ARGS=""
case "$MODE" in
  cluster)
    if [[ ! -f "$OPS_CONFIG" ]]; then
      echo "错误：未找到 $OPS_CONFIG（请先 scripts/docker-cluster.sh init 或运行 start.sh 生成）" >&2
      exit 1
    fi
    # 从 console.yaml 的 docker 段读取集群参数（集群级统一配置）
    NODES_BASE="$(OPS_CONFIG="$OPS_CONFIG" python3 - <<'EOF'
import os, yaml
try:
    d = yaml.safe_load(open(os.environ["OPS_CONFIG"])) or {}
except Exception:
    print(3, 17000)
    raise SystemExit(0)
dk = d.get("docker") or {}
print(dk.get("nodes", 3), dk.get("base_port", 17000))
EOF
)"
    read -r NODES BASE_PORT <<<"$NODES_BASE"
    ADDRS=""
    for ((i = 0; i < NODES; i++)); do
      ADDRS+="localhost:$((BASE_PORT + i)),"
    done
    GRPC_ARGS="-grpc-addr ${ADDRS%,}"
    echo "==> 集群模式：${NODES} 节点，gRPC ${GRPC_ARGS#-grpc-addr }（leader 自动跟随）"
    ;;
  single)
    GRPC_ARGS="-grpc-addr $SINGLE_GRPC"
    echo "==> 单机模式：gRPC $SINGLE_GRPC"
    ;;
  -h|--help)
    sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *)
    echo "错误：未知模式 $MODE（可用 cluster / single / build / stop）" >&2
    exit 1
    ;;
esac

# ---------- 启动 ----------
echo "==> 启动 stratum-gateway（控制台 http://localhost:${HTTP_ADDR##*:}）…"
echo "    （Ctrl+C 停止；日志输出在本终端）"
exec "$BIN" $GRPC_ARGS \
  -http-addr "$HTTP_ADDR" \
  -static "$STATIC" \
  -ops-config "$OPS_CONFIG"
