#!/usr/bin/env bash
# gateway.sh — stratum-gateway 总启动脚本（路由层 + 控制台）。
#
# gateway 只连**路由层**（stratum-router）：leader 发现、写转发与读均衡
# 全部由 router 承担，gateway 不再关心集群拓扑。路由层未在监听时本脚本
# 会自动构建并后台拉起它（PID 记录在 run/.router.pid），Ctrl+C / stop 时
# 一并清理自己拉起的路由层；外部已启动的路由层则复用、不干预。
#
# 模式：
#   cluster（默认）  Docker 集群模式：从 run/console.yaml 的 docker 段
#                    读取节点数/基础端口，启动路由层与 gateway。
#   single           单机模式：路由层连 127.0.0.1:7000，启动 gateway。
#
# 用法：
#   scripts/gateway.sh              集群模式启动（零参数快捷启动）
#   scripts/gateway.sh single       单机模式启动
#   scripts/gateway.sh build        强制重新构建二进制后（默认模式）启动
#   scripts/gateway.sh stop         停止 gateway 与本脚本拉起的路由层
#
# 环境变量：
#   STRATUM_HTTP_ADDR    控制台监听地址（默认 0.0.0.0:8081）
#   STRATUM_ROUTER_ADDR  路由层地址（默认 127.0.0.1:7009）
#   STRATUM_GRPC_ADDR    单机模式下路由层应连接的节点地址（默认 127.0.0.1:7000）
#
# 单独管理路由层可用 scripts/router.sh（start / stop / status）。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN="$ROOT/run/bin/stratum-gateway"
ROUTER_BIN="$ROOT/run/bin/stratum-router"
OPS_CONFIG="$ROOT/run/console.yaml"
STATIC="$ROOT/web"
HTTP_ADDR="${STRATUM_HTTP_ADDR:-0.0.0.0:8081}"
ROUTER_ADDR="${STRATUM_ROUTER_ADDR:-127.0.0.1:7009}"
ROUTER_PID_FILE="$ROOT/run/.router.pid"
ROUTER_STARTED=0

# ---------- 停止 ----------
if [[ "${1:-}" == "stop" ]]; then
  pids=$(pgrep -f "^$BIN " || true)
  if [[ -z "$pids" ]]; then
    echo "gateway 未在运行"
  else
    kill $pids
    echo "已停止 gateway（PID: $(echo "$pids" | tr '\n' ' ')）"
  fi
  if [[ -f "$ROUTER_PID_FILE" ]]; then
    rpid=$(cat "$ROUTER_PID_FILE")
    kill "$rpid" 2>/dev/null || true
    rm -f "$ROUTER_PID_FILE"
    echo "已停止由 gateway.sh 拉起的路由层（PID: $rpid）"
  else
    echo "（路由层由外部启动，未干预；可用 scripts/router.sh stop 停止）"
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
CLUSTER_ADDRS=""
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
    for ((i = 0; i < NODES; i++)); do
      CLUSTER_ADDRS+="localhost:$((BASE_PORT + i)),"
    done
    CLUSTER_ADDRS="${CLUSTER_ADDRS%,}"
    GRPC_ARGS="-grpc-addr $ROUTER_ADDR"
    echo "==> 集群模式：${NODES} 节点（$CLUSTER_ADDRS），gateway 经路由层 $ROUTER_ADDR"
    ;;
  single)
    GRPC_ARGS="-grpc-addr $ROUTER_ADDR"
    CLUSTER_ADDRS="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"
    echo "==> 单机模式：gateway 经路由层 $ROUTER_ADDR（节点 $CLUSTER_ADDRS）"
    ;;
  -h|--help)
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *)
    echo "错误：未知模式 $MODE（可用 cluster / single / build / stop）" >&2
    exit 1
    ;;
esac

# ---------- 路由层（自动拉起） ----------
# gateway 只连路由层；未监听时自动构建并后台拉起（写转发/读均衡由 router
# 承担），PID 记录到 run/.router.pid，退出时一并清理。
router_listening() {
  local host="${ROUTER_ADDR%:*}"
  local port="${ROUTER_ADDR##*:}"
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

cleanup_router() {
  if [[ "$ROUTER_STARTED" -eq 1 ]] && [[ -f "$ROUTER_PID_FILE" ]]; then
    local rpid
    rpid=$(cat "$ROUTER_PID_FILE")
    kill "$rpid" 2>/dev/null || true
    rm -f "$ROUTER_PID_FILE"
    echo "已停止由 gateway.sh 拉起的路由层（PID: $rpid）"
  fi
}
trap cleanup_router EXIT INT TERM

ensure_router() {
  if router_listening; then
    echo "==> 路由层已在 $ROUTER_ADDR 运行（复用）"
    return
  fi
  if [[ ! -x "$ROUTER_BIN" ]]; then
    echo "==> 构建 stratum-router …"
    export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
    mkdir -p "$ROOT/run/bin" "$ROOT/run/gocache" "$ROOT/run/gotmp"
    go build -o "$ROUTER_BIN" ./cmd/stratum-router/
  fi
  echo "==> 自动启动路由层（$ROUTER_ADDR，节点 $CLUSTER_ADDRS）…"
  mkdir -p "$ROOT/run/log"
  nohup "$ROUTER_BIN" -listen "$ROUTER_ADDR" -nodes "$CLUSTER_ADDRS" \
    >"$ROOT/run/log/router.log" 2>&1 &
  echo $! >"$ROUTER_PID_FILE"
  ROUTER_STARTED=1
  for _ in $(seq 1 20); do
    if router_listening; then
      return
    fi
    sleep 0.5
  done
  echo "错误：路由层启动失败，请查看 $ROOT/run/log/router.log" >&2
  cleanup_router
  exit 1
}

# ---------- 启动 ----------
ensure_router
echo "==> 启动 stratum-gateway（控制台 http://localhost:${HTTP_ADDR##*:}）…"
echo "    （Ctrl+C 停止 gateway；本脚本拉起的路由层会一并清理）"
exec "$BIN" $GRPC_ARGS \
  -http-addr "$HTTP_ADDR" \
  -static "$STATIC" \
  -ops-config "$OPS_CONFIG"
