#!/usr/bin/env bash
# router.sh — stratum-router 路由层启停脚本
#
# 路由层（stratum-router）是集群前端：外部客户端（含 gateway）只连它，
# 由它负责 leader 发现、写转发与读负载均衡。
#
# 模式：
#   cluster（默认）  Docker 集群模式：从 run/console.yaml 的 docker 段
#                    读取节点数/基础端口，拼接节点地址并启动路由层。
#   single           单机模式：连 127.0.0.1:7000（可用 STRATUM_GRPC_ADDR 覆盖）。
#   build            强制重新构建二进制后（默认模式）启动。
#   stop             停止正在运行的路由层。
#   status           查看路由层是否在监听。
#
# 用法：
#   scripts/router.sh              集群模式启动（零参数快捷启动）
#   scripts/router.sh single       单机模式启动
#   scripts/router.sh build        强制重新构建后启动
#   scripts/router.sh stop         停止路由层
#   scripts/router.sh status       查看状态
#
# 环境变量：
#   STRATUM_ROUTER_ADDR  路由层监听地址（默认 0.0.0.0:7009）
#   STRATUM_GRPC_ADDR    单机模式下路由层要连接的节点地址（默认 127.0.0.1:7000）

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN="$ROOT/run/bin/stratum-router"
OPS_CONFIG="$ROOT/run/console.yaml"
ROUTER_ADDR="${STRATUM_ROUTER_ADDR:-0.0.0.0:7009}"

router_listening() {
  local host="${ROUTER_ADDR%:*}"
  local port="${ROUTER_ADDR##*:}"
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

# ---------- 状态 ----------
if [[ "${1:-}" == "status" ]]; then
  if router_listening; then
    echo "路由层正在运行：$ROUTER_ADDR"
  else
    echo "路由层未运行（$ROUTER_ADDR）"
  fi
  exit 0
fi

# ---------- 停止 ----------
if [[ "${1:-}" == "stop" ]]; then
  pids=$(pgrep -f "^$BIN " || true)
  if [[ -z "$pids" ]]; then
    echo "路由层未在运行"
  else
    kill $pids
    echo "已停止路由层（PID: $(echo "$pids" | tr '\n' ' ')）"
  fi
  exit 0
fi

# ---------- 构建 ----------
if [[ "${1:-}" == "build" ]]; then
  shift
fi
if [[ ! -x "$BIN" ]] || [[ "${1:-}" == "build" ]]; then
  echo "==> 构建 stratum-router …"
  export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
  mkdir -p "$ROOT/run/bin" "$ROOT/run/gocache" "$ROOT/run/gotmp"
  go build -o "$BIN" ./cmd/stratum-router/
fi

# ---------- 模式解析 ----------
MODE="${1:-cluster}"
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
    echo "==> 集群模式：${NODES} 节点（$CLUSTER_ADDRS）"
    ;;
  single)
    CLUSTER_ADDRS="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"
    echo "==> 单机模式：连节点 $CLUSTER_ADDRS"
    ;;
  -h|--help)
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *)
    echo "错误：未知模式 $MODE（可用 cluster / single / build / stop / status）" >&2
    exit 1
    ;;
esac

# ---------- 启动 ----------
if router_listening; then
  echo "路由层已在 $ROUTER_ADDR 运行，无需重复启动"
  exit 0
fi

echo "==> 启动 stratum-router（监听 $ROUTER_ADDR，节点 $CLUSTER_ADDRS）…"
exec "$BIN" -listen "$ROUTER_ADDR" -nodes "$CLUSTER_ADDRS"
