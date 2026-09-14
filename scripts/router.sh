#!/usr/bin/env bash
# router.sh — stratum-router 服务站启停脚本
#
# 服务站（stratum-router，早期文档里叫"路由层"，是同一个二进制）是集群前端：
# 外部客户端（含 gateway）只连它，由它负责 leader 发现、写转发与读负载均衡，
# 以及 §9 的鉴权闸门（-tokens）与健康熔断。
#
# 模式：
#   cluster（默认）  Docker 集群模式：从 run/console.yaml 的 docker 段
#                    读取节点数/基础端口，拼接节点地址并启动服务站。
#   single           单机模式：连 127.0.0.1:7000（可用 STRATUM_GRPC_ADDR 覆盖）。
#   build            强制重新构建二进制后（默认模式）启动。
#   stop             停止正在运行的服务站。
#   status           查看服务站是否在监听。
#
# 用法：
#   scripts/router.sh              集群模式启动（零参数快捷启动）
#   scripts/router.sh single       单机模式启动
#   scripts/router.sh build        强制重新构建后启动
#   scripts/router.sh stop         停止服务站
#   scripts/router.sh status       查看状态
#
# 环境变量：
#   STRATUM_ROUTER_ADDR  服务站监听地址（默认 0.0.0.0:7009）
#   STRATUM_GRPC_ADDR    单机模式下服务站要连接的节点地址（默认 127.0.0.1:7000）
#   STRATUM_STORAGE_NODES 存储层节点地址（逗号分隔）。两层拓扑（控制组零存储）
#                        下必须设置：路由表问的是"哪些存储节点持有该版本"，而
#                        它要能把这些节点对上一个自己的连接。留空即全部节点同址
#                        （单层部署）。
#   STRATUM_ROUTE_REFRESH 路由表刷新间隔（默认 5s）

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
    echo "服务站正在运行：$ROUTER_ADDR"
  else
    echo "服务站未运行（$ROUTER_ADDR）"
  fi
  exit 0
fi

# ---------- 停止 ----------
if [[ "${1:-}" == "stop" ]]; then
  pids=$(pgrep -f "^$BIN " || true)
  if [[ -z "$pids" ]]; then
    echo "服务站未在运行"
  else
    kill $pids
    echo "已停止服务站（PID: $(echo "$pids" | tr '\n' ' ')）"
  fi
  exit 0
fi

# ---------- 构建 ----------
FORCE_BUILD=0
if [[ "${1:-}" == "build" ]]; then
  FORCE_BUILD=1
  shift
fi

# 二进制可能早于 `-storage-nodes` 这个 flag 存在（本次实测就踩到：9/11 的
# run/bin/stratum-router 不认识它，启动直接 "flag provided but not defined"）。
# 不自动重建的话有两种结局，一种吵一种静：吵的是启动失败；静的是操作者把两层
# 参数去掉，服务站退回"全部节点同址"的单层假设，于是读请求悄悄走错节点——那
# 正是服务站要消灭的那类失败。所以这里主动检测，不认识就重建。
if [[ -x "$BIN" ]] && ! "$BIN" -h 2>&1 | grep -q -- "-storage-nodes"; then
  echo "==> run/bin/stratum-router 过旧（不认识 -storage-nodes），重新构建 …"
  rm -f "$BIN"
fi

if [[ ! -x "$BIN" ]] || ((FORCE_BUILD)); then
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
    print(3, 17000, 0, 17100)
    raise SystemExit(0)
dk = d.get("docker") or {}
print(dk.get("nodes", 3), dk.get("base_port", 17000),
      dk.get("storage_nodes", 0), dk.get("storage_base_port", 17100))
EOF
)"
    read -r NODES BASE_PORT STORAGE_COUNT STORAGE_BASE_PORT <<<"$NODES_BASE"
    for ((i = 0; i < NODES; i++)); do
      CLUSTER_ADDRS+="localhost:$((BASE_PORT + i)),"
    done
    CLUSTER_ADDRS="${CLUSTER_ADDRS%,}"
    echo "==> 集群模式：${NODES} 节点（$CLUSTER_ADDRS）"
    # 两层拓扑（控制组零存储）下，路由表问的是"哪些**存储**节点持有该版本"，
    # 服务站必须能把存储层节点对上一个自己的连接。console.yaml 里已有
    # storage_nodes / storage_base_port，就别再要求操作者手填环境变量：
    # 漏填不会报错，只会让服务站退回"全部节点同址"的单层假设，而那种错误
    # 表现为读请求悄悄走错节点——正是服务站要消灭的那类失败。
    if [[ -z "${STRATUM_STORAGE_NODES:-}" ]] && ((STORAGE_COUNT > 0)); then
      for ((i = 0; i < STORAGE_COUNT; i++)); do
        STRATUM_STORAGE_NODES+="localhost:$((STORAGE_BASE_PORT + i)),"
      done
      STRATUM_STORAGE_NODES="${STRATUM_STORAGE_NODES%,}"
      echo "==> 两层拓扑：存储层 ${STORAGE_COUNT} 节点（$STRATUM_STORAGE_NODES）"
    fi
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
  echo "服务站已在 $ROUTER_ADDR 运行，无需重复启动"
  exit 0
fi

echo "==> 启动 stratum-router（监听 $ROUTER_ADDR，节点 $CLUSTER_ADDRS）…"
STORAGE_ADDRS="${STRATUM_STORAGE_NODES:-}"
if [[ -n "$STORAGE_ADDRS" ]]; then
  echo "==> 存储层节点：$STORAGE_ADDRS"
fi
exec "$BIN" -listen "$ROUTER_ADDR" -nodes "$CLUSTER_ADDRS" -storage-nodes "$STORAGE_ADDRS"
