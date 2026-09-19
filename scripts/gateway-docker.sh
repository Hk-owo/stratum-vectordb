#!/usr/bin/env bash
# gateway-docker.sh — 把 stratum-gateway 跑进集群容器网络（stratum-net）。
#
# 为什么要这个模式：POST /api/query-text 由网关**代调用知识库的 embed 服务**，
# 而集群里 embed 的地址是容器名（如 http://stratum-embed:8080）。宿主进程解析不了
# 容器名，那个端点就只会回 503。把 gateway 放进同一个网络，这一跳就通了。
#
# 代价与前提：
#   - 服务站（stratum-router）是宿主进程，容器要经 host.docker.internal 访问它，
#     所以本脚本以 0.0.0.0:7009 启动它（只监听回环的话容器连不上）；
#   - 服务站自己要连控制节点，地址取宿主的端口映射（两层拓扑下 localhost:17000…
#     就是控制组）。
#
# 用法：
#   scripts/gateway-docker.sh           构建并启动（gateway 容器 + 服务站）
#   scripts/gateway-docker.sh stop      停止 gateway 容器与本脚本拉起的服务站
#   scripts/gateway-docker.sh logs      跟随容器日志
#
# 环境变量：
#   STRATUM_HTTP_PORT      宿主暴露的控制台端口（默认 8081）
#   STRATUM_CONTROL_ADDRS  服务站的节点列表（默认 localhost:17000,localhost:17001,localhost:17002）
#   STRATUM_IMAGE_TAG      gateway 镜像标签（默认 stratum-gateway:latest）

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NAME=stratum-gateway
NETWORK=stratum-net
HTTP_PORT="${STRATUM_HTTP_PORT:-8081}"
CONTROL_ADDRS="${STRATUM_CONTROL_ADDRS:-localhost:17000,localhost:17001,localhost:17002}"
# 两层拓扑下，读侧（Query/Admin 的存储半边）在存储组。空 = 与 controls 同一批
# （all-in-one），由下面的探测决定。
STORAGE_ADDRS="${STRATUM_STORAGE_ADDRS:-}"
IMAGE="${STRATUM_IMAGE_TAG:-stratum-gateway:latest}"
BIN="$ROOT/integration/docker/stratum-gateway"
ROUTER_BIN="$ROOT/run/bin/stratum-router"
ROUTER_PID_FILE="$ROOT/run/.router-docker.pid"
OPS_CONFIG="$ROOT/run/console.yaml"
STATIC_DIR="$ROOT/web/dist"

log() { printf '\033[1;36m[gateway-docker]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[gateway-docker]\033[0m %s\n' "$*" >&2; }

case "${1:-up}" in
  stop)
    if docker rm -f "$NAME" >/dev/null 2>&1; then log "已删除容器 $NAME"; else log "容器 $NAME 未在运行"; fi
    # setsid 之后 $! 是会话领导者的 PID，不一定是 router 自己；按命令行找更可靠。
    pids=$(pgrep -f "$ROUTER_BIN -listen 0.0.0.0:7009" || true)
    if [[ -n "$pids" ]]; then
      kill $pids 2>/dev/null || true
      log "已停止本脚本拉起的服务站（PID: $(echo "$pids" | tr '\n' ' ')）"
    fi
    rm -f "$ROUTER_PID_FILE"
    exit 0
    ;;
  logs)
    exec docker logs -f "$NAME"
    ;;
  up | "") ;;
  -h | --help)
    sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *)
    warn "未知参数 $1（试试 --help）"
    exit 2
    ;;
esac

if [[ ! -d "$STATIC_DIR" ]]; then
  warn "找不到 $STATIC_DIR —— 先构建前端：npm --prefix web run build"
  exit 1
fi
if [[ ! -f "$OPS_CONFIG" ]]; then
  warn "找不到 $OPS_CONFIG（先用 scripts/docker-cluster.sh init 或 start.sh 生成）"
  exit 1
fi

# ---------- 二进制与镜像 ----------
log "构建 stratum-gateway（静态）…"
mkdir -p "$ROOT/run/gocache" "$ROOT/run/gotmp"
(
  export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
  CGO_ENABLED=0 go build -o "$BIN" ./cmd/stratum-gateway/
)

log "构建镜像 $IMAGE …"
docker build -q -t "$IMAGE" -f integration/docker/Dockerfile.gateway . >/dev/null

# ---------- 服务站 ----------
# 容器要经 host.docker.internal 访问它，所以监听 0.0.0.0（只监听回环的话容器连不上）。
if (exec 3<>/dev/tcp/127.0.0.1/7009) 2>/dev/null; then
  log "服务站已在 127.0.0.1:7009（复用；若它只听回环，容器会连不上——见 --help）"
else
  if [[ ! -x "$ROUTER_BIN" ]]; then
    log "构建 stratum-router …"
    (
      export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
      go build -o "$ROUTER_BIN" ./cmd/stratum-router/
    )
  fi
  # 两层拓扑靠探测：存储组在 17100 上监听，all-in-one 没有这个层。少了这一步，
  # HealthCheck / Query 会被转发到控制节点，而它们根本没注册这些服务（答案会是
  # Unimplemented，看起来像后端坏了）。
  if [[ -z "$STORAGE_ADDRS" ]] && (exec 3<>/dev/tcp/127.0.0.1/17100) 2>/dev/null; then
    STORAGE_ADDRS="localhost:17100,localhost:17101,localhost:17102"
  fi
  ROUTER_ARGS=(-listen 0.0.0.0:7009 -nodes "$CONTROL_ADDRS")
  if [[ -n "$STORAGE_ADDRS" ]]; then
    ROUTER_ARGS+=(-storage-nodes "$STORAGE_ADDRS")
    log "启动服务站（0.0.0.0:7009，控制组 $CONTROL_ADDRS，存储组 $STORAGE_ADDRS）…"
  else
    log "启动服务站（0.0.0.0:7009，节点 $CONTROL_ADDRS，无独立存储组）…"
  fi
  mkdir -p "$ROOT/run/log"
  # setsid 是必须的，不是保险：nohup 只挡 SIGHUP，而脚本退出（或它的调用方被清理）
  # 会把整个进程组一起收走——docker-cluster.sh 对 vecstore 用的是同一条理由。
  setsid nohup "$ROUTER_BIN" "${ROUTER_ARGS[@]}" >"$ROOT/run/log/router.log" 2>&1 &
  echo $! >"$ROUTER_PID_FILE"
  sleep 1
fi

# ---------- 容器 ----------
# 静态前端与 ops 配置是**挂载**而不是打进镜像：改前端不该需要重建镜像。
docker rm -f "$NAME" >/dev/null 2>&1 || true
log "启动容器 $NAME（宿主 :$HTTP_PORT → 容器 :8081，网络 $NETWORK）…"
docker run -d --name "$NAME" \
  --network "$NETWORK" \
  -p "${HTTP_PORT}:8081" \
  --add-host host.docker.internal:host-gateway \
  -v "$STATIC_DIR:/app/web/dist:ro" \
  -v "$OPS_CONFIG:/app/run/console.yaml:ro" \
  "$IMAGE" \
  -grpc-addr host.docker.internal:7009 \
  -http-addr 0.0.0.0:8081 \
  -static /app/web/dist \
  -ops-config /app/run/console.yaml >/dev/null

sleep 2
if curl -fsS "http://127.0.0.1:${HTTP_PORT}/api/health" >/dev/null 2>&1; then
  log "就绪：控制台 http://localhost:${HTTP_PORT}（gateway 在 $NETWORK 内，容器名形式的 embed 地址可直接访问）"
else
  warn "容器已起但 /api/health 未通过；看日志：scripts/gateway-docker.sh logs"
fi
