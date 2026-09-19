#!/usr/bin/env bash
# gateway.sh — Stratum 本地入口：服务站（stratum-router）+ 控制台（stratum-gateway），
# 需要时再带上数据库三件套（vecstore / embed / stratum）——也就是原来 start.sh 的一键形态。
#
# 为什么是这两个进程：控制台是 HTTP/JSON → gRPC 的网关，它只连**服务站**；leader 发现、
# 写转发、读均衡全部由服务站承担，所以控制台不需要知道集群拓扑。服务站自己连节点，
# 地址来自下面的「模式」。
#
# 命令：
#   up                            拉起服务站与控制台（默认命令）
#   stop                          停止控制台、本脚本拉起的服务站、容器形态的控制台
#   status                        控制台 / 服务站 / 数据库三件套的状态
#   logs [目标] [--lines N] [-f]  跟日志：gateway（默认）｜router｜vecstore/embed/stratum
#   build [--no-frontend]         强制重建控制台与服务站二进制（默认也重建前端）
#   db <start|stop|restart|status>  经 /ops 管理数据库三件套
#   router <up|stop|status|logs>  只操作服务站
#
# 模式（up）：
#   --single            服务站只连本机单节点：127.0.0.1:7000（STRATUM_GRPC_ADDR 可覆盖）
#   --cluster           服务站地址从 run/console.yaml 的 docker 段派生（控制组 17000+，
#                       两层再加存储组 17100+）。缺省自动判断：那份配置有 docker 段就按
#                       集群派生，否则退回单机——不会像以前那样在单机配置下悄悄去连
#                       17000-17002 那些根本没起的端口。
#   --in-docker         控制台跑进集群容器网络（stratum-net）。集群里的 embed 地址是容器名
#                       （http://stratum-embed:8080），宿主进程解析不了，只有把控制台放进
#                       同一个网络，POST /api/query-text 才够得着它。此时服务站必须监听
#                       0.0.0.0:7009（容器经 host.docker.internal 访问）。
#   --with-db           经 /ops/start 一并拉起数据库三件套（= 原来 start.sh 的一键），
#                       退出时经 /ops/stop 停掉它们。首次运行会生成 run/console.yaml
#                       与缺失的二进制（含 C++ 的 vecstore_server，首次较慢）。
#   --detach            前台是默认（Ctrl+C 全停）；加它则后台运行
#
# 环境变量：
#   STRATUM_HTTP_ADDR     控制台监听地址（默认 0.0.0.0:8081）
#   STRATUM_ROUTER_ADDR   服务站监听地址（默认 127.0.0.1:7009；--in-docker 用 0.0.0.0:7009）
#   STRATUM_GRPC_ADDR     单机模式服务站要连的节点地址（默认 127.0.0.1:7000）
#   STRATUM_STORAGE_NODES 存储层节点地址（逗号分隔）：两层拓扑下读侧只在存储组，留空表示
#                         全部节点同址（单层部署）
#   STRATUM_HTTP_PORT     --in-docker：宿主暴露的控制台端口（默认 8081）
#   STRATUM_IMAGE_TAG     --in-docker：控制台镜像标签（默认 stratum-gateway:latest）
#   STRATUM_NETWORK       --in-docker：Docker 网络（默认 stratum-net）
#   STRATUM_VECSTORE_ADDR / STRATUM_GRPC_ADDR  --with-db 首次生成 run/console.yaml 时用
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GATEWAY_BIN="$ROOT/run/bin/stratum-gateway"
STATION_BIN="$ROOT/run/bin/stratum-router"
OPS_CONFIG="$ROOT/run/console.yaml"
STATIC="$ROOT/web/dist"
LOG_DIR="$ROOT/run/log"
STATION_PID_FILE="$ROOT/run/.station.pid"
GATEWAY_PID_FILE="$ROOT/run/.gateway.pid"

HTTP_ADDR="${STRATUM_HTTP_ADDR:-0.0.0.0:8081}"
HTTP_PORT="${HTTP_ADDR##*:}"
ROUTER_ADDR="${STRATUM_ROUTER_ADDR:-127.0.0.1:7009}"

MODE_AUTO=1
MODE=""
IN_DOCKER=0
WITH_DB=0
DETACH=0
FORCE_BUILD=0
NO_FRONTEND=0
FOLLOW=0
LINES=200

log()  { printf '\033[1;36m[gateway]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[gateway]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[gateway]\033[0m 错误：%s\n' "$*" >&2; exit 1; }
# 头注释就是手册：从第 2 行打到第一个非注释行，行号不会随注释增减而漂。
usage() { awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; }

# ---------- 小工具 ----------
router_listening() {
  local host="${ROUTER_ADDR%:*}" port="${ROUTER_ADDR##*:}"
  [[ "$host" == "0.0.0.0" || "$host" == "*" ]] && host=127.0.0.1
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

# 容器是否存在/状态。不用 `docker inspect NAME` 的退出码判断：docker 在容器、镜像、
# 网络里按名字找，`docker inspect stratum-gateway` 会命中 stratum-gateway:latest
# 这个镜像并成功返回，而那个对象根本没有 State。
container_exists() {
  [[ -n "$(docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true)" ]]
}
container_state() {
  docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true
}

station_pid() {
  [[ -f "$STATION_PID_FILE" ]] || return 1
  local pid; pid="$(cat "$STATION_PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

gateway_pids() { pgrep -f "^$GATEWAY_BIN " || true; }

# ops <method> <path> [json]：控制台自己的管理接口（/ops/*）。
ops() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS --max-time 10 -X "$method" "http://127.0.0.1:${HTTP_PORT}${path}" \
      -H 'Content-Type: application/json' --data "$body"
  else
    curl -sS --max-time 10 -X "$method" "http://127.0.0.1:${HTTP_PORT}${path}"
  fi
}

console_ready() { curl -sf "http://127.0.0.1:${HTTP_PORT}/ops/health" >/dev/null 2>&1; }

# console_docker_line：从 run/console.yaml 的 docker 段读 "节点数 起始端口 存储节点数 存储起始端口"。
# 没有文件、没有 docker 段、没有 PyYAML 时都输出空（由调用方决定退回哪种形态）。
console_docker_line() {
  [[ -f "$OPS_CONFIG" ]] || return 0
  OPS_CONFIG="$OPS_CONFIG" python3 - <<'EOF' 2>/dev/null || true
import os, yaml
try:
    d = yaml.safe_load(open(os.environ["OPS_CONFIG"])) or {}
except Exception:
    raise SystemExit(0)
dk = d.get("docker") or {}
if dk:
    print(dk.get("nodes", 3), dk.get("base_port", 17000),
          dk.get("storage_nodes", 0), dk.get("storage_base_port", 17100))
EOF
}

# ---------- 1. 构建 ----------
build_gateway() {
  export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
  mkdir -p "$ROOT/run/bin" "$ROOT/run/gocache" "$ROOT/run/gotmp" "$LOG_DIR"
  log "构建 stratum-gateway …"
  go build -o "$GATEWAY_BIN" ./cmd/stratum-gateway/
}

build_station() {
  export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
  mkdir -p "$ROOT/run/bin" "$ROOT/run/gocache" "$ROOT/run/gotmp" "$LOG_DIR"
  log "构建 stratum-router（服务站）…"
  go build -o "$STATION_BIN" ./cmd/stratum-router/
}

# 前端是 gateway 的 -static 目标，缺了控制台页面会 404。构建失败只警告：API 与
# /ops 不受影响，把它们一起拖垮不划算。
build_frontend() {
  if [[ "$NO_FRONTEND" -eq 1 ]]; then
    log "前端：已跳过（--no-frontend）"
    return 0
  fi
  if ! command -v npm >/dev/null 2>&1; then
    warn "未找到 npm，跳过前端构建（控制台静态页面不可用，API 与 /ops 不受影响）"
    return 0
  fi
  if [[ ! -d "$ROOT/web/node_modules" ]]; then
    npm --prefix "$ROOT/web" install --silent || warn "npm install 失败"
  fi
  log "构建前端 web/dist …"
  npm --prefix "$ROOT/web" run build >/dev/null \
    || warn "前端构建失败，控制台静态页面将不可用（API 不受影响）"
}

# --with-db 需要 run/bin 下的 vecstore_server / stratum / mock-embed：缺了就补。
# 控制台管的这三个进程就是它们（/ops/start 按 console.yaml 的 bin_dir 找）。
ensure_db_binaries() {
  export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
  mkdir -p "$ROOT/run/bin" "$ROOT/run/gocache" "$ROOT/run/gotmp"
  [[ -x "$ROOT/run/bin/stratum" ]] || { log "构建 stratum …"; go build -o "$ROOT/run/bin/stratum" ./cmd/stratum/; }
  [[ -x "$ROOT/run/bin/mock-embed" ]] || { log "构建 mock-embed …"; go build -o "$ROOT/run/bin/mock-embed" ./integration/docker/mock_embed_server.go; }
  if [[ ! -x "$ROOT/run/bin/vecstore_server" ]]; then
    log "构建 vecstore_server（C++，首次较慢）…"
    cmake -S . -B "$ROOT/run/cmake" >/dev/null
    cmake --build "$ROOT/run/cmake" --target vecstore_server -j"$(nproc)" >/dev/null
    cp "$ROOT/run/cmake/vecstore/vecstore_server" "$ROOT/run/bin/vecstore_server"
  fi
}

cmd_build() {
  build_gateway
  build_station
  build_frontend
}

# ---------- 2. 地址解析 ----------
# 服务站连的节点地址（CLUSTER_ADDRS）与存储组地址（STORAGE_ADDRS）。两层拓扑里读侧
# （Query / HealthCheck 的存储半边）只在存储组：漏掉它，读请求会被转到根本提供不了
# 该服务的控制节点上，答案看起来像后端坏了。
resolve_addrs() {
  local docker_line; docker_line="$(console_docker_line)"

  if [[ "$MODE_AUTO" -eq 1 ]]; then
    if [[ -n "${docker_line// /}" ]]; then
      MODE=cluster
    else
      MODE=single
      [[ -f "$OPS_CONFIG" ]] && log "run/console.yaml 里没有 docker 段 → 按单机形态连接（--cluster 可强制集群形态）"
    fi
  fi

  if [[ "$MODE" == "single" ]]; then
    CLUSTER_ADDRS="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"
    STORAGE_ADDRS="${STRATUM_STORAGE_NODES:-}"
    log "单机形态：服务站连 $CLUSTER_ADDRS"
    return 0
  fi

  local nodes base storage_count storage_base i
  if [[ -n "${docker_line// /}" ]]; then
    read -r nodes base storage_count storage_base <<<"$docker_line"
  else
    nodes="${STRATUM_DOCKER_NODES:-3}"
    base="${STRATUM_DOCKER_BASE_PORT:-17000}"
    storage_count="${STRATUM_DOCKER_STORAGE_NODES:-0}"
    storage_base="${STRATUM_DOCKER_STORAGE_BASE_PORT:-17100}"
  fi
  CLUSTER_ADDRS=""
  for ((i = 0; i < nodes; i++)); do CLUSTER_ADDRS+="localhost:$((base + i)),"; done
  CLUSTER_ADDRS="${CLUSTER_ADDRS%,}"
  STORAGE_ADDRS="${STRATUM_STORAGE_NODES:-}"
  if [[ -z "$STORAGE_ADDRS" ]] && ((storage_count > 0)); then
    for ((i = 0; i < storage_count; i++)); do STORAGE_ADDRS+="localhost:$((storage_base + i)),"; done
    STORAGE_ADDRS="${STORAGE_ADDRS%,}"
  fi
  log "集群形态：节点 $CLUSTER_ADDRS${STORAGE_ADDRS:+｜存储组 $STORAGE_ADDRS}"
}

# ---------- 3. 服务站 ----------
# 二进制可能早于 -storage-nodes 存在（实测踩过：run/bin 里的旧二进制不认识它，启动
# 直接 "flag provided but not defined"）。吵的结局是启动失败；静的是操作者把两层参数
# 去掉，服务站退回「全部节点同址」的单层假设，读请求于是悄悄走错节点——那正是服务站
# 存在的理由。所以这里主动检测，不认识就重建。
ensure_station_binary() {
  if [[ -x "$STATION_BIN" ]] && ! "$STATION_BIN" -h 2>&1 | grep -q -- "-storage-nodes"; then
    log "run/bin/stratum-router 过旧（不认识 -storage-nodes），重新构建 …"
    rm -f "$STATION_BIN"
  fi
  [[ -x "$STATION_BIN" ]] && ((FORCE_BUILD == 0)) || build_station
}

ensure_station() {
  if router_listening; then
    log "服务站已在 $ROUTER_ADDR 运行（复用；外部进程不归本脚本管）"
    [[ "$IN_DOCKER" -eq 1 ]] && \
      warn "若那个服务站只听回环，容器会连不上它——用 scripts/cluster.sh station 或本脚本 --in-docker 拉一个"
    return 0
  fi
  ensure_station_binary
  log "启动服务站（$ROUTER_ADDR｜节点 $CLUSTER_ADDRS${STORAGE_ADDRS:+｜存储组 $STORAGE_ADDRS}）…"
  mkdir -p "$LOG_DIR"
  if [[ -n "$STORAGE_ADDRS" ]]; then
    nohup "$STATION_BIN" -listen "$ROUTER_ADDR" -nodes "$CLUSTER_ADDRS" -storage-nodes "$STORAGE_ADDRS" \
      >>"$LOG_DIR/router.log" 2>&1 &
  else
    nohup "$STATION_BIN" -listen "$ROUTER_ADDR" -nodes "$CLUSTER_ADDRS" >>"$LOG_DIR/router.log" 2>&1 &
  fi
  STATION_STARTED=1
  echo $! >"$STATION_PID_FILE"
  local waited=0
  while ! router_listening; do
    if ((waited++ >= 40)); then
      warn "服务站 4 秒内未监听 $ROUTER_ADDR（见 $LOG_DIR/router.log）"
      rm -f "$STATION_PID_FILE"
      return 1
    fi
    sleep 0.1
  done
  log "服务站已就绪：$ROUTER_ADDR（日志 $LOG_DIR/router.log）"
}

stop_station() {
  local pid
  if pid="$(station_pid)"; then
    kill "$pid" 2>/dev/null || true
    log "已停止本脚本拉起的服务站（PID $pid）"
  fi
  rm -f "$STATION_PID_FILE"
}

# ---------- 4. 控制台配置 ----------
ensure_console_yaml() {
  [[ -f "$OPS_CONFIG" ]] && return 0
  local BIN="$ROOT/run/bin" DATA="$ROOT/run/data"
  local VECSTORE_ADDR="${STRATUM_VECSTORE_ADDR:-127.0.0.1:7100}"
  local GRPC_ADDR="${STRATUM_GRPC_ADDR:-127.0.0.1:7000}"
  local GRPC_PORT="${GRPC_ADDR##*:}"
  mkdir -p "$ROOT/run/configs" "$DATA"
  log "生成控制台配置 $OPS_CONFIG（之后以「运维」页保存的配置为准）"
  cat > "$OPS_CONFIG" <<EOF
node_id: 1
bin_dir: "$BIN"
log_dir: "$LOG_DIR"
config_dir: "$ROOT/run/configs"
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
}

# ---------- 5. 控制台 ----------
start_gateway_host() {
  if [[ -n "$(gateway_pids)" ]]; then
    log "控制台已在运行（PID: $(gateway_pids | tr '\n' ' ')），复用"
    return 0
  fi
  log "启动 stratum-gateway（控制台 http://localhost:${HTTP_PORT}）…"
  mkdir -p "$LOG_DIR"
  local args=(-grpc-addr "$ROUTER_ADDR" -http-addr "$HTTP_ADDR" -static "$STATIC" -ops-config "$OPS_CONFIG")
  if [[ "$DETACH" -eq 1 ]]; then
    nohup "$GATEWAY_BIN" "${args[@]}" >>"$LOG_DIR/gateway.log" 2>&1 &
    echo $! >"$GATEWAY_PID_FILE"
    log "已在后台启动（PID $!；日志 $LOG_DIR/gateway.log）"
  else
    "$GATEWAY_BIN" "${args[@]}" >>"$LOG_DIR/gateway.log" 2>&1 &
    GATEWAY_PID=$!
    log "已启动（PID $GATEWAY_PID；日志 $LOG_DIR/gateway.log）"
  fi
}

stop_gateway_host() {
  local pids; pids="$(gateway_pids)"
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    log "已停止控制台（PID: $(echo "$pids" | tr '\n' ' ')）"
  fi
  if [[ -f "$GATEWAY_PID_FILE" ]]; then
    local p; p="$(cat "$GATEWAY_PID_FILE" 2>/dev/null || true)"
    [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
    rm -f "$GATEWAY_PID_FILE"
  fi
}

start_gateway_docker() {
  local NAME=stratum-gateway IMAGE="${STRATUM_IMAGE_TAG:-stratum-gateway:latest}"
  local NETWORK="${STRATUM_NETWORK:-stratum-net}"
  local PORT="${STRATUM_HTTP_PORT:-8081}"

  [[ -d "$STATIC" ]] || die "找不到 $STATIC —— 先构建前端：npm --prefix web run build（或跑 scripts/gateway.sh build）"

  log "构建控制台静态二进制与镜像 $IMAGE …"
  mkdir -p "$ROOT/run/gocache" "$ROOT/run/gotmp"
  ( export GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp"
    CGO_ENABLED=0 go build -o "$ROOT/integration/docker/stratum-gateway" ./cmd/stratum-gateway/ )
  docker build -q -t "$IMAGE" -f integration/docker/Dockerfile.gateway . >/dev/null

  docker rm -f "$NAME" >/dev/null 2>&1 || true
  log "启动容器 $NAME（宿主 :$PORT → 容器 :8081，网络 $NETWORK）…"
  # 静态前端与 ops 配置是**挂载**而不是打进镜像：改前端不该需要重建镜像。
  docker run -d --name "$NAME" \
    --network "$NETWORK" \
    -p "${PORT}:8081" \
    --add-host host.docker.internal:host-gateway \
    -v "$STATIC:/app/web/dist:ro" \
    -v "$OPS_CONFIG:/app/run/console.yaml:ro" \
    "$IMAGE" \
    -grpc-addr host.docker.internal:7009 \
    -http-addr 0.0.0.0:8081 \
    -static /app/web/dist \
    -ops-config /app/run/console.yaml >/dev/null
  if wait_http_ready "/ops/health" 10; then
    log "就绪：控制台 http://localhost:${PORT}（容器在 $NETWORK 内，容器名形式的 embed 地址可直接访问）"
  else
    warn "容器已起但 /ops/health 未通过；看日志：scripts/gateway.sh logs gateway"
  fi
}

wait_http_ready() {
  local path="$1" tries="${2:-30}" i
  for ((i = 0; i < tries; i++)); do
    curl -sf "http://127.0.0.1:${HTTP_PORT}${path}" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# ---------- 6. up / stop / status / logs / db / router ----------
cmd_up() {
  if [[ "$IN_DOCKER" -eq 1 && "$WITH_DB" -eq 1 ]]; then
    die "--in-docker 与 --with-db 不能同用：数据库三件套是**宿主**进程，容器里的控制台管不到它们"
  fi

  resolve_addrs
  if [[ "$FORCE_BUILD" -eq 1 ]]; then cmd_build; else build_frontend_if_missing; fi
  [[ -d "$STATIC" || "$NO_FRONTEND" -eq 1 ]] || warn "前端产物缺失，控制台页面会 404（API 与 /ops 不受影响）"

  if [[ "$IN_DOCKER" -eq 1 ]]; then
    # 容器要经 host.docker.internal 访问服务站，所以它必须监听 0.0.0.0。
    case "$ROUTER_ADDR" in
      0.0.0.0:*|"*":*) ;;
      *) ROUTER_ADDR="0.0.0.0:${ROUTER_ADDR##*:}" ;;
    esac
  fi
  ensure_station || die "服务站没起来"

  [[ -x "$GATEWAY_BIN" ]] || build_gateway
  ensure_console_yaml

  if [[ "$IN_DOCKER" -eq 1 ]]; then
    start_gateway_docker
    return 0
  fi

  [[ "$WITH_DB" -eq 1 ]] && ensure_db_binaries
  start_gateway_host

  # 前台运行时，退出/Ctrl+C 把控制台、本脚本拉起的服务站、以及 --with-db 拉起的数据库
  # 服务一起收掉，不留孤儿。
  cleanup() {
    echo
    log "停止：${WITH_DB:+数据库服务 + }控制台 + 本脚本拉起的服务站 …"
    [[ "$WITH_DB" -eq 1 ]] && ops POST "/ops/stop" '{}' >/dev/null 2>&1 || true
    if [[ "$DETACH" -eq 1 ]]; then
      stop_gateway_host
    elif [[ -n "${GATEWAY_PID:-}" ]]; then
      kill "$GATEWAY_PID" 2>/dev/null || true
      wait "$GATEWAY_PID" 2>/dev/null || true
    fi
    [[ "${STATION_STARTED:-0}" -eq 1 ]] && stop_station
    return 0
  }
  trap cleanup EXIT INT TERM

  if [[ "$WITH_DB" -eq 1 ]]; then
    if wait_http_ready "/ops/health" 30; then
      log "经 /ops/start 拉起数据库三件套（vecstore / embed / stratum）…"
      ops POST "/ops/start" '{}' >/dev/null 2>&1 || warn "启动请求失败，可到「运维」页手动操作"
      if wait_http_ready "/api/health" 30; then
        log "数据库已就绪"
      else
        warn "数据库尚未就绪，可查看日志：$LOG_DIR/"
      fi
    else
      warn "控制台未就绪，跳过 /ops/start（可稍后在「运维」页操作）"
    fi
  fi

  print_ready
  if [[ "$DETACH" -eq 1 ]]; then
    trap - EXIT INT TERM
    return 0
  fi
  if [[ -z "${GATEWAY_PID:-}" ]]; then
    # 复用了别人起的控制台：等它自己退出（Ctrl+C 结束等待，但不会去停它）。
    local owner; owner="$(gateway_pids | head -1)"
    log "控制台由其它进程管理（PID ${owner:-?}）；本脚本只等待（Ctrl+C 结束等待）"
    while kill -0 "${owner:-0}" 2>/dev/null; do sleep 2; done
    return 0
  fi
  wait "$GATEWAY_PID"
}

# 前端产物缺失时才补建（up 的默认路径；--no-frontend 下只警告）。
build_frontend_if_missing() {
  [[ -d "$STATIC" ]] && return 0
  build_frontend
}

print_ready() {
  echo
  echo "=============================================="
  echo "  Stratum 控制台已启动"
  echo "  浏览器访问：http://localhost:${HTTP_PORT}/"
  echo "  「运维」页：服务启停、启动参数、日志"
  echo "  日志目录：$LOG_DIR/"
  if [[ "$DETACH" -eq 1 ]]; then
    echo "  停止：scripts/gateway.sh stop"
  else
    echo "  按 Ctrl+C 停止控制台${WITH_DB:+（连数据库三件套一起停）}"
  fi
  echo "=============================================="
  echo
}

cmd_stop() {
  # 控制台还在时先让它收自己的子进程（它比外面逐一 kill 更清楚谁在管谁）。
  if console_ready; then
    ops POST "/ops/stop" '{}' >/dev/null 2>&1 || true
  fi
  if container_exists stratum-gateway; then
    docker rm -f stratum-gateway >/dev/null
    log "已删除控制台容器 stratum-gateway"
  fi
  stop_gateway_host
  stop_station
  # 旧脚本（router.sh / gateway-docker.sh 时代）留下的 PID 文件
  rm -f "$ROOT/run/.router.pid" "$ROOT/run/.router-docker.pid"
}

cmd_status() {
  echo "== 控制台"
  local pids; pids="$(gateway_pids)"
  if [[ -n "$pids" ]]; then
    echo "  运行中（PID: $(echo "$pids" | tr '\n' ' ')）"
  elif container_exists stratum-gateway; then
    echo "  容器 stratum-gateway：$(container_state stratum-gateway)"
  else
    echo "  未运行"
  fi
  console_ready && echo "  /ops/health：可达（http://127.0.0.1:${HTTP_PORT}）"

  echo "== 服务站"
  if router_listening; then
    local pid; pid="$(station_pid || true)"
    echo "  正在监听 $ROUTER_ADDR${pid:+（本脚本拉起，PID $pid）}"
  else
    echo "  未监听 $ROUTER_ADDR"
  fi

  echo "== 数据库三件套（经 /ops）"
  if console_ready; then
    ops GET "/ops/status" | jq -r '
      (.services // [])[] | "  \(.service): \(if .running then "running" else "stopped" end)\(if .pid then " (pid \(.pid))" else "" end)"' 2>/dev/null \
      || echo "  （/ops/status 解析失败：scripts/gateway.sh db status 看原始输出）"
  else
    echo "  控制台未就绪，无法查询（先 up）"
  fi
}

cmd_logs() {
  local target="${1:-gateway}"
  [[ $# -gt 0 ]] && shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --lines) LINES="$2"; shift 2 ;;
      -f|--follow) FOLLOW=1; shift ;;
      *) shift ;;
    esac
  done
  if [[ "$target" == "gateway" ]] && container_exists stratum-gateway; then
    if [[ "$FOLLOW" -eq 1 ]]; then docker logs -f --tail "$LINES" stratum-gateway
    else docker logs --tail "$LINES" stratum-gateway 2>&1; fi
    return 0
  fi
  local file
  case "$target" in
    gateway) file="$LOG_DIR/gateway.log" ;;
    router|station) file="$LOG_DIR/router.log" ;;
    *) file="$LOG_DIR/$target.log" ;;
  esac
  [[ -f "$file" ]] || die "没有日志文件 $file（现有：$(ls "$LOG_DIR" 2>/dev/null | tr '\n' ' '))"
  if [[ "$FOLLOW" -eq 1 ]]; then tail -f -n "$LINES" "$file"; else tail -n "$LINES" "$file"; fi
}

cmd_db() {
  local action="${1:-status}"
  case "$action" in
    start|stop|restart)
      console_ready || die "控制台未就绪（http://127.0.0.1:${HTTP_PORT}），先 scripts/gateway.sh up"
      log "POST /ops/$action …"
      ops POST "/ops/$action" '{}' | jq . 2>/dev/null || warn "请求失败，可到「运维」页手动操作"
      ;;
    status)
      console_ready || die "控制台未就绪（http://127.0.0.1:${HTTP_PORT}）"
      ops GET "/ops/status" | jq .
      ;;
    *) die "db 的动作只能是 start / stop / restart / status" ;;
  esac
}

cmd_router() {
  local action="${1:-status}"
  [[ $# -gt 0 ]] && shift
  case "$action" in
    up|start)
      resolve_addrs
      ensure_station || die "服务站没起来"
      ;;
    down|stop)  stop_station ;;
    status)
      resolve_addrs
      if router_listening; then
        local pid; pid="$(station_pid || true)"
        echo "服务站运行中：$ROUTER_ADDR${pid:+（本脚本拉起，PID $pid）}｜节点 $CLUSTER_ADDRS${STORAGE_ADDRS:+｜存储组 $STORAGE_ADDRS}"
      else
        echo "服务站未运行（$ROUTER_ADDR）"
      fi
      ;;
    logs)  cmd_logs router "$@" ;;
    *)     die "router 的动作只能是 up / stop / status / logs" ;;
  esac
}

# ---------- 参数解析 ----------
CMD=""
POS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --single)      MODE_AUTO=0; MODE=single; shift ;;
    --cluster)     MODE_AUTO=0; MODE=cluster; shift ;;
    --in-docker)   IN_DOCKER=1; shift ;;
    --with-db)     WITH_DB=1; shift ;;
    --detach|-d)   DETACH=1; shift ;;
    --build)       FORCE_BUILD=1; shift ;;
    --no-frontend) NO_FRONTEND=1; shift ;;
    --lines)       LINES="$2"; shift 2 ;;
    -f|--follow)   FOLLOW=1; shift ;;
    -h|--help)     usage; exit 0 ;;
    -*)            die "未知参数 $1（试试 --help）" ;;
    *)             if [[ -z "$CMD" ]]; then CMD="$1"; else POS+=("$1"); fi; shift ;;
  esac
done
CMD="${CMD:-up}"

mkdir -p "$LOG_DIR"

case "$CMD" in
  up|start)   cmd_up ;;
  stop|down)  cmd_stop ;;
  status)     cmd_status ;;
  logs)       cmd_logs ${POS[@]+"${POS[@]}"} ;;
  build)      cmd_build ;;
  db)         cmd_db ${POS[@]+"${POS[@]}"} ;;
  router)     cmd_router ${POS[@]+"${POS[@]}"} ;;
  *)          die "未知命令 $CMD（up / stop / status / logs / build / db / router）" ;;
esac
