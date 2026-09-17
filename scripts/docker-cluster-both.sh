#!/usr/bin/env bash
# docker-cluster-both.sh — a two-tier Stratum cluster: a control group and a
# storage group, in containers (Stratum_设计文档v13.md §11 阶段 ④「存储集群独立进程」).
#
# How this differs from scripts/docker-cluster.sh:
#
#   docker-cluster.sh          N uniform nodes, each an all-in-one stratum, each
#                              reaching a vecstore that runs ON THE HOST
#                              (host.docker.internal:710N).
#   docker-cluster-both.sh     a control group (role=control: Raft + metadata,
#                              no data plane at all) PLUS a storage group (role=storage:
#                              no Raft log at all, metadata read over gRPC from
#                              the control group). Every container carries its
#                              own vecstore — there is no host vecstore process.
#
# The storage group is what the split is for: writes are committed by the
# control group and then dispatched to the storage group, which is the only
# place documents, chunks and indexes live. Because each container owns its
# vecstore, the host no longer has to reach into a node's data directory, which
# is what the bind mount in docker-cluster.sh existed for.
#
# Identity spaces: control nodes are 1..N (Raft member IDs — the peers table),
# storage nodes are 11..1N. They must NOT overlap. The storage IDs also key
# storage.nodes, and the assembly resolves a node's own address by looking up its
# node_id there; an overlap would make a control node adopt a storage node's
# address as its own.
#
# Usage:
#   scripts/docker-cluster-both.sh build      build both images
#   scripts/docker-cluster-both.sh init       generate both tiers' configs only
#   scripts/docker-cluster-both.sh up         start control group, then storage group
#   scripts/docker-cluster-both.sh start|stop|restart [id...]   per node, by id
#   scripts/docker-cluster-both.sh logs <id> [--lines N]
#   scripts/docker-cluster-both.sh status     per-container state and the leader
#   scripts/docker-cluster-both.sh logs [name]
#   scripts/docker-cluster-both.sh down       remove all containers (volumes kept)
#   scripts/docker-cluster-both.sh station up|down|status|logs [lines]
#                                             the §9 station (host process, not a
#                                             container); `up` derives both address
#                                             lists from this script's own ports
#
# Options (the same shape docker-cluster.sh uses, with the tier named):
#   --control-nodes N        control-layer node count (default 3)
#   --storage-nodes N        storage-layer node count (default 3)
#   --control-base-port P    first control node's host gRPC port (default 17000)
#   --storage-base-port P    first storage node's host gRPC port (default 17100)
#   --network N              Docker network (default stratum-net)
#   --image IMG              image for every node (default stratum-storage:latest)
#   --no-embed               do not start the mock-embed container (default: start it)
#   --with-station           also start the station after `up`
#   --force                  rebuild containers that already exist
#   --json                   status as machine-readable JSON
#
# Environment:
#   STRATUM_REQUIRE_AUTH     "true" (default) makes nodes serve client-facing
#                            calls only when they carry a station's trust mark;
#                            "false" is for a harness that talks to node ports
#                            directly (see the note in the script)
#   STRATUM_STATION_ADDR     station listen address (default 0.0.0.0:7009, same as
#                            scripts/router.sh; the two are alternatives, not a pair —
#                            this script derives the node address lists itself)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/run/docker-both"
NODES_DIR="$RUN_DIR/nodes"
LOG_DIR="$RUN_DIR/log"
mkdir -p "$NODES_DIR" "$LOG_DIR"

NETWORK=stratum-net
IMAGE=stratum-storage:latest
EMBED_IMAGE=alpine:latest
CONTAINER_PREFIX=stratum-node

JSON_MODE=0
FORCE=0

# The generated node configs point every node's embed calls at the
# mock-embed container, so a cluster without it fails every write with
# "lookup stratum-embed: server misbehaving" — a DNS error that names the
# symptom but not the cause. Starting it is therefore the default, and
# `down` removing it is what `up` has to undo. `--no-embed` is for a harness
# that brings its own embed service.
WITH_EMBED=1

# Whether `up` also starts the station (§9). Off by default: the station is a
# host process the operator may already run from scripts/router.sh, and starting
# a second one would fight over the same port. `--with-station` is for the
# deployment that wants a single command to bring the whole topology up.
WITH_STATION=0

# Whether nodes accept client-facing calls only from a service station (§9.3(5)).
# On by default because that is the deployed shape: the station is the only
# public entry point, and port reachability must not be what decides who gets
# served.
#
# Turn it off (STRATUM_REQUIRE_AUTH=false) only for a harness that talks to node
# ports directly — the T4 tests do, which means those tests exercise a shape
# production does not have. That gap is worth knowing about rather than papering
# over: what they do not cover is the station in front.
REQUIRE_AUTH="${STRATUM_REQUIRE_AUTH:-true}"
CONTROL_COUNT="${STRATUM_CONTROL_COUNT:-3}"
STORAGE_COUNT="${STRATUM_STORAGE_COUNT:-3}"
CONTROL_BASE_PORT="${STRATUM_CONTROL_BASE_PORT:-17000}"
STORAGE_BASE_PORT="${STRATUM_STORAGE_BASE_PORT:-17100}"
STORAGE_ID_BASE=10

log()  { echo -e "\033[1;36m[cluster-both]\033[0m $*"; }
warn() { echo -e "\033[1;33m[cluster-both]\033[0m $*" >&2; }
die()  { echo -e "\033[1;31m[cluster-both]\033[0m 错误：$*" >&2; exit 1; }

control_name() { echo "${CONTAINER_PREFIX}-control${1}"; }
storage_name() { echo "${CONTAINER_PREFIX}-storage${1}"; }
storage_id()   { echo $((STORAGE_ID_BASE + ${1})); }
control_cfg()  { echo "$NODES_DIR/control${1}/config.yaml"; }
storage_cfg()  { echo "$NODES_DIR/storage${1}/config.yaml"; }
container_exists() { docker inspect "$1" >/dev/null 2>&1; }

# ---------- station (§9 服务站) ----------
#
# The station is the only client-facing entry point in this topology: every node
# runs with require_authenticated, so a caller that reaches a node port directly
# gets nowhere (§9.3(5)). That makes starting it part of standing the deployment
# up, not an afterthought — and it is why its start/stop lives HERE, deriving the
# two address lists from this script's own ports, instead of asking the operator
# to re-enter them in another script.
#
# The test suite does not depend on this: integration/docker's TestMain builds
# and starts its own station (STRATUM_T4_STATION_ADDR can point it at this one).
STATION_BIN="$ROOT/run/bin/stratum-router"
STATION_ADDR="${STRATUM_STATION_ADDR:-0.0.0.0:7009}"
STATION_PID_FILE="$RUN_DIR/station.pid"
STATION_LOG="$LOG_DIR/station.log"

# ---------- build ----------

cmd_build() {
  log "构建静态二进制（stratum）…"
  (cd "$ROOT" && CGO_ENABLED=0 go build -o integration/docker/stratum ./cmd/stratum/)
  log "构建 all-in-one 镜像（stratum-node:latest）…"
  docker build -t stratum-node:latest -f "$ROOT/integration/docker/Dockerfile" "$ROOT" >/dev/null
  log "构建 storage 镜像（$IMAGE，含 vecstore）…"
  "$ROOT/scripts/build-storage-image.sh" "$IMAGE" >/dev/null
  log "构建完成"
}

# ---------- network ----------

ensure_network() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || {
    log "创建 Docker 网络 $NETWORK …"
    docker network create "$NETWORK" >/dev/null
  }
}

# ---------- config generation ----------

# peers_block emits the control group's Raft membership. Every node in the
# cluster — control and storage alike — carries it: the control nodes use it as
# their Raft configuration, and a storage node uses it as the address table for
# the control cluster it reads metadata from (RemoteRaftNode).
peers_block() {
  local i
  for ((i = 1; i <= CONTROL_COUNT; i++)); do
    printf '    - id: %s\n      addr: "%s:8000"\n      service_addr: "%s:7000"\n' \
      "$i" "$(control_name "$i")" "$(control_name "$i")"
  done
}

# storage_block emits the storage group. On a control node it is the replica
# topology writes are dispatched to; on a storage node it is the replica set a
# write fans out to.
storage_block() {
  local i
  for ((i = 1; i <= STORAGE_COUNT; i++)); do
    printf '    - id: %s\n      addr: "%s:7000"\n' "$(storage_id "$i")" "$(storage_name "$i")"
  done
}

gen_control_config() {
  local i=$1 cfg
  cfg="$(control_cfg "$i")"
  mkdir -p "$(dirname "$cfg")"
  cat > "$cfg" <<EOF
# Control node $i — generated by scripts/docker-cluster-both.sh
#
# role=control: this node runs the Raft cluster and holds version metadata, and
# that is all it holds. It has no vecstore, no local stores and no indexes —
# every version's data work is dispatched to the storage group below, and reads
# are served there too (QueryService is a storage-side service, since it reads
# the index manager directly). Client calls reach it only through the station
# (§9); require_authenticated below is what enforces that.
node:
  node_id: $i
  role: control
  # §9.3(5): client-facing calls must arrive through a station. Its trust mark
  # is what they carry, so reaching this port directly gets a caller nowhere.
  require_authenticated: $REQUIRE_AUTH
  grpc_addr: "0.0.0.0:7000"
  raft_addr: "0.0.0.0:8000"

raft:
  heartbeat_interval_ms: 200
  election_timeout_min_ms: 2000
  election_timeout_max_ms: 4000
  peers:
$(peers_block)

storage:
  data_dir: "/var/lib/stratum/node${i}"
  nodes:
$(storage_block)

vecstore:
  # In-container: this image runs the vecstore itself (see Dockerfile.storage).
  grpc_addr: "127.0.0.1:7100"

embed:
  service_addr: "http://stratum-embed:8080"

logging:
  level: "info"
EOF
}

gen_storage_config() {
  local i=$1 id cfg
  id="$(storage_id "$i")"
  cfg="$(storage_cfg "$i")"
  mkdir -p "$(dirname "$cfg")"
  cat > "$cfg" <<EOF
# Storage node $i (node_id $id) — generated by scripts/docker-cluster-both.sh
#
# role=storage: no Raft log, no election, never a leader. raft.peers below is
# NOT this node's membership — it is the control cluster's address table, which
# is where this node reads replicated metadata from and reports progress to.
node:
  node_id: $id
  role: storage
  # §9.3(5): same rule as the control tier.
  require_authenticated: $REQUIRE_AUTH
  grpc_addr: "0.0.0.0:7000"
  raft_addr: "0.0.0.0:8000"

raft:
  # The control cluster. A storage node keeps no log of its own.
  peers:
$(peers_block)

storage:
  data_dir: "/var/lib/stratum/node${id}"
  nodes:
$(storage_block)

vecstore:
  grpc_addr: "127.0.0.1:7100"

embed:
  service_addr: "http://stratum-embed:8080"

# §8.6(d) in the test fixture. The scanner is always on (it only reads), but this
# cluster turns COLLECTION on too, with a short sweep interval: otherwise the
# pressure case would have to wait out the 10-minute default before anything could
# happen, and the whole chain (scan → decide → reopen/rebuild → save →
# redistribute) would never run in CI.
#
# append_max_dead_ratio is raised deliberately, and that is a FINDING rather than a
# convenience. §8.6(c) reuses a parent artifact only while its dead share stays
# under this ratio, and §8.6(d)'s collection threshold (GCRatioThreshold) defaults
# to the SAME 0.2. With both at 0.2 the two cancel out: anything dead enough for (d)
# to want is already dead enough for (c) to have rebuilt during the build, so the
# tombstones never survive to be collected and (d) can never fire. Measured:
# deleting 480 of 600 documents left a 362-line index (fully rebuilt, zero
# tombstones) instead of the ~1800-line artifact with tombstones that (d) needs.
#
# Raising (c)'s bar here lets artifacts keep their tombstones, which is the only way
# the (d) chain can be exercised at all. The production values for this pair need a
# deliberate decision — see §5 #14 in the design document.
index_manager:
  gc_enabled: true
  gc_sweep_interval_ms: 5000
  serving_replica_min: 2
  append_max_dead_ratio: 0.95

# Active lag detection (docs/active-lag-detection-design.md). There is no switch: a
# replica that fell behind catches up on its own. The knobs below set the pace.
lag_catchup:
  min_lag_versions: ${LAG_CATCHUP_MIN_LAG:-1}
  jitter_ms: ${LAG_CATCHUP_JITTER_MS:-0}
  max_concurrent_kbs: ${LAG_CATCHUP_MAX_KBS:-0}

logging:
  level: "${LOG_LEVEL:-info}"
EOF
}

# ---------- container lifecycle ----------

# run_node starts one container. Every node uses the storage image: it carries
# the vecstore, and a control node needs it too (role=all runs the index manager
# and the write transaction locally as a fallback). What separates the two tiers
# is the config's role, not the image.
run_node() {
  local name=$1 gport=$2 cfg=$3 data_volume=$4
  local mport=$((gport + 2000))

  if container_exists "$name"; then
    if [[ "$FORCE" -eq 1 ]]; then
      log "删除旧容器 $name（--force 重建）…"
      docker rm -f "$name" >/dev/null
    else
      docker start "$name" >/dev/null
      log "复用已存在的容器 $name"
      return
    fi
  fi

  log "启动 $name（gRPC 宿主 :$gport → 容器 7000）…"
  docker run -d --name "$name" \
    --network "$NETWORK" \
    -p "${gport}:7000" \
    -v "$cfg:/etc/stratum/config.yaml:ro" \
    -v "${data_volume}:/var/lib/stratum" \
    --restart unless-stopped \
    --health-cmd "bash -c 'echo > /dev/tcp/127.0.0.1/7000'" \
    --health-interval 5s --health-timeout 2s --health-retries 6 \
    "$IMAGE" >/dev/null
}

embed_start() {
  if container_exists stratum-embed; then
    docker start stratum-embed >/dev/null
    log "复用 mock-embed 容器 stratum-embed"
  else
    log "启动 mock-embed（宿主 :18080）…"
    docker run -d --name stratum-embed \
      --network "$NETWORK" \
      -p "18080:8080" \
      -v "$ROOT/integration/docker/mock-embed:/app/mock-embed:ro" \
      alpine:latest /app/mock-embed >/dev/null
  fi
}

wait_leader() {
  local timeout="${1:-60}" prev="" t max i name term
  log "等待控制组选出 leader（最多 ${timeout}s）…"
  for _ in $(seq 1 "$timeout"); do
    max=0; cur=""
    for ((i = 1; i <= CONTROL_COUNT; i++)); do
      name="$(control_name "$i")"
      container_exists "$name" || continue
      [[ "$(docker inspect -f '{{.State.Status}}' "$name")" == "running" ]] || continue
      term="$(docker logs "$name" 2>&1 | grep -o '"became leader"[^}]*' | tail -1 | grep -o 'term":[0-9]*' | cut -d: -f2 || true)"
      term="${term:-0}"
      if [[ "$term" -gt "$max" ]]; then max="$term"; cur="$name"; fi
    done
    if [[ -n "$cur" && "$cur" == "$prev" && "$max" -gt 0 ]]; then
      log "leader 已选出：$cur（term $max）"
      return 0
    fi
    prev="$cur"
    sleep 1
  done
  warn "等待 leader 超时（${timeout}s）；用 'logs' 查看各节点日志"
  return 1
}

# ---------- commands ----------

cmd_up() {
  ensure_network
  local i

  log "== 控制组（${CONTROL_COUNT} 节点，role=control，零数据层）=="
  for ((i = 1; i <= CONTROL_COUNT; i++)); do gen_control_config "$i"; done
  for ((i = 1; i <= CONTROL_COUNT; i++)); do
    run_node "$(control_name "$i")" "$((CONTROL_BASE_PORT + i - 1))" \
      "$(control_cfg "$i")" "stratum-control${i}-data"
  done
  wait_leader 60 || true

  log "== 存储组（${STORAGE_COUNT} 节点，role=storage，容器内含 vecstore）=="
  for ((i = 1; i <= STORAGE_COUNT; i++)); do gen_storage_config "$i"; done
  for ((i = 1; i <= STORAGE_COUNT; i++)); do
    run_node "$(storage_name "$i")" "$((STORAGE_BASE_PORT + i - 1))" \
      "$(storage_cfg "$i")" "stratum-storage-container${i}-data"
  done

  if [[ "$WITH_EMBED" -eq 1 ]]; then embed_start; fi
  # --with-station: bring the §9 entry point up too. Not the default (starting a
  # host process is a deployment decision, not a side effect of `up`), but the
  # two commands a two-tier deployment needs are one flag apart.
  if [[ "$WITH_STATION" -eq 1 ]]; then cmd_station_up; fi
  cmd_status
}

# container_for_id maps a node id to its container name. The two tiers share one
# id space — control nodes are 1..N and storage nodes are 11..1N — so the console
# can address any node the same way it addresses one in the all-in-one cluster.
container_for_id() {
  local id=$1
  if [[ "$id" -ge 1 && "$id" -le "$CONTROL_COUNT" ]]; then
    control_name "$id"; return 0
  fi
  local sid=$((id - STORAGE_ID_BASE))
  if [[ "$sid" -ge 1 && "$sid" -le "$STORAGE_COUNT" ]]; then
    storage_name "$sid"; return 0
  fi
  return 1
}

# all_ids lists every node id, control tier first.
all_ids() {
  local i
  for ((i = 1; i <= CONTROL_COUNT; i++)); do echo "$i"; done
  for ((i = 1; i <= STORAGE_COUNT; i++)); do storage_id "$i"; done
}

# cmd_init generates both tiers' configs without starting anything (idempotent).
cmd_init() {
  local i
  for ((i = 1; i <= CONTROL_COUNT; i++)); do gen_control_config "$i"; done
  for ((i = 1; i <= STORAGE_COUNT; i++)); do gen_storage_config "$i"; done
  log "已生成配置：控制组 ${CONTROL_COUNT} + 存储组 ${STORAGE_COUNT}"
}

# cmd_node_start / stop / restart act on the ids given, or on every node.
cmd_node_start() {
  local ids=("$@")
  [[ ${#ids[@]} -eq 0 ]] && mapfile -t ids < <(all_ids)
  local id name
  for id in "${ids[@]}"; do
    name="$(container_for_id "$id")" || die "未知节点 id $id"
    container_exists "$name" || die "容器不存在：$name（先 up）"
    docker start "$name" >/dev/null && log "启动 $name"
  done
}

cmd_node_stop() {
  local ids=("$@")
  [[ ${#ids[@]} -eq 0 ]] && mapfile -t ids < <(all_ids)
  local id name
  for id in "${ids[@]}"; do
    name="$(container_for_id "$id")" || die "未知节点 id $id"
    container_exists "$name" && { docker stop "$name" >/dev/null && log "停止 $name"; }
  done
}

cmd_node_restart() {
  cmd_node_stop "$@"
  cmd_node_start "$@"
}

cmd_down() {
  local ids
  ids="$(docker ps -aq --filter "name=^/${CONTAINER_PREFIX}-" 2>/dev/null || true)"
  if [[ -n "$ids" ]]; then
    docker rm -f $ids >/dev/null
    log "已删除容器：$ids"
  else
    log "没有需要删除的容器"
  fi
  docker rm -f stratum-embed >/dev/null 2>&1 && log "已删除 mock-embed" || true
  # The station is a host process, not a container, so tearing the deployment
  # down has to reach it explicitly — otherwise the next `up` leaves an orphan
  # serving a cluster whose nodes have all been recreated under it.
  cmd_station_down
}

# ---------- station commands ----------

# station_pid echoes the running station's pid, or fails.
station_pid() {
  [[ -f "$STATION_PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$STATION_PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

# station_listening reports whether something accepts connections on STATION_ADDR.
station_listening() {
  local host="${STATION_ADDR%:*}" port="${STATION_ADDR##*:}"
  # 0.0.0.0 is a bind address, not a dial address.
  [[ "$host" == "0.0.0.0" || "$host" == "*" ]] && host=127.0.0.1
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

# node_addr_list emits the host-side gRPC addresses of one tier.
node_addr_list() {
  local base=$1 count=$2 i out=""
  for ((i = 1; i <= count; i++)); do
    out+="localhost:$((base + i - 1)),"
  done
  echo "${out%,}"
}

cmd_station() {
  local action="${1:-status}"
  case "$action" in
    up)          cmd_station_up ;;
    down|stop)   cmd_station_down ;;
    status)      cmd_station_status ;;
    logs)        shift || true; cmd_station_logs "$@" ;;
    *)           die "station 的动作只能是 up / down / status / logs（收到「$action」）" ;;
  esac
}

cmd_station_up() {
  local pid
  if pid="$(station_pid)"; then
    log "服务站已在运行（PID $pid，$STATION_ADDR）"
    return 0
  fi

  # Listening, but not by a process this script started: almost always
  # scripts/router.sh, which is a supported alternative rather than a mistake.
  # Saying so beats a five-second wait followed by "did not start listening",
  # which reads like a bug in this script.
  if station_listening; then
    die "$STATION_ADDR 已在监听，但不是本脚本起的服务站；若那是 scripts/router.sh 起的，直接用它（scripts/router.sh status），或换一个端口：STRATUM_STATION_ADDR=0.0.0.0:7010"
  fi

  # Build every time rather than reusing run/bin as-is: `go build` is
  # incremental, and a binary older than -storage-nodes would start and then
  # die on "flag provided but not defined" — the failure mode router.sh had.
  log "构建 stratum-router …"
  (cd "$ROOT" && GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp" \
     CGO_ENABLED=0 go build -o "$STATION_BIN" ./cmd/stratum-router/)

  local nodes storage
  nodes="$(node_addr_list "$CONTROL_BASE_PORT" "$CONTROL_COUNT")"
  storage="$(node_addr_list "$STORAGE_BASE_PORT" "$STORAGE_COUNT")"
  log "启动服务站（$STATION_ADDR｜控制组 $nodes｜存储组 $storage）…"
  mkdir -p "$(dirname "$STATION_LOG")"
  : > "$STATION_LOG"
  nohup "$STATION_BIN" -listen "$STATION_ADDR" -nodes "$nodes" -storage-nodes "$storage" \
    >>"$STATION_LOG" 2>&1 &
  echo $! > "$STATION_PID_FILE"

  local waited=0
  while ! station_listening; do
    if ! station_pid >/dev/null; then
      warn "服务站启动失败，日志尾部："
      tail -n 5 "$STATION_LOG" >&2 || true
      rm -f "$STATION_PID_FILE"
      die "服务站没能起来（见 $STATION_LOG）"
    fi
    ((waited++ >= 50)) && die "服务站 5 秒内未开始监听 $STATION_ADDR（见 $STATION_LOG）"
    sleep 0.1
  done
  log "服务站已就绪：$STATION_ADDR（日志 $STATION_LOG）"
}

cmd_station_down() {
  local pid
  if pid="$(station_pid)"; then
    kill "$pid" 2>/dev/null || true
    log "已停止服务站（PID $pid）"
  fi
  rm -f "$STATION_PID_FILE"
}

cmd_station_status() {
  local pid
  if pid="$(station_pid)"; then
    printf '服务站  running  pid=%s  addr=%s  control=%s  storage=%s\n' \
      "$pid" "$STATION_ADDR" "$CONTROL_COUNT" "$STORAGE_COUNT"
  else
    printf '服务站  absent   addr=%s\n' "$STATION_ADDR"
  fi
}

cmd_station_logs() {
  local lines="${1:-40}"
  [[ -f "$STATION_LOG" ]] || die "还没有服务站日志（$STATION_LOG）"
  tail -n "$lines" "$STATION_LOG"
}

# container_status / container_health report one container's state as two
# fields. Kept apart (rather than joined) because the all-in-one script's JSON
# separates them and the console reads that shape: two scripts answering the
# same interface differently is how one page grows two rendering paths.
container_status() {
  local name=$1
  if ! container_exists "$name"; then echo "absent"; return; fi
  docker inspect -f '{{.State.Status}}' "$name"
}

container_health() {
  local name=$1
  if ! container_exists "$name"; then echo "-"; return; fi
  docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || echo none
}

cmd_status() {
  local leader="" i name term max=0
  for ((i = 1; i <= CONTROL_COUNT; i++)); do
    name="$(control_name "$i")"
    container_exists "$name" || continue
    term="$(docker logs "$name" 2>&1 | grep -o '"became leader"[^}]*' | tail -1 | grep -o 'term":[0-9]*' | cut -d: -f2 || true)"
    term="${term:-0}"
    if [[ "$term" -gt "$max" ]]; then max="$term"; leader="$name"; fi
  done

  # The console reads this shape. It carries the two tiers explicitly — their
  # counts, their ports and a per-node "tier" field — because the whole point of
  # this script is that the nodes are no longer interchangeable: a control node
  # holds no data and a storage node holds no metadata, so a client that treats
  # the node list as one uniform pool will route reads to a node that cannot
  # serve them.
  # The console reads this shape. It is the all-in-one script's shape — count,
  # base_port and per-node status/health as separate fields — plus the fields
  # only a two-tier cluster has: the topology, the two tier counts and ports, and
  # a per-node "tier". Keeping the common fields identical is what lets the page
  # render both with one code path and only branch where the cluster really is
  # different (grouping, and which port is the "base").
  if [[ "$JSON_MODE" -eq 1 ]]; then
    echo "{"
    echo "  \"network\": \"$NETWORK\","
    echo "  \"topology\": \"two-tier\","
    echo "  \"count\": $((CONTROL_COUNT + STORAGE_COUNT)),"
    echo "  \"control_count\": $CONTROL_COUNT,"
    echo "  \"storage_count\": $STORAGE_COUNT,"
    echo "  \"base_port\": $CONTROL_BASE_PORT,"
    echo "  \"control_base_port\": $CONTROL_BASE_PORT,"
    echo "  \"storage_base_port\": $STORAGE_BASE_PORT,"
    echo "  \"image\": \"$IMAGE\","
    echo "  \"nodes\": ["
    local first=1
    for ((i = 1; i <= CONTROL_COUNT; i++)); do
      [[ "$first" -eq 0 ]] && echo ","
      first=0
      printf '    {"id": %d, "name": "%s", "tier": "control", "role": "control", "status": "%s", "health": "%s", "grpc_port": %d, "leader": %s}' \
        "$i" "$(control_name "$i")" "$(container_status "$(control_name "$i")")" \
        "$(container_health "$(control_name "$i")")" \
        "$((CONTROL_BASE_PORT + i - 1))" "$([ "$(control_name "$i")" = "$leader" ] && echo true || echo false)"
    done
    for ((i = 1; i <= STORAGE_COUNT; i++)); do
      echo ","
      printf '    {"id": %d, "name": "%s", "tier": "storage", "role": "storage", "status": "%s", "health": "%s", "grpc_port": %d, "leader": false}' \
        "$(storage_id "$i")" "$(storage_name "$i")" \
        "$(container_status "$(storage_name "$i")")" "$(container_health "$(storage_name "$i")")" \
        "$((STORAGE_BASE_PORT + i - 1))"
    done
    echo
    echo "  ]"
    echo "}"
    return
  fi

  echo
  echo "== 集群状态（控制组 ${CONTROL_COUNT} + 存储组 ${STORAGE_COUNT} / 网络 $NETWORK）"
  printf '%-26s %-10s %-10s %-8s %s\n' "NAME" "TIER" "STATUS" "gRPC" "LEADER"
  for ((i = 1; i <= CONTROL_COUNT; i++)); do
    name="$(control_name "$i")"
    print_row "$name" "control" "$((CONTROL_BASE_PORT + i - 1))" "$leader"
  done
  for ((i = 1; i <= STORAGE_COUNT; i++)); do
    name="$(storage_name "$i")"
    print_row "$name" "storage" "$((STORAGE_BASE_PORT + i - 1))" "$leader"
  done
  echo
}

print_row() {
  local name=$1 tier=$2 port=$3 leader=$4 status health mark="-"
  if ! container_exists "$name"; then
    printf '%-26s %-10s %-10s %-8s %s\n' "$name" "$tier" "absent" "-" "-"
    return
  fi
  status="$(docker inspect -f '{{.State.Status}}' "$name")"
  health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || echo none)"
  [[ "$name" == "$leader" ]] && mark="yes"
  printf '%-26s %-10s %-10s %-8s %s\n' "$name" "$tier" "${status}/${health}" "$port" "$mark"
}

cmd_logs() {
  local target=${1:-} lines=200 follow=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --lines) lines="$2"; shift 2 ;;
      -f) follow=1; shift ;;
      *) target="$1"; shift ;;
    esac
  done

  # By id, like the all-in-one script: the console passes ids, not names.
  if [[ -n "$target" ]]; then
    local name
    if name="$(container_for_id "$target")"; then
      if [[ "$follow" -eq 1 ]]; then
        docker logs -f --tail "$lines" "$name"
      else
        docker logs --tail "$lines" "$name" 2>&1
      fi
      return
    fi
    # Not an id: accept a container name too, for manual use.
    docker logs --tail "$lines" "$target" 2>&1
    return
  fi
  local i
  for ((i = 1; i <= CONTROL_COUNT; i++)); do
    echo "===== $(control_name "$i") ====="
    docker logs --tail 30 "$(control_name "$i")" 2>&1 | tail -10
  done
  for ((i = 1; i <= STORAGE_COUNT; i++)); do
    echo "===== $(storage_name "$i") ====="
    docker logs --tail 30 "$(storage_name "$i")" 2>&1 | tail -10
  done
}

cmd_help() {
  sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# Options are shaped like docker-cluster.sh's, with the tier spelled out where
# that script says "nodes". A caller (the ops console) can then assemble
# arguments once and point them at either script — rather than growing a second
# assembly path, which is how two orchestrations drift into behaving differently
# for the same button.
#
# The environment variables the script reads remain supported; an explicit flag
# wins over them.
JSON_MODE=0
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)              JSON_MODE=1; shift ;;
    --force)             FORCE=1; shift ;;
    --with-embed)        WITH_EMBED=1; shift ;;
    --no-embed)          WITH_EMBED=0; shift ;;
    --with-station)      WITH_STATION=1; shift ;;
    --control-nodes)     CONTROL_COUNT="$2"; shift 2 ;;
    --storage-nodes)     STORAGE_COUNT="$2"; shift 2 ;;
    --control-base-port) CONTROL_BASE_PORT="$2"; shift 2 ;;
    --storage-base-port) STORAGE_BASE_PORT="$2"; shift 2 ;;
    --network)           NETWORK="$2"; shift 2 ;;
    --image)             IMAGE="$2"; shift 2 ;;
    *)                   ARGS+=("$1"); shift ;;
  esac
done
set -- ${ARGS[@]+"${ARGS[@]}"}

case "${1:-help}" in
  build)   cmd_build ;;
  init)    cmd_init ;;
  up)      cmd_up ;;
  start)   shift; cmd_node_start "$@" ;;
  stop)    shift; cmd_node_stop "$@" ;;
  restart) shift; cmd_node_restart "$@" ;;
  down)    cmd_down ;;
  status)  cmd_status ;;
  logs)    shift; cmd_logs "$@" ;;
  station) shift; cmd_station "$@" ;;
  help|*)  cmd_help ;;
esac
