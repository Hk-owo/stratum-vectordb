#!/usr/bin/env bash
# cluster.sh — Stratum Docker 集群编排（单层 / 两层，一个入口）。
#
# 两种拓扑（--topology）：
#
#   single     N 个同构节点（role=all），每个都自带数据层。它们连的 vecstore 是
#              **宿主进程**（host.docker.internal:710N），节点数据目录 bind mount
#              到宿主 /var/lib/stratum/nodeN，让那个宿主 vecstore 能按路径打开
#              节点持久化的索引文件。镜像 stratum-node:latest（integration/docker/Dockerfile）。
#
#   two-tier   控制组（role=control：Raft + 元数据，零数据层）+ 存储组（role=storage：
#              不参与选举、无 Raft 日志，元数据经 gRPC 从控制组读）。每个容器自带
#              vecstore，所以宿主不再需要伸手进节点的数据目录（单层那个 bind mount
#              就是为这件事存在的）。镜像 stratum-storage:latest
#              （integration/docker/Dockerfile.storage，含 C++ vecstore_server 与它的库）。
#
# 身份空间（两层）：控制节点 1..N（Raft 成员 ID，也就是 peers 表），存储节点
# 11..1N。两者不能重叠：存储 ID 同时是 storage.nodes 的键，节点按 node_id 在那张表
# 里解析自己的地址，重叠会让控制节点认领存储节点的地址。
#
# 用法：
#   scripts/cluster.sh [--topology single|two-tier] <command> [options]
#
# 命令：
#   build                构建二进制与镜像（--only all|node|storage 可只建一半）
#   init [N]             生成节点配置与网络（幂等，不启动）
#   up [N]               init + 启动（幂等；两层下不接位置参数）
#   update [N]           重编 → 重建镜像 → --force 重建容器（数据卷保留）
#   start|stop|restart [id...]   单个/一批节点（缺省全部）
#   status               集群状态（--json 输出机器可读形态，控制台读的就是它）
#   logs [id] [--lines N] [-f]
#   down                 停止并删除容器（保留数据卷）
#   clean                连数据卷/网络/配置一起删（不可逆）
#   embed start|stop|status      可选依赖 mock-embed 容器
#   vecstore start|stop|status   单层拓扑的宿主 vecstore（:710N）
#   station up|down|status|logs  服务站（stratum-router，宿主进程）
#
# 选项：
#   --topology single|two-tier   拓扑（默认 single）
#   --nodes N                    单层节点数；两层等价于 --control-nodes
#   --control-nodes N            两层：控制组节点数（默认 3）
#   --storage-nodes N            两层：存储组节点数（默认 3）
#   --base-port P                单层起始 gRPC 宿主端口（默认 17000；raft=P+1000，
#                                metrics=P+2000）
#   --control-base-port P        两层：控制组起始 gRPC 宿主端口（默认 17000）
#   --storage-base-port P        两层：存储组起始 gRPC 宿主端口（默认 17100）
#   --network N                  Docker 网络名（默认 stratum-net）
#   --image IMG                  节点镜像（单层默认 stratum-node:latest，两层默认
#                                stratum-storage:latest）
#   --force                      重建已存在的容器/配置
#   --with-embed                 单层：同时启动 mock-embed（两层默认就启动）
#   --no-embed                   两层：不启动 mock-embed（自带 embed 服务的场景）
#   --with-station               两层：up 之后一并启动服务站
#   --json                       status 输出机器可读 JSON
#
# 环境变量（显式选项优先）：
#   STRATUM_CONTROL_COUNT / STRATUM_STORAGE_COUNT / STRATUM_CONTROL_BASE_PORT /
#   STRATUM_STORAGE_BASE_PORT / STRATUM_STATION_ADDR / STRATUM_REQUIRE_AUTH /
#   STRATUM_RAFT_MAX_LOG_LENGTH / LAG_CATCHUP_MIN_LAG / LAG_CATCHUP_JITTER_MS /
#   LAG_CATCHUP_MAX_KBS / LOG_LEVEL
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------- 集群参数（默认值） ----------
TOPOLOGY=single
SINGLE_NODES="${STRATUM_NODES:-3}"
CONTROL_COUNT="${STRATUM_CONTROL_COUNT:-3}"
STORAGE_COUNT="${STRATUM_STORAGE_COUNT:-3}"
BASE_PORT="${STRATUM_BASE_PORT:-17000}"
CONTROL_BASE_PORT="${STRATUM_CONTROL_BASE_PORT:-17000}"
STORAGE_BASE_PORT="${STRATUM_STORAGE_BASE_PORT:-17100}"
NETWORK=stratum-net
IMAGE=""                # 空 = 按拓扑取默认（见 resolve_image）
STORAGE_IMAGE="${STRATUM_STORAGE_IMAGE:-stratum-storage:latest}"
CONTAINER_PREFIX=stratum-node
WITH_EMBED=""           # 空 = 按拓扑取默认（单层不启动，两层启动）
WITH_STATION=0
FORCE=0
JSON_MODE=0
ONLY=all
STORAGE_ID_BASE=10
VECSTORE_BASE_PORT=7101
VECSTORE_IMAGE=stratum-node:latest   # 单层的 all-in-one 镜像

# 单层：每个节点独立 vecstore（宿主进程，:710N），RocksDB 目录各自独立。
# 两层：容器自带 vecstore，宿主不参与。

# ---------- 基础工具 ----------
log()  { echo -e "\033[1;36m[cluster]\033[0m $*"; }
warn() { echo -e "\033[1;33m[cluster]\033[0m $*" >&2; }
die()  { echo -e "\033[1;31m[cluster]\033[0m 错误：$*" >&2; exit 1; }
# 容器是否存在。不用 `docker inspect NAME` 的退出码判断：docker 会在容器、镜像、
# 网络里按名字找，于是 `docker inspect stratum-node`（没有这个容器、但有
# stratum-node:latest 这个镜像）会成功，并返回一个没有 State 的对象。
container_exists() {
  [[ -n "$(docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true)" ]]
}
# 容器状态（不存在时为空）。
container_state() {
  docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true
}

is_two_tier() { [[ "$TOPOLOGY" == "two-tier" ]]; }

# 拓扑是否已解析（选项在命令之后也能给，所以「解析完」和「开始用」要分开）。
resolve_image() {
  if [[ -z "$IMAGE" ]]; then
    if is_two_tier; then IMAGE="$STORAGE_IMAGE"; else IMAGE=stratum-node:latest; fi
  fi
  if [[ -z "$WITH_EMBED" ]]; then
    if is_two_tier; then WITH_EMBED=1; else WITH_EMBED=0; fi
  fi
}

# ---------- 拓扑派生：目录与命名 ----------
run_dir()   { if is_two_tier; then echo "$ROOT/run/docker-both"; else echo "$ROOT/run/docker"; fi; }
nodes_dir() { echo "$(run_dir)/nodes"; }
log_dir()   { echo "$(run_dir)/log"; }

control_name() { echo "${CONTAINER_PREFIX}-control${1}"; }
storage_name() { echo "${CONTAINER_PREFIX}-storage${1}"; }
storage_id()   { echo $((STORAGE_ID_BASE + ${1})); }

# node_name <id>：id 空间两层共享（控制 1..N、存储 11..1N），单层就是 1..N。
node_name() {
  local id=$1
  if is_two_tier; then
    if ((id >= 1 && id <= CONTROL_COUNT)); then control_name "$id"; return 0; fi
    local sid=$((id - STORAGE_ID_BASE))
    if ((sid >= 1 && sid <= STORAGE_COUNT)); then storage_name "$sid"; return 0; fi
    return 1
  fi
  echo "${CONTAINER_PREFIX}${id}"
}

# node_tier <id>：两层输出 control/storage，单层输出空。
node_tier() {
  local id=$1
  is_two_tier || { echo ""; return 0; }
  if ((id >= 1 && id <= CONTROL_COUNT)); then echo "control"; else echo "storage"; fi
}

node_cfg() {
  local id=$1
  if ! is_two_tier; then echo "$(nodes_dir)/node${id}/config.yaml"; return; fi
  if [[ "$(node_tier "$id")" == "control" ]]; then
    echo "$(nodes_dir)/control${id}/config.yaml"
  else
    echo "$(nodes_dir)/storage$((id - STORAGE_ID_BASE))/config.yaml"
  fi
}

node_count() {
  if is_two_tier; then echo $((CONTROL_COUNT + STORAGE_COUNT)); else echo "$SINGLE_NODES"; fi
}

# node_ids：单层 1..N；两层控制组在前（与 console 的展示顺序一致）。
node_ids() {
  local i
  if is_two_tier; then
    for ((i = 1; i <= CONTROL_COUNT; i++)); do echo "$i"; done
    for ((i = 1; i <= STORAGE_COUNT; i++)); do storage_id "$i"; done
  else
    for ((i = 1; i <= SINGLE_NODES; i++)); do echo "$i"; done
  fi
}

node_grpc_port() {
  local id=$1
  if ! is_two_tier; then echo $((BASE_PORT + id - 1)); return; fi
  if [[ "$(node_tier "$id")" == "control" ]]; then
    echo $((CONTROL_BASE_PORT + id - 1))
  else
    echo $((STORAGE_BASE_PORT + id - STORAGE_ID_BASE - 1))
  fi
}

# count_from_config 从已生成的配置目录推断节点数（幂等操作要靠它）。
# 返回 0 而不是"目录不存在"的失败码：调用方常把它放进 $(...)，非零会借着
# set -e 把整个脚本带走。
count_from_config() {
  if is_two_tier; then
    local c=0 s=0 d
    for d in "$(nodes_dir)"/control*; do [[ -d "$d" ]] && c=$((c + 1)); done
    for d in "$(nodes_dir)"/storage*; do [[ -d "$d" ]] && s=$((s + 1)); done
    echo $((c + s))
    return 0
  fi
  # 单层：不能用 `ls … | sed … | tail` —— 目录不存在时 ls 返回非零，而 set -o pipefail
  # 让整条管道跟着非零，于是 set -e 在 return 0 之前就把脚本带走了。glob 不匹配时只是
  # 保持字面量，`[[ -d ]]` 判假，安全。
  local d max=0 n
  for d in "$(nodes_dir)"/node*; do
    [[ -d "$d" ]] || continue
    n="${d##*/node}"
    [[ "$n" =~ ^[0-9]+$ ]] || continue
    ((n > max)) || continue
    max=$n
  done
  if ((max > 0)); then echo "$max"; fi
  return 0
}

# resolve_ids：无参数时返回全部已配置节点（没有配置时输出为空，不是错误）。
resolve_ids() {
  if [[ $# -gt 0 ]]; then printf '%s\n' "$@"; return 0; fi
  if is_two_tier; then node_ids; return 0; fi
  local count; count="$(count_from_config)"
  if [[ -n "$count" ]]; then seq 1 "$count"; fi
  return 0
}

# ---------- 构建 ----------
build_node_image() {
  log "构建静态二进制（stratum / mock-embed）…"
  (cd "$ROOT" && CGO_ENABLED=0 go build -o integration/docker/stratum ./cmd/stratum/)
  (cd "$ROOT" && CGO_ENABLED=0 go build -o integration/docker/mock-embed ./integration/docker/mock_embed_server.go)
  log "构建 all-in-one 镜像（$VECSTORE_IMAGE）…"
  docker build -t "$VECSTORE_IMAGE" -f "$ROOT/integration/docker/Dockerfile" "$ROOT"
}

# build_storage_image 组装存储镜像（integration/docker/Dockerfile.storage）：
# 静态 Go 二进制 + C++ vecstore_server + vecstore_server 链接到的全部共享库。
#
# 库清单是**用 ldd 从本机收集**的，不手写：手写的清单在 vecstore 第一次多出一个
# 传递依赖时就错了，而那种错误表现为「在开发机上能起、换台机器就 cannot open
# shared object file」。C++ 二进制不在这里重编（见 vecstore/CMakeLists.txt）。
build_storage_image() {
  local LIBS="$ROOT/run/docker/vecstore-libs"
  local VECSTORE="$ROOT/run/bin/vecstore_server"

  if [[ ! -x "$VECSTORE" ]]; then
    die "缺少 $VECSTORE：先构建 C++ 侧（cmake --build … --target vecstore_server）"
  fi
  if [[ ! -x "$ROOT/integration/docker/stratum" ]]; then
    die "缺少 $ROOT/integration/docker/stratum：先 go build -o integration/docker/stratum ./cmd/stratum/"
  fi

  log "收集 vecstore_server 的共享库到 $LIBS …"
  rm -rf "$LIBS"; mkdir -p "$LIBS"
  ldd "$VECSTORE" | awk '{print $3}' | grep '^/' | sort -u | while read -r lib; do
    cp -L "$lib" "$LIBS/"
  done
  # 加载器随库一起走：宿主 glibc 比基础镜像新，二进制必须在宿主的加载器下、
  # 配着宿主这一套 libc 运行（见 Dockerfile.storage）。
  cp -L /lib64/ld-linux-x86-64.so.2 "$LIBS/"
  log "    $(ls -1 "$LIBS" | wc -l) 个库，$(du -sh "$LIBS" | cut -f1)"

  log "构建存储镜像 $STORAGE_IMAGE …"
  docker build -t "$STORAGE_IMAGE" -f "$ROOT/integration/docker/Dockerfile.storage" "$ROOT"
}

cmd_build() {
  case "$ONLY" in
    all|node) build_node_image ;;
    storage)
      # 只建存储镜像：它需要 integration/docker/stratum，缺了就现编一个
      # （原来独立的 build-storage-image.sh 就是干这件事的）。
      (cd "$ROOT" && CGO_ENABLED=0 go build -o integration/docker/stratum ./cmd/stratum/)
      ;;
  esac
  if is_two_tier || [[ "$ONLY" == "storage" ]]; then
    case "$ONLY" in
      all|storage) build_storage_image ;;
    esac
  fi
  log "构建完成（拓扑 $TOPOLOGY，节点镜像 $IMAGE）"
}

# ---------- 网络与配置 ----------
ensure_network() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || {
    log "创建 Docker 网络 $NETWORK …"
    docker network create "$NETWORK" >/dev/null
  }
}

# 单层：node_id、端口、完整 peers 表、Docker 友好 raft 定时（200ms 心跳 /
# [2s,4s) 选举超时，规避 Docker 网络下的 split-vote）。
gen_single_config() {
  local id=$1 count=$2 cfg
  cfg="$(node_cfg "$id")"
  mkdir -p "$(dirname "$cfg")"
  cat > "$cfg" <<EOF
# Stratum node $id of ${count}-node cluster — generated by scripts/cluster.sh
node:
  node_id: $id
  grpc_addr: "0.0.0.0:7000"
  raft_addr: "0.0.0.0:8000"
  metrics_addr: "0.0.0.0:9000"

raft:
  heartbeat_interval_ms: 200
  election_timeout_min_ms: 2000
  election_timeout_max_ms: 4000
  # 0 = kvraft 默认 1000。压测/大写入量场景可调小以主动触发快照
  # （STRATUM_RAFT_MAX_LOG_LENGTH 覆盖）。
  max_log_length: ${STRATUM_RAFT_MAX_LOG_LENGTH:-0}
  peers:
$(for ((p=1; p<=count; p++)); do
    printf '    - id: %s\n      addr: "%s:8000"\n      service_addr: "%s:7000"\n' \
      "$p" "${CONTAINER_PREFIX}${p}" "${CONTAINER_PREFIX}${p}"
  done)

storage:
  data_dir: "/var/lib/stratum/node${id}"

vecstore:
  # 每节点独立 vecstore：节点 N 连宿主 710N（每副本独立构建索引）。
  grpc_addr: "host.docker.internal:$((VECSTORE_BASE_PORT + id - 1))"

embed:
  # mock-embed 容器与节点同处 $NETWORK：容器网络内直接用容器名访问
  # （宿主的 18080:8080 映射仅供宿主/外部访问）。
  service_addr: "http://stratum-embed:8080"

index_manager:
  lru_capacity: 16
  memory_threshold_mb: 4096
  load_wait_timeout_ms: 5000
  callback_max_retries: 3
  callback_retry_base_interval_ms: 200

write_coordinator:
  max_retries: 3
  retry_base_interval_ms: 100

delete_coordinator:
  max_retries: 5
  retry_base_interval_ms: 500

logging:
  # LOG_LEVEL=debug 打开逐段/决策日志（滞后追赶的调度、上报落地、写入各段耗时这些
  # 都只在 debug 级可见），诊断时才需要。
  level: "${LOG_LEVEL:-info}"
EOF
}

# peers_block 是控制组的 Raft 成员表。两层下每个节点都带它：控制节点把它当自己的
# Raft 配置，存储节点把它当「控制集群」的地址表（RemoteRaftNode 从那里读元数据）。
peers_block() {
  local i
  for ((i = 1; i <= CONTROL_COUNT; i++)); do
    printf '    - id: %s\n      addr: "%s:8000"\n      service_addr: "%s:7000"\n' \
      "$i" "$(control_name "$i")" "$(control_name "$i")"
  done
}

# storage_block 是存储组。控制节点用它决定把写入派发给谁；存储节点用它确定写入
# 要扇出到哪些副本。
storage_block() {
  local i
  for ((i = 1; i <= STORAGE_COUNT; i++)); do
    printf '    - id: %s\n      addr: "%s:7000"\n' "$(storage_id "$i")" "$(storage_name "$i")"
  done
}

gen_control_config() {
  local i=$1 cfg
  cfg="$(node_cfg "$i")"
  mkdir -p "$(dirname "$cfg")"
  cat > "$cfg" <<EOF
# Control node $i — generated by scripts/cluster.sh --topology two-tier
#
# role=control：只跑 Raft 集群、只持版本元数据。没有 vecstore、没有本地存储、
# 没有索引——每个版本的数据工作都派发给下面的存储组，读也在那边服务
# （QueryService 是存储侧服务，它直接读索引管理器）。客户端调用只能经服务站
# 到达它（§9），下面的 require_authenticated 就是这件事的强制手段。
node:
  node_id: $i
  role: control
  require_authenticated: ${STRATUM_REQUIRE_AUTH:-true}
  grpc_addr: "0.0.0.0:7000"
  raft_addr: "0.0.0.0:8000"
  metrics_addr: "0.0.0.0:9000"

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
  # 容器内自带：这个镜像自己跑 vecstore（Dockerfile.storage）。
  grpc_addr: "127.0.0.1:7100"

embed:
  service_addr: "http://stratum-embed:8080"

logging:
  # LOG_LEVEL=debug 打开决策级日志（上报落地、链尾信号等），诊断时才需要。
  level: "${LOG_LEVEL:-info}"
EOF
}

gen_storage_config() {
  local i=$1 id cfg
  id="$(storage_id "$i")"
  cfg="$(node_cfg "$id")"
  mkdir -p "$(dirname "$cfg")"
  cat > "$cfg" <<EOF
# Storage node $i (node_id $id) — generated by scripts/cluster.sh --topology two-tier
#
# role=storage：没有 Raft 日志、不参选、永不当 leader。下面的 raft.peers 不是本节点
# 的成员表，而是**控制集群**的地址表：本节点从那里读复制元数据、向那里报告进度。
node:
  node_id: $id
  role: storage
  require_authenticated: ${STRATUM_REQUIRE_AUTH:-true}
  grpc_addr: "0.0.0.0:7000"
  raft_addr: "0.0.0.0:8000"
  metrics_addr: "0.0.0.0:9000"

raft:
  # 控制集群。存储节点自己没有日志。
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

# §8.6(d) 的测试夹具取值：扫描器一直开（只读），这里把「收」也打开，并把周期压到
# 5 秒——否则压测用例要等满 10 分钟默认值，整条链（扫描 → 决策 → 重开/重建 →
# 保存 → 再分发）在 CI 里根本跑不到。
#
# append_max_dead_ratio 抬高是**必要**的，但**不是充分**的（本轮改了原来的归因）：
# §8.6(c) 复用父产物要同时满足两件事，缺任何一条产物就不带墓碑、(d) 永远没有对象可收：
#
#   1. **有东西可 append**（delta 非空）。纯删除的版本 delta 为空，appendBase 直接返回
#      ok=false —— 与任何阈值无关，产物必然整份重建。“删掉 600 篇里的 480 篇得到 362
#      行（零墓碑）”那次实测的真正原因是这一条，原来把它归给“两个阈值相等”，不准确。
#      内容寻址让它更容易踩到：追加的文本若与库里已有文档重复，同样产生不了新 chunk
#      ——压测用例就踩过（产物 2379 chunk 恰好等于活的 501 篇的 chunk 数，dead_share
#      0.000，扫描当然判不出候选）。
#   2. 父产物的死向量占比 ≤ 该比例。§8.6(d) 的收集阈值默认也是 0.2，两者相等时 (c) 会
#      在 (d) 刚开始关心的占比上就重建，墓碑同样留不到被收集的那一刻。删 1600/2000 的
#      死占比约 0.80，所以必须抬到 0.80 以上（这里给 0.95）。
index_manager:
  gc_enabled: true
  gc_sweep_interval_ms: 5000
  serving_replica_min: 2
  append_max_dead_ratio: 0.95

# 主动落后检测（docs/active-lag-detection-design.md）：没有开关，落后副本自己追，
# 下面三项只定节奏。
lag_catchup:
  min_lag_versions: ${LAG_CATCHUP_MIN_LAG:-1}
  jitter_ms: ${LAG_CATCHUP_JITTER_MS:-0}
  max_concurrent_kbs: ${LAG_CATCHUP_MAX_KBS:-0}

logging:
  level: "${LOG_LEVEL:-info}"
EOF
}

cmd_init() {
  ensure_network
  local i count
  if is_two_tier; then
    for ((i = 1; i <= CONTROL_COUNT; i++)); do
      local cfg; cfg="$(node_cfg "$i")"
      if [[ -f "$cfg" ]] && [[ "$FORCE" -eq 0 ]]; then
        log "配置已存在，跳过：$cfg（--force 覆盖）"
      else
        gen_control_config "$i"
        log "生成配置：$cfg"
      fi
    done
    for ((i = 1; i <= STORAGE_COUNT; i++)); do
      local sid cfg2; sid="$(storage_id "$i")"; cfg2="$(node_cfg "$sid")"
      if [[ -f "$cfg2" ]] && [[ "$FORCE" -eq 0 ]]; then
        log "配置已存在，跳过：$cfg2（--force 覆盖）"
      else
        gen_storage_config "$i"
        log "生成配置：$cfg2"
      fi
    done
    log "已就绪 ${CONTROL_COUNT} 控制节点 + ${STORAGE_COUNT} 存储节点（网络 $NETWORK）"
    return
  fi
  count=${1:-$SINGLE_NODES}
  for ((i = 1; i <= count; i++)); do
    local cfg; cfg="$(node_cfg "$i")"
    if [[ -f "$cfg" ]] && [[ "$FORCE" -eq 0 ]]; then
      log "配置已存在，跳过：$cfg（--force 覆盖）"
    else
      gen_single_config "$i" "$count"
      log "生成配置：$cfg"
    fi
  done
  log "已就绪 ${count} 节点配置（网络 $NETWORK）"
}

# ---------- 容器生命周期 ----------
run_node() {
  local id=$1 name
  name="$(node_name "$id")" || die "未知节点 id $id"
  local gport; gport="$(node_grpc_port "$id")"

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

  if ! is_two_tier; then
    local rport=$((gport + 1000)) mport=$((gport + 2000))
    # 节点数据目录 bind mount 宿主的 /var/lib/stratum/nodeN（容器内外同一路径），
    # 宿主的 vecstore 进程才能按路径访问节点持久化的索引文件
    # (index/<kb>/<version>.index)：docker 命名卷无法被宿主进程按路径打开。
    local hostdir="/var/lib/stratum/node${id}"
    if [[ ! -d "$hostdir" ]]; then
      # docker 自动创建时属主是 root，改成当前用户可写：容器内以 root 运行不受
      # 影响，宿主的 vecstore 以普通用户运行则需要写权限。
      docker run --rm -v "$hostdir:/x" alpine:latest sh -c \
        "chown $(id -u):$(id -g) /x && chmod 775 /x" >/dev/null 2>&1 || \
        { mkdir -p "$hostdir" && chmod 775 "$hostdir"; }
    fi
    log "启动 $name（gRPC :$gport / raft :$rport / metrics :$mport）…"
    docker run -d --name "$name" \
      --network "$NETWORK" \
      -p "${gport}:7000" -p "${rport}:8000" -p "${mport}:9000" \
      -v "$(node_cfg "$id"):/etc/stratum/config.yaml:ro" \
      -v "${hostdir}:/var/lib/stratum/node${id}" \
      --add-host host.docker.internal:host-gateway \
      --restart unless-stopped \
      --health-cmd "nc -z -w2 127.0.0.1 7000" \
      --health-interval 5s --health-timeout 2s --health-retries 6 \
      "$IMAGE" >/dev/null
    return
  fi

  # 两层：每个容器自带 vecstore，数据放命名卷，端口只映射 gRPC。
  local tier; tier="$(node_tier "$id")"
  local vol
  if [[ "$tier" == "control" ]]; then
    vol="stratum-control${id}-data"
  else
    vol="stratum-storage-container$((id - STORAGE_ID_BASE))-data"
  fi
  log "启动 $name（$tier，gRPC 宿主 :$gport → 容器 7000）…"
  docker run -d --name "$name" \
    --network "$NETWORK" \
    -p "${gport}:7000" \
    -v "$(node_cfg "$id"):/etc/stratum/config.yaml:ro" \
    -v "${vol}:/var/lib/stratum" \
    --restart unless-stopped \
    --health-cmd "bash -c 'echo > /dev/tcp/127.0.0.1/7000'" \
    --health-interval 5s --health-timeout 2s --health-retries 6 \
    "$IMAGE" >/dev/null
}

# 节点最后一次当选 leader 时的 term（从未当选输出 0）。raft 语义：当前 leader 一定
# 是「最后一次当选 term 最大」的运行中节点——旧 term 的 leader 收到更高 term 的心跳
# 后会退位。两层的 leader 只可能出在控制组。
node_leader_term() {
  local t
  t="$(docker logs "$1" 2>&1 | grep -o '"became leader"[^}]*' | tail -1 | grep -o 'term":[0-9]*' | cut -d: -f2 || true)"
  echo "${t:-0}"
}

current_leader() {
  local name t max=0 leader=""
  while read -r id; do
    name="$(node_name "$id")" || continue
    if is_two_tier && [[ "$(node_tier "$id")" == "storage" ]]; then continue; fi
    if container_exists "$name" && [[ "$(container_state "$name")" == "running" ]]; then
      t="$(node_leader_term "$name")"
      if [[ "$t" -gt "$max" ]]; then max=$t; leader="$name"; fi
    fi
  done < <(node_ids)
  # "还没选出 leader" 是正常状态（集群没起、或正在选），不是错误：这个函数的结果
  # 常被赋值语句接住（leader="$(current_leader)"），非零会借着 set -e 结束脚本。
  [[ "$max" -gt 0 ]] && echo "$leader"
  return 0
}

wait_leader() {
  local timeout="${1:-60}" prev="" cur="" t max
  log "等待 leader 选举（最多 ${timeout}s）…"
  for _ in $(seq 1 "$timeout"); do
    max=0; cur=""
    while read -r id; do
      local name; name="$(node_name "$id")" || continue
      container_exists "$name" || continue
      [[ "$(container_state "$name")" == "running" ]] || continue
      t="$(node_leader_term "$name")"
      if [[ "$t" -gt "$max" ]]; then max=$t; cur="$name"; fi
    done < <(node_ids)
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

wait_healthy() {
  local timeout="${1:-90}"
  for _ in $(seq 1 "$timeout"); do
    local all=1
    while read -r id; do
      local name st h; name="$(node_name "$id")" || continue
      if ! container_exists "$name"; then all=0; break; fi
      st="$(container_state "$name")"
      h="$(container_state_health "$name")"
      if [[ "$st" != "running" || "$h" != "healthy" ]]; then all=0; break; fi
    done < <(node_ids)
    if [[ "$all" -eq 1 ]]; then
      log "全部 $(node_count) 个节点 healthy"
      return 0
    fi
    sleep 2
  done
  warn "等待全部节点 healthy 超时（${timeout}s）"
  return 1
}

cmd_up() {
  local count=${1:-$SINGLE_NODES}
  if is_two_tier; then
    cmd_init
    log "== 控制组（${CONTROL_COUNT} 节点，role=control，零数据层）=="
    local i
    for ((i = 1; i <= CONTROL_COUNT; i++)); do run_node "$i"; done
    wait_leader 60 || true
    log "== 存储组（${STORAGE_COUNT} 节点，role=storage，容器内含 vecstore）=="
    for ((i = 1; i <= STORAGE_COUNT; i++)); do run_node "$(storage_id "$i")"; done
    if [[ "$WITH_EMBED" -eq 1 ]]; then embed_start; fi
    if [[ "$WITH_STATION" -eq 1 ]]; then cmd_station_up; fi
    cmd_status
    return
  fi
  cmd_init "$count"
  # 每节点独立 vecstore（先于节点启动：节点启动时的索引 reconcile 需要它）
  vecstore_start
  local id
  for ((id = 1; id <= count; id++)); do run_node "$id"; done
  if [[ "$WITH_EMBED" -eq 1 ]]; then embed_start; fi
  wait_leader "$count" || true
  cmd_status
}

cmd_update() {
  local count=${1:-$SINGLE_NODES}
  log "== 更新 docker 集群（编译 → 镜像 → 重建容器，数据卷保留）=="
  cmd_build
  FORCE=1
  if is_two_tier; then cmd_up; else cmd_up "$count"; fi
  wait_healthy 90
  log "更新完成：$(node_count) 个节点已运行最新实现"
}

cmd_start() {
  local ids; ids="$(resolve_ids "$@")"
  local id
  for id in $ids; do run_node "$id"; done
}

cmd_stop() {
  local ids; ids="$(resolve_ids "$@")"
  local id name
  for id in $ids; do
    name="$(node_name "$id")" || die "未知节点 id $id"
    container_exists "$name" && { docker stop "$name" >/dev/null && log "停止 $name"; }
  done
}

cmd_restart() {
  cmd_stop "$@"
  cmd_start "$@"
}

cmd_down() {
  # 只删本拓扑的容器：两种拓扑共用 stratum-node 前缀（单层 stratum-node1、
  # 两层 stratum-node-control1），不加区分会把另一套也一起删掉。
  local filter ids
  if is_two_tier; then filter="^/${CONTAINER_PREFIX}-(control|storage)"; else filter="^/${CONTAINER_PREFIX}[0-9]"; fi
  ids="$(docker ps -aq --filter "name=$filter" 2>/dev/null || true)"
  if [[ -n "$ids" ]]; then
    docker rm -f $ids >/dev/null
    log "已删除容器：$ids"
  else
    log "没有需要删除的节点容器"
  fi
  # mock-embed（stratum-embed）是两种拓扑**共用**的同一个容器名，所以 down 不删它：
  # 单层 down 顺手删掉两层在用（或反之）的 embed，正是"拆了一边、另一边静默坏掉"。
  # 要删它用 `embed stop`，或在 clean 里（那里是"连依赖一起清"的语义）。
  if container_exists stratum-embed; then
    log "mock-embed 容器保留（stratum-embed）；要删：scripts/cluster.sh embed stop"
  fi
  if is_two_tier; then
    # 服务站是宿主进程，不是容器：拆集群必须显式够到它，否则下一次 up 会留下一个
    # 服务于「已经被重建过的集群」的孤儿。
    cmd_station_down
  fi
}

cmd_clean() {
  cmd_down
  # clean 的语义是"连依赖一起清"，这里才删共享的 mock-embed 容器。
  embed_stop
  local i
  if is_two_tier; then
    for ((i = 1; i <= CONTROL_COUNT; i++)); do
      docker volume rm "stratum-control${i}-data" >/dev/null 2>&1 && log "删除数据卷 stratum-control${i}-data"
    done
    for ((i = 1; i <= STORAGE_COUNT; i++)); do
      docker volume rm "stratum-storage-container${i}-data" >/dev/null 2>&1 && log "删除数据卷 stratum-storage-container${i}-data"
    done
  else
    vecstore_stop
    local VECSTORE_DIR="$(run_dir)/vecstore"
    rm -rf "$VECSTORE_DIR" && log "删除 vecstore 数据目录 $VECSTORE_DIR"
    # 删除旧命名数据卷（bind mount 迁移前的遗留）
    for ((i = 1; i <= 32; i++)); do
      docker volume rm "stratum-node${i}-data" >/dev/null 2>&1 && log "删除数据卷 stratum-node${i}-data"
    done
    # 删除 bind mount 的宿主节点数据目录（root 属主，借容器删）
    for ((i = 1; i <= 32; i++)); do
      local hostdir="/var/lib/stratum/node${i}"
      if [[ -d "$hostdir" ]]; then
        docker run --rm -v "/var/lib/stratum:/s" alpine:latest rm -rf "/s/node${i}" >/dev/null 2>&1 \
          && log "删除节点数据目录 $hostdir"
      fi
    done
  fi
  docker network rm "$NETWORK" >/dev/null 2>&1 && log "删除网络 $NETWORK" || true
  local rd; rd="$(run_dir)"
  if [[ -d "$rd" ]]; then
    rm -rf "$rd"
    log "删除运行时目录 $rd"
  fi
  log "清理完成"
}

# ---------- 状态与日志 ----------
container_state_health() {
  docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1" 2>/dev/null || echo none
}

cmd_status() {
  local leader; leader="$(current_leader)"

  # --json：输出机器可读状态（控制台 /ops/docker/status 读的就是这个形态）。
  # 两层的形态是单层那一份加上「只有两层才有」的字段（拓扑、两组数量与端口、
  # 每节点的 tier），这样页面能用一条渲染路径画两种集群，只在真正不同的地方分支。
  if [[ "$JSON_MODE" -eq 1 ]]; then
    echo "{"
    echo "  \"network\": \"$NETWORK\","
    if is_two_tier; then
      echo "  \"topology\": \"two-tier\","
      echo "  \"count\": $((CONTROL_COUNT + STORAGE_COUNT)),"
      echo "  \"control_count\": $CONTROL_COUNT,"
      echo "  \"storage_count\": $STORAGE_COUNT,"
      echo "  \"base_port\": $CONTROL_BASE_PORT,"
      echo "  \"control_base_port\": $CONTROL_BASE_PORT,"
      echo "  \"storage_base_port\": $STORAGE_BASE_PORT,"
    else
      echo "  \"topology\": \"single\","
      echo "  \"count\": $SINGLE_NODES,"
      echo "  \"base_port\": $BASE_PORT,"
    fi
    echo "  \"image\": \"$IMAGE\","
    echo "  \"nodes\": ["
    local first=1 id name status health mark
    for id in $(node_ids); do
      name="$(node_name "$id")" || continue
      status="absent"; health="-"; mark="false"
      if container_exists "$name"; then
        status="$(container_state "$name")"
        health="$(container_state_health "$name")"
        [[ "$name" == "$leader" ]] && mark="true"
      fi
      [[ "$first" -eq 0 ]] && echo ","
      first=0
      if is_two_tier; then
        local tier; tier="$(node_tier "$id")"
        printf '    {"id": %d, "name": "%s", "tier": "%s", "role": "%s", "status": "%s", "health": "%s", "grpc_port": %d, "leader": %s}' \
          "$id" "$name" "$tier" "$tier" "$status" "$health" "$(node_grpc_port "$id")" "$mark"
      else
        printf '    {"id": %d, "name": "%s", "status": "%s", "health": "%s", "grpc_port": %d, "leader": %s}' \
          "$id" "$name" "$status" "$health" "$(node_grpc_port "$id")" "$mark"
      fi
    done
    echo
    echo "  ]"
    echo "}"
    return
  fi

  echo
  if is_two_tier; then
    echo "== 集群状态（控制组 ${CONTROL_COUNT} + 存储组 ${STORAGE_COUNT} / 网络 $NETWORK）"
    printf '%-26s %-10s %-18s %-8s %s\n' "NAME" "TIER" "STATUS" "gRPC" "LEADER"
  else
    echo "== 集群状态（${SINGLE_NODES} 节点 / 网络 $NETWORK）"
    printf '%-26s %-18s %-8s %s\n' "NAME" "STATUS" "gRPC" "LEADER"
  fi
  local id
  for id in $(node_ids); do
    local name; name="$(node_name "$id")" || continue
    local status="absent" health="-" mark="-"
    if container_exists "$name"; then
      status="$(container_state "$name")"
      health="$(container_state_health "$name")"
      [[ "$name" == "$leader" ]] && mark="yes"
    fi
    if is_two_tier; then
      printf '%-26s %-10s %-18s %-8s %s\n' "$name" "$(node_tier "$id")" "${status}/${health}" "$(node_grpc_port "$id")" "$mark"
    else
      printf '%-26s %-18s %-8s %s\n' "$name" "${status}/${health}" "$(node_grpc_port "$id")" "$mark"
    fi
  done
  echo
}

cmd_logs() {
  local target="" lines=200 follow=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --lines) lines="$2"; shift 2 ;;
      -f) follow=1; shift ;;
      *) target="$1"; shift ;;
    esac
  done
  # 按 id 取（控制台传的是 id）；也接受容器名，供手工使用。
  if [[ -n "$target" ]]; then
    local name
    if name="$(node_name "$target")"; then
      if [[ "$follow" -eq 1 ]]; then docker logs -f --tail "$lines" "$name"; else docker logs --tail "$lines" "$name" 2>&1; fi
      return
    fi
    docker logs --tail "$lines" "$target" 2>&1
    return
  fi
  local id
  for id in $(node_ids); do
    local name; name="$(node_name "$id")" || continue
    echo "===== $name ====="
    docker logs --tail 30 "$name" 2>&1 | tail -10
  done
}

# ---------- mock-embed 依赖 ----------
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
      --health-cmd "wget -q -O- http://localhost:8080/health" \
      --health-interval 5s --health-timeout 2s --health-retries 6 \
      alpine:latest /app/mock-embed >/dev/null
  fi
}

embed_stop() {
  container_exists stratum-embed && docker rm -f stratum-embed >/dev/null && log "已删除 mock-embed" || true
}

embed_status() {
  if ! container_exists stratum-embed; then
    echo "mock-embed：未创建（scripts/cluster.sh embed start 可拉起）"
    return 0
  fi
  # 容器存在但没在跑时 `docker ps`（只列运行中）会给出一片空白——那看起来像命令没输出，
  # 而不是"它停着"。两层拓扑的节点配置把 embed 指向这个容器，所以它停着这件事必须看得见。
  local st; st="$(container_state stratum-embed)"
  if [[ "$st" == "running" ]]; then
    docker ps --filter name=^/stratum-embed$ --format 'stratum-embed  running  {{.Status}}'
  else
    echo "mock-embed：$st（节点连不上它；scripts/cluster.sh embed start 拉起）"
  fi
}

# ---------- 单层的宿主 vecstore（:710N） ----------
# 容器自带 vecstore 的两层拓扑不需要这一段；在那里调用会给出一句说明而不是
# 悄悄什么都不做。
vecstore_port() { echo $((VECSTORE_BASE_PORT + $1 - 1)); }
# 还没 init 过（没有节点配置）就返回空：让调用方的循环空转，而不是 seq 报错。
vecstore_ids() {
  local count; count="$(count_from_config)"
  if [[ -n "$count" ]]; then seq 1 "$count"; fi
  return 0
}

vecstore_start() {
  is_two_tier && return 0
  local dir; dir="$(run_dir)/vecstore"
  mkdir -p "$dir" "$(log_dir)"
  local id port
  for id in $(vecstore_ids); do
    port="$(vecstore_port "$id")"
    if ss -tlnp 2>/dev/null | grep -q ":$port "; then
      log "vecstore-$id 已在运行（:${port}）"
      continue
    fi
    log "启动 vecstore-$id（:${port}，数据目录 $dir/node${id}）…"
    mkdir -p "$dir/node${id}"
    # setsid：完全脱离调用进程组，避免脚本/会话退出时被连带清理
    setsid nohup "$ROOT/run/bin/vecstore_server" \
      --rocksdb_path="$dir/node${id}" \
      --grpc_addr="0.0.0.0:${port}" \
      >> "$(log_dir)/vecstore-node${id}.log" 2>&1 &
    sleep 0.5
  done
  local ok=1
  for id in $(vecstore_ids); do
    port="$(vecstore_port "$id")"
    if ! ss -tlnp 2>/dev/null | grep -q ":$port "; then
      warn "vecstore-$id 未监听 :${port}（见 $(log_dir)/vecstore-node${id}.log）"
      ok=0
    fi
  done
  [[ "$ok" -eq 1 ]] && log "全部 vecstore 实例就绪（${VECSTORE_BASE_PORT}-$((VECSTORE_BASE_PORT + $(count_from_config) - 1))）"
  return 0
}

vecstore_stop() {
  is_two_tier && return 0
  local id port
  for id in $(vecstore_ids); do
    port="$(vecstore_port "$id")"
    if pgrep -f "vecstore_server.*grpc_addr=0.0.0.0:${port}" >/dev/null; then
      pkill -f "vecstore_server.*grpc_addr=0.0.0.0:${port}" >/dev/null && log "停止 vecstore-$id（:${port}）"
    fi
  done
  return 0
}

vecstore_status() {
  if is_two_tier; then
    echo "两层拓扑：vecstore 在每个节点容器内（127.0.0.1:7100），宿主上没有它的进程。"
    return 0
  fi
  local id port
  for id in $(vecstore_ids); do
    port="$(vecstore_port "$id")"
    if ss -tlnp 2>/dev/null | grep -q ":$port "; then
      echo "vecstore-$id  running  :${port}  $(run_dir)/vecstore/node${id}"
    else
      echo "vecstore-$id  stopped  :${port}"
    fi
  done
}

# ---------- 服务站（宿主进程） ----------
# 客户端唯一的入口：节点都开了 require_authenticated 时，直接敲节点端口没有用
# （§9.3(5)）。所以起它是部署的一部分。集成测试不依赖这一段（TestMain 自己起）。
STATION_BIN="$ROOT/run/bin/stratum-router"
STATION_ADDR="${STRATUM_STATION_ADDR:-0.0.0.0:7009}"

station_pid_file() { echo "$(run_dir)/station.pid"; }
station_log()     { echo "$(log_dir)/station.log"; }

station_pid() {
  local f; f="$(station_pid_file)"
  [[ -f "$f" ]] || return 1
  local pid; pid="$(cat "$f" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  echo "$pid"
}

station_listening() {
  local host="${STATION_ADDR%:*}" port="${STATION_ADDR##*:}"
  [[ "$host" == "0.0.0.0" || "$host" == "*" ]] && host=127.0.0.1
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

# 一层的节点地址列表；两层返回两组（控制组、存储组）。
node_addr_list() {
  local base=$1 count=$2 i out=""
  for ((i = 1; i <= count; i++)); do out+="localhost:$((base + i - 1)),"; done
  echo "${out%,}"
}

cmd_station_up() {
  local pid
  if pid="$(station_pid)"; then
    log "服务站已在运行（PID $pid，$STATION_ADDR）"
    return 0
  fi
  if station_listening; then
    die "$STATION_ADDR 已在监听，但不是本脚本起的服务站；若那是 scripts/gateway.sh 起的，用它管理，或换端口：STRATUM_STATION_ADDR=0.0.0.0:7010"
  fi

  # 每次都构建而不是直接用 run/bin：go build 是增量的，而比 -storage-nodes 更旧的
  # 二进制会起来后死在 "flag provided but not defined"。
  log "构建 stratum-router …"
  (cd "$ROOT" && GOCACHE="$ROOT/run/gocache" GOTMPDIR="$ROOT/run/gotmp" \
    CGO_ENABLED=0 go build -o "$STATION_BIN" ./cmd/stratum-router/)

  local nodes storage=""
  if is_two_tier; then
    nodes="$(node_addr_list "$CONTROL_BASE_PORT" "$CONTROL_COUNT")"
    storage="$(node_addr_list "$STORAGE_BASE_PORT" "$STORAGE_COUNT")"
  else
    nodes="$(node_addr_list "$BASE_PORT" "$SINGLE_NODES")"
  fi
  log "启动服务站（$STATION_ADDR｜控制组/节点 $nodes${storage:+｜存储组 $storage}）…"
  mkdir -p "$(dirname "$(station_log)")"
  : > "$(station_log)"
  if [[ -n "$storage" ]]; then
    nohup "$STATION_BIN" -listen "$STATION_ADDR" -nodes "$nodes" -storage-nodes "$storage" \
      >>"$(station_log)" 2>&1 &
  else
    nohup "$STATION_BIN" -listen "$STATION_ADDR" -nodes "$nodes" >>"$(station_log)" 2>&1 &
  fi
  echo $! >"$(station_pid_file)"

  local waited=0
  while ! station_listening; do
    if ! station_pid >/dev/null; then
      warn "服务站启动失败，日志尾部："
      tail -n 5 "$(station_log)" >&2 || true
      rm -f "$(station_pid_file)"
      die "服务站没能起来（见 $(station_log)）"
    fi
    ((waited++ >= 50)) && die "服务站 5 秒内未开始监听 $STATION_ADDR（见 $(station_log)）"
    sleep 0.1
  done
  log "服务站已就绪：$STATION_ADDR（日志 $(station_log)）"
}

cmd_station_down() {
  local pid
  if pid="$(station_pid)"; then
    kill "$pid" 2>/dev/null || true
    log "已停止服务站（PID $pid）"
  fi
  rm -f "$(station_pid_file)"
}

cmd_station_status() {
  local pid
  if pid="$(station_pid)"; then
    if is_two_tier; then
      printf '服务站  running  pid=%s  addr=%s  control=%s  storage=%s\n' \
        "$pid" "$STATION_ADDR" "$CONTROL_COUNT" "$STORAGE_COUNT"
    else
      printf '服务站  running  pid=%s  addr=%s  nodes=%s\n' \
        "$pid" "$STATION_ADDR" "$SINGLE_NODES"
    fi
  else
    printf '服务站  absent   addr=%s\n' "$STATION_ADDR"
  fi
}

cmd_station_logs() {
  local lines="${1:-40}"
  [[ -f "$(station_log)" ]] || die "还没有服务站日志（$(station_log)）"
  tail -n "$lines" "$(station_log)"
}

cmd_station() {
  local action="${1:-status}"
  case "$action" in
    up)           cmd_station_up ;;
    down|stop)    cmd_station_down ;;
    status)       cmd_station_status ;;
    logs)         shift || true; cmd_station_logs "$@" ;;
    *)            die "station 的动作只能是 up / down / status / logs（收到「$action」）" ;;
  esac
}

# ---------- 参数解析 ----------
# 头注释本身就是手册：从第 2 行打印到第一个非注释行为止，行号不会随注释增减而漂。
usage() { awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; }

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --topology)           TOPOLOGY="$2"; shift 2 ;;
    --nodes)              SINGLE_NODES="$2"; CONTROL_COUNT="$2"; shift 2 ;;
    --control-nodes)      CONTROL_COUNT="$2"; shift 2 ;;
    --storage-nodes)      STORAGE_COUNT="$2"; shift 2 ;;
    --base-port)          BASE_PORT="$2"; CONTROL_BASE_PORT="$2"; shift 2 ;;
    --control-base-port)  CONTROL_BASE_PORT="$2"; shift 2 ;;
    --storage-base-port)  STORAGE_BASE_PORT="$2"; shift 2 ;;
    --network)            NETWORK="$2"; shift 2 ;;
    --image)              IMAGE="$2"; shift 2 ;;
    --storage-image)      STORAGE_IMAGE="$2"; shift 2 ;;
    --force)              FORCE=1; shift ;;
    --with-embed)         WITH_EMBED=1; shift ;;
    --no-embed)           WITH_EMBED=0; shift ;;
    --with-station)       WITH_STATION=1; shift ;;
    --json)               JSON_MODE=1; shift ;;
    --only)               ONLY="$2"; shift 2 ;;
    -h|--help)            usage; exit 0 ;;
    *)                    ARGS+=("$1"); shift ;;
  esac
done
set -- ${ARGS[@]+"${ARGS[@]}"}

[[ "$TOPOLOGY" == "single" || "$TOPOLOGY" == "two-tier" ]] || die "未知拓扑 $TOPOLOGY（可选 single / two-tier）"
resolve_image
mkdir -p "$(nodes_dir)" "$(log_dir)"

CMD="${1:-help}"
[[ $# -gt 0 ]] && shift

case "$CMD" in
  build)    cmd_build ;;
  init)     cmd_init "${1:-}" ;;
  up)       cmd_up "${1:-$SINGLE_NODES}" ;;
  update)   cmd_update "${1:-$SINGLE_NODES}" ;;
  start)    cmd_start "$@" ;;
  stop)     cmd_stop "$@" ;;
  restart)  cmd_restart "$@" ;;
  status)   cmd_status ;;
  logs)     cmd_logs "$@" ;;
  down)     cmd_down ;;
  clean)    cmd_clean ;;
  embed)
    case "${1:-status}" in
      start)  embed_start ;;
      stop)   embed_stop ;;
      status) embed_status ;;
      *)      die "embed 子命令: start|stop|status" ;;
    esac ;;
  vecstore)
    case "${1:-status}" in
      start)  vecstore_start ;;
      stop)   vecstore_stop ;;
      status) vecstore_status ;;
      *)      die "vecstore 子命令: start|stop|status" ;;
    esac ;;
  station)  cmd_station "$@" ;;
  help|*)   usage ;;
esac
