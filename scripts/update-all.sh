#!/usr/bin/env bash
# update-all.sh — Stratum 全部二进制一键更新脚本
#
# 把本地所有二进制与前端更新到最新源码,并自动重启正在运行的 gateway / router,
# 避免出现「代码改了但跑的还是旧产物」(例如:更新了 docker 集群,却忘了
# gateway / router 是本机独立进程,不随镜像更新;或者改了 web/src,而 gateway
# 服务的还是旧的 web/dist)。
#
# 默认动作:
#   1. 构建全部 Go 二进制到 run/bin/:
#        stratum / stratum-gateway / stratum-router / mock-embed
#   2. 构建前端 web/dist(gateway 的 -static 指向它;控制台页面就是它)
#   3. 若 gateway / router 正在运行,用它们当前的启动参数自动重启
#      (新二进制只有重启进程后才生效)
#
# 可选动作:
#   --vecstore      额外(重新)构建 vecstore_server(C++,较慢,首次才需要)
#   --docker [N]    额外更新**单层** docker 集群(等价于 scripts/cluster.sh
#                   update N:重新编译 → 重建镜像 → --force 重建容器,数据卷保留)
#   --two-tier [N]  额外更新**两层** docker 集群(控制组 N + 存储组 N,走
#                   scripts/cluster.sh --topology two-tier build + up --force:
#                   控制节点 role=control 零数据层,存储节点 role=storage 自带 vecstore)。
#                   与 --docker 互斥,同给以本项为准
#   --no-frontend   跳过前端构建(只改 Go 侧时省一次 npm 构建)
#   --no-restart    只构建,不重启 gateway / router(由你手动重启)
#   --help / -h     显示本说明
#
# 用法:
#   scripts/update-all.sh                  # 构建 Go 二进制 + 前端,重启 gateway/router
#   scripts/update-all.sh --docker 3       # 再更新 3 节点单层 docker 集群
#   scripts/update-all.sh --two-tier 3     # 再更新两层集群(控制 3 + 存储 3)
#   scripts/update-all.sh --vecstore --docker 1
#   scripts/update-all.sh --no-frontend    # 只构建 Go 侧
#   scripts/update-all.sh --no-restart     # 只构建不重启
#
# 前置依赖:Go 1.24+;前端需要 npm(缺了只警告,API 与运维页不受影响);
# --vecstore 额外需要 C++ 工具链 + Faiss/RocksDB/gRPC 等
# (见 vecstore/CMakeLists.txt 顶部说明)。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN="$ROOT/run"
BIN="$RUN/bin"
LOG="$RUN/log"
mkdir -p "$BIN" "$LOG"

# 构建缓存落在 run/ 下,不依赖全局 GOCACHE(某些环境全局缓存只读)。
export GOCACHE="$RUN/gocache"
export GOTMPDIR="$RUN/gotmp"
mkdir -p "$GOCACHE" "$GOTMPDIR"

# ---------- 参数解析 ----------
DO_VECSTORE=0
DO_DOCKER=0
DOCKER_NODES=3
DO_TWO_TIER=0
TWO_TIER_NODES=3
DO_RESTART=1
DO_FRONTEND=1
for arg in "$@"; do
  case "$arg" in
    --vecstore) DO_VECSTORE=1 ;;
    --docker)   DO_DOCKER=1 ;;
    --docker=*) DO_DOCKER=1; DOCKER_NODES="${arg#*=}" ;;
    --two-tier)   DO_TWO_TIER=1 ;;
    --two-tier=*) DO_TWO_TIER=1; TWO_TIER_NODES="${arg#*=}" ;;
    --no-frontend) DO_FRONTEND=0 ;;
    --no-restart) DO_RESTART=0 ;;
    --help|-h)
      sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      # 兼容 "update-all.sh --docker 3" / "--two-tier 3" 与 "...=3" 两种写法
      if [[ "$arg" =~ ^[0-9]+$ ]] && [ "$DO_TWO_TIER" = "1" ] && [ "$TWO_TIER_NODES" = "3" ]; then
        TWO_TIER_NODES="$arg"
      elif [[ "$arg" =~ ^[0-9]+$ ]] && [ "$DO_DOCKER" = "1" ] && [ "$DOCKER_NODES" = "3" ]; then
        DOCKER_NODES="$arg"
      else
        echo "未知参数: $arg(用 --help 查看用法)" >&2
        exit 2
      fi
      ;;
  esac
done

# --docker 与 --two-tier 互斥:同时给出时只做两层(单层脚本对两层拓扑无从下手)。
if [ "$DO_TWO_TIER" = "1" ] && [ "$DO_DOCKER" = "1" ]; then
  echo "提示:--docker 与 --two-tier 同时给出,只做两层更新(--two-tier)" >&2
  DO_DOCKER=0
fi

# ---------- 1. vecstore(C++,可选) ----------
if [ "$DO_VECSTORE" = "1" ]; then
  echo "==> [构建] vecstore_server(C++,较慢)…"
  cmake -S . -B "$RUN/cmake" >/dev/null
  cmake --build "$RUN/cmake" --target vecstore_server -j"$(nproc)" >/dev/null
  cp "$RUN/cmake/vecstore/vecstore_server" "$BIN/vecstore_server"
else
  echo "==> [构建] vecstore_server:已存在则跳过(--vecstore 强制重建)"
fi

# ---------- 2. Go 二进制 ----------
echo "==> [构建] Go 二进制(stratum / stratum-gateway / stratum-router / mock-embed)…"
go build -o "$BIN/stratum" ./cmd/stratum/
go build -o "$BIN/stratum-gateway" ./cmd/stratum-gateway/
go build -o "$BIN/stratum-router" ./cmd/stratum-router/
go build -o "$BIN/mock-embed" ./integration/docker/mock_embed_server.go

echo "==> [构建] 完成,二进制时间戳:"
ls -la --time-style=+%Y-%m-%d_%H:%M:%S "$BIN"/stratum "$BIN"/stratum-gateway "$BIN"/stratum-router "$BIN"/mock-embed

# ---------- 2b. 前端(web/dist) ----------
# gateway 的 -static 指向 web/dist,控制台页面(含「运维」页)就是它。少了这一步,
# 改完 web/src 只有刷新浏览器是不够的——服务的还是旧产物。
if [ "$DO_FRONTEND" = "1" ]; then
  if ! command -v npm >/dev/null 2>&1; then
    echo "==> [构建] 前端:未找到 npm,跳过(控制台静态页面将停留在旧产物;API 不受影响)"
  else
    if [ ! -d "$ROOT/web/node_modules" ]; then
      echo "==> [构建] 前端:安装依赖(npm install)…"
      npm --prefix "$ROOT/web" install --silent || echo "   警告:npm install 失败"
    fi
    echo "==> [构建] 前端:web/dist …"
    if npm --prefix "$ROOT/web" run build >/dev/null; then
      echo "    完成:web/dist 时间戳 $(date -r "$ROOT/web/dist" +%Y-%m-%d_%H:%M:%S 2>/dev/null || echo '已构建')"
    else
      echo "   警告:前端构建失败,控制台静态页面仍是旧产物(API 与 /ops 不受影响)"
    fi
  fi
else
  echo "==> [构建] 前端:已跳过(--no-frontend)"
fi

# ---------- 3. docker 集群(可选) ----------
if [ "$DO_TWO_TIER" = "1" ]; then
  echo "==> [docker] 更新两层集群(控制组 ${TWO_TIER_NODES} + 存储组 ${TWO_TIER_NODES},cluster.sh build + up --force)…"
  "$ROOT/scripts/cluster.sh" --topology two-tier build
  "$ROOT/scripts/cluster.sh" --topology two-tier \
    --control-nodes "$TWO_TIER_NODES" --storage-nodes "$TWO_TIER_NODES" \
    up --force
fi

# --docker 与 --two-tier 互斥(两者同时给出时上面已把 DO_DOCKER 置 0)。
if [ "$DO_DOCKER" = "1" ]; then
  echo "==> [docker] 更新 ${DOCKER_NODES} 节点单层集群(cluster.sh update)…"
  "$ROOT/scripts/cluster.sh" update "$DOCKER_NODES"
fi

# ---------- 4. 重启 gateway / router(默认) ----------
# 从运行中进程的 /proc/<pid>/cmdline 提取完整启动参数并原样复用,这样无论
# 之前是 scripts/gateway.sh 还是手动 nohup 启动的,重启后参数
# 都与当前配置一致(端口、节点列表、static、ops-config 等)。
restart_process() {
  local pattern="$1" name="$2"
  local pids
  pids="$(pgrep -f "$pattern" || true)"
  # 排除僵尸进程(defunct,已在退出中的不做处理)
  local live=()
  local pid
  for pid in $pids; do
    if [ -d "/proc/$pid" ] && ! grep -q defunct "/proc/$pid/status" 2>/dev/null; then
      live+=("$pid")
    fi
  done
  if [ "${#live[@]}" -eq 0 ]; then
    echo "==> [重启] $name:未在运行,跳过"
    return 0
  fi

  # 取第一个存活进程的原始参数(程序路径本身丢弃,第 2 段起为参数)
  local pid0="${live[0]}"
  local args
  args="$(tr '\0' ' ' < "/proc/$pid0/cmdline")"
  args="${args#* }" # 去掉 argv[0]

  echo "==> [重启] $name(PID ${live[*]})→ 用原参数重启: $BIN/$(basename "$pid0") $args"
  kill "${live[@]}" 2>/dev/null || true
  # 等待旧进程真正退出(最多 5 秒)
  for _ in $(seq 1 50); do
    local alive=()
    for pid in "${live[@]}"; do
      [ -d "/proc/$pid" ] && alive+=("$pid")
    done
    [ "${#alive[@]}" -eq 0 ] && break
    sleep 0.1
  done

  nohup "$BIN/$(basename "$pid0")" $args >"$LOG/$name.log" 2>&1 &
  disown 2>/dev/null || true
  echo "   → 已启动(日志:$LOG/$name.log,新 PID $!)"
  sleep 0.5
}

if [ "$DO_RESTART" = "1" ]; then
  restart_process "run/bin/stratum-router" "router"
  restart_process "run/bin/stratum-gateway" "gateway"
else
  echo "==> [重启] 已跳过(--no-restart);请手动重启 gateway/router 使新二进制生效"
fi

echo
echo "=============================================="
echo "  更新完成"
echo "  二进制目录:$BIN/"
echo "  日志目录:  $LOG/"
# 用 if 而不是 `[ ... ] && echo ...`:后者在条件为假时返回非零,如果哪天它成了
# 脚本的最后一条语句,就会让脚本以 1 退出(现在后面还有 echo 才没出事)。
if [ "$DO_FRONTEND" = "1" ]; then echo "  前端:      web/dist 已重建(--no-frontend 可跳过)"; fi
if [ "$DO_DOCKER" = "1" ]; then echo "  docker 集群:已更新 ${DOCKER_NODES} 节点(单层)"; fi
if [ "$DO_TWO_TIER" = "1" ]; then echo "  两层集群:已更新(控制 ${TWO_TIER_NODES} + 存储 ${TWO_TIER_NODES})"; fi
if [ "$DO_RESTART" = "1" ]; then echo "  gateway/router:已按原参数重启"; fi
echo "  提示:浏览器访问控制台时请强制刷新(Ctrl+Shift+R)"
echo "=============================================="
