#!/usr/bin/env bash
# update-all.sh — Stratum 全部二进制一键更新脚本
#
# 把本地所有二进制更新到最新源码,并自动重启正在运行的 gateway / router,
# 避免出现「代码改了但跑的还是旧二进制」(例如:更新了 docker 集群,却忘了
# gateway / router 是本机独立进程,不随镜像更新)。
#
# 默认动作:
#   1. 构建全部 Go 二进制到 run/bin/:
#        stratum / stratum-gateway / stratum-router / mock-embed
#   2. 若 gateway / router 正在运行,用它们当前的启动参数自动重启
#      (新二进制只有重启进程后才生效)
#
# 可选动作:
#   --vecstore      额外(重新)构建 vecstore_server(C++,较慢,首次才需要)
#   --docker [N]    额外更新 docker 集群:等价于 scripts/docker-cluster.sh update N
#                   (重新编译 → 重建镜像 → --force 重建容器,数据卷保留)
#   --no-restart    只构建,不重启 gateway / router(由你手动重启)
#   --help / -h     显示本说明
#
# 用法:
#   scripts/update-all.sh                  # 构建 Go 二进制 + 重启 gateway/router
#   scripts/update-all.sh --docker 3       # 再更新 3 节点 docker 集群
#   scripts/update-all.sh --vecstore --docker 1
#   scripts/update-all.sh --no-restart     # 只构建不重启
#
# 前置依赖:Go 1.24+;--vecstore 额外需要 C++ 工具链 + Faiss/RocksDB/gRPC 等
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
DO_RESTART=1
for arg in "$@"; do
  case "$arg" in
    --vecstore) DO_VECSTORE=1 ;;
    --docker)   DO_DOCKER=1 ;;
    --docker=*) DO_DOCKER=1; DOCKER_NODES="${arg#*=}" ;;
    --no-restart) DO_RESTART=0 ;;
    --help|-h)
      sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      # 兼容 "update-all.sh --docker 3" 与 "update-all.sh --docker=3"
      if [ "$DO_DOCKER" = "1" ] && [[ "$arg" =~ ^[0-9]+$ ]] && [ "$DOCKER_NODES" = "3" ]; then
        DOCKER_NODES="$arg"
      else
        echo "未知参数: $arg(用 --help 查看用法)" >&2
        exit 2
      fi
      ;;
  esac
done

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

# ---------- 3. docker 集群(可选) ----------
if [ "$DO_DOCKER" = "1" ]; then
  echo "==> [docker] 更新 ${DOCKER_NODES} 节点集群(docker-cluster.sh update)…"
  "$ROOT/scripts/docker-cluster.sh" update "$DOCKER_NODES"
fi

# ---------- 4. 重启 gateway / router(默认) ----------
# 从运行中进程的 /proc/<pid>/cmdline 提取完整启动参数并原样复用,这样无论
# 之前是 start.sh、gateway.sh/router.sh 还是手动 nohup 启动的,重启后参数
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
[ "$DO_DOCKER" = "1" ] && echo "  docker 集群:已更新 ${DOCKER_NODES} 节点"
[ "$DO_RESTART" = "1" ] && echo "  gateway/router:已按原参数重启"
echo "  提示:浏览器访问控制台时请强制刷新(Ctrl+Shift+R)"
echo "=============================================="
