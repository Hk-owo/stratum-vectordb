#!/usr/bin/env bash
# stop.sh — 进程级兜底：按二进制名停掉 run/bin 下的全部 Stratum 服务。
#
# 与 scripts/gateway.sh stop 的分工：
#   - scripts/gateway.sh stop 是**首选**：它走控制台的 /ops/stop，让控制台收掉自己
#     管理的子进程，再停控制台与服务站。控制台还能用的时候应该用它。
#   - 本脚本不看控制台，只按 run/bin 下的进程名停：控制台已经挂了、或者进程是别的
#     方式拉起来的（scripts/cluster.sh 的宿主 vecstore、手工 nohup 的节点）时用它。
#
# 停止顺序（与启动相反）：gateway → 服务站(stratum-router) → stratum →
# mock-embed → vecstore_server。先发 SIGTERM 优雅退出，5 秒后仍未退出再发 SIGKILL。
#
# 注意：容器形态的节点归 scripts/cluster.sh 管（cluster.sh down），本脚本只处理
# 宿主进程，不碰容器。
#
# 用法：
#   scripts/ops/stop.sh              # 停止全部服务
#   scripts/ops/stop.sh --dry-run    # 只列出会停止的进程
#   scripts/ops/stop.sh --force      # 不等优雅退出，直接 SIGKILL

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/run/bin"

# 停止顺序：先停依赖方，再停基础组件。
BINS=("stratum-gateway" "stratum-router" "stratum" "mock-embed" "vecstore_server")

DRY_RUN=0
FORCE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --force) FORCE=1; shift ;;
    -h|--help) echo "用法: $0 [--dry-run] [--force]"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

# stratum 需精确匹配，避免前缀误伤 stratum-gateway
bin_pattern() {
  if [[ "$1" == "stratum" ]]; then
    echo "$BIN/stratum([ )]|\$)"
  else
    echo "$BIN/$1"
  fi
}

stop_one() {
  local bin="$1" sig="${2:-TERM}"
  local pids pattern
  pattern=$(bin_pattern "$bin")
  pids=$(pgrep -f "$pattern" || true)
  if [[ -z "$pids" ]]; then
    echo "  $bin: 未在运行"
    return
  fi
  echo "  $bin: 发送 SIG$sig 到 $(echo "$pids" | tr '\n' ' ')"
  [[ "$DRY_RUN" -eq 1 ]] && return
  # shellcheck disable=SC2086
  kill -s "$sig" $pids 2>/dev/null || true
}

echo "Stratum 服务停止脚本（--dry-run 只列不停）"
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "以下进程将被停止："
  for bin in "${BINS[@]}"; do
    pids=$(pgrep -f "$(bin_pattern "$bin")" || true)
    [[ -n "$pids" ]] && echo "  $bin (PID: $(echo "$pids" | tr '\n' ' '))"
  done
  exit 0
fi

for bin in "${BINS[@]}"; do
  stop_one "$bin"
done

if [[ "$FORCE" -ne 1 ]]; then
  # 等待优雅退出（最多 5 秒）
  for _ in $(seq 1 5); do
    remaining=$(pgrep -f "$BIN/" || true)
    [[ -z "$remaining" ]] && break
    sleep 1
  done
  # 仍未退出的强制终止
  for bin in "${BINS[@]}"; do
    stop_one "$bin" KILL
  done
else
  for bin in "${BINS[@]}"; do
    stop_one "$bin" KILL
  done
fi

echo "完成。数据保留在 $ROOT/run/data/（彻底清空请先停服，再删除该目录下的 stratum/ 与 vecstore_rocksdb/）"
