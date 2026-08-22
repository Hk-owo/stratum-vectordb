#!/usr/bin/env bash
# stop.sh — 停止 start.sh 拉起的全部 Stratum 服务。
#
# 停止顺序（与启动相反）：gateway → stratum → mock-embed → vecstore_server。
# 先发 SIGTERM 优雅退出，5 秒后仍未退出再发 SIGKILL。
#
# 用法：
#   scripts/ops/stop.sh              # 停止全部服务
#   scripts/ops/stop.sh --dry-run    # 只列出会停止的进程
#   scripts/ops/stop.sh --force      # 不等优雅退出，直接 SIGKILL

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/run/bin"

# 停止顺序：先停依赖方，再停基础组件。
BINS=("stratum-gateway" "stratum" "mock-embed" "vecstore_server")

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

echo "完成。数据保留在 $ROOT/run/data/（删除前先停服，见 delete_test_db.py）"
