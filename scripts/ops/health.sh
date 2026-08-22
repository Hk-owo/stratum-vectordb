#!/usr/bin/env bash
# health.sh — Stratum 健康检查。
#
# 输出三态健康结果：HEALTHY / DEGRADED / UNHEALTHY（附 details）。
# 适合接入监控：脚本退出码 0 表示 HEALTHY，非 0 表示异常。
#
# 用法：
#   scripts/ops/health.sh
#   scripts/ops/health.sh --api http://10.0.0.5:8081
#   scripts/ops/health.sh --quiet   # 只输出状态字符串，便于脚本判断

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

QUIET=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --quiet) QUIET=1; shift ;;
    -h|--help) echo "用法: $0 [--api URL] [--quiet]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

resp=$(curl -sS -w $'\n%{http_code}' "$STRATUM_API/api/health") || {
  echo "错误：无法连接 $STRATUM_API（gateway 是否已启动？）" >&2
  exit 2
}
code="${resp##*$'\n'}"
resp="${resp%$'\n'*}"

status=$(echo "$resp" | jq -r '.status // empty' 2>/dev/null)
details=$(echo "$resp" | jq -r '.details // empty' 2>/dev/null)

if [[ "$code" =~ ^2[0-9][0-9]$ ]] && [[ "$status" == "HEALTH_STATUS_HEALTHY" ]]; then
  if [[ "$QUIET" -eq 1 ]]; then
    echo "HEALTHY"
  else
    echo "状态: HEALTHY"
    [[ -n "$details" ]] && echo "详情: $details"
  fi
  exit 0
fi

if [[ "$QUIET" -eq 1 ]]; then
  echo "${status:-UNHEALTHY}${code:+(HTTP $code)}"
else
  echo "状态: ${status:-未知}（HTTP $code）"
  [[ -n "$details" ]] && echo "详情: $details"
fi
exit 1
