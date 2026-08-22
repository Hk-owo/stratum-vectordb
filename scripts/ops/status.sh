#!/usr/bin/env bash
# status.sh — Stratum 系统状态总览。
#
# 汇总 AdminService.GetSystemStatus 的输出：健康、卡住的版本
# （index_status 为 FAILED 的版本，可用 kb-rebuild.sh 重建）、
# 删除失败的知识库、WAL 告警、资源占用。
#
# 用法：
#   scripts/ops/status.sh
#   scripts/ops/status.sh --api http://10.0.0.5:8081
#   scripts/ops/status.sh --json    # 输出原始 JSON（不做汇总）

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

JSON=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --json) JSON=1; shift ;;
    -h|--help) echo "用法: $0 [--api URL] [--json]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

raw=$(curl -sS "$STRATUM_API/api/system-status") || {
  echo "错误：无法连接 $STRATUM_API（gateway 是否已启动？）" >&2
  exit 1
}

if [[ "$JSON" -eq 1 ]]; then
  echo "$raw" | jq .
  exit 0
fi

health_status=$(echo "$raw" | jq -r '.health.status // "未知"')
echo "== 健康状态: $health_status"

echo
echo "== 卡住的版本（索引构建失败，可用 kb-rebuild.sh 重建）"
stuck=$(echo "$raw" | jq -r '[.stuck_versions[]?] | length')
if [[ "$stuck" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r '.stuck_versions[]? | "  KB: \(.kb_id)  版本: \(.version_id)  索引状态: \(.index_status)  更新时间: \(.updated_at)"'
fi

echo
echo "== 删除失败的知识库"
deleted_failed=$(echo "$raw" | jq -r '[.delete_failed_kbs[]?] | length')
if [[ "$deleted_failed" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r '.delete_failed_kbs[]? | "  \(.)"'
fi

echo
echo "== WAL 告警"
wal=$(echo "$raw" | jq -r '[.wal_alerts[]?] | length')
if [[ "$wal" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r '.wal_alerts[]? | "  \(.description)（重试 \(.retry_count) 次）"'
fi

echo
echo "== 资源占用"
echo "$raw" | jq -r '
  "  已加载索引数: \(.resource_usage.loaded_index_count // 0)",
  "  chunk 存储: \(.resource_usage.chunk_store_bytes // 0) bytes",
  "  文档存储: \(.resource_usage.doc_store_bytes // 0) bytes"'

# 存在 FAILED 版本或删除失败时给出非零退出码，方便监控脚本感知。
if [[ "$stuck" -gt 0 || "$deleted_failed" -gt 0 ]]; then
  exit 1
fi
