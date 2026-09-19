#!/usr/bin/env bash
# status.sh — Stratum 系统状态总览。
#
# 汇总 AdminService.GetSystemStatus 的输出，把需要人处置的东西都列出来：
#   - 健康（三态）
#   - 卡住的版本：index_status 为 FAILED，可用 kb-rebuild.sh 重建
#   - 永久失败的版本：控制层判了 FAILED_PERMANENT（§10.1），没有东西会自动重试，
#     只能由人重试或放弃（kb-rebuild.sh / kb-discard-version.sh）
#   - 数据缺失的版本：PENDING 且没有任何候选副本持有它的数据（§7.12）——自己
#     好不了，只有写入方用同一个幂等键重发才能救（kb-version-create.sh
#     --client-request-id）
#   - 删除中的版本：异步清理还没收尾
#   - 删除失败的知识库
#   - WAL 告警
#   - 回收受阻的版本（gc_blocked_versions，§8.6(d)）：活跃版本的墓碑占比超标，
#     但其余副本不够，本节点不敢回收。告警而非故障：数据完好、仍可查询
#   - 资源占用
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
health_details=$(echo "$raw" | jq -r '.health.details // empty')
echo "== 健康状态: $(proto_enum_short "$health_status")"
[[ -n "$health_details" ]] && echo "   详情: $health_details"

# status_enum 把 proto 全名削短，只用于显示（PROTO 名太长，满屏都是前缀）。
# jq 里写死了这份映射，因为 jq 调不到 lib.sh 的 proto_enum_short。
STATUS_FILTER='def short: sub("^(INDEX|DATA|KB|HEALTH)_STATUS_"; "") | sub("^FAILURE_SIDE_"; "");'

echo
echo "== 卡住的版本（索引构建失败，可用 kb-rebuild.sh 重建）"
stuck=$(echo "$raw" | jq -r '[.stuck_versions[]?] | length')
if [[ "$stuck" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r "$STATUS_FILTER"'
    .stuck_versions[]? | "  KB: \(.kb_id)  版本: \(.version_id)  索引状态: \(.index_status|short)  更新时间: \(.updated_at)"'
fi

echo
echo "== 永久失败的版本（§10.1：控制层已判定，没有东西会自动重试）"
permanent=$(echo "$raw" | jq -r '[.failed_permanent_versions[]?] | length')
if [[ "$permanent" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r "$STATUS_FILTER"'
    .failed_permanent_versions[]? |
    "  KB: \(.kb_id)  版本: \(.version_id)  侧: \(.side|short)  失败 \(.failure_count) 次  原因: \(.reason)"'
  echo "  处置：数据侧 → 用同一幂等键重发（kb-version-create.sh --client-request-id）；"
  echo "        索引侧 → kb-rebuild.sh <KB> <版本>；都不想要 → kb-discard-version.sh"
fi

echo
echo "== 数据缺失的版本（§7.12：没有任何候选副本持有数据，重发才能救）"
data_missing=$(echo "$raw" | jq -r '[.data_missing_versions[]?] | length')
if [[ "$data_missing" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r "$STATUS_FILTER"'
    .data_missing_versions[]? | "  KB: \(.kb_id)  版本: \(.version_id)  索引状态: \(.index_status|short)"'
fi

echo
echo "== 删除中的版本（异步清理尚未收尾）"
deleting=$(echo "$raw" | jq -r '[.deleting_versions[]?] | length')
if [[ "$deleting" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r "$STATUS_FILTER"'
    .deleting_versions[]? | "  KB: \(.kb_id)  版本: \(.version_id)  索引状态: \(.index_status|short)"'
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
echo "== 回收受阻的版本（§8.6(d)：墓碑超标但副本不足，数据完好、仍可查询）"
gc_blocked=$(echo "$raw" | jq -r '[.gc_blocked_versions[]?] | length')
if [[ "$gc_blocked" -eq 0 ]]; then
  echo "  无"
else
  echo "$raw" | jq -r '
    .gc_blocked_versions[]? |
    "  KB: \(.kb_id)  版本: \(.version_id)  死向量占比: \(.dead_share)  在服务的其它副本: \(.others_serving)（要求 \(.minimum_required)）  始于: \(.blocked_since)"'
  echo "  处置：调大副本数，或调小 index_manager.serving_replica_min"
fi

echo
echo "== 资源占用"
echo "$raw" | jq -r '
  "  已加载索引数: \(.resource_usage.loaded_index_count // 0)",
  "  chunk 存储: \(.resource_usage.chunk_store_bytes // 0) bytes",
  "  文档存储: \(.resource_usage.doc_store_bytes // 0) bytes"'

# 有需要人处置的条目就给非零退出码，方便监控脚本感知。FAILED_PERMANENT 与
# data_missing 也算：它们不会自愈，不报出来就等于没人知道。
if [[ "$stuck" -gt 0 || "$deleted_failed" -gt 0 || "$permanent" -gt 0 || "$data_missing" -gt 0 ]]; then
  exit 1
fi
