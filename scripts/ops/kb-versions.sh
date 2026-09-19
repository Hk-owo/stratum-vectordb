#!/usr/bin/env bash
# kb-versions.sh — 查看知识库的版本链。
#
# 每个版本显示：版本号、父版本、索引状态与数据状态、是否正在删除、创建时间
# （Unix 秒）。
#   index_status  PENDING 构建中 / READY 可查询 / FAILED 构建失败可重建 /
#                 FAILED_PERMANENT 控制层判定，只能人工处置
#   data_status   PENDING 写入中 / DURABLE 已持久 / FAILED_PERMANENT 数据不会到
#   deleting      true 表示异步删除清理还没收尾
#
# 用法：
#   scripts/ops/kb-versions.sh <知识库ID>
#   scripts/ops/kb-versions.sh <知识库ID> --json

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

JSON=0
KB_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --json) JSON=1; shift ;;
    -h|--help) echo "用法: $0 <知识库ID> [--api URL] [--json]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) KB_ID="$1"; shift ;;
  esac
done

if [[ -z "$KB_ID" ]]; then
  echo "错误：缺少知识库 ID（先用 kb-list.sh 查看有哪些）" >&2
  exit 1
fi

resp=$(curl -sS -w $'\n%{http_code}' "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/versions") || {
  echo "错误：无法连接 $STRATUM_API" >&2
  exit 1
}
code="${resp##*$'\n'}"
resp="${resp%$'\n'*}"
if [[ ! "$code" =~ ^2[0-9][0-9]$ ]]; then
  echo "$resp" | jq . >&2
  exit 1
fi

if [[ "$JSON" -eq 1 ]]; then
  echo "$resp" | jq .
  exit 0
fi

count=$(echo "$resp" | jq -r '[.versions[]?] | length')
if [[ "$count" -eq 0 ]]; then
  echo "知识库 $KB_ID 还没有版本（创建知识库不再产生初始版本；写入第一批文档后才有）"
  exit 0
fi
echo "知识库 $KB_ID 共 $count 个版本："
echo
echo "$resp" | jq -r '
  def short: sub("^(INDEX|DATA)_STATUS_"; "");
  .versions[]? |
  "  版本 \(.version_id)  父版本 \(.parent_version_id)  索引 \(.index_status|short)  数据 \(.data_status|short)\(if .deleting then "  [删除中]" else "" end)  创建于 \(.created_at)"'
