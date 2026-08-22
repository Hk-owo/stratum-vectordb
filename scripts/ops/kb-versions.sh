#!/usr/bin/env bash
# kb-versions.sh — 查看知识库的版本链。
#
# 每个版本显示：版本号、父版本、创建时间、索引状态
# （PENDING 构建中 / READY 可查询 / FAILED 构建失败可重建）。
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
echo "知识库 $KB_ID 共 $count 个版本："
echo
echo "$resp" | jq -r '
  .versions[]? |
  "  版本 \(.version_id)  父版本 \(.parent_version_id)  状态 \(.index_status)  创建于 \(.created_at)"' || true
