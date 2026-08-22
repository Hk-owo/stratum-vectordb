#!/usr/bin/env bash
# kb-get.sh — 查看单个知识库的完整配置与激活版本。
#
# 用法：
#   scripts/ops/kb-get.sh <知识库ID>
#   scripts/ops/kb-get.sh <知识库ID> --json   # 输出原始 JSON

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

resp=$(curl -sS -w $'\n%{http_code}' "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')") || {
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

echo "$resp" | jq -r '
  .knowledge_base |
  "知识库 ID:     \(.knowledge_base_id)",
  "名称:          \(.name)",
  "状态:          \(.status)",
  "分块窗口:      \(.chunk_window_size)（重叠 \(.chunk_overlap_size)）",
  "索引/相似度:   \(.index_type) / \(.similarity)",
  "激活版本:      \(.active_version_id)",
  "embed 服务:    \(.embed_config.service_addr)",
  "embed 模型:    \(.embed_config.model_id)"'
