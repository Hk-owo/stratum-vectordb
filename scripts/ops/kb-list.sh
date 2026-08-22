#!/usr/bin/env bash
# kb-list.sh — 列出全部知识库（ID / 名称 / 状态 / 当前激活版本）。
#
# 用法：
#   scripts/ops/kb-list.sh
#   scripts/ops/kb-list.sh --api http://10.0.0.5:8081

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) echo "用法: $0 [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

resp=$(curl -sS "$STRATUM_API/api/knowledge-bases") || {
  echo "错误：无法连接 $STRATUM_API（gateway 是否已启动？）" >&2
  exit 1
}

count=$(echo "$resp" | jq -r '[.knowledge_bases[]?] | length')
echo "共 $count 个知识库"
echo
echo "$resp" | jq -r '
  .knowledge_bases[]? |
  "  \(.knowledge_base_id)  \(.name)  [\(.status)]  激活版本: \(.active_version_id)  (\(.index_type)/\(.similarity))"' || true
