#!/usr/bin/env bash
# kb-warmup.sh — 预热版本索引到内存（异步）。
#
# 适用场景：大版本首次查询慢。预热把指定版本的索引加载进内存缓存，
# 但不切换激活版本，之后该版本的查询就快了。
#
# 用法：
#   scripts/ops/kb-warmup.sh <知识库ID> <版本号>
#   scripts/ops/kb-warmup.sh kb-xxx 5
#
# 可用 status.sh 的"已加载索引数"观察预热效果。

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
VERSION=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) echo "用法: $0 <知识库ID> <版本号> [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *)
      if [[ -z "$KB_ID" ]]; then KB_ID="$1"; else VERSION="$1"; fi
      shift ;;
  esac
done

if [[ -z "$KB_ID" || -z "$VERSION" ]]; then
  echo "错误：用法 $0 <知识库ID> <版本号>" >&2
  exit 1
fi

body=$(jq -n --argjson v "$VERSION" '{version_id: $v}')
resp=$(curl -sS -w $'\n%{http_code}' -X POST \
  "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/warmup" \
  -H 'Content-Type: application/json' --data "$body") || {
  echo "错误：无法连接 $STRATUM_API" >&2
  exit 1
}
code="${resp##*$'\n'}"
resp="${resp%$'\n'*}"
if [[ ! "$code" =~ ^2[0-9][0-9]$ ]]; then
  echo "$resp" | jq . >&2
  echo "错误：HTTP $code" >&2
  exit 1
fi

echo "已触发预热：知识库 $KB_ID 版本 $VERSION（后台异步加载到内存）"
