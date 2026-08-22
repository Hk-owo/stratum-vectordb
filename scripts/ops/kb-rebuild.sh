#!/usr/bin/env bash
# kb-rebuild.sh — 重建指定版本的索引（异步）。
#
# 适用场景：status.sh 显示某版本 index_status 为 FAILED（索引构建失败），
# 修复了原因（如 embed 服务恢复、vecstore 恢复）后重新触发构建。
#
# 用法：
#   scripts/ops/kb-rebuild.sh <知识库ID> <版本号>
#   scripts/ops/kb-rebuild.sh kb-xxx 3
#
# 接口异步返回，构建在后台进行；可用 kb-versions.sh 观察状态变为 READY。

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
  "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/rebuild" \
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

echo "已触发重建：知识库 $KB_ID 版本 $VERSION（后台异步，可用 kb-versions.sh 观察状态）"
