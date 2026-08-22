#!/usr/bin/env bash
# kb-rollback.sh — 切换知识库的激活版本（运行时发布/回滚，无停机）。
#
# 版本切换是即时的：之后所有不带 version_id 的查询都命中新激活版本。
# 旧版本数据仍然保留，可随时再切回去（A/B 测试、回滚事故版本）。
#
# 用法：
#   scripts/ops/kb-rollback.sh <知识库ID> <版本号>
#   scripts/ops/kb-rollback.sh kb-xxx 2          # 切到版本 2
#
# 小技巧：先用 kb-versions.sh 看版本链，用 kb-get.sh 看当前激活版本。

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

body=$(jq -n --argjson v "$VERSION" '{target_version_id: $v}')
resp=$(curl -sS -w $'\n%{http_code}' -X POST \
  "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/rollback" \
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

echo "已切换：知识库 $KB_ID 的激活版本 → $VERSION"
