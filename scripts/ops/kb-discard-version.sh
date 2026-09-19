#!/usr/bin/env bash
# kb-discard-version.sh — 放弃一个写入从未落地的版本。
#
# 适用场景（docs/await-version-plan.md §5 contract 7、§7 Step 6）：某个版本一直
# 停在 PENDING，await 报 data_missing —— 没有任何可达副本持有它的数据。这种版本
# 自己好不了，能做的只有两件：用自己的幂等键把变更**重发**一遍
# （kb-version-create.sh --client-request-id），或者放弃它、腾出元数据。
#
# 它不等于删除版本：放弃没有数据要回收、没有孩子要改挂，去掉元数据就是全部。
# 因此服务端只在版本仍是 PENDING 时接受它；已经有数据的版本属于
# kb-version-delete.sh 的事，会被以 version_not_pending 拒绝。
#
# 重复放弃是幂等的（discarded=false 表示已经没有可放弃的东西）。
#
# 用法：
#   scripts/ops/kb-discard-version.sh <知识库ID> <版本号>
#   scripts/ops/kb-discard-version.sh kb-xxx 7 --yes
#   scripts/ops/kb-discard-version.sh kb-xxx 7 --json

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
VERSION=""
FORCE=0
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes) FORCE=1; shift ;;
    --json) JSON_OUT=1; shift ;;
    -h|--help) echo "用法: $0 <知识库ID> <版本号> [--yes] [--json] [--api URL]"; exit 0 ;;
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

if [[ "$FORCE" -ne 1 ]]; then
  read -r -p "确定要放弃版本 $VERSION 吗（仅当它仍是 PENDING；数据不会被回收）？[y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "已取消"; exit 0; }
fi

body=$(jq -nc --argjson v "$VERSION" '{version_id: $v}')
resp=$(api_post_raw "/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/discard-version" "$body")

if [[ "$JSON_OUT" -eq 1 ]]; then
  echo "$resp" | jq .
  exit 0
fi

discarded=$(echo "$resp" | jq -r '.discarded')
if [[ "$discarded" == "true" ]]; then
  echo "已放弃版本 $VERSION（元数据已移除）"
else
  echo "版本 $VERSION 没有可放弃的东西（可能已被移除；重复放弃是幂等的）"
fi
