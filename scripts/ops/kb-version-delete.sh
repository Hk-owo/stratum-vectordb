#!/usr/bin/env bash
# kb-version-delete.sh — 删除版本（异步清理其数据与索引）。
#
# v13 之后版本删除有三种范围（VersionDeleteMode），默认是历史行为：
#   subtree（默认）  删目标版本**及其全部后继版本**
#   single           只删目标版本本身：它的孩子（链是线性的，至多一个）
#                    被改挂到目标的父版本上——链表式摘除，历来的"删中间版本"
#   ancestors        删目标版本的全部前置版本，让目标成为链首（前缀删除）
#
# 接口是异步的：调用返回后版本先被标记，后台再清理数据/索引。用
# kb-versions.sh 看 [删除中]，用 status.sh 看整体收尾情况。
#
# 用法：
#   scripts/ops/kb-version-delete.sh <知识库ID> <版本号>
#   scripts/ops/kb-version-delete.sh kb-xxx 3 --mode single
#   scripts/ops/kb-version-delete.sh kb-xxx 5 --mode ancestors --yes
#
# 激活版本也能删：删掉后知识库会落回其父版本（没有父版本时就没有激活版本）。

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
VERSION=""
MODE="subtree"
FORCE=0
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --yes) FORCE=1; shift ;;
    --json) JSON_OUT=1; shift ;;
    -h|--help) echo "用法: $0 <知识库ID> <版本号> [--mode subtree|single|ancestors] [--yes] [--json] [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *)
      if [[ -z "$KB_ID" ]]; then KB_ID="$1"; else VERSION="$1"; fi
      shift ;;
  esac
done

if [[ -z "$KB_ID" || -z "$VERSION" ]]; then
  echo "错误：用法 $0 <知识库ID> <版本号> [--mode subtree|single|ancestors]" >&2
  exit 1
fi

MODE_PROTO="$(proto_enum delete_mode "$MODE")" || exit 1

if [[ "$FORCE" -ne 1 ]]; then
  case "$MODE_PROTO" in
    VERSION_DELETE_MODE_SUBTREE)   scope="版本 $VERSION 及其全部后继版本" ;;
    VERSION_DELETE_MODE_SINGLE)    scope="仅版本 $VERSION（其孩子改挂到父版本）" ;;
    VERSION_DELETE_MODE_ANCESTORS) scope="版本 $VERSION 的全部前置版本（$VERSION 成为链首）" ;;
  esac
  read -r -p "确定要删除 $scope 吗？数据与索引会在后台清理 [y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "已取消"; exit 0; }
fi

body=$(jq -nc --argjson v "$VERSION" --arg m "$MODE_PROTO" '{version_id: $v, mode: $m}')
resp=$(api_post_raw "/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/delete-version" "$body")

if [[ "$JSON_OUT" -eq 1 ]]; then
  echo "$resp" | jq .
  exit 0
fi

deleted=$(echo "$resp" | jq -r '.deleted_version_ids | length')
if [[ "$deleted" -eq 0 ]]; then
  echo "没有版本被标记删除（例如 ancestors 模式下目标本来就是链首）"
  exit 0
fi
echo "已标记删除 $deleted 个版本：$(echo "$resp" | jq -r '[.deleted_version_ids[]?] | join(", ")')"
echo "后台清理中：scripts/ops/kb-versions.sh $KB_ID   # 看 [删除中] / status.sh 看全局"
