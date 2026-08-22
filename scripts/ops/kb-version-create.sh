#!/usr/bin/env bash
# kb-version-create.sh — 向知识库提交一批文档变更，生成新版本。
#
# 这是"运行时修改内容"的入口：对知识库做 ADD（新增）/ UPDATE（更新）/
# DELETE（删除）文档，每次提交产生一个新版本（构建索引后即可查询，
# 当前激活版本不变，直到用 kb-rollback.sh 切换）。
#
# 变更从 JSON 文件读入，两种格式均可：
#   1) 完整 body：{"changes": [{"op": "ADD", "doc_id": "d1", "content": "..."}]}
#   2) 纯数组：  [{"op": "ADD", "doc_id": "d1", "content": "..."}]
# op 支持 ADD / UPDATE / DELETE（不区分大小写）；DELETE 不需要 content。
#
# 也支持单条快捷写法（适合少量手工变更）：
#   --add <doc_id> --content "文本"         # 新增
#   --update <doc_id> --content "文本"      # 更新
#   --delete <doc_id>                       # 删除
#
# 用法示例：
#   scripts/ops/kb-version-create.sh kb-xxx --changes changes.json
#   scripts/ops/kb-version-create.sh kb-xxx --add d1 --content "Hello Stratum"
#   scripts/ops/kb-version-create.sh kb-xxx --changes changes.json --parent 3
#   scripts/ops/kb-version-create.sh kb-xxx --json --changes changes.json

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
CHANGES_FILE=""
PARENT=""
QUICK_OP=""
QUICK_DOC=""
QUICK_CONTENT=""
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --changes) CHANGES_FILE="${2:?--changes 需要一个文件路径}"; shift 2 ;;
    --parent) PARENT="$2"; shift 2 ;;
    --add) QUICK_OP="ADD"; QUICK_DOC="${2:?--add 需要一个 doc_id}"; shift 2 ;;
    --update) QUICK_OP="UPDATE"; QUICK_DOC="${2:?--update 需要一个 doc_id}"; shift 2 ;;
    --delete) QUICK_OP="DELETE"; QUICK_DOC="${2:?--delete 需要一个 doc_id}"; shift 2 ;;
    --content) QUICK_CONTENT="${2:?--content 需要一个文本参数}"; shift 2 ;;
    --json) JSON_OUT=1; shift ;;
    -h|--help) echo "用法: $0 <知识库ID> (--changes 文件 | --add/--update/--delete doc_id) [--content 文本] [--parent N] [--json] [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) KB_ID="$1"; shift ;;
  esac
done

if [[ -z "$KB_ID" ]]; then
  echo "错误：缺少知识库 ID" >&2
  exit 1
fi
if [[ -z "$CHANGES_FILE" && -z "$QUICK_DOC" ]]; then
  echo "错误：必须提供 --changes 文件，或 --add/--update/--delete 快捷方式" >&2
  exit 1
fi
if [[ -n "$QUICK_OP" && "$QUICK_OP" != "DELETE" && -z "$QUICK_CONTENT" ]]; then
  echo "错误：--add / --update 必须带 --content" >&2
  exit 1
fi

# ---------- 构造 changes 数组 ----------
if [[ -n "$CHANGES_FILE" ]]; then
  [[ -f "$CHANGES_FILE" ]] || { echo "错误：找不到变更文件 $CHANGES_FILE" >&2; exit 1; }
  # 接受 {"changes": [...]} 或纯 [...]；op 统一转大写。
  changes=$(jq -c '
    if type == "array" then .
    elif has("changes") then .changes
    else error("变更文件必须是 {\"changes\":[...]} 或纯数组")
    end
    | map(.op |= ascii_upcase)' "$CHANGES_FILE") \
    || { echo "错误：变更文件格式不正确（$CHANGES_FILE）" >&2; exit 1; }
  n=$(echo "$changes" | jq 'length')
  echo "读入 $n 条变更：$(echo "$changes" | jq -r '[.[].op] | group_by(.) | map("\(.[0])=\(length)") | join(", ")')"
else
  if [[ "$QUICK_OP" == "DELETE" ]]; then
    changes=$(jq -nc --arg op "$QUICK_OP" --arg id "$QUICK_DOC" '[{op: $op, doc_id: $id}]')
  else
    changes=$(jq -nc --arg op "$QUICK_OP" --arg id "$QUICK_DOC" --arg c "$QUICK_CONTENT" \
      '[{op: $op, doc_id: $id, content: $c}]')
  fi
fi

# ---------- 组装 body 并提交 ----------
if [[ -n "$PARENT" ]]; then
  parent_json=$(jq -nc --argjson p "$PARENT" '$p')
else
  # 缺省时基于当前激活版本创建（形成版本链，父版本 = 激活版本）。
  kb_resp=$(curl -sS -w $'\n%{http_code}' \
    "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')") || {
    echo "错误：无法获取知识库 $KB_ID 信息（gateway 是否已启动？）" >&2
    exit 1
  }
  kb_code="${kb_resp##*$'\n'}"
  kb_resp="${kb_resp%$'\n'*}"
  if [[ ! "$kb_code" =~ ^2[0-9][0-9]$ ]]; then
    echo "$kb_resp" | jq . >&2
    exit 1
  fi
  parent_json=$(echo "$kb_resp" | jq -c '.knowledge_base.active_version_id')
  echo "（未指定 --parent，默认基于当前激活版本 $parent_json）"
fi
body=$(jq -nc --argjson p "$parent_json" --argjson ch "$changes" \
  '{parent_version_id: $p, changes: $ch}')

resp=$(curl -sS -w $'\n%{http_code}' -X POST \
  "$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/versions" \
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

if [[ "$JSON_OUT" -eq 1 ]]; then
  echo "$resp" | jq .
else
  vid=$(echo "$resp" | jq -r '.version_id')
  echo "提交成功：生成版本 $vid（索引构建中；构建完成后可用 kb-rollback.sh 切换为激活版本）"
fi
