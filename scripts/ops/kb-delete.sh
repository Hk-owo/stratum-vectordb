#!/usr/bin/env bash
# kb-delete.sh — 删除知识库。
#
# 删除是异步的：接口立即返回，后台清理数据。删除期间知识库状态变为
# DELETING；如果清理失败会变成 DELETE_FAILED。
#
# 本脚本会轮询删除结果：
#   - 发现 DELETE_FAILED 自动重新发起删除（默认最多重试 3 次）
#   - 知识库从列表消失即视为删除成功
#   - 超时仍未完成则报错退出（非零退出码）
#
# 用法：
#   scripts/ops/kb-delete.sh <知识库ID> [--yes] [--retries N] [--wait S] [--timeout S]
#   scripts/ops/kb-delete.sh <知识库ID> --yes --api http://10.0.0.5:8081

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
FORCE=0
RETRIES=3
WAIT=5
TIMEOUT=120
while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes) FORCE=1; shift ;;
    --retries) RETRIES="$2"; shift 2 ;;
    --wait) WAIT="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    -h|--help) echo "用法: $0 <知识库ID> [--yes] [--retries N] [--wait S] [--timeout S] [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) KB_ID="$1"; shift ;;
  esac
done

if [[ -z "$KB_ID" ]]; then
  echo "错误：缺少知识库 ID（先用 kb-list.sh 查看有哪些）" >&2
  exit 1
fi

if [[ "$FORCE" -ne 1 ]]; then
  read -r -p "确定要删除知识库 $KB_ID 及其全部版本吗？[y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "已取消"; exit 0; }
fi

body=$(jq -n --arg id "$KB_ID" '{knowledge_base_id: $id}')

# 目标 KB 是否仍出现在知识库列表（出现 = 删除未完成/失败）
kb_still_exists() {
  curl -sS "$STRATUM_API/api/knowledge-bases" 2>/dev/null | \
    jq -e --arg id "$KB_ID" '.knowledge_bases[]? | select(.knowledge_base_id == $id)' >/dev/null 2>&1
}

# 目标 KB 是否已标记 DELETE_FAILED
kb_delete_failed() {
  curl -sS "$STRATUM_API/api/system-status" 2>/dev/null | \
    jq -e --arg id "$KB_ID" '.delete_failed_kbs[]? | select(. == $id)' >/dev/null 2>&1
}

# 发起一次删除
do_delete() {
  local body="$1"
  local resp code
  resp=$(curl -sS -w $'\n%{http_code}' -X POST "$STRATUM_API/api/knowledge-bases/delete" \
    -H 'Content-Type: application/json' --data "$body") || return 1
  code="${resp##*$'\n'}"
  if [[ ! "$code" =~ ^2[0-9][0-9]$ ]]; then
    echo "错误：删除接口返回 HTTP $code" >&2
    return 1
  fi
}

if ! do_delete "$body"; then
  exit 1
fi
echo "知识库 $KB_ID 已标记删除，等待后台清理完成（超时 ${TIMEOUT}s，失败自动重试最多 ${RETRIES} 次）…"

attempt=0
elapsed=0
while [[ "$elapsed" -lt "$TIMEOUT" ]]; do
  if kb_delete_failed; then
    attempt=$((attempt + 1))
    if [[ "$attempt" -gt "$RETRIES" ]]; then
      echo "错误：$KB_ID 删除失败并已重试 $RETRIES 次仍为 DELETE_FAILED" >&2
      echo "提示：常见原因是 vecstore 不可用导致 chunk 清理失败；恢复 vecstore 后用 status.sh 观察，或重跑本脚本" >&2
      exit 1
    fi
    echo "检测到删除失败（第 $attempt/$RETRIES 次），重新发起删除…"
    if ! do_delete "$body"; then
      exit 1
    fi
    sleep "$WAIT"
    elapsed=$((elapsed + WAIT))
    continue
  fi

  if ! kb_still_exists; then
    echo "知识库 $KB_ID 已删除"
    exit 0
  fi
  sleep "$WAIT"
  elapsed=$((elapsed + WAIT))
done

echo "错误：等待删除完成超时（${TIMEOUT}s），可用 status.sh 观察状态后重试" >&2
exit 1
