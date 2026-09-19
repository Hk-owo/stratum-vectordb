#!/usr/bin/env bash
# kb-await.sh — 等一个版本达到目标状态（写入后确认落地的正路）。
#
# 提交变更（kb-version-create.sh）只是让版本进入 PENDING；数据落盘与索引构建
# 都是异步的。这个脚本按服务端的 AwaitVersion 语义等待，直到：
#   - 达到目标        → 退出码 0
#   - 进入失败终态    → 退出码 1（并说明能做什么）
#   - 等满 --timeout  → 退出码 2（还没有结论，不是失败）
#
# 目标（--target）：
#   index-ready（默认）  版本可查询、也可被 kb-rollback.sh 切换为激活版本
#   data-durable         只等数据落地，索引另行构建（批量导入场景）
#
# 服务端对**单次**等待有 5 秒上限（§5 contract 5：等待中的调用要占控制节点一个
# goroutine），所以本脚本用 wait_timeout_ms=5000 反复调用，直到总超时。
#
# data_missing=true 是重要信号（§4.3）：没有可达副本持有该版本的数据，它自己
# 好不了。此时的选择只有两个——用同一个幂等键重发（kb-version-create.sh
# --client-request-id KEY），放弃版本（kb-discard-version.sh）；服务端不会替你选。
#
# 用法：
#   scripts/ops/kb-await.sh <知识库ID> <版本号>
#   scripts/ops/kb-await.sh kb-xxx 7 --timeout 300
#   scripts/ops/kb-await.sh kb-xxx 7 --target data-durable --interval 500
#   scripts/ops/kb-await.sh kb-xxx 7 --json     # 每次响应的原始 JSON

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
VERSION=""
TARGET="index-ready"
TIMEOUT=120
INTERVAL=""
JSON_OUT=0

# target 直接用 proto 全名发给服务端（这是 protojson 唯一认的写法）。
target_enum() {
  case "$(printf '%s' "$1" | tr '[:lower:]-' '[:upper:]_')" in
    INDEX_READY|INDEX-READY|AWAIT_TARGET_INDEX_READY)   echo "AWAIT_TARGET_INDEX_READY" ;;
    DATA_DURABLE|DATA-DURABLE|AWAIT_TARGET_DATA_DURABLE) echo "AWAIT_TARGET_DATA_DURABLE" ;;
    *) echo "错误：--target 只支持 index-ready / data-durable（收到 '$1'）" >&2; return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --json) JSON_OUT=1; shift ;;
    -h|--help) echo "用法: $0 <知识库ID> <版本号> [--target index-ready|data-durable] [--timeout S] [--interval MS] [--json] [--api URL]"; exit 0 ;;
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

TARGET_PROTO="$(target_enum "$TARGET")" || exit 1

URL="$STRATUM_API/api/knowledge-bases/$(jq -rn --arg v "$KB_ID" '$v|@uri')/await"
BODY=$(jq -nc --argjson v "$VERSION" --arg t "$TARGET_PROTO" \
  '{version_id: $v, target: $t, wait_timeout_ms: 5000}')

# reached 判断和目标一致：目标 index-ready 时，数据已落地但索引就绪都算到达
# （服务端也是这个口径：DATA_DURABLE 已经越过 data-durable 目标）。
reached() {
  if [[ "$TARGET_PROTO" == "AWAIT_TARGET_DATA_DURABLE" ]]; then
    [[ "$1" == "DATA_DURABLE" || "$1" == "INDEX_READY" ]]
  else
    [[ "$1" == "INDEX_READY" ]]
  fi
}

MISSING_SHOWN=0
deadline=$((SECONDS + TIMEOUT))
while :; do
  resp=$(curl -sS -w $'\n%{http_code}' -X POST "$URL" \
    -H 'Content-Type: application/json' --data "$BODY") || {
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

  [[ "$JSON_OUT" -eq 1 ]] && echo "$resp" | jq .
  stage=$(echo "$resp" | jq -r '.stage')
  missing=$(echo "$resp" | jq -r '.data_missing // false')
  idx=$(echo "$resp" | jq -r '.version.index_status // "" ')
  dat=$(echo "$resp" | jq -r '.version.data_status // "" ')
  retry=$(echo "$resp" | jq -r '.retry_after_ms // empty')

  if reached "$stage"; then
    echo "版本 $VERSION 已达到 $(echo "$TARGET_PROTO" | sed 's/AWAIT_TARGET_//')（stage=$stage，索引=$(proto_enum_short "$idx")，数据=$(proto_enum_short "$dat")）"
    exit 0
  fi

  case "$stage" in
    INDEX_FAILED)
      echo "版本 $VERSION 的索引构建失败（stage=$stage）。处置：scripts/ops/kb-rebuild.sh $KB_ID $VERSION" >&2
      exit 1 ;;
    INDEX_FAILED_PERMANENT|DATA_FAILED_PERMANENT)
      echo "版本 $VERSION 已被控制层判为永久失败（stage=$stage）：没有东西会自动重试。" >&2
      echo "  处置：数据侧 → 用同一幂等键重发；索引侧 → kb-rebuild.sh；放弃 → kb-discard-version.sh $KB_ID $VERSION" >&2
      exit 1 ;;
    DELETING)
      echo "版本 $VERSION 正在删除中（stage=$stage）。" >&2
      exit 1 ;;
  esac

  # data_missing 只说一次：轮询期间它会一直为真，每轮重复三行会淹没真正要看的东西。
  if [[ "$missing" == "true" && "$MISSING_SHOWN" -eq 0 ]]; then
    MISSING_SHOWN=1
    echo "⚠ data_missing：没有任何可达副本持有版本 $VERSION 的数据，它不会自己好。" >&2
    echo "  重发同一批变更（带上原幂等键）→ kb-version-create.sh $KB_ID --client-request-id <KEY>" >&2
    echo "  或放弃这个版本 → kb-discard-version.sh $KB_ID $VERSION" >&2
  fi

  if ((SECONDS >= deadline)); then
    echo "等待超时（${TIMEOUT}s）：版本 $VERSION 仍在 $stage（未失败，只是还没到）" >&2
    echo "  提示：加长 --timeout，或用 status.sh / kb-versions.sh 看全局状态" >&2
    exit 2
  fi

  sleep_ms="${INTERVAL:-${retry:-1000}}"
  sleep "$(awk -v ms="$sleep_ms" 'BEGIN { printf "%.3f", ms/1000 }')"
done
