#!/usr/bin/env bash
# query.sh — 向量检索查询。
#
# 向量来源二选一：
#   --vector "0.1,0.2,…"    直接传向量（维度须与库内向量一致）
#   --text "文本"            走 gateway 的 /api/query-text：**服务端**用该知识库
#                            自己的 embed 配置（service_addr + model_id）把文本
#                            转成向量再检索。不需要在本地复刻 embed 算法，也不会
#                            出现"本地算法和知识库实际使用的模型不一致"这种
#                            静默查错空间的事。
#
# 注意 --text 的前提：gateway 要能访问该知识库的 embed 服务地址。容器集群里
# embed 常是容器名（http://stratum-embed:8080），宿主进程解析不了 —— 那种情况
# 用 scripts/gateway.sh --in-docker 把控制台放进集群网络，或自己算好向量用 --vector。
#
# 默认查询激活版本；--version-id 可指定版本（版本间可 A/B 对比）。
#
# 用法示例：
#   scripts/ops/query.sh kb-xxx --text "什么是回滚" --top-k 5
#   scripts/ops/query.sh kb-xxx --vector "0.1,0.2,0.3" --top-k 5 --threshold 0.5
#   scripts/ops/query.sh kb-xxx --text "A/B 测试" --version-id 2 --aggregation MAX

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KB_ID=""
VECTOR=""
TEXT=""
TOP_K=10
THRESHOLD=""
VERSION_ID=""
AGGREGATION="MEDIAN"
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vector) VECTOR="$2"; shift 2 ;;
    --text) TEXT="$2"; shift 2 ;;
    --top-k) TOP_K="$2"; shift 2 ;;
    --threshold) THRESHOLD="$2"; shift 2 ;;
    --version-id) VERSION_ID="$2"; shift 2 ;;
    --aggregation) AGGREGATION="$2"; shift 2 ;;
    --json) JSON_OUT=1; shift ;;
    -h|--help) echo "用法: $0 <知识库ID> (--text 文本 | --vector 向量) [--top-k N] [--threshold X] [--version-id N] [--aggregation MEDIAN|MAX|MEAN] [--json] [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) KB_ID="$1"; shift ;;
  esac
done

if [[ -z "$KB_ID" ]]; then
  echo "错误：缺少知识库 ID" >&2
  exit 1
fi
if [[ -n "$TEXT" && -n "$VECTOR" ]]; then
  echo "错误：--text 与 --vector 只能二选一" >&2
  exit 1
fi
if [[ -z "$TEXT" && -z "$VECTOR" ]]; then
  echo "错误：必须提供 --text 或 --vector" >&2
  exit 1
fi

# 聚合方式要发 proto 全名，两种端点都是：/api/query 走 protojson（认不出的
# 枚举名静默丢弃 → MAX 变 MEDIAN），/api/query-text 是显式查表，写错直接 400。
AGG="$(proto_enum aggregation "$AGGREGATION")" || exit 1

# ---------- 组装 body ----------
if [[ -n "$TEXT" ]]; then
  ENDPOINT="/api/query-text"
  # /api/query-text 的 version_id 是 JSON 字符串（protojson 把 int64 编码成字符串，
  # 控制台也是原样回传），所以这里不能用 --argjson。
  body=$(jq -nc \
    --arg id "$KB_ID" \
    --arg text "$TEXT" \
    --argjson topk "$TOP_K" \
    --arg agg "$AGG" \
    '{knowledge_base_id: $id, text: $text, top_k: $topk, aggregation: $agg}')
else
  ENDPOINT="/api/query"
  body=$(jq -nc \
    --arg id "$KB_ID" \
    --arg vec "$VECTOR" \
    --argjson topk "$TOP_K" \
    --arg agg "$AGG" \
    '{
      knowledge_base_id: $id,
      vector: ($vec | split(",") | map(tonumber)),
      top_k: $topk,
      aggregation: $agg
    }')
fi
if [[ -n "$THRESHOLD" ]]; then
  body=$(echo "$body" | jq -c --argjson t "$THRESHOLD" '.threshold = $t')
fi
if [[ -n "$VERSION_ID" ]]; then
  if [[ -n "$TEXT" ]]; then
    body=$(echo "$body" | jq -c --arg v "$VERSION_ID" '.version_id = $v')
  else
    body=$(echo "$body" | jq -c --argjson v "$VERSION_ID" '.version_id = $v')
  fi
fi

resp=$(curl -sS -w $'\n%{http_code}' -X POST "$STRATUM_API$ENDPOINT" \
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
  exit 0
fi

vid=$(echo "$resp" | jq -r '.version_id')
degraded=$(echo "$resp" | jq -r '.storage_degraded // false')
echo "查询知识库 $KB_ID（命中版本 $vid，top-k=${TOP_K}${THRESHOLD:+，threshold=$THRESHOLD}，aggregation=$(proto_enum_short "$AGG")）"
# storage_degraded：存储层副本不足的**报告**，不是错误——结果是完整的，但
# 该知道（docs/storage-degradation-signal-plan.md §10.1）。
if [[ "$degraded" == "true" ]]; then
  echo "  ⚠ storage_degraded：该知识库的存储副本低于法定数，结果仍完整但持久性有风险"
fi
echo
n=$(echo "$resp" | jq -r '[.results[]?] | length')
if [[ "$n" -eq 0 ]]; then
  echo "  无结果（版本可能为空，或没有达到 threshold）"
else
  echo "$resp" | jq -r '
    .results[]? |
    "  #\(.score|tostring|.[0:7])  \(.doc_id)  \(.content|if length > 80 then .[0:80] + "…" else . end)"'
fi
