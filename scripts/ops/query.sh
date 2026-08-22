#!/usr/bin/env bash
# query.sh — 向量检索查询。
#
# 向量来源二选一：
#   --vector "0.1,0.2,…"    直接传向量（维度须与库内向量一致）
#   --text "文本"            用 mock embed 的确定性算法从文本生成向量
#                            （仅当知识库使用 mock embed 服务时有效；
#                             生产 embed 服务请自行调用后传 --vector）
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
MODEL_ID="mock-embed-v1"
DIM=768
TOP_K=10
THRESHOLD=""
VERSION_ID=""
AGGREGATION="MEDIAN"
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vector) VECTOR="$2"; shift 2 ;;
    --text) TEXT="$2"; shift 2 ;;
    --model-id) MODEL_ID="$2"; shift 2 ;;
    --dim) DIM="$2"; shift 2 ;;
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

# ---------- 生成/解析向量 ----------
if [[ -n "$TEXT" ]]; then
  VECTOR=$(python3 - "$TEXT" "$MODEL_ID" "$DIM" <<'PYEOF'
import hashlib, math, struct, sys

text, model_id, dim = sys.argv[1], sys.argv[2], int(sys.argv[3])
chunk_id = hashlib.sha256((text + model_id).encode("utf-8")).hexdigest()
h = hashlib.sha256(chunk_id.encode("ascii")).digest()
vec = []
for i in range(dim):
    b = h[(i + i // 32) % 32] / 255.0
    vec.append(struct.unpack("f", struct.pack("f", b))[0])
norm = math.sqrt(sum(x * x for x in vec)) or 1.0
vec = [struct.unpack("f", struct.pack("f", x / norm))[0] for x in vec]
print(",".join(repr(x) for x in vec))
PYEOF
) || { echo "错误：文本生成向量失败" >&2; exit 1; }
  echo "（已用 mock embed 算法从文本生成 $DIM 维向量）"
fi

# ---------- 组装 body ----------
body=$(jq -nc \
  --arg id "$KB_ID" \
  --arg vec "$VECTOR" \
  --argjson topk "$TOP_K" \
  --arg agg "$AGGREGATION" \
  '{
    knowledge_base_id: $id,
    vector: ($vec | split(",") | map(tonumber)),
    top_k: $topk,
    aggregation: $agg
  }')
if [[ -n "$THRESHOLD" ]]; then
  body=$(echo "$body" | jq -c --argjson t "$THRESHOLD" '.threshold = $t')
fi
if [[ -n "$VERSION_ID" ]]; then
  body=$(echo "$body" | jq -c --argjson v "$VERSION_ID" '.version_id = $v')
fi

resp=$(curl -sS -w $'\n%{http_code}' -X POST "$STRATUM_API/api/query" \
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
echo "查询知识库 $KB_ID（命中版本 $vid，top-k=${TOP_K}${THRESHOLD:+，threshold=$THRESHOLD}）"
echo
n=$(echo "$resp" | jq -r '[.results[]?] | length')
if [[ "$n" -eq 0 ]]; then
  echo "  无结果（版本可能为空，或没有达到 threshold）"
else
  echo "$resp" | jq -r '
    .results[]? |
    "  #\(.score|tostring|.[0:7])  \(.doc_id)  \(.content|if length > 80 then .[0:80] + "…" else . end)"'
fi
