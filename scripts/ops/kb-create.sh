#!/usr/bin/env bash
# kb-create.sh — 创建知识库。
#
# 注意：创建知识库**不再产生初始版本**（docs/cursor-persistence-plan.md §5，
# CreateKnowledgeBaseResponse.initial_version_id 恒为 0）。第一个版本由第一次
# 写入产生，见 kb-version-create.sh；在那之前知识库是空的，也仍然可查询。
#
# 用法：
#   scripts/ops/kb-create.sh --name 产品手册
#   scripts/ops/kb-create.sh --name 产品手册 \
#     --chunk-window 512 --chunk-overlap 64 \
#     --index-type HNSW --similarity COSINE \
#     --embed-addr http://localhost:8080 --model-id mock-embed-v1
#
# 可选参数都有默认值：分块窗口 512 / 重叠 64，索引 HNSW，相似度 COSINE，
# 量化器 OFF（都不给就是这三样）。切块算法的常量在 internal/types，不在这里。
#
# 索引类型 / 相似度 / 量化器只认下表的值（也可以直接写 proto 全名，
# 如 INDEX_TYPE_IVF）：
#   --index-type   HNSW | IVF | FLAT
#   --similarity   COSINE | EUCLIDEAN | INNER_PRODUCT
#   --quantizer    OFF | SQ8 | SQ_BF16 | SQ_FP16 | PQ     （创建后不可改）
#   --pq-m / --pq-nbits  仅 PQ 用；不填用服务端默认（96 / 8）

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NAME=""
WINDOW=512
OVERLAP=64
INDEX_TYPE="HNSW"
SIMILARITY="COSINE"
QUANTIZER="OFF"
PQ_M=""
PQ_NBITS=""
EMBED_ADDR="http://localhost:8080"
MODEL_ID="mock-embed-v1"
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="${2:?--name 需要一个参数}"; shift 2 ;;
    --chunk-window) WINDOW="$2"; shift 2 ;;
    --chunk-overlap) OVERLAP="$2"; shift 2 ;;
    --index-type) INDEX_TYPE="$2"; shift 2 ;;
    --similarity) SIMILARITY="$2"; shift 2 ;;
    --quantizer) QUANTIZER="$2"; shift 2 ;;
    --pq-m) PQ_M="$2"; shift 2 ;;
    --pq-nbits) PQ_NBITS="$2"; shift 2 ;;
    --embed-addr) EMBED_ADDR="$2"; shift 2 ;;
    --model-id) MODEL_ID="$2"; shift 2 ;;
    --json) JSON_OUT=1; shift ;;
    -h|--help) echo "用法: $0 --name 名称 [--chunk-window N] [--chunk-overlap N] [--index-type HNSW|IVF|FLAT] [--similarity COSINE|EUCLIDEAN|INNER_PRODUCT] [--quantizer OFF|SQ8|SQ_BF16|SQ_FP16|PQ] [--pq-m N] [--pq-nbits N] [--embed-addr URL] [--model-id ID] [--json] [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$NAME" ]]; then
  echo "错误：--name 是必填参数" >&2
  exit 1
fi

# 枚举一律归一化成 proto 全名：gateway 的 protojson 对认不出的枚举名是静默
# 丢弃（退回默认值），也就是说 --index-type IVF 会悄悄建成 HNSW。
body=$(jq -n \
  --arg name "$NAME" \
  --argjson window "$WINDOW" \
  --argjson overlap "$OVERLAP" \
  --arg index "$(proto_enum index_type "$INDEX_TYPE")" \
  --arg sim "$(proto_enum similarity "$SIMILARITY")" \
  --arg quant "$(proto_enum quantizer "$QUANTIZER")" \
  --arg embed "$EMBED_ADDR" \
  --arg model "$MODEL_ID" \
  '{name: $name, chunk_window_size: $window, chunk_overlap_size: $overlap,
    index_type: $index, similarity: $sim, quantizer: $quant,
    embed_config: {service_addr: $embed, model_id: $model}}')
if [[ -n "$PQ_M" ]]; then
  body=$(echo "$body" | jq -c --argjson m "$PQ_M" '.pq_m = $m')
fi
if [[ -n "$PQ_NBITS" ]]; then
  body=$(echo "$body" | jq -c --argjson n "$PQ_NBITS" '.pq_nbits = $n')
fi

resp=$(api_post_raw "/api/knowledge-bases" "$body")
if [[ "$JSON_OUT" -eq 1 ]]; then
  echo "$resp" | jq .
  exit 0
fi
kb_id=$(echo "$resp" | jq -r '.knowledge_base_id')
echo "创建成功：知识库 $kb_id（尚无版本；用 kb-version-create.sh 写入第一批文档）"
