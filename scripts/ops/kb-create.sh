#!/usr/bin/env bash
# kb-create.sh — 创建知识库。
#
# 创建后自动产生初始版本（空）。之后用 kb-version-create.sh 写入文档。
#
# 用法：
#   scripts/ops/kb-create.sh --name 产品手册
#   scripts/ops/kb-create.sh --name 产品手册 \
#     --chunk-window 512 --chunk-overlap 64 \
#     --index-type HNSW --similarity COSINE \
#     --embed-addr http://localhost:8080 --model-id mock-embed-v1
#
# 可选参数都有默认值（与 configs/config1.yaml 的 knowledge_base_defaults 一致）。

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NAME=""
WINDOW=512
OVERLAP=64
INDEX_TYPE="HNSW"
SIMILARITY="COSINE"
EMBED_ADDR="http://localhost:8080"
MODEL_ID="mock-embed-v1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="${2:?--name 需要一个参数}"; shift 2 ;;
    --chunk-window) WINDOW="$2"; shift 2 ;;
    --chunk-overlap) OVERLAP="$2"; shift 2 ;;
    --index-type) INDEX_TYPE="$2"; shift 2 ;;
    --similarity) SIMILARITY="$2"; shift 2 ;;
    --embed-addr) EMBED_ADDR="$2"; shift 2 ;;
    --model-id) MODEL_ID="$2"; shift 2 ;;
    -h|--help) echo "用法: $0 --name 名称 [选项] [--api URL]"; exit 0 ;;
    -a|--api) STRATUM_API="http://${2#http://}"; STRATUM_API="${STRATUM_API%/}"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$NAME" ]]; then
  echo "错误：--name 是必填参数" >&2
  exit 1
fi

body=$(jq -n \
  --arg name "$NAME" \
  --argjson window "$WINDOW" \
  --argjson overlap "$OVERLAP" \
  --arg index "$INDEX_TYPE" \
  --arg sim "$SIMILARITY" \
  --arg embed "$EMBED_ADDR" \
  --arg model "$MODEL_ID" \
  '{name: $name, chunk_window_size: $window, chunk_overlap_size: $overlap,
    index_type: $index, similarity: $sim,
    embed_config: {service_addr: $embed, model_id: $model}}')

api_post "/api/knowledge-bases" "$body" \
  | jq -r '"创建成功：知识库 \(.knowledge_base_id)，初始版本 \(.initial_version_id)"'
