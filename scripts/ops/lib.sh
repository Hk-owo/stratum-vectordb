#!/usr/bin/env bash
# lib.sh — Stratum 运维脚本公共库。
#
# 提供：
#   - API 地址解析（--api / -a 参数 > STRATUM_HTTP_ADDR 环境变量 > 默认值）
#   - api_get / api_post 请求封装：自动带 Content-Type、非 2xx 时打印后端
#     错误并退出；响应统一用 jq 美化输出
#   - 依赖检查（curl / jq）
#
# 用法（在每个运维脚本开头 source 本文件）：
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#   api_get "/api/health"
#   api_post "/api/knowledge-bases/delete" '{"knowledge_base_id":"kb-1"}'

set -euo pipefail

# ---------- 依赖检查 ----------
for _cmd in curl jq; do
  if ! command -v "$_cmd" >/dev/null 2>&1; then
    echo "错误：缺少依赖命令 '$_cmd'，请先安装（apt install curl jq）" >&2
    exit 1
  fi
done
unset _cmd

# ---------- API 地址解析 ----------
# 支持 -a/--api 参数与 STRATUM_HTTP_ADDR 环境变量（scripts/gateway.sh 使用的同名变量）。
# 地址写法宽松：可带或不带 http:// 前缀、可带或可不带尾部斜杠。
STRATUM_API="${STRATUM_HTTP_ADDR:-127.0.0.1:8081}"
STRATUM_API="http://${STRATUM_API#http://}"
STRATUM_API="${STRATUM_API%/}"

# ---------- 请求封装 ----------
# 打印响应 JSON（jq 美化）；请求失败或后端返回错误时打印原因并以非零退出。
api_get() {
  local path="$1"
  local resp code
  resp=$(curl -sS -w $'\n%{http_code}' "$STRATUM_API$path") || {
    echo "错误：无法连接 $STRATUM_API$path（gateway 是否已启动？）" >&2
    exit 1
  }
  code="${resp##*$'\n'}"
  resp="${resp%$'\n'*}"
  if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
    [[ -n "$resp" ]] && echo "$resp" | jq .
  else
    echo "$resp" | jq . >&2 || echo "$resp" >&2
    echo "错误：HTTP $code" >&2
    exit 1
  fi
}

api_post() {
  local path="$1" body="$2"
  local resp code
  resp=$(curl -sS -w $'\n%{http_code}' -X POST "$STRATUM_API$path" \
    -H 'Content-Type: application/json' --data "$body") || {
    echo "错误：无法连接 $STRATUM_API$path（gateway 是否已启动？）" >&2
    exit 1
  }
  code="${resp##*$'\n'}"
  resp="${resp%$'\n'*}"
  if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
    [[ -n "$resp" ]] && echo "$resp" | jq .
  else
    echo "$resp" | jq . >&2 || echo "$resp" >&2
    echo "错误：HTTP $code" >&2
    exit 1
  fi
}

# api_post_raw 与 api_post 同一条失败路径，但**不**打印响应：成功时把响应体
# 回显给调用方，由脚本自己决定是输出摘要还是原始 JSON（--json）。
api_post_raw() {
  local path="$1" body="$2"
  local resp code
  resp=$(curl -sS -w $'\n%{http_code}' -X POST "$STRATUM_API$path" \
    -H 'Content-Type: application/json' --data "$body") || {
    echo "错误：无法连接 $STRATUM_API$path（gateway 是否已启动？）" >&2
    exit 1
  }
  code="${resp##*$'\n'}"
  resp="${resp%$'\n'*}"
  if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
    echo "$resp"
  else
    echo "$resp" | jq . >&2 || echo "$resp" >&2
    echo "错误：HTTP $code" >&2
    exit 1
  fi
}

# ---------- 枚举名 ----------# gateway 用 protojson（且 DiscardUnknown）解析请求体：枚举字段只认 proto 全名
# （如 CHANGE_OP_UPDATE）或数字。写简名（UPDATE）不会报错，而是**被静默丢弃、
# 退回零值**——UPDATE 变 ADD、MAX 变 MEDIAN 就是这么发生的，而且结果看起来
# 完全正常。所以凡是要发枚举的脚本都从这里取全名：写错的值直接报错退出，
# 而不是发出去变成另一个语义。
proto_enum() {
  local kind="$1" value="$2" upper
  upper="$(printf '%s' "$value" | tr '[:lower:]-' '[:upper:]_')"
  case "$kind:$upper" in
    index_type:HNSW|index_type:INDEX_TYPE_HNSW)               echo "INDEX_TYPE_HNSW" ;;
    index_type:IVF|index_type:INDEX_TYPE_IVF)                 echo "INDEX_TYPE_IVF" ;;
    index_type:FLAT|index_type:INDEX_TYPE_FLAT)               echo "INDEX_TYPE_FLAT" ;;
    similarity:COSINE|similarity:SIMILARITY_COSINE)           echo "SIMILARITY_COSINE" ;;
    similarity:EUCLIDEAN|similarity:SIMILARITY_EUCLIDEAN)     echo "SIMILARITY_EUCLIDEAN" ;;
    similarity:INNER_PRODUCT|similarity:SIMILARITY_INNER_PRODUCT) echo "SIMILARITY_INNER_PRODUCT" ;;
    quantizer:OFF|quantizer:QUANTIZER_OFF)                    echo "QUANTIZER_OFF" ;;
    quantizer:SQ8|quantizer:QUANTIZER_SQ8)                    echo "QUANTIZER_SQ8" ;;
    quantizer:SQ_BF16|quantizer:QUANTIZER_SQ_BF16)            echo "QUANTIZER_SQ_BF16" ;;
    quantizer:SQ_FP16|quantizer:QUANTIZER_SQ_FP16)            echo "QUANTIZER_SQ_FP16" ;;
    quantizer:PQ|quantizer:QUANTIZER_PQ)                      echo "QUANTIZER_PQ" ;;
    aggregation:MEDIAN|aggregation:MEAN|aggregation:MAX)      echo "AGGREGATION_METHOD_$upper" ;;
    aggregation:AGGREGATION_METHOD_MEDIAN|aggregation:AGGREGATION_METHOD_MEAN|aggregation:AGGREGATION_METHOD_MAX) echo "$upper" ;;
    change_op:ADD|change_op:UPDATE|change_op:DELETE)          echo "CHANGE_OP_$upper" ;;
    change_op:CHANGE_OP_ADD|change_op:CHANGE_OP_UPDATE|change_op:CHANGE_OP_DELETE) echo "$upper" ;;
    delete_mode:SUBTREE|delete_mode:SINGLE|delete_mode:ANCESTORS) echo "VERSION_DELETE_MODE_$upper" ;;
    delete_mode:VERSION_DELETE_MODE_SUBTREE|delete_mode:VERSION_DELETE_MODE_SINGLE|delete_mode:VERSION_DELETE_MODE_ANCESTORS) echo "$upper" ;;
    *)
      echo "错误：$kind 不支持取值 '$value'（可选：$(proto_enum_values "$kind")）" >&2
      return 1
      ;;
  esac
}

# proto_enum_values 只用于把可选值列进报错信息。
proto_enum_values() {
  case "$1" in
    index_type)  echo "HNSW / IVF / FLAT" ;;
    similarity)  echo "COSINE / EUCLIDEAN / INNER_PRODUCT" ;;
    quantizer)   echo "OFF / SQ8 / SQ_BF16 / SQ_FP16 / PQ" ;;
    aggregation) echo "MEDIAN / MEAN / MAX" ;;
    change_op)   echo "ADD / UPDATE / DELETE" ;;
    delete_mode) echo "subtree / single / ancestors" ;;
    *)           echo "?" ;;
  esac
}

# proto_enum_short 反过来用：把 proto 全名削成短名，脚本输出时好看一些。
# gateway 以 proto 名返回状态（如 INDEX_STATUS_READY）。
proto_enum_short() {
  case "$1" in
    INDEX_STATUS_*)       echo "${1#INDEX_STATUS_}" ;;
    DATA_STATUS_*)        echo "${1#DATA_STATUS_}" ;;
    KB_STATUS_*)          echo "${1#KB_STATUS_}" ;;
    AGGREGATION_METHOD_*) echo "${1#AGGREGATION_METHOD_}" ;;
    FAILURE_SIDE_*)       echo "${1#FAILURE_SIDE_}" ;;
    HEALTH_STATUS_*)      echo "${1#HEALTH_STATUS_}" ;;
    *)                    echo "$1" ;;
  esac
}
