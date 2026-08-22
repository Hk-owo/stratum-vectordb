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
# 支持 -a/--api 参数与 STRATUM_HTTP_ADDR 环境变量（start.sh 使用的同名变量）。
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
