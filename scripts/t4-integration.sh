#!/usr/bin/env bash
# t4-integration.sh — 跑 T4 的 Docker 集群集成套件（integration/docker, -tags=docker）。
#
# 两种拓扑（都由 scripts/cluster.sh 起，用 --topology 区分）：
#
#   both        两层：控制组 3 + 存储组 3。每个容器自带 vecstore，索引在容器内构建。
#   all-in-one  单层：3 个统一节点 + 宿主 vecstore 进程，也就是 CI 早期用的那一种。
#
# 用法：
#   scripts/t4-integration.sh                          # both 拓扑，整套
#   scripts/t4-integration.sh -r 'TestT4_Await'        # 只跑匹配的用例（正则）
#   scripts/t4-integration.sh -t all-in-one            # 换拓扑
#   scripts/t4-integration.sh --no-up                  # 集群已经起着，别再动它
#   scripts/t4-integration.sh --down                   # 跑完把集群停掉（默认不停）
#   scripts/t4-integration.sh -- -v -count=1           # 额外参数透传给 go test
#
# 注意（2025-09 实测，未修）：单层拓扑下宿主 vecstore 的 Save 会失败
# （节点日志里 "index: Save RPC: rpc error ... Unexpected error in RPC handling"），
# 于是没有任何版本能变成 READY —— 需要 READY 的用例（含 await/SDK 那几条）都会红，
# 而这与用例本身无关。两层拓扑不受影响，CI 也因此只用两层。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TOPOLOGY=both
RUN_FILTER=""
TIMEOUT="900s"
DO_UP=1
DO_DOWN=0
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--topology) TOPOLOGY="${2:?--topology 需要 both 或 all-in-one}"; shift 2 ;;
    -r|--run)      RUN_FILTER="${2:?--run 需要一个正则}"; shift 2 ;;
    --timeout)     TIMEOUT="${2:?--timeout 需要一个时长，如 900s}"; shift 2 ;;
    --no-up)       DO_UP=0; shift ;;
    --down)        DO_DOWN=1; shift ;;
    --)            shift; EXTRA=("$@"); break ;;
    -h|--help)     awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; exit 0 ;;
    *) echo "未知参数: $1（试试 --help）" >&2; exit 2 ;;
  esac
done

case "$TOPOLOGY" in
  both)
    UP_CMD=("$ROOT/scripts/cluster.sh" --topology two-tier up)
    export STRATUM_T4_NODE_SERVICES="stratum-node-control1,stratum-node-control2,stratum-node-control3"
    export STRATUM_T4_STORAGE_SERVICES="stratum-node-storage1,stratum-node-storage2,stratum-node-storage3"
    DOWN_CMD=("$ROOT/scripts/cluster.sh" --topology two-tier down)
    ;;
  all-in-one)
    UP_CMD=("$ROOT/scripts/cluster.sh" up 3 --with-embed)
    export STRATUM_T4_NODE_SERVICES="stratum-node1,stratum-node2,stratum-node3"
    DOWN_CMD=("$ROOT/scripts/cluster.sh" down)
    ;;
  *)
    echo "未知拓扑: $TOPOLOGY（可选 both / all-in-one）" >&2
    exit 2
    ;;
esac

if [[ $DO_UP -eq 1 ]]; then
  echo "[t4] 启动 $TOPOLOGY 集群…"
  "${UP_CMD[@]}"
fi

# 直连节点的用例要自己签服务站的信任标记（HMAC，H4 of
# docs/code-review-2026-09-24.md），密钥由 cluster.sh 生成在所属 run 目录下。测试
# 自己也会去找这几个路径，这里导出是双保险。
case "$TOPOLOGY" in
  both)       SECRET_FILE="$ROOT/run/docker-both/station-secret" ;;
  all-in-one) SECRET_FILE="$ROOT/run/docker/station-secret" ;;
esac
if [[ -f "$SECRET_FILE" ]]; then
  STRATUM_STATION_SECRET="$(cat "$SECRET_FILE")"
  export STRATUM_STATION_SECRET
else
  echo "[t4] 警告：没有 $SECRET_FILE —— 直连节点的用例会以 unauthenticated 失败" >&2
fi

args=(go test ./integration/docker/... -tags=docker -count=1 -timeout "$TIMEOUT" -v)
[[ -n "$RUN_FILTER" ]] && args+=(-run "$RUN_FILTER")
[[ ${#EXTRA[@]} -gt 0 ]] && args+=("${EXTRA[@]}")

echo "[t4] 拓扑=$TOPOLOGY 运行: ${args[*]}"
status=0
(cd "$ROOT" && "${args[@]}") || status=$?

if [[ $DO_DOWN -eq 1 ]]; then
  echo "[t4] 停止集群…"
  "${DOWN_CMD[@]}" >/dev/null 2>&1 || true
fi

if [[ $status -eq 0 ]]; then
  echo "[t4] 通过"
else
  echo "[t4] 失败（退出码 $status）" >&2
fi
exit "$status"
