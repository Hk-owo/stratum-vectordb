#!/usr/bin/env bash
# latency-baseline.sh — 重复测量建基线：同一个用例跑 N 轮，报告分布与可分辨度。
#
# 动机（2026-09-22 实测）：单次运行的查询延迟 p50 波动 CV ≈ 10–12%，尾部分位
# （p95/p99）CV ≈ 22–24%。所以任何小于这个幅度的改动，用单次 A/B 都测不出来 ——
# "优化了却看不出变化"通常不是优化没用，而是样本不够。
#
# 判断"多少轮才分得开"：n ≈ (2·CV/target)² （约 95% 置信）。CV=11% 时可按
# 30%/16%/9%/5%/3% 分别对应 ~1/2/6/20/64 轮。脚本跑完会自动打印这张表。
#
# 选哪个指标也有讲究：端到端 p50（本脚本报的那个）含服务站与 wire，CV ~12%；
# 节点内 total_us 的 CV ~10%，两者接近。真正低噪声的是**分段计时**
# （`query: stage timings` 的单个 *_us）：它在同一次运行内比较，没有跨轮噪声，
# 所以改动了哪一段就直接看那一段，比重复跑端到端便宜得多。
#
# 用法：
#   scripts/latency-baseline.sh                  # 2000 篇 × 8 轮（默认）
#   scripts/latency-baseline.sh -n 20            # 20 轮（可分辨 ~5%）
#   scripts/latency-baseline.sh -d 8000 -n 6     # 8000 篇 × 6 轮
#   scripts/latency-baseline.sh --station 127.0.0.1:7020
#
# 前置：3+3 集群已起，且有一个服务站。分段计时还要集群与服务站都是 debug 级
# （见 README「查询成本的构成」一节里的命令）。
#
# 注意（跨轮变量，脚本会打印出来）：每轮都新建知识库，所以数据状态、页缓存、
# 以及数据卷累积导致的后台 compaction 都在变——load average 会随轮次爬升。要让
# 轮间可比，测之前让集群静置，并留意打印出的 load 是否单调爬升。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DOCS=2000
ROUNDS=8
QUERIES=224
STATION="${STRATUM_T4_STATION_ADDR:-127.0.0.1:7020}"
TIMEOUT="60m"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--docs)     DOCS="${2:?--docs 需要篇数}"; shift 2 ;;
    -n|--rounds)   ROUNDS="${2:?--rounds 需要轮数}"; shift 2 ;;
    -q|--queries)  QUERIES="${2:?--queries 需要次数}"; shift 2 ;;
    --station)     STATION="${2:?--station 需要 host:port}"; shift 2 ;;
    --timeout)     TIMEOUT="${2:?--timeout 需要时长}"; shift 2 ;;
    -h|--help)     awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; exit 0 ;;
    *) echo "未知参数: $1（试试 --help）" >&2; exit 2 ;;
  esac
done

if ! (exec 3<>/dev/tcp/"${STATION%%:*}"/"${STATION##*:}") 2>/dev/null; then
  echo "服务站 $STATION 连不上。先起一个（见 README「分段计时」），或用 --station 指定。" >&2
  exit 1
fi
exec 3>&- 2>/dev/null || true

echo "== 基线测量：docs=$DOCS queries=$QUERIES rounds=$ROUNDS station=$STATION =="
echo "start load: $(cut -d' ' -f1 /proc/loadavg)"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

for i in $(seq 1 "$ROUNDS"); do
  start=$(date +%s)
  out="$(cd "$ROOT" && STRATUM_T4_STATION_ADDR="$STATION" \
      STRATUM_STRESS_DOCS="$DOCS" STRATUM_STRESS_QUERIES="$QUERIES" \
      go test ./integration/docker/ -tags=docker -count=1 \
      -run TestT4_QueryLatency -v -timeout "$TIMEOUT" 2>&1 \
      | grep -E 'QUERY-LATENCY SUMMARY' || true)"
  elapsed=$(( $(date +%s) - start ))

  if [[ -z "$out" ]]; then
    echo "round $i: 没有拿到 SUMMARY（用例失败或超时？），跳过"
    continue
  fi
  # SUMMARY: docs=2000 queries=224 cold=…ms p50=…ms p95=…ms p99=…ms mean=…ms zero_p50=…ms
  p50="$(sed -n 's/.* p50=\([0-9.]*\)ms.*/\1/p' <<<"$out")"
  p95="$(sed -n 's/.* p95=\([0-9.]*\)ms.*/\1/p' <<<"$out")"
  p99="$(sed -n 's/.* p99=\([0-9.]*\)ms.*/\1/p' <<<"$out")"
  mean="$(sed -n 's/.* mean=\([0-9.]*\)ms.*/\1/p' <<<"$out")"
  printf '%s %s %s %s %s\n' "$p50" "$p95" "$p99" "$mean" >>"$tmp"
  printf 'round %-3s %3ss  load %-5s  p50=%-8s p95=%-8s p99=%-8s mean=%s\n' \
    "$i" "$elapsed" "$(cut -d' ' -f1 /proc/loadavg)" "$p50" "$p95" "$p99" "$mean"
done

python3 - "$tmp" "$DOCS" "$QUERIES" <<'PY'
import statistics as st, sys

path, docs, queries = sys.argv[1], sys.argv[2], sys.argv[3]
rows = [l.split() for l in open(path) if l.strip()]
if not rows:
    sys.exit("没有可用样本")
p50 = [float(r[0]) for r in rows]
p95 = [float(r[1]) for r in rows]
p99 = [float(r[2]) for r in rows]

print(f"\n== 汇总（docs={docs} queries={queries} rounds={len(rows)}，单位 ms）==")
for name, v in (("p50", p50), ("p95", p95), ("p99", p99)):
    mean, sd = st.mean(v), (st.stdev(v) if len(v) > 1 else 0.0)
    print(f"  {name}: min={min(v):.2f} median={st.median(v):.2f} mean={mean:.2f} "
          f"max={max(v):.2f} max/min={max(v)/min(v):.2f}x CV={100*sd/mean:.1f}%")

mean, sd = st.mean(p50), (st.stdev(p50) if len(p50) > 1 else 0.0)
cv = sd / mean
if cv:
    print(f"\n  p50 的本底噪声 CV={100*cv:.1f}%；要分辨某幅度差异所需的最少轮数（~95% 置信）：")
    for target in (0.30, 0.16, 0.09, 0.05, 0.03):
        n = (2 * cv / target) ** 2
        print(f"    {int(target*100):3d}%: n≈{n:5.1f} 轮")
    print(f"\n  本次 {len(rows)} 轮的中位数标准误：±{100*sd/len(rows)**0.5/mean:.1f}%")
PY
