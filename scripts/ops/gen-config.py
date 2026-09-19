#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stratum 配置文件生成/调整工具（运行前修改参数）。

运维不需要看代码就能生成或调整节点的 YAML 配置。支持两种用法：

  1) 从零生成单节点/多节点配置：
     python3 scripts/ops/gen-config.py --out /tmp/node1.yaml \
         --node-id 1 --grpc-addr 0.0.0.0:7000 --raft-addr 0.0.0.0:8000 \
         --data-dir /var/lib/stratum/node1 \
         --peers "1=localhost:8000=localhost:7000,2=node2:8000=node2:7000,3=node3:8000=node3:7000"

  2) 基于现有配置文件调整参数（--set 通用覆盖，支持任意层级）：
     python3 scripts/ops/gen-config.py --base configs/config1.yaml \
         --set index_manager.lru_capacity=32 \
         --set write_coordinator.max_retries=5 \
         --set storage.data_dir=/data/stratum/node1 \
         --out configs/my-node1.yaml

  3) 生成两层拓扑（控制组 + 存储组，Stratum_设计文档v13.md §11 阶段 ④）：
     # 控制节点：跑 Raft 与元数据，零数据层
     python3 scripts/ops/gen-config.py --role control --node-id 1 \
         --peers "1=c1:8000=c1:7000,2=c2:8000=c2:7000,3=c3:8000=c3:7000" \
         --storage-nodes "11=s1:7000,12=s2:7000,13=s3:7000" \
         --require-auth true --out run/configs/control1.yaml
     # 存储节点：不参与选举，元数据从控制组读；node_id 必须出现在 --storage-nodes 里
     python3 scripts/ops/gen-config.py --role storage --node-id 11 \
         --peers "1=c1:8000=c1:7000,2=c2:8000=c2:7000,3=c3:8000=c3:7000" \
         --storage-nodes "11=s1:7000,12=s2:7000,13=s3:7000" \
         --require-auth true --out run/configs/storage1.yaml

常用参数（未指定时取默认值，与 cmd/stratum 内置默认一致）：
  --node-id 节点 ID（多节点时 1/2/3…；两层拓扑里存储层常用 11/12/13）
  --role all|control|storage  节点角色（默认 all：控制层 + 数据层同进程）
  --require-auth true|false   是否要求调用方来自服务站（§9.3(5)，默认 false）
  --grpc-addr / --raft-addr 对外 gRPC / Raft 内部通信地址
  --data-dir 数据目录（docstore/wal/raft/索引都放这里）
  --vecstore-addr vecstore_server 的 gRPC 地址
  --embed-addr embed 服务地址
  --peers 集群成员表，格式 "id=raft地址=service地址,…"
          （service 地址是数据同步用的 gRPC 地址，多节点必填；
           单节点可省略 service 段。存储节点这一项填的是**控制组**的地址表）
  --storage-nodes 存储组成员表 "id=地址,…"，两层拓扑必填（存储节点从它解析自己的地址）
  --log-level debug|info|warn|error
  --set a.b.c=value 任意参数覆盖，值自动识别为数字/布尔/字符串
  --out 输出路径（默认打印到 stdout）

只依赖 Python 标准库 + PyYAML（yaml 包；多数发行版 python3-yaml 已装）。

生成结果与 configs/config1.yaml / integration/docker/config*.yaml 同构，
可直接用 -config 参数启动：./stratum -config <生成的配置>
"""
from __future__ import annotations

import argparse
import sys
from copy import deepcopy

try:
    import yaml
except ImportError:
    print("错误：缺少 PyYAML，请先安装（如 apt install python3-yaml 或 pip install pyyaml）",
          file=sys.stderr)
    sys.exit(1)

# 与 cmd/stratum 内置默认值 / configs/config1.yaml 对齐的模板。
# 注意：fileConfig 只解析它自己结构里带 yaml tag 的键（logging.* / node.* /
# raft.* / control_plane.* / storage.* / vecstore.* / embed.* / index_manager.* /
# write_coordinator.* / delete_coordinator.* / bloom_filter.* / gc.* /
# lag_catchup.*）。这里只列真正会被解析的键：写进来而不被解析的键会静默失效，
# 比不写更糟。
#
# 凡是 0 / false 都表示"用代码里的默认值"（各字段的默认值见
# cmd/stratum/main.go 的 defaultConfig 与 configs/config1.yaml 的注释），
# 所以这份模板给出的行为与"什么都不配"一致，只有显式改过的键才改变行为。
ROLES = ("all", "control", "storage")

DEFAULT_TEMPLATE = {
    "logging": {
        "level": "info",
    },
    "node": {
        "node_id": 1,
        # all = 控制层 + 数据层同进程（默认，历史行为）；
        # control = 只跑 Raft 与元数据，零数据层；
        # storage = 不参与选举、不持有 Raft 日志，元数据经 gRPC 从控制组读。
        "role": "all",
        # §9.3(5)：true 时只服务带服务站信任标记的调用（集群对外形态）。
        "require_authenticated": False,
        "grpc_addr": "0.0.0.0:7000",
        "raft_addr": "0.0.0.0:8000",
        "metrics_addr": "0.0.0.0:9000",
    },
    "raft": {
        # 200ms 心跳 / [2s,4s) 选举超时：Docker 网络下的推荐值，规避 split-vote。
        "heartbeat_interval_ms": 200,
        "election_timeout_min_ms": 2000,
        "election_timeout_max_ms": 4000,
        # 0 = kvraft 默认 1000；调小可更早触发快照。
        "max_log_length": 0,
        "peers": [
            {"id": 1, "addr": "localhost:8000", "service_addr": "localhost:7000"},
        ],
    },
    "control_plane": {
        # 判 FAILED_PERMANENT 之前容忍的失败次数（§10.1）。0 = 默认（5）。
        "failure_budget": 0,
    },
    "storage": {
        "data_dir": "/var/lib/stratum/node1",
        # 存储组（两层拓扑）。留空 = 每个 Raft 成员都存数据（all 角色）。
        # 控制节点用它决定把写入派发给谁；存储节点从它解析自己的地址，
        # 所以存储节点的 node_id 必须出现在这张表里。
    },
    "vecstore": {
        "grpc_addr": "127.0.0.1:7100",
    },
    "embed": {
        "service_addr": "http://localhost:8080",
    },
    "index_manager": {
        "lru_capacity": 16,
        # 内存字节阈值；0 = 不换出（记账口径见 configs/config1.yaml 的注释）。
        "memory_threshold_mb": 4096,
        # 量化粗筛候选数（§2.2）。0 = 由 vecstore 用 clamp(top_k × 8, 16, 4096)。
        "candidate_n": 0,
        "load_wait_timeout_ms": 5000,
        "callback_max_retries": 3,
        "callback_retry_base_interval_ms": 200,
        # §8.6a 冷版本免图分层。0 = 关闭（每个版本都建完整 HNSW 图）。
        "cold_threshold_ms": 0,
        "cold_sweep_interval_ms": 0,
        # 磁盘保留的"访问保护"：0 = 默认 24h，负数 = 关闭（只按版本号保新）。
        "retention_protect_window_ms": 0,
        # 被保护的版本数上限；0 = 取 gc.version_retention_count。
        "retention_protect_max": 0,
        # §8.6(c) 纯追加复用的死向量上限：0 = 默认（0.2），1.0 = 从不因墓碑重建。
        "append_max_dead_ratio": 0,
        # §3 codebook 刷新（只对学习码本的量化器有效）。0 = 默认（漂移比 0.25 / 50 次追加）。
        "max_codebook_drift_ratio": 0,
        "max_codebook_appends": 0,
        # 基线小于该规模时不看漂移比。0 = 默认（1000）；负数 = 取消下界。
        "min_codebook_baseline_vectors": 0,
        # §8.6(d) 墓碑回收：扫描一直在跑（只读），gc_enabled 才决定"收"。
        "gc_enabled": False,
        # 收集前要求"除我之外还有几个副本在服务"：0 = 默认（2）。
        "serving_replica_min": 0,
        # 带图形态的整图重建阈值：0 = 默认（0.5）。
        "graph_rebuild_ratio": 0,
        # 扫描周期：0 = 默认（10 分钟）；负数 = 关闭扫描（连收集一起没有）。
        "gc_sweep_interval_ms": 0,
        # 同时进行的索引构建数 / 分发数上限：0 = 默认（CPU 核数 / 4）。
        "build_concurrency": 0,
        "push_concurrency": 0,
        # §8.8 构建残留的超时回收：0 = 默认 30 分钟；负数 = 关闭。
        "build_abandon_timeout_ms": 0,
    },
    "write_coordinator": {
        "max_retries": 3,
        "retry_base_interval_ms": 100,
    },
    "delete_coordinator": {
        "max_retries": 5,
        "retry_base_interval_ms": 500,
    },
    "bloom_filter": {
        "expected_items": 1000000,
        "false_positive_rate": 0.01,
    },
    "gc": {
        # 每个 KB 按版本号保新的索引份数。
        "version_retention_count": 50,
        "sweep_interval_s": 300,
    },
    "lag_catchup": {
        # docs/active-lag-detection-design.md：落后副本自己追上来。
        "min_lag_versions": 1,
        "jitter_ms": 0,
        "max_concurrent_kbs": 0,
    },
}


def coerce(value: str):
    """把 --set 的字符串值转成 int/float/bool/字符串。"""
    v = value.strip()
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        pass
    return v


def set_path(cfg: dict, path: str, value: str) -> None:
    """按 a.b.c 路径设置值，路径不存在时创建。"""
    keys = path.split(".")
    node = cfg
    for k in keys[:-1]:
        nxt = node.get(k)
        if not isinstance(nxt, dict):
            nxt = {}
            node[k] = nxt
        node = nxt
    node[keys[-1]] = coerce(value)


def parse_peers(spec: str):
    """解析 "id=raft_addr[=service_addr],…" → peers 列表。"""
    peers = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        fields = [f.strip() for f in part.split("=")]
        if len(fields) < 2 or len(fields) > 3:
            raise ValueError(
                f"peers 段 '{part}' 格式错误，应为 id=raft地址[=service地址]")
        try:
            pid = int(fields[0])
        except ValueError:
            raise ValueError(f"peers 段 '{part}' 的 id 必须是数字")
        raft_addr = fields[1]
        if len(fields) == 3:
            # 显式给出 service 地址
            peers.append({"id": pid, "addr": raft_addr, "service_addr": fields[2]})
        else:
            # 未给 service 地址：不填，由 main() 的多节点校验兜底
            peers.append({"id": pid, "addr": raft_addr})
    if not peers:
        raise ValueError("--peers 不能为空")
    return peers


def parse_storage_nodes(spec: str):
    """解析 "id=地址,…" → storage.nodes 列表（两层拓扑的存储组）。"""
    nodes = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        fields = [f.strip() for f in part.split("=")]
        if len(fields) != 2:
            raise ValueError(f"storage-nodes 段 '{part}' 格式错误，应为 id=地址")
        try:
            nid = int(fields[0])
        except ValueError:
            raise ValueError(f"storage-nodes 段 '{part}' 的 id 必须是数字")
        nodes.append({"id": nid, "addr": fields[1]})
    if not nodes:
        raise ValueError("--storage-nodes 不能为空")
    return nodes


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Stratum 配置生成/调整工具", add_help=True)
    ap.add_argument("--base", help="基于现有 YAML 文件调整（缺省用内置模板）")
    ap.add_argument("--out", help="输出文件路径（缺省打印到 stdout）")
    ap.add_argument("--node-id", type=int)
    ap.add_argument("--role", choices=ROLES,
                    help="节点角色：all（默认）/ control / storage")
    ap.add_argument("--require-auth", choices=("true", "false"),
                    help="是否只服务带服务站信任标记的调用（§9.3(5)）")
    ap.add_argument("--grpc-addr")
    ap.add_argument("--raft-addr")
    ap.add_argument("--data-dir")
    ap.add_argument("--vecstore-addr")
    ap.add_argument("--embed-addr")
    ap.add_argument("--log-level", help="debug / info / warn / error")
    ap.add_argument("--peers", help='格式 "id=raft地址[=service地址],…"')
    ap.add_argument("--storage-nodes", help='存储组，格式 "id=地址,…"（两层拓扑）')
    ap.add_argument("--set", action="append", default=[],
                    metavar="a.b.c=value", help="任意参数覆盖，可多次指定")
    args = ap.parse_args()

    if args.base:
        with open(args.base, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        # 确保结构完整：缺的字段补默认值，避免覆盖后丢字段。
        cfg = deep_merge(deepcopy(DEFAULT_TEMPLATE), cfg)
    else:
        cfg = deepcopy(DEFAULT_TEMPLATE)

    # 显式参数（优先级高于 --set，因为这里更明确）
    if args.node_id is not None:
        cfg["node"]["node_id"] = args.node_id
    if args.role:
        cfg["node"]["role"] = args.role
    if args.require_auth is not None:
        cfg["node"]["require_authenticated"] = (args.require_auth == "true")
    if args.grpc_addr:
        cfg["node"]["grpc_addr"] = args.grpc_addr
    if args.raft_addr:
        cfg["node"]["raft_addr"] = args.raft_addr
    if args.log_level:
        cfg["logging"]["level"] = args.log_level
    if args.data_dir:
        cfg["storage"]["data_dir"] = args.data_dir
    if args.vecstore_addr:
        cfg["vecstore"]["grpc_addr"] = args.vecstore_addr
    if args.embed_addr:
        cfg["embed"]["service_addr"] = args.embed_addr
    if args.peers:
        cfg["raft"]["peers"] = parse_peers(args.peers)
    if args.storage_nodes:
        cfg["storage"]["nodes"] = parse_storage_nodes(args.storage_nodes)

    # --set 覆盖
    for item in args.set:
        if "=" not in item:
            print(f"错误：--set 参数 '{item}' 缺少 '='，应为 a.b.c=value", file=sys.stderr)
            return 1
        path, value = item.split("=", 1)
        set_path(cfg, path.strip(), value)

    # ---------- 校验 ----------
    peers = cfg.get("raft", {}).get("peers", [])
    # 多节点（>1 个 peer）时每个 peer 必须有 service_addr（数据同步用）
    if len(peers) > 1:
        missing = [p["id"] for p in peers if not p.get("service_addr")]
        if missing:
            print(f"错误：多节点配置要求每个 peer 都带 service 地址，"
                  f"节点 {missing} 缺少（格式 id=raft地址=service地址）", file=sys.stderr)
            return 1

    role = cfg.get("node", {}).get("role", "all")
    storage_nodes = cfg.get("storage", {}).get("nodes") or []
    node_id = cfg.get("node", {}).get("node_id")
    # 存储节点从 storage.nodes 里按自己的 node_id 解析自身地址；缺了它，这个节点
    # 要么解析不出自己的地址，要么把别人的地址当成自己的，所以这里拦住。
    if role == "storage":
        if not storage_nodes:
            print("错误：role=storage 必须给 --storage-nodes（存储节点从它解析自身地址）",
                  file=sys.stderr)
            return 1
        if node_id not in [n.get("id") for n in storage_nodes]:
            print(f"错误：role=storage 时 node_id={node_id} 必须出现在 --storage-nodes 里"
                  f"（现有 id：{[n.get('id') for n in storage_nodes]}）", file=sys.stderr)
            return 1
    if role == "control" and not storage_nodes:
        print("提示：role=control 未给 --storage-nodes：没有存储组可派发，写入只会留在"
              "元数据层（单机试验可以，两层部署不行）", file=sys.stderr)

    # 序列化：不排序、保留中文、数字不强制引号
    out = yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False,
                         default_flow_style=False, width=120)
    header = (
        "# 本文件由 scripts/ops/gen-config.py 生成/调整，可直接用 -config 启动：\n"
        "#   ./stratum -config <本文件>\n"
        "# 字段说明见 configs/config1.yaml 与 cmd/stratum/main.go 的 fileConfig。\n"
        f"# role={role}\n"
    )
    out = header + out

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"已写出 {args.out}（node_id={node_id}，role={role}，"
              f"peers={len(peers)}，storage_nodes={len(storage_nodes)}，"
              f"data_dir={cfg['storage']['data_dir']}）")
    else:
        print(out)
    return 0


def deep_merge(base: dict, override: dict) -> dict:
    """递归合并：override 中的值覆盖 base，字典递归、其它直接替换。"""
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_merge(base[k], v)
        else:
            base[k] = v
    return base


if __name__ == "__main__":
    sys.exit(main())
