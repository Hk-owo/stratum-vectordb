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

常用参数（未指定时取默认值，与 cmd/stratum 内置默认一致）：
  --node-id 节点 ID（多节点时 1/2/3…）
  --grpc-addr / --raft-addr 对外 gRPC / Raft 内部通信地址
  --data-dir 数据目录（docstore/wal/raft/索引都放这里）
  --vecstore-addr vecstore_server 的 gRPC 地址
  --embed-addr embed 服务地址
  --peers 集群成员表，格式 "id=raft地址=service地址,…"
          （service 地址是数据同步用的 gRPC 地址，多节点必填；
           单节点可省略 service 段）
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
# 注意：fileConfig 目前只解析 node.* / raft.peers / storage.data_dir /
# vecstore.grpc_addr / embed.service_addr / index_manager.* /
# write_coordinator.* / delete_coordinator.*；其余字段仅为文档/预留。
DEFAULT_TEMPLATE = {
    "node": {
        "node_id": 1,
        "grpc_addr": "0.0.0.0:7000",
        "raft_addr": "0.0.0.0:8000",
        "metrics_addr": "0.0.0.0:9000",
        "gateway_http_addr": "0.0.0.0:8081",
    },
    "raft": {
        "peers": [
            {"id": 1, "addr": "localhost:8000", "service_addr": "localhost:7000"},
        ],
    },
    "storage": {
        "data_dir": "/var/lib/stratum/node1",
    },
    "vecstore": {
        "grpc_addr": "127.0.0.1:7100",
    },
    "embed": {
        "service_addr": "http://localhost:8080",
    },
    "index_manager": {
        "lru_capacity": 16,
        "memory_threshold_mb": 4096,
        "load_wait_timeout_ms": 5000,
        "callback_max_retries": 3,
        "callback_retry_base_interval_ms": 200,
    },
    "write_coordinator": {
        "max_retries": 3,
        "retry_base_interval_ms": 100,
    },
    "delete_coordinator": {
        "max_retries": 5,
        "retry_base_interval_ms": 500,
    },
    "logging": {
        "level": "info",
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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Stratum 配置生成/调整工具", add_help=True)
    ap.add_argument("--base", help="基于现有 YAML 文件调整（缺省用内置模板）")
    ap.add_argument("--out", help="输出文件路径（缺省打印到 stdout）")
    ap.add_argument("--node-id", type=int)
    ap.add_argument("--grpc-addr")
    ap.add_argument("--raft-addr")
    ap.add_argument("--data-dir")
    ap.add_argument("--vecstore-addr")
    ap.add_argument("--embed-addr")
    ap.add_argument("--peers", help='格式 "id=raft地址[=service地址],…"')
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
    if args.grpc_addr:
        cfg["node"]["grpc_addr"] = args.grpc_addr
    if args.raft_addr:
        cfg["node"]["raft_addr"] = args.raft_addr
    if args.data_dir:
        cfg["storage"]["data_dir"] = args.data_dir
    if args.vecstore_addr:
        cfg["vecstore"]["grpc_addr"] = args.vecstore_addr
    if args.embed_addr:
        cfg["embed"]["service_addr"] = args.embed_addr
    if args.peers:
        cfg["raft"]["peers"] = parse_peers(args.peers)

    # --set 覆盖
    for item in args.set:
        if "=" not in item:
            print(f"错误：--set 参数 '{item}' 缺少 '='，应为 a.b.c=value", file=sys.stderr)
            return 1
        path, value = item.split("=", 1)
        set_path(cfg, path.strip(), value)

    # 校验：多节点（>1 个 peer）时每个 peer 必须有 service_addr（数据同步用）
    peers = cfg.get("raft", {}).get("peers", [])
    if len(peers) > 1:
        missing = [p["id"] for p in peers if not p.get("service_addr")]
        if missing:
            print(f"错误：多节点配置要求每个 peer 都带 service 地址，"
                  f"节点 {missing} 缺少（格式 id=raft地址=service地址）", file=sys.stderr)
            return 1

    # 序列化：不排序、保留中文、数字不强制引号
    out = yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False,
                         default_flow_style=False, width=120)
    header = (
        "# 本文件由 scripts/ops/gen-config.py 生成/调整，可直接用 -config 启动：\n"
        "#   ./stratum -config <本文件>\n"
        "# 字段说明见 configs/config1.yaml 与 cmd/stratum/main.go 的 fileConfig。\n"
    )
    out = header + out

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"已写出 {args.out}（node_id={cfg['node']['node_id']}，"
              f"peers={len(peers)}，data_dir={cfg['storage']['data_dir']}）")
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
