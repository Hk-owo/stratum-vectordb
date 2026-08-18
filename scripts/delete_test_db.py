#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""删除 Stratum 的磁盘数据（等价于清空 / 重置数据库）。

结论先说：Stratum 的所有数据都落在磁盘文件里，删掉对应目录并重启服务
就是一个全新的空库。Go 侧是 PebbleDB + WAL + Raft 硬状态，C++ 侧是
RocksDB + Faiss HNSW 索引，各自对应一个目录：

  Go 侧  (stratum)：<data-dir>/stratum
      docstore/  chunkdoc/  versiondoc/  wal/  raft/
  C++ 侧 (vecstore_server)：<data-dir>/vecstore_rocksdb
      RocksDB 数据 + Faiss HNSW 索引

默认按开发环境 ./start.sh 的目录布局（run/data/）来删；如果你用
config1.yaml（/var/lib/stratum/node1），用 --data-dir 指过去即可。

⚠️ 重要：删除前必须先停掉服务（stratum、vecstore_server、gateway 等）。
    进程运行中会持有文件锁和内存态，直接删文件可能损坏数据，或导致
    重启后 WAL/Raft 状态与磁盘不一致。

用法示例：
  python3 scripts/delete_test_db.py --dry-run      # 只看会删哪些目录
  python3 scripts/delete_test_db.py                # 交互确认后删除
  python3 scripts/delete_test_db.py --yes          # 跳过确认直接删除
  python3 scripts/delete_test_db.py --data-dir /var/lib/stratum/node1

只依赖 Python 标准库。若只想删除单个知识库（不重启、保留其它数据），
请用 API：POST /api/knowledge-bases/delete 传 {knowledge_base_id}。
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys

# 相对 <data-dir> 的两个数据子目录，与 start.sh / config1.yaml 对应。
GO_DIR = "stratum"
VECSTORE_DIR = "vecstore_rocksdb"


def repo_root() -> str:
    """脚本位于 <repo>/scripts/，仓库根是上一级目录。"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def targets(data_dir: str, go: bool, vecstore: bool) -> list[str]:
    out = []
    if go:
        out.append(os.path.join(data_dir, GO_DIR))
    if vecstore:
        out.append(os.path.join(data_dir, VECSTORE_DIR))
    return out


def confirm(prompt: str) -> bool:
    if not sys.stdin.isatty():
        print("stdin 不是终端，无法交互确认。请用 --yes 显式确认，或先 --dry-run 查看。")
        return False
    ans = input(prompt + " [y/N] ").strip().lower()
    return ans in ("y", "yes")


def main() -> int:
    parser = argparse.ArgumentParser(description="删除 Stratum 磁盘数据目录（清空/重置数据库）")
    parser.add_argument("--data-dir", default=os.path.join(repo_root(), "run", "data"),
                        help="数据根目录，默认 <repo>/run/data（start.sh 布局）")
    parser.add_argument("--skip-go", action="store_true", help="不删除 Go 侧数据目录 (<data-dir>/stratum)")
    parser.add_argument("--skip-vecstore", action="store_true", help="不删除 C++ 侧数据目录 (<data-dir>/vecstore_rocksdb)")
    parser.add_argument("--yes", "-y", action="store_true", help="跳过确认，直接删除")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要删除的目录，不实际删除")
    args = parser.parse_args()

    if args.skip_go and args.skip_vecstore:
        raise SystemExit("--skip-go 和 --skip-vecstore 不能同时使用，那等于什么都不删。")

    dirs = targets(os.path.abspath(args.data_dir), not args.skip_go, not args.skip_vecstore)

    print("将要删除以下数据目录（对应磁盘文件）：")
    for d in dirs:
        exists = os.path.isdir(d)
        marker = "" if exists else "  (不存在)"
        print(f"  - {d}{marker}")

    existing = [d for d in dirs if os.path.isdir(d)]
    if not existing:
        print("没有可删除的目录，数据库可能已经是空的了。")
        return 0

    if args.dry_run:
        print("\n--dry-run：未做任何删除。")
        return 0

    print("\n⚠️  删除前请确认：stratum / vecstore_server / gateway 等进程均已停止。")
    if not args.yes and not confirm("确定删除以上目录吗？此操作不可恢复"):
        print("已取消。")
        return 1

    for d in existing:
        shutil.rmtree(d)
        print(f"已删除: {d}")

    print("\n完成。重启 ./start.sh 后即为全新的空数据库。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
