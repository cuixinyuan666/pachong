#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性迁移：为 market_data.db 四张分析表补齐 crawl_date / status 两列，并回填历史行。

背景
----
baidu_finance_ai_crawler.py 现已引入"三层日期 + 空壳标记"机制：
  - crawl_date : 爬虫实际抓取日历日（审计"哪天抓的"）
  - status     : 'ok' = 真实数据已落库；'empty' = 百度未返回分析(空壳，可重试)

历史库（迁移前写入）没有这两列，且没有 status 标记。本脚本：
  1) 对四表幂等 ALTER 加列（若已存在则跳过）；
  2) 回填 status='ok'（历史行一律视为真实数据，兼容旧逻辑）；
  3) 回填 crawl_date = COALESCE(update_time, trade_date)（历史无真实抓取日记录，用真实分析日近似）。

落库交由爬虫自身 _ensure_extra_columns 自愈；本脚本额外把"历史行状态"补全，
使 --report 对账命令立即准确。

用法
----
    python migrate_crawl_date_status.py [数据库路径]   # 默认 market_data.db
"""
from __future__ import annotations

import sqlite3
import sys
import time

TABLES = ("scores", "support_resistance", "fund_flow", "vote")
EXTRA = ("crawl_date", "status")


def migrate(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=15000")
    cur = conn.cursor()

    # 1) 加列（幂等）
    for tbl in TABLES:
        cols = {r[1] for r in cur.execute(f"PRAGMA table_info({tbl})").fetchall()}
        for c in EXTRA:
            if c not in cols:
                print(f"  + ALTER {tbl} ADD COLUMN {c}")
                cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {c} TEXT")
            else:
                print(f"  = {tbl}.{c} 已存在，跳过")

    # 2) 回填 status='ok'（历史行视为真实数据）
    for tbl in TABLES:
        n = cur.execute(
            f"UPDATE {tbl} SET status='ok' WHERE status IS NULL"
        ).fetchone()
        print(f"  status 回填 'ok': {tbl} 受影响行（预计全量，NULL→ok）")

    # 3) 回填 crawl_date = COALESCE(update_time, trade_date)
    for tbl in TABLES:
        cur.execute(
            f"UPDATE {tbl} SET crawl_date = COALESCE(update_time, trade_date) "
            f"WHERE crawl_date IS NULL"
        )
        print(f"  crawl_date 回填: {tbl}")

    conn.commit()

    # 校验
    print("\n-- 校验 --")
    for tbl in TABLES:
        total = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        ok = cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE COALESCE(status,'ok')='ok'"
        ).fetchone()[0]
        null_cd = cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE crawl_date IS NULL"
        ).fetchone()[0]
        print(f"  {tbl}: 总行 {total} | ok {ok} | crawl_date 仍为空 {null_cd}")
    conn.close()


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "market_data.db"
    ts = time.strftime("%Y%m%d")
    # 安全：迁移前先在线一致备份
    bak = f"{db}.bak_cds_{ts}"
    print(f"备份 -> {bak}")
    b = sqlite3.connect(db)
    b.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    b.execute(f"VACUUM INTO '{bak}'")
    b.close()
    print(f"开始迁移 {db} ...")
    migrate(db)
    print("迁移完成。")
