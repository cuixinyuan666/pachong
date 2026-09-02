#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_crawl_stats.py — 为新增的 crawl_stats(逐股累计抓取统计) 表回填历史基线。

背景：
    crawl_stats 表（code PK, crawl_count, last_success, last_status, last_attempt, updated_at）
    是后来新增的。在爬虫开始维护该表之前，已有大量 scores 历史快照，本脚本用这些快照
    反推每只股票的历史基线，使"爬取次数 / 最近是否成功"在老库上也能立刻有值。

基线口径（仅基线，非精确尝试次数——精确计数需爬虫实际运行时累计）：
    - crawl_count  = 该 code 在 scores 表中的历史行数（≈曾成功存储的快照数）
    - last_status  = 该 code 按 (crawl_date DESC, rowid DESC) 取最新一行的 status
    - last_success = 1 if last_status=='ok' else 0
    - last_attempt = 最新一行的 crawl_date
已存在于 crawl_stats 的 code 不会覆盖（保留爬虫实跑累计的精确值）。

用法：
    python migrate_crawl_stats.py [db_path]   # 默认 market_data.db
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone, timedelta

HERE = __file__  # placeholder, replaced below
import os
DB_PATH = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "market_data.db")


def _now_cst() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA busy_timeout=30000")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS crawl_stats ("
        "code TEXT PRIMARY KEY, crawl_count INTEGER NOT NULL DEFAULT 0, "
        "last_success INTEGER NOT NULL DEFAULT 0, last_status TEXT, "
        "last_attempt TEXT, updated_at TEXT)"
    )

    # 已存在的 code 不再回填（保留爬虫实跑的精确累计）
    cur.execute(
        "SELECT code, status, crawl_date FROM scores "
        "ORDER BY code, crawl_date DESC, rowid DESC"
    )
    agg = {}
    for code, status, crawl_date in cur.fetchall():
        if code not in agg:
            agg[code] = {"count": 0, "last_status": status, "last_attempt": crawl_date}
        agg[code]["count"] += 1

    now = _now_cst()
    n = 0
    for code, d in agg.items():
        last = d["last_status"] or "ok"
        ls = 1 if last == "ok" else 0
        cur.execute(
            "INSERT INTO crawl_stats (code, crawl_count, last_success, last_status, "
            "last_attempt, updated_at) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(code) DO NOTHING",
            (code, d["count"], ls, last, d["last_attempt"], now),
        )
        n += 1

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM crawl_stats")
    total = cur.fetchone()[0]
    conn.close()
    print("crawl_stats 基线回填完成：新增 %d 支，现共 %d 支（已存在的不覆盖）。" % (n, total))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
