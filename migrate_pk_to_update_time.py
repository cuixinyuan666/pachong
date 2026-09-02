"""
一次性迁移：把四张分析表的主键从 (trade_date, code) 改为 (update_time, code)
（support_resistance 为 (update_time, code, cycle)），并按"真实分析日"去重归并。

为什么：百度财经 AI 接口只返回"当前单日"分析，不接收日期参数。原主键用 trade_date
会导致 07-19(周日)/07-21(周二) 等"伪交易日"与真实交易日撞车。改用 update_time 作主键后，
INSERT OR REPLACE 会按真实分析日自动去重/合并跨批数据（如 07-20 新鲜部分 + 07-21 合成周一视图）。

安全：先 VACUUM INTO 在线一致备份（WAL 下读写不互斥），busy_timeout 防锁。
去重：窗口函数按 COALESCE(update_time, trade_date), code[, cycle] 分区，取 trade_date 最新一行。
"""
import os
import sqlite3

DB = r"C:\Users\Administrator\WorkBuddy\2026-07-18-17-52-45\market_data.db"
BAK = DB + ".bak_pk_migration_20260721"

NEW_TABLES = {
    "scores": """CREATE TABLE scores_new (
        trade_date TEXT, code TEXT, name TEXT,
        synthesis TEXT, technology TEXT, capital TEXT, market TEXT, finance TEXT,
        is_new TEXT, update_time TEXT,
        PRIMARY KEY (update_time, code))""",
    "support_resistance": """CREATE TABLE support_resistance_new (
        trade_date TEXT, code TEXT, cycle TEXT, support_level TEXT, resistance_level TEXT,
        level_desc TEXT, rating_text TEXT, rating_level TEXT, rating_status TEXT,
        bullish_events TEXT, bearish_events TEXT, rank_str TEXT, industry_name TEXT,
        update_time TEXT,
        PRIMARY KEY (update_time, code, cycle))""",
    "fund_flow": """CREATE TABLE fund_flow_new (
        trade_date TEXT, code TEXT, super_net REAL, large_net REAL, medium_net REAL,
        little_net REAL, super_rate TEXT, large_rate TEXT, medium_rate TEXT,
        little_rate TEXT, main_net REAL, update_time TEXT,
        PRIMARY KEY (update_time, code))""",
    "vote": """CREATE TABLE vote_new (
        trade_date TEXT, code TEXT, vote_up TEXT, vote_down TEXT, total_num TEXT,
        vote_up_rate TEXT, vote_down_rate TEXT, week_up TEXT, week_down TEXT, week_rate TEXT,
        update_time TEXT,
        PRIMARY KEY (update_time, code))""",
}

COLS = {
    "scores": "trade_date, code, name, synthesis, technology, capital, market, finance, is_new, update_time",
    "support_resistance": "trade_date, code, cycle, support_level, resistance_level, level_desc, rating_text, rating_level, rating_status, bullish_events, bearish_events, rank_str, industry_name, update_time",
    "fund_flow": "trade_date, code, super_net, large_net, medium_net, little_net, super_rate, large_rate, medium_rate, little_rate, main_net, update_time",
    "vote": "trade_date, code, vote_up, vote_down, total_num, vote_up_rate, vote_down_rate, week_up, week_down, week_rate, update_time",
}

PARTS = {
    "scores": "COALESCE(update_time, trade_date), code",
    "support_resistance": "COALESCE(update_time, trade_date), code, cycle",
    "fund_flow": "COALESCE(update_time, trade_date), code",
    "vote": "COALESCE(update_time, trade_date), code",
}


def main():
    # 1) 在线一致备份
    src = sqlite3.connect(DB)
    src.execute("PRAGMA busy_timeout=30000")
    if not os.path.exists(BAK):
        src.execute(f"VACUUM INTO '{BAK}'")
        print(f"[备份] 已生成一致备份: {os.path.basename(BAK)}")
    else:
        print(f"[备份] 备份已存在，跳过: {os.path.basename(BAK)}")
    src.close()

    # 2) 迁移
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA busy_timeout=60000")
    cur = conn.cursor()

    for t in ["scores", "support_resistance", "fund_flow", "vote"]:
        before = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        cur.execute(NEW_TABLES[t])
        cur.execute(
            f"""
            INSERT INTO {t}_new ({COLS[t]})
            SELECT {COLS[t]} FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {PARTS[t]} ORDER BY trade_date DESC
                ) AS rn FROM {t}
            ) WHERE rn = 1
            """
        )
        after = cur.execute(f"SELECT COUNT(*) FROM {t}_new").fetchone()[0]
        distinct_ut = cur.execute(
            f"SELECT COUNT(DISTINCT update_time) FROM {t}_new"
        ).fetchone()[0]
        null_ut = cur.execute(
            f"SELECT COUNT(*) FROM {t}_new WHERE update_time IS NULL"
        ).fetchone()[0]
        # 列出仍存在的"伪交易日" trade_date 标签（仅信息）
        print(
            f"[{t}] before={before} after={after} "
            f"distinct_update_time={distinct_ut} null_update_time={null_ut}"
        )
        cur.execute(f"DROP TABLE {t}")
        cur.execute(f"ALTER TABLE {t}_new RENAME TO {t}")

    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    print("[完成] 四表主键已改为 (update_time, code)，去重归并完成。")


if __name__ == "__main__":
    main()
