#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场报告生成器：读取 market_data.db（时序快照），生成最新快照的概览报告。

覆盖口径修正（2026-07-22）：
  旧版按 MAX(trade_date) 取单一批次日，但 save_snapshot 重爬时会把老旧股票以"今天"为
  trade_date 写入，导致数据被切碎到多个 trade_date，报告只看最新批次日会漏掉数千支。
  新版改为"每支 code 取最新 update_time 快照"（scores 主键为 (update_time, code)），
  并据此关联 support/fund_flow/vote，覆盖股票数按 DISTINCT code 统计，真实反映抓取覆盖。

用法:
    python make_report_full.py --db market_data.db --out report_full.html
"""

import argparse
import html
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

DB_DEFAULT = "market_data.db"
OUT_DEFAULT = "report_full.html"

# 每支 code 的最新快照键（update_time）作为所有表关联基准：
#   scores 主键 (update_time, code) -> 取 MAX(update_time) 即该 code 最新一次分析。
LATEST_KEY_SQL = (
    "SELECT code, MAX(update_time) AS ut FROM scores "
    "WHERE COALESCE(status,'ok')='ok' GROUP BY code"
)


def snapshot_date(conn):
    """报告所依据的"最新抓取日"：取 crawl_stats 中最近一次成功抓取日。"""
    row = conn.execute(
        "SELECT MAX(last_attempt) FROM crawl_stats WHERE last_status='ok'").fetchone()
    if row and row[0]:
        return row[0][:10]
    row = conn.execute("SELECT MAX(trade_date) FROM scores").fetchone()
    return row[0] if row else None


def esc(x):
    return html.escape("" if x is None else str(x))


def fmt_num(x):
    try:
        f = float(x)
        return f"{f:+.2f}" if f >= 0 else f"{f:.2f}"
    except Exception:
        return esc(x)


def parse_wan(x):
    """解析百度投票的"万"单位：'1.2万' -> 12000。

    旧逻辑 str.replace('万','0000') 会把 '1.2万' 算成 120000（10 倍），此处修正为
    float(x) * 10000，保证排序与绝对数值都正确。
    """
    if not x:
        return 0
    s = str(x).strip()
    try:
        if "万" in s:
            return int(float(s.replace("万", "")) * 10000)
        return int(float(s))
    except Exception:
        return 0


def build_table(headers, rows, aligns=None):
    aligns = aligns or ["left"] * len(headers)
    th = "".join(f'<th style="text-align:{aligns[i]}">{esc(h)}</th>'
                 for i, h in enumerate(headers))
    body = ""
    for r in rows:
        tds = "".join(
            f'<td style="text-align:{aligns[i]}">{r[i]}</td>'
            for i in range(len(r)))
        body += f"<tr>{tds}</tr>"
    return f'<table><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>'


def gen(db_path, out_path):
    conn = sqlite3.connect(db_path)
    td = snapshot_date(conn)
    if not td:
        print("数据库为空，无数据可生成报告。")
        return False
    names = {r[0]: r[1] for r in conn.execute("SELECT code, name FROM stocks")}

    # 覆盖股票数：真实拥有 ok 快照的 distinct code 数（修复旧版按单 trade_date 低估的问题）
    total = conn.execute(
        "SELECT COUNT(*) FROM (SELECT code FROM scores "
        "WHERE COALESCE(status,'ok')='ok' GROUP BY code)").fetchone()[0]

    # 五维评分（综合）— 关联每支 code 最新快照
    rows = conn.execute(
        "SELECT s.code, s.synthesis, s.technology, s.capital, s.market, s.finance "
        "FROM scores s JOIN (" + LATEST_KEY_SQL + ") l "
        "ON s.code=l.code AND s.update_time=l.ut "
        "ORDER BY CAST(s.synthesis AS REAL) DESC").fetchall()
    score_rows = [(names.get(c, ""), c, s, t, ca, m, f) for (c, s, t, ca, m, f) in rows]

    # 资金净流入（主力）
    fr = conn.execute(
        "SELECT f.code, f.super_net, f.large_net, f.medium_net, f.little_net, f.main_net "
        "FROM fund_flow f JOIN (" + LATEST_KEY_SQL + ") l "
        "ON f.code=l.code AND f.update_time=l.ut").fetchall()
    flow_sorted = sorted(fr, key=lambda r: (r[5] or -1e9), reverse=True)
    inflow_top = [(names.get(c, ""), c, fmt_num(mn), fmt_num(sn), fmt_num(ln),
                  fmt_num(men), fmt_num(ln2)) for (c, sn, ln, men, ln2, mn) in flow_sorted[:20]]
    outflow_top = [(names.get(c, ""), c, fmt_num(mn), fmt_num(sn), fmt_num(ln),
                   fmt_num(men), fmt_num(ln2)) for (c, sn, ln, men, ln2, mn) in flow_sorted[-20:]]

    # 支撑 / 阻力
    sr = conn.execute(
        "SELECT sr.code, sr.cycle, sr.support_level, sr.resistance_level, sr.rating_text, sr.industry_name "
        "FROM support_resistance sr JOIN (" + LATEST_KEY_SQL + ") l "
        "ON sr.code=l.code AND sr.update_time=l.ut "
        "WHERE sr.support_level IS NOT NULL ORDER BY sr.code").fetchall()
    sr_rows = [(names.get(c, ""), c, cyc, esc(sup), esc(res), esc(rt), esc(ind))
               for (c, cyc, sup, res, rt, ind) in sr]

    # 投票（本周）
    vt = conn.execute(
        "SELECT v.code, v.vote_up, v.vote_down, v.week_up, v.week_down "
        "FROM vote v JOIN (" + LATEST_KEY_SQL + ") l "
        "ON v.code=l.code AND v.update_time=l.ut").fetchall()
    vote_rows = []
    for (c, vu, vd, wu, wd) in vt:
        vote_rows.append((names.get(c, ""), c, esc(vu), esc(vd), esc(wu), esc(wd),
                          parse_wan(wu) + parse_wan(wd)))
    vote_rows.sort(key=lambda r: r[6], reverse=True)
    vote_disp = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in vote_rows[:20]]

    now = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
    css = """
    * { box-sizing: border-box; }
    body { background:#0f1115; color:#d8dee9; font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin:0; padding:24px; }
    h1 { font-size:22px; color:#e8eefc; border-bottom:1px solid #2a2f3a; padding-bottom:8px; }
    h2 { font-size:17px; color:#8fd0ff; margin-top:28px; }
    .meta { color:#8a93a6; font-size:13px; margin-bottom:6px; }
    .card { background:#161a21; border:1px solid #232834; border-radius:10px; padding:14px 16px; margin:10px 0; }
    table { border-collapse:collapse; width:100%; font-size:13px; margin-top:6px; }
    th, td { padding:7px 10px; border-bottom:1px solid #232834; white-space:nowrap; }
    th { color:#9fb0c8; text-align:left; font-weight:600; }
    tbody tr:hover { background:#1c2230; }
    .pos { color:#ff6b6b; } .neg { color:#4ec9a0; }
    .tag { display:inline-block; background:#1f2733; color:#9fd0ff; border-radius:6px; padding:2px 8px; font-size:12px; }
    """
    html_parts = [f"<html><head><meta charset='utf-8'><title>全市场报告 {td}</title><style>{css}</style></head><body>"]
    html_parts.append(f"<h1>A股全市场 · AI 技术分析快照</h1>")
    html_parts.append(f'<div class="meta">最新抓取日: <b>{esc(td)}</b> ｜ 覆盖股票: <b>{total}</b> ｜ 生成于 {esc(now)} (UTC+8)</div>')

    html_parts.append('<h2>五维评分 · 综合排行（Top 20）</h2>')
    html_parts.append(build_table(
        ["名称", "代码", "综合", "技术", "资金", "市场", "财务"],
        [(r[0], r[1], esc(r[2]), esc(r[3]), esc(r[4]), esc(r[5]), esc(r[6])) for r in score_rows[:20]],
        aligns=["left", "left", "right", "right", "right", "right", "right"]))

    html_parts.append('<h2>主力资金净流入 · Top 20</h2>')
    html_parts.append(build_table(
        ["名称", "代码", "主力净流入", "超大单", "大单", "中单", "小单"],
        inflow_top, aligns=["left", "left", "right", "right", "right", "right", "right"]))

    html_parts.append('<h2>主力资金净流出 · Top 20</h2>')
    html_parts.append(build_table(
        ["名称", "代码", "主力净流入", "超大单", "大单", "中单", "小单"],
        outflow_top, aligns=["left", "left", "right", "right", "right", "right", "right"]))

    html_parts.append('<h2>支撑位 / 阻力位（长期，截至最新抓取日）</h2>')
    if sr_rows:
        html_parts.append(build_table(
            ["名称", "代码", "周期", "支撑位", "阻力位", "评级", "所属行业"],
            sr_rows, aligns=["left", "left", "left", "right", "right", "left", "left"]))
    else:
        html_parts.append('<div class="card">该抓取日无支撑/阻力数据。</div>')

    html_parts.append('<h2>股评投票 · 本周看涨/看跌（Top 20 by 总票数）</h2>')
    html_parts.append(build_table(
        ["名称", "代码", "总看涨", "总看跌", "本周看涨", "本周看跌"],
        vote_disp, aligns=["left", "left", "right", "right", "right", "right"]))

    html_parts.append("</body></html>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))
    print(f"报告已生成: {out_path}（最新抓取日 {td}，覆盖 {total} 支）")
    conn.close()
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--out", default=OUT_DEFAULT)
    args = ap.parse_args()
    if not gen(args.db, args.out):
        sys.exit(1)


if __name__ == "__main__":
    main()
