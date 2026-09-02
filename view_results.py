#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度财经 AI 爬虫 · 解析结果查看器（零依赖 / 零 Rust 编译）
=====================================================

用途:
    调试"解析结果对不对"时，不必重新编译 Rust GUI。本脚本直接读取
    Python 爬虫写入的 market_data.db，生成一份**自包含**的 HTML 报告
    （数据内嵌，无需服务器），用浏览器打开即可查看四张表
    （scores / support_resistance / fund_flow / vote）的解析结果，
    支持按 代码/名称 搜索、按 真实分析日(update_time) 过滤、按 状态 过滤。

用法:
    python view_results.py                 # 读 ./market_data.db -> ./view_report.html
    python view_results.py --db X.db --out Y.html
    python view_results.py --limit 200     # 仅前 N 支（超大库时加快生成）

之后每次爬完，重跑本脚本刷新报告即可。Rust GUI 只在最后出成品时编一次。
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

from crawler_common import source_verify_links


def _now_cst() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def load(db_path: str, limit: int | None = None):
    """读取四张表并组装成可序列化的记录列表。

    返回 dict: {"ok": True, "meta": {...}, "records": [...]} 或 {"ok": False, "error": str}
    """
    if not os.path.exists(db_path):
        return {"ok": False, "error": f"数据库不存在: {db_path}"}

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=5000")  # 抓取中并发读库时等待而非报错
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 确认至少有 scores 表
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scores'")
        if not cur.fetchone():
            conn.close()
            return {"ok": False, "error": "scores 表不存在，可能还不是爬虫产出库"}
    except sqlite3.Error as e:
        conn.close()
        return {"ok": False, "error": f"读取失败: {e}"}

    def _cols(tbl):
        try:
            return {r[1] for r in cur.execute(f"PRAGMA table_info({tbl})").fetchall()}
        except sqlite3.Error:
            return set()

    has_ff = "code" in _cols("fund_flow")
    has_vote = "code" in _cols("vote")
    has_sr = "code" in _cols("support_resistance")

    scores_rows = cur.execute("SELECT * FROM scores ORDER BY update_time DESC, code").fetchall()
    if limit:
        scores_rows = scores_rows[:limit]

    ff_map, vote_map, sr_map = {}, {}, {}
    if has_ff:
        for r in cur.execute("SELECT * FROM fund_flow"):
            ff_map[(r["code"], r["update_time"])] = dict(r)
    if has_vote:
        for r in cur.execute("SELECT * FROM vote"):
            vote_map[(r["code"], r["update_time"])] = dict(r)
    if has_sr:
        for r in cur.execute("SELECT * FROM support_resistance"):
            sr_map[(r["code"], r["update_time"], r["cycle"])] = dict(r)

    # 逐股累计抓取统计（crawl_stats，按 code 建表）：爬取次数 + 最近是否成功
    cs_map = {}
    has_cs = False
    try:
        for r in cur.execute("SELECT code, crawl_count, last_success, last_status, last_attempt, updated_at FROM crawl_stats"):
            cs_map[r["code"]] = {
                "crawl_count": r["crawl_count"],
                "last_success": r["last_success"],
                "last_status": r["last_status"],
                "last_attempt": r["last_attempt"],
                "updated_at": r["updated_at"],
            }
        has_cs = True
    except sqlite3.Error:
        has_cs = False

    # 东方财富 千股千评(em_comment) + 估值(em_valuation)：按 code 取最新一条
    emc_map, emv_map = {}, {}
    has_em = False
    try:
        for r in cur.execute(
                "SELECT * FROM em_comment WHERE (code, trade_date) IN ("
                "SELECT code, MAX(trade_date) FROM em_comment GROUP BY code)"):
            d = dict(r)
            emc_map[d["code"]] = d
        for r in cur.execute(
                "SELECT * FROM em_valuation WHERE (code, trade_date) IN ("
                "SELECT code, MAX(trade_date) FROM em_valuation GROUP BY code)"):
            d = dict(r)
            emv_map[d["code"]] = d
        has_em = True
    except sqlite3.Error:
        has_em = False

    # 东方财富 定性诊断文字(em_diag_text) + 诊断概率(em_diag_prob)：按 code 取最新一条
    emt_map, emp_map = {}, {}
    has_emt = False
    has_emp = False
    try:
        for r in cur.execute(
                "SELECT * FROM em_diag_text WHERE (code, trade_date) IN ("
                "SELECT code, MAX(trade_date) FROM em_diag_text GROUP BY code)"):
            d = dict(r)
            emt_map[d["code"]] = d
        has_emt = True
    except sqlite3.Error:
        has_emt = False
    try:
        for r in cur.execute(
                "SELECT * FROM em_diag_prob WHERE (code, trade_date) IN ("
                "SELECT code, MAX(trade_date) FROM em_diag_prob GROUP BY code)"):
            d = dict(r)
            emp_map[d["code"]] = d
        has_emp = True
    except sqlite3.Error:
        has_emp = False

    # 东方财富 参与意愿(em_participation) + 市场排名(em_popularity)：按 code 取最新一条
    empart_map, empop_map = {}, {}
    has_empart = False
    has_empop = False
    try:
        for r in cur.execute(
                "SELECT * FROM em_participation WHERE (code, trade_date) IN ("
                "SELECT code, MAX(trade_date) FROM em_participation GROUP BY code)"):
            d = dict(r)
            empart_map[d["code"]] = d
        has_empart = True
    except sqlite3.Error:
        has_empart = False
    try:
        # 市场排名需历史以计算「较昨日变化 N 名」：取每 code 全部并按 trade_date 排序
        rows = cur.execute("SELECT * FROM em_popularity ORDER BY code, trade_date").fetchall()
        bycode = {}
        for r in rows:
            d = dict(r)
            bycode.setdefault(d["code"], []).append(d)
        for code, lst in bycode.items():
            lst.sort(key=lambda x: x["trade_date"])
            latest = dict(lst[-1])
            prev = lst[-2] if len(lst) >= 2 else None
            if (prev is not None and latest.get("emp_market_rank") is not None
                    and prev.get("emp_market_rank") is not None):
                latest["emp_rank_change"] = (latest["emp_market_rank"]
                                             - prev["emp_market_rank"])
            empop_map[code] = latest
        has_empop = True
    except sqlite3.Error:
        has_empop = False
    conn.close()

    records = []
    dates = set()
    ok_cnt = empty_cnt = 0
    latest_real = None
    for s in scores_rows:
        s = dict(s)
        code = s.get("code")
        ut = s.get("update_time")
        status = s.get("status") or "ok"
        if status == "ok":
            ok_cnt += 1
            if ut:
                dates.add(ut)
                if latest_real is None or ut > latest_real:
                    latest_real = ut
        else:
            empty_cnt += 1
            if ut:
                dates.add(ut)
        ff = ff_map.get((code, ut), {}) if has_ff else {}
        vt = vote_map.get((code, ut), {}) if has_vote else {}
        sr = {
            "long": sr_map.get((code, ut, "long")) if has_sr else None,
            "short": sr_map.get((code, ut, "short")) if has_sr else None,
        }
        rec = {"s": s, "ff": ff, "vt": vt, "sr": sr,
               "cs": cs_map.get(code),
               "emc": emc_map.get(code), "emv": emv_map.get(code),
               "emt": emt_map.get(code), "emp": emp_map.get(code),
               "empart": empart_map.get(code), "empop": empop_map.get(code)}
        if status != "ok":
            srcs = ["baidu"]
            # 东财表在、但这只票没有东财行 → 也给出东财页，方便核对是不是源站也没有
            if has_em and not emc_map.get(code):
                srcs.append("em")
            rec["verify_links"] = source_verify_links(code, srcs)
        records.append(rec)

    meta = {
        "total": len(records),
        "ok": ok_cnt,
        "empty": empty_cnt,
        "latest_real": latest_real,
        "dates": sorted(dates, reverse=True),
        "generated_at": _now_cst(),
        "db": os.path.abspath(db_path),
        "has_ff": has_ff,
        "has_vote": has_vote,
        "has_sr": has_sr,
        "has_cs": has_cs,
        "has_em": has_em,
        "has_emt": has_emt,
        "has_emp": has_emp,
        "has_empart": has_empart,
        "has_empop": has_empop,
    }
    return {"ok": True, "meta": meta, "records": records}


HTML_SKELETON = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>百度财经 AI 解析结果查看器</title>
<style>
  :root {{ --bg:#f7f8fa; --card:#fff; --line:#e5e7eb; --ink:#1f2937; --mut:#6b7280;
          --red:#d12c2c; --green:#0a8f3c; --blue:#2563eb; --amber:#b45309; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;
         background:var(--bg); color:var(--ink); font-size:14px; }}
  header {{ padding:14px 20px; background:var(--card); border-bottom:1px solid var(--line); position:sticky; top:0; z-index:5; }}
  h1 {{ font-size:17px; margin:0 0 8px; }}
  .summary {{ display:flex; flex-wrap:wrap; gap:14px; color:var(--mut); font-size:13px; }}
  .summary b {{ color:var(--ink); }}
  .controls {{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-top:10px; }}
  input, select {{ padding:6px 9px; border:1px solid var(--line); border-radius:8px; font-size:13px; background:#fff; color:var(--ink); }}
  input[type=text] {{ min-width:200px; }}
  .wrap {{ padding:16px 20px; }}
  table {{ border-collapse:collapse; width:100%; background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
  th, td {{ padding:8px 10px; text-align:left; border-bottom:1px solid var(--line); white-space:nowrap; }}
  th {{ background:#f1f3f5; cursor:pointer; user-select:none; position:sticky; top:118px; z-index:3; }}
  tbody tr {{ cursor:pointer; }}
  tbody tr:hover {{ background:#f0f6ff; }}
  td.num, th.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .red {{ color:var(--red); }} .green {{ color:var(--green); }}
  .tag {{ display:inline-block; padding:1px 7px; border-radius:999px; font-size:12px; }}
  .tag.ok {{ background:#e7f7ec; color:var(--green); }}
  .tag.empty {{ background:#fdeaea; color:var(--red); }}
  .detail {{ background:#fbfcfe; }}
  .detail .box {{ padding:12px 16px; display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:14px; }}
  .panel {{ border:1px solid var(--line); border-radius:10px; padding:10px 12px; background:#fff; }}
  .panel h3 {{ margin:0 0 8px; font-size:13px; color:var(--blue); }}
  .kv {{ display:flex; justify-content:space-between; gap:10px; padding:3px 0; border-bottom:1px dashed #eef0f2; }}
  .kv:last-child {{ border-bottom:none; }}
  .kv span:first-child {{ color:var(--mut); }}
  .muted {{ color:var(--mut); }}
  .empty-note {{ color:var(--red); padding:8px 12px; }}
  .src-links {{ padding:0 12px 12px; }}
  .src-row {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; padding:4px 0; }}
  .src-row a {{ color:var(--blue); word-break:break-all; }}
  .copy-btn {{ font-size:12px; padding:2px 8px; cursor:pointer; }}
  footer {{ padding:10px 20px; color:var(--mut); font-size:12px; border-top:1px solid var(--line); }}
  .pill {{ background:#eef2ff; color:var(--blue); border-radius:999px; padding:1px 8px; font-size:12px; }}
</style>
</head>
<body>
<header>
  <h1>百度财经 AI 解析结果查看器 <span class="pill">零依赖 · 直接读 market_data.db</span></h1>
  <div class="summary" id="summary"></div>
  <div class="controls">
    <input type="text" id="q" placeholder="搜索 代码 / 名称…" oninput="render()">
    <select id="date" onchange="render()"><option value="">全部真实分析日</option></select>
    <select id="status" onchange="render()" title="未拿到=本次爬虫没取到，不是源站已确认无数据。点开该行可打开源站链接人工核对。">
      <option value="">全部状态</option>
      <option value="ok">真实(ok)</option>
      <option value="empty">未拿到(待核对)</option>
    </select>
    <select id="sort" onchange="render()">
      <option value="code">排序: 代码</option>
      <option value="update_time">排序: 真实分析日</option>
      <option value="main_net">排序: 主力净流入</option>
      <option value="crawl_count">排序: 爬取次数</option>
      <option value="synthesis">排序: 综合评分</option>
      <option value="emc_total_score">排序: 东财综合</option>
      <option value="emc_rank">排序: 东财排名</option>
      <option value="emc_org">排序: 东财机构参与</option>
      <option value="emc_focus">排序: 东财关注指数</option>
      <option value="emv_pe">排序: 东财PE</option>
      <option value="empart_change">排序: 参与意愿变化</option>
      <option value="empop_rank">排序: 市场排名</option>
    </select>
    <span id="count" class="muted"></span>
  </div>
</header>
<div class="wrap">
  <table>
    <thead><tr>
      <th onclick="setSort('code')">代码</th>
      <th onclick="setSort('name')">名称</th>
      <th onclick="setSort('update_time')">真实分析日</th>
      <th>抓取日</th>
      <th>状态</th>
      <th class="num" onclick="setSort('crawl_count')">爬取次数</th>
      <th>最近成功</th>
      <th class="num" onclick="setSort('emc_total_score')">东财综合</th>
      <th class="num" onclick="setSort('emc_rank')">东财排名</th>
      <th class="num" onclick="setSort('emc_org')">东财机构</th>
      <th class="num" onclick="setSort('emc_focus')">东财关注</th>
      <th class="num" onclick="setSort('emc_prime')">东财成本</th>
      <th class="num" onclick="setSort('emv_pe')">东财PE</th>
      <th class="num" onclick="setSort('emp_rise1')">次日概率</th>
      <th class="num" onclick="setSort('emp_rank')">打败%</th>
      <th class="num" onclick="setSort('empart_change')">参与意愿Δ</th>
      <th class="num" onclick="setSort('empop_rank')">市场排名</th>
      <th class="num" onclick="setSort('synthesis')">综合</th>
      <th class="num">技术</th>
      <th class="num">资金</th>
      <th class="num">市场</th>
      <th class="num">财务</th>
      <th class="num" onclick="setSort('main_net')">主力净流入(亿)</th>
      <th class="num">看涨/看跌</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
</div>
<footer id="footer"></footer>

<script id="meta" type="application/json">{META}</script>
<script id="data" type="application/json">{DATA}</script>
<script>
const META = JSON.parse(document.getElementById('meta').textContent);
const DATA = JSON.parse(document.getElementById('data').textContent);

function num(v) {{
  if (v === null || v === undefined || v === '') return '';
  const n = Number(v);
  if (isNaN(n)) return String(v);
  const cls = n > 0 ? 'red' : (n < 0 ? 'green' : '');
  return `<span class="${{cls}}">${{n}}</span>`;
}}
function txt(v) {{ return (v === null || v === undefined) ? '' : String(v); }}
function successTag(cs) {{
  if (!cs) return '<span class="muted">—</span>';
  const ok = Number(cs.last_success) === 1;
  const st = cs.last_status || '';
  const label = ok ? '成功' : (st === 'empty' ? '未拿到(待核对)' : (st === 'fail' ? '失败(待核对)' : '否'));
  const cls = ok ? 'ok' : (st === 'empty' ? 'empty' : 'empty');
  return `<span class="tag ${{cls}}">${{label}}</span>`;
}}

function renderSummary() {{
  const m = META;
  document.getElementById('summary').innerHTML =
    `总计 <b>${{m.total}}</b> 支 · 真实 <b>${{m.ok}}</b> · 未拿到(待核对) <b>${{m.empty}}</b> ·
     最新真实日 <b>${{m.latest_real || '—'}}</b> · 生成 <b>${{m.generated_at}}</b>`;
  const sel = document.getElementById('date');
  m.dates.forEach(d => {{ const o = document.createElement('option'); o.value = d; o.textContent = d; sel.appendChild(o); }});
  document.getElementById('footer').textContent = '数据源: ' + m.db + (m.has_ff||m.has_vote||m.has_sr ? '' : '（仅 scores 表）');
}}

function setSort(k) {{ document.getElementById('sort').value = k; render(); }}

function filtered() {{
  const q = document.getElementById('q').value.trim().toLowerCase();
  const date = document.getElementById('date').value;
  const st = document.getElementById('status').value;
  const sort = document.getElementById('sort').value;
  let rows = DATA.filter(r => {{
    const s = r.s;
    if (date && (s.update_time || '') !== date) return false;
    if (st && (s.status || 'ok') !== st) return false;
    if (q) {{
      const hay = ((s.code||'') + ' ' + (s.name||'')).toLowerCase();
      if (!hay.includes(q)) return false;
    }}
    return true;
  }});
  const val = (r, k) => {{
    if (k === 'main_net') return Number(r.ff && r.ff.main_net) || 0;
    if (k === 'crawl_count') return Number(r.cs && r.cs.crawl_count) || 0;
    if (k === 'synthesis') return Number(r.s.synthesis) || 0;
    if (k === 'emc_total_score') return Number(r.emc && r.emc.emc_total_score) || 0;
    if (k === 'emc_rank') return Number(r.emc && r.emc.emc_rank) || 0;
    if (k === 'emc_org') return Number(r.emc && r.emc.emc_org_participate) || 0;
    if (k === 'emc_focus') return Number(r.emc && r.emc.emc_focus) || 0;
    if (k === 'emc_prime') return Number(r.emc && r.emc.emc_prime_cost) || 0;
    if (k === 'emv_pe') return Number(r.emv && r.emv.emv_pe_ttm) || 0;
    if (k === 'emp_rise1') return Number(r.emp && r.emp.emt_rise_1_prob) || 0;
    if (k === 'emp_rank') return Number(r.emp && r.emp.emt_rank_ratio) || 0;
    if (k === 'empart_change') return Number(r.empart && r.empart.emp_wish_change) || 0;
    if (k === 'empop_rank') return Number(r.empop && r.empop.emp_market_rank) || 0;
    return (r.s[k] || '').toString();
  }};
  rows.sort((a,b) => {{
    let x = val(a,sort), y = val(b,sort);
    if (typeof x === 'number' && typeof y === 'number') return y - x;
    return String(x).localeCompare(String(y), 'zh');
  }});
  return rows;
}}

function copyUrl(u) {{
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(u);
  }}
}}
function verifyLinksHTML(r) {{
  const links = r.verify_links || [];
  let rows = '';
  links.forEach(L => {{
    const u = L.url || '';
    rows += '<div class="src-row"><span class="muted">' + txt(L.source) + ' · ' + txt(L.label)
      + '</span><a href="' + u + '" target="_blank" rel="noopener">打开</a>'
      + '<button class="copy-btn" type="button" data-url="' + encodeURIComponent(u)
      + '" onclick="event.stopPropagation();copyUrl(decodeURIComponent(this.dataset.url))">复制链接</button>'
      + '<span class="muted" style="word-break:break-all">' + u + '</span></div>';
  }});
  return '<div class="empty-note">⚠ 本次爬取未拿到数据（待人工确认，不是确认无数据）。请打开下面源站核对是否真没有这只票/这段数据。未写入子表，下次仍会重试。</div>'
    + '<div class="src-links"><div class="panel"><h3>源站核对链接</h3>'
    + (rows || '<div class="muted">无可用链接</div>') + '</div></div>';
}}

function detailHTML(r) {{
  const s = r.s, ff = r.ff || {{}}, vt = r.vt || {{}}, sr = r.sr || {{}};
  if ((s.status || 'ok') !== 'ok') {{
    return verifyLinksHTML(r);
  }}
  let h = '<div class="box">';
  // 支撑/阻力
  if (META.has_sr) {{
    ['long','short'].forEach(cyc => {{
      const d = sr[cyc];
      h += '<div class="panel"><h3>支撑/阻力 · ' + (cyc==='long'?'长期':'短期') + '</h3>';
      if (!d) {{ h += '<div class="muted">无数据</div>'; }}
      else {{
        h += kv('支撑位', txt(d.support_level));
        h += kv('阻力位', txt(d.resistance_level));
        h += kv('智能评级', txt(d.rating_text));
        h += kv('评级等级', txt(d.rating_level));
        h += kv('行业', txt(d.industry_name));
        h += kv('排名', txt(d.rank_str));
        if (d.level_desc) h += kv('说明', txt(d.level_desc));
        if (d.bullish_events) h += kv('看多事件', txt(d.bullish_events));
        if (d.bearish_events) h += kv('看空事件', txt(d.bearish_events));
      }}
      h += '</div>';
    }});
  }}
  // 资金流向
  if (META.has_ff) {{
    h += '<div class="panel"><h3>资金流向（亿）</h3>';
    h += kv('超大单', num(ff.super_net));
    h += kv('大单', num(ff.large_net));
    h += kv('中单', num(ff.medium_net));
    h += kv('小单', num(ff.little_net));
    h += kv('主力净流入', num(ff.main_net));
    h += kv('超大占比', txt(ff.super_rate));
    h += kv('大单占比', txt(ff.large_rate));
    h += '</div>';
  }}
  // 投票
  if (META.has_vote) {{
    h += '<div class="panel"><h3>看涨/看跌投票</h3>';
    h += kv('总看涨', txt(vt.vote_up));
    h += kv('总看跌', txt(vt.vote_down));
    h += kv('看涨率', txt(vt.vote_up_rate));
    h += kv('看跌率', txt(vt.vote_down_rate));
    h += kv('本周看涨', txt(vt.week_up));
    h += kv('本周看跌', txt(vt.week_down));
    h += kv('本周看涨率', txt(vt.week_rate));
    h += '</div>';
  }}
    if (META.has_cs && r.cs) {
    h += '<div class="panel"><h3>抓取统计</h3>';
    h += kv('爬取次数', txt(r.cs.crawl_count));
    h += kv('最近是否成功', successTag(r.cs));
    if (r.cs.last_attempt) h += kv('最近尝试日', txt(r.cs.last_attempt));
    if (r.cs.updated_at) h += kv('最近更新', txt(r.cs.updated_at));
    h += '</div>';
  }
  if (META.has_em && r.emc) {
    h += '<div class="panel"><h3>东方财富 · 千股千评诊断</h3>';
    h += kv('综合得分', num(r.emc.emc_total_score));
    h += kv('全市场排名', txt(r.emc.emc_rank));
    h += kv('排名变动', txt(r.emc.emc_rank_up));
    h += kv('关注指数', num(r.emc.emc_focus));
    h += kv('机构参与度', num(r.emc.emc_org_participate));
    h += kv('控盘程度', ctrlDegree(r.emc.emc_org_participate));
    h += kv('机构参与比例', num(r.emc.emc_ratio));
    h += kv('主力成本(实时)', num(r.emc.emc_prime_cost));
    h += kv('主力成本(20日)', num(r.emc.emc_prime_cost_20d));
    h += kv('主力成本(60日)', num(r.emc.emc_prime_cost_60d));
    h += kv('主力净流入', num(r.emc.emc_prime_inflow));
    h += kv('超大单流入', num(r.emc.emc_superdeal_in));
    h += kv('超大单流出', num(r.emc.emc_superdeal_out));
    h += kv('大单流入', num(r.emc.emc_bigdeal_in));
    h += kv('大单流出', num(r.emc.emc_bigdeal_out));
    h += kv('买入超大单占比', num(r.emc.emc_buy_superdeal_ratio));
    h += kv('买入大单占比', num(r.emc.emc_buy_bigdeal_ratio));
    h += kv('数据日', txt(r.emc.trade_date));
    h += '</div>';
  }
  if (META.has_em && r.emv) {
    h += '<div class="panel"><h3>东方财富 · 基本面估值</h3>';
    h += kv('PE(TTM)', num(r.emv.emv_pe_ttm));
    h += kv('PE(LAR)', num(r.emv.emv_pe_lar));
    h += kv('PB(MRQ)', num(r.emv.emv_pb_mrq));
    h += kv('PCF_OCF(LAR)', num(r.emv.emv_pcf_ocf_lar));
    h += kv('PCF_OCF(TTM)', num(r.emv.emv_pcf_ocf_ttm));
    h += kv('PS(TTM)', num(r.emv.emv_ps_ttm));
    h += kv('PEG', num(r.emv.emv_peg));
    h += kv('总市值', num(r.emv.emv_total_market_cap));
    h += kv('流通市值', num(r.emv.emv_float_market_cap));
    h += kv('板块', txt(r.emv.emv_board));
    h += kv('数据日', txt(r.emv.trade_date));
    h += '</div>';
  }
  if (META.has_emt && r.emt) {
    h += '<div class="panel"><h3>东方财富 · 定性诊断</h3>';
    h += kv('趋势量能/支撑压力', txt(r.emt.emt_comment_txt));
    h += kv('消息面/资金面', txt(r.emt.emt_words_explain));
    h += kv('数据日', txt(r.emt.trade_date));
    h += '</div>';
  }
  if (META.has_emp && r.emp) {
    h += '<div class="panel"><h3>东方财富 · 诊断概率</h3>';
    h += kv('次日上涨概率', num(r.emp.emt_rise_1_prob) + ' %');
    h += kv('5日上涨概率', num(r.emp.emt_rise_5_prob) + ' %');
    h += kv('次日平均涨跌', num(r.emp.emt_avg_1_inc));
    h += kv('5日平均涨跌', num(r.emp.emt_avg_5_inc));
    h += kv('打败比例', num(r.emp.emt_rank_ratio) + ' %');
    h += kv('样本数(次日)', txt(r.emp.emt_all_count_1));
    h += kv('样本数(5日)', txt(r.emp.emt_all_count_5));
    h += kv('数据日', txt(r.emp.trade_date));
    h += '</div>';
    if (r.empart) {{
      h += '<div class="panel"><h3>东方财富 · 参与意愿</h3>';
      h += kv('当日参与意愿值', num(r.empart.emp_wish));
      h += kv('五日平均参与意愿值', num(r.empart.emp_wish_5d));
      h += kv('当日参与意愿变化%', num(r.empart.emp_wish_change));
      h += kv('五日参与意愿变化%', num(r.empart.emp_wish_5d_change));
      h += kv('数据日', txt(r.empart.trade_date));
      h += '</div>';
    }}
    if (r.empop) {{
      h += '<div class="panel"><h3>东方财富 · 市场排名</h3>';
      h += kv('综合市场排名', txt(r.empop.emp_market_rank) + ' / ' + txt(r.empop.emp_market_num));
      h += kv('行业排名', txt(r.empop.emp_industry_rank));
      if (r.empop.emp_rank_change !== undefined) {{
        const ch = r.empop.emp_rank_change;
        const tag = ch < 0 ? '上升 ' + (-ch) + ' 名' : (ch > 0 ? '下降 ' + ch + ' 名' : '持平');
        h += kv('较昨日', tag);
      }}
      h += kv('综合得分变化率%', num(r.empop.emp_change_rate));
      h += kv('全市场股票数', txt(r.empop.emp_market_stock_num));
      h += kv('关注指数', num(r.empop.emp_focus_index));
      h += kv('关注排名', txt(r.empop.emp_focus_rank) + ' / ' + txt(r.empop.emp_focus_total));
      h += kv('数据日', txt(r.empop.trade_date));
      h += '</div>';
    }}
  }
  h += '</div>';
  return h;
}
function kv(k,v) { return '<div class="kv"><span>'+k+'</span><span>'+v+'</span></div>'; }
function ctrlDegree(op) { if(op===null||op===undefined||op==='') return '-'; const n=Number(op); if(isNaN(n)) return '-'; if(n<0.3) return '低度控盘'; if(n<0.7) return '中度控盘'; return '高度控盘'; }

let openCode = null;
function render() {{
  const rows = filtered();
  const tb = document.getElementById('rows');
  tb.innerHTML = '';
  rows.forEach((r, i) => {{
    const s = r.s;
    const status = s.status || 'ok';
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td>${{txt(s.code)}}</td>` +
      `<td>${{txt(s.name)}}</td>` +
      `<td>${{txt(s.update_time)}}</td>` +
      `<td>${{txt(s.crawl_date)}}</td>` +
      `<td><span class="tag ${{status}}">${{status==='ok'?'真实':'未拿到(待核对)'}}</span></td>` +
      (META.has_cs ? `<td class="num">${{txt(r.cs && r.cs.crawl_count)}}</td>` +
        `<td>${{successTag(r.cs)}}</td>` : '') +
      (META.has_em ? `<td class="num">${{num(r.emc && r.emc.emc_total_score)}}</td>` +
        `<td class="num">${{txt(r.emc && r.emc.emc_rank)}}</td>` +
        `<td class="num">${{num(r.emc && r.emc.emc_org_participate)}}</td>` +
        `<td class="num">${{num(r.emc && r.emc.emc_focus)}}</td>` +
        `<td class="num">${{num(r.emc && r.emc.emc_prime_cost)}}</td>` +
        `<td class="num">${{num(r.emv && r.emv.emv_pe_ttm)}}</td>` : '') +
      (META.has_emp ? `<td class="num">${{num(r.emp && r.emp.emt_rise_1_prob)}}</td>` +
        `<td class="num">${{num(r.emp && r.emp.emt_rank_ratio)}}</td>` : '') +
      (META.has_empart ? `<td class="num">${{num(r.empart && r.empart.emp_wish_change)}}</td>` : '') +
      (META.has_empop ? `<td class="num">${{txt(r.empop && r.empop.emp_market_rank)}}</td>` : '') +
      `<td class="num">${{num(s.synthesis)}}</td>` +
      `<td class="num">${{num(s.technology)}}</td>` +
      `<td class="num">${{num(s.capital)}}</td>` +
      `<td class="num">${{num(s.market)}}</td>` +
      `<td class="num">${{num(s.finance)}}</td>` +
      `<td class="num">${{num(r.ff && r.ff.main_net)}}</td>` +
      `<td class="num">${{txt(r.vt && r.vt.vote_up)}}/${{txt(r.vt && r.vt.vote_down)}}</td>`;
    const key = s.code + '|' + s.update_time;
    tr.onclick = () => {{
      const next = openCode === key ? null : key;
      openCode = next;
      render();
    }};
    tb.appendChild(tr);
    if (openCode === key) {{
      const dr = document.createElement('tr');
      dr.className = 'detail';
      dr.innerHTML = '<td colspan="24">' + detailHTML(r) + '</td>';
      tb.appendChild(dr);
    }}
  }});
  document.getElementById('count').textContent = '显示 ' + rows.length + ' / ' + DATA.length + ' 支';
}}

renderSummary();
render();
</script>
</body>
</html>
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="百度财经 AI 解析结果查看器（生成自包含 HTML）")
    ap.add_argument("--db", default="market_data.db", help="SQLite 路径（默认 ./market_data.db）")
    ap.add_argument("--out", default="view_report.html", help="输出 HTML 路径（默认 ./view_report.html）")
    ap.add_argument("--limit", type=int, default=None, help="仅前 N 支（调试大库用）")
    args = ap.parse_args(argv)

    res = load(args.db, limit=args.limit)
    if not res.get("ok"):
        print("错误: " + res.get("error", "未知错误"), file=sys.stderr)
        return 1

    meta_json = json.dumps(res["meta"], ensure_ascii=False)
    data_json = json.dumps(res["records"], ensure_ascii=False)
    html = HTML_SKELETON.replace("{META}", meta_json).replace("{DATA}", data_json)

    out_path = args.out
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    m = res["meta"]
    print(f"已生成: {os.path.abspath(out_path)}")
    print(f"总计 {m['total']} 支 | 真实 {m['ok']} | 未拿到(待核对) {m['empty']} | 最新真实日 {m['latest_real']}")
    print(f"大小: {os.path.getsize(out_path)/1024:.0f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
