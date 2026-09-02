#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 market_data.db 生成一个自包含的交互式 SQLite 查看器(单文件 HTML)。

用法:
    python make_viewer.py --db market_data.db --out market_data_viewer.html

特性:
    - 无需任何第三方库,浏览器直接打开
    - 顶部 Tab 切换 5 张表(stocks/scores/support_resistance/fund_flow/vote)
    - 每表支持关键字搜索 + 点击列头排序
    - 底部 3 个图表:综合评分 Top20、主力资金净流入 Top20、本周看涨占比 Top20
    - 数据随时可重跑本脚本刷新(数据库在回填时也能看当前快照)
"""
import argparse
import json
import sqlite3

LABELS = {
    "stocks": "股票代码表",
    "scores": "五维评分",
    "support_resistance": "支撑/阻力位",
    "fund_flow": "个股资金流向",
    "vote": "股评投票",
}

CHART_TITLES = {
    "scores": "综合评分 Top20",
    "fund_flow": "主力资金净流入 Top20(正=流入/负=流出)",
    "vote": "本周看涨占比 Top20(%)",
}


def build(db_path: str, out_path: str):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    payload = {}
    for t in tables:
        cols = [d[1] for d in cur.execute(f"PRAGMA table_info({t})")]
        rows = [dict(zip(cols, r)) for r in cur.execute(f"SELECT * FROM {t}")]
        payload[t] = {"columns": cols, "rows": rows, "label": LABELS.get(t, t)}

    con.close()

    data_json = json.dumps(payload, ensure_ascii=False, default=str)

    html = TEMPLATE.replace("__DATA__", data_json).replace(
        "__GEN__", db_path)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"written: {out_path}  (tables: {', '.join(tables)})")


TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SQLite 查看器 · market_data</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root{ --bg:#0f1419; --panel:#1a2129; --line:#2a3038; --fg:#e6edf3; --muted:#8b98a5;
         --accent:#4f9cf9; --up:#ff5b5b; --down:#26a69a; }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 -apple-system,"Segoe UI",Roboto,"Microsoft YaHei",sans-serif}
  header{padding:14px 18px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
  h1{font-size:16px;margin:0;font-weight:600}
  .meta{color:var(--muted);font-size:12px}
  .tabs{display:flex;gap:6px;padding:10px 18px 0;flex-wrap:wrap}
  .tab{padding:7px 14px;border:1px solid var(--line);border-bottom:none;border-radius:8px 8px 0 0;
       background:var(--panel);color:var(--muted);cursor:pointer;user-select:none}
  .tab.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .panel{border:1px solid var(--line);border-radius:0 8px 8px 8px;margin:0 18px 14px;background:var(--panel);overflow:hidden}
  .toolbar{padding:10px 12px;display:flex;gap:10px;align-items:center;border-bottom:1px solid var(--line)}
  .toolbar input{flex:1;max-width:360px;padding:7px 10px;background:var(--bg);border:1px solid var(--line);
                 border-radius:6px;color:var(--fg);font-size:13px}
  .toolbar .count{color:var(--muted);font-size:12px}
  .scroller{overflow:auto;max-height:420px}
  table{border-collapse:collapse;width:100%;font-size:13px}
  th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
  th:first-child,td:first-child,th.l,td.l{text-align:left}
  thead th{position:sticky;top:0;background:#222b35;cursor:pointer;user-select:none;z-index:1}
  thead th:hover{color:var(--accent)}
  tbody tr:hover{background:#202830}
  .charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px;padding:6px 18px 20px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px}
  .card h3{margin:0 0 10px;font-size:13px;color:var(--muted);font-weight:500}
  canvas{max-height:240px}
  .num-up{color:var(--up)} .num-down{color:var(--down)}
</style>
</head>
<body>
<header>
  <h1>SQLite 查看器 · <span id="dbname"></span></h1>
  <div class="meta" id="meta"></div>
</header>
<div class="tabs" id="tabs"></div>
<div id="panels"></div>
<div class="charts" id="charts"></div>

<script>
const DB = __DATA__;
const tables = Object.keys(DB);
document.getElementById('dbname').textContent = "__GEN__";
document.getElementById('meta').textContent =
  "共 " + tables.length + " 张表 · 生成于浏览器打开时";

// ---- 标签页 ----
const tabsEl = document.getElementById('tabs');
const panelsEl = document.getElementById('panels');
let activeTab = tables[0];

tables.forEach(t => {
  const tab = document.createElement('div');
  tab.className = 'tab' + (t===activeTab?' active':'');
  tab.textContent = DB[t].label + " (" + DB[t].rows.length + ")";
  tab.onclick = () => { activeTab = t; render(); };
  tab.dataset.t = t;
  tabsEl.appendChild(tab);
});

function colorize(v){
  if(typeof v === 'number' || (typeof v==='string' && /^-?\d+(\.\d+)?$/.test(v))){
    const n = parseFloat(v);
    if(n>0) return 'num-up'; if(n<0) return 'num-down';
  }
  return '';
}

function render(){
  [...tabsEl.children].forEach(c=>c.classList.toggle('active', c.dataset.t===activeTab));
  const t = activeTab, info = DB[t];
  const cols = info.columns, rows = info.rows;
  let html = `<div class="panel"><div class="toolbar">
      <input placeholder="搜索 ${info.label}…" id="q" oninput="filter()">
      <span class="count" id="cnt"></span></div>
      <div class="scroller"><table><thead><tr>`;
  cols.forEach((c,i)=>{
    const left = (c==='code'||c==='name'||c==='cycle'||c==='industry_name') ? ' l':'';
    html += `<th class="${left}" onclick="sortBy(${i})">${c} ⇅</th>`;
  });
  html += `</tr></thead><tbody id="tbody"></tbody></table></div></div>`;
  panelsEl.innerHTML = html;
  window._cols = cols; window._rows = rows;
  filter();
}

let sortDir = 1, sortIdx = -1;
function sortBy(i){
  if(sortIdx===i) sortDir*=-1; else {sortIdx=i; sortDir=1;}
  window._rows = [...window._rows].sort((a,b)=>{
    let x=a[window._cols[i]], y=b[window._cols[i]];
    const nx=parseFloat(x), ny=parseFloat(y);
    if(!isNaN(nx)&&!isNaN(ny)) return (nx-ny)*sortDir;
    return String(x).localeCompare(String(y),'zh')*sortDir;
  });
  filter();
}
function filter(){
  const q = (document.getElementById('q')?.value||'').toLowerCase();
  const cols = window._cols, rows = window._rows;
  const tb = document.getElementById('tbody');
  const view = rows.filter(r=>cols.some(c=>String(r[c]).toLowerCase().includes(q)));
  let h='';
  view.slice(0,2000).forEach(r=>{
    h+='<tr>';
    cols.forEach(c=>{
      const left=(c==='code'||c==='name'||c==='cycle'||c==='industry_name')?' l':'';
      const cls=colorize(r[c]);
      h+=`<td class="${left} ${cls}">${r[c]===null?'<span style="color:#666">null</span>':r[c]}</td>`;
    });
    h+='</tr>';
  });
  tb.innerHTML=h;
  document.getElementById('cnt').textContent =
    `显示 ${view.length}${view.length>2000?' (前2000)':''} / 共 ${rows.length}`;
}

// ---- 图表 ----
function chartCard(title, labels, values, color){
  const wrap = document.createElement('div'); wrap.className='card';
  wrap.innerHTML = `<h3>${title}</h3><canvas></canvas>`;
  document.getElementById('charts').appendChild(wrap);
  new Chart(wrap.querySelector('canvas'), {
    type:'bar',
    data:{ labels, datasets:[{data:values, backgroundColor:color, borderRadius:3}]},
    options:{ plugins:{legend:{display:false}},
      scales:{ x:{ticks:{color:'#8b98a5',maxRotation:60,minRotation:45,font:{size:10}},
                     grid:{color:'#2a3038'}},
               y:{ticks:{color:'#8b98a5'},grid:{color:'#2a3038'}} } }
  });
}
function buildCharts(){
  const chartsEl = document.getElementById('charts'); chartsEl.innerHTML='';
  const add=(title,rows,labelKey,valKey,color)=>{
    if(!rows.length) return;
    const s=[...rows].sort((a,b)=>parseFloat(b[valKey])-parseFloat(a[valKey])).slice(0,20);
    chartCard(title, s.map(r=>r[labelKey]), s.map(r=>parseFloat(r[valKey])), color);
  };
  add(CHART_TITLES.scores, DB.scores?.rows||[], 'code', 'synthesis', '#4f9cf9');
  add(CHART_TITLES.fund_flow, DB.fund_flow?.rows||[], 'code', 'main_net', '#f7b955');
  if(DB.vote && DB.vote.rows.length){
    const wk = DB.vote.rows.filter(r=>r.week_up!=null && r.week_down!=null && (parseFloat(r.week_up)+parseFloat(r.week_down))>0)
      .map(r=>({code:r.code, rate: 100*parseFloat(r.week_up)/(parseFloat(r.week_up)+parseFloat(r.week_down))}));
    add(CHART_TITLES.vote, wk, 'code', 'rate', '#26a69a');
  }
}
render(); buildCharts();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="market_data.db")
    ap.add_argument("--out", default="market_data_viewer.html")
    a = ap.parse_args()
    build(a.db, a.out)
