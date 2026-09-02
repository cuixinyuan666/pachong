#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度财经 · AI 技术分析 全市场数据爬虫
====================================

目标: 用 easy-tdx 拉取全部 A 股代码，逐支抓取百度财经 AI 技术分析数据，
      写入同一份 SQLite（按 交易日 + 代码 追加每日快照）。

抓取的数据维度（均为百度 AI 自动生成的分析结论）:
    /vapi/v1/analysis        五维评分（综合/技术/资金/市场/财务）
    /sapi/v1/get_analyse     支撑位 / 阻力位 / 智能评级（long / short 周期）
    /vapi/v1/fundflow        资金流向（超大/大/中/小单 净流向，日级）
    /vapi/v1/stockvoterecords 看涨/看跌投票（本周为真实数据，其余周期服务端固定占位）

设计要点
--------
  * 直接调用 JSON 接口（React SPA 的真实数据源），不渲染页面。
  * 合理的请求头（UA / Referer / X-Requested-With / Accept）。
  * 指数退避重试 + 429 限流等待（Retry-After）。
  * 频率限制器（最小间隔 + 每分钟上限）。
  * 内置 A 股交易日历（周末 + 2026 法定假日），非交易日自动跳过。
  * 断点续跑：已存在 (交易日, 代码) 且状态为真实(ok) 的记录默认跳过；
    未拿到(empty，本次百度没返回分析) 不跳过、可重试；empty ≠ 源站确认无数据，
    日志会给出源站链接供人工打开核对。
  * 三层日期语义，杜绝"抓取日期 ≠ 数据真实日期"：
      - crawl_date   = 爬虫实际运行的日历日（新增列，用于审计"哪天抓的"）
      - trade_date   = 批次标签（周末/假日回退到上一交易日，与百度返回日一致）
      - update_time  = 百度返回的真实分析日（权威，主键组成部分，去重依据）
  * 仅依赖标准库；easy-tdx 仅用于拉取代码清单（带本地缓存）。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta, date

try:
    from http.client import HTTPException, IncompleteRead
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlparse, parse_qs, unquote, urlencode, quote
    from urllib.request import Request, urlopen
except ImportError:  # pragma: no cover
    pass

# 公共层：与东方财富爬虫共享的交易日历 / 限流 / HTTP / 代码清单 / 续跑逻辑
from crawler_common import (
    HOLIDAYS, is_trading_day, today_cst, _now_cst_str, resolve_trade_date,
    RateLimiter, ForbiddenError, fetch_json, get_a_share_codes,
    bump_crawl_stats, skip_recent_ok, ensure_crawl_stats_source,
    format_unconfirmed_empty_msg,
)

# --------------------------------------------------------------------------- #
# 配置
# --------------------------------------------------------------------------- #
API_HOST = "https://finance.pae.baidu.com"
ANALYSIS_PATH = "/vapi/v1/analysis"
# K线技术事件分析接口：支撑位/阻力位/智能评级（分 long/short 周期）
KLINE_ANALYSE_PATH = "/sapi/v1/get_analyse"
# 资金流向接口（参数用 finance_type 下划线，而非 financeType）
FUND_FLOW_PATH = "/vapi/v1/fundflow"
# 看涨/看跌投票接口
VOTE_PATH = "/vapi/v1/stockvoterecords"
PAGE_HOST = "https://finance.baidu.com"

# 一组较真实的桌面浏览器 UA，随机抽取以降低被识别为脚本的概率
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

logger = logging.getLogger("baidu_ai_crawler")

# --------------------------------------------------------------------------- #
# 从 Web 界面传入的 Cookie（必须在 logger 定义之后）
# --------------------------------------------------------------------------- #
BAIDU_COOKIE_DICT = os.environ.get("_BAIDU_COOKIE_DICT")
USER_COOKIES_FROM_WEB = {}
if BAIDU_COOKIE_DICT:
    try:
        USER_COOKIES_FROM_WEB = json.loads(BAIDU_COOKIE_DICT)
        logger.info("已接收 %s 个 Cookie 从 Web 界面", len(USER_COOKIES_FROM_WEB))
    except Exception:
        USER_COOKIES_FROM_WEB = {}


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #
def build_headers(referer: str) -> dict:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Origin": PAGE_HOST,
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }
    
    # 添加从 Web 界面传入的 Cookie
    if USER_COOKIES_FROM_WEB:
        cookie_str = "; ".join([f"{k}={v}" for k, v in USER_COOKIES_FROM_WEB.items()])
        headers["Cookie"] = cookie_str
        logger.debug(f"已添加 {len(USER_COOKIES_FROM_WEB)} 个 Cookie")
    
    return headers


def build_api_url(stock: dict) -> str:
    params = {"code": stock["code"], "market": stock["market"],
              "financeType": stock["financeType"]}
    return f"{API_HOST}{ANALYSIS_PATH}?{urlencode(params)}"


def build_kline_url(stock: dict, cycle: str) -> str:
    params = {"code": stock["code"], "market": stock["market"],
              "financeType": stock["financeType"], "cycle": cycle}
    return f"{API_HOST}{KLINE_ANALYSE_PATH}?{urlencode(params)}"


def build_page_url(stock: dict) -> str:
    return f"{PAGE_HOST}/ai-tech-analysi/{stock['financeType']}/{stock['market']}-{stock['code']}"


def build_tab_page_url(stock: dict, tab: str) -> str:
    """构造个股详情页某 Tab 的 URL（用作接口 Referer，符合反爬要求）。

    注意: tab 为中文，必须百分号编码，否则 urllib 在写入请求头时会因
    latin-1 无法编码中文而报错。
    """
    return f"{PAGE_HOST}/stock/{stock['market']}-{stock['code']}?mainTab={quote(tab)}"


def build_fundflow_url(stock: dict) -> str:
    params = {"finance_type": stock.get("financeType", "stock"),
              "code": stock["code"], "market": stock["market"], "fund_flow_type": ""}
    return f"{API_HOST}{FUND_FLOW_PATH}?{urlencode(params)}"


def build_vote_url(stock: dict) -> str:
    params = {"code": stock["code"], "market": stock["market"],
              "finance_type": stock.get("financeType", "stock")}
    return f"{API_HOST}{VOTE_PATH}?{urlencode(params)}"


import random  # noqa: E402  (放在文件中部仅为可读性，实际等同顶部导入)




# --------------------------------------------------------------------------- #
# 解析
# --------------------------------------------------------------------------- #
def parse_analysis(result: dict, stock: dict) -> dict:
    syn = result.get("synthesisScore", {}) or {}
    technology = result.get("technologyScore", {}) or {}
    capital = result.get("capitalScore", {}) or {}
    market = result.get("marketScore", {}) or {}
    finance = result.get("financeScore", {}) or {}
    name = syn.get("stockName") or stock.get("name") or technology.get("stockName")
    if not name and (technology.get("increase") or {}).get("items"):
        name = technology["increase"]["items"][0].get("text")
    return {
        "stock": {"name": name, "code": stock["code"], "market": stock["market"],
                  "financeType": stock["financeType"]},
        "synthesis": {"rating": syn.get("rating"), "title": syn.get("title"),
                      "desc": syn.get("desc"), "updateTime": syn.get("updateTime"),
                      "industryRanking": syn.get("industryRanking"),
                      "industryName": syn.get("firstIndustryName"),
                      "marketRanking": syn.get("marketRanking")},
        "technology": {"title": technology.get("title"), "score": technology.get("score"),
                       "desc": technology.get("desc"), "updateTime": technology.get("updateTime")},
        "capital": {"title": capital.get("title"), "score": capital.get("score"),
                    "desc": capital.get("desc"), "updateTime": capital.get("updateTime")},
        "market": {"title": market.get("title"), "score": market.get("score"),
                   "desc": market.get("desc"), "updateTime": market.get("updateTime")},
        "finance": {"title": finance.get("title"), "score": finance.get("score"),
                    "desc": finance.get("desc"), "updateTime": finance.get("updateTime")},
        "is_new": result.get("isNew"),
    }


def parse_kline_analyse(result: dict, cycle: str) -> dict:
    li = result.get("levelInfo", {}) or {}
    sl = result.get("stopLoss", {}) or {}
    rt = result.get("rating", {}) or {}
    rk = result.get("rank", {}) or {}
    return {
        "cycle": cycle,
        "cycle_text": "长期" if cycle == "long" else "短期",
        "support_level": li.get("supportLevel"),
        "resistance_level": li.get("resistanceLevel"),
        "level_desc": li.get("desc"),
        "rating_text": rt.get("text"),
        "rating_level": rt.get("level"),
        "rating_status": rt.get("status"),
        "bullish_events": rt.get("bullish"),
        "bearish_events": rt.get("bearish"),
        "rank_str": (rk.get("name") or "") + " " + (rk.get("rankvalue") or ""),
        "industry_name": rk.get("industryName"),
    }


def parse_fundflow_daily(content: dict) -> dict:
    """从 fundflow.content.fundFlowSpread.result 提取个股日级资金净流向。

    注意: fundFlowBlock.result 为行业级(板块)资金流，todayMainFlow.mainNetIn
    为多支股票共享的聚合值，二者均非个股数据；个股自身的超大/大/中/小单
    净流向位于 fundFlowSpread.result 的 superGrp/largeGrp/mediumGrp/littleGrp。
    返回: 四档净流入(带符号) 与 买入占比(%)，以及主力净流入(超大+大单)。
    """
    fs = (content or {}).get("fundFlowSpread") or {}
    res = fs.get("result") or {}
    if not res:
        return {}

    def _grp(key):
        return res.get(key) or {}

    def _num(v):
        if v is None:
            return None
        try:
            return float(str(v).replace("+", "").replace("%", "").strip())
        except Exception:
            return None

    super_g = _grp("superGrp")
    large_g = _grp("largeGrp")
    medium_g = _grp("mediumGrp")
    little_g = _grp("littleGrp")
    out = {
        "super_net": _num(super_g.get("netTurnover")),
        "large_net": _num(large_g.get("netTurnover")),
        "medium_net": _num(medium_g.get("netTurnover")),
        "little_net": _num(little_g.get("netTurnover")),
        "super_rate": super_g.get("turnoverInRate"),
        "large_rate": large_g.get("turnoverInRate"),
        "medium_rate": medium_g.get("turnoverInRate"),
        "little_rate": little_g.get("turnoverInRate"),
    }
    sn = out["super_net"] or 0.0
    ln = out["large_net"] or 0.0
    out["main_net"] = round(sn + ln, 4)
    return out


def parse_vote(payload: dict) -> dict:
    """解析 /vapi/v1/stockvoterecords（看涨/看跌投票）。

    说明: 今日/本月/今年 分周期投票服务端固定返回 0/0/50%（该股票无对应
    周期投票记录），仅「本周」为真实数据。这是服务端行为，并非抓取错误。
    """
    records = payload.get("voteRecords") or {}
    periods = {v.get("title"): v for v in (records.get("voteRes") or [])}
    week = periods.get("本周") or {}
    return {
        "vote_up": payload.get("voteUp"),
        "vote_down": payload.get("voteDown"),
        "total_num": payload.get("totalNum"),
        "vote_up_rate": payload.get("voteUpRate"),
        "vote_down_rate": payload.get("voteDownRate"),
        "week_up": week.get("voteUp"),
        "week_down": week.get("voteDown"),
        "week_rate": week.get("voteUpRate"),
    }


# --------------------------------------------------------------------------- #
# 单次抓取（五维评分 + 支撑阻力 + 资金流向 + 投票）
# --------------------------------------------------------------------------- #
def crawl_stock(stock, limiter, max_retries=3, timeout=15,
                include_raw=False, skip_kline=False,
                with_fundflow=True, with_vote=True) -> dict:
    page_url = build_page_url(stock)
    api_url = build_api_url(stock)
    headers = build_headers(referer=page_url)
    logger.info("抓取 %s (%s)", stock.get("name") or stock["code"], stock["code"])
    data = fetch_json(api_url, headers, limiter, max_retries, timeout)
    result = data.get("Result", {}) or {}
    parsed = parse_analysis(result, stock)
    tz = timezone(timedelta(hours=8))
    out = {
        "meta": {
            "source_page": page_url,
            "api_endpoint": api_url,
            "fetched_at": datetime.now(tz).isoformat(timespec="seconds"),
            "query_id": data.get("QueryID"),
        },
        "ai_analysis": parsed,
    }
    if include_raw:
        out["raw_result"] = result

    # 支撑位 / 阻力位 / 智能评级（分长/短周期）
    if not skip_kline:
        sr = {}
        for cycle in ("long", "short"):
            try:
                kurl = build_kline_url(stock, cycle)
                kdata = fetch_json(kurl, headers, limiter, max_retries, timeout)
                sr[cycle] = parse_kline_analyse(kdata.get("Result", {}) or {}, cycle)
            except Exception as e:
                logger.warning("支撑阻力(%s)失败: %s", cycle, e)
                sr[cycle] = None
        if any(sr.values()):
            out["support_resistance"] = sr

    # 资金流向（日级）
    if with_fundflow:
        try:
            furl = build_fundflow_url(stock)
            fheaders = build_headers(referer=build_tab_page_url(stock, "资金"))
            fdata = fetch_json(furl, fheaders, limiter, max_retries, timeout)
            froot = fdata.get("Result", {}) or {}
            fcontent = (froot.get("Result") or froot).get("content") or {}
            out["fund_flow"] = parse_fundflow_daily(fcontent)
        except Exception as e:
            logger.warning("资金流向失败: %s", e)
            out["fund_flow"] = None

    # 看涨/看跌投票
    if with_vote:
        try:
            vurl = build_vote_url(stock)
            vheaders = build_headers(referer=build_tab_page_url(stock, "股评"))
            vdata = fetch_json(vurl, vheaders, limiter, max_retries, timeout)
            out["vote"] = parse_vote(vdata.get("Result", {}) or {})
        except Exception as e:
            logger.warning("投票失败: %s", e)
            out["vote"] = None
    return out


# --------------------------------------------------------------------------- #
# 全市场代码清单（easy-tdx，带本地缓存）
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# SQLite 时序落盘（始终同一份数据库，按 交易日+代码 追加快照）
# --------------------------------------------------------------------------- #
SCHEMA = """
CREATE TABLE IF NOT EXISTS stocks (
    code TEXT PRIMARY KEY, name TEXT, market TEXT
);
CREATE TABLE IF NOT EXISTS scores (
    trade_date TEXT, code TEXT, name TEXT,
    synthesis TEXT, technology TEXT, capital TEXT, market TEXT, finance TEXT,
    is_new TEXT, update_time TEXT,
    crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code)
);
CREATE TABLE IF NOT EXISTS support_resistance (
    trade_date TEXT, code TEXT, cycle TEXT, support_level TEXT, resistance_level TEXT,
    level_desc TEXT, rating_text TEXT, rating_level TEXT, rating_status TEXT,
    bullish_events TEXT, bearish_events TEXT, rank_str TEXT, industry_name TEXT,
    update_time TEXT, crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code, cycle)
);
CREATE TABLE IF NOT EXISTS fund_flow (
    trade_date TEXT, code TEXT, super_net REAL, large_net REAL, medium_net REAL,
    little_net REAL, super_rate TEXT, large_rate TEXT, medium_rate TEXT,
    little_rate TEXT, main_net REAL, update_time TEXT, crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code)
);
CREATE TABLE IF NOT EXISTS vote (
    trade_date TEXT, code TEXT, vote_up TEXT, vote_down TEXT, total_num TEXT,
    vote_up_rate TEXT, vote_down_rate TEXT, week_up TEXT, week_down TEXT, week_rate TEXT,
    update_time TEXT, crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code)
);
-- 逐股累计抓取统计（按 code 独立建表，不受 update_time 变动影响）：
--   crawl_count  累计被尝试抓取的次数（成功/未拿到/失败都 +1；续跑跳过不计入）
--   last_success 最近一次尝试是否成功拿到真实分析（1=成功 0=未拿到或失败）
--   last_status  'ok' / 'empty'(本次未拿到,待核对,不是确认无数据) / 'fail'
--   last_attempt 最近一次尝试的日历日（crawl_date）
--   updated_at   最近一次更新的 CST 时间戳
CREATE TABLE IF NOT EXISTS crawl_stats (
    code TEXT NOT NULL,
    crawl_count INTEGER NOT NULL DEFAULT 0,
    last_success INTEGER NOT NULL DEFAULT 0,
    last_status TEXT,
    last_attempt TEXT,
    updated_at TEXT,
    source TEXT NOT NULL DEFAULT 'baidu',
    PRIMARY KEY(code, source)
);
"""

# 四表除主键列外，还需 crawl_date / status 两列（历史库可能缺，故落库前自愈补齐）。
_EXTRA_COLS = ("crawl_date", "status")


def _ensure_extra_columns(conn) -> None:
    """幂等地为四表补齐 crawl_date / status 列（仅当缺失时 ALTER）。

    这样无论是全新库（SCHEMA 已含）还是历史库（SCHEMA 不报错但缺列），
    爬虫都能直接写入新列，无需额外的手动迁移步骤。
    """
    for tbl in ("scores", "support_resistance", "fund_flow", "vote"):
        existing = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()}
        for col in _EXTRA_COLS:
            if col not in existing:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} TEXT")


def save_snapshot(db_path, trade_date, stock, out, conn=None) -> None:
    """将单支股票的抓取结果 UPSERT 入时序库；主键为 (update_time, code)
    （support_resistance 含 cycle），按"真实分析日"去重——百度接口只返回当前单日分析，
    同 update_time 跨批(如 07-20+07-21)自动合并，不会产生 07-19 类伪交易日。

    三层日期 + 未拿到标记:
      - crawl_date  存"实际抓取日历日"(today_cst)
      - update_time 存百度返回的真实分析日；若百度未返回分析(updateTime 为空)，
        则该股为"未拿到"(empty)：仅写 scores 壳行(以 trade_date 作占位主键防止堆积)，
        并标记 status='empty'（待人工打开源站核对，不是确认无数据），
        子表不写，等待下次重试补齐。
      - status: 'ok' = 真实数据已落库；'empty' = 本次爬取未拿到(可重试/待核对)。
    """
    owned = conn is None
    if owned:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=15000")  # 与 Rust 端一致；遇并发锁等待而非报错
    c = conn.cursor()
    c.executescript(SCHEMA)
    _ensure_extra_columns(conn)
    code = stock["code"]
    name = (out.get("ai_analysis", {}).get("stock", {}).get("name")
            or stock.get("name") or "")
    c.execute("INSERT OR REPLACE INTO stocks VALUES (?,?,?)", (code, name, stock["market"]))

    a = out.get("ai_analysis", {})
    # 快照级更新时间：借 scores 的综合维度更新时间作为本股票同次抓取的统一时间
    update_time = a.get("synthesis", {}).get("updateTime")
    has_analysis = bool(update_time)
    crawl_date = today_cst().isoformat()
    status = "ok" if has_analysis else "empty"
    # 未拿到(update_time 为空)：用 trade_date 作占位主键，避免 NULL 主键在 (update_time,code)
    # 下被 SQLite 视为互异而无限堆积；status='empty' 只表示本次没拿到，不是源站确认无数据。
    pk_update_time = update_time if has_analysis else trade_date
    c.execute(
        "INSERT OR REPLACE INTO scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (trade_date, code, name,
         a.get("synthesis", {}).get("rating"), a.get("technology", {}).get("score"),
         a.get("capital", {}).get("score"), a.get("market", {}).get("score"),
         a.get("finance", {}).get("score"), a.get("is_new"),
         pk_update_time, crawl_date, status),
    )
    # 累计抓取统计：本次是一次实际尝试（无论 ok 还是 empty 都计）；复用同一连接避免多连接锁竞争
    bump_crawl_stats(db_path, code, success=has_analysis, status=status, source="baidu", conn=conn)

    # 未拿到：不写子表，直接返回（占位 scores 已落库；下次重试；人工用源站链接核对）
    if not has_analysis:
        conn.commit()
        if owned:
            conn.close()
        return

    for cyc, d in (out.get("support_resistance") or {}).items():
        if not d:
            continue
        c.execute(
            "INSERT OR REPLACE INTO support_resistance VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (trade_date, code, d.get("cycle"), d.get("support_level"),
             d.get("resistance_level"), d.get("level_desc"), d.get("rating_text"),
             d.get("rating_level"), d.get("rating_status"), d.get("bullish_events"),
             d.get("bearish_events"), d.get("rank_str"), d.get("industry_name"),
             update_time, crawl_date, status),
        )

    ff = out.get("fund_flow") or {}
    if ff:
        c.execute(
            "INSERT OR REPLACE INTO fund_flow VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (trade_date, code, ff.get("super_net"), ff.get("large_net"),
             ff.get("medium_net"), ff.get("little_net"), ff.get("super_rate"),
             ff.get("large_rate"), ff.get("medium_rate"), ff.get("little_rate"),
             ff.get("main_net"), update_time, crawl_date, status),
        )

    vt = out.get("vote") or {}
    if vt:
        c.execute(
            "INSERT OR REPLACE INTO vote VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (trade_date, code, vt.get("vote_up"), vt.get("vote_down"),
             vt.get("total_num"), vt.get("vote_up_rate"), vt.get("vote_down_rate"),
             vt.get("week_up"), vt.get("week_down"), vt.get("week_rate"),
             update_time, crawl_date, status),
        )
    conn.commit()
    if owned:
        conn.close()


def crawl_market(db_path, trade_date, limiter, codes, skip_existing=True,
                fresh_days: int = 2, progress_log=False,
                 max_retries=3, timeout=15, limit=None) -> dict:
    """全市场抓取：逐支写入同一份时序数据库。返回统计信息。

    使用单一复用连接（shared_conn）贯穿 SCHEMA 初始化 → skip 判重 → save_snapshot
    → bump_crawl_stats，彻底避免原先每支股票失败时新开连接导致的 WAL 多连接写锁竞争
    （表现为 OperationalError: database is locked）。
    """
    # 单一复用连接：WAL + 较长 busy_timeout，遇并发锁等待而非立即报错。
    shared_conn = sqlite3.connect(db_path)
    shared_conn.execute("PRAGMA busy_timeout=15000")
    shared_conn.execute("PRAGMA journal_mode=WAL")
    c = shared_conn.cursor()
    c.executescript(SCHEMA)
    _ensure_extra_columns(shared_conn)
    ensure_crawl_stats_source(shared_conn)

    # done=真实新增(ok)；empty=本次未拿到(待核对，不是确认无数据)；skip=已有真实跳过；fail=抓取失败
    stats = {"total": 0, "done": 0, "skip": 0, "fail": 0, "empty": 0}
    # 反爬自愈: 连续多支遭 403 封禁则长冷却，避免持续空跑加重封禁
    CONSEC_403_THRESHOLD = 8
    COOLDOWN_SEC = 600
    MAX_COOLDOWNS = 12
    consecutive_403 = 0
    cooldown_count = 0
    targets = codes[:limit] if limit else codes
    stats["total"] = len(targets)
    t0 = time.time()
    for i, item in enumerate(targets, 1):
        code = item["code"]
        stock = {"code": code, "market": "ab", "financeType": "stock",
                 "name": item.get("name", "")}
        if skip_existing and skip_recent_ok(shared_conn, code, "baidu", fresh_days):
            stats["skip"] += 1
            if progress_log:
                logger.info("[prog] %s skip", code)
            continue
        try:
            out = crawl_stock(stock, limiter, max_retries=max_retries, timeout=timeout)
            # save_snapshot 在复用连接下写入并自行 commit（涵盖内部 bump_crawl_stats）
            save_snapshot(db_path, trade_date, stock, out, conn=shared_conn)
            # 真实 / 未拿到 分流：updateTime 为空 → empty（待核对，不当成确认无数据）
            if out.get("ai_analysis", {}).get("synthesis", {}).get("updateTime"):
                stats["done"] += 1
                if progress_log:
                    logger.info("[prog] %s ok", code)
            else:
                stats["empty"] += 1
                if progress_log:
                    logger.info("[prog] %s empty", code)
                extra = []
                meta = out.get("meta") or {}
                if meta.get("api_endpoint"):
                    extra.append(("五维评分接口", meta["api_endpoint"]))
                if meta.get("source_page"):
                    extra.append({
                        "source": "百度财经", "kind": "page",
                        "label": "AI分析页(本次Referer)", "url": meta["source_page"],
                    })
                logger.warning(format_unconfirmed_empty_msg(
                    code, stock.get("name") or "", ["baidu"],
                    extra_request_urls=extra or None,
                ))
            consecutive_403 = 0
            if i % 50 == 0:
                el = time.time() - t0
                logger.info("[%d/%d] 真实 %d 未拿到 %d 跳过 %d 失败 %d 用时 %.0fs",
                            i, stats["total"], stats["done"], stats["empty"],
                            stats["skip"], stats["fail"], el)
        except Exception as e:
            stats["fail"] += 1
            if progress_log:
                logger.info("[prog] %s fail", code)
            # 累计抓取统计：本次尝试失败（403/网络异常等）；复用 shared_conn，随后手动 commit
            bump_crawl_stats(db_path, code, success=False, status="fail", source="baidu", conn=shared_conn)
            shared_conn.commit()
            msg = str(e)
            if "403" in msg or "Forbidden" in msg:
                consecutive_403 += 1
                if consecutive_403 >= CONSEC_403_THRESHOLD:
                    cooldown_count += 1
                    if cooldown_count > MAX_COOLDOWNS:
                        logger.error("连续冷却 %d 次仍遭 403，判定为持久封禁，停止剩余抓取。"
                                     "已成功 %d 支 / 跳过 %d 支 / 失败 %d 支。",
                                     MAX_COOLDOWNS, stats["done"], stats["skip"], stats["fail"])
                        break
                    # C 失败 → B：Selenium headless 刷新 Cookie，成功则短歇后继续；失败再长冷却
                    logger.warning("连续 %d 支遭百度 403，C 方案失效，切换 B 方案"
                                   "（Selenium headless 刷新 Cookie）（第 %d/%d 次）",
                                   consecutive_403, cooldown_count, MAX_COOLDOWNS)
                    refreshed = False
                    try:
                        from baidu_selenium_fallback import refresh_and_apply
                        cookies = refresh_and_apply()
                        if cookies:
                            global USER_COOKIES_FROM_WEB
                            USER_COOKIES_FROM_WEB = dict(cookies)
                            refreshed = True
                            logger.info("[B] Cookie 已刷新（%d 个），5 秒后继续抓取",
                                        len(cookies))
                            time.sleep(5)
                    except Exception as be:
                        logger.error("[B] Selenium 回退异常: %s", be)
                    if not refreshed:
                        logger.warning("[B] 刷新失败，回退为冷却 %.0f 秒后继续", COOLDOWN_SEC)
                        time.sleep(COOLDOWN_SEC)
                    consecutive_403 = 0
            else:
                consecutive_403 = 0
            logger.error("股票 %s 抓取失败: %s", code, e)
            logger.warning(format_unconfirmed_empty_msg(
                code, stock.get("name") or "", ["baidu"], reason="fail",
            ))
    shared_conn.close()
    return stats


# --------------------------------------------------------------------------- #
# 数据对账报告（只读）
# --------------------------------------------------------------------------- #
def report(db_path, trade_date=None) -> int:
    """数据对账报告：核对"抓了什么 / 真实日期 / 空壳 / 缺失 / 时效"。

    直击两大痛点:
      1) 数据没真正获取到？→ 统计 status='empty' 的未拿到数量（待人工核对，不是确认无数据）。
      2) 抓取日期 ≠ 数据真实日期？→ 按 update_time(真实分析日) 分布展示，
         并对比最新真实日与今天，揭示滞后天数；crawl_date 列可追溯"哪天抓的"。
    可选 --trade-date 将范围限定到该批次(trade_date 标签)。
    """
    import datetime as _dt
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=15000")
    _ensure_extra_columns(conn)  # 确保列存在，避免旧库缺列报错
    cur = conn.cursor()
    where = "WHERE trade_date=?" if trade_date else ""
    where_and = "AND" if trade_date else "WHERE"
    params = (trade_date,) if trade_date else ()

    total = cur.execute(f"SELECT COUNT(*) FROM scores {where}", params).fetchone()[0]
    ok = cur.execute(
        f"SELECT COUNT(*) FROM scores {where} {where_and} COALESCE(status,'ok')='ok'", params
    ).fetchone()[0]
    empty = total - ok
    print("===== 数据对账报告 =====")
    print(f"范围: {'全部' if not trade_date else 'trade_date=' + trade_date}")
    print(f"scores 总行数: {total} | 真实(ok): {ok} | 未拿到(empty,待核对): {empty}")
    if total:
        print(f"未拿到占比: {empty * 100.0 / total:.1f}%")

    print("\n-- 按真实分析日(update_time)分布 --")
    rows = cur.execute(
        f"SELECT update_time, COUNT(*), "
        f"SUM(CASE WHEN COALESCE(status,'ok')='ok' THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN COALESCE(status,'ok')<>'ok' THEN 1 ELSE 0 END) "
        f"FROM scores {where} GROUP BY update_time ORDER BY update_time DESC",
        params,
    ).fetchall()
    for ut, n, ok_n, empty_n in rows:
        print(f"  {ut}: 共 {n} | 真实 {ok_n} | 未拿到 {empty_n}")

    print("\n-- 子表覆盖（与真实 scores 同 update_time+code 存在的代码数）--")
    for tbl in ("support_resistance", "fund_flow", "vote"):
        q = (f"SELECT COUNT(DISTINCT t.code) FROM {tbl} t WHERE EXISTS "
             f"(SELECT 1 FROM scores s WHERE s.code=t.code AND s.update_time=t.update_time "
             f"AND COALESCE(s.status,'ok')='ok')")
        cnt = cur.execute(q).fetchone()[0]
        print(f"  {tbl}: {cnt} 支")

    latest = cur.execute(
        "SELECT MAX(update_time) FROM scores WHERE COALESCE(status,'ok')='ok'"
    ).fetchone()[0]
    today = today_cst().isoformat()
    print(f"\n最新真实分析日: {latest} | 今天: {today}")
    if latest:
        try:
            gap = (today_cst() - _dt.date.fromisoformat(latest)).days
            flag = "  ⚠ 已滞后，建议立即补抓" if gap > 1 else ""
            print(f"距今天 {gap} 天{flag}")
        except Exception:
            pass
    conn.close()
    return 0


# --------------------------------------------------------------------------- #
# 命令行入口
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="百度财经 AI 技术分析 全市场爬虫")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--stock", nargs=3, metavar=("CODE", "MARKET", "FINANCETYPE"),
                     help="单支模式: 代码 市场 类型（如 301228 ab stock）")
    grp.add_argument("--market", action="store_true",
                     help="全市场模式: 用 easy-tdx 拉全部 A 股代码并逐支抓取")
    parser.add_argument("--name", help="单支模式股票名称（可选）")
    parser.add_argument("--trade-date", default=None,
                        help="交易日 YYYY-MM-DD（默认今天，Asia/Shanghai）")
    parser.add_argument("--db", default="market_data.db",
                        help="SQLite 数据库路径（始终同一份，默认 ./market_data.db）")
    parser.add_argument("--codes-file", default="a_stocks.json",
                        help="A 股代码清单缓存文件（默认 ./a_stocks.json）")
    parser.add_argument("--refresh-codes", action="store_true",
                        help="强制重新从 easy-tdx 拉取代码清单")
    parser.add_argument("--limit", type=int, default=None,
                        help="全市场模式仅抓取前 N 支（测试用）")
    parser.add_argument("--no-skip", action="store_true",
                        help="不跳过已存在的 (交易日,代码) 记录")
    parser.add_argument("--fresh-days", type=int, default=2,
                        help="判重新鲜度窗口(天)：仅当股票最新真实分析日 update_time 早于"
                             "今天-N 天才重爬；默认 2。设很大(如 99999)即'成功过就永不重爬'")
    parser.add_argument("--output", default=None,
                        help="单支模式输出 JSON 路径")
    parser.add_argument("--min-interval", type=float, default=1.0,
                        help="两次请求最小间隔（秒），默认 1.0")
    parser.add_argument("--max-per-minute", type=int, default=40,
                        help="每分钟最大请求数，默认 40")
    parser.add_argument("--rate-wait-cap", type=float, default=None,
                        help="达每分钟上限后最多等待秒数（默认不限制=等满剩余窗口）；"
                             "设 0 表示达上限后立即开新窗口不空等")
    parser.add_argument("--rate-window", type=float, default=60.0,
                        help="限流窗口长度（秒），默认 60")
    parser.add_argument("--max-retries", type=int, default=3, help="失败重试次数，默认 3")
    parser.add_argument("--timeout", type=int, default=15, help="单次请求超时（秒）")
    parser.add_argument("--raw", action="store_true", help="单支模式包含原始 Result")
    parser.add_argument("--no-kline", action="store_true", help="跳过支撑/阻力接口")
    parser.add_argument("-v", "--verbose", action="store_true", help="调试日志")
    parser.add_argument("--report", action="store_true",
                        help="打印数据对账报告（空壳数/真实分析日分布/时效滞后），不抓取")
    parser.add_argument("--progress-log", action="store_true",
                        help="调试页用：逐股输出紧凑 [prog] 代码 状态 行，供 Web UI 实时计数（不影响落库）")
    args = parser.parse_args(argv)

    # 对账模式：只读扫描，打印报告后即退出，不改库、不抓取
    if args.report:
        return report(args.db, args.trade_date)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    raw = (datetime.strptime(args.trade_date, "%Y-%m-%d").date()
           if args.trade_date else today_cst())
    trade_date = resolve_trade_date(raw)
    if trade_date != raw:
        logger.info("日期 %s 非交易日，回退至上一交易日 %s",
                    raw.isoformat(), trade_date.isoformat())
        print(f"{raw.isoformat()} 非交易日，已回退至上一交易日 {trade_date.isoformat()}")

    # 全市场模式：trade_date 经 resolve_trade_date 已回退到交易日；
    # 此分支仅作安全网（极端情况下回退未命中交易日时跳过，避免空跑）。
    if args.market and not is_trading_day(trade_date):
        logger.info("%s 非交易日（周末/法定假日），跳过本次全市场抓取。", trade_date.isoformat())
        print(f"{trade_date.isoformat()} 非交易日，已跳过。")
        return 0

    limiter = RateLimiter(min_interval=args.min_interval,
                          max_per_minute=args.max_per_minute, jitter=0.6,
                          rate_wait_cap=args.rate_wait_cap,
                          rate_window_sec=args.rate_window)
    logger.info("限流: min_interval=%.2f max_per_minute=%d rate_wait_cap=%s rate_window=%.0f",
                args.min_interval, args.max_per_minute,
                args.rate_wait_cap, args.rate_window)

    # ---- 全市场模式 ----
    if args.market:
        try:
            codes = get_a_share_codes(cache_file=args.codes_file,
                                      refresh=args.refresh_codes)
        except Exception as e:
            logger.error("获取 A 股代码失败: %s", e)
            return 1
        if not codes:
            logger.error("代码清单为空，退出。")
            return 1
        logger.info("全市场抓取 %s，共 %d 支 A 股", trade_date.isoformat(), len(codes))
        stats = crawl_market(
            db_path=args.db, trade_date=trade_date.isoformat(), limiter=limiter,
            codes=codes, skip_existing=not args.no_skip, fresh_days=args.fresh_days,
            max_retries=args.max_retries, timeout=args.timeout, limit=args.limit,
            progress_log=args.progress_log,
        )
        print(f"\n===== 全市场抓取完成 {trade_date.isoformat()} =====")
        print(f"总计 {stats['total']} 支 | 真实新增 {stats['done']} | 未拿到(待核对) {stats['empty']} "
              f"| 跳过(已有真实) {stats['skip']} | 失败 {stats['fail']}")
        print(f"数据库: {args.db}")
        return 0

    # ---- 单支模式 ----
    if not args.stock:
        parser.error("请指定 --stock 或 --market")
    stock = {"code": args.stock[0], "market": args.stock[1],
             "financeType": args.stock[2], "name": args.name}
    try:
        out = crawl_stock(stock, limiter, max_retries=args.max_retries,
                          timeout=args.timeout, include_raw=args.raw,
                          skip_kline=args.no_kline)
    except Exception as e:
        logger.error("抓取失败: %s", e)
        return 1

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"输出文件: {args.output}")

    # 控制台摘要
    a = out["ai_analysis"]
    s = a["stock"]
    print("\n===== 抓取成功 =====")
    print(f"股票: {s['name']} ({s['code']})")
    print(f"综合: {a['synthesis'].get('rating')}  技术: {a['technology'].get('score')}  "
          f"资金: {a['capital'].get('score')}  市场: {a['market'].get('score')}  "
          f"财务: {a['finance'].get('score')}")
    sr = out.get("support_resistance")
    if sr:
        for cyc in ("long", "short"):
            d = sr.get(cyc)
            if not d:
                continue
            print(f"[{d['cycle_text']}] 支撑 {d['support_level']}  阻力 {d['resistance_level']}  "
                  f"评级 {d['rating_text']}")
    ff = out.get("fund_flow") or {}
    if ff:
        print(f"[资金] 超大 {ff.get('super_net')} 大单 {ff.get('large_net')}  "
              f"中单 {ff.get('medium_net')} 小单 {ff.get('little_net')}  "
              f"主力 {ff.get('main_net')} (亿)")
    vt = out.get("vote") or {}
    if vt:
        print(f"[投票] 总看涨 {vt.get('vote_up')} / 看跌 {vt.get('vote_down')}  "
              f"本周 {vt.get('week_up')}/{vt.get('week_down')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
