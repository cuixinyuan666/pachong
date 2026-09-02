#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""东方财富「千股千评? 估?全市场爬虫"
落库同一?market_data.db?  * em_comment    —?千股千评诊断维度(综合得?排名/机构参与?主力成本/关注指数/资金流)
  * em_valuation  —?基本面估值维度(PE/PB/PS/PEG/市?板块?  * em_diag_text  —?定性诊断文字(RPT_STOCK_TRENDVOLUME_COMMENT 趋势量能 + RPT_STOCK_WORDS_PK 消息面)
  * em_diag_prob  —?诊断卡片数值(RPT_STOCK_CHANGERATE 次日/5日上涨概?平均涨跌 + RPT_CUSTOM_STOCK_PK 打败%? * em_participation —?市场参与意愿(RPT_STOCK_PARTICIPATION 当日/五日参与意愿?变化%? * em_popularity  —?市场排名(RPT_STOCK_PK_RANK 综合市场排名/评估市场总数/行业排名/打败%/变化?+ RPT_STOCK_MARKETFOCUS 关注指数/关注排名?
设计要点(与 baidu_finance_ai_crawler.py 保持同一套框架)?  * 诊断接口 RPT_DMSK_TS_STOCKNEW 是批量接口(500/页分页拉全市场,高效)?  * 估值接?RPT_VALUEANALYSIS_DET 仅支持单?filter,逐股抓取并受 RateLimiter 限流?  * 所有字段名加东方财富标识前缀:em_comment -> `emc_*`,em_valuation -> `emv_*`? * skip 基于统一 crawl_stats ?v3 续跑逻辑(source='em',与百度同源):?fresh_days
   天东财成功抓过该 code 即整支跳过,不再无限重爬长期稳定的股票? * --progress-log 输出 `[prog] CODE skip|ok|fail` 行,供调试页实时计数? * 复用单一 sqlite 连接(shared_conn)贯?SCHEMA 初始?-> 判重 -> upsert,避?WAL 锁竞争? * 注意:东方财富与百度抓的是同一?A 股代码,且共用同一?crawl_stats(以 source 列区?   baidu/em),两爬虫续跑口径一致;东财抓取成败?--progress-log 与返?stats 体现?
用法:
  python eastmoney_stockcomment_crawler.py --market [--db market_data.db] [--limit N]
      [--no-skip] [--fresh-days 2] [--progress-log] [--min-interval 0.1] [--max-per-minute 200]
"""
import argparse
import json
import logging
import random
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
    CST = ZoneInfo("Asia/Shanghai")
except Exception:
    CST = None

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("em_stockcomment")

# 公共层:与百度爬虫共享的交易日历 / 限流 / HTTP / 续跑逻辑
from crawler_common import (
    RateLimiter, today_cst, _now_cst_str, fetch_json,
    bump_crawl_stats, skip_recent_ok, ensure_crawl_stats_source, ForbiddenError,
    em_datacenter_url, em_stockcomment_list_url,
)

# Cookie 增强支持(C方案 - 东方财富版)
from enhance_eastmoney_crawler import get_em_cookie_manager, enhance_em_headers

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REF = "https://data.eastmoney.com/stockcomment/"
BASE = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PAGE_SIZE = 500  # 东方财富单页上限
# 统一 HTTP 请求头（供公共 fetch_json / urllib 后端使用）
EM_HEADERS = {"User-Agent": UA, "Referer": REF,
              "Accept": "application/json, text/plain, */*"}

def em_headers_with_cookie():
    """获取?Cookie 的增强请求头"""
    mgr = get_cookie_mgr()
    if mgr:
        return mgr.build_headers_with_cookies(EM_HEADERS)
    return EM_HEADERS

# Cookie manager
_em_cookie_mgr = None


def get_cookie_mgr():
    """获取或创建东方财?Cookie 管理"""
    global _em_cookie_mgr
    if _em_cookie_mgr is None:
        _em_cookie_mgr = get_em_cookie_manager()
    return _em_cookie_mgr





# --------------------------------------------------------------------------- #
# 表结
# --------------------------------------------------------------------------- #
SCHEMA = """
CREATE TABLE IF NOT EXISTS em_comment (
    code              TEXT NOT NULL,
    name              TEXT,
    trade_date        TEXT NOT NULL,
    emc_total_score   REAL,
    emc_rank          INTEGER,
    emc_rank_up       INTEGER,
    emc_focus         REAL,
    emc_org_participate REAL,
    emc_ratio         REAL,
    emc_prime_cost    REAL,
    emc_prime_cost_20d REAL,
    emc_prime_cost_60d REAL,
    emc_prime_inflow  REAL,
    emc_superdeal_in  REAL,
    emc_superdeal_out REAL,
    emc_bigdeal_in    REAL,
    emc_bigdeal_out   REAL,
    emc_buy_superdeal_ratio REAL,
    emc_buy_bigdeal_ratio   REAL,
    crawl_date        TEXT,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS em_valuation (
    code              TEXT NOT NULL,
    name              TEXT,
    trade_date        TEXT NOT NULL,
    emv_pe_ttm        REAL,
    emv_pe_lar        REAL,
    emv_pb_mrq        REAL,
    emv_pcf_ocf_lar   REAL,
    emv_pcf_ocf_ttm   REAL,
    emv_ps_ttm        REAL,
    emv_peg           REAL,
    emv_total_market_cap  REAL,
    emv_float_market_cap REAL,
    emv_board         TEXT,
    crawl_date        TEXT,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS em_diag_text (
    code              TEXT NOT NULL,
    name              TEXT,
    trade_date        TEXT NOT NULL,
    emt_comment_txt   TEXT,
    emt_words_explain TEXT,
    crawl_date        TEXT,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS em_diag_prob (
    code              TEXT NOT NULL,
    name              TEXT,
    trade_date        TEXT NOT NULL,
    emt_rise_1_prob   REAL,
    emt_rise_5_prob   REAL,
    emt_avg_1_inc     REAL,
    emt_avg_5_inc     REAL,
    emt_all_count_1   INTEGER,
    emt_all_count_5   INTEGER,
    emt_rank_ratio    REAL,
    crawl_date        TEXT,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS em_participation (
    code              TEXT NOT NULL,
    name              TEXT,
    trade_date        TEXT NOT NULL,
    emp_wish          REAL,
    emp_wish_5d       REAL,
    emp_wish_change   REAL,
    emp_wish_5d_change REAL,
    crawl_date        TEXT,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS em_popularity (
    code              TEXT NOT NULL,
    name              TEXT,
    trade_date        TEXT NOT NULL,
    emp_market_rank   INTEGER,
    emp_market_num    INTEGER,
    emp_industry_rank INTEGER,
    emp_change_rate   REAL,
    emp_market_stock_num INTEGER,
    emp_focus_rank    INTEGER,
    emp_focus_total   INTEGER,
    emp_focus_index   REAL,
    crawl_date        TEXT,
    PRIMARY KEY (trade_date, code)
);
"""


# --------------------------------------------------------------------------- #
# 抓取:诊断(批量分页
# --------------------------------------------------------------------------- #
def fetch_stocknew_all(limiter: RateLimiter, timeout: int) -> list:
    """分页拉取 RPT_DMSK_TS_STOCKNEW 全市场,返回行列表(原始 dict)"""
    rows = []
    pn = 1
    while True:
        qs = {
            "reportName": "RPT_DMSK_TS_STOCKNEW",
            "columns": "ALL",
            "pageSize": str(PAGE_SIZE),
            "pageNumber": str(pn),
            "sortColumns": "SECURITY_CODE",
            "sortTypes": "1",
            "client": "PC",
            "source": "WEB",
        }
        url = BASE + "?" + urllib.parse.urlencode(qs)
        try:
            d = fetch_json(url, em_headers_with_cookie(), limiter, backend="urllib",
                           timeout=timeout, max_retries=3)
        except Exception as e:
            logger.warning(
                "东财批量诊断第 %d 页失败: %s（待人工确认，不是确认无数据）\n"
                "  东方财富 · 千股千评列表页: %s\n"
                "  东方财富 · 本次请求接口: %s",
                pn, e, em_stockcomment_list_url(), url,
            )
            break
        result = d.get("result") or {}
        data = result.get("data") or []
        if not data:
            if pn == 1:
                # 批量第一页就空：未拿到 ≠ 源站确认无数据，给出可打开的源站/接口
                logger.warning(
                    "东财批量诊断第1页未拿到数据（待人工确认，不是确认无数据）。请打开源站核对：\n"
                    "  东方财富 · 千股千评列表页: %s\n"
                    "  东方财富 · 批量接口: %s",
                    em_stockcomment_list_url(), url,
                )
            break
        rows.extend(data)
        if len(data) < PAGE_SIZE:
            break
        pn += 1
    return rows


# --------------------------------------------------------------------------- #
# 抓取:估值(单股
# --------------------------------------------------------------------------- #
def fetch_valuation(code: str, limiter: RateLimiter, timeout: int,
                    max_retries: int) -> dict | None:
    """RPT_VALUEANALYSIS_DET 单股估值,返回原始 dict ?None"""
    qs = {
        "reportName": "RPT_VALUEANALYSIS_DET",
        "columns": "ALL",
        "filter": '(SECURITY_CODE="%s")' % code,
        "client": "PC",
        "source": "WEB",
    }
    url = BASE + "?" + urllib.parse.urlencode(qs)
    try:
        d = fetch_json(url, em_headers_with_cookie(), limiter, backend="urllib",
                       timeout=timeout, max_retries=max_retries)
    except Exception as e:
        logger.warning("估?%s 失败: %s", code, e)
        return None
    result = d.get("result") or {}
    data = result.get("data") or []
    return data[0] if data else None


def fetch_diag_text(code: str, limiter: RateLimiter, timeout: int,
                    max_retries: int) -> dict | None:
    """逐股 fetch RPT_STOCK_TRENDVOLUME_COMMENT(COMMENT_TXT) + RPT_STOCK_WORDS_PK(WORDS_EXPLAIN)?    返回 {code, name, trade_date, emt_comment_txt, emt_words_explain} ?None"""
    comment_txt, words_explain, td, name = None, None, None, None
    specs = [
        ("RPT_STOCK_TRENDVOLUME_COMMENT", "COMMENT_TXT"),
        ("RPT_STOCK_WORDS_PK", "WORDS_EXPLAIN"),
    ]
    for report, field in specs:
        qs = {
            "reportName": report,
            "columns": "ALL",
            "filter": '(SECURITY_CODE="%s")' % code,
            "client": "PC",
            "source": "WEB",
        }
        url = BASE + "?" + urllib.parse.urlencode(qs)
        try:
            d = fetch_json(url, em_headers_with_cookie(), limiter, backend="urllib",
                           timeout=timeout, max_retries=max_retries)
        except Exception as e:
            logger.warning("定性文?%s %s 失败: %s", code, report, e)
            continue
        result = d.get("result") or {}
        data = result.get("data") or []
        if data:
            row = data[0]
            if field == "COMMENT_TXT":
                comment_txt = row.get("COMMENT_TXT")
            else:
                words_explain = row.get("WORDS_EXPLAIN")
            if td is None:
                td = (row.get("TRADE_DATE") or "")[:10]
            if name is None:
                name = row.get("SECURITY_NAME_ABBR")
    if comment_txt is None and words_explain is None:
        return None
    return {"code": code, "name": name, "trade_date": td or "",
            "emt_comment_txt": comment_txt, "emt_words_explain": words_explain}


def _secucode(code: str) -> str:
    """根据代码前缀补全东财 SECUCODE 后缀(沪.SH / ?SZ / 北交所.BJ)"""
    if code.startswith("6"):
        return code + ".SH"
    if code.startswith(("0", "3", "2")):
        return code + ".SZ"
    if code.startswith(("8", "4")):
        return code + ".BJ"
    return code + ".SZ"


def fetch_diag_prob(code: str, limiter: RateLimiter, timeout: int,
                    max_retries: int) -> dict | None:
    """RPT_STOCK_CHANGERATE(RISE_1/5_PROBABILITY, AVERAGE_1/5_INCREASE, ALL_COUNT_1/5)
    + RPT_CUSTOM_STOCK_PK(STOCK_RANK_RATIO 打败%),合并返回诊断概率数值"""
    d = {"code": code, "name": None, "trade_date": "",
         "emt_rise_1_prob": None, "emt_rise_5_prob": None,
         "emt_avg_1_inc": None, "emt_avg_5_inc": None,
         "emt_all_count_1": None, "emt_all_count_5": None,
         "emt_rank_ratio": None}
    secu = _secucode(code)
    specs = [
        ("RPT_STOCK_CHANGERATE", {
            "RISE_1_PROBABILITY": "emt_rise_1_prob",
            "RISE_5_PROBABILITY": "emt_rise_5_prob",
            "AVERAGE_1_INCREASE": "emt_avg_1_inc",
            "AVERAGE_5_INCREASE": "emt_avg_5_inc",
            "ALL_COUNT_1": "emt_all_count_1",
            "ALL_COUNT_5": "emt_all_count_5",
        }),
        ("RPT_CUSTOM_STOCK_PK", {
            "STOCK_RANK_RATIO": "emt_rank_ratio",
        }),
    ]
    found = False
    for report, fmap in specs:
        qs = {"reportName": report, "columns": "ALL",
              "filter": '(SECUCODE="%s")' % secu, "client": "PC", "source": "WEB"}
        url = BASE + "?" + urllib.parse.urlencode(qs)
        try:
            j = fetch_json(url, em_headers_with_cookie(), limiter, backend="urllib",
                           timeout=timeout, max_retries=max_retries)
        except Exception as e:
            logger.warning("诊断概率 %s %s 失败: %s", code, report, e)
            continue
        data = (j.get("result") or {}).get("data") or []
        if data:
            row = data[0]
            d["name"] = d["name"] or row.get("SECURITY_NAME_ABBR")
            if not d["trade_date"]:
                d["trade_date"] = (row.get("DIAGNOSE_DATE")
                                   or row.get("TRADE_DATE") or "")[:10]
            for src, dst in fmap.items():
                if row.get(src) is not None:
                    d[dst] = _f(row.get(src))
                    found = True
    return d if found else None


def fetch_participation(code: str, limiter: RateLimiter, timeout: int,
                        max_retries: int) -> dict | None:
    """RPT_STOCK_PARTICIPATION(filter ?SECURITY_CODE,无后缀):市场参与意愿?    返回 {code,name,trade_date,emp_wish,emp_wish_5d,emp_wish_change,emp_wish_5d_change} ?None"""
    qs = {"reportName": "RPT_STOCK_PARTICIPATION", "columns": "ALL",
          "filter": '(SECURITY_CODE="%s")' % code, "client": "PC", "source": "WEB"}
    url = BASE + "?" + urllib.parse.urlencode(qs)
    try:
        j = fetch_json(url, em_headers_with_cookie(), limiter, backend="urllib",
                       timeout=timeout, max_retries=max_retries)
    except Exception as e:
        logger.warning("参与意愿 %s 失败: %s", code, e)
        return None
    data = (j.get("result") or {}).get("data") or []
    if data:
        row = data[0]
        return {
            "code": code,
            "name": row.get("SECURITY_NAME_ABBR"),
            "trade_date": (row.get("TRADE_DATE") or "")[:10],
            "emp_wish": _f(row.get("PARTICIPATION_WISH")),
            "emp_wish_5d": _f(row.get("PARTICIPATION_WISH_5DAYS")),
            "emp_wish_change": _f(row.get("PARTICIPATION_WISH_CHANGE")),
            "emp_wish_5d_change": _f(row.get("PARTICIPATION_WISH_5DAYSCHANGE")),
        }
    return None


def fetch_popularity(code: str, limiter: RateLimiter, timeout: int,
                     max_retries: int) -> dict | None:
    """市场排名维度?    RPT_STOCK_PK_RANK(每日更新)-> 综合市场排名/评估市场总数/行业排名/打败%/变化率;
    RPT_STOCK_MARKETFOCUS(关注指?关注排名,可能陈旧)-> 关注指数/关注排名/关注总数?    合并返回?None"""
    d = {"code": code, "name": None, "trade_date": "",
         "emp_market_rank": None, "emp_market_num": None,
         "emp_industry_rank": None, "emp_change_rate": None,
         "emp_market_stock_num": None,
         "emp_focus_rank": None, "emp_focus_total": None, "emp_focus_index": None}
    specs = [
        ("RPT_STOCK_PK_RANK", {
            "MARKET_RANK": "emp_market_rank",
            "EVALUATE_MARKET_NUM": "emp_market_num",
            "INDUSTRY_RANK": "emp_industry_rank",
            "CHANGE_RATE": "emp_change_rate",
            "MARKET_STOCK_NUM": "emp_market_stock_num",
        }),
        ("RPT_STOCK_MARKETFOCUS", {
            "MARKET_FOCUS_RANK": "emp_focus_rank",
            "TOTAL_MARKET": "emp_focus_total",
            "MARKET_FOCUS": "emp_focus_index",
        }),
    ]
    found = False
    for report, fmap in specs:
        qs = {"reportName": report, "columns": "ALL",
              "filter": '(SECURITY_CODE="%s")' % code, "client": "PC", "source": "WEB"}
        url = BASE + "?" + urllib.parse.urlencode(qs)
        try:
            j = fetch_json(url, em_headers_with_cookie(), limiter, backend="urllib",
                           timeout=timeout, max_retries=max_retries)
        except Exception as e:
            logger.warning("市场排名 %s %s 失败: %s", code, report, e)
            continue
        data = (j.get("result") or {}).get("data") or []
        if data:
            row = data[0]
            d["name"] = d["name"] or row.get("SECURITY_NAME_ABBR")
            if not d["trade_date"]:
                d["trade_date"] = (row.get("TRADE_DATE") or "")[:10]
            for src, dst in fmap.items():
                if row.get(src) is not None:
                    if dst in ("emp_focus_index", "emp_change_rate"):
                        d[dst] = _f(row.get(src))
                    else:
                        d[dst] = _i(row.get(src))
                    found = True
    return d if found else None


# --------------------------------------------------------------------------- #
# 落库
# --------------------------------------------------------------------------- #
def save_em(db_path: str, trade_date: str, crawled_emc: list,
            crawled_emv: dict, crawled_diag: list | None = None,
            crawled_prob: list | None = None, crawled_part: list | None = None,
            crawled_pop: list | None = None, conn=None) -> None:
    """upsert em_comment / em_valuation / em_diag_text / em_diag_prob /
    em_participation / em_popularity"""
    owned = conn is None
    if owned:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=15000")
    try:
        c = conn.cursor()
        c.executescript(SCHEMA)
        crawl_date = today_cst().isoformat()
        for row in crawled_emc:
            c.execute(
                "INSERT OR REPLACE INTO em_comment VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["code"], row["name"], row["trade_date"],
                    row.get("emc_total_score"), row.get("emc_rank"),
                    row.get("emc_rank_up"), row.get("emc_focus"),
                    row.get("emc_org_participate"), row.get("emc_ratio"),
                    row.get("emc_prime_cost"), row.get("emc_prime_cost_20d"),
                    row.get("emc_prime_cost_60d"), row.get("emc_prime_inflow"),
                    row.get("emc_superdeal_in"), row.get("emc_superdeal_out"),
                    row.get("emc_bigdeal_in"), row.get("emc_bigdeal_out"),
                    row.get("emc_buy_superdeal_ratio"), row.get("emc_buy_bigdeal_ratio"),
                    crawl_date,
                ),
            )
        for code, v in crawled_emv.items():
            c.execute(
                "INSERT OR REPLACE INTO em_valuation VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    code, v.get("name"), v.get("trade_date"),
                    v.get("emv_pe_ttm"), v.get("emv_pe_lar"), v.get("emv_pb_mrq"),
                    v.get("emv_pcf_ocf_lar"), v.get("emv_pcf_ocf_ttm"),
                    v.get("emv_ps_ttm"), v.get("emv_peg"),
                    v.get("emv_total_market_cap"), v.get("emv_float_market_cap"),
                    v.get("emv_board"), crawl_date,
                ),
            )
        for d in (crawled_diag or []):
            c.execute(
                "INSERT OR REPLACE INTO em_diag_text VALUES (?,?,?,?,?,?)",
                (
                    d["code"], d.get("name"), d.get("trade_date"),
                    d.get("emt_comment_txt"), d.get("emt_words_explain"),
                    crawl_date,
                ),
            )
        for d in (crawled_prob or []):
            c.execute(
                "INSERT OR REPLACE INTO em_diag_prob VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    d["code"], d.get("name"), d.get("trade_date"),
                    d.get("emt_rise_1_prob"), d.get("emt_rise_5_prob"),
                    d.get("emt_avg_1_inc"), d.get("emt_avg_5_inc"),
                    d.get("emt_all_count_1"), d.get("emt_all_count_5"),
                    d.get("emt_rank_ratio"), crawl_date,
                ),
            )
        for d in (crawled_part or []):
            c.execute(
                "INSERT OR REPLACE INTO em_participation VALUES (?,?,?,?,?,?,?,?)",
                (
                    d["code"], d.get("name"), d.get("trade_date"),
                    d.get("emp_wish"), d.get("emp_wish_5d"),
                    d.get("emp_wish_change"), d.get("emp_wish_5d_change"),
                    crawl_date,
                ),
            )
        for d in (crawled_pop or []):
            c.execute(
                "INSERT OR REPLACE INTO em_popularity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    d["code"], d.get("name"), d.get("trade_date"),
                    d.get("emp_market_rank"), d.get("emp_market_num"),
                    d.get("emp_industry_rank"), d.get("emp_change_rate"),
                    d.get("emp_market_stock_num"),
                    d.get("emp_focus_rank"), d.get("emp_focus_total"),
                    d.get("emp_focus_index"), crawl_date,
                ),
            )
        conn.commit()
    finally:
        if owned:
            conn.close()


# --------------------------------------------------------------------------- #
# 主流
# --------------------------------------------------------------------------- #
def crawl_market_em(db_path: str, trade_date: str, limiter: RateLimiter,
                    skip_existing: bool = True, fresh_days: int = 2,
                    progress_log: bool = False, max_retries: int = 3,
                    timeout: int = 15, limit: int | None = None) -> dict:
    """全市场抓取东方财富诊?估值"""
    stats = {"total": 0, "done": 0, "skip": 0, "fail": 0}

    # 单一复用连接
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=15000")
    try:
        conn.executescript(SCHEMA)
        ensure_crawl_stats_source(conn)

        # 1) 批量拉诊?
        raw = fetch_stocknew_all(limiter, timeout)
        stats["total"] = len(raw)
        if not raw:
            logger.warning(
                "东财全市场诊断本次未拿到数据（待人工确认，不是确认无数据）。请打开源站核对：\n"
                "  东方财富 · 千股千评列表页: %s\n"
                "  东方财富 · 批量接口: %s",
                em_stockcomment_list_url(),
                em_datacenter_url(
                    "RPT_DMSK_TS_STOCKNEW",
                    {"pageSize": str(PAGE_SIZE), "pageNumber": "1",
                     "sortColumns": "SECURITY_CODE", "sortTypes": "1"},
                ),
            )

        # 2) 逐支映射 + skip + 落库诊断 + 估?+ 定性文?+ 概率
        crawled_emc, crawled_emv, crawled_diag, crawled_prob = [], {}, [], []
        crawled_part, crawled_pop = [], []
        for item in raw:
            # 限制处理数量(调?--limit):已抓?已跳过达到上限则停止
            if limit is not None and (stats["done"] + stats["skip"]) >= limit:
                break
            code = item.get("SECURITY_CODE")
            if not code:
                continue
            td = (item.get("TRADE_DATE") or "")[:10]
            name = item.get("SECURITY_NAME_ABBR")

            # 统一 v3 续跑判重(与百度同源 crawl_stats,source='em'):
            # ?fresh_days 天东财成功抓过该 code -> 整支跳过,不发请?
            if skip_existing and skip_recent_ok(conn, code, "em", fresh_days):
                stats["skip"] += 1
                if progress_log:
                    logger.info("[prog] %s skip", code)
            else:
                emc = {
                    "code": code, "name": name, "trade_date": td,
                    "emc_total_score": _f(item.get("TOTALSCORE")),
                    "emc_rank": _i(item.get("RANK")),
                    "emc_rank_up": _i(item.get("RANK_UP")),
                    "emc_focus": _f(item.get("FOCUS")),
                    "emc_org_participate": _f(item.get("ORG_PARTICIPATE")),
                    "emc_ratio": _f(item.get("RATIO")),
                    "emc_prime_cost": _f(item.get("PRIME_COST")),
                    "emc_prime_cost_20d": _f(item.get("PRIME_COST_20DAYS")),
                    "emc_prime_cost_60d": _f(item.get("PRIME_COST_60DAYS")),
                    "emc_prime_inflow": _f(item.get("PRIME_INFLOW")),
                    "emc_superdeal_in": _f(item.get("SUPERDEAL_INFLOW")),
                    "emc_superdeal_out": _f(item.get("SUPERDEAL_OUTFLOW")),
                    "emc_bigdeal_in": _f(item.get("BIGDEAL_INFLOW")),
                    "emc_bigdeal_out": _f(item.get("BIGDEAL_OUTFLOW")),
                    "emc_buy_superdeal_ratio": _f(item.get("BUY_SUPERDEAL_RATIO")),
                    "emc_buy_bigdeal_ratio": _f(item.get("BUY_BIGDEAL_RATIO")),
                }
                crawled_emc.append(emc)

                # 估值(单股?
                v = fetch_valuation(code, limiter, timeout, max_retries)
                if v:
                    crawled_emv[code] = {
                        "name": v.get("SECURITY_NAME_ABBR") or name,
                        "trade_date": (v.get("TRADE_DATE") or td)[:10],
                        "emv_pe_ttm": _f(v.get("PE_TTM")),
                        "emv_pe_lar": _f(v.get("PE_LAR")),
                        "emv_pb_mrq": _f(v.get("PB_MRQ")),
                        "emv_pcf_ocf_lar": _f(v.get("PCF_OCF_LAR")),
                        "emv_pcf_ocf_ttm": _f(v.get("PCF_OCF_TTM")),
                        "emv_ps_ttm": _f(v.get("PS_TTM")),
                        "emv_peg": _f(v.get("PEG_CAR")),
                        "emv_total_market_cap": _f(v.get("TOTAL_MARKET_CAP")),
                        "emv_float_market_cap": _f(v.get("NOTLIMITED_MARKETCAP_A")),
                        "emv_board": v.get("BOARD_NAME"),
                    }

                stats["done"] += 1
                if progress_log:
                    logger.info("[prog] %s ok", code)

            # 定性文?/ 诊断概率 / 参与意愿 / 市场排名:统一跳过整支后,
            # ?skip 即全部维度补抓(东财一次成功会写全部维度)
            d = fetch_diag_text(code, limiter, timeout, max_retries)
            if d:
                crawled_diag.append(d)
            p = fetch_diag_prob(code, limiter, timeout, max_retries)
            if p:
                crawled_prob.append(p)
            pp = fetch_participation(code, limiter, timeout, max_retries)
            if pp:
                crawled_part.append(pp)
            o = fetch_popularity(code, limiter, timeout, max_retries)
            if o:
                crawled_pop.append(o)
            # 整支成功(已发出请求并尝试全部维度)-> ?crawl_stats ok,供续跑 v3 判重
            bump_crawl_stats(db_path, code, success=True, status="ok", source="em", conn=conn)

            # 分批落库:任一缓冲达到 25 即提交,避免长循环累积丢?+ 页面渐进可见
            if (len(crawled_emc) >= 25 or len(crawled_diag) >= 25
                    or len(crawled_prob) >= 25 or len(crawled_part) >= 25
                    or len(crawled_pop) >= 25):
                save_em(db_path, trade_date, crawled_emc, crawled_emv,
                         crawled_diag, crawled_prob, crawled_part, crawled_pop,
                         conn=conn)
                crawled_emc, crawled_emv, crawled_diag, crawled_prob = [], {}, [], []
                crawled_part, crawled_pop = [], []

        # 3) 落库剩余(诊?+ 估?+ 定性文?+ 概率 + 参与意愿 + 市场排名?
        if crawled_emc or crawled_diag or crawled_prob or crawled_part or crawled_pop:
            save_em(db_path, trade_date, crawled_emc, crawled_emv,
                     crawled_diag, crawled_prob, crawled_part, crawled_pop,
                     conn=conn)
    finally:
        conn.close()
    return stats


def _f(x):
    """?float,空/异常?None"""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def _i(x):
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="东方财富 千股千评+估?全市场爬")
    parser.add_argument("--market", action="store_true", help="全市场抓")
    parser.add_argument("--db", default="market_data.db", help="数据库路")
    parser.add_argument("--limit", type=int, default=None,
                        help="限制处理的股票数(调试用")
    parser.add_argument("--no-skip", action="store_true",
                        help="不跳过已存在的新鲜记")
    parser.add_argument("--fresh-days", type=int, default=2,
                        help="skip 新鲜度窗口:已有数据?>= 今天-该?则跳过,默认 2")
    parser.add_argument("--progress-log", action="store_true",
                        help="逐股输出 [prog] 代码 状?行,供调试页实时计数")
    parser.add_argument("--min-interval", type=float, default=0.1,
                        help="请求最小间隔(秒),默?0.1")
    parser.add_argument("--max-per-minute", type=int, default=200,
                        help="每分钟最大请求数(估值逐股用),默认 200")
    parser.add_argument("--rate-wait-cap", type=float, default=None,
                        help="达每分钟上限后最多等待秒数；0=立即开新窗口")
    parser.add_argument("--rate-window", type=float, default=60.0,
                        help="限流窗口长度（秒），默认 60")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="失败重试次数,默认 3")
    parser.add_argument("--timeout", type=int, default=15,
                        help="单次请求超时(秒)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if not args.market:
        parser.error("请指定 --market")

    trade_date = today_cst().isoformat()
    limiter = RateLimiter(min_interval=args.min_interval,
                          max_per_minute=args.max_per_minute, jitter=0.0,
                          rate_wait_cap=args.rate_wait_cap,
                          rate_window_sec=args.rate_window)
    stats = crawl_market_em(
        db_path=args.db, trade_date=trade_date, limiter=limiter,
        skip_existing=not args.no_skip, fresh_days=args.fresh_days,
        progress_log=args.progress_log, max_retries=args.max_retries,
        timeout=args.timeout, limit=args.limit)

    print(f"\n===== 东方财富抓取完成 {trade_date} =====")
    print(f"总计 {stats['total']} ?| 新抓?{stats['done']} | 跳过(已新? {stats['skip']}")
    print(f"数据? {args.db}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
