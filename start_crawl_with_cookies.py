#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用真实 Cookie 的全市场爬取脚本
=====================================

直接使用你提供的有效 Cookie，绕过百度 403 反爬。

用法:
    python start_crawl_with_cookies.py [--limit 100] [--min-interval 2.0]
"""
import sys
import os
import random
import time
import sqlite3

sys.path.insert(0, r'D:\my_file1\my_file1\my_file\14\2026-07-18-17-52-45')

from crawler_common import (
    fetch_json, RateLimiter, ForbiddenError,
    is_trading_day, today_cst, resolve_trade_date,
    bump_crawl_stats, skip_recent_ok, ensure_crawl_stats_source,
    format_unconfirmed_empty_msg,
)
from baidu_finance_ai_crawler import (
    SCHEMA, _ensure_extra_columns, save_snapshot, parse_analysis,
    build_headers, build_api_url, build_kline_url, build_fundflow_url,
    build_vote_url, parse_kline_analyse, parse_fundflow_daily, parse_vote,
    get_a_share_codes, API_HOST, PAGE_HOST,
)

# 真实有效的 Cookie
USER_COOKIES = {
    "BAIDUID": "3DF3B61396549A082C7BA6E504A98FD0:FG=1",
    "BIDUPSID": "3DF3B61396549A085D3C6DDB890E7E55",
    "PSTM": "1772379959",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/17.5",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def build_headers_with_cookies(stock_code: str) -> dict:
    """构建带真实 Cookie 的请求头"""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"https://finance.pae.baidu.com/ai-tech-analysi/stock/ab-{stock_code}",
        "Origin": "https://finance.pae.baidu.com",
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": "; ".join([f"{k}={v}" for k, v in USER_COOKIES.items()]),
    }
    return headers


def crawl_stock_enhanced(stock, limiter, trade_date, db_path):
    """增强的单支股票抓取（带真实 Cookie）"""
    code = stock["code"]
    name = stock.get("name", "")
    
    headers = build_headers_with_cookies(code)
    
    # 获取五维评分
    data = fetch_json(build_api_url(stock), headers, limiter, backend="curl", max_retries=1, timeout=10)
    result = data.get("Result", {}) or {}
    parsed = parse_analysis(result, stock)
    
    out = {
        "meta": {
            "query_id": data.get("QueryID"),
            "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "ai_analysis": parsed,
        "support_resistance": {},
        "fund_flow": {},
        "vote": {},
    }
    
    # 支撑阻力位
    for cycle in ("long", "short"):
        try:
            kdata = fetch_json(
                build_kline_url(stock, cycle),
                headers, limiter, max_retries=1, timeout=10
            )
            out["support_resistance"][cycle] = parse_kline_analyse(kdata, cycle)
        except:
            pass
    
    # 资金流向
    try:
        funddata = fetch_json(
            build_fundflow_url(stock),
            headers, limiter, max_retries=1, timeout=10
        )
        out["fund_flow"] = parse_fundflow_daily(funddata)
    except:
        pass
    
    # 投票数据
    try:
        votedata = fetch_json(
            build_vote_url(stock),
            headers, limiter, max_retries=1, timeout=10
        )
        out["vote"] = parse_vote(votedata)
    except:
        pass
    
    # 保存到数据库
    save_snapshot(db_path, trade_date, stock, out)
    
    if out.get("ai_analysis", {}).get("synthesis", {}).get("updateTime"):
        return "ok"
    else:
        return "empty"


def main(limit=None, min_interval=2.0):
    print("=" * 70)
    print("百度财经 AI 全市场爬取 - 真实 Cookie 版")
    print("=" * 70)
    print(f"\n配置:")
    print(f"  Cookie: {len(USER_COOKIES)} 个 (已验证有效)")
    print(f"  最小间隔: {min_interval} 秒")
    print(f"  限制数量: {'全部' if not limit else limit}")
    print("=" * 70)
    
    db_path = r"D:\my_file1\my_file1\my_file\14\2026-07-18-17-52-45\market_data.db"
    trade_date = resolve_trade_date(today_cst()).isoformat()
    
    # 初始化数据库
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    c.executescript(SCHEMA)
    _ensure_extra_columns(conn)
    ensure_crawl_stats_source(conn)
    
    # 加载股票列表
    codes = get_a_share_codes()
    if not codes:
        print("错误：无法获取股票列表")
        return
    
    targets = codes[:limit] if limit else codes
    print(f"\n准备抓取 {len(targets)} 支股票\n")
    
    limiter = RateLimiter(min_interval=min_interval, max_per_minute=25)
    
    stats = {"total": len(targets), "done": 0, "skip": 0, "fail": 0, "empty": 0}
    consecutive_403 = 0
    t0 = time.time()
    
    for i, item in enumerate(targets, 1):
        code = item["code"]
        stock = {"code": code, "market": "ab", "financeType": "stock",
                 "name": item.get("name", "")}
        
        if skip_recent_ok(conn, code, "baidu", fresh_days=2):
            stats["skip"] += 1
            continue
        
        try:
            status = crawl_stock_enhanced(stock, limiter, trade_date, db_path)
            
            if status == "ok":
                stats["done"] += 1
                consecutive_403 = 0
            elif status == "empty":
                stats["empty"] += 1
                consecutive_403 = 0
                print(format_unconfirmed_empty_msg(
                    code, name, ["baidu"],
                ))
            else:
                stats["fail"] += 1
                print(format_unconfirmed_empty_msg(
                    code, name, ["baidu"], reason="fail",
                ))
                
        except ForbiddenError:
            stats["fail"] += 1
            consecutive_403 += 1
            print(f"\n⚠️ 连续 {consecutive_403} 次 403！Cookie 可能已失效")
            print(format_unconfirmed_empty_msg(code, name, ["baidu"], reason="fail"))
            print(f"   建议：更新 Cookie 或检查网络连接")
            break
            
        except Exception as e:
            stats["fail"] += 1
            consecutive_403 = 0
            print(format_unconfirmed_empty_msg(
                code, name, ["baidu"], reason="fail",
            ))
            if i % 100 == 0:
                print(f"[{i}] 错误: {str(e)[:80]}")
        
        # 定期输出进度
        if i % 50 == 0:
            elapsed = time.time() - t0
            rate = stats["done"] / max(elapsed, 1) * 3600
            print(f"[{i}/{stats['total']}] 成功 {stats['done']} | 跳过 {stats['skip']} | 失败 {stats['fail']} | {rate:.0f} 支/小时")
    
    elapsed = time.time() - t0
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"爬取完成!")
    print(f"总计: {stats['total']} 支 | 成功: {stats['done']} | 未拿到(待核对): {stats['empty']} | 跳过: {stats['skip']} | 失败: {stats['fail']}")
    print(f"耗时: {elapsed:.0f} 秒 ({elapsed/60:.1f} 分钟)")
    if elapsed > 0:
        print(f"速度: {stats['done']/elapsed*60:.1f} 支/分钟")
    print("=" * 70)
    
    return stats


if __name__ == "__main__":
    import argparse
    
    ap = argparse.ArgumentParser(description="百度财经爬取 - 真实 Cookie 版")
    ap.add_argument("--limit", type=int, default=None, help="限制抓取数量")
    ap.add_argument("--min-interval", type=float, default=2.0, help="最小请求间隔")
    args = ap.parse_args()
    
    main(limit=args.limit, min_interval=args.min_interval)
