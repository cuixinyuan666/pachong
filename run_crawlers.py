#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股全市场爬虫统一入口：百度 AI 技术分析 + 东方财富 千股千评。

把两个数据源的爬虫聚合到一个 CLI，减少重复调用、统一续跑口径：
  * --source baidu  -> 百度财经 AI 技术分析（五维评分/支撑阻力/资金流/投票）
  * --source em     -> 东方财富 千股千评 + 估值 + 诊断概率/文字/参与意愿/市场排名
  * --source all    -> 两者都跑（默认），共用同一份 market_data.db

两爬虫均依赖 crawler_common 提供的交易日历 / 限流 / HTTP / 代码清单 / 续跑逻辑，
续跑判重统一走 crawl_stats 表（source 列区分 baidu/em），避免重复抓取。

用法:
  python run_crawlers.py --source all [--db market_data.db] [--limit N]
      [--no-skip] [--fresh-days 2] [--progress-log]
"""
import argparse
import sys

import baidu_finance_ai_crawler as baidu
import eastmoney_stockcomment_crawler as em


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="A股全市场爬虫统一入口（百度 + 东方财富）")
    parser.add_argument("--source", choices=["baidu", "em", "all"], default="all",
                        help="抓取来源：baidu=百度AI技术分析, em=东财千股千评, all=两者都跑(默认)")
    parser.add_argument("--db", default="market_data.db", help="数据库路径")
    parser.add_argument("--limit", type=int, default=None,
                        help="限制处理的股票数（调试用）")
    parser.add_argument("--no-skip", action="store_true",
                        help="不跳过已存在的新鲜记录")
    parser.add_argument("--fresh-days", type=int, default=2,
                        help="skip 新鲜度窗口：已有数据日 >= 今天-该值 则跳过，默认 2")
    parser.add_argument("--progress-log", action="store_true",
                        help="逐股输出 [prog] 代码 状态 行，供调试页实时计数")
    args = parser.parse_args(argv)

    # 组装传给各爬虫子命令的通用参数
    extra = []
    if args.limit is not None:
        extra += ["--limit", str(args.limit)]
    if args.no_skip:
        extra += ["--no-skip"]
    extra += ["--fresh-days", str(args.fresh_days)]
    if args.progress_log:
        extra += ["--progress-log"]

    rc = 0
    if args.source in ("baidu", "all"):
        print("\n===== 百度财经 AI 技术分析 =====")
        rc = baidu.main(["--market", "--db", args.db] + extra) or rc
    if args.source in ("em", "all"):
        print("\n===== 东方财富 千股千评 =====")
        rc = em.main(["--market", "--db", args.db] + extra) or rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
