#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""crawler_common.py — 百度 / 东方财富 全市场爬虫共享公共层。

抽离两爬虫重复的底层能力，统一行为、消除分叉：
  * 交易日历: is_trading_day / today_cst / _now_cst_str / resolve_trade_date
  * RateLimiter（限流，统一默认参数）
  * fetch_json（HTTP: curl 后端给百度, urllib 后端给东财; 403 -> ForbiddenError）
  * get_a_share_codes（A股代码清单: easy-tdx 优先 + adata 兜底 + 阈值缓存）
  * 统一续跑 crawl_stats（v3, 加 source 列区分 baidu/em）+ skip_recent_ok 判定

设计要点
--------
  * crawl_stats 主键保持 (code)，新增 source 列（DEFAULT 'baidu'）区分数据源；
    旧库（无 source 列）由 ensure_crawl_stats_source 幂等补列，已有行
    COALESCE(source,'baidu') 命中百度，向后兼容。
  * skip 判定 v3: 「近 fresh_days 天该 source 成功抓取过 -> 跳过，不发请求」；
    从未成功 / 失败 / 未拿到(empty，待人工确认) -> 必重爬。empty 不当成确认无数据。
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
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

import random  # noqa: E402  (限流 jitter 用)

logger = logging.getLogger("crawler_common")

# --------------------------------------------------------------------------- #
# 交易日历（2026 年，沪深北交易所官方休市安排）
# 数据来源: 上交所/深交所/北交所 2025-12-22 发布的 2026 年部分节假日休市安排。
# 如需 2027 年，请在 HOLIDAYS 字典中追加对应年份的闭市日期集合。
# --------------------------------------------------------------------------- #
HOLIDAYS: dict[int, set] = {
    2026: {
        # 元旦: 1/1-1/3（1/4 周日已计入周末）
        "2026-01-01", "2026-01-02", "2026-01-03",
        # 春节: 2/15-2/23（2/14、2/28 周六已计入周末）
        "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
        "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23",
        # 清明节: 4/4-4/6
        "2026-04-04", "2026-04-05", "2026-04-06",
        # 劳动节: 5/1-5/5（5/9 周六已计入周末）
        "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
        # 端午节: 6/19-6/21
        "2026-06-19", "2026-06-20", "2026-06-21",
        # 中秋节: 9/25-9/27（9/20 周日、10/10 周六已计入周末）
        "2026-09-25", "2026-09-26", "2026-09-27",
        # 国庆节: 10/1-10/7
        "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
        "2026-10-05", "2026-10-06", "2026-10-07",
    },
}


def is_trading_day(d: date | None = None) -> bool:
    """判断某日是否为 A 股交易日（周一至周五，且非法定假日）。"""
    d = d or (datetime.now(timezone(timedelta(hours=8))).date())
    if d.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    hol = HOLIDAYS.get(d.year, set())
    return d.isoformat() not in hol


def today_cst() -> date:
    """返回 Asia/Shanghai (UTC+8) 的当前日期。"""
    return datetime.now(timezone(timedelta(hours=8))).date()


def _now_cst_str() -> str:
    """返回 Asia/Shanghai 当前时间戳（YYYY-MM-DD HH:MM:SS），用于 crawl_stats.updated_at。"""
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def resolve_trade_date(d: date) -> date:
    """若 d 非交易日（周末/法定假日），向前回退到最近的上一交易日。

    用于让 trade_date 始终对应一个真实交易日：周末或假日触发抓取时，
    其网页返回的是最近一个交易日（上一交易日）的数据。
    """
    cap = 0
    while not is_trading_day(d) and cap < 30:
        d = d - timedelta(days=1)
        cap += 1
    return d


# --------------------------------------------------------------------------- #
# 频率限制器（统一默认参数）
# --------------------------------------------------------------------------- #
class RateLimiter:
    def __init__(self, min_interval: float = 1.0, max_per_minute: int = 40,
                 jitter: float = 0.5, rate_wait_cap: float | None = None,
                 rate_window_sec: float = 60.0):
        self.min_interval = float(min_interval)
        self.max_per_minute = int(max_per_minute)
        self.jitter = float(jitter)
        # 达每分钟上限时，实际等待 = min(窗口剩余, rate_wait_cap)；None 表示不等待上限
        self.rate_wait_cap = None if rate_wait_cap is None else float(rate_wait_cap)
        self.rate_window_sec = float(rate_window_sec) if rate_window_sec > 0 else 60.0
        self._last = 0.0
        self._window_start = 0.0
        self._count = 0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        if now - self._window_start >= self.rate_window_sec:
            self._window_start = now
            self._count = 0
        if self._count >= self.max_per_minute:
            wait_to = (self._window_start + self.rate_window_sec) - now
            if self.rate_wait_cap is not None and self.rate_wait_cap >= 0:
                wait_to = min(wait_to, self.rate_wait_cap)
            if wait_to > 0:
                logger.info(
                    "达到每分钟请求上限(%d/%d)，限流等待 %.1f 秒"
                    "（窗口 %.0fs，等待上限 %s）",
                    self.max_per_minute, self.max_per_minute, wait_to,
                    self.rate_window_sec,
                    ("无" if self.rate_wait_cap is None else "%.1f" % self.rate_wait_cap),
                )
                time.sleep(wait_to)
            self._window_start = time.monotonic()
            self._count = 0
        self._last = time.monotonic()
        self._count += 1
        if self.jitter > 0:
            time.sleep(random.uniform(0, self.jitter))


# --------------------------------------------------------------------------- #
# 异常
# --------------------------------------------------------------------------- #
class ForbiddenError(RuntimeError):
    """HTTP 403 反爬封禁。供调用方精确识别（区别于一般网络错误）。"""


# --------------------------------------------------------------------------- #
# 网络抓取（含重试 / 限流 / 429 处理）
# 双后端：curl（百度，绕开 OpenSSL TLS 指纹 403）/ urllib（东财，简单直连）
# --------------------------------------------------------------------------- #
def fetch_json(url, headers, limiter, backend="curl", max_retries=3,
               timeout=15, backoff_base=2.0):
    """统一抓取 JSON。

    backend='curl' : 用系统 curl 子进程（百度 CDN 对 Python urllib 的 TLS 指纹
                     JA3 直接 403，curl/Schannel 可正常返回）。
    backend='urllib': 用标准库 urllib（东财 datacenter-web 不封 TLS 指纹）。
    两者遇到 HTTP 403 均抛 ForbiddenError（消息含 '403'），其余可重试错误按指数退避。
    """
    if backend == "curl":
        return _fetch_curl(url, headers, limiter, max_retries, timeout, backoff_base)
    return _fetch_urllib(url, headers, limiter, max_retries, timeout, backoff_base)


def _fetch_curl(url, headers, limiter, max_retries=3, timeout=15, backoff_base=2.0):
    """百度专用：系统 curl 子进程抓取（绕开 TLS 指纹封锁）。"""
    # Cookie 轮换支持（C方案）
    from baidu_cookie_pool import get_cookie_pool
    pool = get_cookie_pool()
    session = pool.get_best_session()
    use_cookie = False
    if session.cookies:
        use_cookie = True
        cookie_str = "; ".join([f"{k}={v}" for k, v in session.cookies.items()])
        headers["Cookie"] = cookie_str
        logger.debug(f"使用 Session: {session.session_id}, Cookie: {len(session.cookies)} 个")
    
    curl_path = os.environ.get("CURL_PATH") or "curl"
    last_err = None
    for attempt in range(1, max_retries + 1):
        limiter.wait()
        # 不主动发 Accept-Encoding: gzip（本机 curl 精简版不支持 --compressed），
        # 百度对无 gzip 协商的请求返回明文 JSON，避免解压复杂度。
        hdrs = {k: v for k, v in headers.items() if k.lower() != "accept-encoding"}
        args = [curl_path, "-sS", "-m", str(timeout),
                "-A", hdrs.pop("User-Agent", "")]
        for k, v in hdrs.items():
            args += ["-H", f"{k}: {v}"]
        fd, tmp = tempfile.mkstemp(suffix=".body")
        os.close(fd)
        hfd, htmp = tempfile.mkstemp(suffix=".hdr")
        os.close(hfd)
        args += ["-o", tmp, "-D", htmp, "-w", "%{http_code}", url]
        try:
            proc = subprocess.run(args, capture_output=True, text=True,
                                  timeout=timeout + 15)
            status = proc.stdout.strip()
            rc = int(status) if status.isdigit() else 0
            if rc == 0:
                raise RuntimeError(f"curl 无状态码(exit={proc.returncode}, err={proc.stderr[:120]})")
            if rc == 429:
                ra = float(backoff_base)
                try:
                    with open(htmp, encoding="utf-8", errors="replace") as hf:
                        for line in hf:
                            if line.lower().startswith("retry-after:"):
                                try:
                                    ra = float(line.split(":", 1)[1].strip())
                                except Exception:
                                    pass
                except Exception:
                    pass
                logger.warning("收到 429 限流，等待 %.0f 秒后重试", ra)
                time.sleep(ra)
                last_err = f"HTTP 429 (retry-after={ra})"
                continue
            if 500 <= rc < 600:
                last_err = f"HTTP {rc}"
                sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.warning("HTTP %s，第 %d 次重试，等待 %.1f 秒", rc, attempt, sl)
                time.sleep(sl)
                continue
            if rc == 403:
                # 反爬封禁：抛出可精确识别的 ForbiddenError（消息含 403）
                # 记录失败
                if use_cookie and session:
                    pool.record_failure(session.session_id)
                raise ForbiddenError(f"HTTP 403 (curl) url={url[:80]}")
            if rc != 200:
                last_err = f"HTTP {rc}"
                sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.warning("HTTP %s，第 %d 次重试，等待 %.1f 秒", rc, attempt, sl)
                time.sleep(sl)
                continue
            with open(tmp, encoding="utf-8", errors="replace") as f:
                text = f.read()
            data = json.loads(text)
            rc_code = data.get("ResultCode")
            if rc_code is not None and str(rc_code) != "0":
                raise ValueError(f"API 业务错误 ResultCode={rc_code} msg={data.get('ResultMsg')}")
            
            # 记录成功
            if use_cookie and session:
                pool.record_success(session.session_id)
            
            return data
        except ForbiddenError:
            raise
        except (ValueError, json.JSONDecodeError) as e:
            last_err = e
            sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
            logger.warning("解析错误 %s，第 %d 次重试，等待 %.1f 秒", e, attempt, sl)
            time.sleep(sl)
            continue
        except subprocess.TimeoutExpired:
            last_err = "curl 超时"
            sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
            time.sleep(sl)
            continue
        except FileNotFoundError:
            raise RuntimeError("未找到 curl 可执行文件，请安装 curl 或设置 CURL_PATH 环境变量")
        finally:
            for p in (tmp, htmp):
                try:
                    os.remove(p)
                except Exception:
                    pass
    raise RuntimeError(f"抓取失败，已重试 {max_retries} 次: {last_err}")


def _fetch_urllib(url, headers, limiter, max_retries=3, timeout=15, backoff_base=2.0):
    """东财专用：标准库 urllib 直连（datacenter-web 不封 TLS 指纹）。"""
    last_err = None
    for attempt in range(1, max_retries + 1):
        limiter.wait()
        req = Request(url, headers=headers)
        try:
            with urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read().decode("utf-8"))
            return data
        except HTTPError as e:
            if e.code == 403:
                raise ForbiddenError(f"HTTP 403 (urllib) url={url[:80]}")
            last_err = f"HTTP {e.code}"
            sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
            logger.warning("HTTP %s，第 %d 次重试，等待 %.1f 秒", e.code, attempt, sl)
            time.sleep(sl)
        except (URLError, HTTPException, IncompleteRead, TimeoutError, OSError) as e:
            last_err = e
            sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
            logger.warning("网络错误 %s，第 %d 次重试，等待 %.1f 秒", e, attempt, sl)
            time.sleep(sl)
        except (ValueError, json.JSONDecodeError) as e:
            last_err = e
            sl = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
            logger.warning("解析错误 %s，第 %d 次重试，等待 %.1f 秒", e, attempt, sl)
            time.sleep(sl)
    raise RuntimeError(f"抓取失败，已重试 {max_retries} 次: {last_err}")


# --------------------------------------------------------------------------- #
# A 股代码清单（easy-tdx 优先 + adata 兜底 + 阈值缓存）
# --------------------------------------------------------------------------- #
def _fetch_a_share_codes() -> list:
    """用 easy-tdx 逐市场拉取全部 A 股代码（仅筛选沪/深/京 A 股前缀）。

    该函数本身可能较慢（TDX 服务器偶发粘滞），调用方会用线程 + wall-clock
    预算包裹，避免无限挂起。
    """
    codes: list = []
    seen: set = set()
    try:
        from easy_tdx import TdxClient
        from easy_tdx.models.enums import Market as M
    except ImportError:
        logger.error("未安装 easy-tdx，请先: pip install easy-tdx")
        return codes
    prefixes = {
        M.SH: ("60", "68"),   # 沪市主板 / 科创板
        M.SZ: ("00", "30"),   # 深市主板 / 创业板
        M.BJ: ("8", "4"),     # 北交所
    }
    try:
        client = TdxClient(auto_reconnect=False)
        client.connect()
        for mkt, pres in prefixes.items():
            start = 0
            for _ in range(20):  # 最多 20 页 (20000 支/市场)，安全上限
                try:
                    df = client.get_security_list(mkt, start)
                except Exception as e:
                    logger.warning("%s 第 %d 页失败: %s", mkt.name, start, e)
                    break
                if df is None or len(df) == 0:
                    break
                for _, row in df.iterrows():
                    code = str(row.get("code", "")).strip()
                    name = str(row.get("name", "")).strip()
                    if not code or not name:
                        continue
                    if not code.startswith(pres):
                        continue
                    if code in seen:
                        continue
                    seen.add(code)
                    codes.append({"code": code, "name": name})
                if len(df) < 1000:
                    break
                start += 1000
            logger.info("%s 市场抓取 %d 支 A 股", mkt.name,
                        sum(1 for c in codes if c["code"].startswith(pres)))
    except Exception as e:
        logger.warning("TDX 整体拉取失败: %s", e)
    finally:
        try:
            client.disconnect()
        except Exception:
            pass
    return codes


def _fetch_a_share_codes_adata() -> list:
    """备用数据源：用 adata 获取全量 A 股代码与名称。

    easy-tdx 在本环境（沙箱 TDX 服务器返回异常/分页粘滞）可能拿不到数据，
    此时改用 adata（开源 A 股数据库，多数据源融合，在本环境可正常返回约 5500 支）。
    仅作为兜底，不影响“优先使用 easy-tdx”的设计初衷。
    """
    try:
        import adata
        df = adata.stock.info.all_code()
    except Exception as e:
        logger.warning("adata 获取代码失败: %s", e)
        return []
    out: list = []
    for _, r in df.iterrows():
        code = str(r.get("stock_code", "")).strip()
        name = str(r.get("short_name", "")).strip()
        if code.isdigit():
            code = code.zfill(6)          # 防止 pandas 把 000001 读成整数 1
        if code and name:
            out.append({"code": code, "name": name})
    logger.info("adata 返回 %d 支 A 股", len(out))
    return out


def get_a_share_codes(cache_file="a_stocks.json", refresh=False, max_age_days=7,
                      budget: int = 30, min_codes: int = 4000) -> list:
    """获取全部 A 股代码与名称；结果缓存到本地 JSON。

    优先使用 easy-tdx；若其在本环境返回不足（沙箱 TDX 服务器异常），自动改用
    adata 兜底，确保总能拿到全量 A 股代码。网络拉取由线程包裹并受 wall-clock
    预算约束（默认 30s），超时则采用已获取的部分代码返回，确保脚本（含每日自动化）
    不会因 TDX 服务器粘滞而无限挂起。仅当确实拉到代码且达阈值时才写入缓存。

    返回: [{"code": "600000", "name": "浦发银行"}, ...]
    """
    if not refresh and os.path.exists(cache_file):
        try:
            cache = json.load(open(cache_file, encoding="utf-8"))
            updated = cache.get("updated", "")
            try:
                age = (today_cst() - datetime.strptime(updated, "%Y-%m-%d").date()).days
            except Exception:
                age = 999
            if cache.get("stocks") and age <= max_age_days:
                logger.info("使用缓存代码清单(%s, %d 支)", updated, len(cache["stocks"]))
                return cache["stocks"]
        except Exception as e:
            logger.warning("读取缓存失败: %s", e)

    holder = {"codes": []}

    def worker():
        holder["codes"] = _fetch_a_share_codes()

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=budget)
    if t.is_alive():
        logger.warning("TDX 拉取超时(%ss)，使用已获取的部分代码(%d 支)",
                       budget, len(holder["codes"]))
    codes = holder["codes"]

    # 兜底：若 easy-tdx 返回"部分结果"(介于阈值以下)，不应直接缓存这份残缺清单，
    # 否则后续每天只爬这几百支。阈值 min_codes 设为 4000（A股全量约 5500 支），
    # 不足则强制合并 adata 结果取并集，避免长期使用残缺清单。
    if len(codes) < min_codes:
        if codes:
            logger.warning("easy-tdx 仅返回 %d 支（低于阈值 %d），尝试 adata 补齐",
                           len(codes), min_codes)
        else:
            logger.warning("easy-tdx 未返回代码，改用 adata 兜底")
        alt = _fetch_a_share_codes_adata()
        if alt:
            seen = {c["code"] for c in codes}
            added = 0
            for c in alt:
                if c["code"] not in seen:
                    codes.append(c)
                    seen.add(c["code"])
                    added += 1
            logger.info("合并 adata 后代码数: %d（原 %d + 新增 %d）",
                        len(codes), len(seen) - added, added)

    # 仅当达到最低阈值才写缓存，避免把残缺清单永久固化；低于阈值时让下次运行重试拉全量
    if codes and len(codes) >= min_codes:
        cache = {"updated": today_cst().isoformat(), "stocks": codes}
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            logger.info("代码清单已缓存到 %s (%d 支)", cache_file, len(codes))
        except Exception as e:
            logger.warning("缓存代码清单失败: %s", e)
    elif codes:
        logger.warning("代码数 %d 仍低于缓存阈值 %d，本次不写缓存，下次运行重试拉取全量。",
                       len(codes), min_codes)
    else:
        logger.error("TDX 未返回任何代码（可能网络受限），未更新缓存。")
    return codes


# --------------------------------------------------------------------------- #
# 统一续跑 crawl_stats（v3, source 区分 baidu/em）
# --------------------------------------------------------------------------- #
def ensure_crawl_stats_source(conn) -> None:
    """幂等为 crawl_stats 补 source 列（旧库无该列时）。已有行默认 'baidu'。"""
    existing = {r[1] for r in conn.execute("PRAGMA table_info(crawl_stats)").fetchall()}
    if "source" not in existing:
        conn.execute("ALTER TABLE crawl_stats ADD COLUMN source TEXT DEFAULT 'baidu'")


def bump_crawl_stats(db_path: str, code: str, success: bool, status: str,
                     source: str = "baidu", conn=None) -> None:
    """累计更新逐股抓取统计（crawl_stats 表）。

    - crawl_count：每次实际尝试 +1（成功/空壳/失败都计；续跑跳过不计）。
    - last_success：1=本次拿到真实分析，0=空壳或失败。
    - last_status：'ok' / 'empty'(本次未拿到，待核对) / 'fail'。empty 不是确认无数据。
    - last_attempt / updated_at：日历日 / 精确时间戳。
    - source：'baidu' | 'em'，区分数据源，使两爬虫共用一张表且互不影响判重。
    - 若传入 conn（复用调用方单连接），则不自行开关连接，避免 WAL 锁竞争。
    """
    owned = conn is None
    if owned:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout=15000")  # 与 Rust 端一致；遇并发锁等待而非报错
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS crawl_stats ("
            "code TEXT NOT NULL, crawl_count INTEGER NOT NULL DEFAULT 0, "
            "last_success INTEGER NOT NULL DEFAULT 0, last_status TEXT, "
            "last_attempt TEXT, updated_at TEXT, "
            "source TEXT NOT NULL DEFAULT 'baidu', "
            "PRIMARY KEY(code, source))"
        )
        ensure_crawl_stats_source(conn)
        conn.execute(
            "INSERT INTO crawl_stats (code, crawl_count, last_success, last_status, "
            "last_attempt, updated_at, source) VALUES (?, 1, ?, ?, ?, ?, ?) "
            "ON CONFLICT(code, source) DO UPDATE SET "
            "crawl_count = crawl_count + 1, "
            "last_success = excluded.last_success, "
            "last_status = excluded.last_status, "
            "last_attempt = excluded.last_attempt, "
            "updated_at = excluded.updated_at, "
            "source = excluded.source",
            (code, 1 if success else 0, status, today_cst().isoformat(), _now_cst_str(), source),
        )
        if owned:
            conn.commit()
    finally:
        if owned:
            conn.close()


def skip_recent_ok(conn, code: str, source: str, fresh_days: int = 2) -> bool:
    """v3 续跑判定：近 fresh_days 天该 source 成功抓取过 -> 跳过（True），不发请求。

    从未成功 / 或成功日已早于窗口 / 未拿到(empty，待人工确认) / 失败 -> 返回 False（需重爬）。
    empty 只表示本次爬虫没拿到，不是源站已确认无数据，因此不能当成功跳过。
    """
    cutoff = (today_cst() - timedelta(days=fresh_days)).isoformat()[:10]
    return conn.execute(
        "SELECT 1 FROM crawl_stats WHERE code=? "
        "AND COALESCE(source,'baidu')=? "
        "AND last_status='ok' "
        "AND substr(last_attempt,1,10) >= ? LIMIT 1",
        (code, source, cutoff)).fetchone() is not None


# --------------------------------------------------------------------------- #
# 源站核对链接（未拿到 ≠ 确认无数据；给人打开网页核对）
# 规则对齐现有爬虫：百度用 build_page_url / analysis 接口；东财用千股千评个股页。
# --------------------------------------------------------------------------- #
BAIDU_API_HOST = "https://finance.pae.baidu.com"
BAIDU_PAGE_HOST = "https://finance.baidu.com"
EM_PAGE_HOST = "https://data.eastmoney.com"
EM_API_BASE = "https://datacenter-web.eastmoney.com/api/data/v1/get"


def baidu_ai_page_url(code: str, market: str = "ab", finance_type: str = "stock") -> str:
    """百度财经 AI 分析页（爬虫 Referer，可在浏览器打开）。"""
    return f"{BAIDU_PAGE_HOST}/ai-tech-analysi/{finance_type}/{market}-{code}"


def baidu_stock_page_url(code: str, market: str = "ab") -> str:
    """百度财经个股页。"""
    return f"{BAIDU_PAGE_HOST}/stock/{market}-{code}"


def baidu_analysis_api_url(code: str, market: str = "ab", finance_type: str = "stock") -> str:
    """百度五维评分接口（本次实际请求的那条）。"""
    return f"{BAIDU_API_HOST}/vapi/v1/analysis?{urlencode({'code': code, 'market': market, 'financeType': finance_type})}"


def baidu_kline_api_url(code: str, cycle: str, market: str = "ab",
                       finance_type: str = "stock") -> str:
    """百度支撑/阻力接口；cycle=long|short 写进 URL。"""
    return (
        f"{BAIDU_API_HOST}/sapi/v1/get_analyse?"
        f"{urlencode({'code': code, 'market': market, 'financeType': finance_type, 'cycle': cycle})}"
    )


def em_stockcomment_page_url(code: str) -> str:
    """东财千股千评个股页（已实测：/stockcomment/stock/000001.html）。"""
    return f"{EM_PAGE_HOST}/stockcomment/stock/{code}.html"


def em_stockcomment_list_url() -> str:
    """东财千股千评列表页（批量接口的 Referer）。"""
    return f"{EM_PAGE_HOST}/stockcomment/"


def em_datacenter_url(report: str, extra: dict | None = None) -> str:
    """东财 datacenter-web 接口（带 reportName + 已有 filter/分页参数）。"""
    qs = {
        "reportName": report,
        "columns": "ALL",
        "client": "PC",
        "source": "WEB",
    }
    if extra:
        qs.update(extra)
    return EM_API_BASE + "?" + urlencode(qs)


def source_verify_links(
    code: str,
    sources: list | tuple = ("baidu",),
    *,
    market: str = "ab",
    finance_type: str = "stock",
    extra_request_urls: list | None = None,
) -> list:
    """返回人工核对用链接列表：[{source, kind, label, url}, ...]。

    kind=page 给人打开；kind=request 是本次实际打过的接口（浏览器打开可见 JSON）。
    只拼现有数据源真实规则，不编造不可用 URL。
    """
    links = []
    srcs = {str(s).lower() for s in (sources or ())}
    if "baidu" in srcs:
        links.append({
            "source": "百度财经", "kind": "page", "label": "AI分析页",
            "url": baidu_ai_page_url(code, market, finance_type),
        })
        links.append({
            "source": "百度财经", "kind": "page", "label": "个股页",
            "url": baidu_stock_page_url(code, market),
        })
        links.append({
            "source": "百度财经", "kind": "request", "label": "五维评分接口",
            "url": baidu_analysis_api_url(code, market, finance_type),
        })
        for cyc, lab in (("long", "支撑阻力·长期"), ("short", "支撑阻力·短期")):
            links.append({
                "source": "百度财经", "kind": "request", "label": lab,
                "url": baidu_kline_api_url(code, cyc, market, finance_type),
            })
    if "em" in srcs:
        links.append({
            "source": "东方财富", "kind": "page", "label": "千股千评个股页",
            "url": em_stockcomment_page_url(code),
        })
        links.append({
            "source": "东方财富", "kind": "page", "label": "千股千评列表页",
            "url": em_stockcomment_list_url(),
        })
        links.append({
            "source": "东方财富", "kind": "request", "label": "估值接口",
            "url": em_datacenter_url(
                "RPT_VALUEANALYSIS_DET",
                {"filter": '(SECURITY_CODE="%s")' % code},
            ),
        })
    seen = {x["url"] for x in links}
    for item in extra_request_urls or []:
        if isinstance(item, dict):
            url = item.get("url") or ""
            if not url or url in seen:
                continue
            links.append({
                "source": item.get("source") or "数据源",
                "kind": item.get("kind") or "request",
                "label": item.get("label") or "本次请求",
                "url": url,
            })
            seen.add(url)
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            lab, url = str(item[0]), str(item[1])
            if url and url not in seen:
                links.append({
                    "source": "数据源", "kind": "request", "label": lab, "url": url,
                })
                seen.add(url)
        elif isinstance(item, str) and item and item not in seen:
            links.append({
                "source": "数据源", "kind": "request", "label": "本次请求接口",
                "url": item,
            })
            seen.add(item)
    return links


def format_unconfirmed_empty_msg(
    code: str,
    name: str = "",
    sources: list | tuple = ("baidu",),
    extra_request_urls: list | None = None,
    reason: str = "empty",
    market: str = "ab",
    finance_type: str = "stock",
) -> str:
    """日志/终端用：本次未拿到或失败，附源站链接，供人工打开核对。"""
    what = "失败" if reason == "fail" else "未拿到数据"
    head = f"股票 {code}"
    if name:
        head += f" {name}"
    head += f" 本次爬取{what}（待人工确认，不是确认无数据）。请打开源站核对："
    lines = [head]
    for L in source_verify_links(
        code, sources, market=market, finance_type=finance_type,
        extra_request_urls=extra_request_urls,
    ):
        lines.append(f"  {L['source']} · {L['label']}: {L['url']}")
    return "\n".join(lines)
