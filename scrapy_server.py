#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度财经 AI 爬虫 · 合并调试页（看结果 + 触发抓取，零依赖本地服务）
============================================================

用途:
    调试期替代"反复编译 Rust GUI / 反复手敲命令"。一个本地服务页搞定：
      1) 看解析结果 —— 实时读取 market_data.db 的四表，表格化展示，
         支持 代码/名称搜索、按真实分析日(update_time)过滤、按状态过滤、排序、
         点击行展开看 支撑阻力/资金流向/投票。
      2) 触发抓取 —— 点"开始全市场抓取"即用 sys.executable 跑
         baidu_finance_ai_crawler.py --market，实时日志流回页面，跑完自动刷新结果。
      3) 停止 —— 抓取中可点"停止"终止子进程，避免失控。

特点:
    * 纯 Python 标准库（http.server + threading），无第三方依赖。
    * 仅绑定 127.0.0.1（本机调试），不暴露到网络。
    * 数据通过 /api/data 实时拉取，无需手动重生成 HTML。

用法:
    python scrapy_server.py                 # 默认 http://127.0.0.1:8765
    python scrapy_server.py --port 9000     # 指定端口
    浏览器打开 http://127.0.0.1:8765
"""

from __future__ import annotations

import argparse
import re
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("baidu_finance_server")

# 打包成 exe 后子进程仍调用本 exe，用该参数转去跑对应爬虫模块
_RUN_SCRIPT_FLAG = "--run-script"


def _is_frozen() -> bool:
    """PyInstaller 打成 exe 后 sys.frozen=True。"""
    return bool(getattr(sys, "frozen", False))


def _app_dir() -> str:
    """可写目录：exe 旁（打包）或 .py 旁（源码）。db/日志/代码清单都落这里。"""
    if _is_frozen():
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


HERE = _app_dir()
SCRIPT_PATH = os.path.join(HERE, "baidu_finance_ai_crawler.py")
EM_SCRIPT_PATH = os.path.join(HERE, "eastmoney_stockcomment_crawler.py")
LOG_PATH = os.path.join(HERE, "crawl_auto.log")
DB_PATH = os.path.join(HERE, "market_data.db")

# 复用 view_results 的读取逻辑（同目录，安全 import）
sys.path.insert(0, HERE)
import view_results  # noqa: E402


def _crawler_ready(script_path: str) -> bool:
    """源码模式检查 .py 是否在；exe 模式爬虫已打进包，视为可用。"""
    if _is_frozen():
        return True
    return os.path.exists(script_path)


def _ensure_bundled_files() -> None:
    """首次双击 exe：把包内的 A 股代码清单拷到 exe 旁边，供爬虫当缓存用。"""
    if not _is_frozen():
        return
    meipass = getattr(sys, "_MEIPASS", "")
    if not meipass:
        return
    import shutil
    for name in ("a_stocks.json",):
        dst = os.path.join(HERE, name)
        src = os.path.join(meipass, name)
        if not os.path.exists(dst) and os.path.exists(src):
            shutil.copy2(src, dst)


def _now_cst() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- #
# 全局运行状态（带锁）
# --------------------------------------------------------------------------- #
STATE: dict = {
    "running": False,
    "proc": None,
    "log": [],
    "started_at": None,
    "finished_at": None,
    "exit_code": None,
    "last_summary": None,
    "round": {"planned": None, "scraped": 0, "skip": 0,
              "done": 0, "empty": 0, "fail": 0, "blocked403": False},
}
STATE_LOCK = threading.Lock()
HTTP_SERVER = None  # main() 赋值
_SHUTDOWN_TIMER: threading.Timer | None = None
_SHUTDOWN_LOCK = threading.Lock()
_SELF_PID = os.getpid()

# 解析子进程实时日志，提取本轮进度计数
PROG_RE = re.compile(r"\[(\d+)/(\d+)\] 真实 (\d+) (?:未拿到|空壳) (\d+) 跳过 (\d+) 失败 (\d+)")
BLK_RE = re.compile(r"持久封禁|停止剩余抓取|仍遭 403")
PLAN_RE = re.compile(r"共 (\d+) 支")                       # 全市场抓取 … 共 5532 支
# 调试页逐股进度行：[prog] 001365 ok / skip / empty / fail
PROG_LINE_RE = re.compile(r"\[prog\] (\d{6}) (\w+)")


def _append_log(line: str, lf=None) -> None:
    """把一行子进程输出写入 STATE 日志，并解析进度计数。"""
    line = line.rstrip("\n\r")

    # 进度计数始终更新；逐股 [prog] 行不写入 UI 日志，避免上万行刷屏卡顿
    mp_prog = PROG_LINE_RE.search(line)
    store_in_ui = mp_prog is None

    with STATE_LOCK:
        if store_in_ui:
            STATE["log"].append(line)
            if len(STATE["log"]) > 500:
                STATE["log"] = STATE["log"][-300:]
        m = PROG_RE.search(line)
        r = STATE["round"]
        if m:
            cur, total, done, empty, skip, fail = (int(x) for x in m.groups())
            r["planned"] = total
            r["scraped"] = cur
            r["done"] = done
            r["empty"] = empty
            r["skip"] = skip
            r["fail"] = fail
        else:
            if mp_prog:
                r["scraped"] = (r["scraped"] or 0) + 1
                st = mp_prog.group(2)
                if st == "ok":
                    r["done"] = (r["done"] or 0) + 1
                elif st == "skip":
                    r["skip"] = (r["skip"] or 0) + 1
                elif st == "empty":
                    r["empty"] = (r["empty"] or 0) + 1
                elif st == "fail":
                    r["fail"] = (r["fail"] or 0) + 1
            mp2 = PLAN_RE.search(line)
            if mp2:
                r["planned"] = int(mp2.group(1))
        if BLK_RE.search(line):
            r["blocked403"] = True
    if lf is not None:
        try:
            lf.write(line + "\n")
            lf.flush()
        except Exception:
            pass


def _decode_pipe_line(raw: bytes) -> str:
    """子进程在 Windows 上常输出 GBK；优先 utf-8，失败再 gbk。"""
    if not raw:
        return ""
    for enc in ("utf-8", "gbk", "cp936"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _drain_stdout(proc: subprocess.Popen, header: str) -> int:
    """实时读取子进程 stdout/stderr 合并流，返回退出码。"""
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as lf:
            lf.write("\n===== %s @ %s =====\n" % (header, _now_cst()))
            lf.flush()
            _append_log("[%s] 开始" % header, lf)
            if proc.stdout is not None:
                while True:
                    raw = proc.stdout.readline()
                    if not raw:
                        break
                    _append_log(_decode_pipe_line(raw), lf)
    except Exception as e:
        _append_log("[pump错误] %s" % e)
        logger.exception("读取子进程输出失败")
    finally:
        try:
            proc.wait(timeout=30)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            proc.wait()
    return proc.returncode if proc.returncode is not None else -1


def _finish_run(exit_code: int, phase: str | None = None) -> None:
    with STATE_LOCK:
        STATE["running"] = False
        STATE["finished_at"] = _now_cst()
        STATE["exit_code"] = exit_code
        if phase:
            STATE["round"]["phase"] = phase
        summary = []
        for ln in STATE["log"]:
            if ("全市场抓取完成" in ln or ln.strip().startswith("总计")
                    or ln.strip().startswith("数据库:")):
                summary.append(ln)
        STATE["last_summary"] = ("\n".join(summary[-3:]) if summary
                                  else "进程退出码 %s" % exit_code)
        STATE["proc"] = None


def _popen_crawler(script: str, env: dict, extra_args: list | None = None) -> subprocess.Popen:
    """统一启动子进程：二进制管道 + 智能解码；子进程尽量 UTF-8。"""
    env = dict(env)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    if _is_frozen():
        # exe 里没有独立 python，子进程再拉起自身并带 --run-script 转去爬虫模块
        cmd = [sys.executable, _RUN_SCRIPT_FLAG, os.path.basename(script),
               "--market", "--progress-log"]
    else:
        cmd = [sys.executable, "-u", script, "--market", "--progress-log"]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.Popen(
        cmd,
        cwd=HERE, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        # 二进制读取，由 _decode_pipe_line 处理 utf-8/gbk
        bufsize=0,
    )


def _rate_args_from_opts(opts: dict | None) -> list:
    """从 Web 传入的 opts 生成限流 CLI 参数。"""
    opts = opts or {}
    args = []
    try:
        mi = float(opts.get("min_interval", 1.0))
        if mi > 0:
            args += ["--min-interval", str(mi)]
    except (TypeError, ValueError):
        args += ["--min-interval", "1.0"]
    try:
        mpm = int(opts.get("max_per_minute", 40))
        if mpm > 0:
            args += ["--max-per-minute", str(mpm)]
    except (TypeError, ValueError):
        args += ["--max-per-minute", "40"]
    # 达上限后最多等待：空/缺省=不限制；0=不等待
    if "rate_wait_cap" in opts and opts.get("rate_wait_cap") not in (None, ""):
        try:
            cap = float(opts.get("rate_wait_cap"))
            if cap >= 0:
                args += ["--rate-wait-cap", str(cap)]
        except (TypeError, ValueError):
            pass
    try:
        win = float(opts.get("rate_window", 60))
        if win > 0:
            args += ["--rate-window", str(win)]
    except (TypeError, ValueError):
        args += ["--rate-window", "60"]
    return args


def _read_json_body(handler: "Handler") -> dict:
    n = int(handler.headers.get("Content-Length") or 0)
    if n <= 0:
        return {}
    raw = handler.rfile.read(n)
    try:
        return json.loads(raw.decode("utf-8") or "{}")
    except Exception:
        return {}


def _pump(proc: subprocess.Popen) -> None:
    """读取子进程输出，追加到日志、同步写 crawl_auto.log，结束后更新状态。"""
    code = _drain_stdout(proc, "合并页触发抓取")
    _finish_run(code)


def start_scrape(opts: dict | None = None) -> tuple[bool, str]:
    with STATE_LOCK:
        if STATE["running"]:
            return False, "已有抓取任务在运行中，请先等其结束或点停止"
        STATE["running"] = True
        STATE["log"] = []
        STATE["started_at"] = _now_cst()
        STATE["finished_at"] = None
        STATE["exit_code"] = None
        STATE["last_summary"] = None
        STATE["round"] = {"planned": None, "scraped": 0, "skip": 0,
                          "done": 0, "empty": 0, "fail": 0, "blocked403": False,
                          "phase": "baidu"}
        STATE["rate_opts"] = dict(opts or {})
    
    # 加载 Cookie 并传递给爬虫
    from baidu_cookie_pool import get_cookie_pool
    pool = get_cookie_pool()
    session = pool.get_best_session()
    
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    
    # 如果有有效 Cookie，写入环境变量供爬虫读取
    if session and session.cookies:
        env["_BAIDU_COOKIE_DICT"] = json.dumps(session.cookies)
        logger.info(f"[OK] 已传递 {len(session.cookies)} 个 Cookie 给百度爬虫")
    else:
        logger.warning("[WARN] 无有效 Cookie，爬虫将不使用 Cookie")
    
    rate_args = _rate_args_from_opts(opts)
    proc = _popen_crawler(SCRIPT_PATH, env, rate_args)
    with STATE_LOCK:
        STATE["proc"] = proc
        STATE["log"].append("[%s] 已启动百度抓取子进程 pid=%s %s"
                            % (_now_cst(), proc.pid, " ".join(rate_args)))
    threading.Thread(target=_pump, args=(proc,), daemon=True).start()
    return True, "已开始百度全市场抓取（%s）" % " ".join(rate_args)


def start_scrape_em(opts: dict | None = None) -> tuple[bool, str]:
    """触发东方财富「千股千评 + 估值」全市场抓取。复用同一运行状态锁，避免与百度抓取并发写库。"""
    with STATE_LOCK:
        if STATE["running"]:
            return False, "已有抓取任务在运行中，请先等其结束或点停止"
        STATE["running"] = True
        STATE["log"] = []
        STATE["started_at"] = _now_cst()
        STATE["finished_at"] = None
        STATE["exit_code"] = None
        STATE["last_summary"] = None
        STATE["round"] = {"planned": None, "scraped": 0, "skip": 0,
                          "done": 0, "empty": 0, "fail": 0, "blocked403": False,
                          "phase": "eastmoney"}
        STATE["rate_opts"] = dict(opts or {})
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    if not _crawler_ready(EM_SCRIPT_PATH):
        with STATE_LOCK:
            STATE["running"] = False
        return False, "未找到东方财富爬虫脚本: %s" % EM_SCRIPT_PATH
    rate_args = _rate_args_from_opts(opts)
    proc = _popen_crawler(EM_SCRIPT_PATH, env, rate_args)
    with STATE_LOCK:
        STATE["proc"] = proc
        STATE["log"].append("[%s] 已启动东财抓取子进程 pid=%s %s"
                            % (_now_cst(), proc.pid, " ".join(rate_args)))
    threading.Thread(target=_pump, args=(proc,), daemon=True).start()
    return True, "已开始东方财富全市场抓取（%s）" % " ".join(rate_args)


def start_combined_crawl(opts: dict | None = None) -> tuple[bool, str]:
    """一键启动：先百度，再东财（无缝衔接）"""
    with STATE_LOCK:
        if STATE["running"]:
            return False, "已有抓取任务在运行中，请先等其结束或点停止"
        STATE["running"] = True
        STATE["log"] = []
        STATE["started_at"] = _now_cst()
        STATE["finished_at"] = None
        STATE["exit_code"] = None
        STATE["last_summary"] = None
        STATE["round"] = {"planned": None, "scraped": 0, "skip": 0,
                          "done": 0, "empty": 0, "fail": 0, "blocked403": False,
                          "phase": "combined_baidu"}
        STATE["rate_opts"] = dict(opts or {})
    
    # 加载 Cookie
    from baidu_cookie_pool import get_cookie_pool
    pool = get_cookie_pool()
    session = pool.get_best_session()
    
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    
    if session and session.cookies:
        env["_BAIDU_COOKIE_DICT"] = json.dumps(session.cookies)
        logger.info(f"[OK] 组合抓取已传递 {len(session.cookies)} 个 Cookie")
    else:
        logger.warning("[WARN] 无有效 Cookie")
    
    rate_args = _rate_args_from_opts(opts)
    proc = _popen_crawler(SCRIPT_PATH, env, rate_args)
    with STATE_LOCK:
        STATE["proc"] = proc
        STATE["log"].append("[%s] 已启动组合抓取-百度阶段 pid=%s %s"
                            % (_now_cst(), proc.pid, " ".join(rate_args)))
    threading.Thread(target=_pump_combined, args=(proc,), daemon=True).start()
    return True, "已启动组合抓取（百度→东财）（%s）" % " ".join(rate_args)


def _pump_combined(proc: subprocess.Popen) -> None:
    """组合爬取：实时泵百度日志，成功后再泵东财；全程写 STATE['log']。"""
    try:
        code = _drain_stdout(proc, "组合抓取-百度阶段")
        with STATE_LOCK:
            STATE["exit_code"] = code

        if code != 0:
            _append_log("[组合] 百度退出码 %s，跳过东财" % code)
            _finish_run(code, phase="baidu_failed")
            return

        if not _crawler_ready(EM_SCRIPT_PATH):
            _append_log("[组合] 未找到东财脚本，结束")
            _finish_run(code, phase="em_missing")
            return

        _append_log("[组合] 百度完成，开始东方财富抓取…")
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        try:
            from enhance_eastmoney_crawler import get_em_cookie_manager
            get_em_cookie_manager()  # 预热 Cookie 管理器（可选）
        except Exception as e:
            _append_log("[组合] 东财 Cookie 管理器跳过: %s" % e)

        with STATE_LOCK:
            rate_opts = dict(STATE.get("rate_opts") or {})
        rate_args = _rate_args_from_opts(rate_opts)
        proc_em = _popen_crawler(EM_SCRIPT_PATH, env, rate_args)
        with STATE_LOCK:
            STATE["round"]["phase"] = "combined_eastmoney"
            STATE["proc"] = proc_em
            STATE["log"].append("[%s] 已启动东财阶段 pid=%s %s"
                                % (_now_cst(), proc_em.pid, " ".join(rate_args)))

        code_em = _drain_stdout(proc_em, "组合抓取-东财阶段")
        _finish_run(code_em, phase="completed")
    except Exception as e:
        logger.exception("组合抓取错误")
        _append_log("[组合错误] %s" % e)
        _finish_run(-1, phase="error")


def stop_scrape() -> tuple[bool, str]:
    with STATE_LOCK:
        proc = STATE["proc"]
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        with STATE_LOCK:
            STATE["proc"] = None
            STATE["running"] = False
        return True, "已发送停止信号"
    return False, "当前没有运行中的抓取"


def _kill_matching_python(patterns: list[str], exclude_self: bool = True) -> list[int]:
    """Windows：按命令行关键字强制结束相关 python/本 exe 进程，返回被杀 PID 列表。"""
    killed: list[int] = []
    if sys.platform != "win32":
        return killed
    exe_name = os.path.basename(sys.executable)
    names = ["python.exe", "pythonw.exe"]
    if _is_frozen() and exe_name and exe_name.lower() not in names:
        names.append(exe_name)
    rows: list[tuple[int, str]] = []
    try:
        for name in names:
            safe = name.replace("'", "")
            ps = (
                f"Get-CimInstance Win32_Process -Filter \"Name='{safe}'\" | "
                "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
            )
            out = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command", ps],
                text=True, errors="replace", timeout=20,
            )
            data = json.loads(out) if out.strip() else []
            if isinstance(data, dict):
                data = [data]
            rows.extend(
                (int(x.get("ProcessId") or 0), str(x.get("CommandLine") or ""))
                for x in data
            )
    except Exception as e:
        logger.warning("枚举进程失败: %s", e)
        return killed

    for pid, cmd in rows:
        if not pid:
            continue
        if exclude_self and pid == _SELF_PID:
            continue
        cmd_l = (cmd or "").lower()
        if not any(p.lower() in cmd_l for p in patterns):
            continue
        try:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)],
                           capture_output=True, timeout=10)
            killed.append(pid)
            logger.info("已结束进程 pid=%s", pid)
        except Exception as e:
            logger.warning("结束 pid=%s 失败: %s", pid, e)
    return killed


def _cancel_pending_shutdown() -> None:
    global _SHUTDOWN_TIMER
    with _SHUTDOWN_LOCK:
        if _SHUTDOWN_TIMER is not None:
            try:
                _SHUTDOWN_TIMER.cancel()
            except Exception:
                pass
            _SHUTDOWN_TIMER = None


def _do_shutdown_now(reason: str = "") -> None:
    """停止抓取子进程、清理相关 python，并关闭 HTTP 服务。"""
    logger.info("关闭 Web 后端: %s", reason or "manual")
    try:
        stop_scrape()
    except Exception:
        pass
    patterns = [
        "baidu_finance_ai_crawler.py",
        "eastmoney_stockcomment_crawler.py",
        "baidu_selenium_fallback",
        "scrapy_server.py",
        _RUN_SCRIPT_FLAG,
    ]
    _kill_matching_python(patterns, exclude_self=True)

    def _stop_server():
        time.sleep(0.25)
        srv = HTTP_SERVER
        if srv is not None:
            try:
                srv.shutdown()
            except Exception:
                pass
            try:
                srv.server_close()
            except Exception:
                pass
        # 确保本进程退出（避免残留监听）
        time.sleep(0.15)
        os._exit(0)

    threading.Thread(target=_stop_server, daemon=True).start()


def schedule_shutdown(delay_sec: float = 2.5, reason: str = "page_closed") -> str:
    """关页/异常退出：延迟关闭，便于 F5 刷新时被后续请求取消。"""
    global _SHUTDOWN_TIMER
    with _SHUTDOWN_LOCK:
        if _SHUTDOWN_TIMER is not None:
            try:
                _SHUTDOWN_TIMER.cancel()
            except Exception:
                pass
        t = threading.Timer(delay_sec, lambda: _do_shutdown_now(reason))
        t.daemon = True
        _SHUTDOWN_TIMER = t
        t.start()
    return "将在 %.1f 秒后关闭后端（刷新页面可取消）" % delay_sec


def force_shutdown(reason: str = "exit_button") -> str:
    """退出按钮：立即关闭。"""
    _cancel_pending_shutdown()
    _do_shutdown_now(reason)
    return "正在关闭 Web 后端及抓取进程"


# --------------------------------------------------------------------------- #
# 历史日完整性分析（已抓真实支数 vs 全集）
# --------------------------------------------------------------------------- #
def analyze_completeness(db_path: str) -> dict:
    """按历史日分析抓取完整性：已抓真实数据支数 vs 全集（代码清单）支数。

    两个维度：
      - by_update_time: 按百度真实分析日(update_time)，该日有多少支股票有真实(ok)分析。
      - by_trade_date : 按抓取批次日(trade_date)，该批次成功落库(ok)的支数（含空壳数）。
    全集(分母)取自 a_stocks.json 代码清单；缺失时回退为“历史出现过 ok 的去重支数”。
    """
    import json as _json
    universe = None
    cache_file = os.path.join(HERE, "a_stocks.json")
    if os.path.exists(cache_file):
        try:
            cache = _json.load(open(cache_file, encoding="utf-8"))
            stocks = cache.get("stocks") or []
            if stocks:
                universe = len(stocks)
        except Exception:
            pass
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=5000")
    cur = conn.cursor()
    if universe is None:
        try:
            universe = cur.execute(
                "SELECT COUNT(DISTINCT code) FROM scores "
                "WHERE COALESCE(status,'ok')='ok'"
            ).fetchone()[0] or 0
        except Exception:
            universe = 0
    cur.execute(
        "SELECT update_time, COUNT(DISTINCT code) FROM scores "
        "WHERE COALESCE(status,'ok')='ok' AND update_time IS NOT NULL AND update_time<>'' "
        "GROUP BY update_time ORDER BY update_time DESC"
    )
    by_ut = [{"day": r[0], "ok": r[1]} for r in cur.fetchall()]
    cur.execute(
        "SELECT trade_date, "
        "SUM(CASE WHEN COALESCE(status,'ok')='ok' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN COALESCE(status,'ok')<>'ok' THEN 1 ELSE 0 END) "
        "FROM scores GROUP BY trade_date ORDER BY trade_date DESC"
    )
    by_td = [{"day": r[0], "ok": r[1], "empty": r[2]} for r in cur.fetchall()]
    conn.close()
    for d in by_ut:
        d["coverage"] = round(100.0 * d["ok"] / universe, 1) if universe else 0.0
    for d in by_td:
        d["coverage"] = round(100.0 * d["ok"] / universe, 1) if universe else 0.0
    return {"ok": True, "universe": universe, "by_update_time": by_ut, "by_trade_date": by_td}


# --------------------------------------------------------------------------- #
# HTTP 处理
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        # 任何正常访问都取消「关页延迟关闭」，避免 F5 误杀后端
        if path not in ("/api/page_closed", "/api/shutdown"):
            _cancel_pending_shutdown()
        if path in ("/", "/index.html"):
            self._send_html(PAGE_HTML)
        elif path == "/api/data":
            res = view_results.load(DB_PATH)
            if not res.get("ok"):
                self._send_json({"ok": False, "error": res.get("error")}, 200)
            else:
                self._send_json({"ok": True, "meta": res["meta"],
                                 "records": res["records"]}, 200)
        elif path == "/api/status":
            with STATE_LOCK:
                self._send_json({
                    "running": STATE["running"],
                    "started_at": STATE["started_at"],
                    "finished_at": STATE["finished_at"],
                    "exit_code": STATE["exit_code"],
                    "last_summary": STATE["last_summary"],
                    "round": STATE["round"],
                    "log": STATE["log"][-200:],
                })
        elif path == "/api/completeness":
            try:
                res = analyze_completeness(DB_PATH)
                self._send_json(res)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)})
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path not in ("/api/page_closed", "/api/shutdown"):
            _cancel_pending_shutdown()
        opts = _read_json_body(self)
        if path == "/api/scrape":
            ok, msg = start_scrape(opts)
            self._send_json({"started": ok, "msg": msg})
        elif path == "/api/scrape_em":
            ok, msg = start_scrape_em(opts)
            self._send_json({"started": ok, "msg": msg})
        elif path == "/api/scrape_combined":
            ok, msg = start_combined_crawl(opts)
            self._send_json({"started": ok, "msg": msg})
        elif path == "/api/scrape/stop":
            ok, msg = stop_scrape()
            self._send_json({"stopped": ok, "msg": msg})
        elif path == "/api/shutdown":
            msg = force_shutdown("api_shutdown")
            self._send_json({"ok": True, "msg": msg})
        elif path == "/api/page_closed":
            msg = schedule_shutdown(2.5, "page_closed")
            self._send_json({"ok": True, "msg": msg})
        else:
            self._send_json({"error": "not found"}, 404)

    def log_message(self, fmt, *args):
        pass  # 静默访问日志，避免刷屏


# --------------------------------------------------------------------------- #
# 页面（看结果 + 触发抓取 + 实时日志）
# --------------------------------------------------------------------------- #
PAGE_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>百度财经 AI · 抓取+查看 合并调试页</title>
<style>
  :root { --bg:#f7f8fa; --card:#fff; --line:#e5e7eb; --ink:#1f2937; --mut:#6b7280;
          --red:#d12c2c; --green:#0a8f3c; --blue:#2563eb; --amber:#b45309; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;
         background:var(--bg); color:var(--ink); font-size:14px; }
  header { padding:14px 20px; background:var(--card); border-bottom:1px solid var(--line); position:sticky; top:0; z-index:5; }
  h1 { font-size:17px; margin:0 0 10px; }
  .bar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  button { padding:7px 14px; border:1px solid var(--line); border-radius:8px; font-size:13px; cursor:pointer;
           background:#fff; color:var(--ink); }
  button.primary { background:var(--blue); color:#fff; border-color:var(--blue); }
  button.danger { background:#fff; color:var(--red); border-color:var(--red); }
  button:disabled { opacity:.5; cursor:not-allowed; }
  .status { font-size:13px; color:var(--mut); }
  .status .dot { display:inline-block; width:8px; height:8px; border-radius:50%; background:var(--mut); margin-right:5px; vertical-align:middle; }
  .status.run .dot { background:var(--amber); }
  .summary { color:var(--mut); font-size:12px; margin-top:6px; white-space:pre-wrap; }
  .logwrap { margin:12px 20px 0; }
  .logwrap h2 { font-size:13px; color:var(--mut); margin:0 0 6px; }
  pre#log { background:#0f172a; color:#d6e2f0; padding:10px 12px; border-radius:10px; height:180px; overflow:auto;
            font-size:12px; line-height:1.5; white-space:pre-wrap; margin:0; }
  .wrap { padding:16px 20px; }
  .filters { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:10px; }
  input, select { padding:6px 9px; border:1px solid var(--line); border-radius:8px; font-size:13px; background:#fff; color:var(--ink); }
  input[type=text] { min-width:200px; }
  table { border-collapse:collapse; width:100%; background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  th, td { padding:8px 10px; text-align:left; border-bottom:1px solid var(--line); white-space:nowrap; }
  th { background:#f1f3f5; cursor:pointer; user-select:none; position:sticky; top:0; }
  tbody tr { cursor:pointer; }
  tbody tr:hover { background:#f0f6ff; }
  td.num, th.num { text-align:right; font-variant-numeric:tabular-nums; }
  .red { color:var(--red); } .green { color:var(--green); }
  .tag { display:inline-block; padding:1px 7px; border-radius:999px; font-size:12px; }
  .tag.ok { background:#e7f7ec; color:var(--green); }
  .tag.empty { background:#fdeaea; color:var(--red); }
  .detail { background:#fbfcfe; }
  .detail .box { padding:12px 16px; display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:14px; }
  .panel { border:1px solid var(--line); border-radius:10px; padding:10px 12px; background:#fff; }
  .panel h3 { margin:0 0 8px; font-size:13px; color:var(--blue); }
  .kv { display:flex; justify-content:space-between; gap:10px; padding:3px 0; border-bottom:1px dashed #eef0f2; }
  .kv:last-child { border-bottom:none; }
  .kv span:first-child { color:var(--mut); }
  .muted { color:var(--mut); }
  .empty-note { color:var(--red); padding:8px 12px; }
  .src-links { padding:0 12px 12px; }
  .src-row { display:flex; flex-wrap:wrap; gap:8px; align-items:center; padding:4px 0; }
  .src-row a { color:var(--blue); word-break:break-all; }
  .copy-btn { font-size:12px; padding:2px 8px; cursor:pointer; }
  footer { padding:10px 20px; color:var(--mut); font-size:12px; border-top:1px solid var(--line); margin-top:14px; }
  .pill { background:#eef2ff; color:var(--blue); border-radius:999px; padding:1px 8px; font-size:12px; }
  .comphead { display:flex; flex-wrap:wrap; gap:12px; align-items:center; margin-bottom:8px; }
  .comphead h2 { font-size:15px; margin:0; }
  .comptabs { display:flex; gap:8px; margin:6px 0 8px; }
  .comptabs .tab { background:#fff; border:1px solid var(--line); border-radius:8px; padding:5px 12px; cursor:pointer; font-size:13px; }
  .comptabs .tab.active { background:var(--blue); color:#fff; border-color:var(--blue); }
  #compTable th:nth-child(4), #compTable td:nth-child(4) { text-align:left; }
  .bar-cell { position:relative; min-width:120px; }
  .bar-bg { background:#eef0f2; border-radius:6px; height:14px; overflow:hidden; }
  .bar-fg { height:100%; background:var(--blue); }
  .round { display:flex; flex-wrap:wrap; gap:10px; margin-top:10px; align-items:center; }
  .rchip { background:#f1f5ff; border:1px solid var(--line); border-radius:999px; padding:4px 12px;
           font-size:13px; color:var(--mut); }
  .rchip b { color:var(--ink); margin-left:5px; font-variant-numeric:tabular-nums; }
  .rchip.ok b { color:var(--green); }
  .rchip.warn { background:#fdeaea; color:var(--red); border-color:#f5c2c2; }
</style>
</head>
<body>
<header>
  <h1>百度财经 AI · 抓取 + 查看 合并调试页 <span class="pill">本机 127.0.0.1</span></h1>
  <div class="bar">
    <button id="combinedBtn" class="primary" onclick="startCombined()" style="background:#e67e22;border-color:#e67e22;font-size:14px;padding:8px 16px">⚡ 一键抓取（百度+东财）</button>
    <button id="startBtn" class="primary" onclick="startScrape()">▶ 仅百度</button>
    <button id="stopBtn" class="danger" onclick="stopScrape()" disabled>■ 停止</button>
    <button onclick="refreshData()">⟳ 刷新结果</button>
    <button id="startEmBtn" class="primary" onclick="startScrapeEm()" style="background:#0a8f3c;border-color:#0a8f3c">▶ 仅东财</button>
    <button id="exitBtn" class="danger" onclick="exitBackend()" title="关闭 Web 服务与全部抓取进程">⏻ 退出后端</button>
    <span id="status" class="status"><span class="dot"></span><span id="statusText">空闲</span></span>
    <span id="count" class="muted"></span>
  </div>
  <div class="bar" style="margin-top:8px;gap:12px;align-items:center">
    <label class="muted" style="font-size:13px">最小间隔(秒)
      <input id="minInterval" type="number" min="0.05" step="0.1" value="1.0" style="width:72px;margin-left:4px">
    </label>
    <label class="muted" style="font-size:13px">每分钟上限
      <input id="maxPerMinute" type="number" min="1" step="1" value="40" style="width:72px;margin-left:4px">
    </label>
    <label class="muted" style="font-size:13px">达上限最多等待(秒)
      <input id="rateWaitCap" type="number" min="0" step="1" value="15" style="width:72px;margin-left:4px" title="达到每分钟上限后最多空等多少秒；0=不等待立即开新窗口">
    </label>
    <span class="muted" style="font-size:12px">日志里「限流等待 xxx 秒」由此控制（默认最多 15 秒，不再傻等满剩余整分钟）</span>
  </div>
  <div id="summary" class="summary"></div>
  <div class="logwrap">
    <h2>实时日志（crawl_auto.log 同步）</h2>
    <pre id="log">（未开始）</pre>
  </div>
  <div class="round" id="roundPanel" style="display:none">
    <span class="rchip">本轮计划抓取 <b id="rPlanned">–</b></span>
    <span class="rchip">本轮已经抓取 <b id="rScraped">–</b></span>
    <span class="rchip">本轮跳过 <b id="rSkip">–</b></span>
    <span class="rchip ok">已完成 <b id="rDone">–</b></span>
    <span class="rchip warn" id="rBlock" style="display:none">⚠ 被 403 掐断</span>
  </div>
</header>

<div class="wrap">
  <div class="comphead">
    <h2>历史日完整性分析</h2>
    <span class="muted" id="compUniverse"></span>
    <button onclick="refreshCompleteness()">⟳ 刷新完整性</button>
  </div>
  <div class="comptabs">
    <button class="tab active" id="tabUT" onclick="showComp('ut')">按真实分析日</button>
    <button class="tab" id="tabTD" onclick="showComp('td')">按抓取批次日</button>
  </div>
  <table id="compTable">
    <thead><tr>
      <th>历史日</th>
      <th class="num">已抓真实(支)</th>
      <th>覆盖率</th>
      <th class="num" id="compEmptyHead" style="display:none">未拿到(支)</th>
      <th>判定</th>
    </tr></thead>
    <tbody id="compRows"></tbody>
  </table>
  <div class="muted" style="margin:6px 0 4px">
    阈值：覆盖率 ≥95% 标记「完整」，50–95%「部分」，&lt;50%「极少」。
    全集 = 代码清单总数（含本次未拿到、需人工打开源站核对的股票，故 100% 未必可达）；已抓 = 该日有真实(ok)分析的去重支数。
    未拿到 ≠ 源站确认无数据：点开列表行可看到源站链接。
  </div>
</div>

<div class="wrap">
  <div class="filters">
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
      <option value="emp_rise1">排序: 次日上涨概率</option>
      <option value="emp_rank">排序: 打败比例</option>
      <option value="empart_change">排序: 参与意愿变化</option>
      <option value="empop_rank">排序: 市场排名</option>
    </select>
  </div>
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

<script>
let META = {dates:[]};
let DATA = [];
let openCode = null;
let COMP = {by_update_time:[], by_trade_date:[], universe:0};
let compMode = 'ut';

function num(v){ if(v===null||v===undefined||v==='') return ''; const n=Number(v); if(isNaN(n)) return String(v);
  const cls = n>0?'red':(n<0?'green':''); return '<span class="'+cls+'">'+n+'</span>'; }
function txt(v){ return (v===null||v===undefined)?'':String(v); }
function successTag(cs){
  if(!cs) return '<span class="muted">—</span>';
  const ok = Number(cs.last_success)===1; const st = cs.last_status||'';
  const label = ok?'成功':(st==='empty'?'未拿到(待核对)':(st==='fail'?'失败(待核对)':'否'));
  const cls = ok?'ok':'empty';
  return '<span class="tag '+cls+'">'+label+'</span>';
}
function kv(k,v){ return '<div class="kv"><span>'+k+'</span><span>'+v+'</span></div>'; }
function ctrlDegree(op){ if(op===null||op===undefined||op==='') return '-'; const n=Number(op); if(isNaN(n)) return '-'; if(n<0.3) return '低度控盘'; if(n<0.7) return '中度控盘'; return '高度控盘'; }

// ---- 历史日完整性 ----
function compStatus(cov){ if(cov>=95) return ['完整','ok']; if(cov>=50) return ['部分','amber']; return ['极少','empty']; }
async function refreshCompleteness(){
  try{
    const r=await fetch('/api/completeness'); const j=await r.json();
    if(!j.ok){ return; }
    COMP=j;
    document.getElementById('compUniverse').textContent='全集(代码清单) '+j.universe+' 支';
    renderComp();
  }catch(e){}
}
function showComp(m){
  compMode=m;
  document.getElementById('tabUT').classList.toggle('active', m==='ut');
  document.getElementById('tabTD').classList.toggle('active', m==='td');
  renderComp();
}
function renderComp(){
  const rows = compMode==='ut'?COMP.by_update_time:COMP.by_trade_date;
  const isTD = compMode==='td';
  document.getElementById('compEmptyHead').style.display = isTD?'table-cell':'none';
  const tb=document.getElementById('compRows'); tb.innerHTML='';
  rows.forEach(d=>{
    const [label,cls]=compStatus(d.coverage);
    const w = Math.max(2, Math.min(100, d.coverage));
    let html='<td>'+d.day+'</td>'+
      '<td class="num">'+d.ok+'</td>'+
      '<td class="bar-cell"><div class="bar-bg"><div class="bar-fg" style="width:'+w+'%"></div></div>'+
      '<span class="muted"> '+d.coverage+'%</span></td>';
    if(isTD) html+='<td class="num">'+(d.empty||0)+'</td>';
    html+='<td><span class="tag '+cls+'">'+label+'</span></td>';
    const tr=document.createElement('tr'); tr.innerHTML=html; tb.appendChild(tr);
  });
}

async function refreshData(){
  try{
    const r = await fetch('/api/data'); const j = await r.json();
    if(!j.ok){ document.getElementById('footer').textContent = '数据读取失败: '+(j.error||''); return; }
    META = j.meta; DATA = j.records;
    document.getElementById('footer').textContent = '数据源: '+META.db;
    const sel=document.getElementById('date'); sel.innerHTML='<option value="">全部真实分析日</option>';
    META.dates.forEach(d=>{ const o=document.createElement('option'); o.value=d; o.textContent=d; sel.appendChild(o); });
    document.getElementById('summary').textContent =
      '总计 '+META.total+' 支 · 真实 '+META.ok+' · 未拿到(待核对) '+META.empty+' · 最新真实日 '+(
      META.latest_real||'—')+' · 生成 '+META.generated_at;
    render();
    refreshCompleteness();
  }catch(e){ document.getElementById('footer').textContent='读取数据出错: '+e; }
}

function setSort(k){ document.getElementById('sort').value=k; render(); }
function filtered(){
  const q=document.getElementById('q').value.trim().toLowerCase();
  const date=document.getElementById('date').value, st=document.getElementById('status').value;
  const sort=document.getElementById('sort').value;
  let rows=DATA.filter(r=>{ const s=r.s;
    if(date && (s.update_time||'')!==date) return false;
    if(st && (s.status||'ok')!==st) return false;
    if(q){ const hay=((s.code||'')+' '+(s.name||'')).toLowerCase(); if(!hay.includes(q)) return false; }
    return true; });
  const val=(r,k)=>{ if(k==='main_net') return Number(r.ff&&r.ff.main_net)||0;
    if(k==='crawl_count') return Number(r.cs&&r.cs.crawl_count)||0;
    if(k==='synthesis') return Number(r.s.synthesis)||0;
    if(k==='emc_total_score') return Number(r.emc&&r.emc.emc_total_score)||0;
    if(k==='emc_rank') return Number(r.emc&&r.emc.emc_rank)||0;
    if(k==='emc_org') return Number(r.emc&&r.emc.emc_org_participate)||0;
    if(k==='emc_focus') return Number(r.emc&&r.emc.emc_focus)||0;
    if(k==='emc_prime') return Number(r.emc&&r.emc.emc_prime_cost)||0;
    if(k==='emv_pe') return Number(r.emv&&r.emv.emv_pe_ttm)||0;
    if(k==='emp_rise1') return Number(r.emp&&r.emp.emt_rise_1_prob)||0;
    if(k==='emp_rank') return Number(r.emp&&r.emp.emt_rank_ratio)||0;
    if(k==='empart_change') return Number(r.empart&&r.empart.emp_wish_change)||0;
    if(k==='empop_rank') return Number(r.empop&&r.empop.emp_market_rank)||0;
    return (r.s[k]||'').toString(); };
  rows.sort((a,b)=>{ let x=val(a,sort),y=val(b,sort);
    if(typeof x==='number'&&typeof y==='number') return y-x;
    return String(x).localeCompare(String(y),'zh'); });
  return rows;
}
function copyUrl(u){
  if(navigator.clipboard && navigator.clipboard.writeText){ navigator.clipboard.writeText(u); }
}
function verifyLinksHTML(r){
  const links=r.verify_links||[];
  let rows='';
  links.forEach(L=>{
    const u=L.url||'';
    rows+='<div class="src-row"><span class="muted">'+txt(L.source)+' · '+txt(L.label)
      +'</span><a href="'+u+'" target="_blank" rel="noopener">打开</a>'
      +'<button class="copy-btn" type="button" data-url="'+encodeURIComponent(u)
      +'" onclick="event.stopPropagation();copyUrl(decodeURIComponent(this.dataset.url))">复制链接</button>'
      +'<span class="muted" style="word-break:break-all">'+u+'</span></div>';
  });
  return '<div class="empty-note">⚠ 本次爬取未拿到数据（待人工确认，不是确认无数据）。请打开下面源站核对是否真没有这只票/这段数据。未写入子表，下次仍会重试。</div>'
    +'<div class="src-links"><div class="panel"><h3>源站核对链接</h3>'
    +(rows||'<div class="muted">无可用链接</div>')+'</div></div>';
}
function detailHTML(r){
  const s=r.s, ff=r.ff||{}, vt=r.vt||{}, sr=r.sr||{};
  if((s.status||'ok')!=='ok') return verifyLinksHTML(r);
  let h='<div class="box">';
  if(META.has_sr){ ['long','short'].forEach(cyc=>{ const d=sr[cyc];
    h+='<div class="panel"><h3>支撑/阻力 · '+(cyc==='long'?'长期':'短期')+'</h3>';
    if(!d){ h+='<div class="muted">无数据</div>'; } else {
      h+=kv('支撑位',txt(d.support_level)); h+=kv('阻力位',txt(d.resistance_level));
      h+=kv('智能评级',txt(d.rating_text)); h+=kv('评级等级',txt(d.rating_level));
      h+=kv('行业',txt(d.industry_name)); h+=kv('排名',txt(d.rank_str));
      if(d.level_desc) h+=kv('说明',txt(d.level_desc));
      if(d.bullish_events) h+=kv('看多事件',txt(d.bullish_events));
      if(d.bearish_events) h+=kv('看空事件',txt(d.bearish_events));
    } h+='</div>'; }); }
  if(META.has_ff){ h+='<div class="panel"><h3>资金流向（亿）</h3>';
    h+=kv('超大单',num(ff.super_net)); h+=kv('大单',num(ff.large_net)); h+=kv('中单',num(ff.medium_net));
    h+=kv('小单',num(ff.little_net)); h+=kv('主力净流入',num(ff.main_net));
    h+=kv('超大占比',txt(ff.super_rate)); h+=kv('大单占比',txt(ff.large_rate)); h+='</div>'; }
  if(META.has_vote){ h+='<div class="panel"><h3>看涨/看跌投票</h3>';
    h+=kv('总看涨',txt(vt.vote_up)); h+=kv('总看跌',txt(vt.vote_down));
    h+=kv('看涨率',txt(vt.vote_up_rate)); h+=kv('看跌率',txt(vt.vote_down_rate));
    h+=kv('本周看涨',txt(vt.week_up)); h+=kv('本周看跌',txt(vt.week_down));     h+=kv('本周看涨率',txt(vt.week_rate));
    h+='</div>'; }
  if(META.has_cs && r.cs){ h+='<div class="panel"><h3>抓取统计</h3>';
    h+=kv('爬取次数',txt(r.cs.crawl_count)); h+=kv('最近是否成功',successTag(r.cs));
    if(r.cs.last_attempt) h+=kv('最近尝试日',txt(r.cs.last_attempt));
    if(r.cs.updated_at) h+=kv('最近更新',txt(r.cs.updated_at));
    h+='</div>'; }
  if(META.has_em && r.emc){ h+='<div class="panel"><h3>东方财富 · 千股千评诊断</h3>';
    h+=kv('综合得分',num(r.emc.emc_total_score)); h+=kv('全市场排名',txt(r.emc.emc_rank));
    h+=kv('排名变动',txt(r.emc.emc_rank_up)); h+=kv('关注指数',num(r.emc.emc_focus));
    h+=kv('机构参与度',num(r.emc.emc_org_participate)); h+=kv('控盘程度',ctrlDegree(r.emc.emc_org_participate)); h+=kv('机构参与比例',num(r.emc.emc_ratio));
    h+=kv('主力成本(实时)',num(r.emc.emc_prime_cost)); h+=kv('主力成本(20日)',num(r.emc.emc_prime_cost_20d));
    h+=kv('主力成本(60日)',num(r.emc.emc_prime_cost_60d)); h+=kv('主力净流入',num(r.emc.emc_prime_inflow));
    h+=kv('超大单流入',num(r.emc.emc_superdeal_in)); h+=kv('超大单流出',num(r.emc.emc_superdeal_out));
    h+=kv('大单流入',num(r.emc.emc_bigdeal_in)); h+=kv('大单流出',num(r.emc.emc_bigdeal_out));
    h+=kv('买入超大单占比',num(r.emc.emc_buy_superdeal_ratio)); h+=kv('买入大单占比',num(r.emc.emc_buy_bigdeal_ratio));
    h+=kv('数据日',txt(r.emc.trade_date)); h+='</div>'; }
  if(META.has_em && r.emv){ h+='<div class="panel"><h3>东方财富 · 基本面估值</h3>';
    h+=kv('PE(TTM)',num(r.emv.emv_pe_ttm)); h+=kv('PE(LAR)',num(r.emv.emv_pe_lar));
    h+=kv('PB(MRQ)',num(r.emv.emv_pb_mrq)); h+=kv('PCF_OCF(LAR)',num(r.emv.emv_pcf_ocf_lar));
    h+=kv('PCF_OCF(TTM)',num(r.emv.emv_pcf_ocf_ttm)); h+=kv('PS(TTM)',num(r.emv.emv_ps_ttm));
    h+=kv('PEG',num(r.emv.emv_peg)); h+=kv('总市值',num(r.emv.emv_total_market_cap));
    h+=kv('流通市值',num(r.emv.emv_float_market_cap));     h+=kv('板块',txt(r.emv.emv_board));
    h+=kv('数据日',txt(r.emv.trade_date)); h+='</div>'; }
  if(META.has_emt && r.emt){ h+='<div class="panel"><h3>东方财富 · 定性诊断</h3>';
    h+=kv('趋势量能/支撑压力',txt(r.emt.emt_comment_txt)); h+=kv('消息面/资金面',txt(r.emt.emt_words_explain));
    h+=kv('数据日',txt(r.emt.trade_date)); h+='</div>'; }
  if(META.has_emp && r.emp){ h+='<div class="panel"><h3>东方财富 · 诊断概率</h3>';
    h+=kv('次日上涨概率',num(r.emp.emt_rise_1_prob)+' %'); h+=kv('5日上涨概率',num(r.emp.emt_rise_5_prob)+' %');
    h+=kv('次日平均涨跌',num(r.emp.emt_avg_1_inc)); h+=kv('5日平均涨跌',num(r.emp.emt_avg_5_inc));
    h+=kv('打败比例',num(r.emp.emt_rank_ratio)+' %'); h+=kv('样本数(次日)',txt(r.emp.emt_all_count_1));
    h+=kv('样本数(5日)',txt(r.emp.emt_all_count_5)); h+=kv('数据日',txt(r.emp.trade_date)); h+='</div>'; }
  if(r.empop){ h+='<div class="panel"><h3>东方财富 · 市场排名</h3>';
    h+=kv('综合市场排名',txt(r.empop.emp_market_rank)+' / '+txt(r.empop.emp_market_num));
    h+=kv('行业排名',txt(r.empop.emp_industry_rank));
    if(r.empop.emp_rank_change!==undefined){ const ch=r.empop.emp_rank_change; const tag=ch<0?('上升 '+String(-ch)+' 名'):(ch>0?('下降 '+String(ch)+' 名'):'持平'); h+=kv('较昨日',tag); }
    h+=kv('综合得分变化率%',num(r.empop.emp_change_rate));
    h+=kv('全市场股票数',txt(r.empop.emp_market_stock_num));
    h+=kv('关注指数',num(r.empop.emp_focus_index));
    h+=kv('关注排名',txt(r.empop.emp_focus_rank)+' / '+txt(r.empop.emp_focus_total));
    h+=kv('数据日',txt(r.empop.trade_date)); h+='</div>'; }
  if(r.empart){ h+='<div class="panel"><h3>东方财富 · 参与意愿</h3>';
    h+=kv('当日参与意愿值',num(r.empart.emp_wish));
    h+=kv('五日平均参与意愿值',num(r.empart.emp_wish_5d));
    h+=kv('当日参与意愿变化%',num(r.empart.emp_wish_change));
    h+=kv('五日参与意愿变化%',num(r.empart.emp_wish_5d_change));
    h+=kv('数据日',txt(r.empart.trade_date)); h+='</div>'; }
  h+='</div>'; return h;
}
function render(){
  const rows=filtered(); const tb=document.getElementById('rows'); tb.innerHTML='';
  rows.forEach(r=>{ const s=r.s; const status=s.status||'ok';
    const tr=document.createElement('tr');
    tr.innerHTML=`<td>${txt(s.code)}</td><td>${txt(s.name)}</td><td>${txt(s.update_time)}</td>`+
      `<td>${txt(s.crawl_date)}</td><td><span class="tag ${status}">${status==='ok'?'真实':'未拿到(待核对)'}</span></td>`+
      (META.has_cs?`<td class="num">${txt(r.cs&&r.cs.crawl_count)}</td>`+
        `<td>${successTag(r.cs)}</td>`:'')+
      (META.has_em?`<td class="num">${num(r.emc&&r.emc.emc_total_score)}</td>`+
        `<td class="num">${txt(r.emc&&r.emc.emc_rank)}</td>`+
        `<td class="num">${num(r.emc&&r.emc.emc_org_participate)}</td>`+
        `<td class="num">${num(r.emc&&r.emc.emc_focus)}</td>`+
        `<td class="num">${num(r.emc&&r.emc.emc_prime_cost)}</td>`+
        `<td class="num">${num(r.emv&&r.emv.emv_pe_ttm)}</td>`:'')+
      (META.has_emp?`<td class="num">${num(r.emp&&r.emp.emt_rise_1_prob)}</td>`+
        `<td class="num">${num(r.emp&&r.emp.emt_rank_ratio)}</td>`:'')+
      (META.has_empart?`<td class="num">${num(r.empart&&r.empart.emp_wish_change)}</td>`:'')+
      (META.has_empop?`<td class="num">${txt(r.empop&&r.empop.emp_market_rank)}</td>`:'')+
      `<td class="num">${num(s.synthesis)}</td><td class="num">${num(s.technology)}</td>`+
      `<td class="num">${num(s.capital)}</td><td class="num">${num(s.market)}</td>`+
      `<td class="num">${num(s.finance)}</td><td class="num">${num(r.ff&&r.ff.main_net)}</td>`+
      `<td class="num">${txt(r.vt&&r.vt.vote_up)}/${txt(r.vt&&r.vt.vote_down)}</td>`;
    const key=s.code+'|'+s.update_time;
    tr.onclick=()=>{ openCode = openCode===key?null:key; render(); };
    tb.appendChild(tr);
    if(openCode===key){ const dr=document.createElement('tr'); dr.className='detail';
      dr.innerHTML='<td colspan="24">'+detailHTML(r)+'</td>'; tb.appendChild(dr); }
  });
  document.getElementById('count').textContent='显示 '+rows.length+' / '+DATA.length+' 支';
}

// ---- 抓取控制 + 实时状态轮询 ----
let wasRunning=false;
async function poll(){
  try{
    const r=await fetch('/api/status'); const s=await r.json();
    const st=document.getElementById('status'); const dot=st.querySelector('.dot');
    const txtEl=document.getElementById('statusText');
    const startBtn=document.getElementById('startBtn'), stopBtn=document.getElementById('stopBtn');
    if(s.running){ st.classList.add('run'); txtEl.textContent='抓取中… 开始于 '+s.started_at;
      startBtn.disabled=true; stopBtn.disabled=false; }
    else { st.classList.remove('run');
      if(wasRunning){ txtEl.textContent='已完成（'+s.finished_at+(s.exit_code!==null?('，退出码 '+s.exit_code):'')+'）';
        if(s.last_summary) document.getElementById('summary').textContent=s.last_summary; }
      else txtEl.textContent='空闲';
      startBtn.disabled=false; stopBtn.disabled=true; }
    const logEl=document.getElementById('log');
    const _logN=(s.log&&s.log.length)||0;
    const _joined=_logN?s.log.join('\\n'):'（未开始）';
    if(logEl.dataset.sig !== String(_logN)+':'+(_joined.length)+':'+(_joined.slice(-40))){
      logEl.dataset.sig = String(_logN)+':'+(_joined.length)+':'+(_joined.slice(-40));
      logEl.textContent = _joined;
      logEl.scrollTop = logEl.scrollHeight;
    }
    // 本轮进度计数
    const rd = s.round || {};
    const rp = document.getElementById('roundPanel');
    if(rd && (rd.planned!==null || rd.scraped || rd.done || rd.skip)){
      rp.style.display='';
      document.getElementById('rPlanned').textContent = rd.planned!==null?rd.planned:'–';
      document.getElementById('rScraped').textContent = rd.scraped||0;
      document.getElementById('rSkip').textContent = rd.skip||0;
      document.getElementById('rDone').textContent = rd.done||0;
      document.getElementById('rBlock').style.display = rd.blocked403?'inline-block':'none';
    } else {
      rp.style.display='none';
    }
    if(wasRunning && !s.running){ await refreshData(); }  // 跑完自动刷新结果
    wasRunning = s.running;
  }catch(e){}
  setTimeout(poll, 1500);
}
function rateOpts(){
  const mi=parseFloat(document.getElementById('minInterval').value);
  const mpm=parseInt(document.getElementById('maxPerMinute').value,10);
  const capRaw=document.getElementById('rateWaitCap').value;
  const cap=parseFloat(capRaw);
  const o={
    min_interval: (isFinite(mi)&&mi>0)?mi:1.0,
    max_per_minute: (isFinite(mpm)&&mpm>0)?mpm:40,
    rate_window: 60
  };
  if(capRaw!=='' && isFinite(cap) && cap>=0) o.rate_wait_cap = cap;
  return o;
}
async function startScrape(){
  const r=await fetch('/api/scrape',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(rateOpts())}); const j=await r.json();
  document.getElementById('summary').textContent = j.msg;
  if(j.started){ document.getElementById('log').textContent=''; }
}
async function startScrapeEm(){
  const r=await fetch('/api/scrape_em',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(rateOpts())}); const j=await r.json();
  document.getElementById('summary').textContent = j.msg;
  if(j.started){ document.getElementById('log').textContent=''; }
}
async function startCombined(){
  const r=await fetch('/api/scrape_combined',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(rateOpts())}); const j=await r.json();
  document.getElementById('summary').textContent = j.msg + '（自动完成百度→东财）';
  if(j.started){ document.getElementById('log').textContent=''; }
}
async function stopScrape(){
  await fetch('/api/scrape/stop',{method:'POST'});
}
async function exitBackend(){
  if(!confirm('将关闭 Web 服务，并强制结束全部抓取相关 Python 进程。确定？')) return;
  window.__exitingBackend = true;
  try{
    await fetch('/api/shutdown',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
  }catch(e){}
  document.getElementById('summary').textContent='后端已退出，可关闭本标签页';
  document.getElementById('statusText').textContent='已关闭';
}
// 关页 / 崩溃式离开：延迟通知后端关闭（F5 后新页面请求会取消）
window.addEventListener('pagehide', function(ev){
  if(window.__exitingBackend) return;
  if(ev.persisted) return;
  try{
    navigator.sendBeacon('/api/page_closed', new Blob(['{}'],{type:'application/json'}));
  }catch(e){
    try{ fetch('/api/page_closed',{method:'POST',body:'{}',keepalive:true,headers:{'Content-Type':'application/json'}}); }catch(_){}
  }
});

refreshData();
poll();
</script>
</body>
</html>
"""


def _dispatch_frozen_child() -> None:
    """exe 子进程入口：--run-script 百度/东财 爬虫.py → 调对应模块 main()。"""
    if _RUN_SCRIPT_FLAG not in sys.argv:
        return
    i = sys.argv.index(_RUN_SCRIPT_FLAG)
    name = sys.argv[i + 1] if i + 1 < len(sys.argv) else ""
    rest = sys.argv[i + 2:]
    sys.argv = [name] + rest
    base = os.path.splitext(os.path.basename(name))[0]
    if base == "baidu_finance_ai_crawler":
        import baidu_finance_ai_crawler as mod
    elif base == "eastmoney_stockcomment_crawler":
        import eastmoney_stockcomment_crawler as mod
    else:
        raise SystemExit("未知爬虫脚本: %s" % name)
    raise SystemExit(mod.main())


def main(argv=None) -> int:
    global HTTP_SERVER
    _ensure_bundled_files()
    ap = argparse.ArgumentParser(description="百度财经 AI 合并调试页（看+抓）")
    ap.add_argument("--port", type=int, default=8765, help="监听端口（默认 8765）")
    ap.add_argument("--host", default="127.0.0.1", help="绑定地址（默认 127.0.0.1）")
    args = ap.parse_args(argv)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    HTTP_SERVER = server
    url = f"http://{args.host}:{args.port}"
    print(f"合并调试页已启动: {url}")
    print(f"  读取数据库: {DB_PATH}")
    print(f"  爬虫脚本:   {SCRIPT_PATH}")
    print("  按 Ctrl+C 停止；页面「退出后端」或关页也会关闭服务。")
    if _is_frozen():
        # 双击 exe 时自动打开浏览器，免手抄地址
        import webbrowser
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        try:
            stop_scrape()
        except Exception:
            pass
        server.server_close()
        HTTP_SERVER = None
    return 0


if __name__ == "__main__":
    if _RUN_SCRIPT_FLAG in sys.argv:
        _dispatch_frozen_child()
    sys.exit(main())
