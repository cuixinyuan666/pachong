#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度财经 B 方案：Selenium headless 刷新 Cookie

在 C 方案（Cookie/冷却）连续 403 后调用：无头 Chrome 访问百度财经，
提取 Cookie，写回 cookie 池与调用方，供后续 curl/urllib 继续抓取。
另一台电脑没有 selenium 时会自动 pip 安装。
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from typing import Optional

logger = logging.getLogger("baidu_selenium_fallback")

HERE = os.path.dirname(os.path.abspath(__file__))
COOKIE_CACHE = os.path.join(HERE, "baidu_cookies.json")
PIP_PKGS = ("selenium", "webdriver-manager")


def _try_import_selenium() -> bool:
    try:
        import selenium  # noqa: F401
        from webdriver_manager.chrome import ChromeDriverManager  # noqa: F401
        return True
    except ImportError:
        return False


def _pip(args: list) -> bool:
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False


def _ensure_selenium() -> bool:
    """没有 selenium 就自动 pip 装上。"""
    if _try_import_selenium():
        return True
    print("AUTO_PIP", flush=True)
    if not _pip(["install", "-q", *PIP_PKGS]):
        subprocess.run(
            [sys.executable, "-m", "ensurepip", "--upgrade"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        _pip(["install", "--upgrade", "pip"])
        if not _pip(["install", "-q", *PIP_PKGS]):
            logger.error("自动 pip 安装 selenium / webdriver-manager 失败")
            return False
    return _try_import_selenium()


def refresh_cookies_headless(timeout_page: float = 8.0) -> Optional[dict]:
    """Headless Chrome 访问百度财经并返回 Cookie dict；失败返回 None。"""
    if not _ensure_selenium():
        logger.error("自动安装 selenium 失败，无法刷新 Cookie")
        return None

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=zh-CN")
    opts.page_load_strategy = "eager"
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    # 降低自动化特征
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(45)
        driver.set_script_timeout(30)
        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
            )
        except Exception:
            pass

        urls = [
            "https://finance.baidu.com/",
            "https://finance.baidu.com/stock/ab-000001",
        ]
        cookies: dict = {}
        from selenium.common.exceptions import TimeoutException
        for url in urls:
            logger.info("[B/Selenium] 访问 %s", url)
            try:
                driver.get(url)
            except TimeoutException:
                logger.warning("[B/Selenium] 页面加载超时，继续提取已有 Cookie: %s", url)
            time.sleep(min(timeout_page, 5.0))
            for c in driver.get_cookies():
                name, val = c.get("name"), c.get("value")
                if name and val is not None:
                    cookies[name] = val

        if not cookies:
            logger.warning("[B/Selenium] 未拿到任何 Cookie")
            return None

        try:
            with open(COOKIE_CACHE, "w", encoding="utf-8") as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("[B/Selenium] 写缓存失败: %s", e)

        logger.info("[B/Selenium] 刷新成功，共 %d 个 Cookie: %s",
                    len(cookies), ",".join(sorted(cookies.keys())[:12]))
        return cookies
    except Exception as e:
        logger.error("[B/Selenium] 刷新失败: %s", e)
        return None
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def apply_cookies_to_runtime(cookies: dict) -> None:
    """Cookie 写入 json；exe 读 baidu_cookies.json，不依赖 Cookie 池模块。"""
    if not cookies:
        return
    os.environ["_BAIDU_COOKIE_DICT"] = json.dumps(cookies, ensure_ascii=False)


def refresh_and_apply() -> Optional[dict]:
    """一站式：headless 刷新并应用到运行时。成功返回 cookies，失败 None。"""
    cookies = refresh_cookies_headless()
    if cookies:
        apply_cookies_to_runtime(cookies)
    return cookies


if __name__ == "__main__":
    c = refresh_and_apply()
    print("OK" if c else "FAIL")
    print(len(c or {}))
