#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富爬虫增强模块 — 添加 Cookie 支持 + Session 复用

增强功能:
1. 自动获取东方财富 Cookie（通过浏览器）
2. Cookie 轮换，避免单 IP 被封
3. 与现有 eastmoney_stockcomment_crawler.py 无缝集成
"""
import sys
import os
import json
import time
import random
import logging
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

logger = logging.getLogger("em_cookie_enhancer")

# --------------------------------------------------------------------------- #
# 东方财富 Cookie 管理器
# --------------------------------------------------------------------------- #
EM_COOKIE_POOL_DIR = str(Path.home() / ".eastmoney_cookies")


class EastMoneyCookieManager:
    """东方财富 Cookie 管理器"""
    
    def __init__(self, pool_dir=None):
        self.pool_dir = pool_dir or EM_COOKIE_POOL_DIR
        self.cookies = {}
        self.session_id = f"em_{int(time.time())}"
        self._load_cookies()
    
    def _load_cookies(self):
        """从文件加载 Cookie"""
        pool_file = os.path.join(self.pool_dir, "em_cookies.json")
        if os.path.exists(pool_file):
            try:
                with open(pool_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cookies = data.get("cookies", {})
                    logger.info(f"加载了 {len(self.cookies)} 个东方财富 Cookie")
            except Exception as e:
                logger.warning(f"加载 Cookie 失败: {e}")
    
    def _save_cookies(self):
        """保存 Cookie 到文件"""
        os.makedirs(self.pool_dir, exist_ok=True)
        pool_file = os.path.join(self.pool_dir, "em_cookies.json")
        try:
            with open(pool_file, 'w', encoding='utf-8') as f:
                json.dump({"cookies": self.cookies}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存 Cookie 失败: {e}")
    
    def add_manual_cookies(self, cookies: dict):
        """手动添加 Cookie"""
        self.cookies.update(cookies)
        self._save_cookies()
        logger.info(f"添加了 {len(cookies)} 个东方财富 Cookie")
    
    def build_headers_with_cookies(self, base_headers: dict) -> dict:
        """在基础请求头中添加 Cookie"""
        headers = base_headers.copy()
        
        if self.cookies:
            cookie_str = "; ".join([f"{k}={v}" for k, v in self.cookies.items()])
            headers["Cookie"] = cookie_str
            logger.debug(f"已添加 Cookie ({len(self.cookies)} 个)")
        
        return headers
    
    def extract_from_browser(self) -> bool:
        """从浏览器提取 Cookie（需要 Selenium）"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
            
            driver = webdriver.Chrome(options=chrome_options)
            
            # 访问东方财富数据页面
            urls = [
                "https://data.eastmoney.com/",
                "https://datacenter-web.eastmoney.com/",
            ]
            
            for url in urls:
                try:
                    driver.get(url)
                    time.sleep(3)
                except:
                    pass
            
            cookies = {}
            for cookie in driver.get_cookies():
                cookies[cookie['name']] = cookie['value']
            
            driver.quit()
            
            if cookies:
                self.add_manual_cookies(cookies)
                logger.info(f"从浏览器提取了 {len(cookies)} 个 Cookie")
                return True
            else:
                logger.warning("未从浏览器提取到 Cookie")
                return False
                
        except ImportError:
            logger.error("Selenium 未安装，无法提取 Cookie")
            print("提示: 运行 'pip install selenium webdriver-manager' 安装")
            return False
        except Exception as e:
            logger.error(f"提取 Cookie 失败: {e}")
            return False


# --------------------------------------------------------------------------- #
# 快速初始化函数
# --------------------------------------------------------------------------- #
_em_cookie_mgr = None


def get_em_cookie_manager() -> EastMoneyCookieManager:
    """获取或创建全局 Cookie 管理器"""
    global _em_cookie_mgr
    if _em_cookie_mgr is None:
        _em_cookie_mgr = EastMoneyCookieManager()
    return _em_cookie_mgr


def enhance_em_headers(base_headers: dict) -> dict:
    """增强东方财富请求头（自动添加 Cookie）"""
    mgr = get_em_cookie_manager()
    return mgr.build_headers_with_cookies(base_headers)


def test_em_cookies():
    """测试 Cookie 是否有效"""
    mgr = get_em_cookie_manager()
    
    if not mgr.cookies:
        print("没有可用的 Cookie，正在从浏览器提取...")
        if mgr.extract_from_browser():
            print("[OK] Cookie 提取成功")
        else:
            print("[FAIL] Cookie 提取失败")
            return False
    
    # 测试 API
    from crawler_common import fetch_json, RateLimiter
    
    limiter = RateLimiter(min_interval=1.0, max_per_minute=20)
    
    # 测试批量接口
    test_url = ("https://datacenter-web.eastmoney.com/api/data/v1/get?"
                "reportName=RPT_DMSK_TS_STOCKNEW&columns=ALL&pageSize=5&pageNumber=1")
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
               "Referer": "https://data.eastmoney.com/"}
    
    headers = mgr.build_headers_with_cookies(headers)
    
    try:
        data = fetch_json(test_url, headers, limiter, backend="urllib", max_retries=1, timeout=10)
        if data.get("Result") is not None:
            print("[OK] Cookie 有效！批量接口正常")
            return True
        else:
            print("[WARN] Cookie 可能无效，返回数据结构异常")
            return False
    except Exception as e:
        print(f"[FAIL] Cookie 测试失败: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("东方财富 Cookie 增强模块测试")
    print("=" * 60)
    
    success = test_em_cookies()
    
    if success:
        print("\n东方财富爬虫可以开始使用了！")
        print("运行: python eastmoney_stockcomment_crawler.py --market")
    else:
        print("\n建议:")
        print("1. 安装 Selenium: pip install selenium webdriver-manager")
        print("2. 从浏览器导出 Cookie 并添加到池中")
        print("3. 或降低请求频率")
