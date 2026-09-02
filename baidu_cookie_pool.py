#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度财经 Cookie 池模块 — C方案：Cookie 轮换 + Session 复用

功能：
1. 自动生成/管理多个浏览器 Session（模拟不同用户）
2. Cookie 轮换策略，避免单 IP 高频请求
3. 自动提取新 Cookie（可通过 Selenium 采集，或手动粘贴）
4. 与现有爬虫无缝集成

使用方式：
    from baidu_cookie_pool import CookiePool, EnhancedFetcher
    
    pool = CookiePool()
    pool.load_or_create_cookies()
    
    fetcher = EnhancedFetcher(cookie_pool=pool)
    data = fetcher.fetch_json(url)  # 自动选择 Cookie
"""
import json
import os
import time
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
from http.cookiejar import CookieJar
from urllib.request import Request, build_opener, HTTPCookieProcessor
from urllib.error import URLError, HTTPError

logger = logging.getLogger("baidu_cookie_pool")

# 全局 Cookie 池实例
_cookie_pool = None


def get_cookie_pool():
    """获取或创建全局 Cookie 池实例"""
    global _cookie_pool
    if _cookie_pool is None:
        _cookie_pool = CookiePool()
    return _cookie_pool


class BaiduSession:
    """单个百度财经浏览 Session（含 UA、Cookie、创建时间）"""
    
    def __init__(self, session_id: str, user_agent: str, cookies: dict = None, 
                 created_at: str = None, last_used: str = None, success_count: int = 0,
                 fail_count: int = 0):
        self.session_id = session_id
        self.user_agent = user_agent
        self.cookies = cookies or {}
        self.created_at = created_at or datetime.now().isoformat()
        self.last_used = last_used or datetime.now().isoformat()
        self.success_count = success_count
        self.fail_count = fail_count
    
    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "user_agent": self.user_agent,
            "cookies": self.cookies,
            "created_at": self.created_at,
            "last_used": self.last_used,
            "success_count": self.success_count,
            "fail_count": self.fail_count,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'BaiduSession':
        return cls(
            session_id=data["session_id"],
            user_agent=data["user_agent"],
            cookies=data.get("cookies", {}),
            created_at=data.get("created_at"),
            last_used=data.get("last_used"),
            success_count=data.get("success_count", 0),
            fail_count=data.get("fail_count", 0),
        )


class CookiePool:
    """Cookie 池管理器"""
    
    # 常用 UA 列表（增加多样性）
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/17.5",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    ]
    
    def __init__(self, pool_dir: str = None, max_sessions: int = 10):
        self.pool_dir = pool_dir or str(Path.home() / ".baidu_cookie_pool")
        self.max_sessions = max_sessions
        self.sessions: list[BaiduSession] = []
        self._load_sessions()
        
    def _load_sessions(self):
        """从文件加载 Sessions"""
        pool_file = os.path.join(self.pool_dir, "sessions.json")
        if os.path.exists(pool_file):
            try:
                with open(pool_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.sessions = [BaiduSession.from_dict(s) for s in data]
                logger.info(f"加载了 {len(self.sessions)} 个 Session")
            except Exception as e:
                logger.warning(f"加载 Session 失败: {e}")
                self.sessions = []
    
    def _save_sessions(self):
        """保存 Sessions 到文件"""
        os.makedirs(self.pool_dir, exist_ok=True)
        pool_file = os.path.join(self.pool_dir, "sessions.json")
        try:
            data = [s.to_dict() for s in self.sessions]
            with open(pool_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存 Session 失败: {e}")
    
    def get_best_session(self) -> BaiduSession:
        """获取最佳 Session（成功率最高且使用次数最少）"""
        if not self.sessions:
            return self.create_new_session()
        
        # 按成功率排序，优先选成功率高的
        sorted_sessions = sorted(
            self.sessions,
            key=lambda s: (
                s.success_count / max(s.success_count + s.fail_count, 1),
                -s.success_count  # 成功次数多的优先
            ),
            reverse=True
        )
        
        # 返回第一个可用的
        best = sorted_sessions[0]
        best.last_used = datetime.now().isoformat()
        self._save_sessions()
        return best
    
    def record_success(self, session_id: str):
        """记录成功"""
        for s in self.sessions:
            if s.session_id == session_id:
                s.success_count += 1
                break
        self._save_sessions()
    
    def record_failure(self, session_id: str):
        """记录失败"""
        for s in self.sessions:
            if s.session_id == session_id:
                s.fail_count += 1
                # 如果失败太多，标记为需要刷新
                if s.fail_count > 50:
                    s.cookies = {}  # 清空 cookie，下次重建
                break
        self._save_sessions()
    
    def create_new_session(self, cookies: dict = None, user_agent: str = None) -> BaiduSession:
        """创建新 Session"""
        if len(self.sessions) >= self.max_sessions:
            # 淘汰最差的 Session
            worst = min(self.sessions, key=lambda s: s.success_count - s.fail_count)
            self.sessions.remove(worst)
            logger.info(f"淘汰最差 Session: {worst.session_id}")
        
        session_id = f"sess_{int(time.time())}_{random.randint(1000, 9999)}"
        ua = user_agent or random.choice(self.USER_AGENTS)
        
        session = BaiduSession(
            session_id=session_id,
            user_agent=ua,
            cookies=cookies or {},
        )
        self.sessions.append(session)
        self._save_sessions()
        logger.info(f"创建新 Session: {session_id}")
        return session
    
    def add_manual_cookies(self, cookies: dict, user_agent: str = None):
        """手动添加 Cookie（用于从浏览器导出后导入）"""
        session = self.create_new_session(cookies=cookies, user_agent=user_agent)
        logger.info(f"手动添加了 {len(cookies)} 个 Cookie")
    
    def get_stats(self) -> dict:
        """获取池统计信息"""
        total = len(self.sessions)
        active = sum(1 for s in self.sessions if s.cookies)
        total_success = sum(s.success_count for s in self.sessions)
        total_fail = sum(s.fail_count for s in self.sessions)
        return {
            "total": total,
            "active": active,
            "success_rate": round(total_success / max(total_success + total_fail, 1) * 100, 2),
            "max_capacity": self.max_sessions,
        }


class EnhancedFetcher:
    """增强的 HTTP 抓取器，支持 Cookie 轮换"""
    
    def __init__(self, cookie_pool: CookiePool = None, default_max_retries: int = 3):
        self.cookie_pool = cookie_pool or CookiePool()
        self.default_max_retries = default_max_retries
    
    def build_request(self, url: str, cookies: dict = None, user_agent: str = None) -> Request:
        """构建带 Cookie 的请求"""
        # 如果没有指定 Cookie，从池中获取
        if not cookies and self.cookie_pool.sessions:
            session = self.cookie_pool.get_best_session()
            cookies = session.cookies
            user_agent = session.user_agent
        
        headers = {
            "User-Agent": user_agent or random.choice(CookiePool.USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://finance.baidu.com/",
            "Origin": "https://finance.pae.baidu.com",
            "X-Requested-With": "XMLHttpRequest",
        }
        
        if cookies:
            cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            headers["Cookie"] = cookie_str
        
        req = Request(url, headers=headers)
        return req
    
    def fetch_with_retry(self, url: str, max_retries: int = None) -> dict:
        """带重试的抓取，自动轮换 Cookie"""
        max_retries = max_retries or self.default_max_retries
        current_session = None
        
        for attempt in range(max_retries):
            try:
                # 获取或创建 Session
                if not current_session or not current_session.cookies:
                    current_session = self.cookie_pool.get_best_session()
                    if not current_session.cookies:
                        # 如果没有 Cookie，创建新的
                        current_session = self.cookie_pool.create_new_session()
                
                req = self.build_request(
                    url,
                    cookies=current_session.cookies,
                    user_agent=current_session.user_agent
                )
                
                opener = build_opener()
                response = opener.open(req, timeout=15)
                data = json.loads(response.read().decode('utf-8'))
                
                # 记录成功
                self.cookie_pool.record_success(current_session.session_id)
                return data
                
            except HTTPError as e:
                if e.code == 403:
                    # 403 说明这个 Session 失效了，换下一个
                    if current_session:
                        self.cookie_pool.record_failure(current_session.session_id)
                        current_session = None
                    logger.warning(f"403 错误，尝试更换 Session (attempt {attempt + 1}/{max_retries})")
                else:
                    raise
                    
            except Exception as e:
                logger.error(f"抓取失败: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                else:
                    raise
        
        raise RuntimeError(f"抓取失败，已重试 {max_retries} 次")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    pool = CookiePool()
    print("Cookie 池统计:")
    stats = pool.get_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")
    
    print("\n使用示例:")
    print("""
# 从浏览器导出 Cookie 并添加到池中
cookies = {
    "BAIDUID": "xxx:FG=1",
    " BIDUPSID": "xxx",
    " PSTM": "xxx",
}
pool.add_manual_cookies(cookies)

# 使用增强抓取器
fetcher = EnhancedFetcher(cookie_pool=pool)
data = fetcher.fetch_with_retry("https://finance.pae.baidu.com/vapi/v1/analysis?code=000001&market=ab")
""")
