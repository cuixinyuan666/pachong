#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度财经 Cookie 采集器 — B方案前奏

通过 Selenium 打开真实浏览器访问百度财经，自动提取 Cookie。
这是手动 + 半自动方案，为后续全自动化做准备。

使用方式：
    python baidu_cookie_fetcher.py --collect    # 采集 Cookie
    python baidu_cookie_fetcher.py --test        # 测试 Cookie 是否有效
"""
import argparse
import json
import os
import sys
import time
import logging

logger = logging.getLogger("cookie_fetcher")

def check_selenium_installed():
    """检查 Selenium 是否安装"""
    try:
        import selenium  # noqa: F401
        return True
    except ImportError:
        return False

def install_selenium():
    """安装 Selenium 和 WebDriver Manager"""
    import subprocess
    packages = ["selenium", "webdriver-manager"]
    print(f"正在安装: {', '.join(packages)}")
    subprocess.check_call([sys.executable, "-m", "pip", "install"] + packages)

def collect_cookies_chrome():
    """
    使用 Chrome 浏览器访问百度财经并提取 Cookie
    
    注意：需要用户手动配合一次，打开浏览器登录后关闭
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    
    print("=" * 60)
    print("百度财经 Cookie 采集器")
    print("=" * 60)
    print("\n步骤:")
    print("1. 浏览器将自动打开百度财经页面")
    print("2. 请勿登录（保持匿名状态即可）")
    print("3. 等待 10 秒让页面加载")
    print("4. 按 Ctrl+C 停止采集")
    print("\n" + "=" * 60)
    
    # 配置 Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 无头模式
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # 设置随机 UA
    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/17.5",
    ]
    chrome_options.add_argument(f"--user-agent={ua_list[0]}")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"\n浏览器初始化失败: {e}")
        print("请确保已安装 Chrome 浏览器")
        return None
    
    try:
        # 访问百度财经首页
        urls = [
            "https://finance.baidu.com/",
            "https://finance.pae.baidu.com/vapi/v1/analysis?code=000001&market=ab",
        ]
        
        all_cookies = {}
        for url in urls:
            print(f"\n正在访问: {url}")
            driver.get(url)
            time.sleep(5)  # 等待页面加载
            
            # 提取所有 Cookie
            cookies = driver.get_cookies()
            for cookie in cookies:
                all_cookies[cookie['name']] = cookie['value']
            
            print(f"已获取 {len(cookies)} 个 Cookie")
        
        driver.quit()
        
        print("\n" + "=" * 60)
        print(f"采集完成！共获取 {len(all_cookies)} 个 Cookie")
        print("=" * 60)
        
        # 保存 Cookie
        output_file = os.path.join(os.path.dirname(__file__), "baidu_cookies.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_cookies, f, ensure_ascii=False, indent=2)
        
        print(f"\nCookie 已保存到: {output_file}")
        print("\n下一步:")
        print(f"1. 运行: python baidu_cookie_pool.py")
        print(f"2. 编辑 crawler_common.py 中的 fetch_json 函数")
        print(f"3. 添加 Cookie 支持")
        
        return all_cookies
        
    except KeyboardInterrupt:
        driver.quit()
        print("\n\n采集已中断")
        return None
    except Exception as e:
        driver.quit()
        print(f"\n采集失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_cookie(cookie_dict: dict):
    """测试 Cookie 是否有效"""
    import urllib.request
    
    url = "https://finance.pae.baidu.com/vapi/v1/analysis?code=000001&market=ab"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://finance.baidu.com/",
    }
    
    if cookie_dict:
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
        headers["Cookie"] = cookie_str
        print("使用 Cookie 测试...")
    else:
        print("不使用 Cookie 测试...")
    
    try:
        req = urllib.request.Request(url, headers=headers)
        response = urllib.request.urlopen(req, timeout=10)
        data = json.loads(response.read().decode('utf-8'))
        
        print(f"\n✅ 请求成功！")
        print(f"返回数据: {json.dumps(data, ensure_ascii=False, indent=2)[:200]}...")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 请求失败: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="百度财经 Cookie 采集工具")
    parser.add_argument("--collect", action="store_true", help="采集 Cookie")
    parser.add_argument("--test", action="store_true", help="测试 Cookie")
    parser.add_argument("--cookie-file", type=str, default=None, help="Cookie 文件路径")
    
    args = parser.parse_args()
    
    # 检查 Selenium
    if not check_selenium_installed():
        print("Selenium 未安装，正在安装...")
        install_selenium()
    
    if args.collect:
        cookies = collect_cookies_chrome()
        if cookies:
            print("\n请手动保存以下 Cookie 到文件，然后使用 --test 测试:")
            print(json.dumps(cookies, ensure_ascii=False, indent=2))
    
    elif args.test:
        cookie_file = args.cookie_file or os.path.join(
            os.path.dirname(__file__), "baidu_cookies.json"
        )
        if os.path.exists(cookie_file):
            with open(cookie_file, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            test_cookie(cookies)
        else:
            print(f"Cookie 文件不存在: {cookie_file}")
            print("请先运行 --collect 采集 Cookie")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
