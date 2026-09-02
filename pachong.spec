# -*- mode: python ; coding: utf-8 -*-
# PyInstaller：把调试页 + 百度/东财爬虫打成单个 pachong.exe

from PyInstaller.utils.hooks import collect_all

datas = [("a_stocks.json", ".")]
binaries = []
hiddenimports = [
    "view_results",
    "crawler_common",
    "baidu_finance_ai_crawler",
    "eastmoney_stockcomment_crawler",
    "baidu_cookie_pool",
    "baidu_cookie_fetcher",
    "baidu_selenium_fallback",
    "enhance_eastmoney_crawler",
    "run_crawlers",
    "start_crawl_with_cookies",
    "pandas",
    "adata",
    "easy_tdx",
]

for pkg in ("adata", "easy_tdx", "pandas"):
    try:
        pkg_datas, pkg_binaries, pkg_hidden = collect_all(pkg)
        datas += pkg_datas
        binaries += pkg_binaries
        hiddenimports += pkg_hidden
    except Exception:
        pass

a = Analysis(
    ["scrapy_server.py"],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="pachong",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
)
