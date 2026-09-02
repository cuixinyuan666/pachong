# pachong

百度财经 AI 技术分析 + 东方财富千股千评的本地爬虫与调试页。

## 下载 exe（Rust GUI）

到 [Releases](https://github.com/cuixinyuan666/pachong/releases) 下载 `baidu_finance_rust-windows-x64.exe`，双击打开 **MarketPulse** 图形界面。

数据库 `market_data.db`、日志会写在 **exe 同一目录**（可把 `settings.toml` 放旁边改路径）。

GitHub Actions 在 `windows-latest` 上 `cargo build --release`。打 `v*` 标签即自动挂到 Release。

## 源码运行

```text
python scrapy_server.py
```

浏览器打开 `http://127.0.0.1:8765`。命令行统一入口：

```text
python run_crawlers.py --source all
```

## 不会进仓库的内容

Cookie、SQLite、MinGW 工具链、Rust `target/`、带抓取结果的 HTML 报告都已 gitignore。请把 Cookie 放本机，不要提交。
