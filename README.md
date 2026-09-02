# pachong

百度财经 AI 技术分析 + 东方财富千股千评的本地爬虫与调试页。

## 下载 exe

到 [Releases](https://github.com/cuixinyuan666/pachong/releases) 下载 `pachong-windows-x64.exe`，双击后会打开本机页面 `http://127.0.0.1:8765`（仅本机，不对外）。

数据库 `market_data.db`、日志、A 股代码清单会生成在 **exe 同一目录**。

GitHub Actions 在每次打 `v*` 标签时自动编译并挂到该 Release。也可在 Actions 里手动 `Run workflow`。

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
