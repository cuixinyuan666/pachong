# pachong

百度财经 AI 技术分析 + 东方财富千股千评的本地爬虫与调试页。

## 另一台电脑：从云仓库完整部署 Rust 版 EXE（推荐）

只需能访问 GitHub，**不必在本机安装 Rust**。克隆仓库后运行部署脚本，自动下载最新 Release 里的 exe 与配套脚本。

```powershell
git clone https://github.com/cuixinyuan666/pachong.git
cd pachong
.\scripts\deploy-marketpulse.ps1
```

或双击仓库根目录的 `部署MarketPulse.bat`。

默认安装到当前目录下的 `MarketPulse\`：

| 文件 | 说明 |
|------|------|
| `baidu_finance_rust.exe` | MarketPulse 图形界面（Rust 编译） |
| `baidu_selenium_fallback.py` | 百度 403 时 Selenium 备用脚本 |
| `logs\` | 会话日志目录（运行后写入） |
| `market_data.db` | 首次抓取后自动生成于 exe 同目录 |

### 常用参数

```powershell
# 指定安装目录
.\scripts\deploy-marketpulse.ps1 -InstallDir "D:\MarketPulse"

# 固定某一版本（与 Releases 标签一致）
.\scripts\deploy-marketpulse.ps1 -Tag v1.10.2

# 覆盖已下载文件
.\scripts\deploy-marketpulse.ps1 -Force

# 本机已装 Rust，从源码编译（需先 clone 仓库）
.\scripts\deploy-marketpulse.ps1 -BuildFromSource -InstallDir "D:\MarketPulse"
```

### 两台电脑协作（爬取机 + 查看机）

1. **爬取机**：部署后运行 exe，数据写入同目录 `market_data.db`；可用界面「发送到 Telegram」备份。
2. **查看机**：同样执行上述部署，在界面「下载历史库」或「导入本地历史库」恢复数据后继续查询/排名。

## 手动下载 exe

到 [Releases](https://github.com/cuixinyuan666/pachong/releases) 下载 `baidu_finance_rust-windows-x64.exe` 与 `baidu_selenium_fallback.py`，放在同一文件夹后双击 exe。

数据库 `market_data.db`、日志会写在 **exe 同一目录**（可把 `settings.toml` 放旁边改路径）。

GitHub Actions 在 `windows-latest` 上 `cargo build --release`。打 `v*` 标签即自动挂到 Release。

## 源码运行（Python 调试页）

```text
python scrapy_server.py
```

浏览器打开 `http://127.0.0.1:8765`。命令行统一入口：

```text
python run_crawlers.py --source all
```

## 不会进仓库的内容

Cookie、SQLite、MinGW 工具链、Rust `target/`、带抓取结果的 HTML 报告都已 gitignore。请把 Cookie 放本机，不要提交。
