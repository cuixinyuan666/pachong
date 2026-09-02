# 百度财经 AI 技术分析 · 全市场爬虫 (Rust 版)

对齐 Python：`baidu_finance_ai_crawler.py` + `eastmoney_stockcomment_crawler.py`（共用 `market_data.db`）。

## 已对齐能力
- 百度四接口 + Cookie/403 冷却/Selenium 刷 Cookie
- 东财批量诊断 + 估值/定性/概率/参与意愿/市场排名（六表）
- 续跑：`crawl_stats` + `source` + `empty_streak` 空壳惩罚（N/M）
- 限流：`rate_wait_cap`（达上限最多等待）
- GUI：仅百度 / 仅东财 / 一键(百度→东财)

## 可执行文件

```
baidu_finance_rust.exe          # 工作区根目录（已拷贝，静态链接）
```

双击即可打开图形界面。也支持无界面命令行模式（见下）。

## 两种运行方式

### 1. 图形界面（GUI）
双击 `baidu_finance_rust.exe`，窗口内可设置交易日 / 数据库路径 / 限流参数，
点「开始抓取」即开始，实时显示：
- 进度条 + 百分比 + ETA
- 当前股票代码/名称 + 当前接口（评分 / 支撑阻力 / 资金流向 / 投票）
- 限流速率、连续 403 计数、冷却剩余秒数
- 新增 / 跳过 / 失败统计，单只耗时 + 总耗时
- 滚动实时日志

### 2. 无界面（headless，供自动化 / 服务器）
```bash
baidu_finance_rust.exe --headless \
    --trade-date 2026-07-17 \
    --db C:/.../market_data.db \
    --limit 30           # 可选，限制数量
    --min-interval 1.0   # 两次请求最小间隔秒
    --max-per-minute 35  # 每分钟上限
    --force              # 忽略非交易日检查
```
非交易日自动跳过（内置 2026 沪深北休市日历）；已抓的 `(交易日,代码)` 自动跳过（断点续跑）。

## 数据源
- A 股代码清单：内置 `assets/a_stocks.json`（5532 支，`include_str!` 编译期嵌入，零网络依赖）。

## 反爬策略（与 Python 版一致）
- UA 轮换（4 个）、请求随机抖动、429 Retry-After 退避、5xx 指数退避。
- **连续 8 支遭 403 → 自动冷却 600 秒**（自愈），最多 12 轮仍封则停止；抓到 1 支成功即重置计数。

## 数据库表（复用 market_data.db）
`stocks` / `scores` / `support_resistance` / `fund_flow` / `vote`，
均以 `(trade_date, code)` 为主键 UPSERT。

## 源码结构
```
rust_crawler/
  Cargo.toml
  .cargo/config.toml     # gnu 目标的 linker/CC/ar 配置
  src/
    main.rs      # egui UI + headless 入口 + 交易日历
    crawler.rs   # 引擎：遍历/断点续跑/403 冷却/统计
    http.rs      # 4 端点 URL 构造 + JSON 解析 + 限流器
    db.rs        # rusqlite 落盘（5 表 schema + UPSERT）
    models.rs    # 数据模型
    state.rs     # UI/爬虫共享状态 AppState
  assets/
    a_stocks.json          # 内置代码清单
```

## 从源码构建（Windows）
本机默认 Rust 目标为 msvc，但无 MSVC；改用 **MinGW-w64 + gnu 目标**：
```bash
# 1) MinGW-w64 工具链已放在 ../toolchain/mingw64/（gcc 16.1.0）
export PATH="/c/Users/Administrator/WorkBuddy/2026-07-18-17-52-45/toolchain/mingw64/bin:$PATH"
# 2) .cargo/config.toml 已指定 linker=gcc.exe / ar / CC（完整路径）
cargo build --release --target x86_64-pc-windows-gnu
# 产物: target/x86_64-pc-windows-gnu/release/baidu_finance_rust.exe
```
> 关键点：gnu 目标链接需要 `dlltool.exe` / `windres.exe` 在 PATH 上（rustc 按名查找），
> 因此 PATH 必须用 MSYS 形式 `/c/...`（原生子进程能正确解析）。
