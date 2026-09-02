# Rust 爬虫集成百度+东财 - 实施方案

## 📋 当前状态

| 组件 | 百度抓取 | 东财抓取 | 状态 |
|------|---------|---------|------|
| Python 百度 | ✅ | ❌ | 已实现 |
| Python 东财 | ❌ | ✅ | 已实现 |
| Web 界面 | ⚠️ 分离按钮 | ⚠️ 分离按钮 | **已添加组合按钮** ✅ |
| Rust 百度 | ✅ | ❌ | 仅百度 |

---

## ✅ Web 界面修改完成

### 新增功能

1. **"一键抓取"按钮** (橙色，置顶显示)
   - 点击后自动先抓百度，完成后自动抓东财
   - 进度实时显示在日志区域
   - 状态显示当前阶段（百度/东财/完成）

2. **独立按钮**（保持不变）
   - "▶ 仅百度" - 只抓百度
   - "▶ 仅东财" - 只抓东财

3. **API 端点**
   - `/api/scrape_combined` - 触发组合抓取
   - `/api/scrape` - 百度（已有）
   - `/api/scrape_em` - 东财（已有）

### 使用方式

访问 http://127.0.0.1:8765，点击 **"⚡ 一键抓取（百度+东财）"** 即可！

---

## 🔧 Rust 需要做的修改

### Step 1: 添加东方财富数据源

修改 `src/main.rs` 或新增模块 `em_crawler.rs`：

```rust
// 新增文件: src/em_crawler.rs
use rusqlite::{Connection, Result};

pub struct EastMoneyCrawler {
    client: reqwest::blocking::Client,
    db_path: String,
    rate_limiter: RateLimiter,
}

impl EastMoneyCrawler {
    pub fn new(db_path: &str) -> Self {
        Self {
            client: reqwest::blocking::Client::builder()
                .user_agent("Mozilla/5.0 ...")
                .build().unwrap(),
            db_path: db_path.to_string(),
            rate_limiter: RateLimiter::new(Duration::from_secs(1)),
        }
    }
    
    pub fn crawl_market(&self) -> Result<Stats> {
        // 1. 拉取批量诊断 (RPT_DMSK_TS_STOCKNEW)
        let diagnosis = self.fetch_diagnosis_batch()?;
        
        // 2. 逐股拉估值 (RPT_VALUEANALYSIS_DET)
        for stock in &diagnosis.codes {
            let valuation = self.fetch_valuation(stock)?;
            self.save_to_db(stock, &valuation)?;
        }
        
        Ok(Stats { ... })
    }
    
    fn fetch_diagnosis_batch(&self) -> Result<Vec<Stock>> {
        // https://datacenter-web.eastmoney.com/api/data/v1/get?
        // reportName=RPT_DMSK_TS_STOCKNEW&columns=ALL&pageSize=500&pageNumber=1
    }
    
    fn fetch_valuation(&self, code: &str) -> Result<EastMoneyValuation> {
        // https://datacenter-web.eastmoney.com/api/data/v1/get?
        // reportName=RPT_VALUEANALYSIS_DET&filter=(SECURITY_CODE="%s")
    }
}
```

### Step 2: 整合 Baidu + EastMoney 到 CLI

修改 `main.rs` 添加新命令：

```rust
// src/main.rs 中添加

enum Command {
    Baidu,
    EastMoney,
    Combined,  // 新增
}

fn main() {
    let args = Args::parse();
    
    match args.command {
        Command::Baidu => {
            let crawler = BaiduCrawler::new(&args.db);
            let stats = crawler.crawl_market(args.trade_date, args.limit);
            println!("百度完成: {:?}", stats);
        },
        Command::EastMoney => {
            let crawler = EastMoneyCrawler::new(&args.db);
            let stats = crawler.crawl_market(args.trade_date, args.limit);
            println!("东财完成: {:?}", stats);
        },
        Command::Combined => {
            // 先百度，再东财
            {
                let crawler = BaiduCrawler::new(&args.db);
                let stats = crawler.crawl_market(args.trade_date.clone(), args.limit);
                println!("百度完成: {:?}", stats);
            }
            
            // 东财
            {
                let crawler = EastMoneyCrawler::new(&args.db);
                let stats = crawler.crawl_market(args.trade_date, args.limit);
                println!("东财完成: {:?}", stats);
            }
        },
    }
}

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
    
    #[arg(long)]
    trade_date: Option<String>,
    
    #[arg(long)]
    db: Option<String>,
    
    #[arg(long)]
    limit: Option<usize>,
}
```

### Step 3: 共享数据库 Schema

确保 Rust 和 Python 写入相同的表结构：

```rust
// 百度表
CREATE TABLE scores (...);
CREATE TABLE support_resistance (...);
CREATE TABLE fund_flow (...);
CREATE TABLE vote (...);

// 东财表
CREATE TABLE em_comment (...);
CREATE TABLE em_valuation (...);
CREATE TABLE em_diag_text (...);
CREATE TABLE em_diag_prob (...);
CREATE TABLE em_participation (...);
CREATE TABLE em_popularity (...);
```

### Step 4: 进度输出格式统一

为了与 Web 界面兼容，Rust 也输出 `[prog] CODE status` 格式：

```rust
impl BaiduCrawler {
    fn on_progress(&self, code: &str, status: &str) {
        println!("[prog] {} {}", code, status);
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
}
```

---

## 📊 集成效果对比

### 修改前

```
Web 界面:
├─ [▶ 百度] ──→ Python 百度爬虫 ──→ market_data.db (scores...)
└─ [▶ 东财] ──→ Python 东财爬虫 ──→ market_data.db (em_*)

Rust:
└─ baidu_finance_rust.exe ──→ market_data.db
```

### 修改后

```
Web 界面:
├─ [⚡ 一键抓取] ──→ Python 百度 → Python 东财 ──→ market_data.db
├─ [▶ 仅百度] ───→ Python 百度爬虫 ──→ market_data.db (scores...)
├─ [▶ 仅东财] ───→ Python 东财爬虫 ──→ market_data.db (em_*)
└─ [▶ Rust] ────→ Rust 百度爬虫 ──→ market_data.db (scores...)

Rust (新增):
├─ baidu_finance_rust.exe combined ──→ market_data.db (百度+东财)
└─ baidu_finance_rust.exe eastmoney ──→ market_data.db (东财)
```

---

## 🎯 立即可以使用的功能

### Web 界面（已生效）

1. 启动 Web 服务：
   ```bash
   python scrapy_server.py
   ```

2. 访问 http://127.0.0.1:8765

3. 点击 **"⚡ 一键抓取（百度+东财）"**

4. 等待完成（预计 2-4 小时）

5. 查看结果表格

---

## 📝 Rust 后续工作清单

- [ ] 创建 `src/em_crawler.rs`
- [ ] 实现 `EastMoneyCrawler` struct
- [ ] 添加 CLI 参数 `combined` 和 `eastmoney`
- [ ] 测试批量接口调用
- [ ] 测试单股估值接口
- [ ] 数据库写入测试
- [ ] 与 Web 界面日志格式对齐

---

**总结**: Web 界面已经实现了"一键抓取"功能！Rust 版本需要额外开发才能支持东财数据。
