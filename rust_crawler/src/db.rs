//! SQLite 时序落盘：复用 market_data.db 的 5 张表结构（与 Python 版完全一致），
//! 以 (update_time, code) 为主键 UPSERT（support_resistance 含 cycle），保证数据连续、不丢。
//! 与 Python 版一致：百度接口只返回当前单日分析，按"真实分析日"去重，避免伪交易日。
//!
//! 三层日期 + 空壳标记（与 Python 爬虫一致）：
//!   - crawl_date : 实际抓取日历日(Asia/Shanghai)，审计"哪天抓的"。
//!   - trade_date : 批次标签（周末/假日回退到上一交易日，与百度返回日一致）。
//!   - update_time: 百度返回的真实分析日（权威，主键组成，去重依据）。
//!   - status     : 'ok'=真实数据已落库；'empty'=百度未返回分析(空壳)，续爬可重试。
//!
//! 使用【单连接 + WAL + busy_timeout】，支持与 Python 爬虫先后/安全地访问同一份库：
//! 本程序在开始抓取前会先停止 Python 爬虫（禁止并存），单连接也避免频繁开关连接的开销。

use rusqlite::{params, Connection};
use chrono::{Utc, Duration as ChronoDuration};
use crate::models::*;

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS stocks (
    code TEXT PRIMARY KEY, name TEXT, market TEXT
);
CREATE TABLE IF NOT EXISTS scores (
    trade_date TEXT, code TEXT, name TEXT,
    synthesis TEXT, technology TEXT, capital TEXT, market TEXT, finance TEXT,
    is_new TEXT, update_time TEXT,
    crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code)
);
CREATE TABLE IF NOT EXISTS support_resistance (
    trade_date TEXT, code TEXT, cycle TEXT, support_level TEXT, resistance_level TEXT,
    level_desc TEXT, rating_text TEXT, rating_level TEXT, rating_status TEXT,
    bullish_events TEXT, bearish_events TEXT, rank_str TEXT, industry_name TEXT,
    update_time TEXT, crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code, cycle)
);
CREATE TABLE IF NOT EXISTS fund_flow (
    trade_date TEXT, code TEXT, super_net REAL, large_net REAL, medium_net REAL,
    little_net REAL, super_rate TEXT, large_rate TEXT, medium_rate TEXT,
    little_rate TEXT, main_net REAL, update_time TEXT, crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code)
);
CREATE TABLE IF NOT EXISTS vote (
    trade_date TEXT, code TEXT, vote_up TEXT, vote_down TEXT, total_num TEXT,
    vote_up_rate TEXT, vote_down_rate TEXT, week_up TEXT, week_down TEXT, week_rate TEXT,
    update_time TEXT, crawl_date TEXT, status TEXT,
    PRIMARY KEY (update_time, code)
);
-- 逐股累计抓取统计（按 code 独立建表，不受 update_time 变动影响）：
--   crawl_count  累计被尝试抓取的次数（成功/空壳/失败都 +1；续跑跳过不计入）
--   last_success 最近一次尝试是否成功拿到真实分析（1=成功 0=空壳或失败）
--   last_status  'ok' / 'empty' / 'fail'
--   last_attempt 最近一次尝试的日历日（crawl_date）
--   updated_at   最近一次更新的 CST 时间戳
CREATE TABLE IF NOT EXISTS crawl_stats (
    code TEXT NOT NULL,
    crawl_count INTEGER NOT NULL DEFAULT 0,
    last_success INTEGER NOT NULL DEFAULT 0,
    last_status TEXT,
    last_attempt TEXT,
    updated_at TEXT,
    source TEXT NOT NULL DEFAULT 'baidu',
    empty_streak INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (code, source)
);
";

/// 东财六表（与 Python eastmoney_stockcomment_crawler.SCHEMA 一致）
pub const EM_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS em_comment (
    code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
    emc_total_score REAL, emc_rank INTEGER, emc_rank_up INTEGER,
    emc_focus REAL, emc_org_participate REAL, emc_ratio REAL,
    emc_prime_cost REAL, emc_prime_cost_20d REAL, emc_prime_cost_60d REAL,
    emc_prime_inflow REAL, emc_superdeal_in REAL, emc_superdeal_out REAL,
    emc_bigdeal_in REAL, emc_bigdeal_out REAL,
    emc_buy_superdeal_ratio REAL, emc_buy_bigdeal_ratio REAL,
    crawl_date TEXT,
    PRIMARY KEY (trade_date, code)
);
CREATE TABLE IF NOT EXISTS em_valuation (
    code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
    emv_pe_ttm REAL, emv_pe_lar REAL, emv_pb_mrq REAL,
    emv_pcf_ocf_lar REAL, emv_pcf_ocf_ttm REAL, emv_ps_ttm REAL, emv_peg REAL,
    emv_total_market_cap REAL, emv_float_market_cap REAL, emv_board TEXT,
    crawl_date TEXT,
    PRIMARY KEY (trade_date, code)
);
CREATE TABLE IF NOT EXISTS em_diag_text (
    code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
    emt_comment_txt TEXT, emt_words_explain TEXT, crawl_date TEXT,
    PRIMARY KEY (trade_date, code)
);
CREATE TABLE IF NOT EXISTS em_diag_prob (
    code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
    emt_rise_1_prob REAL, emt_rise_5_prob REAL,
    emt_avg_1_inc REAL, emt_avg_5_inc REAL,
    emt_all_count_1 INTEGER, emt_all_count_5 INTEGER,
    emt_rank_ratio REAL, crawl_date TEXT,
    PRIMARY KEY (trade_date, code)
);
CREATE TABLE IF NOT EXISTS em_participation (
    code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
    emp_wish REAL, emp_wish_5d REAL, emp_wish_change REAL, emp_wish_5d_change REAL,
    crawl_date TEXT,
    PRIMARY KEY (trade_date, code)
);
CREATE TABLE IF NOT EXISTS em_popularity (
    code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
    emp_market_rank INTEGER, emp_market_num INTEGER, emp_industry_rank INTEGER,
    emp_change_rate REAL, emp_market_stock_num INTEGER,
    emp_focus_rank INTEGER, emp_focus_total INTEGER, emp_focus_index REAL,
    crawl_date TEXT,
    PRIMARY KEY (trade_date, code)
);
";

/// 持有单条 SQLite 连接的句柄。所有读写共用它，并预置 WAL + busy_timeout。
pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(db_path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(db_path)?;
        // WAL：提升并发读写能力；busy_timeout：与 Python 先后访问同一库时等待锁而非报错；
        // synchronous=NORMAL：WAL 下足够安全且更快。
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; \
             PRAGMA busy_timeout=15000; \
             PRAGMA synchronous=NORMAL;",
        )?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(EM_SCHEMA)?;
        let db = Self { conn };
        db.ensure_extra_columns()?;
        db.ensure_crawl_stats_v3()?;
        Ok(db)
    }

    /// 对齐 Python：crawl_stats 补 source / empty_streak，并迁移为 (code, source) 主键。
    fn ensure_crawl_stats_v3(&self) -> rusqlite::Result<()> {
        let cols: Vec<String> = self
            .conn
            .prepare("PRAGMA table_info(crawl_stats)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if cols.is_empty() {
            return Ok(());
        }
        if !cols.iter().any(|c| c == "source") {
            self.conn
                .execute("ALTER TABLE crawl_stats ADD COLUMN source TEXT DEFAULT 'baidu'", [])?;
            let _ = self
                .conn
                .execute("UPDATE crawl_stats SET source='baidu' WHERE source IS NULL", []);
        }
        if !cols.iter().any(|c| c == "empty_streak") {
            self.conn.execute(
                "ALTER TABLE crawl_stats ADD COLUMN empty_streak INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        // 若仍是旧 PK(code)，重建为 (code, source)
        let sql: String = self
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='crawl_stats'",
                [],
                |r| r.get(0),
            )
            .unwrap_or_default();
        let needs_rebuild = sql.contains("code TEXT PRIMARY KEY")
            || (!sql.contains("PRIMARY KEY (code, source)")
                && !sql.contains("PRIMARY KEY(code, source)"));
        if needs_rebuild && sql.contains("crawl_stats") {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS crawl_stats_v3 (
                    code TEXT NOT NULL,
                    crawl_count INTEGER NOT NULL DEFAULT 0,
                    last_success INTEGER NOT NULL DEFAULT 0,
                    last_status TEXT,
                    last_attempt TEXT,
                    updated_at TEXT,
                    source TEXT NOT NULL DEFAULT 'baidu',
                    empty_streak INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (code, source)
                );
                INSERT OR IGNORE INTO crawl_stats_v3
                    (code, crawl_count, last_success, last_status, last_attempt, updated_at, source, empty_streak)
                SELECT code, crawl_count, last_success, last_status, last_attempt, updated_at,
                       COALESCE(source,'baidu'), COALESCE(empty_streak,0)
                FROM crawl_stats;
                DROP TABLE crawl_stats;
                ALTER TABLE crawl_stats_v3 RENAME TO crawl_stats;",
            )?;
        }
        Ok(())
    }

    /// 幂等地为四表补齐 crawl_date / status 列（仅当缺失时 ALTER）。
    /// 这样无论是本 SCHEMA 新建的库，还是历史库（建表时缺列），都能正确写入新列。
    fn ensure_extra_columns(&self) -> rusqlite::Result<()> {
        for tbl in ["scores", "support_resistance", "fund_flow", "vote"] {
            let cols: Vec<String> = self
                .conn
                .prepare(&format!("PRAGMA table_info({})", tbl))?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if !cols.iter().any(|c| c == "crawl_date") {
                self.conn
                    .execute(&format!("ALTER TABLE {} ADD COLUMN crawl_date TEXT", tbl), [])?;
            }
            if !cols.iter().any(|c| c == "status") {
                self.conn
                    .execute(&format!("ALTER TABLE {} ADD COLUMN status TEXT", tbl), [])?;
            }
        }
        Ok(())
    }

    /// 对齐 Python should_skip_code：ok 新鲜跳过，或空壳冷却跳过。
    /// 返回 Some("ok") | Some("empty_cooldown") | None
    pub fn should_skip(
        &self,
        code: &str,
        source: &str,
        fresh_days: i64,
        empty_limit: i64,
        empty_cooldown_days: i64,
    ) -> rusqlite::Result<Option<&'static str>> {
        let today = (Utc::now() + ChronoDuration::hours(8))
            .date_naive();
        let ok_cutoff = (today + ChronoDuration::days(-(fresh_days.max(0))))
            .format("%Y-%m-%d")
            .to_string();
        let row = self.conn.query_row(
            "SELECT last_status, substr(last_attempt,1,10), COALESCE(empty_streak,0)
             FROM crawl_stats
             WHERE code=? AND COALESCE(source,'baidu')=? LIMIT 1",
            params![code, source],
            |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            },
        );
        let (status, last_day, streak) = match row {
            Ok(v) => v,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e),
        };
        if status.as_deref() == Some("ok") {
            if let Some(d) = &last_day {
                if d.as_str() >= ok_cutoff.as_str() {
                    return Ok(Some("ok"));
                }
            }
        }
        if empty_limit > 0 && empty_cooldown_days > 0 && status.as_deref() == Some("empty") {
            if streak >= empty_limit {
                let cool_cutoff = (today + ChronoDuration::days(-(empty_cooldown_days)))
                    .format("%Y-%m-%d")
                    .to_string();
                if let Some(d) = &last_day {
                    if d.as_str() >= cool_cutoff.as_str() {
                        return Ok(Some("empty_cooldown"));
                    }
                }
            }
        }
        Ok(None)
    }

    /// 旧接口：保留给完整性/其它调用；内部改走 should_skip(source=baidu)。
    pub fn exists(&self, trade_date: &str, fresh_days: i64, code: &str) -> rusqlite::Result<bool> {
        let _ = trade_date;
        Ok(self
            .should_skip(code, "baidu", fresh_days, 0, 0)?
            .is_some())
    }

    /// 该交易日已抓取的股票数（无论由 Python 还是本程序写入）。
    pub fn count_for_date(&self, trade_date: &str) -> rusqlite::Result<usize> {
        let n = self.conn.query_row(
            "SELECT COUNT(*) FROM scores WHERE trade_date=?",
            params![trade_date],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(n as usize)
    }

    /// 续爬候选交易日：返回 scores 中数据最多的那个 trade_date。
    /// 用于双击 exe 时自动续上「上次没爬完的那天」，而不是默认成今天。
    pub fn resume_candidate_date(&self) -> Option<String> {
        self.conn
            .query_row(
                "SELECT trade_date FROM scores GROUP BY trade_date ORDER BY COUNT(*) DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
    }

    pub fn save_snapshot(
        &mut self,
        trade_date: &str,
        stock: &StockRef,
        res: &StockResult,
    ) -> rusqlite::Result<()> {
        let name = res
            .name
            .clone()
            .or_else(|| Some(stock.name.clone()))
            .unwrap_or_default();
        // 三层日期：crawl_date=实际抓取日历日(Asia/Shanghai)；update_time=百度真实分析日(权威)；
        // trade_date=批次标签。空壳(update_time 为空)以 trade_date 作占位主键防堆积。
        let crawl_date = (Utc::now() + ChronoDuration::hours(8))
            .date_naive()
            .format("%Y-%m-%d")
            .to_string();
        let updated_at = (Utc::now() + ChronoDuration::hours(8))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let has_analysis = res.scores.update_time.is_some();
        let status = if has_analysis { "ok" } else { "empty" };
        let pk_update_time = res
            .scores
            .update_time
            .clone()
            .unwrap_or_else(|| trade_date.to_string());

        // 单事务落盘：任一句失败自动回滚，避免「scores 已写、其余 4 表缺」的半截行
        // （否则续爬以 scores 存在为跳过依据，会把残缺股票永久跳过）。
        let tx = self.conn.transaction()?;

        tx.execute(
            "INSERT OR REPLACE INTO stocks VALUES (?,?,?)",
            params![&stock.code, &name, "ab"],
        )?;

        tx.execute(
            "INSERT OR REPLACE INTO scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                &trade_date,
                &stock.code,
                &name,
                &res.scores.synthesis_rating,
                &res.scores.technology,
                &res.scores.capital,
                &res.scores.market,
                &res.scores.finance,
                &res.scores.is_new,
            &pk_update_time,
            &crawl_date,
            &status,
        ],
    )?;

    // 累计抓取统计：对齐 Python（source=baidu + empty_streak）
    let streak_expr = match status {
        "empty" => "COALESCE(empty_streak, 0) + 1",
        "ok" => "0",
        _ => "COALESCE(empty_streak, 0)",
    };
    let streak_ins: i64 = if status == "empty" { 1 } else { 0 };
    tx.execute(
        &format!(
            "INSERT INTO crawl_stats (code, crawl_count, last_success, last_status, last_attempt, updated_at, source, empty_streak) \
             VALUES (?1, 1, ?2, ?3, ?4, ?5, 'baidu', ?6) \
             ON CONFLICT(code, source) DO UPDATE SET \
               crawl_count = crawl_count + 1, \
               last_success = excluded.last_success, \
               last_status = excluded.last_status, \
               last_attempt = excluded.last_attempt, \
               updated_at = excluded.updated_at, \
               empty_streak = {streak_expr}"
        ),
        params![
            &stock.code,
            if has_analysis { 1i32 } else { 0i32 },
            &status,
            &crawl_date,
            &updated_at,
            streak_ins,
        ],
    )?;

    // 空壳：不写子表，直接提交（占位 scores 壳已落库，等待重试补齐真实数据）。
    if !has_analysis {
        tx.commit()?;
        return Ok(());
    }

        for s in &res.support {
            tx.execute(
                "INSERT OR REPLACE INTO support_resistance VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![
                    &trade_date,
                    &stock.code,
                    &s.cycle,
                    &s.support_level,
                    &s.resistance_level,
                    &s.level_desc,
                    &s.rating_text,
                    &s.rating_level,
                    &s.rating_status,
                    &s.bullish_events,
                    &s.bearish_events,
                    &s.rank_str,
                    &s.industry_name,
                    &res.scores.update_time,
                    &crawl_date,
                    &status,
                ],
            )?;
        }

        if let Some(ff) = &res.fund_flow {
            tx.execute(
                "INSERT OR REPLACE INTO fund_flow VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![
                    &trade_date,
                    &stock.code,
                    &ff.super_net,
                    &ff.large_net,
                    &ff.medium_net,
                    &ff.little_net,
                    &ff.super_rate,
                    &ff.large_rate,
                    &ff.medium_rate,
                    &ff.little_rate,
                    &ff.main_net,
                    &res.scores.update_time,
                    &crawl_date,
                    &status,
                ],
            )?;
        }

        if let Some(v) = &res.vote {
            tx.execute(
                "INSERT OR REPLACE INTO vote VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![
                    &trade_date,
                    &stock.code,
                    &v.vote_up,
                    &v.vote_down,
                    &v.total_num,
                    &v.vote_up_rate,
                    &v.vote_down_rate,
                    &v.week_up,
                    &v.week_down,
                    &v.week_rate,
                    &res.scores.update_time,
                    &crawl_date,
                    &status,
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// 累计更新逐股抓取统计（crawl_stats）。对齐 Python：source + empty_streak。
    pub fn bump_crawl_stats(
        &self,
        code: &str,
        success: bool,
        status: &str,
    ) -> rusqlite::Result<()> {
        self.bump_crawl_stats_source(code, success, status, "baidu")
    }

    pub fn bump_crawl_stats_source(
        &self,
        code: &str,
        success: bool,
        status: &str,
        source: &str,
    ) -> rusqlite::Result<()> {
        let crawl_date = (Utc::now() + ChronoDuration::hours(8))
            .date_naive()
            .format("%Y-%m-%d")
            .to_string();
        let updated_at = (Utc::now() + ChronoDuration::hours(8))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let streak_expr = match status {
            "empty" => "COALESCE(empty_streak, 0) + 1",
            "ok" => "0",
            _ => "COALESCE(empty_streak, 0)",
        };
        let streak_ins: i64 = if status == "empty" { 1 } else { 0 };
        self.conn.execute(
            &format!(
                "INSERT INTO crawl_stats (code, crawl_count, last_success, last_status, last_attempt, updated_at, source, empty_streak) \
                 VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(code, source) DO UPDATE SET \
                   crawl_count = crawl_count + 1, \
                   last_success = excluded.last_success, \
                   last_status = excluded.last_status, \
                   last_attempt = excluded.last_attempt, \
                   updated_at = excluded.updated_at, \
                   empty_streak = {streak_expr}"
            ),
            params![
                code,
                if success { 1i32 } else { 0i32 },
                status,
                &crawl_date,
                &updated_at,
                source,
                streak_ins,
            ],
        )?;
        Ok(())
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// 某 (trade_date, code) 在指定表中是否存在一行。
    /// 表名取自白名单常量，绝不接受外部输入，安全。
    fn row_exists(&self, table: &str, trade_date: &str, code: &str) -> rusqlite::Result<bool> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE trade_date=? AND code=? LIMIT 1",
            table
        );
        let r = self.conn.query_row(&sql, params![trade_date, code], |_| Ok(()));
        Ok(r.is_ok())
    }

    /// 单只股票在某交易日的四大数据项完整性。
    /// - `scores`：scores 表有行即视为存在（save_snapshot 必写 scores）。
    /// - `support`：support_resistance 表任一 cycle 有行即视为存在。
    /// - `fund_flow` / `vote`：对应表有行即视为存在。
    ///
    /// 仅 SELECT，不改库。
    pub fn completeness(
        &self,
        trade_date: &str,
        code: &str,
    ) -> rusqlite::Result<Completeness> {
        let scores = self.row_exists("scores", trade_date, code)?;
        let support = self.row_exists("support_resistance", trade_date, code)?;
        let fund_flow = self.row_exists("fund_flow", trade_date, code)?;
        let vote = self.row_exists("vote", trade_date, code)?;
        Ok(Completeness {
            scores,
            support,
            fund_flow,
            vote,
        })
    }
}

/// 四大数据项的存在性快照。
#[derive(Debug, Clone, Default)]
pub struct Completeness {
    pub scores: bool,
    pub support: bool,
    pub fund_flow: bool,
    pub vote: bool,
}

impl Completeness {
    /// 返回缺失项名称列表（scores / support_resistance / fund_flow / vote）。
    pub fn missing_vec(&self) -> Vec<String> {
        let mut v = Vec::new();
        if !self.scores {
            v.push("scores".to_string());
        }
        if !self.support {
            v.push("support_resistance".to_string());
        }
        if !self.fund_flow {
            v.push("fund_flow".to_string());
        }
        if !self.vote {
            v.push("vote".to_string());
        }
        v
    }
}
