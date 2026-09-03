//! 东方财富千股千评 / 估值。
//! 能批量分页的报表一次拉全市场；只对估值、打败% 逐股（接口强制带代码）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use chrono::{Duration as ChronoDuration, Utc};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, REFERER, USER_AGENT};
use rusqlite::params;
use serde_json::Value;

use crate::crawler::CrawlConfig;
use crate::db::Db;
use crate::http::RateLimiter;
use crate::settings::Settings;
use crate::state::{note_problem, wait_user_ack, AppState, CrawlStatus, ErrorNotice, UserAck};
use crate::verify::{classify_kind, network_hint, source_verify_links};

const EM_BASE: &str = "https://datacenter-web.eastmoney.com/api/data/v1/get";
const EM_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
const EM_REF: &str = "https://data.eastmoney.com/stockcomment/";
const PAGE_SIZE: usize = 500;
const LEFTOVER_WORKERS: usize = 8;
const PASS_RETRY: usize = 1;

fn em_empty_hint(url: &str) -> String {
    format!(
        "本次爬取未拿到数据（待人工确认，不是确认无数据）。请打开源站核对：\n  东方财富 · 千股千评列表页: {EM_REF}\n  东方财富 · 本次请求接口: {url}"
    )
}

fn em_headers() -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(USER_AGENT, HeaderValue::from_static(EM_UA));
    h.insert(REFERER, HeaderValue::from_static(EM_REF));
    h.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/plain, */*"),
    );
    h
}

fn secucode(code: &str) -> String {
    if code.starts_with('6') {
        format!("{code}.SH")
    } else if code.starts_with('8') || code.starts_with('4') {
        format!("{code}.BJ")
    } else {
        format!("{code}.SZ")
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_f64().map(|f| f as i64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn today_cst() -> String {
    (Utc::now() + ChronoDuration::hours(8))
        .date_naive()
        .format("%Y-%m-%d")
        .to_string()
}

fn ymd(s: &str) -> String {
    s.chars().take(10).collect()
}

fn code_of(v: &Value) -> String {
    v.get("SECURITY_CODE")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn row_date(v: &Value) -> String {
    v.get("TRADE_DATE")
        .or_else(|| v.get("DIAGNOSE_DATE"))
        .and_then(|x| x.as_str())
        .map(ymd)
        .unwrap_or_default()
}

fn em_url(report: &str, extra: &[(&str, String)]) -> String {
    let mut q: Vec<(String, String)> = vec![
        ("reportName".to_string(), report.to_string()),
        ("columns".to_string(), "ALL".to_string()),
        ("client".to_string(), "PC".to_string()),
        ("source".to_string(), "WEB".to_string()),
    ];
    for (k, v) in extra {
        q.push(((*k).to_string(), v.clone()));
    }
    let qs: String = q
        .iter()
        .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{EM_BASE}?{qs}")
}

fn result_data(v: &Value) -> Vec<Value> {
    v.get("result")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default()
}

fn fetch_em_json(
    client: &Client,
    limiter: &mut RateLimiter,
    url: &str,
    max_retries: u32,
    timeout_secs: u64,
) -> anyhow::Result<Value> {
    let headers = em_headers();
    let mut last_err = String::new();
    for attempt in 1..=max_retries.max(1) {
        limiter.wait();
        match crate::http::send_get_with_fallback(
            client,
            url,
            Some(&headers),
            std::time::Duration::from_secs(timeout_secs),
        ) {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 403 {
                    anyhow::bail!("HTTP 403");
                }
                if !status.is_success() {
                    last_err = format!("HTTP {}", status.as_u16());
                    std::thread::sleep(std::time::Duration::from_secs_f64(
                        2.0_f64.powi((attempt - 1) as i32) + rand::random::<f64>(),
                    ));
                    continue;
                }
                let text = resp.text().unwrap_or_default();
                match serde_json::from_str::<Value>(&text) {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        last_err = e.to_string();
                        continue;
                    }
                }
            }
            Err(e) => {
                last_err = format!("网络错误: {e}{}", crate::http::proxy_hint_suffix());
                std::thread::sleep(std::time::Duration::from_secs_f64(
                    2.0_f64.powi((attempt - 1) as i32) + rand::random::<f64>(),
                ));
            }
        }
    }
    anyhow::bail!("抓取失败: {last_err}")
}

/// 全市场分页（500/页）。date_filter 形如 (TRADE_DATE='2026-09-02 00:00:00')。
fn fetch_paged(
    client: &Client,
    limiter: &mut RateLimiter,
    report: &str,
    extra: &[(&str, String)],
    max_retries: u32,
    timeout: u64,
    log: &dyn Fn(&str),
    label: &str,
) -> Vec<Value> {
    let mut rows = Vec::new();
    let mut pn = 1usize;
    log(&format!("东财批量 {label} …"));
    loop {
        let mut params = extra.to_vec();
        params.push(("pageSize", PAGE_SIZE.to_string()));
        params.push(("pageNumber", pn.to_string()));
        let url = em_url(report, &params);
        match fetch_em_json(client, limiter, &url, max_retries, timeout) {
            Ok(j) => {
                let data = result_data(&j);
                if data.is_empty() {
                    break;
                }
                let n = data.len();
                rows.extend(data);
                log(&format!("{label} 第 {pn} 页 +{n}，累计 {}", rows.len()));
                if n < PAGE_SIZE {
                    break;
                }
                pn += 1;
            }
            Err(e) => {
                log(&format!("{label} 第 {pn} 页失败，自动再试: {e}"));
                match fetch_em_json(client, limiter, &url, max_retries, timeout) {
                    Ok(j) => {
                        let data = result_data(&j);
                        if data.is_empty() {
                            break;
                        }
                        let n = data.len();
                        rows.extend(data);
                        if n < PAGE_SIZE {
                            break;
                        }
                        pn += 1;
                    }
                    Err(e2) => {
                        log(&format!("{label} 第 {pn} 页仍失败，停止该表: {e2}"));
                        break;
                    }
                }
            }
        }
    }
    log(&format!("{label} 完成，共 {} 条", rows.len()));
    rows
}

fn fetch_one(
    client: &Client,
    limiter: &mut RateLimiter,
    report: &str,
    filter: &str,
    max_retries: u32,
    timeout: u64,
) -> Option<Value> {
    let url = em_url(
        report,
        &[
            ("filter", filter.to_string()),
            ("pageSize", "1".into()),
            ("pageNumber", "1".into()),
            ("sortColumns", "TRADE_DATE".into()),
            ("sortTypes", "-1".into()),
        ],
    );
    let j = fetch_em_json(client, limiter, &url, max_retries, timeout).ok()?;
    result_data(&j).into_iter().next()
}

fn index_latest(rows: Vec<Value>) -> HashMap<String, Value> {
    let mut m = HashMap::new();
    for row in rows {
        let c = code_of(&row);
        if c.is_empty() {
            continue;
        }
        let td = row_date(&row);
        match m.get(&c) {
            Some(old) if row_date(old) > td => {}
            _ => {
                m.insert(c, row);
            }
        }
    }
    m
}

fn fast_limiter() -> RateLimiter {
    RateLimiter::new_ex(0.05, 800, 0.0, 0.0, 0, 0.0, 0.0, Some(2.0), 60.0)
}

fn insert_comment(db: &Db, item: &Value, crawl_date: &str) {
    let code = code_of(item);
    if code.is_empty() {
        return;
    }
    let name = item
        .get("SECURITY_NAME_ABBR")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let td = row_date(item);
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_comment VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            &code,
            name,
            &td,
            as_f64(item.get("TOTALSCORE").unwrap_or(&Value::Null)),
            as_i64(item.get("RANK").unwrap_or(&Value::Null)),
            as_i64(item.get("RANK_UP").unwrap_or(&Value::Null)),
            as_f64(item.get("FOCUS").unwrap_or(&Value::Null)),
            as_f64(item.get("ORG_PARTICIPATE").unwrap_or(&Value::Null)),
            as_f64(item.get("RATIO").unwrap_or(&Value::Null)),
            as_f64(item.get("PRIME_COST").unwrap_or(&Value::Null)),
            as_f64(item.get("PRIME_COST_20DAYS").unwrap_or(&Value::Null)),
            as_f64(item.get("PRIME_COST_60DAYS").unwrap_or(&Value::Null)),
            as_f64(item.get("PRIME_INFLOW").unwrap_or(&Value::Null)),
            as_f64(item.get("SUPERDEAL_INFLOW").unwrap_or(&Value::Null)),
            as_f64(item.get("SUPERDEAL_OUTFLOW").unwrap_or(&Value::Null)),
            as_f64(item.get("BIGDEAL_INFLOW").unwrap_or(&Value::Null)),
            as_f64(item.get("BIGDEAL_OUTFLOW").unwrap_or(&Value::Null)),
            as_f64(item.get("BUY_SUPERDEAL_RATIO").unwrap_or(&Value::Null)),
            as_f64(item.get("BUY_BIGDEAL_RATIO").unwrap_or(&Value::Null)),
            crawl_date,
        ],
    );
}

fn insert_diag_text(
    db: &Db,
    code: &str,
    fallback_name: &str,
    fallback_td: &str,
    crawl_date: &str,
    comment: Option<&Value>,
    words: Option<&Value>,
) {
    let mut txt = None;
    let mut expl = None;
    let mut name = fallback_name.to_string();
    let mut td = fallback_td.to_string();
    if let Some(row) = comment {
        txt = row
            .get("COMMENT_TXT")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
            name = n.to_string();
        }
        let d = row_date(row);
        if !d.is_empty() {
            td = d;
        }
    }
    if let Some(row) = words {
        expl = row
            .get("WORDS_EXPLAIN")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
            name = n.to_string();
        }
    }
    if txt.is_none() && expl.is_none() {
        return;
    }
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_diag_text VALUES (?,?,?,?,?,?)",
        params![code, name, td, txt, expl, crawl_date],
    );
}

fn insert_diag_prob(
    db: &Db,
    code: &str,
    fallback_name: &str,
    fallback_td: &str,
    crawl_date: &str,
    chg: Option<&Value>,
    pk: Option<&Value>,
) {
    let mut name = fallback_name.to_string();
    let mut td = fallback_td.to_string();
    let mut rise1 = None;
    let mut rise5 = None;
    let mut avg1 = None;
    let mut avg5 = None;
    let mut cnt1 = None;
    let mut cnt5 = None;
    let mut ratio = None;
    let mut found = false;
    if let Some(row) = chg {
        found = true;
        rise1 = as_f64(row.get("RISE_1_PROBABILITY").unwrap_or(&Value::Null));
        rise5 = as_f64(row.get("RISE_5_PROBABILITY").unwrap_or(&Value::Null));
        avg1 = as_f64(row.get("AVERAGE_1_INCREASE").unwrap_or(&Value::Null));
        avg5 = as_f64(row.get("AVERAGE_5_INCREASE").unwrap_or(&Value::Null));
        cnt1 = as_i64(row.get("ALL_COUNT_1").unwrap_or(&Value::Null));
        cnt5 = as_i64(row.get("ALL_COUNT_5").unwrap_or(&Value::Null));
        if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
            name = n.to_string();
        }
        let d = row_date(row);
        if !d.is_empty() {
            td = d;
        }
    }
    if let Some(row) = pk {
        found = true;
        ratio = as_f64(row.get("STOCK_RANK_RATIO").unwrap_or(&Value::Null));
    }
    if !found {
        return;
    }
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_diag_prob VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        params![
            code, name, td, rise1, rise5, avg1, avg5, cnt1, cnt5, ratio, crawl_date
        ],
    );
}

fn insert_participation(
    db: &Db,
    code: &str,
    fallback_name: &str,
    fallback_td: &str,
    crawl_date: &str,
    row: &Value,
) {
    let name = row
        .get("SECURITY_NAME_ABBR")
        .and_then(|v| v.as_str())
        .unwrap_or(fallback_name);
    let td = {
        let d = row_date(row);
        if d.is_empty() {
            fallback_td.to_string()
        } else {
            d
        }
    };
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_participation VALUES (?,?,?,?,?,?,?,?)",
        params![
            code,
            name,
            td,
            as_f64(row.get("PARTICIPATION_WISH").unwrap_or(&Value::Null)),
            as_f64(row.get("PARTICIPATION_WISH_5DAYS").unwrap_or(&Value::Null)),
            as_f64(row.get("PARTICIPATION_WISH_CHANGE").unwrap_or(&Value::Null)),
            as_f64(
                row.get("PARTICIPATION_WISH_5DAYSCHANGE")
                    .unwrap_or(&Value::Null)
            ),
            crawl_date,
        ],
    );
}

fn insert_popularity(
    db: &Db,
    code: &str,
    fallback_name: &str,
    fallback_td: &str,
    crawl_date: &str,
    rank: Option<&Value>,
    focus: Option<&Value>,
) {
    let mut name = fallback_name.to_string();
    let mut td = fallback_td.to_string();
    let mut market_rank = None;
    let mut market_num = None;
    let mut industry_rank = None;
    let mut change_rate = None;
    let mut market_stock_num = None;
    let mut focus_rank = None;
    let mut focus_total = None;
    let mut focus_index = None;
    let mut found = false;
    if let Some(row) = rank {
        found = true;
        market_rank = as_i64(row.get("MARKET_RANK").unwrap_or(&Value::Null));
        market_num = as_i64(row.get("EVALUATE_MARKET_NUM").unwrap_or(&Value::Null));
        industry_rank = as_i64(row.get("INDUSTRY_RANK").unwrap_or(&Value::Null));
        change_rate = as_f64(row.get("CHANGE_RATE").unwrap_or(&Value::Null));
        market_stock_num = as_i64(row.get("MARKET_STOCK_NUM").unwrap_or(&Value::Null));
        if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
            name = n.to_string();
        }
        let d = row_date(row);
        if !d.is_empty() {
            td = d;
        }
    }
    if let Some(row) = focus {
        found = true;
        focus_rank = as_i64(row.get("MARKET_FOCUS_RANK").unwrap_or(&Value::Null));
        focus_total = as_i64(row.get("TOTAL_MARKET").unwrap_or(&Value::Null));
        focus_index = as_f64(row.get("MARKET_FOCUS").unwrap_or(&Value::Null));
    }
    if !found {
        return;
    }
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_popularity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            code,
            name,
            td,
            market_rank,
            market_num,
            industry_rank,
            change_rate,
            market_stock_num,
            focus_rank,
            focus_total,
            focus_index,
            crawl_date
        ],
    );
}

fn insert_valuation(db: &Db, code: &str, fallback_name: &str, fallback_td: &str, crawl_date: &str, v: &Value) {
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_valuation VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            code,
            v.get("SECURITY_NAME_ABBR")
                .and_then(|x| x.as_str())
                .unwrap_or(fallback_name),
            {
                let d = row_date(v);
                if d.is_empty() {
                    fallback_td.to_string()
                } else {
                    d
                }
            },
            as_f64(v.get("PE_TTM").unwrap_or(&Value::Null)),
            as_f64(v.get("PE_LAR").unwrap_or(&Value::Null)),
            as_f64(v.get("PB_MRQ").unwrap_or(&Value::Null)),
            as_f64(v.get("PCF_OCF_LAR").unwrap_or(&Value::Null)),
            as_f64(v.get("PCF_OCF_TTM").unwrap_or(&Value::Null)),
            as_f64(v.get("PS_TTM").unwrap_or(&Value::Null)),
            as_f64(v.get("PEG_CAR").unwrap_or(&Value::Null)),
            as_f64(v.get("TOTAL_MARKET_CAP").unwrap_or(&Value::Null)),
            as_f64(v.get("NOTLIMITED_MARKETCAP_A").unwrap_or(&Value::Null)),
            v.get("BOARD_NAME").and_then(|x| x.as_str()),
            crawl_date,
        ],
    );
}

#[derive(Clone)]
struct Leftover {
    code: String,
    name: String,
    td: String,
}

fn crawl_leftover(
    items: Vec<Leftover>,
    client: &Client,
    db: &Arc<Mutex<Db>>,
    state: &Arc<Mutex<AppState>>,
    stop: &Arc<AtomicBool>,
    crawl_date: &str,
    max_retries: u32,
    timeout: u64,
    chg_map: &HashMap<String, Value>,
) -> (usize, usize, Vec<Leftover>) {
    if items.is_empty() {
        return (0, 0, Vec::new());
    }
    let queue = Arc::new(Mutex::new(items));
    let done_n = Arc::new(AtomicUsize::new(0));
    let fail_n = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(Mutex::new(Vec::new()));
    let chg_map = Arc::new(chg_map.clone());
    let crawl_date = crawl_date.to_string();
    let mut handles = Vec::new();
    for _ in 0..LEFTOVER_WORKERS {
        let queue = queue.clone();
        let client = client.clone();
        let db = db.clone();
        let state = state.clone();
        let stop = stop.clone();
        let done_n = done_n.clone();
        let fail_n = fail_n.clone();
        let failed = failed.clone();
        let chg_map = chg_map.clone();
        let crawl_date = crawl_date.clone();
        handles.push(thread::spawn(move || {
            let mut limiter = fast_limiter();
            loop {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let item = {
                    let mut q = match queue.lock() {
                        Ok(g) => g,
                        Err(_) => break,
                    };
                    q.pop()
                };
                let Some(item) = item else { break };
                if let Ok(mut st) = state.lock() {
                    st.current_code = item.code.clone();
                    st.current_name = item.name.clone();
                    st.current_endpoint = "东财估值/打败%".into();
                }
                let val = fetch_one(
                    &client,
                    &mut limiter,
                    "RPT_VALUEANALYSIS_DET",
                    &format!("(SECURITY_CODE=\"{}\")", item.code),
                    max_retries,
                    timeout,
                );
                let pk = fetch_one(
                    &client,
                    &mut limiter,
                    "RPT_CUSTOM_STOCK_PK",
                    &format!("(SECUCODE=\"{}\")", secucode(&item.code)),
                    max_retries,
                    timeout,
                );
                if val.is_none() && pk.is_none() {
                    fail_n.fetch_add(1, Ordering::SeqCst);
                    note_problem(
                        &state,
                        "抓取失败",
                        &item.code,
                        &item.name,
                        "估值与打败%均未拿到（网络或接口失败，将自动补抓）",
                        &["em"],
                    );
                    if let Ok(mut f) = failed.lock() {
                        f.push(item);
                    }
                    continue;
                }
                {
                    let dbg = match db.lock() {
                        Ok(g) => g,
                        Err(_) => {
                            fail_n.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                    };
                    if let Some(v) = &val {
                        insert_valuation(
                            &dbg,
                            &item.code,
                            &item.name,
                            &item.td,
                            &crawl_date,
                            v,
                        );
                    }
                    insert_diag_prob(
                        &dbg,
                        &item.code,
                        &item.name,
                        &item.td,
                        &crawl_date,
                        chg_map.get(&item.code),
                        pk.as_ref(),
                    );
                    let _ = dbg.bump_crawl_stats_source(&item.code, true, "ok", "em");
                }
                done_n.fetch_add(1, Ordering::SeqCst);
                if let Ok(mut st) = state.lock() {
                    st.done = done_n.load(Ordering::SeqCst);
                    st.failed = fail_n.load(Ordering::SeqCst);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let leftover_fail = failed.lock().map(|g| g.clone()).unwrap_or_default();
    (
        done_n.load(Ordering::SeqCst),
        fail_n.load(Ordering::SeqCst),
        leftover_fail,
    )
}

/// 东财全市场抓取：批量报表 + 逐股估值/打败% + 失败补抓。
pub fn run_em_crawler(
    config: CrawlConfig,
    state: Arc<Mutex<AppState>>,
    stop: Arc<AtomicBool>,
    _settings: &Settings,
) {
    let log = |s: &str| {
        if let Ok(mut st) = state.lock() {
            st.push_log(s.to_string());
        }
    };

    let db = match Db::open(&config.db_path) {
        Ok(d) => d,
        Err(e) => {
            let detail = format!("打开数据库失败: {e}");
            let ack = wait_user_ack(
                &state,
                ErrorNotice {
                    kind: "落库失败".into(),
                    code: String::new(),
                    name: "数据库".into(),
                    detail: detail.clone(),
                    hint: "检查 exe 旁的 market_data.db 路径是否可写，磁盘是否满。".into(),
                    links: vec![],
                },
            );
            if let Ok(mut st) = state.lock() {
                st.status = if ack == UserAck::Stop {
                    CrawlStatus::Stopped
                } else {
                    CrawlStatus::Error
                };
                st.status_msg = detail;
            }
            return;
        }
    };

    let client = match crate::http::build_blocking_client(config.timeout) {
        Ok(c) => {
            log(&format!("HTTP 客户端就绪：{}", crate::http::last_proxy_desc()));
            c
        }
        Err(e) => {
            log(&format!("HTTP 客户端失败: {e}"));
            if let Ok(mut st) = state.lock() {
                st.status = CrawlStatus::Error;
                st.status_msg = e;
            }
            return;
        }
    };

    let mut limiter = fast_limiter();
    let mut raw;
    loop {
        raw = fetch_paged(
            &client,
            &mut limiter,
            "RPT_DMSK_TS_STOCKNEW",
            &[
                ("sortColumns", "SECURITY_CODE".into()),
                ("sortTypes", "1".into()),
            ],
            config.max_retries,
            config.timeout,
            &log,
            "千股千评",
        );
        if !raw.is_empty() {
            break;
        }
        let list_url = em_url(
            "RPT_DMSK_TS_STOCKNEW",
            &[
                ("pageSize", PAGE_SIZE.to_string()),
                ("pageNumber", "1".into()),
            ],
        );
        let detail = em_empty_hint(&list_url);
        log(&detail);
        let ack = wait_user_ack(
            &state,
            ErrorNotice {
                kind: classify_kind(&detail),
                code: String::new(),
                name: "东财批量列表".into(),
                detail,
                hint: network_hint().into(),
                links: source_verify_links("000001", &["em"]),
            },
        );
        match ack {
            UserAck::Retry => continue,
            UserAck::Stop => {
                if let Ok(mut st) = state.lock() {
                    st.status = CrawlStatus::Stopped;
                }
                return;
            }
            UserAck::Continue => break,
        }
    }
    let total = match config.limit {
        Some(n) => raw.len().min(n),
        None => raw.len(),
    };
    if let Ok(mut st) = state.lock() {
        st.total = total;
        st.status = CrawlStatus::Running;
        st.status_msg = "东财批量抓取中".into();
    }
    log(&format!(
        "全市场 {} 支。批量拉概率/文字/排名/参与意愿；估值与打败% 仍逐股（{} 路）",
        total, LEFTOVER_WORKERS
    ));

    let data_day = raw
        .first()
        .map(row_date)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| config.trade_date.clone());
    let date_filter = format!("(TRADE_DATE='{data_day} 00:00:00')");
    log(&format!("东财数据日 {data_day}，按该日过滤历史表"));

    let chg_map = index_latest(fetch_paged(
        &client,
        &mut limiter,
        "RPT_STOCK_CHANGERATE",
        &[],
        config.max_retries,
        config.timeout,
        &log,
        "上涨概率",
    ));
    let comment_map = index_latest(fetch_paged(
        &client,
        &mut limiter,
        "RPT_STOCK_TRENDVOLUME_COMMENT",
        &[],
        config.max_retries,
        config.timeout,
        &log,
        "诊断文字",
    ));
    let words_map = index_latest(fetch_paged(
        &client,
        &mut limiter,
        "RPT_STOCK_WORDS_PK",
        &[],
        config.max_retries,
        config.timeout,
        &log,
        "消息面",
    ));
    let rank_map = index_latest(fetch_paged(
        &client,
        &mut limiter,
        "RPT_STOCK_PK_RANK",
        &[],
        config.max_retries,
        config.timeout,
        &log,
        "市场排名",
    ));
    let part_map = index_latest(fetch_paged(
        &client,
        &mut limiter,
        "RPT_STOCK_PARTICIPATION",
        &[("filter", date_filter.clone())],
        config.max_retries,
        config.timeout,
        &log,
        "参与意愿",
    ));
    let focus_map = index_latest(fetch_paged(
        &client,
        &mut limiter,
        "RPT_STOCK_MARKETFOCUS",
        &[("filter", date_filter)],
        config.max_retries,
        config.timeout,
        &log,
        "关注指数",
    ));

    let crawl_date = today_cst();
    let t0 = Instant::now();
    let mut done = 0usize;
    let mut skipped = 0usize;
    let mut leftover: Vec<Leftover> = Vec::new();

    for (i, item) in raw.into_iter().take(total).enumerate() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let code = code_of(&item);
        if code.is_empty() {
            continue;
        }
        let name = item
            .get("SECURITY_NAME_ABBR")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let td = {
            let d = row_date(&item);
            if d.is_empty() {
                config.trade_date.clone()
            } else {
                d
            }
        };
        if config.resume {
            match db.should_skip(
                &code,
                "em",
                config.fresh_days,
                config.empty_limit,
                config.empty_cooldown_days,
            ) {
                Ok(Some(_)) => {
                    skipped += 1;
                    continue;
                }
                Ok(None) => {}
                Err(e) => log(&format!("skip 查询失败 {code}: {e}")),
            }
        }
        insert_comment(&db, &item, &crawl_date);
        insert_diag_text(
            &db,
            &code,
            &name,
            &td,
            &crawl_date,
            comment_map.get(&code),
            words_map.get(&code),
        );
        insert_diag_prob(
            &db,
            &code,
            &name,
            &td,
            &crawl_date,
            chg_map.get(&code),
            None,
        );
        if let Some(p) = part_map.get(&code) {
            insert_participation(&db, &code, &name, &td, &crawl_date, p);
        }
        insert_popularity(
            &db,
            &code,
            &name,
            &td,
            &crawl_date,
            rank_map.get(&code),
            focus_map.get(&code),
        );
        leftover.push(Leftover { code, name, td });
        done += 1;
        if (i + 1) % 500 == 0 {
            log(&format!(
                "批量落库 {}/{} 用时 {:.0}s",
                i + 1,
                total,
                t0.elapsed().as_secs_f64()
            ));
        }
    }

    if let Ok(mut st) = state.lock() {
        st.done = done;
        st.skipped = skipped;
        st.total = leftover.len() + skipped;
        st.status_msg = format!("东财逐股估值 {} 只、{} 路", leftover.len(), LEFTOVER_WORKERS);
    }
    log(&format!(
        "批量落库完成：评论/文字/概率/排名已写入。开始 {} 路补估值与打败%，待抓 {}",
        LEFTOVER_WORKERS,
        leftover.len()
    ));

    let db = Arc::new(Mutex::new(db));
    let mut fail_left = leftover;
    let mut val_done = 0usize;
    let mut val_fail = 0usize;
    for pass in 0..=PASS_RETRY {
        if stop.load(Ordering::SeqCst) || fail_left.is_empty() {
            break;
        }
        if pass > 0 {
            log(&format!(
                "估值/打败% 失败 {} 只，自动补抓第 {} 轮",
                fail_left.len(),
                pass
            ));
        }
        let (d, f, remain) = crawl_leftover(
            std::mem::take(&mut fail_left),
            &client,
            &db,
            &state,
            &stop,
            &crawl_date,
            config.max_retries.max(3),
            config.timeout,
            &chg_map,
        );
        val_done += d;
        val_fail = f;
        fail_left = remain;
    }

    if let Ok(mut st) = state.lock() {
        st.status = if stop.load(Ordering::SeqCst) {
            CrawlStatus::Stopped
        } else {
            CrawlStatus::Done
        };
        st.done = done;
        st.skipped = skipped;
        st.failed = val_fail;
        st.status_msg = format!(
            "东财完成: 批量{done} 跳过{skipped} 估值成功{val_done} 仍失败{val_fail}"
        );
    }
    log(&format!(
        "东财结束 批量{} 跳过{} 估值成功{} 仍失败{} 用时 {:.0}s。失败的下次点「继续」会重爬。完整日志: {}",
        done,
        skipped,
        val_done,
        val_fail,
        t0.elapsed().as_secs_f64(),
        crate::state::session_log_path().display()
    ));
}
