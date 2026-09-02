//! 东方财富千股千评 / 估值 —— 对齐 Python `eastmoney_stockcomment_crawler.py`。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
use crate::state::{wait_user_ack, AppState, CrawlStatus, ErrorNotice, UserAck};
use crate::verify::{classify_kind, network_hint, source_verify_links};

const EM_BASE: &str = "https://datacenter-web.eastmoney.com/api/data/v1/get";
const EM_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
const EM_REF: &str = "https://data.eastmoney.com/stockcomment/";
const PAGE_SIZE: usize = 500;

/// 东财千股千评个股页（已实测 /stockcomment/stock/000001.html）
fn em_stock_page_url(code: &str) -> String {
    format!("https://data.eastmoney.com/stockcomment/stock/{code}.html")
}

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

fn fetch_one(
    client: &Client,
    limiter: &mut RateLimiter,
    report: &str,
    filter: &str,
    max_retries: u32,
    timeout: u64,
) -> Option<Value> {
    let url = em_url(report, &[("filter", filter.to_string())]);
    let j = fetch_em_json(client, limiter, &url, max_retries, timeout).ok()?;
    result_data(&j).into_iter().next()
}

fn fetch_stocknew_all(
    client: &Client,
    limiter: &mut RateLimiter,
    max_retries: u32,
    timeout: u64,
    log: &dyn Fn(&str),
) -> (Vec<Value>, Option<String>) {
    let mut rows = Vec::new();
    let mut pn = 1usize;
    let mut last_fail = None;
    log("开始分页拉取东财 RPT_DMSK_TS_STOCKNEW …");
    loop {
        let url = em_url(
            "RPT_DMSK_TS_STOCKNEW",
            &[
                ("pageSize", PAGE_SIZE.to_string()),
                ("pageNumber", pn.to_string()),
                ("sortColumns", "SECURITY_CODE".into()),
                ("sortTypes", "1".into()),
            ],
        );
        match fetch_em_json(client, limiter, &url, max_retries, timeout) {
            Ok(j) => {
                let data = result_data(&j);
                if data.is_empty() {
                    if pn == 1 {
                        log(&em_empty_hint(&url));
                    }
                    break;
                }
                let n = data.len();
                rows.extend(data);
                log(&format!("批量诊断第 {pn} 页: +{n}，累计 {}", rows.len()));
                if n < PAGE_SIZE {
                    break;
                }
                pn += 1;
            }
            Err(e) => {
                last_fail = Some(e.to_string());
                log(&format!("批量诊断第 {pn} 页失败: {e}"));
                break;
            }
        }
    }
    log(&format!("批量诊断完成，共 {} 支", rows.len()));
    (rows, last_fail)
}

fn save_diag_text(
    db: &Db,
    code: &str,
    name: &str,
    td: &str,
    crawl_date: &str,
    client: &Client,
    limiter: &mut RateLimiter,
    max_retries: u32,
    timeout: u64,
) {
    let mut comment = None;
    let mut words = None;
    let mut out_td = td.to_string();
    let mut out_name = name.to_string();
    for (report, field) in [
        ("RPT_STOCK_TRENDVOLUME_COMMENT", "COMMENT_TXT"),
        ("RPT_STOCK_WORDS_PK", "WORDS_EXPLAIN"),
    ] {
        if let Some(row) = fetch_one(
            client,
            limiter,
            report,
            &format!("(SECURITY_CODE=\"{code}\")"),
            max_retries,
            timeout,
        ) {
            if let Some(s) = row.get(field).and_then(|v| v.as_str()) {
                if field == "COMMENT_TXT" {
                    comment = Some(s.to_string());
                } else {
                    words = Some(s.to_string());
                }
            }
            if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
                out_name = n.to_string();
            }
            if let Some(t) = row.get("TRADE_DATE").and_then(|v| v.as_str()) {
                out_td = t.chars().take(10).collect();
            }
        }
    }
    if comment.is_none() && words.is_none() {
        return;
    }
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_diag_text VALUES (?,?,?,?,?,?)",
        params![code, out_name, out_td, comment, words, crawl_date],
    );
}

fn save_diag_prob(
    db: &Db,
    code: &str,
    name: &str,
    td: &str,
    crawl_date: &str,
    client: &Client,
    limiter: &mut RateLimiter,
    max_retries: u32,
    timeout: u64,
) {
    let secu = secucode(code);
    let mut rise1 = None;
    let mut rise5 = None;
    let mut avg1 = None;
    let mut avg5 = None;
    let mut cnt1 = None;
    let mut cnt5 = None;
    let mut ratio = None;
    let mut out_td = td.to_string();
    let mut out_name = name.to_string();
    let mut found = false;
    if let Some(row) = fetch_one(
        client,
        limiter,
        "RPT_STOCK_CHANGERATE",
        &format!("(SECUCODE=\"{secu}\")"),
        max_retries,
        timeout,
    ) {
        rise1 = as_f64(row.get("RISE_1_PROBABILITY").unwrap_or(&Value::Null));
        rise5 = as_f64(row.get("RISE_5_PROBABILITY").unwrap_or(&Value::Null));
        avg1 = as_f64(row.get("AVERAGE_1_INCREASE").unwrap_or(&Value::Null));
        avg5 = as_f64(row.get("AVERAGE_5_INCREASE").unwrap_or(&Value::Null));
        cnt1 = as_i64(row.get("ALL_COUNT_1").unwrap_or(&Value::Null));
        cnt5 = as_i64(row.get("ALL_COUNT_5").unwrap_or(&Value::Null));
        found = true;
        if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
            out_name = n.to_string();
        }
        if let Some(t) = row
            .get("DIAGNOSE_DATE")
            .or_else(|| row.get("TRADE_DATE"))
            .and_then(|v| v.as_str())
        {
            out_td = t.chars().take(10).collect();
        }
    }
    if let Some(row) = fetch_one(
        client,
        limiter,
        "RPT_CUSTOM_STOCK_PK",
        &format!("(SECUCODE=\"{secu}\")"),
        max_retries,
        timeout,
    ) {
        ratio = as_f64(row.get("STOCK_RANK_RATIO").unwrap_or(&Value::Null));
        found = true;
    }
    if !found {
        return;
    }
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_diag_prob VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        params![
            code, out_name, out_td, rise1, rise5, avg1, avg5, cnt1, cnt5, ratio, crawl_date
        ],
    );
}

fn save_participation(
    db: &Db,
    code: &str,
    name: &str,
    td: &str,
    crawl_date: &str,
    client: &Client,
    limiter: &mut RateLimiter,
    max_retries: u32,
    timeout: u64,
) {
    let Some(row) = fetch_one(
        client,
        limiter,
        "RPT_STOCK_PARTICIPATION",
        &format!("(SECURITY_CODE=\"{code}\")"),
        max_retries,
        timeout,
    ) else {
        return;
    };
    let out_name = row
        .get("SECURITY_NAME_ABBR")
        .and_then(|v| v.as_str())
        .unwrap_or(name);
    let out_td = row
        .get("TRADE_DATE")
        .and_then(|v| v.as_str())
        .unwrap_or(td)
        .chars()
        .take(10)
        .collect::<String>();
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_participation VALUES (?,?,?,?,?,?,?,?)",
        params![
            code,
            out_name,
            out_td,
            as_f64(row.get("PARTICIPATION_WISH").unwrap_or(&Value::Null)),
            as_f64(row.get("PARTICIPATION_WISH_5DAYS").unwrap_or(&Value::Null)),
            as_f64(row.get("PARTICIPATION_WISH_CHANGE").unwrap_or(&Value::Null)),
            as_f64(row.get("PARTICIPATION_WISH_5DAYSCHANGE").unwrap_or(&Value::Null)),
            crawl_date,
        ],
    );
}

fn save_popularity(
    db: &Db,
    code: &str,
    name: &str,
    td: &str,
    crawl_date: &str,
    client: &Client,
    limiter: &mut RateLimiter,
    max_retries: u32,
    timeout: u64,
) {
    let mut market_rank = None;
    let mut market_num = None;
    let mut industry_rank = None;
    let mut change_rate = None;
    let mut market_stock_num = None;
    let mut focus_rank = None;
    let mut focus_total = None;
    let mut focus_index = None;
    let mut out_td = td.to_string();
    let mut out_name = name.to_string();
    let mut found = false;
    if let Some(row) = fetch_one(
        client,
        limiter,
        "RPT_STOCK_PK_RANK",
        &format!("(SECURITY_CODE=\"{code}\")"),
        max_retries,
        timeout,
    ) {
        market_rank = as_i64(row.get("MARKET_RANK").unwrap_or(&Value::Null));
        market_num = as_i64(row.get("EVALUATE_MARKET_NUM").unwrap_or(&Value::Null));
        industry_rank = as_i64(row.get("INDUSTRY_RANK").unwrap_or(&Value::Null));
        change_rate = as_f64(row.get("CHANGE_RATE").unwrap_or(&Value::Null));
        market_stock_num = as_i64(row.get("MARKET_STOCK_NUM").unwrap_or(&Value::Null));
        found = true;
        if let Some(n) = row.get("SECURITY_NAME_ABBR").and_then(|v| v.as_str()) {
            out_name = n.to_string();
        }
        if let Some(t) = row.get("TRADE_DATE").and_then(|v| v.as_str()) {
            out_td = t.chars().take(10).collect();
        }
    }
    if let Some(row) = fetch_one(
        client,
        limiter,
        "RPT_STOCK_MARKETFOCUS",
        &format!("(SECURITY_CODE=\"{code}\")"),
        max_retries,
        timeout,
    ) {
        focus_rank = as_i64(row.get("MARKET_FOCUS_RANK").unwrap_or(&Value::Null));
        focus_total = as_i64(row.get("TOTAL_MARKET").unwrap_or(&Value::Null));
        focus_index = as_f64(row.get("MARKET_FOCUS").unwrap_or(&Value::Null));
        found = true;
    }
    if !found {
        return;
    }
    let _ = db.conn().execute(
        "INSERT OR REPLACE INTO em_popularity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            code,
            out_name,
            out_td,
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

fn ask_em(state: &Arc<Mutex<AppState>>, code: &str, name: &str, detail: &str) -> UserAck {
    let kind = classify_kind(detail);
    let hint = if kind == "网络错误" {
        network_hint().to_string()
    } else {
        "未拿到不等于源站确认无数据。请打开东财千股千评页核对。".into()
    };
    wait_user_ack(
        state,
        ErrorNotice {
            kind,
            code: code.to_string(),
            name: name.to_string(),
            detail: detail.to_string(),
            hint,
            links: source_verify_links(code, &["em"]),
        },
    )
}

/// 东财全市场抓取（对齐 Python crawl_market_em）。
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
            let ack = ask_em(&state, "", "HTTP客户端", &format!("网络错误: {e}"));
            if let Ok(mut st) = state.lock() {
                st.status = if ack == UserAck::Stop {
                    CrawlStatus::Stopped
                } else {
                    CrawlStatus::Error
                };
                st.status_msg = e;
            }
            return;
        }
    };

    let mut limiter = RateLimiter::new_ex(
        config.min_interval,
        config.max_per_minute,
        0.0,
        0.0,
        0,
        0.0,
        0.0,
        config.rate_wait_cap,
        60.0,
    );

    let mut raw;
    loop {
        let (rows, page_fail) = fetch_stocknew_all(
            &client,
            &mut limiter,
            config.max_retries,
            config.timeout,
            &log,
        );
        raw = rows;
        if !raw.is_empty() {
            break;
        }
        let list_url = em_url(
            "RPT_DMSK_TS_STOCKNEW",
            &[
                ("pageSize", PAGE_SIZE.to_string()),
                ("pageNumber", "1".into()),
                ("sortColumns", "SECURITY_CODE".into()),
                ("sortTypes", "1".into()),
            ],
        );
        let mut detail = em_empty_hint(&list_url);
        if let Some(e) = page_fail {
            detail = format!("{e}\n{detail}");
        }
        log(&detail);
        let kind = classify_kind(&detail);
        let ack = wait_user_ack(
            &state,
            ErrorNotice {
                kind,
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
        st.status_msg = "东财抓取中".into();
    }
    log(&format!("全市场抓取 {}，共 {} 支", config.trade_date, total));

    let crawl_date = today_cst();
    let t0 = Instant::now();
    let mut done = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    for (i, item) in raw.into_iter().take(total).enumerate() {
        if stop.load(Ordering::SeqCst) {
            if let Ok(mut st) = state.lock() {
                st.status = CrawlStatus::Stopped;
                st.status_msg = "用户中止".into();
            }
            break;
        }
        let code = item
            .get("SECURITY_CODE")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if code.is_empty() {
            continue;
        }
        let name = item
            .get("SECURITY_NAME_ABBR")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let td = item
            .get("TRADE_DATE")
            .and_then(|v| v.as_str())
            .unwrap_or(&config.trade_date)
            .chars()
            .take(10)
            .collect::<String>();

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
                    if let Ok(mut st) = state.lock() {
                        st.skipped = skipped;
                        st.current_code = code;
                    }
                    continue;
                }
                Ok(None) => {}
                Err(e) => log(&format!("skip 查询失败 {code}: {e}")),
            }
        }

        if let Ok(mut st) = state.lock() {
            st.current_code = code.clone();
            st.current_name = name.clone();
        }

        let save_res = (|| -> anyhow::Result<()> {
            db.conn().execute(
                "INSERT OR REPLACE INTO em_comment VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![
                    &code,
                    &name,
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
                    &crawl_date,
                ],
            )?;

            if let Some(v) = fetch_one(
                &client,
                &mut limiter,
                "RPT_VALUEANALYSIS_DET",
                &format!("(SECURITY_CODE=\"{code}\")"),
                config.max_retries,
                config.timeout,
            ) {
                db.conn().execute(
                    "INSERT OR REPLACE INTO em_valuation VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params![
                        &code,
                        v.get("SECURITY_NAME_ABBR")
                            .and_then(|x| x.as_str())
                            .unwrap_or(&name),
                        v.get("TRADE_DATE")
                            .and_then(|x| x.as_str())
                            .unwrap_or(&td)
                            .chars()
                            .take(10)
                            .collect::<String>(),
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
                        &crawl_date,
                    ],
                )?;
            }

            save_diag_text(
                &db,
                &code,
                &name,
                &td,
                &crawl_date,
                &client,
                &mut limiter,
                config.max_retries,
                config.timeout,
            );
            save_diag_prob(
                &db,
                &code,
                &name,
                &td,
                &crawl_date,
                &client,
                &mut limiter,
                config.max_retries,
                config.timeout,
            );
            save_participation(
                &db,
                &code,
                &name,
                &td,
                &crawl_date,
                &client,
                &mut limiter,
                config.max_retries,
                config.timeout,
            );
            save_popularity(
                &db,
                &code,
                &name,
                &td,
                &crawl_date,
                &client,
                &mut limiter,
                config.max_retries,
                config.timeout,
            );
            Ok(())
        })();

        match save_res {
            Ok(()) => {
                let _ = db.bump_crawl_stats_source(&code, true, "ok", "em");
                done += 1;
            }
            Err(e) => {
                failed += 1;
                let _ = db.bump_crawl_stats_source(&code, false, "fail", "em");
                log(&format!(
                    "落库失败 {code}: {e}；请打开源站核对: {}",
                    em_stock_page_url(&code)
                ));
                let ack = ask_em(&state, &code, &name, &format!("落库失败: {e}"));
                if ack == UserAck::Stop {
                    stop.store(true, Ordering::SeqCst);
                }
            }
        }

        if let Ok(mut st) = state.lock() {
            st.done = done;
            st.failed = failed;
            st.skipped = skipped;
        }
        if (i + 1) % 25 == 0 {
            log(&format!(
                "[{}/{}] 真实 {} 跳过 {} 失败 {} 用时 {:.0}s",
                i + 1,
                total,
                done,
                skipped,
                failed,
                t0.elapsed().as_secs_f64()
            ));
        }
    }

    if let Ok(mut st) = state.lock() {
        st.status = CrawlStatus::Done;
        st.status_msg = format!("东财完成: 新增{done} 跳过{skipped} 失败{failed}");
        st.done = done;
        st.skipped = skipped;
        st.failed = failed;
    }
    log(&format!(
        "东财抓取循环结束 真实{done} 跳过{skipped} 失败{failed} 用时 {:.0}s",
        t0.elapsed().as_secs_f64()
    ));
}
