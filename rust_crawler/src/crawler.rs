//! 爬虫引擎：遍历内置代码清单，逐支抓取并落库。
//! 含：断点续跑（跳过已存在的 trade_date+code）、连续 403 自动长冷却（自愈）、限流、统计与状态上报。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::http::{crawl_one, RateLimiter};
use crate::models::StockRef;
use crate::state::{note_problem, wait_user_ack, AppState, CrawlStatus, ErrorNotice, UserAck};
use crate::db;
use crate::settings::Settings;
use crate::checklist::{CheckList, CheckItem, CheckStatus};
use crate::verify::{classify_kind, network_hint, source_verify_links};
use chrono::Utc;

#[derive(Clone)]
pub struct CrawlConfig {
    pub db_path: String,
    pub trade_date: String,
    pub codes: Vec<StockRef>,
    pub min_interval: f64,
    pub max_per_minute: usize,
    pub jitter: f64,
    pub max_retries: u32,
    pub timeout: u64,
    pub limit: Option<usize>,
    /// true=接着爬（跳过已存在且新鲜的）；false=从头爬（全部重抓覆盖）。
    pub resume: bool,
    /// 判重新鲜度窗口(天)，对齐 Python fresh_days。
    pub fresh_days: i64,
    /// 连续空壳 ≥ N 后冷却；0=关闭。对齐 Python empty_limit。
    pub empty_limit: i64,
    /// 空壳冷却天数；0=关闭。对齐 Python empty_cooldown_days。
    pub empty_cooldown_days: i64,
    /// 达每分钟上限最多等待秒；None=等满窗口。
    pub rate_wait_cap: Option<f64>,
}

const CONSEC_403_THRESHOLD: usize = 8;
const COOLDOWN_SEC: f64 = 600.0;
const MAX_COOLDOWNS: usize = 12;

/// 对齐 Python：调用同目录 baidu_selenium_fallback.refresh_and_apply()
fn try_selenium_cookie_refresh(log: &dyn Fn(&str)) -> bool {
    let py = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
    let script = format!(
        "import sys; sys.path.insert(0, r'{}'); from baidu_selenium_fallback import refresh_and_apply; c=refresh_and_apply(); print('OK' if c else 'FAIL'); print(len(c or {{}}))",
        crate::settings::workspace_dir()
    );
    match std::process::Command::new(&py)
        .args(["-c", &script])
        .current_dir(crate::settings::workspace_dir())
        .output()
    {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            if !stderr.trim().is_empty() {
                log(&format!("[B/Selenium stderr] {}", stderr.chars().take(300).collect::<String>()));
            }
            if stdout.contains("OK") {
                let path = format!("{}/baidu_cookies.json", crate::settings::workspace_dir());
                if let Some(c) = crate::http::load_cookies_from_json_file(&path) {
                    crate::http::set_live_cookie_header(&c);
                    true
                } else {
                    log("[B] Selenium 成功但未读到 baidu_cookies.json");
                    false
                }
            } else {
                log(&format!("[B] Selenium 未成功: {}", stdout.chars().take(200).collect::<String>()));
                false
            }
        }
        Err(e) => {
            log(&format!("[B] 无法启动 Python Selenium: {}", e));
            false
        }
    }
}

fn ask_user(
    state: &Arc<Mutex<AppState>>,
    code: &str,
    name: &str,
    detail: &str,
    sources: &[&str],
) -> UserAck {
    let kind = classify_kind(detail);
    let hint = if kind == "网络错误" {
        network_hint().to_string()
    } else {
        "未拿到不等于源站确认无数据。请打开源站，看页面上有没有对应内容、是否和本次结果一致。".into()
    };
    wait_user_ack(
        state,
        ErrorNotice {
            kind,
            code: code.to_string(),
            name: name.to_string(),
            detail: detail.to_string(),
            hint,
            links: source_verify_links(code, sources),
        },
    )
}

pub fn run_crawler(
    config: CrawlConfig,
    state: Arc<Mutex<AppState>>,
    stop: Arc<AtomicBool>,
    settings: &Settings,
    checklist: &mut CheckList,
) {
    let log = |s: &str| {
        if let Ok(mut st) = state.lock() {
            st.push_log(s.to_string());
        }
    };

    // 单连接（WAL + busy_timeout），整轮抓取复用；Db::open 内部已建表。
    let mut db = match db::Db::open(&config.db_path) {
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

    // 单 HTTP 客户端：代理端口死了（VPN 已关）就直连，避免整表网络错误
    let client = match crate::http::build_blocking_client(config.timeout) {
        Ok(c) => {
            log(&format!("HTTP 客户端就绪：{}", crate::http::last_proxy_desc()));
            c
        }
        Err(e) => {
            log(&format!("HTTP 客户端创建失败: {}", e));
            let ack = ask_user(
                &state,
                "",
                "HTTP客户端",
                &format!("网络错误: {e}"),
                &["baidu"],
            );
            if let Ok(mut st) = state.lock() {
                st.status = if ack == UserAck::Stop {
                    CrawlStatus::Stopped
                } else {
                    CrawlStatus::Error
                };
                st.status_msg = e.to_string();
            }
            return;
        }
    };
    // Cookie 预热（①）：先抓一次样本股落地页，让 Baidu 下发会话 cookie
    crate::http::warm_up(&client, "600000");
    // 对齐 Python：环境变量 / baidu_cookies.json 注入 Cookie
    if let Ok(env_c) = std::env::var("_BAIDU_COOKIE_DICT") {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&env_c) {
            if let Some(obj) = v.as_object() {
                let parts: Vec<String> = obj
                    .iter()
                    .filter_map(|(k, val)| val.as_str().map(|s| format!("{}={}", k, s)))
                    .collect();
                if !parts.is_empty() {
                    crate::http::set_live_cookie_header(&parts.join("; "));
                    log(&format!("已从环境变量加载 {} 个 Cookie", parts.len()));
                }
            }
        }
    } else {
        let cookie_path = format!("{}/baidu_cookies.json", crate::settings::workspace_dir());
        if let Some(c) = crate::http::load_cookies_from_json_file(&cookie_path) {
            crate::http::set_live_cookie_header(&c);
            log("已从 baidu_cookies.json 加载 Cookie");
        }
    }
    log("HTTP 客户端就绪 (cookie_store 已开启, 已预热 cookie)");

    let mut limiter = RateLimiter::new_ex(
        config.min_interval,
        config.max_per_minute,
        config.jitter,
        settings.pacing.interval_jitter_extra,
        settings.pacing.micro_break_every,
        settings.pacing.micro_break_min,
        settings.pacing.micro_break_max,
        config.rate_wait_cap,
        60.0,
    );

    let targets: Vec<StockRef> = match config.limit {
        Some(n) => config.codes.iter().take(n).cloned().collect(),
        None => config.codes.clone(),
    };
    let total = targets.len();

    {
        if let Ok(mut st) = state.lock() {
            st.total = total;
            st.status = CrawlStatus::Running;
            st.status_msg.clear();
        }
    }

    let t0 = Instant::now();
    let mut done = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut consecutive_403 = 0usize;
    let mut cooldown_count = 0usize;

    for (i, item) in targets.into_iter().enumerate() {
        if stop.load(Ordering::SeqCst) {
            if let Ok(mut st) = state.lock() {
                st.status = CrawlStatus::Stopped;
                st.status_msg = "用户中止".into();
            }
            break;
        }

        let code = item.code.clone();
        let name = item.name.clone();

        // 断点续跑：对齐 Python should_skip_code（ok 新鲜 / 空壳冷却）
        if config.resume {
            match db.should_skip(
                &code,
                "baidu",
                config.fresh_days,
                config.empty_limit,
                config.empty_cooldown_days,
            ) {
                Ok(Some(reason)) => {
                    skipped += 1;
                    if reason == "empty_cooldown" {
                        log(&format!("{} 空壳冷却跳过", code));
                    }
                    if let Ok(mut st) = state.lock() {
                        st.skipped = skipped;
                    }
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    log(&format!("DB 查询失败 {}: {}", code, e));
                }
            }
        }

        if let Ok(mut st) = state.lock() {
            st.current_code = code.clone();
            st.current_name = name.clone();
        }

        let single_start = Instant::now();
        let stock = StockRef { code: code.clone(), name: name.clone() };

        // 单只失败/不完整只记网页链接，不弹窗；整只循环里仍处理 403 冷却
        loop {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let res = crawl_one(&client, &mut limiter, &stock, &config, &state);
            match res {
                Ok(parsed) => {
                    let empty = parsed.scores.update_time.is_none();
                    if empty {
                        note_problem(
                            &state,
                            "未拿到数据",
                            &code,
                            &name,
                            "本次未拿到数据（不是确认源站无数据）",
                            &["baidu"],
                        );
                    }
                    if let Err(e) = db.save_snapshot(&config.trade_date, &stock, &parsed) {
                        note_problem(
                            &state,
                            "落库失败",
                            &code,
                            &name,
                            &format!("保存失败: {e}"),
                            &["baidu"],
                        );
                        failed += 1;
                        let _ = db.bump_crawl_stats(&code, false, "fail");
                    } else {
                        done += 1;
                        consecutive_403 = 0;
                        if settings.check.check_after_each_stock {
                            match db.completeness(&config.trade_date, &code) {
                                Ok(c) => {
                                    let missing = c.missing_vec();
                                    let n = missing.len();
                                    if n >= settings.check.missing_count_threshold {
                                        let detail = format!(
                                            "完整性缺 {} 项: {:?}",
                                            n, missing
                                        );
                                        log(&format!("⚠ [完整性] {} ({}) {}", code, name, detail));
                                        let prev = checklist.items.iter().find(|i| i.code == code);
                                        let tries = prev.map(|i| i.tries).unwrap_or(0);
                                        let last_try = prev.and_then(|i| i.last_try.clone());
                                        let status = if tries >= settings.rescrape.max_tries {
                                            CheckStatus::Exhausted
                                        } else {
                                            CheckStatus::Pending
                                        };
                                        checklist.upsert(CheckItem {
                                            code: code.clone(),
                                            name: name.clone(),
                                            missing: missing.clone(),
                                            missing_count: n,
                                            tries,
                                            last_try,
                                            status,
                                        });
                                        note_problem(&state, "数据不完整", &code, &name, &detail, &["baidu"]);
                                    }
                                }
                                Err(e) => {
                                    note_problem(
                                        &state,
                                        "数据不完整",
                                        &code,
                                        &name,
                                        &format!("完整性检查失败: {e}"),
                                        &["baidu"],
                                    );
                                }
                            }
                        }
                    }
                    break;
                }
                Err(e) => {
                    let msg = e.clone();
                    log(&format!("股票 {} 抓取失败: {}", code, e));
                    note_problem(&state, "抓取失败", &code, &name, &msg, &["baidu"]);
                    failed += 1;
                    let _ = db.bump_crawl_stats(&code, false, "fail");
                    if msg.contains("403") || msg.contains("Forbidden") || msg.contains("CHALLENGE") {
                        consecutive_403 += 1;
                        if consecutive_403 >= CONSEC_403_THRESHOLD {
                            cooldown_count += 1;
                            if cooldown_count > MAX_COOLDOWNS {
                                if let Ok(mut st) = state.lock() {
                                    st.status = CrawlStatus::Error;
                                    st.status_msg = format!(
                                        "连续冷却 {} 次仍遭 403，判定为持久封禁，停止。已完成 {} 跳过 {} 失败 {}",
                                        MAX_COOLDOWNS, done, skipped, failed
                                    );
                                }
                                stop.store(true, Ordering::SeqCst);
                                break;
                            }
                            if let Ok(mut st) = state.lock() {
                                st.status = CrawlStatus::Cooling;
                                st.status_msg = format!(
                                    "连续 {} 支遭 403，C→B Selenium headless 刷新 Cookie (第 {}/{})",
                                    consecutive_403, cooldown_count, MAX_COOLDOWNS
                                );
                                st.consecutive_403 = consecutive_403;
                            }
                            log("C 方案失效，切换 B 方案（调用 Python Selenium headless 刷新 Cookie）");
                            let refreshed = try_selenium_cookie_refresh(&log);
                            if refreshed {
                                log("[B] Cookie 已刷新，5 秒后继续");
                                std::thread::sleep(std::time::Duration::from_secs(5));
                            } else {
                                log(&format!("[B] 刷新失败，回退冷却 {:.0}s", COOLDOWN_SEC));
                                let mut remaining = COOLDOWN_SEC;
                                while remaining > 0.0 {
                                    if stop.load(Ordering::SeqCst) {
                                        break;
                                    }
                                    let step = remaining.min(1.0);
                                    std::thread::sleep(std::time::Duration::from_secs_f64(step));
                                    remaining -= step;
                                    if let Ok(mut st) = state.lock() {
                                        st.cooldown_remaining = remaining;
                                    }
                                }
                            }
                            consecutive_403 = 0;
                            if let Ok(mut st) = state.lock() {
                                st.cooldown_remaining = 0.0;
                                if !stop.load(Ordering::SeqCst) {
                                    st.status = CrawlStatus::Running;
                                }
                            }
                        }
                    } else {
                        consecutive_403 = 0;
                    }
                    break;
                }
            }
        }

        let elapsed = t0.elapsed().as_secs_f64();
        let processed = done + failed;
        let avg = if processed > 0 { elapsed / processed as f64 } else { 0.0 };
        let remain = total.saturating_sub(done + skipped + failed);
        let eta = if avg > 0.0 { avg * remain as f64 } else { 0.0 };
        if let Ok(mut st) = state.lock() {
            st.done = done;
            st.failed = failed;
            st.total_elapsed = elapsed;
            st.avg_per_stock = avg;
            st.eta_secs = eta;
            st.single_elapsed = single_start.elapsed().as_secs_f64();
        }

        if (i + 1) % 50 == 0 {
            log(&format!(
                "[{}/{}] 完成 {} 跳过 {} 失败 {} 用时 {:.0}s",
                i + 1, total, done, skipped, failed, elapsed
            ));
        }
    }

    if let Ok(mut st) = state.lock() {
        if st.status != CrawlStatus::Error && st.status != CrawlStatus::Stopped {
            st.status = CrawlStatus::Done;
            st.status_msg = format!("完成: 新增 {} 跳过 {} 失败 {}", done, skipped, failed);
        }
    }

    // 落库结束后，若开启了逐股校验，则把待核查清单写回文件。
    if settings.check.check_after_each_stock {
        checklist.trade_date = config.trade_date.clone();
        checklist.updated_at = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        match checklist.save(&settings.general.checklist_path) {
            Ok(()) => {
                let abnormal = checklist
                    .items
                    .iter()
                    .filter(|i| i.missing_count >= settings.check.missing_count_threshold)
                    .count();
                log(&format!(
                    "待核查清单已更新: 异常 {} 支 -> {}",
                    abnormal, settings.general.checklist_path
                ));
            }
            Err(e) => log(&format!("写入待核查清单失败: {}", e)),
        }
    }

    log(&format!(
        "===== 全市场抓取结束 新增 {} 跳过 {} 失败 {} =====",
        done, skipped, failed
    ));
}
