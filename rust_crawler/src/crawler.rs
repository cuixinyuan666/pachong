//! 爬虫引擎：遍历内置代码清单，逐支抓取并落库。
//! 含：断点续跑（跳过已存在的 trade_date+code）、连续 403 自动长冷却（自愈）、限流、统计与状态上报。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::VecDeque;

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
/// 同时抓取路数；遇 403 降到 4 再降到 1。
const MAX_WORKERS: usize = 16;
const RAMP_OK: usize = 24;

struct Pace {
    max_inflight: usize,
    inflight: usize,
    pause_until: Instant,
    proxy_pause: Instant,
    direct_pause: Instant,
    consec_403: usize,
    consec_ok: usize,
    cooldown_rounds: usize,
    refreshing: bool,
}

struct InflightSlot {
    pace: Arc<Mutex<Pace>>,
}

impl Drop for InflightSlot {
    fn drop(&mut self) {
        if let Ok(mut g) = self.pace.lock() {
            g.inflight = g.inflight.saturating_sub(1);
        }
    }
}

fn is_403(msg: &str) -> bool {
    msg.contains("403") || msg.contains("Forbidden") || msg.contains("CHALLENGE") || msg.contains("验证")
}

fn acquire_slot(
    pace: &Arc<Mutex<Pace>>,
    stop: &AtomicBool,
    use_proxy: bool,
    serial_interval: f64,
) -> Option<InflightSlot> {
    loop {
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        let now = Instant::now();
        let (need_wait, got) = {
            let mut g = match pace.lock() {
                Ok(g) => g,
                Err(_) => return None,
            };
            let path_pause = if use_proxy { g.proxy_pause } else { g.direct_pause };
            let until = if g.pause_until > path_pause {
                g.pause_until
            } else {
                path_pause
            };
            if now < until {
                (Some(until.saturating_duration_since(now)), false)
            } else if g.inflight < g.max_inflight {
                g.inflight += 1;
                let serial = g.max_inflight == 1;
                (if serial && serial_interval > 0.0 {
                    Some(Duration::from_secs_f64(serial_interval))
                } else {
                    None
                }, true)
            } else {
                (Some(Duration::from_millis(20)), false)
            }
        };
        if got {
            if let Some(d) = need_wait {
                thread::sleep(d);
            }
            return Some(InflightSlot {
                pace: pace.clone(),
            });
        }
        if let Some(d) = need_wait {
            thread::sleep(d.min(Duration::from_millis(200)));
        }
    }
}

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
    let db = match db::Db::open(&config.db_path) {
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

    // 出口：自动模式且 VPN/系统代理活着时，一半走代理一半直连。
    let exits = match crate::http::build_exit_clients(config.timeout) {
        Ok(e) => {
            log(&e.desc);
            e
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
    if let Some(c) = &exits.proxy {
        crate::http::warm_up(c, "600000");
    }
    crate::http::warm_up(&exits.direct, "600000");
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

    let targets: Vec<StockRef> = match config.limit {
        Some(n) => config.codes.iter().take(n).cloned().collect(),
        None => config.codes.clone(),
    };
    let (done, skipped, failed) = crawl_baidu_parallel(
        &config,
        &state,
        &stop,
        settings,
        checklist,
        db,
        exits,
        targets,
    );

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

fn crawl_baidu_parallel(
    config: &CrawlConfig,
    state: &Arc<Mutex<AppState>>,
    stop: &Arc<AtomicBool>,
    settings: &Settings,
    checklist: &mut CheckList,
    db: db::Db,
    exits: crate::http::ExitClients,
    targets: Vec<StockRef>,
) -> (usize, usize, usize) {
    let log = |s: &str| {
        if let Ok(mut st) = state.lock() {
            st.push_log(s.to_string());
        }
    };
    let total = targets.len();
    let db = Arc::new(Mutex::new(db));
    let cl = Arc::new(Mutex::new(std::mem::take(checklist)));

    let mut todo = VecDeque::new();
    let mut skipped = 0usize;
    {
        let dbg = match db.lock() {
            Ok(g) => g,
            Err(_) => {
                log("数据库锁失败");
                return (0, 0, total);
            }
        };
        for item in targets {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            if config.resume {
                match dbg.should_skip(
                    &item.code,
                    "baidu",
                    config.fresh_days,
                    config.empty_limit,
                    config.empty_cooldown_days,
                ) {
                    Ok(Some(reason)) => {
                        skipped += 1;
                        if reason == "empty_cooldown" {
                            log(&format!("{} 空壳冷却跳过", item.code));
                        }
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) => log(&format!("DB 查询失败 {}: {}", item.code, e)),
                }
            }
            todo.push_back(item);
        }
    }

    if let Ok(mut st) = state.lock() {
        st.total = total;
        st.skipped = skipped;
        st.status = CrawlStatus::Running;
        st.status_msg = format!("{}路并发 {}", MAX_WORKERS, exits.desc);
    }
    log(&format!(
        "开始并发抓取：{}路，待抓 {} 已跳过 {}。{}",
        MAX_WORKERS,
        todo.len(),
        skipped,
        exits.desc
    ));
    log("16路时不按「间隔/上限」排队；遇 403 降到 4 路再降到 1 路（1 路时才恢复间隔）。连续成功后再加回去。");

    let queue = Arc::new(Mutex::new(todo));
    let done_n = Arc::new(AtomicUsize::new(0));
    let fail_n = Arc::new(AtomicUsize::new(0));
    let t0 = Instant::now();
    let now0 = Instant::now();
    let pace = Arc::new(Mutex::new(Pace {
        max_inflight: MAX_WORKERS,
        inflight: 0,
        pause_until: now0,
        proxy_pause: now0,
        direct_pause: now0,
        consec_403: 0,
        consec_ok: 0,
        cooldown_rounds: 0,
        refreshing: false,
    }));

    let check_after = settings.check.check_after_each_stock;
    let miss_th = settings.check.missing_count_threshold;
    let max_tries = settings.rescrape.max_tries;
    let serial_interval = config.min_interval;

    let mut handles = Vec::new();
    for w in 0..MAX_WORKERS {
        let (client, use_proxy) = if exits.dual {
            if w % 2 == 0 {
                (
                    exits
                        .proxy
                        .clone()
                        .unwrap_or_else(|| exits.direct.clone()),
                    true,
                )
            } else {
                (exits.direct.clone(), false)
            }
        } else if let Some(p) = &exits.proxy {
            (p.clone(), true)
        } else {
            (exits.direct.clone(), false)
        };
        let tag = if use_proxy { "代理" } else { "直连" };
        let queue = queue.clone();
        let db = db.clone();
        let cl = cl.clone();
        let done_n = done_n.clone();
        let fail_n = fail_n.clone();
        let pace = pace.clone();
        let state = state.clone();
        let stop = stop.clone();
        let config = config.clone();
        handles.push(thread::spawn(move || {
            let mut limiter = RateLimiter::new_ex(
                0.0, 1_000_000, 0.0, 0.0, 0, 0.0, 0.0, Some(0.0), 1.0,
            );
            loop {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let Some(_slot) = acquire_slot(&pace, &stop, use_proxy, serial_interval) else {
                    break;
                };
                let stock = {
                    let mut q = match queue.lock() {
                        Ok(g) => g,
                        Err(_) => break,
                    };
                    q.pop_front()
                };
                let Some(stock) = stock else { break };
                let code = stock.code.clone();
                let name = stock.name.clone();
                if let Ok(mut st) = state.lock() {
                    st.current_code = code.clone();
                    st.current_name = name.clone();
                }
                let single_start = Instant::now();
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
                        let save_ok = {
                            let mut dbg = match db.lock() {
                                Ok(g) => g,
                                Err(_) => {
                                    fail_n.fetch_add(1, Ordering::SeqCst);
                                    continue;
                                }
                            };
                            if let Err(e) = dbg.save_snapshot(&config.trade_date, &stock, &parsed) {
                                note_problem(
                                    &state,
                                    "落库失败",
                                    &code,
                                    &name,
                                    &format!("保存失败: {e}"),
                                    &["baidu"],
                                );
                                let _ = dbg.bump_crawl_stats(&code, false, "fail");
                                false
                            } else {
                                true
                            }
                        };
                        if !save_ok {
                            fail_n.fetch_add(1, Ordering::SeqCst);
                        } else {
                            done_n.fetch_add(1, Ordering::SeqCst);
                            if let Ok(mut p) = pace.lock() {
                                p.consec_403 = 0;
                                p.consec_ok += 1;
                                if p.consec_ok >= RAMP_OK && p.max_inflight < MAX_WORKERS {
                                    let next = match p.max_inflight {
                                        1 => 4,
                                        4 => 8,
                                        _ => MAX_WORKERS,
                                    };
                                    p.max_inflight = next;
                                    p.consec_ok = 0;
                                    if let Ok(mut st) = state.lock() {
                                        st.push_log(format!("连续成功，恢复到 {next} 路并发"));
                                        st.status_msg = format!("{next}路并发");
                                        if st.status == CrawlStatus::Cooling {
                                            st.status = CrawlStatus::Running;
                                        }
                                    }
                                }
                            }
                            if check_after {
                                if let Ok(dbg) = db.lock() {
                                    match dbg.completeness(&config.trade_date, &code) {
                                        Ok(c) => {
                                            let missing = c.missing_vec();
                                            let n = missing.len();
                                            if n >= miss_th {
                                                let detail =
                                                    format!("完整性缺 {} 项: {:?}", n, missing);
                                                if let Ok(mut st) = state.lock() {
                                                    st.push_log(format!(
                                                        "⚠ [完整性] {} ({}) {}",
                                                        code, name, detail
                                                    ));
                                                }
                                                if let Ok(mut list) = cl.lock() {
                                                    let prev = list.items.iter().find(|i| i.code == code);
                                                    let tries = prev.map(|i| i.tries).unwrap_or(0);
                                                    let last_try = prev.and_then(|i| i.last_try.clone());
                                                    let status = if tries >= max_tries {
                                                        CheckStatus::Exhausted
                                                    } else {
                                                        CheckStatus::Pending
                                                    };
                                                    list.upsert(CheckItem {
                                                        code: code.clone(),
                                                        name: name.clone(),
                                                        missing: missing.clone(),
                                                        missing_count: n,
                                                        tries,
                                                        last_try,
                                                        status,
                                                    });
                                                }
                                                note_problem(
                                                    &state,
                                                    "数据不完整",
                                                    &code,
                                                    &name,
                                                    &detail,
                                                    &["baidu"],
                                                );
                                            }
                                        }
                                        Err(e) => note_problem(
                                            &state,
                                            "数据不完整",
                                            &code,
                                            &name,
                                            &format!("完整性检查失败: {e}"),
                                            &["baidu"],
                                        ),
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let msg = e.clone();
                        if let Ok(mut st) = state.lock() {
                            st.push_log(format!("[{tag}] 股票 {code} 抓取失败: {e}"));
                        }
                        note_problem(&state, "抓取失败", &code, &name, &msg, &["baidu"]);
                        fail_n.fetch_add(1, Ordering::SeqCst);
                        if let Ok(dbg) = db.lock() {
                            let _ = dbg.bump_crawl_stats(&code, false, "fail");
                        }
                        if is_403(&msg) {
                            handle_403(&pace, &state, stop.as_ref(), use_proxy);
                        } else if let Ok(mut p) = pace.lock() {
                            p.consec_403 = 0;
                        }
                    }
                }
                let elapsed = t0.elapsed().as_secs_f64();
                let done = done_n.load(Ordering::SeqCst);
                let failed = fail_n.load(Ordering::SeqCst);
                let processed = done + failed;
                let avg = if processed > 0 {
                    elapsed / processed as f64
                } else {
                    0.0
                };
                let remain = queue.lock().map(|q| q.len()).unwrap_or(0);
                if let Ok(mut st) = state.lock() {
                    st.done = done;
                    st.failed = failed;
                    st.total_elapsed = elapsed;
                    st.avg_per_stock = avg;
                    st.eta_secs = if avg > 0.0 { avg * remain as f64 } else { 0.0 };
                    st.single_elapsed = single_start.elapsed().as_secs_f64();
                    if let Ok(p) = pace.lock() {
                        st.consecutive_403 = p.consec_403;
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }
    crate::http::set_direct_fallback(true);
    if let Ok(g) = cl.lock() {
        *checklist = g.clone();
    }
    (
        done_n.load(Ordering::SeqCst),
        skipped,
        fail_n.load(Ordering::SeqCst),
    )
}

fn handle_403(
    pace: &Arc<Mutex<Pace>>,
    state: &Arc<Mutex<AppState>>,
    stop: &AtomicBool,
    use_proxy: bool,
) {
    let log = |s: &str| {
        if let Ok(mut st) = state.lock() {
            st.push_log(s.to_string());
        }
    };
    let mut do_refresh = false;
    let mut long_cool = false;
    {
        let Ok(mut p) = pace.lock() else { return };
        p.consec_ok = 0;
        p.consec_403 += 1;
        let old = p.max_inflight;
        if p.max_inflight > 4 {
            p.max_inflight = 4;
        } else if p.max_inflight > 1 {
            p.max_inflight = 1;
        }
        let new_n = p.max_inflight;
        let pause = if new_n == 1 { 20 } else { 8 };
        p.pause_until = Instant::now() + Duration::from_secs(pause);
        if use_proxy {
            p.proxy_pause = Instant::now() + Duration::from_secs(30);
        } else {
            p.direct_pause = Instant::now() + Duration::from_secs(30);
        }
        if new_n < old {
            log(&format!("遇 403，并发从 {old} 路降到 {new_n} 路，暂停 {pause}s"));
        }
        if new_n == 1 && !p.refreshing && p.consec_403 >= 2 {
            p.refreshing = true;
            p.cooldown_rounds += 1;
            if p.cooldown_rounds > MAX_COOLDOWNS {
                if let Ok(mut st) = state.lock() {
                    st.status = CrawlStatus::Error;
                    st.status_msg = format!(
                        "连续冷却 {} 次仍遭 403，判定为持久封禁",
                        MAX_COOLDOWNS
                    );
                }
                stop.store(true, Ordering::SeqCst);
                p.refreshing = false;
                return;
            }
            do_refresh = true;
        }
        if new_n == 1 && p.consec_403 >= CONSEC_403_THRESHOLD {
            long_cool = true;
        }
        if let Ok(mut st) = state.lock() {
            st.status = CrawlStatus::Cooling;
            st.status_msg = format!("{new_n}路（403降速）");
            st.consecutive_403 = p.consec_403;
        }
    }
    if do_refresh {
        log("1 路仍 403，刷新 Cookie（Selenium）");
        let refreshed = try_selenium_cookie_refresh(&log);
        if refreshed {
            log("[B] Cookie 已刷新，5 秒后继续");
            thread::sleep(Duration::from_secs(5));
        } else if long_cool {
            log(&format!("[B] 刷新失败，冷却 {:.0}s", COOLDOWN_SEC));
            let mut remaining = COOLDOWN_SEC;
            while remaining > 0.0 {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_secs(1));
                remaining -= 1.0;
                if let Ok(mut st) = state.lock() {
                    st.cooldown_remaining = remaining;
                }
            }
        } else {
            thread::sleep(Duration::from_secs(20));
        }
        if let Ok(mut p) = pace.lock() {
            p.refreshing = false;
            p.consec_403 = 0;
        }
        if !stop.load(Ordering::SeqCst) {
            if let Ok(mut st) = state.lock() {
                st.cooldown_remaining = 0.0;
                st.status = CrawlStatus::Running;
                st.status_msg = "1路（403后）".into();
            }
        }
    }
}
