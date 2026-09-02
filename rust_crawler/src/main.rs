#![windows_subsystem = "windows"]

//! 百度财经 AI 技术分析 · 全市场爬虫（Rust 重写 + egui 进度面板）
//!
//! 复用同一份 market_data.db（与 Python 版 schema 完全一致），内置 a_stocks.json
//! 作为 A 股代码源，UI 实时显示进度/ETA/当前股票/端点/限流/403 冷却/统计/日志/耗时。

mod crawler;
mod db;
mod http;
mod models;
mod state;
mod settings;
mod checklist;
mod em_crawler;

use std::os::windows::process::CommandExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::{Utc, Duration as ChronoDuration, Datelike};
use eframe::{App, Frame, NativeOptions};
use eframe::egui;

use crate::crawler::{CrawlConfig, run_crawler};
use crate::em_crawler::run_em_crawler;
use crate::models::StockRef;
use crate::state::{AppState, CrawlStatus};
use crate::db::Db;
use crate::settings::Settings;
use crate::checklist::{CheckList, CheckItem, CheckStatus};

/// 内置 A 股代码清单（5532 支，编译期嵌入）。
const EMBEDDED_CODES: &str = include_str!("../assets/a_stocks.json");

/// 2026 年 A 股休市日（沪深北交易所官方安排，与 Python 版一致）。
const HOLIDAYS_2026: &[&str] = &[
    "2026-01-01", "2026-01-02", "2026-01-03",
    "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
    "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23",
    "2026-04-04", "2026-04-05", "2026-04-06",
    "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
    "2026-06-19", "2026-06-20", "2026-06-21",
    "2026-09-25", "2026-09-26", "2026-09-27",
    "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
    "2026-10-05", "2026-10-06", "2026-10-07",
];

/// 商业化金融工具风 design tokens（对齐 Anthropic frontend-design：单一品牌色、忌紫/奶油默认风）。
const INK: egui::Color32 = egui::Color32::from_rgb(11, 31, 42);
const TXT: egui::Color32 = egui::Color32::from_rgb(15, 36, 48);
const DIM: egui::Color32 = egui::Color32::from_rgb(91, 107, 118);
const PAPER: egui::Color32 = egui::Color32::from_rgb(243, 246, 248);
const SURFACE: egui::Color32 = egui::Color32::from_rgb(255, 255, 255);
const LINE: egui::Color32 = egui::Color32::from_rgb(213, 222, 229);
const ACCENT: egui::Color32 = egui::Color32::from_rgb(13, 148, 136);
const ACCENT_STRONG: egui::Color32 = egui::Color32::from_rgb(15, 118, 110);
const WARN: egui::Color32 = egui::Color32::from_rgb(194, 120, 3);
const DANGER: egui::Color32 = egui::Color32::from_rgb(185, 28, 28);
const OK: egui::Color32 = egui::Color32::from_rgb(4, 120, 87);
const MUTED: egui::Color32 = egui::Color32::from_rgb(100, 116, 128);

struct CrawlerApp {
    state: Arc<Mutex<AppState>>,
    stop_flag: Arc<AtomicBool>,
    crawler_thread: Option<thread::JoinHandle<()>>,
    trade_date_input: String,
    db_path_input: String,
    min_interval_input: String,
    max_per_minute_input: String,
    force_input: bool,
    /// true=接着爬（跳过已存在）；false=从头爬（全覆盖重爬）。
    resume_mode: bool,
    /// 抓取源：0=百度 1=东财 2=一键(百度→东财)
    source_mode: usize,
    empty_limit_input: String,
    empty_cooldown_input: String,
    rate_wait_cap_input: String,
    /// 当前交易日已在库中抓取的数量（用于 UI 提示「接着/从头」）。
    existing_count: usize,
    /// 代码清单总数（5532）。
    total_codes: usize,
    /// 是否检测到 Python 后台爬虫正在运行（进程级检测命令行含 baidu_finance_ai_crawler）。
    python_running: bool,
    /// GUI 启动时清理僵尸进程的结果描述（如实显示「已清除 / 无法清除」）。
    zombie_msg: String,
    /// 「检查完整性」后台线程句柄（用于禁用按钮直到完成）。
    check_thread: Option<thread::JoinHandle<()>>,
    /// 「回填重抓」后台线程句柄（用于禁用按钮直到完成）。
    rescrape_thread: Option<thread::JoinHandle<()>>,
    /// 首帧强制最大化（部分 Windows/egui 组合下仅 with_maximized 不够）。
    did_maximize: bool,
}

impl Default for CrawlerApp {
    fn default() -> Self {
        let settings = Settings::load();
        let db_path = if !settings.general.db_path.is_empty() {
            settings.general.db_path.clone()
        } else {
            format!("{}/market_data.db", crate::settings::workspace_dir())
        };
        // 默认交易日：优先续上「库里已有最多数据的那天」，否则退回最近交易日。
        // 这样双击 exe 会自动接着爬上次没爬完的日期，而不是默认成今天(可能无数据/非交易日)。
        let default_date = default_trade_date(&db_path);
        let total_codes = load_codes().len();
        // 启动即检测：该日期是否已抓取过多少、Python 是否在跑。
        let existing_count = count_existing(&db_path, &default_date);
        let python_running = detect_python_running();
        Self {
            state: Arc::new(Mutex::new(AppState::default())),
            stop_flag: Arc::new(AtomicBool::new(false)),
            crawler_thread: None,
            trade_date_input: default_date,
            db_path_input: db_path,
            min_interval_input: format!("{}", settings.rate.min_interval),
            max_per_minute_input: format!("{}", settings.rate.max_per_minute),
            force_input: false,
            resume_mode: true,
            source_mode: 0,
            empty_limit_input: "3".into(),
            empty_cooldown_input: "7".into(),
            rate_wait_cap_input: "15".into(),
            existing_count,
            total_codes,
            python_running,
            zombie_msg: kill_zombie_rust_processes(),
            check_thread: None,
            rescrape_thread: None,
            did_maximize: false,
        }
    }
}

fn load_codes() -> Vec<StockRef> {
    let v: serde_json::Value = match serde_json::from_str(EMBEDDED_CODES) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let arr = v
        .get("stocks")
        .and_then(|s| s.as_array())
        .cloned()
        .unwrap_or_default();
    arr.iter()
        .filter_map(|s| {
            let code = s.get("code")?.as_str()?.to_string();
            let name = s.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
            Some(StockRef { code, name })
        })
        .collect()
}

fn is_trading_day(date_str: &str) -> bool {
    if let Ok(d) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        let wd = d.weekday();
        if wd == chrono::Weekday::Sat || wd == chrono::Weekday::Sun {
            return false;
        }
        if HOLIDAYS_2026.iter().any(|h| *h == date_str) {
            return false;
        }
        return true;
    }
    false
}

/// 续爬默认交易日：库里有数据则取数据最多的那天；否则取最近交易日。
/// 这样双击 exe 会自动续上「上次没爬完的那天」，而不是默认成今天(可能无数据/非交易日)。
fn default_trade_date(db_path: &str) -> String {
    let today = (Utc::now() + ChronoDuration::hours(8)).date_naive();
    if let Ok(db) = crate::db::Db::open(db_path) {
        if let Some(d) = db.resume_candidate_date() {
            return d;
        }
    }
    last_trading_day(today)
}

/// 从 from 日向前找最近一个交易日（跳过周末与 2026 休市日）。
fn last_trading_day(from: chrono::NaiveDate) -> String {
    let mut d = from;
    for _ in 0..30 {
        let s = d.format("%Y-%m-%d").to_string();
        if is_trading_day(&s) {
            return s;
        }
        d = d - ChronoDuration::days(1);
    }
    from.format("%Y-%m-%d").to_string()
}

/// 检测 Python 后台爬虫是否正在运行：进程级检测（与 kill 同一套匹配），
/// 用 Get-CimInstance 查命令行含 baidu_finance_ai_crawler 的 python.exe。
/// 相比「看日志 mtime」更可靠：Python 进入 403 冷却(10 分钟不写日志)时也能正确识别；
/// 且杀进程后复检能正确判「已停」（日志 mtime 不会因被杀而改变）。
fn detect_python_running() -> bool {
    let ps = "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" \
        | Where-Object { $_.CommandLine -like '*baidu_finance_ai_crawler*' } \
        | Select-Object -First 1 ProcessId";
    match std::process::Command::new("powershell")
        .creation_flags(0x08000000) // CREATE_NO_WINDOW：彻底隐藏弹出的控制台窗口
        .args(["-NoProfile", "-Command", ps])
        .output()
    {
        Ok(o) => {
            let out = String::from_utf8_lossy(&o.stdout);
            !out.trim().is_empty()
        }
        Err(_) => false,
    }
}

/// 停止运行 baidu_finance_ai_crawler.py 的 python 进程（断开 Python 爬虫，禁止并存）。
/// 返回可读的结果描述。
fn kill_python_crawler() -> String {
    // 先列出匹配的进程 PID，再逐个强制结束，最后回显 PID。
    let ps = "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" \
        | Where-Object { $_.CommandLine -like '*baidu_finance_ai_crawler*' } \
        | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; $_.ProcessId }";
    match std::process::Command::new("powershell")
        .creation_flags(0x08000000)
        .args(["-NoProfile", "-Command", ps])
        .output()
    {
        Ok(o) => {
            let out = String::from_utf8_lossy(&o.stdout);
            if out.trim().is_empty() {
                "未发现 Python 爬虫进程".into()
            } else {
                format!("已停止 Python 爬虫: {}", out.trim())
            }
        }
        Err(e) => format!("停止失败: {}", e),
    }
}

/// 对齐 Web：用同目录 Python 脚本抓东财/百度（限流参数可自定义）。
fn spawn_python_crawler(
    script_name: &str,
    db_path: &str,
    min_interval: &str,
    max_per_minute: &str,
    state: Arc<Mutex<AppState>>,
) {
    let py = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
    let script = format!("{}/{}", crate::settings::workspace_dir(), script_name);
    let mi = min_interval.parse::<f64>().unwrap_or(1.0);
    let mpm = max_per_minute.parse::<usize>().unwrap_or(40);
    if let Ok(mut g) = state.lock() {
        g.push_log(format!(
            "启动 Python {} --market --db {} --min-interval {} --max-per-minute {}",
            script_name, db_path, mi, mpm
        ));
        g.status = CrawlStatus::Running;
        g.status_msg = format!("Python {}", script_name);
    }
    let out = std::process::Command::new(&py)
        .creation_flags(0x08000000)
        .args([
            "-u",
            &script,
            "--market",
            "--progress-log",
            "--db",
            db_path,
            "--min-interval",
            &format!("{}", mi),
            "--max-per-minute",
            &format!("{}", mpm),
        ])
        .current_dir(crate::settings::workspace_dir())
        .output();
    match out {
        Ok(o) => {
            let code = o.status.code().unwrap_or(-1);
            let tail = String::from_utf8_lossy(&o.stdout);
            let last = tail.lines().rev().take(3).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join(" | ");
            if let Ok(mut g) = state.lock() {
                g.push_log(format!("Python {} 退出码 {} {}", script_name, code, last));
                g.status = if code == 0 { CrawlStatus::Done } else { CrawlStatus::Error };
                g.status_msg = format!("{} 退出 {}", script_name, code);
            }
        }
        Err(e) => {
            if let Ok(mut g) = state.lock() {
                g.push_log(format!("启动 Python 失败: {}", e));
                g.status = CrawlStatus::Error;
                g.status_msg = e.to_string();
            }
        }
    }
}

/// 启动 GUI 时清理本程序遗留的「僵尸」进程：同名 exe 但工作集极小（<5MB，典型 32K）且非自身的进程。
/// 这些通常是早前后台运行被异常中断后未回收的孤儿进程。
/// 区分「已清除」与「无法清除(Windows 限制)」如实回报（已退出的僵尸进程 Windows 会拒绝访问，需重启系统清除）。
fn kill_zombie_rust_processes() -> String {
    let self_pid = std::process::id();
    // 1) 枚举候选：非自身 且 工作集 <5MB（真正的僵尸只有 ~32K）。
    let enum_ps = format!(
        "$ids = @(Get-CimInstance Win32_Process -Filter \"Name='baidu_finance_rust.exe'\" \
         | Where-Object {{ $_.ProcessId -ne {self_pid} -and ($_.WorkingSetSize -lt 5242880) }} \
         | ForEach-Object {{ $_.ProcessId }}); $ids -join ','"
    );
    let candidates: Vec<u32> = match std::process::Command::new("powershell")
        .creation_flags(0x08000000)
        .args(["-NoProfile", "-Command", &enum_ps])
        .output()
    {
        Ok(o) => String::from_utf8_lossy(&o.stdout)
            .split(',')
            .filter_map(|x| x.trim().parse::<u32>().ok())
            .collect(),
        Err(_) => Vec::new(),
    };
    if candidates.is_empty() {
        return "未检测到僵尸进程".to_string();
    }
    // 2) 逐个尝试强杀（taskkill /F /T 最彻底），并复检是否仍在。
    let mut killed: Vec<u32> = Vec::new();
    let mut failed: Vec<u32> = Vec::new();
    for pid in candidates {
        let _ = std::process::Command::new("taskkill")
            .creation_flags(0x08000000)
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .output();
        let still = std::process::Command::new("powershell")
            .creation_flags(0x08000000)
            .args([
                "-NoProfile",
                "-Command",
                &format!(
                    "if(Get-CimInstance Win32_Process -Filter \"ProcessId={}\"){{1}}else{{0}}",
                    pid
                ),
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).contains('1'))
            .unwrap_or(false);
        if still {
            failed.push(pid);
        } else {
            killed.push(pid);
        }
    }
    let mut msg = String::new();
    if !killed.is_empty() {
        msg.push_str(&format!(
            "已清除僵尸: {}",
            killed
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !failed.is_empty() {
        if !msg.is_empty() {
            msg.push_str("; ");
        }
        msg.push_str(&format!(
            "无法清除(Windows 限制): {}",
            failed
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if msg.is_empty() {
        msg = "未发现僵尸进程".to_string();
    }
    msg
}

/// 该交易日是否已在库中抓取过股票。
fn count_existing(db_path: &str, trade_date: &str) -> usize {
    match crate::db::Db::open(db_path) {
        Ok(db) => db.count_for_date(trade_date).unwrap_or(0),
        Err(_) => 0,
    }
}

/// 目标数据库是否就是 Python 爬虫默认写入的 market_data.db（同目录同名）。
/// 仅当目标是这份共享库时，才需要在启动时断开 Python。
fn is_shared_db(db_path: &str) -> bool {
    let p = std::path::Path::new(db_path);
    p.file_name()
        .map(|n| n == "market_data.db")
        .unwrap_or(false)
        && p
            .parent()
            .map(|d| {
                std::env::current_exe()
                    .ok()
                    .and_then(|e| e.parent().map(|ed| ed == d))
                    .unwrap_or(false)
            })
            .unwrap_or(false)
}

/// 分区面板：白底细描边，左侧 3px 品牌色条（金融控制台 vernacula）。
fn panel(ui: &mut egui::Ui, title: &str, body: impl FnOnce(&mut egui::Ui)) {
    let frame = egui::Frame::none()
        .fill(SURFACE)
        .stroke(egui::Stroke::new(1.0, LINE))
        .rounding(egui::Rounding::same(4.0))
        .inner_margin(egui::Margin::symmetric(14.0, 12.0));
    frame.show(ui, |ui| {
        ui.horizontal(|ui| {
            let (rect, _) = ui.allocate_exact_size(egui::vec2(3.0, 14.0), egui::Sense::hover());
            ui.painter().rect_filled(rect, 1.0, ACCENT);
            ui.add_space(8.0);
            ui.colored_label(INK, egui::RichText::new(title).size(13.0).strong());
        });
        ui.add_space(8.0);
        body(ui);
    });
}

/// KPI：小标签 + 大号数字。
fn kpi(ui: &mut egui::Ui, label: &str, value: usize, color: egui::Color32) {
    let frame = egui::Frame::none()
        .fill(PAPER)
        .rounding(egui::Rounding::same(4.0))
        .inner_margin(egui::Margin::symmetric(12.0, 10.0));
    frame.show(ui, |ui| {
        ui.colored_label(DIM, egui::RichText::new(label).size(11.0));
        ui.add_space(2.0);
        ui.colored_label(color, egui::RichText::new(format!("{}", value)).size(24.0).strong());
    });
}

/// 状态徽章。
fn status_badge(ui: &mut egui::Ui, status: &CrawlStatus) {
    let (text, color) = match status {
        CrawlStatus::Idle => ("空闲", MUTED),
        CrawlStatus::Running => ("运行中", ACCENT),
        CrawlStatus::Cooling => ("冷却中", WARN),
        CrawlStatus::Done => ("已完成", OK),
        CrawlStatus::Stopped => ("已停止", MUTED),
        CrawlStatus::Error => ("错误", DANGER),
    };
    let frame = egui::Frame::none()
        .fill(egui::Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 28))
        .stroke(egui::Stroke::new(1.0, color))
        .rounding(egui::Rounding::same(3.0))
        .inner_margin(egui::Margin::symmetric(10.0, 4.0));
    frame.show(ui, |ui| {
        ui.colored_label(color, egui::RichText::new(text).size(12.0).strong());
    });
}

/// 套用商业化浅色主题：冷灰纸面 + 青绿强调，无彩虹主题切换。
fn apply_theme(ctx: &egui::Context) {
    let mut v = egui::Visuals::light();
    v.dark_mode = false;
    v.window_fill = PAPER;
    v.panel_fill = PAPER;
    v.faint_bg_color = PAPER;
    v.extreme_bg_color = egui::Color32::from_rgb(232, 237, 241);
    v.override_text_color = Some(TXT);
    v.widgets.noninteractive.bg_fill = SURFACE;
    v.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, LINE);
    v.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, TXT);
    v.widgets.inactive.bg_fill = SURFACE;
    v.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, LINE);
    v.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, TXT);
    v.widgets.hovered.bg_fill = egui::Color32::from_rgb(232, 245, 243);
    v.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, ACCENT);
    v.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, INK);
    v.widgets.active.bg_fill = ACCENT_STRONG;
    v.widgets.active.fg_stroke = egui::Stroke::new(1.0, egui::Color32::WHITE);
    v.widgets.open.bg_fill = SURFACE;
    v.selection.bg_fill = egui::Color32::from_rgb(204, 235, 230);
    v.window_rounding = egui::Rounding::same(4.0);
    v.menu_rounding = egui::Rounding::same(4.0);
    v.window_stroke = egui::Stroke::new(1.0, LINE);
    ctx.set_visuals(v);

    let mut style = (*ctx.style()).clone();
    style.spacing.item_spacing = egui::vec2(8.0, 8.0);
    style.spacing.window_margin = egui::Margin::same(0.0);
    style.spacing.button_padding = egui::vec2(14.0, 7.0);
    ctx.set_style(style);
}

impl CrawlerApp {
    fn try_start_crawl(&mut self) {
        let finished = self
            .crawler_thread
            .as_ref()
            .map(|h| h.is_finished())
            .unwrap_or(true);
        if !finished {
            return;
        }
        let trade_date = self.trade_date_input.clone();
        if !self.force_input && !is_trading_day(&trade_date) {
            if let Ok(mut g) = self.state.lock() {
                g.status = CrawlStatus::Error;
                g.status_msg = format!(
                    "{} 非交易日，已阻止（勾选\"强制\"可忽略）",
                    trade_date
                );
            }
            return;
        }
        let codes = load_codes();
        if codes.is_empty() {
            if let Ok(mut g) = self.state.lock() {
                g.status = CrawlStatus::Error;
                g.status_msg = "代码清单为空".into();
            }
            return;
        }
        if detect_python_running() && is_shared_db(&self.db_path_input) {
            let msg = kill_python_crawler();
            thread::sleep(Duration::from_secs(2));
            if detect_python_running() {
                if let Ok(mut g) = self.state.lock() {
                    g.status = CrawlStatus::Error;
                    g.status_msg = format!(
                        "{} 但 Python 仍在运行，已阻止启动（禁止并存）",
                        msg
                    );
                }
                return;
            }
            self.existing_count = count_existing(&self.db_path_input, &trade_date);
            self.python_running = false;
            if let Ok(mut g) = self.state.lock() {
                g.push_log(format!("{}，本程序接管剩余抓取", msg));
            }
        }
        let mi: f64 = self.min_interval_input.parse().unwrap_or(1.0);
        let mp: usize = self.max_per_minute_input.parse().unwrap_or(40);
        let empty_limit: i64 = self.empty_limit_input.parse().unwrap_or(3);
        let empty_cd: i64 = self.empty_cooldown_input.parse().unwrap_or(7);
        let rate_cap: Option<f64> = self
            .rate_wait_cap_input
            .parse::<f64>()
            .ok()
            .map(|v| if v < 0.0 { 0.0 } else { v });
        let db_path = self.db_path_input.clone();
        let state = self.state.clone();
        let stop = self.stop_flag.clone();
        stop.store(false, Ordering::SeqCst);
        let settings = crate::settings::Settings::load();
        let mut checklist =
            crate::checklist::CheckList::load_or_default(&settings.general.checklist_path);
        let source_mode = self.source_mode;
        let resume = self.resume_mode;
        let config = CrawlConfig {
            db_path: db_path.clone(),
            trade_date: trade_date.clone(),
            codes: codes.clone(),
            min_interval: mi,
            max_per_minute: mp,
            jitter: 0.6,
            max_retries: 3,
            timeout: 15,
            limit: None,
            resume,
            fresh_days: 2,
            empty_limit,
            empty_cooldown_days: empty_cd,
            rate_wait_cap: rate_cap,
        };
        let handle = thread::spawn(move || match source_mode {
            1 => run_em_crawler(config, state, stop, &settings),
            2 => {
                run_crawler(
                    config.clone(),
                    state.clone(),
                    stop.clone(),
                    &settings,
                    &mut checklist,
                );
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let mut em_cfg = config;
                em_cfg.min_interval = 0.1;
                em_cfg.max_per_minute = 200;
                em_cfg.jitter = 0.0;
                run_em_crawler(em_cfg, state, stop, &settings);
            }
            _ => run_crawler(config, state, stop, &settings, &mut checklist),
        });
        self.crawler_thread = Some(handle);
    }
}

impl App for CrawlerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        if !self.did_maximize {
            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(true));
            self.did_maximize = true;
        }
        apply_theme(ctx);

        let (
            total,
            done,
            skipped,
            failed,
            cur_code,
            cur_name,
            cur_ep,
            status,
            status_msg,
            c403,
            cd_rem,
            single_el,
            total_el,
            eta,
            avg,
            logs,
        ) = {
            let g = self.state.lock().unwrap();
            (
                g.total,
                g.done,
                g.skipped,
                g.failed,
                g.current_code.clone(),
                g.current_name.clone(),
                g.current_endpoint.clone(),
                g.status.clone(),
                g.status_msg.clone(),
                g.consecutive_403,
                g.cooldown_remaining,
                g.single_elapsed,
                g.total_elapsed,
                g.eta_secs,
                g.avg_per_stock,
                g.logs.clone(),
            )
        };

        let processed = done + skipped + failed;
        let frac = if total > 0 {
            processed as f32 / total as f32
        } else {
            0.0
        };
        let pct = frac * 100.0;
        let remain = self.total_codes.saturating_sub(self.existing_count);
        let busy_check = self
            .check_thread
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false);
        let busy_res = self
            .rescrape_thread
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false);
        let is_busy = matches!(status, CrawlStatus::Running | CrawlStatus::Cooling);

        // —— 顶栏：产品名 + 状态 + 主操作 ——
        egui::TopBottomPanel::top("top_bar")
            .exact_height(56.0)
            .frame(
                egui::Frame::none()
                    .fill(INK)
                    .inner_margin(egui::Margin::symmetric(20.0, 10.0)),
            )
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.vertical(|ui| {
                        ui.colored_label(
                            egui::Color32::WHITE,
                            egui::RichText::new("MarketPulse").size(18.0).strong(),
                        );
                        ui.colored_label(
                            egui::Color32::from_rgb(148, 173, 184),
                            egui::RichText::new("A股全市场数据采集控制台").size(11.0),
                        );
                    });
                    ui.add_space(16.0);
                    status_badge(ui, &status);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new("停止")
                                        .color(egui::Color32::WHITE)
                                        .strong(),
                                )
                                .fill(egui::Color32::from_rgb(120, 40, 40)),
                            )
                            .clicked()
                        {
                            self.stop_flag.store(true, Ordering::SeqCst);
                        }
                        ui.add_space(6.0);
                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new(if is_busy {
                                        "抓取进行中…"
                                    } else {
                                        "开始抓取"
                                    })
                                    .color(egui::Color32::WHITE)
                                    .strong(),
                                )
                                .fill(ACCENT),
                            )
                            .clicked()
                        {
                            self.try_start_crawl();
                        }
                    });
                });
            });

        // —— 底栏：会话日志 ——
        egui::TopBottomPanel::bottom("log_bar")
            .resizable(true)
            .default_height(200.0)
            .min_height(120.0)
            .frame(
                egui::Frame::none()
                    .fill(SURFACE)
                    .stroke(egui::Stroke::new(1.0, LINE))
                    .inner_margin(egui::Margin::symmetric(16.0, 10.0)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.colored_label(INK, egui::RichText::new("会话日志").size(12.0).strong());
                    ui.colored_label(DIM, egui::RichText::new("实时输出 · 自动贴底").size(11.0));
                });
                ui.add_space(4.0);
                egui::Frame::none()
                    .fill(egui::Color32::from_rgb(248, 250, 251))
                    .stroke(egui::Stroke::new(1.0, LINE))
                    .rounding(egui::Rounding::same(3.0))
                    .inner_margin(egui::Margin::same(8.0))
                    .show(ui, |ui| {
                        egui::ScrollArea::vertical()
                            .stick_to_bottom(true)
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                ui.style_mut().override_text_style = Some(egui::TextStyle::Monospace);
                                for line in &logs {
                                    ui.colored_label(TXT, line.as_str());
                                }
                            });
                    });
            });

        // —— 左侧：作业配置 ——
        egui::SidePanel::left("cfg_panel")
            .resizable(true)
            .default_width(340.0)
            .width_range(280.0..=420.0)
            .frame(
                egui::Frame::none()
                    .fill(PAPER)
                    .inner_margin(egui::Margin::symmetric(14.0, 12.0)),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        panel(ui, "作业参数", |ui| {
                            ui.horizontal(|ui| {
                                ui.colored_label(DIM, "交易日");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.trade_date_input)
                                        .desired_width(120.0),
                                );
                            });
                            ui.horizontal(|ui| {
                                ui.colored_label(DIM, "数据库");
                            });
                            ui.add(
                                egui::TextEdit::singleline(&mut self.db_path_input)
                                    .desired_width(f32::INFINITY),
                            );
                            ui.horizontal_wrapped(|ui| {
                                ui.colored_label(DIM, "间隔(s)");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.min_interval_input)
                                        .desired_width(48.0),
                                );
                                ui.colored_label(DIM, "上限/分");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.max_per_minute_input)
                                        .desired_width(48.0),
                                );
                                ui.checkbox(&mut self.force_input, "强制非交易日");
                            });
                        });

                        ui.add_space(8.0);
                        panel(ui, "数据源与续爬", |ui| {
                            ui.horizontal_wrapped(|ui| {
                                ui.radio_value(&mut self.source_mode, 0, "仅百度");
                                ui.radio_value(&mut self.source_mode, 1, "仅东财");
                                ui.radio_value(&mut self.source_mode, 2, "一键(百度→东财)");
                            });
                            ui.horizontal_wrapped(|ui| {
                                ui.radio_value(&mut self.resume_mode, true, "继续");
                                ui.radio_value(&mut self.resume_mode, false, "从头覆盖");
                                if ui.button("刷新进度").clicked() {
                                    self.existing_count = count_existing(
                                        &self.db_path_input,
                                        &self.trade_date_input,
                                    );
                                    self.python_running = detect_python_running();
                                }
                            });
                            ui.horizontal_wrapped(|ui| {
                                ui.colored_label(DIM, "空壳N");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.empty_limit_input)
                                        .desired_width(36.0),
                                );
                                ui.colored_label(DIM, "冷却M天");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.empty_cooldown_input)
                                        .desired_width(36.0),
                                );
                                ui.colored_label(DIM, "等待上限秒");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.rate_wait_cap_input)
                                        .desired_width(36.0),
                                );
                            });
                            ui.colored_label(
                                DIM,
                                format!(
                                    "库内已抓 {} / 共 {} 支",
                                    self.existing_count, self.total_codes
                                ),
                            );
                            let hint = if self.existing_count > 0 {
                                if self.resume_mode {
                                    format!("将接着爬剩余 {} 支", remain)
                                } else {
                                    "将从头全量覆盖".to_string()
                                }
                            } else {
                                "该交易日暂无数据，将全量抓取".to_string()
                            };
                            ui.colored_label(TXT, hint);
                            if self.python_running {
                                ui.colored_label(WARN, "Python 爬虫在跑：开始抓取将先接管");
                            } else {
                                ui.colored_label(OK, "未检测到 Python 后台爬虫");
                            }
                            ui.colored_label(DIM, format!("僵尸清理: {}", self.zombie_msg));
                        });

                        ui.add_space(8.0);
                        panel(ui, "数据质量", |ui| {
                            ui.horizontal_wrapped(|ui| {
                                if ui
                                    .add_enabled(!busy_check, egui::Button::new("检查完整性"))
                                    .clicked()
                                {
                                    let state = self.state.clone();
                                    self.check_thread = Some(thread::spawn(move || {
                                        let settings = crate::settings::Settings::load();
                                        run_check(&settings, state);
                                    }));
                                }
                                if ui
                                    .add_enabled(!busy_res, egui::Button::new("回填重抓"))
                                    .clicked()
                                {
                                    let state = self.state.clone();
                                    let stop = self.stop_flag.clone();
                                    self.rescrape_thread = Some(thread::spawn(move || {
                                        let settings = crate::settings::Settings::load();
                                        run_rescrape(&settings, state, stop);
                                    }));
                                }
                            });
                            ui.colored_label(
                                DIM,
                                "检查写入清单；回填对缺失股强制补齐",
                            );
                            ui.add_space(4.0);
                            ui.colored_label(DIM, "可选：对齐 Web 的 Python 脚本");
                            ui.horizontal_wrapped(|ui| {
                                if ui.button("仅东财(Py)").clicked() {
                                    let db = self.db_path_input.clone();
                                    let mi = self.min_interval_input.clone();
                                    let mpm = self.max_per_minute_input.clone();
                                    let state = self.state.clone();
                                    thread::spawn(move || {
                                        spawn_python_crawler(
                                            "eastmoney_stockcomment_crawler.py",
                                            &db,
                                            &mi,
                                            &mpm,
                                            state,
                                        );
                                    });
                                }
                                if ui.button("一键百度+东财(Py)").clicked() {
                                    let db = self.db_path_input.clone();
                                    let mi = self.min_interval_input.clone();
                                    let mpm = self.max_per_minute_input.clone();
                                    let state = self.state.clone();
                                    thread::spawn(move || {
                                        if let Ok(mut g) = state.lock() {
                                            g.push_log("一键：启动 Python 百度全市场…".into());
                                        }
                                        spawn_python_crawler(
                                            "baidu_finance_ai_crawler.py",
                                            &db,
                                            &mi,
                                            &mpm,
                                            state.clone(),
                                        );
                                        if let Ok(mut g) = state.lock() {
                                            g.push_log("一键：启动 Python 东财全市场…".into());
                                        }
                                        spawn_python_crawler(
                                            "eastmoney_stockcomment_crawler.py",
                                            &db,
                                            &mi,
                                            &mpm,
                                            state,
                                        );
                                    });
                                }
                            });
                        });
                    });
            });

        // —— 主区：会话进度（签名元素：大号完成率） ——
        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(PAPER)
                    .inner_margin(egui::Margin::symmetric(20.0, 16.0)),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        // Signature: session completion meter
                        egui::Frame::none()
                            .fill(SURFACE)
                            .stroke(egui::Stroke::new(1.0, LINE))
                            .rounding(egui::Rounding::same(4.0))
                            .inner_margin(egui::Margin::symmetric(24.0, 20.0))
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.vertical(|ui| {
                                        ui.colored_label(
                                            DIM,
                                            egui::RichText::new("SESSION COMPLETION").size(11.0),
                                        );
                                        ui.colored_label(
                                            INK,
                                            egui::RichText::new(format!("{:.1}%", pct))
                                                .size(48.0)
                                                .strong(),
                                        );
                                        ui.colored_label(
                                            DIM,
                                            format!("已处理 {} / {}", processed, total),
                                        );
                                    });
                                    ui.add_space(24.0);
                                    ui.vertical(|ui| {
                                        ui.add_space(18.0);
                                        ui.add(
                                            egui::ProgressBar::new(frac)
                                                .fill(ACCENT)
                                                .desired_width(ui.available_width().max(180.0))
                                                .desired_height(12.0),
                                        );
                                        if !status_msg.is_empty() {
                                            ui.add_space(8.0);
                                            ui.colored_label(DIM, status_msg.as_str());
                                        }
                                    });
                                });
                            });

                        ui.add_space(12.0);
                        ui.columns(3, |c| {
                            kpi(&mut c[0], "新增", done, OK);
                            kpi(&mut c[1], "跳过", skipped, MUTED);
                            kpi(&mut c[2], "失败", failed, DANGER);
                        });

                        ui.add_space(12.0);
                        panel(ui, "当前任务", |ui| {
                            ui.horizontal(|ui| {
                                ui.colored_label(DIM, "标的");
                                ui.colored_label(
                                    INK,
                                    egui::RichText::new(format!("{}  {}", cur_name, cur_code))
                                        .size(16.0)
                                        .strong(),
                                );
                            });
                            ui.horizontal(|ui| {
                                ui.colored_label(DIM, "接口");
                                ui.monospace(cur_ep.as_str());
                            });
                            ui.add_space(6.0);
                            ui.horizontal_wrapped(|ui| {
                                ui.colored_label(DIM, "连续 403");
                                ui.colored_label(
                                    if c403 > 0 { WARN } else { TXT },
                                    egui::RichText::new(format!("{}", c403)).strong(),
                                );
                                ui.add_space(12.0);
                                ui.colored_label(DIM, "冷却");
                                ui.colored_label(
                                    if cd_rem > 0.0 { WARN } else { TXT },
                                    egui::RichText::new(format!("{:.0}s", cd_rem)).strong(),
                                );
                            });
                            ui.horizontal_wrapped(|ui| {
                                ui.colored_label(DIM, format!("单只 {:.2}s", single_el));
                                ui.colored_label(DIM, format!("均速 {:.2}s/支", avg));
                                ui.colored_label(DIM, format!("已用 {:.0}s", total_el));
                                ui.colored_label(DIM, format!("ETA {:.0}s", eta));
                            });
                        });
                    });
            });

        ctx.request_repaint_after(Duration::from_millis(150));
    }
}

/// 追加一行到日志文件（windows 子系统无控制台，不能用 println）。
fn append_log(path: &std::path::PathBuf, line: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = f.write_all(format!("{}\n", line).as_bytes());
    }
}

/// 无界面模式（已弃用：GUI 为唯一入口，此函数不再被调用，仅保留以免破坏编译）。
#[allow(dead_code)]
fn run_headless(args: &[String], settings: &crate::settings::Settings) {
    let get = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1).cloned())
    };
    let has = |flag: &str| args.iter().any(|a| a == flag);

    // 日志写到 exe 同目录的 rust_crawl.log。
    let log_path = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("rust_crawl.log")))
        .unwrap_or_else(|| std::path::PathBuf::from("rust_crawl.log"));

    let db_path = get("--db").unwrap_or_else(||
        "C:/Users/Administrator/WorkBuddy/2026-07-18-17-52-45/market_data.db".to_string());
    // 默认交易日：续上库里数据最多的那天（与 GUI 一致），除非显式指定。
    let trade_date = get("--trade-date").unwrap_or_else(|| default_trade_date(&db_path));
    let limit = get("--limit").and_then(|s| s.parse::<usize>().ok());
    let min_interval = get("--min-interval").and_then(|s| s.parse::<f64>().ok()).unwrap_or(1.0);
    let max_per_minute = get("--max-per-minute").and_then(|s| s.parse::<usize>().ok()).unwrap_or(40);
    let force = has("--force");

    if !force && !is_trading_day(&trade_date) {
        append_log(&log_path, &format!("[headless] {} 非交易日，已跳过（--force 可强制）", trade_date));
        return;
    }
    let codes = load_codes();
    if codes.is_empty() {
        append_log(&log_path, "[headless] 代码清单为空，退出");
        return;
    }
    append_log(&log_path, &format!("[headless] 交易日={} 数据库={} 代码数={} limit={:?}",
             trade_date, db_path, codes.len(), limit));

    let state = Arc::new(Mutex::new(AppState::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut checklist = crate::checklist::CheckList::load_or_default(&settings.general.checklist_path);
    let config = CrawlConfig {
        db_path, trade_date, codes,
        min_interval, max_per_minute, jitter: 0.6,
        max_retries: 3, timeout: 15, limit,
        resume: true, fresh_days: 2,
        empty_limit: 3, empty_cooldown_days: 7, rate_wait_cap: Some(15.0),
    };
    // 后台线程写进度到日志文件
    let st2 = state.clone();
    let stop2 = stop.clone();
    let log2 = log_path.clone();
    let printer = thread::spawn(move || {
        let mut last = 0usize;
        loop {
            thread::sleep(Duration::from_secs(3));
            let (done, skipped, failed, total, cur, ep, status) = {
                let g = st2.lock().unwrap();
                (g.done, g.skipped, g.failed, g.total,
                 g.current_code.clone(), g.current_endpoint.clone(), g.status.clone())
            };
            let processed = done + skipped + failed;
            if processed != last {
                append_log(&log2, &format!("[进度] {}/{} 新增{} 跳过{} 失败{} 当前={} {} 状态={:?}",
                         processed, total, done, skipped, failed, cur, ep, status));
                last = processed;
            }
            if matches!(status, CrawlStatus::Done | CrawlStatus::Error | CrawlStatus::Stopped)
                && processed >= total {
                break;
            }
            if stop2.load(Ordering::SeqCst) { break; }
        }
    });
    run_crawler(config, state.clone(), stop.clone(), settings, &mut checklist);
    let _ = printer.join();
    let g = state.lock().unwrap();
    append_log(&log_path, &format!("[headless] 结束: 新增{} 跳过{} 失败{} {}",
             g.done, g.skipped, g.failed, g.status_msg));
}

/// 独立完整性检查（只读，不改库）：扫描 stocks 表每一只股票，对某交易日查
/// scores/support_resistance/fund_flow/vote 四表整行是否存在，缺 >= 阈值项即记为异常，
/// 写入 needed_check_list.json，并把进度推到 GUI 日志。由 GUI「检查完整性」按钮调用。
fn run_check(settings: &Settings, state: Arc<Mutex<AppState>>) {
    let log_path = PathBuf::from(&settings.general.log_path);
    let db_path = settings.general.db_path.clone();
    let log = |s: &str| {
        append_log(&log_path, s);
        if let Ok(mut st) = state.lock() {
            st.push_log(s.to_string());
        }
    };

    let db = match Db::open(&db_path) {
        Ok(d) => d,
        Err(e) => {
            log(&format!("[check] 打开数据库失败: {}", e));
            if let Ok(mut st) = state.lock() {
                st.status = CrawlStatus::Error;
                st.status_msg = format!("检查失败: {}", e);
            }
            return;
        }
    };

    // 交易日：settings.check.trade_date（"latest" 取数据最多那天）
    let trade_date = if settings.check.trade_date == "latest" {
        db.resume_candidate_date()
            .unwrap_or_else(|| default_trade_date(&db_path))
    } else {
        settings.check.trade_date.clone()
    };

    let codes = load_codes();
    if codes.is_empty() {
        log("[check] 代码清单为空，退出");
        return;
    }
    let threshold = settings.check.missing_count_threshold;

    if let Ok(mut st) = state.lock() {
        st.status = CrawlStatus::Running;
        st.status_msg = "完整性检查中…".into();
    }

    let mut checklist = CheckList::load_or_default(&settings.general.checklist_path);
    let mut abnormal = 0usize;
    log(&format!(
        "[check] 交易日={} 数据库={} 阈值(缺≥{}项) 代码数={}",
        trade_date, db_path, threshold, codes.len()
    ));

    for stock in &codes {
        match db.completeness(&trade_date, &stock.code) {
            Ok(c) => {
                let missing = c.missing_vec();
                let n = missing.len();
                if n >= threshold {
                    abnormal += 1;
                    log(&format!(
                        "[check] ⚠ {} ({}) 缺 {} 项: {:?}",
                        stock.code, stock.name, n, missing
                    ));
                    let prev = checklist.items.iter().find(|i| i.code == stock.code);
                    let tries = prev.map(|i| i.tries).unwrap_or(0);
                    let last_try = prev.and_then(|i| i.last_try.clone());
                    let status = if tries >= settings.rescrape.max_tries {
                        CheckStatus::Exhausted
                    } else {
                        CheckStatus::Pending
                    };
                    checklist.upsert(CheckItem {
                        code: stock.code.clone(),
                        name: stock.name.clone(),
                        missing,
                        missing_count: n,
                        tries,
                        last_try,
                        status,
                    });
                }
            }
            Err(e) => log(&format!("[check] 查询失败 {}: {}", stock.code, e)),
        }
    }

    checklist.trade_date = trade_date.clone();
    checklist.updated_at = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    if let Err(e) = checklist.save(&settings.general.checklist_path) {
        log(&format!("[check] 写清单失败: {}", e));
    }
    log(&format!(
        "[check] 结束: 扫描 {} 支, 异常 {} 支 -> {}",
        codes.len(),
        abnormal,
        settings.general.checklist_path
    ));
    if let Ok(mut st) = state.lock() {
        st.status = CrawlStatus::Done;
        st.status_msg = format!("完整性检查完成: 异常 {} 支", abnormal);
    }
}

/// 回填（重抓）模式：读 needed_check_list.json，对仍异常且未耗尽重试次数的股票强制重抓，
/// 补齐缺失项。最多重试 max_tries 次，每次之间按 backoff_minutes 退避；非交易日且开启跳过则不回填。
/// 由 GUI「回填重抓」按钮调用，支持「停止」按钮中断（stop 标志）。
fn run_rescrape(settings: &Settings, state: Arc<Mutex<AppState>>, stop: Arc<AtomicBool>) {
    let log_path = PathBuf::from(&settings.general.log_path);
    let db_path = settings.general.db_path.clone();
    let log = |s: &str| {
        append_log(&log_path, s);
        if let Ok(mut st) = state.lock() {
            st.push_log(s.to_string());
        }
    };

    let mut checklist = CheckList::load_or_default(&settings.general.checklist_path);
    if checklist.items.is_empty() {
        log("[rescrape] 清单为空，无需回填");
        if let Ok(mut st) = state.lock() {
            st.status = CrawlStatus::Idle;
            st.status_msg = "回填: 清单为空".into();
        }
        return;
    }
    let trade_date = checklist.trade_date.clone();

    // 非交易日不回填
    if settings.rescrape.skip_on_non_trading_day && !is_trading_day(&trade_date) {
        log(&format!(
            "[rescrape] {} 非交易日，跳过回填（skip_on_non_trading_day=true）",
            trade_date
        ));
        if let Ok(mut st) = state.lock() {
            st.status = CrawlStatus::Idle;
            st.status_msg = "回填: 非交易日跳过".into();
        }
        return;
    }

    if let Ok(mut st) = state.lock() {
        st.status = CrawlStatus::Running;
        st.status_msg = "回填重抓中…".into();
    }

    log(&format!(
        "[rescrape] 交易日={} 最大重试={} 退避={:?}",
        trade_date, settings.rescrape.max_tries, settings.rescrape.backoff_minutes
    ));

    let max = settings.rescrape.max_tries as usize;
    for attempt in 1..=max {
        if stop.load(Ordering::SeqCst) {
            log("[rescrape] 用户中止");
            break;
        }
        // 本趟待处理：pending 且未达上限且仍超阈值
        let due: Vec<String> = checklist
            .items
            .iter()
            .filter(|i| {
                i.status == CheckStatus::Pending
                    && i.tries < settings.rescrape.max_tries
                    && i.missing_count >= settings.check.missing_count_threshold
            })
            .map(|i| i.code.clone())
            .collect();
        if due.is_empty() {
            break;
        }
        log(&format!(
            "[rescrape] 第 {}/{} 次回填，待处理 {} 支",
            attempt, max, due.len()
        ));

        for code in &due {
            if stop.load(Ordering::SeqCst) {
                log("[rescrape] 用户中止");
                break;
            }
            let name = checklist
                .items
                .iter()
                .find(|i| i.code == *code)
                .map(|i| i.name.clone())
                .unwrap_or_default();

            // 强制重抓单只：resume=false 绕过「已存在即跳过」，save_snapshot 以 INSERT OR REPLACE 覆盖。
            let cfg = CrawlConfig {
                db_path: db_path.clone(),
                trade_date: trade_date.clone(),
                codes: vec![StockRef {
                    code: code.clone(),
                    name: name.clone(),
                }],
                min_interval: 1.0,
                max_per_minute: 40,
                jitter: 0.6,
                max_retries: 3,
                timeout: 15,
                limit: Some(1),
                resume: false,
                fresh_days: 2,
                empty_limit: 0,
                empty_cooldown_days: 0,
                rate_wait_cap: Some(15.0),
            };
            let st = Arc::new(Mutex::new(AppState::default()));
            let stp = Arc::new(AtomicBool::new(false));
            run_crawler(cfg, st, stp, settings, &mut checklist);

            // 重抓后重新计算完整性
            match Db::open(&db_path).and_then(|d| d.completeness(&trade_date, code)) {
                Ok(c) => {
                    let missing = c.missing_vec();
                    let n = missing.len();
                    if let Some(it) = checklist.items.iter_mut().find(|i| i.code == *code) {
                        it.tries += 1;
                        it.last_try =
                            Some(Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string());
                        if n == 0 {
                            it.missing.clear();
                            it.missing_count = 0;
                            it.status = CheckStatus::Resolved;
                            log(&format!("[rescrape] ✓ {} 已补齐", code));
                        } else {
                            it.missing = missing;
                            it.missing_count = n;
                            it.status = if it.tries >= settings.rescrape.max_tries {
                                CheckStatus::Exhausted
                            } else {
                                CheckStatus::Pending
                            };
                            log(&format!(
                                "[rescrape] ✗ {} 仍缺 {} 项: {:?} (tries={})",
                                code, n, it.missing, it.tries
                            ));
                        }
                    }
                }
                Err(e) => log(&format!("[rescrape] 重算失败 {}: {}", code, e)),
            }
            let _ = checklist.save(&settings.general.checklist_path);
        }

        // 是否还有未补齐的 pending 项，决定是否退避后继续
        let still = checklist.items.iter().any(|i| {
            i.status == CheckStatus::Pending
                && i.missing_count >= settings.check.missing_count_threshold
        });
        if !still || attempt >= max {
            break;
        }
        let mins = settings
            .rescrape
            .backoff_minutes
            .get(attempt - 1)
            .copied()
            .unwrap_or(15) as f64;
        log(&format!("[rescrape] 等待 {:.0} 分钟后再试", mins));
        // 可中断的退避等待
        let mut rem = mins * 60.0;
        while rem > 0.0 {
            if stop.load(Ordering::SeqCst) {
                log("[rescrape] 用户中止(退避中)");
                break;
            }
            let step = rem.min(1.0);
            thread::sleep(Duration::from_secs_f64(step));
            rem -= step;
        }
    }

    let _ = checklist.save(&settings.general.checklist_path);
    let remain = checklist
        .items
        .iter()
        .filter(|i| {
            i.missing_count >= settings.check.missing_count_threshold
                && i.status != CheckStatus::Resolved
        })
        .count();
    log(&format!("[rescrape] 结束: 仍有 {} 支异常未补齐", remain));
    if let Ok(mut st) = state.lock() {
        st.status = CrawlStatus::Done;
        st.status_msg = format!("回填结束: 仍异常 {} 支", remain);
    }
}

fn setup_fonts(ctx: &egui::Context) {
    // 优先从 Windows 系统字体加载 CJK 字体，让 egui 能显示中文。
    // 若找不到，保持默认字体并打印警告（中文会显示为方块）。
    let candidates = [
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\msyh.ttf",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\simsun.ttc",
    ];
    for path in &candidates {
        if let Ok(bytes) = std::fs::read(path) {
            let mut fonts = egui::FontDefinitions::default();
            fonts.font_data.insert(
                "cjk".to_owned(),
                egui::FontData::from_owned(bytes),
            );
            // 把 CJK 字体放在 Proportional 首位，中文和英文都能渲染。
            if let Some(proportional) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
                proportional.insert(0, "cjk".to_owned());
            }
            // Monospace 也加一份回退，保证日志区中文可读。
            if let Some(monospace) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
                monospace.insert(0, "cjk".to_owned());
            }
            ctx.set_fonts(fonts);
            return;
        }
    }
    // 无系统 CJK 字体时静默降级（windows 子系统无 stderr，避免 eprintln panic）。
}

fn main() -> eframe::Result<()> {
    // GUI 为唯一入口：双击 exe 即进入图形界面；启动默认最大化。
    let options = NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 800.0])
            .with_min_inner_size([960.0, 640.0])
            .with_resizable(true)
            .with_maximized(true)
            .with_title("MarketPulse · A股数据采集"),
        ..Default::default()
    };
    eframe::run_native(
        "MarketPulse",
        options,
        Box::new(|cc| {
            setup_fonts(&cc.egui_ctx);
            Ok(Box::new(CrawlerApp::default()))
        }),
    )
}
