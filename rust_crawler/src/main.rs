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
mod verify;
mod telegram;
mod lookup;

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
use crate::state::{note_problem, submit_ack, wait_user_ack, AppState, CrawlStatus, ErrorNotice, ProblemItem, UserAck};
use crate::db::Db;
use crate::settings::Settings;
use crate::checklist::{CheckList, CheckItem, CheckStatus};
use crate::verify::{baidu_analysis_api_url, network_hint, source_verify_links};

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

/// 个股查询主区：详情或全市场排名列表。
#[derive(Clone, Copy, PartialEq, Eq)]
enum LookupPage {
    Detail,
    Rank,
}

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
    /// 弹窗「同样原因本轮不再问」勾选。
    mute_same_kind: bool,
    /// 启动后只做一次网络探测。
    did_net_probe: bool,
    /// 启动后把本机上次发库编号补写到 bot 简介一次。
    did_tg_resync: bool,
    /// 联网方式：0自动 1强制直连 2走系统代理
    net_mode: usize,
    /// 联网方式说明弹窗。
    show_net_help: bool,
    tg_token: String,
    tg_chat: String,
    show_tg_help: bool,
    tg_thread: Option<thread::JoinHandle<()>>,
    /// 个股查询输入框（6 位代码）。
    lookup_code: String,
    /// 当前正在看的个股（可能是从排名跳过来的）。
    lookup: Option<crate::lookup::StockSnapshot>,
    /// 本次在输入框查出的那只，返回时回到它。
    origin_snap: Option<crate::lookup::StockSnapshot>,
    lookup_err: String,
    lookup_page: LookupPage,
    rank: Option<crate::lookup::RankBoard>,
    rank_err: String,
    show_rank_help: bool,
    /// 主区：0=抓取进度 1=个股查询。
    main_tab: usize,
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
        let state = Arc::new(Mutex::new(AppState::default()));
        if let Ok(mut g) = state.lock() {
            g.push_log(format!(
                "会话日志完整写入 {}（按日追加，不截断）",
                crate::state::session_log_path().display()
            ));
        }
        Self {
            state,
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
            mute_same_kind: false,
            did_net_probe: false,
            did_tg_resync: false,
            net_mode: crate::http::current_proxy_mode().as_index(),
            show_net_help: false,
            tg_token: crate::telegram::load().bot_token,
            tg_chat: crate::telegram::load().chat_id,
            show_tg_help: false,
            tg_thread: None,
            lookup_code: String::new(),
            lookup: None,
            origin_snap: None,
            lookup_err: String::new(),
            lookup_page: LookupPage::Detail,
            rank: None,
            rank_err: String::new(),
            show_rank_help: false,
            main_tab: 0,
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

/// 字段右侧排序标识：未排显示「排」，当前字段显示「正」或「倒」。
fn sort_field_button(
    ui: &mut egui::Ui,
    sort_id: &str,
    current: Option<&crate::lookup::RankBoard>,
) -> bool {
    let (txt, active, asc) = match current {
        Some(b) if b.spec_id == sort_id => {
            if b.ascending {
                ("正", true, true)
            } else {
                ("倒", true, false)
            }
        }
        _ => ("排", false, true),
    };
    let color = if active {
        egui::Color32::WHITE
    } else {
        ACCENT
    };
    let mut btn = egui::Button::new(egui::RichText::new(txt).size(11.0).color(color).strong());
    btn = if active {
        btn.fill(if asc { ACCENT } else { WARN })
    } else {
        btn.fill(PAPER).stroke(egui::Stroke::new(1.0, ACCENT))
    };
    ui.add(btn.small()).clicked()
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
        CrawlStatus::NeedConfirm => ("待确认", DANGER),
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
        if let Ok(g) = self.state.lock() {
            if g.pending_error.is_some() || matches!(g.status, CrawlStatus::NeedConfirm) {
                return;
            }
        }
        let trade_date = self.trade_date_input.clone();
        if !self.force_input && !is_trading_day(&trade_date) {
            popup_notice(
                &self.state,
                ErrorNotice {
                    kind: "其它错误".into(),
                    code: String::new(),
                    name: trade_date.clone(),
                    detail: format!("{} 非交易日，已阻止（勾选「强制」可忽略）", trade_date),
                    hint: "休市日源站通常也没有当日数据。勾选「强制」后再点开始。".into(),
                    links: vec![],
                },
            );
            return;
        }
        let codes = load_codes();
        if codes.is_empty() {
            popup_notice(
                &self.state,
                ErrorNotice {
                    kind: "其它错误".into(),
                    code: String::new(),
                    name: "代码清单".into(),
                    detail: "代码清单为空".into(),
                    hint: "内置 a_stocks.json 未能加载，请重新下载完整安装包。".into(),
                    links: vec![],
                },
            );
            return;
        }
        if detect_python_running() && is_shared_db(&self.db_path_input) {
            let msg = kill_python_crawler();
            thread::sleep(Duration::from_secs(2));
            if detect_python_running() {
                popup_notice(
                    &self.state,
                    ErrorNotice {
                        kind: "其它错误".into(),
                        code: String::new(),
                        name: "Python爬虫".into(),
                        detail: format!("{} 但 Python 仍在运行，已阻止启动（禁止并存）", msg),
                        hint: "请先关掉 Python 版爬虫窗口，再点开始。".into(),
                        links: vec![],
                    },
                );
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
        if let Ok(mut g) = self.state.lock() {
            g.mute_error_kinds.clear();
            g.problems.clear();
        }
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
            timeout: 30,
            limit: None,
            resume,
            fresh_days: 2,
            empty_limit,
            empty_cooldown_days: empty_cd,
            rate_wait_cap: rate_cap,
        };
        let handle = thread::spawn(move || match source_mode {
            1 => {
                let mut em_cfg = config;
                em_cfg.min_interval = 0.05;
                em_cfg.max_per_minute = 800;
                em_cfg.jitter = 0.0;
                run_em_crawler(em_cfg, state, stop, &settings);
            }
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
                em_cfg.min_interval = 0.05;
                em_cfg.max_per_minute = 800;
                em_cfg.jitter = 0.0;
                run_em_crawler(em_cfg, state, stop, &settings);
            }
            _ => run_crawler(config, state, stop, &settings, &mut checklist),
        });
        self.crawler_thread = Some(handle);
    }

    /// 按左侧/主区输入的代码查库，切到「个股查询」页；这次查出的股记为返回原点。
    fn run_stock_lookup(&mut self) {
        self.lookup_err.clear();
        self.rank_err.clear();
        self.main_tab = 1;
        self.lookup_page = LookupPage::Detail;
        match crate::lookup::lookup_stock(&self.db_path_input, &self.lookup_code) {
            Ok(mut snap) => {
                Self::fill_snap_name(&mut snap);
                self.origin_snap = Some(snap.clone());
                self.lookup = Some(snap);
                self.rank = None;
            }
            Err(e) => {
                self.lookup = None;
                self.origin_snap = None;
                self.lookup_err = e;
            }
        }
    }

    fn fill_snap_name(snap: &mut crate::lookup::StockSnapshot) {
        if snap.name.is_empty() {
            let code = snap.code.clone();
            if let Some(s) = load_codes().into_iter().find(|s| s.code == code) {
                snap.name = s.name;
            }
        }
    }

    fn origin_label(&self) -> String {
        match &self.origin_snap {
            Some(s) if !s.name.is_empty() => format!("{} {}", s.code, s.name),
            Some(s) => s.code.clone(),
            None => "原先个股".into(),
        }
    }

    fn back_to_origin(&mut self) {
        self.lookup_page = LookupPage::Detail;
        self.rank_err.clear();
        if let Some(origin) = self.origin_snap.clone() {
            self.lookup_code = origin.code.clone();
            self.lookup = Some(origin);
        }
    }

    /// 从排名点进去：只换正在看的股，不改返回原点。
    fn jump_to_stock(&mut self, code: &str) {
        self.lookup_err.clear();
        self.rank_err.clear();
        self.lookup_page = LookupPage::Detail;
        match crate::lookup::lookup_stock(&self.db_path_input, code) {
            Ok(mut snap) => {
                Self::fill_snap_name(&mut snap);
                self.lookup = Some(snap);
            }
            Err(e) => {
                self.lookup_err = e;
            }
        }
    }

    fn run_rank_dir(&mut self, spec_id: &str, ascending: bool) {
        self.rank_err.clear();
        self.main_tab = 1;
        match crate::lookup::rank_market(&self.db_path_input, spec_id, ascending) {
            Ok(board) => {
                self.rank = Some(board);
                self.lookup_page = LookupPage::Rank;
            }
            Err(e) => {
                self.rank_err = e;
            }
        }
    }

    /// toggle=true：同一字段再点一次改倒序；false：打开已有排名或默认字段正序。
    fn run_rank(&mut self, spec_id: &str, toggle: bool) {
        let asc = if toggle {
            match &self.rank {
                Some(b) if b.spec_id == spec_id => !b.ascending,
                _ => true,
            }
        } else {
            self.rank
                .as_ref()
                .filter(|b| b.spec_id == spec_id)
                .map(|b| b.ascending)
                .unwrap_or(true)
        };
        if !toggle {
            if let Some(b) = &self.rank {
                if b.spec_id == spec_id && b.ascending == asc {
                    self.lookup_page = LookupPage::Rank;
                    return;
                }
            }
        }
        self.run_rank_dir(spec_id, asc);
    }

    fn open_rank_from_button(&mut self) {
        self.main_tab = 1;
        if self.lookup.is_none() && self.origin_snap.is_none() {
            self.rank_err = "请先查询一只股票，再点排名".into();
            self.lookup_page = LookupPage::Detail;
            return;
        }
        if let Some(id) = self.rank.as_ref().map(|b| b.spec_id.clone()) {
            self.run_rank(&id, false);
            return;
        }
        let snap = self.lookup.as_ref().or(self.origin_snap.as_ref());
        if let Some(id) = snap.and_then(crate::lookup::default_rank_id) {
            self.run_rank(id, false);
        } else {
            self.rank_err = "当前个股没有可排序的数值字段".into();
        }
    }

    fn needs_lookup_back(&self) -> bool {
        if self.lookup_page == LookupPage::Rank {
            return true;
        }
        match (&self.lookup, &self.origin_snap) {
            (Some(a), Some(b)) => a.code != b.code,
            _ => false,
        }
    }

    /// 主区个股查询：分组卡片 + 两列标签/值，长文整行折行。
    fn ui_stock_lookup(&mut self, ui: &mut egui::Ui) {
        let mut do_query = false;
        let mut do_copy = false;
        let mut do_rank = false;
        let mut do_back = false;
        panel(ui, "按代码查询", |ui| {
            ui.colored_label(DIM, "查的是本地库里该股各表最新一行，不是实时行情。代码可写 1、000001、000001.SZ。");
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.lookup_code)
                        .desired_width(140.0)
                        .hint_text("如 000001")
                        .font(egui::TextStyle::Monospace),
                );
                if ui.button("查询").clicked()
                    || (resp.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                {
                    do_query = true;
                }
                if ui
                    .add_enabled(self.lookup.is_some() || self.origin_snap.is_some(), egui::Button::new("排名"))
                    .clicked()
                {
                    do_rank = true;
                }
                if ui.button("说明").clicked() {
                    self.show_rank_help = true;
                }
                if ui
                    .add_enabled(self.lookup.is_some(), egui::Button::new("复制全部"))
                    .clicked()
                {
                    do_copy = true;
                }
                if self.needs_lookup_back() {
                    let back = format!("返回 {}", self.origin_label());
                    if ui.button(back).clicked() {
                        do_back = true;
                    }
                }
            });
            ui.colored_label(
                DIM,
                "有「排」的字段可点：第一次正序、再点倒序，对全市场已抓取数据排名。",
            );
        });
        if do_query {
            self.run_stock_lookup();
        }
        if do_rank {
            self.open_rank_from_button();
        }
        if do_back {
            self.back_to_origin();
        }
        if do_copy {
            if let Some(s) = &self.lookup {
                ui.ctx().copy_text(s.as_text());
            }
        }
        ui.add_space(10.0);
        if !self.lookup_err.is_empty() {
            ui.colored_label(DANGER, &self.lookup_err);
        }
        if !self.rank_err.is_empty() {
            ui.colored_label(WARN, &self.rank_err);
        }
        if self.lookup_page == LookupPage::Rank {
            self.ui_rank_board(ui);
            return;
        }
        if !self.lookup_err.is_empty() && self.lookup.is_none() {
            return;
        }
        let Some(snap) = self.lookup.clone() else {
            ui.colored_label(DIM, "在上方或左侧输入股票代码后点查询，这里按分组列出该股全部字段。");
            return;
        };

        let jumped = self
            .origin_snap
            .as_ref()
            .map(|o| o.code != snap.code)
            .unwrap_or(false);
        egui::Frame::none()
            .fill(SURFACE)
            .stroke(egui::Stroke::new(1.0, LINE))
            .rounding(egui::Rounding::same(4.0))
            .inner_margin(egui::Margin::symmetric(20.0, 16.0))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.colored_label(
                        INK,
                        egui::RichText::new(&snap.code).size(28.0).strong().monospace(),
                    );
                    ui.add_space(12.0);
                    ui.vertical(|ui| {
                        ui.colored_label(
                            INK,
                            egui::RichText::new(if snap.name.is_empty() {
                                "—"
                            } else {
                                snap.name.as_str()
                            })
                            .size(20.0)
                            .strong(),
                        );
                        ui.colored_label(
                            if snap.found { DIM } else { WARN },
                            &snap.hint,
                        );
                        if jumped {
                            ui.colored_label(
                                ACCENT,
                                format!("从排名查看。返回可回到 {}", self.origin_label()),
                            );
                        }
                    });
                });
            });

        ui.add_space(12.0);
        if snap.sections.is_empty() {
            return;
        }
        let mut sort_click: Option<&'static str> = None;
        for sec in &snap.sections {
            panel(ui, &sec.title, |ui| {
                if sec.wide {
                    for f in &sec.rows {
                        ui.colored_label(DIM, egui::RichText::new(&f.label).size(11.0));
                        ui.add(
                            egui::Label::new(egui::RichText::new(&f.value).size(13.0).color(INK))
                                .wrap(),
                        );
                        ui.add_space(8.0);
                    }
                } else {
                    egui::Grid::new(format!("lookup_{}", sec.title))
                        .num_columns(4)
                        .min_col_width(72.0)
                        .spacing([14.0, 8.0])
                        .striped(true)
                        .show(ui, |ui| {
                            for (i, f) in sec.rows.iter().enumerate() {
                                ui.colored_label(DIM, egui::RichText::new(&f.label).size(12.0));
                                ui.horizontal(|ui| {
                                    ui.add(
                                        egui::Label::new(
                                            egui::RichText::new(&f.value)
                                                .size(13.0)
                                                .color(INK)
                                                .strong(),
                                        )
                                        .wrap(),
                                    );
                                    if let Some(id) = f.sort_id {
                                        if sort_field_button(ui, id, self.rank.as_ref()) {
                                            sort_click = Some(id);
                                        }
                                    }
                                });
                                if i % 2 == 1 {
                                    ui.end_row();
                                }
                            }
                            if sec.rows.len() % 2 == 1 {
                                ui.end_row();
                            }
                        });
                }
            });
            ui.add_space(10.0);
        }
        if let Some(id) = sort_click {
            self.run_rank(id, true);
        }
    }

    fn ui_rank_board(&mut self, ui: &mut egui::Ui) {
        let Some(board) = self.rank.clone() else {
            ui.colored_label(DIM, "还没有排名。点字段右侧「排」，或先点「排名」。");
            return;
        };
        let origin_code = self
            .origin_snap
            .as_ref()
            .map(|s| s.code.clone())
            .unwrap_or_default();
        let origin_label = self.origin_label();
        let my_rank = board.origin_rank(&origin_code);
        let mut jump: Option<String> = None;
        let mut set_asc: Option<bool> = None;

        panel(ui, &format!("全市场排名 · {}", board.label), |ui| {
            ui.colored_label(
                DIM,
                format!(
                    "{} · 已抓取 {} 支有此数值。点一行查看该股，返回回到 {}。",
                    if board.ascending {
                        "正序（从小到大）"
                    } else {
                        "倒序（从大到小）"
                    },
                    board.rows.len(),
                    origin_label
                ),
            );
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui.selectable_label(board.ascending, "正序").clicked() && !board.ascending {
                    set_asc = Some(true);
                }
                if ui.selectable_label(!board.ascending, "倒序").clicked() && board.ascending {
                    set_asc = Some(false);
                }
            });
            if let Some(r) = my_rank {
                ui.colored_label(
                    ACCENT,
                    egui::RichText::new(format!(
                        "{} 当前第 {} / {}",
                        origin_label,
                        r,
                        board.rows.len()
                    ))
                    .strong(),
                );
            } else if !origin_code.is_empty() {
                ui.colored_label(DIM, format!("{} 这一项没有可比较数值，不在列表里", origin_label));
            }
        });
        ui.add_space(8.0);

        ui.horizontal(|ui| {
            ui.add_sized(
                [52.0, 18.0],
                egui::Label::new(egui::RichText::new("#").size(12.0).color(DIM)),
            );
            ui.add_sized(
                [76.0, 18.0],
                egui::Label::new(egui::RichText::new("代码").size(12.0).color(DIM)),
            );
            ui.add_sized(
                [120.0, 18.0],
                egui::Label::new(egui::RichText::new("名称").size(12.0).color(DIM)),
            );
            ui.colored_label(DIM, egui::RichText::new(&board.label).size(12.0));
        });
        ui.separator();

        let row_h = 28.0;
        let n = board.rows.len();
        // 用主区剩余固定高度，避免 max_height=∞ 时列表撑满全量行、滚轮无效。
        let list_h = (ui.max_rect().bottom() - ui.cursor().top() - 6.0).max(160.0);
        egui::ScrollArea::vertical()
            .id_salt("rank_rows")
            .auto_shrink([false, false])
            .max_height(list_h)
            .show_rows(ui, row_h, n, |ui, range| {
                for i in range {
                    let row = &board.rows[i];
                    let is_origin = row.code == origin_code;
                    let (rect, resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width().max(320.0), row_h),
                        egui::Sense::click(),
                    );
                    if is_origin {
                        ui.painter().rect_filled(
                            rect,
                            2.0,
                            egui::Color32::from_rgba_unmultiplied(13, 148, 136, 40),
                        );
                    } else if resp.hovered() {
                        ui.painter().rect_filled(rect, 2.0, SURFACE);
                    }
                    let y = rect.center().y;
                    let num_color = if is_origin { ACCENT } else { DIM };
                    ui.painter().text(
                        egui::pos2(rect.left() + 8.0, y),
                        egui::Align2::LEFT_CENTER,
                        format!("{}", row.rank),
                        egui::FontId::monospace(13.0),
                        num_color,
                    );
                    ui.painter().text(
                        egui::pos2(rect.left() + 56.0, y),
                        egui::Align2::LEFT_CENTER,
                        &row.code,
                        egui::FontId::monospace(13.0),
                        INK,
                    );
                    let name = if row.name.is_empty() {
                        "—"
                    } else {
                        row.name.as_str()
                    };
                    ui.painter().text(
                        egui::pos2(rect.left() + 140.0, y),
                        egui::Align2::LEFT_CENTER,
                        name,
                        egui::FontId::proportional(13.0),
                        INK,
                    );
                    ui.painter().text(
                        egui::pos2(rect.left() + 280.0, y),
                        egui::Align2::LEFT_CENTER,
                        &row.value_text,
                        egui::FontId::proportional(13.0),
                        if is_origin { ACCENT } else { INK },
                    );
                    if resp.clicked() {
                        jump = Some(row.code.clone());
                    }
                }
            });

        if let Some(asc) = set_asc {
            let id = board.spec_id.clone();
            self.run_rank_dir(&id, asc);
        }
        if let Some(code) = jump {
            self.jump_to_stock(&code);
        }
    }

    /// 出错弹窗：必须点确认/重试/停止；打开源站只开浏览器，不替你点确认。
    fn show_error_dialog(&mut self, ctx: &egui::Context, notice: &ErrorNotice) {
        let title = format!("请确认 · {}", notice.kind);
        egui::Window::new(title)
            .collapsible(false)
            .resizable(true)
            .default_width(560.0)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.add_space(8.0);
                ui.colored_label(
                    DANGER,
                    egui::RichText::new(&notice.kind).size(18.0).strong(),
                );
                if !notice.code.is_empty() || !notice.name.is_empty() {
                    ui.colored_label(
                        INK,
                        egui::RichText::new(format!("{}  {}", notice.name, notice.code))
                            .size(15.0)
                            .strong(),
                    );
                }
                ui.add_space(6.0);
                ui.label("未拿到 / 失败 ≠ 源站确认无数据。请打开下面链接，看页面上有没有对应内容、是否和本次结果一致。");
                ui.add_space(6.0);
                egui::ScrollArea::vertical()
                    .max_height(140.0)
                    .show(ui, |ui| {
                        ui.colored_label(TXT, &notice.detail);
                    });
                if !notice.hint.is_empty() {
                    ui.add_space(8.0);
                    ui.colored_label(DIM, &notice.hint);
                }
                ui.add_space(10.0);
                ui.colored_label(INK, egui::RichText::new("出问题的网页").strong());
                let page_links: Vec<_> = notice
                    .links
                    .iter()
                    .filter(|l| l.kind == "page")
                    .cloned()
                    .collect();
                ui.horizontal_wrapped(|ui| {
                    if page_links.is_empty() {
                        ui.colored_label(DIM, "（本条没有可跳转的网页）");
                    }
                    for link in &page_links {
                        let text = format!("{} · {}", link.source, link.label);
                        if ui.button(text).clicked() {
                            if let Err(e) = webbrowser::open(&link.url) {
                                if let Ok(mut g) = self.state.lock() {
                                    g.push_log(format!("无法打开浏览器: {e}  {}", link.url));
                                }
                            }
                        }
                    }
                });
                ui.add_space(6.0);
                for link in &page_links {
                    ui.horizontal(|ui| {
                        ui.colored_label(DIM, "网页");
                        ui.monospace(&link.url);
                    });
                }
                ui.add_space(10.0);
                ui.checkbox(
                    &mut self.mute_same_kind,
                    "同样原因本轮不再弹出（仍会记失败，只是不问）",
                );
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("确认继续")
                                    .color(egui::Color32::WHITE)
                                    .strong(),
                            )
                            .fill(ACCENT),
                        )
                        .clicked()
                    {
                        submit_ack(&self.state, UserAck::Continue, self.mute_same_kind);
                    }
                    if ui.button("重试本只").clicked() {
                        submit_ack(&self.state, UserAck::Retry, self.mute_same_kind);
                    }
                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("停止抓取")
                                    .color(egui::Color32::WHITE)
                                    .strong(),
                            )
                            .fill(DANGER),
                        )
                        .clicked()
                    {
                        self.stop_flag.store(true, Ordering::SeqCst);
                        submit_ack(&self.state, UserAck::Stop, false);
                    }
                });
                ui.add_space(4.0);
                ui.colored_label(
                    DIM,
                    "点「打开源站」只打开浏览器，还需要再点确认/重试/停止，爬虫才会继续。",
                );
            });
    }
}

impl App for CrawlerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        if !self.did_maximize {
            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(true));
            self.did_maximize = true;
        }
        if !self.did_net_probe {
            self.did_net_probe = true;
            spawn_startup_probe(self.state.clone());
        }
        if !self.did_tg_resync {
            self.did_tg_resync = true;
            let state = self.state.clone();
            thread::spawn(move || {
                crate::telegram::resync_published_file_id(&|s| {
                    if let Ok(mut g) = state.lock() {
                        g.push_log(s.to_string());
                    }
                });
            });
        }
        if self.tg_thread.as_ref().map(|h| h.is_finished()).unwrap_or(false) {
            if let Some(h) = self.tg_thread.take() {
                let _ = h.join();
                self.trade_date_input = default_trade_date(&self.db_path_input);
                self.existing_count =
                    count_existing(&self.db_path_input, &self.trade_date_input);
                self.resume_mode = true;
            }
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
            log_total,
            pending_error,
            problems,
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
                {
                    let n = g.logs.len();
                    if n > 4000 {
                        g.logs[n - 4000..].to_vec()
                    } else {
                        g.logs.clone()
                    }
                },
                g.logs.len(),
                g.pending_error.clone(),
                g.problems.clone(),
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
        let awaiting = pending_error.is_some()
            || matches!(status, CrawlStatus::NeedConfirm);
        let is_busy = matches!(
            status,
            CrawlStatus::Running | CrawlStatus::Cooling | CrawlStatus::NeedConfirm
        );

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
                            egui::RichText::new(format!(
                                "A股全市场数据采集控制台  v{}",
                                env!("CARGO_PKG_VERSION")
                            ))
                            .size(11.0),
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
                            if pending_error.is_some() {
                                submit_ack(&self.state, UserAck::Stop, false);
                            }
                        }
                        ui.add_space(6.0);
                        if ui
                                .add(
                                egui::Button::new(
                                    egui::RichText::new(if awaiting {
                                        "请先确认弹窗"
                                    } else if is_busy {
                                        "抓取进行中…"
                                    } else {
                                        "开始抓取"
                                    })
                                    .color(egui::Color32::WHITE)
                                    .strong(),
                                )
                                .fill(if awaiting { WARN } else { ACCENT })
                                .sense(if is_busy {
                                    egui::Sense::hover()
                                } else {
                                    egui::Sense::click()
                                }),
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
                    ui.colored_label(
                        DIM,
                        egui::RichText::new(format!("共 {log_total} 行 · 完整文件不删除")).size(11.0),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("打开日志目录").clicked() {
                            let dir = crate::state::session_log_dir();
                            let _ = std::fs::create_dir_all(&dir);
                            let _ = std::process::Command::new("explorer").arg(dir).spawn();
                        }
                        if ui.button("复制全部").clicked() {
                            let text = std::fs::read_to_string(crate::state::session_log_path())
                                .unwrap_or_else(|_| logs.join("\n"));
                            ui.ctx().copy_text(text);
                        }
                    });
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
                            ui.colored_label(
                                DIM,
                                "默认 16 路同时抓。遇 403 自动降到 4/1 路。自动模式若探测到 VPN/系统代理，一半走代理一半直连。",
                            );
                        });

                        ui.add_space(8.0);
                        panel(ui, "联网方式", |ui| {
                            let old = self.net_mode;
                            ui.radio_value(&mut self.net_mode, 0, "自动(代理死了就直连)");
                            ui.radio_value(&mut self.net_mode, 1, "强制直连");
                            ui.radio_value(&mut self.net_mode, 2, "走系统代理");
                            if self.net_mode != old {
                                crate::http::set_proxy_mode(crate::http::ProxyMode::from_index(
                                    self.net_mode,
                                ));
                                let name = match self.net_mode {
                                    1 => "强制直连",
                                    2 => "走系统代理",
                                    _ => "自动(代理死了就直连)",
                                };
                                if let Ok(mut g) = self.state.lock() {
                                    g.push_log(format!("联网方式已改为：{name}（下次抓取/重新探测生效）"));
                                }
                            }
                            ui.colored_label(DIM, crate::http::last_proxy_desc());
                            ui.horizontal_wrapped(|ui| {
                                if ui.button("这种方式怎么用").clicked() {
                                    self.show_net_help = true;
                                }
                                if ui.button("重新探测").clicked() {
                                    crate::http::set_proxy_mode(crate::http::ProxyMode::from_index(
                                        self.net_mode,
                                    ));
                                    spawn_startup_probe(self.state.clone());
                                }
                            });
                        });

                        ui.add_space(8.0);
                        panel(ui, "发送数据库到 Telegram", |ui| {
                            ui.colored_label(DIM, "免费通道。国内请先开 VPN 再发。");
                            ui.horizontal(|ui| {
                                ui.colored_label(DIM, "Token");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.tg_token)
                                        .password(true)
                                        .desired_width(f32::INFINITY),
                                );
                            });
                            ui.horizontal(|ui| {
                                ui.colored_label(DIM, "Chat ID");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.tg_chat)
                                        .desired_width(f32::INFINITY),
                                );
                            });
                            let tg_busy = self
                                .tg_thread
                                .as_ref()
                                .map(|h| !h.is_finished())
                                .unwrap_or(false);
                            ui.horizontal_wrapped(|ui| {
                                if ui.button("怎么发").clicked() {
                                    self.show_tg_help = true;
                                }
                                if ui
                                    .add_enabled(!tg_busy && !is_busy, egui::Button::new(if tg_busy {
                                        "正在处理…"
                                    } else {
                                        "发送数据库"
                                    }))
                                    .clicked()
                                {
                                    let cfg = crate::telegram::TelegramCfg {
                                        bot_token: self.tg_token.trim().to_string(),
                                        chat_id: self.tg_chat.trim().to_string(),
                                        last_file_id: crate::telegram::load().last_file_id,
                                    };
                                    if let Err(e) = crate::telegram::save(&cfg) {
                                        if let Ok(mut g) = self.state.lock() {
                                            g.push_log(format!("保存 telegram.json 失败: {e}"));
                                        }
                                    }
                                    let db = self.db_path_input.clone();
                                    let state = self.state.clone();
                                    self.tg_thread = Some(thread::spawn(move || {
                                        let log = |s: &str| {
                                            if let Ok(mut g) = state.lock() {
                                                g.push_log(s.to_string());
                                            }
                                        };
                                        match crate::telegram::send_database(&db, &cfg, &log) {
                                            Ok(()) => {}
                                            Err(e) => log(&format!("发送失败: {e}")),
                                        }
                                    }));
                                }
                                if ui
                                    .add_enabled(!tg_busy && !is_busy, egui::Button::new("下载历史库"))
                                    .clicked()
                                {
                                    let cfg = crate::telegram::TelegramCfg {
                                        bot_token: self.tg_token.trim().to_string(),
                                        chat_id: self.tg_chat.trim().to_string(),
                                        last_file_id: crate::telegram::load().last_file_id,
                                    };
                                    let _ = crate::telegram::save(&cfg);
                                    let db = self.db_path_input.clone();
                                    let state = self.state.clone();
                                    self.tg_thread = Some(thread::spawn(move || {
                                        let log = |s: &str| {
                                            if let Ok(mut g) = state.lock() {
                                                g.push_log(s.to_string());
                                            }
                                        };
                                        match crate::telegram::download_latest_database(&db, &cfg, &log)
                                        {
                                            Ok(()) => {}
                                            Err(e) => log(&format!("下载历史库失败: {e}")),
                                        }
                                    }));
                                }
                                if ui
                                    .add_enabled(!tg_busy && !is_busy, egui::Button::new("导入本地历史库"))
                                    .clicked()
                                {
                                    if let Some(path) = rfd::FileDialog::new()
                                        .add_filter("历史库", &["gz", "db"])
                                        .pick_file()
                                    {
                                        let db = self.db_path_input.clone();
                                        let state = self.state.clone();
                                        self.tg_thread = Some(thread::spawn(move || {
                                            let log = |s: &str| {
                                                if let Ok(mut g) = state.lock() {
                                                    g.push_log(s.to_string());
                                                }
                                            };
                                            match crate::telegram::install_history_file(
                                                &path, &db, &log,
                                            ) {
                                                Ok(()) => {}
                                                Err(e) => log(&format!("导入历史库失败: {e}")),
                                            }
                                        }));
                                    }
                                }
                            });
                            ui.colored_label(
                                DIM,
                                "发送成功后本机库会删掉（Telegram 只留当前一组）。下载总是拉这一组，保持「继续」再爬。",
                            );
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
                        panel(ui, "查询个股", |ui| {
                            ui.colored_label(DIM, "输入代码，主区展示库内该股全部字段");
                            ui.add_space(4.0);
                            ui.horizontal(|ui| {
                                let resp = ui.add(
                                    egui::TextEdit::singleline(&mut self.lookup_code)
                                        .desired_width(88.0)
                                        .hint_text("如 000001")
                                        .font(egui::TextStyle::Monospace),
                                );
                                if ui.button("查询").clicked()
                                    || (resp.has_focus()
                                        && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                                {
                                    self.run_stock_lookup();
                                }
                                if ui
                                    .add_enabled(
                                        self.lookup.is_some() || self.origin_snap.is_some(),
                                        egui::Button::new("排名"),
                                    )
                                    .clicked()
                                {
                                    self.open_rank_from_button();
                                }
                                if ui.small_button("?").clicked() {
                                    self.show_rank_help = true;
                                }
                            });
                            if !self.lookup_err.is_empty() {
                                ui.colored_label(DANGER, self.lookup_err.as_str());
                            } else if let Some(s) = &self.lookup {
                                ui.colored_label(
                                    if s.found { OK } else { WARN },
                                    format!(
                                        "{} {}",
                                        s.code,
                                        if s.name.is_empty() { "—" } else { s.name.as_str() }
                                    ),
                                );
                            }
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
                        });
                    });
            });

        // —— 主区：抓取进度 / 个股查询 ——
        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(PAPER)
                    .inner_margin(egui::Margin::symmetric(20.0, 16.0)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if ui
                        .selectable_label(self.main_tab == 0, "抓取进度")
                        .clicked()
                    {
                        self.main_tab = 0;
                    }
                    if ui
                        .selectable_label(self.main_tab == 1, "个股查询")
                        .clicked()
                    {
                        self.main_tab = 1;
                    }
                });
                ui.add_space(10.0);
                if self.main_tab == 1 {
                    let body = ui.available_size();
                    ui.allocate_ui(body, |ui| {
                        if self.lookup_page == LookupPage::Rank {
                            self.ui_stock_lookup(ui);
                        } else {
                            egui::ScrollArea::vertical()
                                .id_salt("lookup_scroll")
                                .auto_shrink([false, false])
                                .show(ui, |ui| {
                                    self.ui_stock_lookup(ui);
                                });
                        }
                    });
                    return;
                }
                egui::ScrollArea::vertical()
                    .id_salt("progress_scroll")
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

                        ui.add_space(12.0);
                        panel(ui, "失败或不完整（网页）", |ui| {
                            ui.colored_label(
                                DIM,
                                format!("本轮 {} 条。点按钮打开对应网页，不卡住抓取。", problems.len()),
                            );
                            ui.horizontal_wrapped(|ui| {
                                if ui.button("导出全部链接").clicked() {
                                    export_problem_links(&problems);
                                }
                            });
                            egui::ScrollArea::vertical()
                                .max_height(180.0)
                                .show(ui, |ui| {
                                    if problems.is_empty() {
                                        ui.colored_label(DIM, "暂无");
                                    }
                                    for p in problems.iter().rev().take(80) {
                                        ui.horizontal_wrapped(|ui| {
                                            ui.colored_label(
                                                DANGER,
                                                egui::RichText::new(&p.kind).small(),
                                            );
                                            ui.colored_label(
                                                INK,
                                                format!("{} {}", p.name, p.code),
                                            );
                                            if !p.page_url.is_empty() {
                                                let label = if p.page_label.is_empty() {
                                                    "打开网页".into()
                                                } else {
                                                    p.page_label.clone()
                                                };
                                                if ui.small_button(label).clicked() {
                                                    let _ = webbrowser::open(&p.page_url);
                                                }
                                            }
                                        });
                                        ui.colored_label(DIM, &p.detail);
                                        if !p.page_url.is_empty() {
                                            ui.monospace(&p.page_url);
                                        }
                                        ui.add_space(4.0);
                                    }
                                });
                        });
                    });
            });

        if let Some(notice) = pending_error.as_ref() {
            self.show_error_dialog(ctx, notice);
        }

        if self.show_net_help {
            let mut open = self.show_net_help;
            egui::Window::new("联网方式：操作步骤")
                .collapsible(false)
                .resizable(true)
                .default_width(520.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .open(&mut open)
                .show(ctx, |ui| {
                    ui.label("VPN 关掉后网页能开、爬虫不能：多半不是源站没数据，是系统还留着死代理。");
                    ui.add_space(6.0);
                    ui.label("1) 推荐「自动」：先看系统代理 / HTTPS_PROXY 的端口还在不在听。VPN、Clash 关掉后 127.0.0.1:7890 通常已经没人听，程序改直连，和浏览器一致。");
                    ui.label("2) 仍失败就点「强制直连」。完全不走代理，适合刚关 VPN、网页已经能开的情况。");
                    ui.label("3) 必须翻墙才能到源站时，先打开 VPN/Clash 的系统代理，再选「走系统代理」。");
                    ui.label("4) 点「重新探测」会立刻打百度财经和东财。失败会弹窗，可点源站链接核对。");
                    ui.label("5) 浏览器能开、程序提示 DNS/resolve：浏览器可能开了安全 DNS，本机 DNS 还是 VPN 的。可在网卡里改 DNS，或 ipconfig /flushdns。");
                    ui.label("6) 抓取默认 16 路同时进行。自动模式探测到 VPN/Clash 系统代理还活着：一半请求走代理、一半直连（两个出口）。遇 403 会降到 4 路再降到 1 路，连续成功后再加回去。");
                });
            if !open {
                self.show_net_help = false;
            }
        }

        if self.show_tg_help {
            let mut open = self.show_tg_help;
            egui::Window::new("发送数据库到 Telegram：操作步骤")
                .collapsible(false)
                .resizable(true)
                .default_width(540.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .open(&mut open)
                .show(ctx, |ui| {
                    ui.label(crate::telegram::help_text());
                });
            if !open {
                self.show_tg_help = false;
            }
        }

        if self.show_rank_help {
            let mut open = self.show_rank_help;
            egui::Window::new("市场排名：操作步骤")
                .collapsible(false)
                .resizable(true)
                .default_width(520.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .open(&mut open)
                .show(ctx, |ui| {
                    ui.label("1) 先在「查询个股」输入代码（如 000001），点查询，看到该股全部字段。");
                    ui.label("2) 能比大小的字段右侧会出现「排」。点第一次按正序（从小到大）排全市场已抓取数据；再点同一标识改为倒序。");
                    ui.label("3) 查询框旁的「排名」会打开最近一次排名；还没排过则默认用综合得分或技术。");
                    ui.label("4) 点排名列表里的一行，进入该股详情。点「返回 000001 平安银行」会回到当初查询的那只，不会停在跳转股。");
                    ui.label("5) 排名用各表最新一行，只含库里已经抓到、且该字段有数字的股票。");
                });
            if !open {
                self.show_rank_help = false;
            }
        }

        if pending_error.is_some() {
            ctx.request_repaint();
        } else {
            ctx.request_repaint_after(Duration::from_millis(150));
        }
    }
}

fn export_problem_links(problems: &[ProblemItem]) {
    let path = PathBuf::from(crate::settings::workspace_dir()).join("fail_links.txt");
    let mut text = String::from("# 失败或不完整 — 网页链接（不是接口）\n\n");
    for p in problems {
        text.push_str(&format!(
            "[{}] {} {} — {}\n{}\n\n",
            p.kind, p.code, p.name, p.detail, p.page_url
        ));
    }
    match std::fs::write(&path, text.as_bytes()) {
        Ok(()) => {
            let _ = std::process::Command::new("notepad.exe").arg(&path).spawn();
        }
        Err(_) => {}
    }
}

/// 界面线程不能卡住等确认，所以丢到后台线程里走同一套弹窗。
fn popup_notice(state: &Arc<Mutex<AppState>>, notice: ErrorNotice) {
    let state = state.clone();
    thread::spawn(move || {
        let _ = wait_user_ack(&state, notice);
    });
}

/// 启动后探测百度/东财是否通；VPN 关掉后的死代理会在这里改直连。
fn spawn_startup_probe(state: Arc<Mutex<AppState>>) {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(400));
        if let Ok(g) = state.lock() {
            if matches!(
                g.status,
                CrawlStatus::Running | CrawlStatus::Cooling | CrawlStatus::NeedConfirm
            ) {
                return;
            }
        }
        if let Some(px) = crate::http::detect_proxy_url() {
            if let Ok(mut g) = state.lock() {
                g.push_log(format!("读到系统/环境代理: {px}"));
            }
        }
        let client = match crate::http::build_blocking_client(25) {
            Ok(c) => c,
            Err(e) => {
                let _ = wait_user_ack(
                    &state,
                    ErrorNotice {
                        kind: "网络错误".into(),
                        code: String::new(),
                        name: "启动探测".into(),
                        detail: format!("HTTP 客户端创建失败: {e}"),
                        hint: network_hint().into(),
                        links: source_verify_links("000001", &["baidu", "em"]),
                    },
                );
                return;
            }
        };
        if let Ok(mut g) = state.lock() {
            g.push_log(format!("联网探测使用：{}", crate::http::last_proxy_desc()));
        }
        let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
        let mut headers = reqwest::header::HeaderMap::new();
        if let Ok(v) = reqwest::header::HeaderValue::from_str(ua) {
            headers.insert(reqwest::header::USER_AGENT, v);
        }
        let checks: [(&str, String, Vec<crate::verify::VerifyLink>); 3] = [
            (
                "百度财经首页",
                "https://finance.baidu.com/".into(),
                source_verify_links("000001", &["baidu"]),
            ),
            (
                "百度分析接口",
                baidu_analysis_api_url("000001"),
                source_verify_links("000001", &["baidu"]),
            ),
            (
                "东财千股千评",
                "https://data.eastmoney.com/stockcomment/".into(),
                source_verify_links("000001", &["em"]),
            ),
        ];
        let mut fails: Vec<String> = Vec::new();
        let mut links = Vec::new();
        for (name, url, ln) in &checks {
            match crate::http::send_get_with_fallback(
                &client,
                url,
                Some(&headers),
                Duration::from_secs(20),
            ) {
                Ok(resp) => {
                    let code = resp.status().as_u16();
                    // 403/401 说明已经连上（多半是 Cookie），不算「网络错误」
                    if code >= 500 || code == 0 {
                        fails.push(format!("{name} HTTP {code}\n{url}"));
                        links.extend(ln.clone());
                    }
                }
                Err(e) => {
                    fails.push(format!(
                        "{name} 网络错误: {e}{}",
                        crate::http::proxy_hint_suffix()
                    ));
                    links.extend(ln.clone());
                }
            }
        }
        if fails.is_empty() {
            if let Ok(mut g) = state.lock() {
                g.push_log(format!(
                    "启动探测：百度财经 / 东财可达。{}",
                    crate::http::last_proxy_desc()
                ));
            }
            return;
        }
        if let Ok(g) = state.lock() {
            if matches!(g.status, CrawlStatus::Running | CrawlStatus::Cooling) {
                return;
            }
        }
        let _ = wait_user_ack(
            &state,
            ErrorNotice {
                kind: "网络错误".into(),
                code: String::new(),
                name: "启动探测".into(),
                detail: fails.join("\n\n"),
                hint: network_hint().into(),
                links,
            },
        );
    });
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
        max_retries: 3, timeout: 30, limit,
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
            let _ = wait_user_ack(
                &state,
                ErrorNotice {
                    kind: "落库失败".into(),
                    code: String::new(),
                    name: "完整性检查".into(),
                    detail: format!("打开数据库失败: {e}"),
                    hint: "检查数据库路径是否存在、是否被其它程序占用。".into(),
                    links: vec![],
                },
            );
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
                    let detail = format!("完整性缺 {} 项: {:?}", n, missing);
                    log(&format!(
                        "[check] ⚠ {} ({}) {}",
                        stock.code, stock.name, detail
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
                    note_problem(
                        &state,
                        "数据不完整",
                        &stock.code,
                        &stock.name,
                        &detail,
                        &["baidu"],
                    );
                }
            }
            Err(e) => log(&format!("[check] 查询失败 {}: {}", stock.code, e)),
        }
    }

    checklist.trade_date = trade_date.clone();
    checklist.updated_at = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    if let Err(e) = checklist.save(&settings.general.checklist_path) {
        log(&format!("[check] 写清单失败: {}", e));
        let _ = wait_user_ack(
            &state,
            ErrorNotice {
                kind: "落库失败".into(),
                code: String::new(),
                name: "待核查清单".into(),
                detail: format!("写清单失败: {e}"),
                hint: "检查 needed_check_list.json 路径是否可写。".into(),
                links: vec![],
            },
        );
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
                timeout: 30,
                limit: Some(1),
                resume: false,
                fresh_days: 2,
                empty_limit: 0,
                empty_cooldown_days: 0,
                rate_wait_cap: Some(15.0),
            };
            let st = state.clone();
            let stp = stop.clone();
            run_crawler(cfg, st, stp, settings, &mut checklist);
            if let Ok(mut g) = state.lock() {
                if !matches!(g.status, CrawlStatus::Stopped) {
                    g.status = CrawlStatus::Running;
                    g.status_msg = "回填重抓中…".into();
                }
            }

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
