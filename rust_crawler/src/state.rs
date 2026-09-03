//! GUI 与爬虫线程共享的进度状态（当时当下写入，UI 150ms 刷一次）。
//! 会话日志：每一行都追加到 exe 旁 logs/session-日期.log，不截断、不覆盖。

use std::collections::HashSet;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, OnceLock};

use crate::verify::VerifyLink;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrawlStatus {
    Idle,
    Running,
    Cooling,
    NeedConfirm, // 弹窗等你点确认 / 打开源站
    Done,
    Stopped,
    Error,
}

impl Default for CrawlStatus {
    fn default() -> Self {
        Self::Idle
    }
}

/// 弹窗里用户的选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserAck {
    Continue, // 已核对，当未拿到/失败，继续下一只
    Retry,    // 重试当前这只
    Stop,     // 停止整轮抓取
}

#[derive(Debug, Clone)]
pub struct ErrorNotice {
    pub kind: String,
    pub code: String,
    pub name: String,
    pub detail: String,
    pub hint: String,
    pub links: Vec<VerifyLink>,
}

#[derive(Debug, Clone)]
pub struct ProblemItem {
    pub kind: String,
    pub code: String,
    pub name: String,
    pub detail: String,
    pub page_url: String,
    pub page_label: String,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub total: usize,
    pub done: usize,
    pub skipped: usize,
    pub failed: usize,
    pub current_code: String,
    pub current_name: String,
    pub current_endpoint: String,
    pub status: CrawlStatus,
    pub status_msg: String,
    pub consecutive_403: usize,
    pub cooldown_remaining: f64,
    pub single_elapsed: f64,
    pub total_elapsed: f64,
    pub eta_secs: f64,
    pub avg_per_stock: f64,
    pub logs: Vec<String>,
    pub pending_error: Option<ErrorNotice>,
    pub error_ack_tx: Option<SyncSender<UserAck>>,
    pub mute_error_kinds: HashSet<String>,
    /// 弹窗出现前的状态，点确认后还原（启动探测在空闲时不要变成「运行中」）
    pub resume_status: CrawlStatus,
    /// 失败/不完整：只给人网页链接，不卡住整轮。
    pub problems: Vec<ProblemItem>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            total: 0,
            done: 0,
            skipped: 0,
            failed: 0,
            current_code: String::new(),
            current_name: String::new(),
            current_endpoint: String::new(),
            status: CrawlStatus::Idle,
            status_msg: String::new(),
            consecutive_403: 0,
            cooldown_remaining: 0.0,
            single_elapsed: 0.0,
            total_elapsed: 0.0,
            eta_secs: 0.0,
            avg_per_stock: 0.0,
            logs: load_today_session_logs(),
            pending_error: None,
            error_ack_tx: None,
            mute_error_kinds: HashSet::new(),
            resume_status: CrawlStatus::Idle,
            problems: Vec::new(),
        }
    }
}

impl AppState {
    /// 追加一行会话日志：界面保留全文，同时写入 logs/session-日期.log。
    pub fn push_log(&mut self, line: String) {
        let ts = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let stamped = format!("[{ts}] {line}");
        append_session_log(&stamped);
        self.logs.push(stamped);
    }
}

pub fn session_log_dir() -> PathBuf {
    PathBuf::from(crate::settings::workspace_dir()).join("logs")
}

pub fn session_log_path() -> PathBuf {
    let day = chrono::Local::now().format("%Y-%m-%d");
    session_log_dir().join(format!("session-{day}.log"))
}

fn load_today_session_logs() -> Vec<String> {
    match std::fs::read_to_string(session_log_path()) {
        Ok(s) => s.lines().map(|l| l.to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

fn append_session_log(line: &str) {
    let _ = (|| -> std::io::Result<()> {
        create_dir_all(session_log_dir())?;
        let mut f = SESSION_FILE
            .get_or_init(|| {
                Mutex::new(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(session_log_path())
                        .ok(),
                )
            })
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "log lock"))?;
        if f.is_none() {
            *f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(session_log_path())
                .ok();
        }
        if let Some(file) = f.as_mut() {
            writeln!(file, "{line}")?;
            file.flush()?;
        }
        Ok(())
    })();
}

static SESSION_FILE: OnceLock<Mutex<Option<std::fs::File>>> = OnceLock::new();

/// 出错后卡住爬虫线程，直到界面弹窗里点了确认/重试/停止。
pub fn wait_user_ack(state: &Arc<Mutex<AppState>>, notice: ErrorNotice) -> UserAck {
    let kind = notice.kind.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    {
        let mut g = match state.lock() {
            Ok(g) => g,
            Err(_) => return UserAck::Stop,
        };
        if g.mute_error_kinds.contains(&kind) {
            g.push_log(format!(
                "[同类已确认不再问] {} {} {} {}",
                notice.kind, notice.code, notice.name, notice.detail
            ));
            return UserAck::Continue;
        }
        g.push_log(format!(
            "⚠ 需确认: [{}] {} {} — {}",
            notice.kind, notice.code, notice.name, notice.detail
        ));
        g.pending_error = Some(notice);
        g.error_ack_tx = Some(tx);
        if !matches!(g.status, CrawlStatus::NeedConfirm) {
            g.resume_status = g.status.clone();
        }
        g.status = CrawlStatus::NeedConfirm;
        g.status_msg = "请在弹窗点「确认继续」或打开源站核对是否符合".into();
    }
    match rx.recv() {
        Ok(a) => a,
        Err(_) => UserAck::Stop,
    }
}

pub fn submit_ack(state: &Arc<Mutex<AppState>>, ack: UserAck, mute_this_kind: bool) {
    let mut g = match state.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    if mute_this_kind {
        let kind = g.pending_error.as_ref().map(|n| n.kind.clone());
        if let Some(kind) = kind {
            g.mute_error_kinds.insert(kind);
        }
    }
    if let Some(tx) = g.error_ack_tx.take() {
        let _ = tx.send(ack);
    }
    g.pending_error = None;
    if matches!(g.status, CrawlStatus::NeedConfirm) {
        g.status = match ack {
            UserAck::Stop => CrawlStatus::Stopped,
            _ => match g.resume_status.clone() {
                CrawlStatus::NeedConfirm => CrawlStatus::Idle,
                other => other,
            },
        };
        g.status_msg.clear();
    }
}

/// 单只失败/不完整：记下网页链接，不弹窗卡住整轮。
pub fn note_problem(
    state: &Arc<Mutex<AppState>>,
    kind: &str,
    code: &str,
    name: &str,
    detail: &str,
    sources: &[&str],
) {
    use crate::verify::page_links_for_stock;
    let links = page_links_for_stock(code, sources);
    let (page_url, page_label) = match links.first() {
        Some(l) => (l.url.clone(), format!("{} · {}", l.source, l.label)),
        None => (String::new(), String::new()),
    };
    if let Ok(mut g) = state.lock() {
        let line = if page_url.is_empty() {
            format!("⚠ [{kind}] {code} {name} — {detail}")
        } else {
            format!("⚠ [{kind}] {code} {name} — {detail}  网页: {page_url}")
        };
        g.push_log(line);
        g.problems.push(ProblemItem {
            kind: kind.to_string(),
            code: code.to_string(),
            name: name.to_string(),
            detail: detail.to_string(),
            page_url,
            page_label,
        });
    }
}
