//! GUI 与爬虫线程共享的进度状态（当时当下写入，UI 150ms 刷一次）。

use std::collections::HashSet;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};

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
            logs: Vec::new(),
            pending_error: None,
            error_ack_tx: None,
            mute_error_kinds: HashSet::new(),
            resume_status: CrawlStatus::Idle,
        }
    }
}

impl AppState {
    /// 追加一行会话日志，最多保留 800 行以免内存涨。
    pub fn push_log(&mut self, line: String) {
        self.logs.push(line);
        if self.logs.len() > 800 {
            let extra = self.logs.len() - 800;
            self.logs.drain(0..extra);
        }
    }
}

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
