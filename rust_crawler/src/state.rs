//! GUI 与爬虫线程共享的进度状态（当时当下写入，UI 150ms 刷一次）。

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrawlStatus {
    Idle,
    Running,
    Cooling,
    Done,
    Stopped,
    Error,
}

impl Default for CrawlStatus {
    fn default() -> Self {
        Self::Idle
    }
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
