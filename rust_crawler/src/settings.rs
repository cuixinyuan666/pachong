//! 读 settings.toml（exe 旁优先）。没有文件时用与 Python Web 对齐的默认限流参数。

use serde::Deserialize;
use std::path::PathBuf;
use std::sync::OnceLock;

/// 工作区根：exe 所在目录（发布包把 exe 和 db/脚本放一起）。
pub fn workspace_dir() -> &'static str {
    static DIR: OnceLock<String> = OnceLock::new();
    DIR.get_or_init(|| {
        if let Ok(exe) = std::env::current_exe() {
            if let Some(parent) = exe.parent() {
                return parent.to_string_lossy().replace('\\', "/");
            }
        }
        std::env::current_dir()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| ".".into())
    })
    .as_str()
}

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub general: General,
    #[serde(default)]
    pub rate: Rate,
    #[serde(default)]
    pub check: Check,
    #[serde(default)]
    pub rescrape: Rescrape,
    #[serde(default)]
    pub pacing: Pacing,
}

#[derive(Debug, Clone, Deserialize)]
pub struct General {
    #[serde(default)]
    pub db_path: String,
    #[serde(default = "default_checklist")]
    pub checklist_path: String,
    #[serde(default = "default_log")]
    pub log_path: String,
}

fn default_checklist() -> String {
    format!("{}/needed_check_list.json", workspace_dir())
}
fn default_log() -> String {
    format!("{}/rust_crawl.log", workspace_dir())
}

impl Default for General {
    fn default() -> Self {
        Self {
            db_path: String::new(),
            checklist_path: default_checklist(),
            log_path: default_log(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rate {
    #[serde(default = "default_min_interval")]
    pub min_interval: f64,
    #[serde(default = "default_max_per_minute")]
    pub max_per_minute: usize,
}
fn default_min_interval() -> f64 {
    1.0
}
fn default_max_per_minute() -> usize {
    40
}
impl Default for Rate {
    fn default() -> Self {
        Self {
            min_interval: default_min_interval(),
            max_per_minute: default_max_per_minute(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Check {
    #[serde(default = "default_true")]
    pub check_after_each_stock: bool,
    #[serde(default = "default_missing_threshold")]
    pub missing_count_threshold: usize,
    #[serde(default = "default_latest")]
    pub trade_date: String,
}
fn default_true() -> bool {
    true
}
fn default_missing_threshold() -> usize {
    1
}
fn default_latest() -> String {
    "latest".into()
}
impl Default for Check {
    fn default() -> Self {
        Self {
            check_after_each_stock: true,
            missing_count_threshold: 1,
            trade_date: "latest".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rescrape {
    #[serde(default = "default_max_tries")]
    pub max_tries: i64,
    #[serde(default = "default_backoff")]
    pub backoff_minutes: Vec<u64>,
    #[serde(default = "default_true")]
    pub skip_on_non_trading_day: bool,
    #[serde(default)]
    pub force: bool,
}
fn default_max_tries() -> i64 {
    3
}
fn default_backoff() -> Vec<u64> {
    vec![5, 15, 30]
}
impl Default for Rescrape {
    fn default() -> Self {
        Self {
            max_tries: 3,
            backoff_minutes: default_backoff(),
            skip_on_non_trading_day: true,
            force: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pacing {
    #[serde(default = "default_jitter_extra")]
    pub interval_jitter_extra: f64,
    #[serde(default = "default_micro_every")]
    pub micro_break_every: usize,
    #[serde(default = "default_micro_min")]
    pub micro_break_min: f64,
    #[serde(default = "default_micro_max")]
    pub micro_break_max: f64,
}
fn default_jitter_extra() -> f64 {
    0.8
}
fn default_micro_every() -> usize {
    50
}
fn default_micro_min() -> f64 {
    5.0
}
fn default_micro_max() -> f64 {
    25.0
}
impl Default for Pacing {
    fn default() -> Self {
        Self {
            interval_jitter_extra: default_jitter_extra(),
            micro_break_every: default_micro_every(),
            micro_break_min: default_micro_min(),
            micro_break_max: default_micro_max(),
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            general: General::default(),
            rate: Rate::default(),
            check: Check::default(),
            rescrape: Rescrape::default(),
            pacing: Pacing::default(),
        }
    }
}

impl Settings {
    /// 依次试：exe 旁 settings.toml → 当前目录 → 内置默认。
    pub fn load() -> Self {
        let candidates = [
            PathBuf::from(workspace_dir()).join("settings.toml"),
            PathBuf::from("settings.toml"),
        ];
        for p in &candidates {
            if let Ok(text) = std::fs::read_to_string(p) {
                if let Ok(s) = toml::from_str::<Settings>(&text) {
                    return s;
                }
            }
        }
        Settings::default()
    }
}
