//! 完整性检查清单（needed_check_list.json）：缺项股票、重试次数、回填状态。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Pending,
    Exhausted,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckItem {
    pub code: String,
    pub name: String,
    #[serde(default)]
    pub missing: Vec<String>,
    #[serde(default)]
    pub missing_count: usize,
    #[serde(default)]
    pub tries: i64,
    #[serde(default)]
    pub last_try: Option<String>,
    pub status: CheckStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckList {
    #[serde(default)]
    pub trade_date: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub items: Vec<CheckItem>,
}

impl CheckList {
    pub fn load_or_default(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let text = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(path, text).map_err(|e| e.to_string())
    }

    pub fn upsert(&mut self, item: CheckItem) {
        if let Some(old) = self.items.iter_mut().find(|i| i.code == item.code) {
            *old = item;
        } else {
            self.items.push(item);
        }
    }
}
