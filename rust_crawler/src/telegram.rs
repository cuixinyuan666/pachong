//! 把本地 market_data.db 发到 Telegram（免费、官方 Bot API，不当成网盘账号登录）。
//! 国内直连常被拦：发送前请开 VPN，联网方式选「走系统代理」。

use std::fs::File;
use std::io::{copy, BufReader};
use std::path::{Path, PathBuf};

use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::settings::workspace_dir;

const TG_DOC_LIMIT: u64 = 49 * 1024 * 1024;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelegramCfg {
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: String,
}

fn cfg_path() -> PathBuf {
    PathBuf::from(workspace_dir()).join("telegram.json")
}

pub fn load() -> TelegramCfg {
    let text = match std::fs::read_to_string(cfg_path()) {
        Ok(t) => t,
        Err(_) => return TelegramCfg::default(),
    };
    serde_json::from_str(&text).unwrap_or_default()
}

pub fn save(cfg: &TelegramCfg) -> Result<(), String> {
    let text = serde_json::to_string_pretty(cfg).map_err(|e| e.to_string())?;
    std::fs::write(cfg_path(), text).map_err(|e| e.to_string())
}

/// 先把 WAL 刷进主库，再拷贝、gzip，用 Bot API sendDocument 发出去。
pub fn send_database(db_path: &str, cfg: &TelegramCfg, log: &dyn Fn(&str)) -> Result<(), String> {
    let token = cfg.bot_token.trim();
    let chat = cfg.chat_id.trim();
    if token.is_empty() || chat.is_empty() {
        return Err("请先填 Bot Token 和 Chat ID（点「怎么发」看步骤）".into());
    }
    if !Path::new(db_path).is_file() {
        return Err(format!("找不到数据库: {db_path}"));
    }

    log("正在把 WAL 刷进主库，避免发出去的是半成品…");
    match Db::open(db_path) {
        Ok(db) => {
            let _ = db.conn().execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
        }
        Err(e) => log(&format!("打开库做 checkpoint 失败（仍尝试拷贝）: {e}")),
    }

    let tmp = std::env::temp_dir();
    let raw = tmp.join("marketpulse_send.db");
    let gz_path = tmp.join("marketpulse_send.db.gz");
    std::fs::copy(db_path, &raw).map_err(|e| format!("拷贝数据库失败: {e}"))?;

    log("正在压缩数据库…");
    {
        let mut enc = GzEncoder::new(File::create(&gz_path).map_err(|e| e.to_string())?, Compression::fast());
        let mut src = BufReader::new(File::open(&raw).map_err(|e| e.to_string())?);
        copy(&mut src, &mut enc).map_err(|e| e.to_string())?;
        enc.finish().map_err(|e| e.to_string())?;
    }
    let _ = std::fs::remove_file(&raw);
    let size = std::fs::metadata(&gz_path)
        .map(|m| m.len())
        .unwrap_or(0);
    log(&format!(
        "压缩后 {:.1} MB",
        size as f64 / (1024.0 * 1024.0)
    ));
    if size > TG_DOC_LIMIT {
        let _ = std::fs::remove_file(&gz_path);
        return Err(format!(
            "压缩后仍超过 Telegram 50MB 限制（{:.1} MB）。请用 U 盘或微信文件传输助手拷贝原库。",
            size as f64 / (1024.0 * 1024.0)
        ));
    }

    log("正在发给 Telegram…（国内请先开 VPN）");
    let client = crate::http::build_blocking_client(180)?;
    let url = format!("https://api.telegram.org/bot{token}/sendDocument");
    let caption = format!(
        "MarketPulse 数据库 {}\n{:.1} MB gzip",
        chrono::Local::now().format("%Y-%m-%d %H:%M"),
        size as f64 / (1024.0 * 1024.0)
    );
    let form = reqwest::blocking::multipart::Form::new()
        .text("chat_id", chat.to_string())
        .text("caption", caption)
        .file("document", &gz_path)
        .map_err(|e| format!("组装文件失败: {e}"))?;
    let resp = client
        .post(&url)
        .multipart(form)
        .send()
        .map_err(|e| format!("网络错误: {e}{}", crate::http::proxy_hint_suffix()))?;
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    let _ = std::fs::remove_file(&gz_path);
    if !status.is_success() {
        return Err(format!("Telegram HTTP {status}: {}", body.chars().take(300).collect::<String>()));
    }
    let ok = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("ok").and_then(|x| x.as_bool()))
        .unwrap_or(false);
    if !ok {
        return Err(format!(
            "Telegram 拒绝: {}",
            body.chars().take(300).collect::<String>()
        ));
    }
    log("已发到 Telegram。");
    Ok(())
}

pub fn help_text() -> &'static str {
    "Telegram 是目前能直接从本程序发文件的免费通道（官方 Bot API）。百度网盘/蓝奏云没有可用的免费开放接口，本程序不会去登那些网盘。\n\n\
     操作步骤：\n\
     1) 手机打开 Telegram，搜 BotFather，发 /newbot，按提示拿到 Bot Token\n\
     2) 搜自己刚建的 bot，点开始，给它发一句「你好」\n\
     3) 浏览器打开：https://api.telegram.org/bot<你的Token>/getUpdates\n\
     4) 在返回的 JSON 里找 chat.id（一串数字），填到本页 Chat ID\n\
     5) Token 和 Chat ID 填好后点「发送数据库」\n\
     6) 国内直连 Telegram 常被拦：先开 VPN，左侧联网方式选「走系统代理」再发\n\
     7) 压缩后超过 50MB 发不出去，请用 U 盘或微信文件传输助手拷贝 market_data.db"
}
