//! 把本地 market_data.db 发到 Telegram（免费、官方 Bot API，不当成网盘账号登录）。
//! 国内直连常被拦：发送前请开 VPN，联网方式选「走系统代理」。

use std::fs::File;
use std::io::{copy, BufReader, Read};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
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
    /// 最近一次发出去的文件 id，本机可再下；另一台需把压缩包转发给 bot。
    #[serde(default)]
    pub last_file_id: String,
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
    if let Some(fid) = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.pointer("/result/document/file_id")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
        })
    {
        let mut stored = cfg.clone();
        stored.last_file_id = fid;
        let _ = save(&stored);
    }
    log("已发到 Telegram。另一台电脑：把这条压缩包转发给同一个 bot，再点「下载历史库」。");
    Ok(())
}

pub fn help_text() -> &'static str {
    "Telegram 是目前能直接从本程序发/收文件的免费通道（官方 Bot API）。\n\n\
     发库：\n\
     1) BotFather 建 bot，拿到 Token；给 bot 发「你好」，填 Chat ID\n\
     2) 点「发送数据库」（国内先开 VPN，联网方式选走系统代理）\n\
     3) 压缩后超过 50MB 发不出去，请用 U 盘或微信文件传输助手拷贝 market_data.db\n\n\
     下载历史库再接着爬：\n\
     1) 另一台电脑填同一套 Token / Chat ID\n\
     2) 打开 Telegram，把机器人发给你的压缩包【转发给这个 bot】\n\
     3) 点「下载历史库」：会备份当前库，换成这份历史库\n\
     4) 保持「继续」，再点开始抓取（已抓过的会跳过）\n\
     也可以点「导入本地历史库」，选 U 盘/微信里的 .db 或 .gz"
}

fn looks_like_history(name: &str, mime: &str) -> bool {
    let n = name.to_lowercase();
    let m = mime.to_lowercase();
    n.ends_with(".gz")
        || n.ends_with(".db")
        || n.contains("market")
        || m.contains("gzip")
        || m.contains("sqlite")
}

/// 从 Telegram 取最新一份历史库，覆盖到 db_path（先备份 .bak）。
pub fn download_latest_database(db_path: &str, cfg: &TelegramCfg, log: &dyn Fn(&str)) -> Result<(), String> {
    let token = cfg.bot_token.trim();
    if token.is_empty() {
        return Err("请先填 Bot Token".into());
    }
    let client = crate::http::build_blocking_client(180)?;
    let mut file_id = String::new();
    let mut file_name = String::new();

    log("正在向 Telegram 询问最近的文件…");
    let url = format!("https://api.telegram.org/bot{token}/getUpdates?limit=100");
    let body = client
        .get(&url)
        .send()
        .map_err(|e| format!("网络错误: {e}{}", crate::http::proxy_hint_suffix()))?
        .text()
        .unwrap_or_default();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    if let Some(arr) = v.get("result").and_then(|x| x.as_array()) {
        let want_chat = cfg.chat_id.trim().to_string();
        let mut best_id = i64::MIN;
        for u in arr {
            let uid = u.get("update_id").and_then(|x| x.as_i64()).unwrap_or(0);
            let msg = u.get("message").or_else(|| u.get("channel_post"));
            let Some(msg) = msg else { continue };
            if !want_chat.is_empty() {
                let cid = msg
                    .pointer("/chat/id")
                    .and_then(|x| x.as_i64().map(|n| n.to_string()).or_else(|| x.as_str().map(|s| s.to_string())))
                    .unwrap_or_default();
                if cid != want_chat {
                    continue;
                }
            }
            let Some(doc) = msg.get("document") else { continue };
            let Some(fid) = doc.get("file_id").and_then(|x| x.as_str()) else { continue };
            let name = doc.get("file_name").and_then(|x| x.as_str()).unwrap_or("");
            let mime = doc.get("mime_type").and_then(|x| x.as_str()).unwrap_or("");
            if !name.is_empty() && !looks_like_history(name, mime) {
                continue;
            }
            if uid >= best_id {
                best_id = uid;
                file_id = fid.to_string();
                file_name = name.to_string();
            }
        }
    }

    if file_id.is_empty() {
        if !cfg.last_file_id.trim().is_empty() {
            log("对话里没有新文件，改用本机记下的上次发出文件。");
            file_id = cfg.last_file_id.trim().to_string();
            file_name = "market_data.db.gz".into();
        } else {
            return Err(
                "没有找到历史文件。请在 Telegram 里把机器人发给你的压缩包【转发给这个 bot】，再点下载。".into(),
            );
        }
    }

    log(&format!("找到文件 {file_name}，开始下载…"));
    let gf = format!("https://api.telegram.org/bot{token}/getFile?file_id={file_id}");
    let gbody = client
        .get(&gf)
        .send()
        .map_err(|e| format!("getFile 失败: {e}"))?
        .text()
        .unwrap_or_default();
    let gv: serde_json::Value = serde_json::from_str(&gbody).unwrap_or(serde_json::Value::Null);
    let path = gv
        .pointer("/result/file_path")
        .and_then(|x| x.as_str())
        .ok_or_else(|| format!("getFile 无效: {}", gbody.chars().take(200).collect::<String>()))?;
    let file_url = format!("https://api.telegram.org/file/bot{token}/{path}");
    let bytes = client
        .get(&file_url)
        .send()
        .map_err(|e| format!("下载失败: {e}{}", crate::http::proxy_hint_suffix()))?
        .bytes()
        .map_err(|e| e.to_string())?;
    log(&format!("已下载 {:.1} MB", bytes.len() as f64 / (1024.0 * 1024.0)));
    install_history_bytes(&bytes, db_path, log)
}

/// 从本地 .db / .gz 导入，覆盖当前库（先备份 .bak）。
pub fn install_history_file(src: &Path, dest_db: &str, log: &dyn Fn(&str)) -> Result<(), String> {
    log(&format!("正在读取 {}", src.display()));
    let bytes = std::fs::read(src).map_err(|e| format!("读文件失败: {e}"))?;
    install_history_bytes(&bytes, dest_db, log)
}

fn install_history_bytes(bytes: &[u8], dest_db: &str, log: &dyn Fn(&str)) -> Result<(), String> {
    let decoded: Vec<u8> = if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        log("正在解压 gzip…");
        let mut d = GzDecoder::new(bytes);
        let mut out = Vec::new();
        d.read_to_end(&mut out).map_err(|e| format!("解压失败: {e}"))?;
        out
    } else {
        bytes.to_vec()
    };
    if decoded.len() < 16 || !decoded.starts_with(b"SQLite format 3") {
        return Err("这不是 SQLite 数据库（解压后文件头不对）".into());
    }
    let dest = PathBuf::from(dest_db);
    if dest.exists() {
        let bak = dest.with_extension("db.bak");
        log(&format!("当前库备份为 {}", bak.display()));
        std::fs::copy(&dest, &bak).map_err(|e| format!("备份失败: {e}"))?;
    }
    if let Some(parent) = dest.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&dest, &decoded).map_err(|e| format!("写入历史库失败: {e}"))?;
    let _ = std::fs::remove_file(format!("{dest_db}-wal"));
    let _ = std::fs::remove_file(format!("{dest_db}-shm"));
    match Db::open(dest_db) {
        Ok(db) => {
            let date = db.resume_candidate_date().unwrap_or_else(|| "（空）".into());
            log(&format!(
                "历史库已就绪。数据最多的交易日={date}。请保持「继续」，再点开始抓取。"
            ));
        }
        Err(e) => return Err(format!("历史库写好了但打不开: {e}")),
    }
    Ok(())
}
