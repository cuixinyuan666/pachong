//! 把本地 market_data.db 发到 Telegram（免费、官方 Bot API，不当成网盘账号登录）。
//! 国内直连常被拦：发送前请开 VPN，联网方式选「走系统代理」。
//!
//! 机器人下载单文件约 20MB：压缩后拆成多包；简介里只记当前这一组的清单编号。
//! 任意机器重发：先上传新组，再删旧组消息；成功后删除本机库，唯一一份在 Telegram。

use std::fs::File;
use std::io::{copy, BufReader, Read};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::settings::workspace_dir;

/// 略小于 getFile 20MB 上限，避免差几个字节下不下来。
const PART_MAX: usize = 19 * 1024 * 1024;
const MANIFEST_MARK: &str = "MANIFEST=";
const FILE_ID_MARK: &str = "FILEID=";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelegramCfg {
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: String,
    /// 当前这一组的清单文件编号（简介里也有一份）。
    #[serde(default)]
    pub last_file_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DbPart {
    i: u32,
    file_id: String,
    message_id: i64,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DbSet {
    v: u32,
    kind: String,
    total_bytes: u64,
    created: String,
    parts: Vec<DbPart>,
    #[serde(default)]
    manifest_message_id: i64,
}

enum PublishedPtr {
    Manifest(String),
    LegacyFile(String),
}

struct SentDoc {
    file_id: String,
    message_id: i64,
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

/// 先把 WAL 刷进主库，gzip 后按约 19MB 分包发出；成功后删旧组、再删本机库。
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
    let nparts = ((size + PART_MAX as u64 - 1) / PART_MAX as u64).max(1);
    log(&format!(
        "压缩后 {:.1} MB，将拆成 {nparts} 个包（每包不超过 19MB）",
        size as f64 / (1024.0 * 1024.0)
    ));

    let client = crate::http::build_blocking_client(300)?;
    let old_msg_ids = collect_old_message_ids(&client, token, log);

    log("正在发给 Telegram…（国内请先开 VPN）");
    let stamp = chrono::Local::now().format("%Y-%m-%d %H:%M").to_string();
    let mut new_parts: Vec<DbPart> = Vec::new();
    let mut uploaded: Vec<i64> = Vec::new();
    let mut gz = File::open(&gz_path).map_err(|e| format!("读压缩包失败: {e}"))?;
    let mut buf = vec![0u8; PART_MAX];
    let mut idx: u32 = 0;
    loop {
        let n = gz.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        let caption = format!("MarketPulse 库 {}/{}  {}\n发完后只保留这一组", idx + 1, nparts, stamp);
        let fname = format!("mp_db.part{idx:02}.bin");
        match send_document(&client, token, chat, &fname, buf[..n].to_vec(), caption) {
            Ok(doc) => {
                log(&format!("已上传第 {}/{} 包", idx + 1, nparts));
                uploaded.push(doc.message_id);
                new_parts.push(DbPart {
                    i: idx,
                    file_id: doc.file_id,
                    message_id: doc.message_id,
                    size: n as u64,
                });
            }
            Err(e) => {
                let _ = std::fs::remove_file(&gz_path);
                cleanup_messages(&client, token, chat, &uploaded, log);
                return Err(e);
            }
        }
        idx += 1;
        thread::sleep(Duration::from_millis(200));
    }
    drop(gz);
    let _ = std::fs::remove_file(&gz_path);
    if new_parts.is_empty() {
        return Err("压缩包是空的，没有可发的内容".into());
    }

    let set = DbSet {
        v: 2,
        kind: "gz-parts".into(),
        total_bytes: new_parts.iter().map(|p| p.size).sum(),
        created: stamp,
        parts: new_parts,
        manifest_message_id: 0,
    };
    let man_bytes = serde_json::to_vec(&set).map_err(|e| e.to_string())?;
    let man_doc = match send_document(
        &client,
        token,
        chat,
        "mp_db.manifest.json",
        man_bytes,
        "MarketPulse 库清单（请用程序下载，勿改）".into(),
    ) {
        Ok(d) => d,
        Err(e) => {
            cleanup_messages(&client, token, chat, &uploaded, log);
            return Err(e);
        }
    };
    uploaded.push(man_doc.message_id);

    let mut stored = cfg.clone();
    stored.last_file_id = man_doc.file_id.clone();
    let _ = save(&stored);
    match publish_pointer(
        &client,
        token,
        &synced_description(&man_doc.file_id, &uploaded),
    ) {
        Ok(()) => log("已切换到新的一组库（旧组即将从对话里删掉）。"),
        Err(e) => {
            cleanup_messages(&client, token, chat, &uploaded, log);
            return Err(format!("新库已上传，但切换清单失败，已撤回新包: {e}"));
        }
    }

    let old_only: Vec<i64> = old_msg_ids
        .into_iter()
        .filter(|id| !uploaded.contains(id))
        .collect();
    if !old_only.is_empty() {
        log("正在删除 Telegram 里的旧组…");
        cleanup_messages(&client, token, chat, &old_only, log);
    }

    match remove_local_database(db_path, log) {
        Ok(()) => {}
        Err(e) => {
            log(&format!("{e}"));
            return Err(e);
        }
    }
    Ok(())
}

fn synced_description(file_id: &str, msg_ids: &[i64]) -> String {
    let msgs = msg_ids
        .iter()
        .copied()
        .filter(|id| *id != 0)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let full = format!(
        "MarketPulse 历史库同步（程序自动维护，请勿改） {MANIFEST_MARK}{file_id} MSGS={msgs}"
    );
    if full.chars().count() <= 512 {
        return full;
    }
    // 简介最多 512 字：只留下清单那条消息号，分片号在 JSON 里。
    let last = msg_ids.iter().copied().rev().find(|id| *id != 0).unwrap_or(0);
    format!("MarketPulse 历史库同步（程序自动维护，请勿改） {MANIFEST_MARK}{file_id} MSGS={last}")
}

fn parse_msg_ids(desc: &str) -> Vec<i64> {
    let Some(idx) = desc.find("MSGS=") else {
        return Vec::new();
    };
    desc[idx + 5..]
        .split_whitespace()
        .next()
        .unwrap_or("")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|id: &i64| *id != 0)
        .collect()
}

fn parse_mark(desc: &str, mark: &str) -> Option<String> {
    let idx = desc.find(mark)?;
    let id = desc[idx + mark.len()..]
        .split_whitespace()
        .next()?
        .trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

fn parse_published_ptr(desc: &str) -> Option<PublishedPtr> {
    if let Some(id) = parse_mark(desc, MANIFEST_MARK) {
        return Some(PublishedPtr::Manifest(id));
    }
    if let Some(id) = parse_mark(desc, FILE_ID_MARK) {
        return Some(PublishedPtr::LegacyFile(id));
    }
    None
}

fn publish_pointer(client: &reqwest::blocking::Client, token: &str, description: &str) -> Result<(), String> {
    let url = format!("https://api.telegram.org/bot{token}/setMyDescription");
    let resp = client
        .post(&url)
        .form(&[("description", description)])
        .send()
        .map_err(|e| e.to_string())?;
    let body = resp.text().unwrap_or_default();
    let ok = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("ok").and_then(|x| x.as_bool()))
        .unwrap_or(false);
    if !ok {
        return Err(body.chars().take(200).collect());
    }
    Ok(())
}

/// 简介里已有当前组则不动，避免本机旧编号覆盖另一台刚发的新组。
pub fn resync_published_file_id(log: &dyn Fn(&str)) {
    let cfg = load();
    let token = cfg.bot_token.trim();
    if token.is_empty() {
        return;
    }
    let client = match crate::http::build_blocking_client(30) {
        Ok(c) => c,
        Err(e) => {
            log(&format!("同步下载编号时联网失败: {e}"));
            return;
        }
    };
    match read_published_ptr(&client, token) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let fid = cfg.last_file_id.trim();
            if fid.is_empty() {
                return;
            }
            let desc = format!(
                "MarketPulse 历史库同步（程序自动维护，请勿改） {FILE_ID_MARK}{fid}"
            );
            match publish_pointer(&client, token, &desc) {
                Ok(()) => log("已把本机记下的编号补进 bot 简介。"),
                Err(e) => log(&format!("同步下载编号失败: {e}")),
            }
        }
        Err(e) => log(&format!("读 bot 简介失败: {e}")),
    }
}

fn read_description_text(
    client: &reqwest::blocking::Client,
    token: &str,
) -> Result<String, String> {
    let url = format!("https://api.telegram.org/bot{token}/getMyDescription");
    let body = client
        .get(&url)
        .send()
        .map_err(|e| e.to_string())?
        .text()
        .unwrap_or_default();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    if v.get("ok").and_then(|x| x.as_bool()) != Some(true) {
        return Err(body.chars().take(200).collect());
    }
    Ok(v.pointer("/result/description")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string())
}

fn read_published_ptr(
    client: &reqwest::blocking::Client,
    token: &str,
) -> Result<Option<PublishedPtr>, String> {
    Ok(parse_published_ptr(&read_description_text(client, token)?))
}

fn collect_old_message_ids(
    client: &reqwest::blocking::Client,
    token: &str,
    log: &dyn Fn(&str),
) -> Vec<i64> {
    let desc = match read_description_text(client, token) {
        Ok(s) => s,
        Err(e) => {
            log(&format!("读当前组失败（仍会发新组）: {e}"));
            return Vec::new();
        }
    };
    let mut ids = parse_msg_ids(&desc);
    match parse_published_ptr(&desc) {
        Some(PublishedPtr::Manifest(fid)) => match download_file(client, token, &fid) {
            Ok(bytes) => {
                if let Ok(set) = serde_json::from_slice::<DbSet>(&bytes) {
                    for p in set.parts {
                        if p.message_id != 0 && !ids.contains(&p.message_id) {
                            ids.push(p.message_id);
                        }
                    }
                    if set.manifest_message_id != 0 && !ids.contains(&set.manifest_message_id) {
                        ids.push(set.manifest_message_id);
                    }
                }
            }
            Err(e) => log(&format!("当前清单下载失败（仍会发新组）: {e}")),
        },
        Some(PublishedPtr::LegacyFile(_)) => {
            log("旧版是单文件，对话里那条需要你在 Telegram 里手动删。");
        }
        None => {}
    }
    ids
}

fn send_document(
    client: &reqwest::blocking::Client,
    token: &str,
    chat: &str,
    filename: &str,
    bytes: Vec<u8>,
    caption: String,
) -> Result<SentDoc, String> {
    let url = format!("https://api.telegram.org/bot{token}/sendDocument");
    let part = reqwest::blocking::multipart::Part::bytes(bytes)
        .file_name(filename.to_string())
        .mime_str("application/octet-stream")
        .map_err(|e| e.to_string())?;
    let form = reqwest::blocking::multipart::Form::new()
        .text("chat_id", chat.to_string())
        .text("caption", caption)
        .part("document", part);
    let resp = client
        .post(&url)
        .multipart(form)
        .send()
        .map_err(|e| format!("网络错误: {e}{}", crate::http::proxy_hint_suffix()))?;
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    if !status.is_success() {
        return Err(format!(
            "Telegram HTTP {status}: {}",
            body.chars().take(300).collect::<String>()
        ));
    }
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    if v.get("ok").and_then(|x| x.as_bool()) != Some(true) {
        return Err(format!(
            "Telegram 拒绝: {}",
            body.chars().take(300).collect::<String>()
        ));
    }
    let file_id = v
        .pointer("/result/document/file_id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| "响应里没有 file_id".to_string())?
        .to_string();
    let message_id = v
        .pointer("/result/message_id")
        .and_then(|x| x.as_i64())
        .unwrap_or(0);
    Ok(SentDoc { file_id, message_id })
}

fn delete_message(client: &reqwest::blocking::Client, token: &str, chat: &str, message_id: i64) {
    if message_id == 0 {
        return;
    }
    let url = format!("https://api.telegram.org/bot{token}/deleteMessage");
    let _ = client
        .post(&url)
        .form(&[
            ("chat_id", chat.to_string()),
            ("message_id", message_id.to_string()),
        ])
        .send();
}

fn cleanup_messages(
    client: &reqwest::blocking::Client,
    token: &str,
    chat: &str,
    ids: &[i64],
    log: &dyn Fn(&str),
) {
    for id in ids {
        delete_message(client, token, chat, *id);
        thread::sleep(Duration::from_millis(50));
    }
    if !ids.is_empty() {
        log(&format!("已请求删除 {} 条旧消息", ids.len()));
    }
}

fn download_file(
    client: &reqwest::blocking::Client,
    token: &str,
    file_id: &str,
) -> Result<Vec<u8>, String> {
    let encoded = urlencoding::encode(file_id);
    let gf = format!("https://api.telegram.org/bot{token}/getFile?file_id={encoded}");
    let gbody = client
        .get(&gf)
        .send()
        .map_err(|e| format!("getFile 失败: {e}{}", crate::http::proxy_hint_suffix()))?
        .text()
        .unwrap_or_default();
    if gbody.to_lowercase().contains("too big") || gbody.to_lowercase().contains("file is too big") {
        return Err("有分片超过机器人下载上限（约 20MB）".into());
    }
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
    Ok(bytes.to_vec())
}

fn remove_local_database(db_path: &str, log: &dyn Fn(&str)) -> Result<(), String> {
    for extra in ["-wal", "-shm", ""] {
        let p = if extra.is_empty() {
            db_path.to_string()
        } else {
            format!("{db_path}{extra}")
        };
        if Path::new(&p).exists() {
            std::fs::remove_file(&p).map_err(|e| {
                format!("Telegram 里已是最新一组，但本机库删不掉（{p}）: {e}。请先停止爬虫后手动删。")
            })?;
        }
    }
    log("本机数据库已删除，当前唯一一份在 Telegram。继续爬请先点「下载历史库」。");
    Ok(())
}

pub fn help_text() -> &'static str {
    "Telegram 是目前能直接从本程序发/收文件的免费通道（官方 Bot API）。\n\n\
     发库：\n\
     1) BotFather 建 bot，拿到 Token；给 bot 发「你好」，填 Chat ID\n\
     2) 停止爬虫后点「发送数据库」（国内先开 VPN，联网方式选走系统代理）\n\
     3) 程序会把库压成 gzip，再拆成多个约 19MB 的包发出去\n\
     4) 对话里只保留当前这一组；成功后本机 market_data.db 会删掉\n\
     5) 不要两台电脑同时点发送\n\n\
     下载后再接着爬：\n\
     1) 填同一套 Token / Chat ID\n\
     2) 点「下载历史库」：总是拉当前这一组，拼回去再解压\n\
     3) 保持「继续」，再点开始抓取（已抓过的会跳过）\n\
     也可以点「导入本地历史库」，选 U 盘/微信里的 .db 或 .gz"
}

/// 从 Telegram 取当前这一组历史库，覆盖到 db_path（先备份 .bak）。
pub fn download_latest_database(db_path: &str, cfg: &TelegramCfg, log: &dyn Fn(&str)) -> Result<(), String> {
    let token = cfg.bot_token.trim();
    if token.is_empty() {
        return Err("请先填 Bot Token".into());
    }
    let client = crate::http::build_blocking_client(300)?;

    log("正在读取当前这一组库的清单…");
    let ptr = match read_published_ptr(&client, token) {
        Ok(p) => p,
        Err(e) => {
            log(&format!("读 bot 简介失败: {e}"));
            None
        }
    };

    if let Some(PublishedPtr::Manifest(fid)) = &ptr {
        log("已找到当前组清单，开始按包下载。");
        return download_manifest_set(&client, token, fid, db_path, log);
    }

    let mut file_id = String::new();
    if let Some(PublishedPtr::LegacyFile(fid)) = ptr {
        log("检测到旧版单文件，按整包下载。");
        file_id = fid;
    }
    if file_id.is_empty() && !cfg.last_file_id.trim().is_empty() {
        file_id = cfg.last_file_id.trim().to_string();
        log("简介没有清单，改用本机记下的编号。");
    }
    if file_id.is_empty() {
        if let Some(fid) = file_id_from_updates(&client, token, cfg.chat_id.trim()) {
            log("对话里发现文件，改用这一份。");
            file_id = fid;
        }
    }
    if file_id.is_empty() {
        return Err("没有找到历史库。请先在任一台电脑点「发送数据库」。".into());
    }

    let bytes = download_file(&client, token, &file_id)?;
    if let Ok(set) = serde_json::from_slice::<DbSet>(&bytes) {
        return download_parts(&client, token, &set, db_path, log);
    }
    log(&format!("已下载 {:.1} MB", bytes.len() as f64 / (1024.0 * 1024.0)));
    install_history_bytes(&bytes, db_path, log)
}

fn download_manifest_set(
    client: &reqwest::blocking::Client,
    token: &str,
    manifest_id: &str,
    db_path: &str,
    log: &dyn Fn(&str),
) -> Result<(), String> {
    let bytes = download_file(client, token, manifest_id)?;
    let set: DbSet = serde_json::from_slice(&bytes)
        .map_err(|e| format!("清单不是 JSON: {e}"))?;
    download_parts(client, token, &set, db_path, log)
}

fn download_parts(
    client: &reqwest::blocking::Client,
    token: &str,
    set: &DbSet,
    db_path: &str,
    log: &dyn Fn(&str),
) -> Result<(), String> {
    let mut parts = set.parts.clone();
    parts.sort_by_key(|p| p.i);
    if parts.is_empty() {
        return Err("清单里没有分片".into());
    }
    let mut gz = Vec::with_capacity(set.total_bytes as usize);
    for (n, p) in parts.iter().enumerate() {
        log(&format!("正在下载第 {}/{} 包…", n + 1, parts.len()));
        let chunk = download_file(client, token, &p.file_id)?;
        if p.size > 0 && chunk.len() as u64 != p.size {
            log(&format!(
                "第 {} 包大小不符（清单 {}，实际 {}），仍继续拼接",
                n + 1,
                p.size,
                chunk.len()
            ));
        }
        gz.extend_from_slice(&chunk);
    }
    log(&format!(
        "各包已齐，共 {:.1} MB，开始解压",
        gz.len() as f64 / (1024.0 * 1024.0)
    ));
    install_history_bytes(&gz, db_path, log)
}

fn looks_like_history(name: &str, mime: &str) -> bool {
    let n = name.to_lowercase();
    let m = mime.to_lowercase();
    n.ends_with(".gz")
        || n.ends_with(".db")
        || n.ends_with(".json")
        || n.ends_with(".bin")
        || n.contains("market")
        || n.contains("mp_db")
        || m.contains("gzip")
        || m.contains("sqlite")
}

fn file_id_from_updates(client: &reqwest::blocking::Client, token: &str, want_chat: &str) -> Option<String> {
    let url = format!("https://api.telegram.org/bot{token}/getUpdates?limit=100");
    let body = client.get(&url).send().ok()?.text().ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    let arr = v.get("result")?.as_array()?;
    let mut best_id = i64::MIN;
    let mut file_id = String::new();
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
        }
    }
    if file_id.is_empty() {
        None
    } else {
        Some(file_id)
    }
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
