//! HTTP 客户端、URL 构造、JSON 解析、频率限制器。
//! 严格移植自 Python 版 baidu_finance_ai_crawler.py 的字段路径与反爬逻辑。

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, COOKIE, ORIGIN, REFERER, USER_AGENT};
use serde_json::Value;
use std::sync::OnceLock;

use crate::models::*;
use crate::state::AppState;

/// 运行时 Cookie 头（Selenium B 方案刷新后写入；与 Python `_BAIDU_COOKIE_DICT` 对齐）
static LIVE_COOKIE: OnceLock<Mutex<String>> = OnceLock::new();

fn live_cookie_slot() -> &'static Mutex<String> {
    LIVE_COOKIE.get_or_init(|| Mutex::new(String::new()))
}

pub fn set_live_cookie_header(cookie: &str) {
    if let Ok(mut g) = live_cookie_slot().lock() {
        *g = cookie.to_string();
    }
}

pub fn load_cookies_from_json_file(path: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    let obj = v.as_object()?;
    let parts: Vec<String> = obj
        .iter()
        .filter_map(|(k, val)| val.as_str().map(|s| format!("{}={}", k, s)))
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("; "))
    }
}

pub const API_HOST: &str = "https://finance.pae.baidu.com";
pub const PAGE_HOST: &str = "https://finance.baidu.com";

/// 浏览器 UA 画像：含 UA 串 + 对应的 sec-ch-ua 客户端提示头（现代 Chrome 必带，Safari 不发送）。
/// 缺这组头是「非浏览器」的明显特征，补上后指纹更接近真实 Chrome。
pub struct UaProfile {
    pub ua: &'static str,
    pub sec_ch_ua: &'static str,
    pub platform: &'static str,
}

pub const UA_PROFILES: &[UaProfile] = &[
    UaProfile {
        ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        sec_ch_ua: "\"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\", \"Not?A_Brand\";v=\"24\"",
        platform: "\"Windows\"",
    },
    UaProfile {
        ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        sec_ch_ua: "\"Chromium\";v=\"121\", \"Google Chrome\";v=\"121\", \"Not?A_Brand\";v=\"24\"",
        platform: "\"Windows\"",
    },
    UaProfile {
        ua: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        sec_ch_ua: "\"Chromium\";v=\"121\", \"Google Chrome\";v=\"121\", \"Not?A_Brand\";v=\"24\"",
        platform: "\"Linux\"",
    },
    // Safari 不发送 sec-ch-ua 系列头，留空即可
    UaProfile {
        ua: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        sec_ch_ua: "",
        platform: "\"macOS\"",
    },
];

/// 随机挑一个 UA 画像（含其 sec-ch-ua 配对），整只股票的四接口复用同一画像。
pub fn random_ua() -> &'static UaProfile {
    &UA_PROFILES[rand::random::<usize>() % UA_PROFILES.len()]
}

/// 构造浏览器化请求头；带上 sec-ch-ua 客户端提示头（Safari 画像 sec_ch_ua 为空则跳过）。
fn build_headers(referer: &str, p: &UaProfile) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(USER_AGENT, HeaderValue::from_static(p.ua));
    h.insert(ACCEPT, HeaderValue::from_static("application/json, text/plain, */*"));
    h.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9,en;q=0.8"));
    h.insert(REFERER, HeaderValue::from_str(referer).unwrap());
    h.insert(ORIGIN, HeaderValue::from_static(PAGE_HOST));
    h.insert(HeaderName::from_static("x-requested-with"), HeaderValue::from_static("XMLHttpRequest"));
    h.insert(HeaderName::from_static("accept-encoding"), HeaderValue::from_static("gzip, deflate, br"));
    h.insert(HeaderName::from_static("connection"), HeaderValue::from_static("keep-alive"));
    h.insert(HeaderName::from_static("sec-fetch-dest"), HeaderValue::from_static("empty"));
    h.insert(HeaderName::from_static("sec-fetch-mode"), HeaderValue::from_static("cors"));
    h.insert(HeaderName::from_static("sec-fetch-site"), HeaderValue::from_static("cross-site"));
    if !p.sec_ch_ua.is_empty() {
        h.insert(HeaderName::from_static("sec-ch-ua"), HeaderValue::from_static(p.sec_ch_ua));
        h.insert(HeaderName::from_static("sec-ch-ua-mobile"), HeaderValue::from_static("?0"));
        h.insert(HeaderName::from_static("sec-ch-ua-platform"), HeaderValue::from_static(p.platform));
    }
    if let Ok(g) = live_cookie_slot().lock() {
        if !g.is_empty() {
            if let Ok(hv) = HeaderValue::from_str(&g) {
                h.insert(COOKIE, hv);
            }
        }
    }
    h
}

// --------------------------- URL 构造 --------------------------- //

pub fn build_analysis_url(code: &str) -> String {
    format!("{}/vapi/v1/analysis?code={}&market=ab&financeType=stock", API_HOST, code)
}

pub fn build_kline_url(code: &str, cycle: &str) -> String {
    format!("{}/sapi/v1/get_analyse?code={}&market=ab&financeType=stock&cycle={}", API_HOST, code, cycle)
}

pub fn build_fundflow_url(code: &str) -> String {
    format!("{}/vapi/v1/fundflow?finance_type=stock&code={}&market=ab&fund_flow_type=", API_HOST, code)
}

pub fn build_vote_url(code: &str) -> String {
    format!("{}/vapi/v1/stockvoterecords?code={}&market=ab&finance_type=stock", API_HOST, code)
}

pub fn build_page_referer(code: &str) -> String {
    format!("{}/ai-tech-analysi/stock/ab-{}", PAGE_HOST, code)
}

pub fn build_tab_referer(code: &str, tab: &str) -> String {
    format!("{}/stock/ab-{}?mainTab={}", PAGE_HOST, code, urlencoding::encode(tab))
}

// --------------------------- 频率限制器 --------------------------- //

pub fn now_secs() -> f64 {
    std::time::Instant::now().elapsed().as_secs_f64()
}

pub fn sleep_secs(s: f64) {
    if s > 0.0 {
        thread::sleep(Duration::from_secs_f64(s));
    }
}

pub struct RateLimiter {
    min_interval: f64,
    max_per_minute: usize,
    jitter: f64,
    /// 达每分钟上限时最多等待秒数；None=等满窗口剩余；Some(0)=立即开新窗口
    rate_wait_cap: Option<f64>,
    rate_window_sec: f64,
    interval_jitter_extra: f64,
    micro_break_every: usize,
    micro_break_min: f64,
    micro_break_max: f64,
    last: f64,
    window_start: f64,
    count: usize,
    req_count: usize,
}

impl RateLimiter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        min_interval: f64,
        max_per_minute: usize,
        jitter: f64,
        interval_jitter_extra: f64,
        micro_break_every: usize,
        micro_break_min: f64,
        micro_break_max: f64,
    ) -> Self {
        Self::new_ex(
            min_interval,
            max_per_minute,
            jitter,
            interval_jitter_extra,
            micro_break_every,
            micro_break_min,
            micro_break_max,
            Some(15.0),
            60.0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_ex(
        min_interval: f64,
        max_per_minute: usize,
        jitter: f64,
        interval_jitter_extra: f64,
        micro_break_every: usize,
        micro_break_min: f64,
        micro_break_max: f64,
        rate_wait_cap: Option<f64>,
        rate_window_sec: f64,
    ) -> Self {
        let t = now_secs();
        Self {
            min_interval,
            max_per_minute,
            jitter,
            rate_wait_cap,
            rate_window_sec: if rate_window_sec > 0.0 {
                rate_window_sec
            } else {
                60.0
            },
            interval_jitter_extra,
            micro_break_every,
            micro_break_min,
            micro_break_max,
            last: t,
            window_start: t,
            count: 0,
            req_count: 0,
        }
    }

    pub fn wait(&mut self) {
        let now = now_secs();
        let extra = if self.interval_jitter_extra > 0.0 {
            rand::random::<f64>() * self.interval_jitter_extra
        } else {
            0.0
        };
        let need = self.min_interval + extra;
        let elapsed = now - self.last;
        if elapsed < need {
            sleep_secs(need - elapsed);
        }
        let now = now_secs();
        if now - self.window_start >= self.rate_window_sec {
            self.window_start = now;
            self.count = 0;
        }
        if self.count >= self.max_per_minute {
            let mut wait_to = (self.window_start + self.rate_window_sec) - now;
            if let Some(cap) = self.rate_wait_cap {
                if cap >= 0.0 {
                    wait_to = wait_to.min(cap);
                }
            }
            if wait_to > 0.0 {
                sleep_secs(wait_to);
            }
            self.window_start = now_secs();
            self.count = 0;
        }
        self.last = now_secs();
        self.count += 1;
        self.req_count += 1;
        if self.jitter > 0.0 {
            sleep_secs(rand::random::<f64>() * self.jitter);
        }
        if self.micro_break_every > 0 && self.req_count % self.micro_break_every == 0 {
            let span = (self.micro_break_max - self.micro_break_min).max(0.0);
            let mb = self.micro_break_min + rand::random::<f64>() * span;
            if mb > 0.0 {
                sleep_secs(mb);
            }
        }
    }
}

// --------------------------- 抓取（含重试/限流/429/403） --------------------------- //

pub fn fetch_json(
    client: &Client,
    url: &str,
    headers: &HeaderMap,
    limiter: &mut RateLimiter,
    max_retries: u32,
    timeout: u64,
) -> Result<Value, String> {
    let mut last_err = String::new();
    for attempt in 1..=max_retries {
        limiter.wait();
        let resp = client
            .get(url)
            .headers(headers.clone())
            .timeout(Duration::from_secs(timeout))
            .send();
        match resp {
            Ok(r) => {
                let status = r.status();
                if status.as_u16() == 429 {
                    let ra = r
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(2.0);
                    sleep_secs(ra);
                    last_err = "HTTP 429 限流".into();
                    continue;
                }
                if status.is_server_error() {
                    let backoff = 2.0_f64.powi((attempt as i32) - 1) + rand::random::<f64>();
                    sleep_secs(backoff);
                    last_err = format!("HTTP {}", status);
                    continue;
                }
                if !status.is_success() {
                    // 403 等客户端错误直接抛出，由上层判断冷却
                    return Err(format!("HTTP {}", status.as_u16()));
                }
                // 读取文本以检测「反爬验证页」（③）：部分站点返回 200 但 body 是验证码/人机验证页。
                let text = match r.text() {
                    Ok(t) => t,
                    Err(e) => {
                        let backoff = 2.0_f64.powi((attempt as i32) - 1) + rand::random::<f64>();
                        sleep_secs(backoff);
                        last_err = format!("读取响应失败: {}", e);
                        continue;
                    }
                };
                if text.contains("验证码")
                    || text.contains("安全验证")
                    || text.contains("请完成")
                    || text.contains("captcha")
                    || text.contains("Captcha")
                    || text.contains("Verification")
                {
                    // 命中挑战页：上层按 403 类处理（冷却 + 轮换 UA/代理）
                    sleep_secs(3.0 + rand::random::<f64>() * 5.0);
                    last_err = "命中反爬验证页(CHALLENGE)".into();
                    continue;
                }
                let data: Value = match serde_json::from_str(&text) {
                    Ok(d) => d,
                    Err(e) => {
                        let backoff = 2.0_f64.powi((attempt as i32) - 1) + rand::random::<f64>();
                        sleep_secs(backoff);
                        last_err = format!("JSON 解析失败: {}", e);
                        continue;
                    }
                };
                if let Some(rc) = data.get("ResultCode") {
                    let s = match rc {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        _ => String::new(),
                    };
                    if s != "0" {
                        return Err(format!("API 业务错误 ResultCode={}", s));
                    }
                }
                return Ok(data);
            }
            Err(e) => {
                let backoff = 2.0_f64.powi((attempt as i32) - 1) + rand::random::<f64>();
                sleep_secs(backoff);
                last_err = format!("网络错误: {}", e);
                continue;
            }
        }
    }
    Err(format!("抓取失败，已重试 {} 次: {}", max_retries, last_err))
}

// --------------------------- Cookie 预热（①） --------------------------- //

/// 预热：先抓一次个股落地页 HTML，让 Baidu 下发 BAIDUID 等 cookie；
/// 后续 API 请求带上这套会话，避免「无会话」特征被识别为自动化。
/// 失败不影响主流程（仅丢了一次预热机会）。
pub fn warm_up(client: &Client, code: &str) {
    let referer = build_page_referer(code);
    let ua = random_ua();
    let headers = build_headers(&referer, ua);
    let url = format!("{}/ai-tech-analysi/stock/ab-{}", PAGE_HOST, code);
    if let Ok(resp) = client
        .get(&url)
        .headers(headers)
        .timeout(Duration::from_secs(10))
        .send()
    {
        // 丢弃 body，仅为了让 cookie jar 记录 Set-Cookie
        let _ = resp.bytes();
    }
}

// --------------------------- 解析 --------------------------- //

/// 取接口的 Result 节点（data["Result"]）。
pub fn result_of(data: &Value) -> &Value {
    data.get("Result").unwrap_or(&Value::Null)
}

fn opt_string(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn opt_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.replace(['+', '%'], "").trim().parse::<f64>().ok(),
        _ => None,
    }
}

pub fn parse_analysis(result: &Value, stock_name: &str) -> StockResult {
    let syn = result.get("synthesisScore").and_then(|v| v.as_object());
    let tech = result.get("technologyScore");
    let cap = result.get("capitalScore");
    let mkt = result.get("marketScore");
    let fin = result.get("financeScore");

    let name = syn
        .and_then(|o| opt_string(o.get("stockName")))
        .or_else(|| {
            if stock_name.is_empty() {
                None
            } else {
                Some(stock_name.to_string())
            }
        })
        .or_else(|| {
            tech.and_then(|v| v.get("increase"))
                .and_then(|v| v.get("items"))
                .and_then(|v| v.get(0))
                .and_then(|v| opt_string(v.get("text")))
        });

    let mut out = StockResult::default();
    out.name = name;
    out.scores.synthesis_rating = syn.and_then(|o| opt_string(o.get("rating")));
    out.scores.technology = tech.and_then(|v| opt_f64(v.get("score")));
    out.scores.capital = cap.and_then(|v| opt_f64(v.get("score")));
    out.scores.market = mkt.and_then(|v| opt_f64(v.get("score")));
    out.scores.finance = fin.and_then(|v| opt_f64(v.get("score")));
    out.scores.is_new = result.get("isNew").and_then(|v| opt_string(Some(v)));
    out.scores.update_time = syn.and_then(|o| opt_string(o.get("updateTime")));
    out
}

pub fn parse_kline(result: &Value, cycle: &str) -> Option<SupportResistance> {
    let li = result.get("levelInfo");
    let rt = result.get("rating");
    let rk = result.get("rank");

    let support = opt_string(li.and_then(|v| v.get("supportLevel")));
    let resistance = opt_string(li.and_then(|v| v.get("resistanceLevel")));
    let level_desc = opt_string(li.and_then(|v| v.get("desc")));
    let rating_text = opt_string(rt.and_then(|v| v.get("text")));
    let rating_level = opt_string(rt.and_then(|v| v.get("level")));
    let rating_status = opt_string(rt.and_then(|v| v.get("status")));
    let bullish = opt_string(rt.and_then(|v| v.get("bullish")));
    let bearish = opt_string(rt.and_then(|v| v.get("bearish")));

    let rank_str = match rk {
        Some(_r) => {
            let name = opt_string(rk.and_then(|v| v.get("name"))).unwrap_or_default();
            let val = opt_string(rk.and_then(|v| v.get("rankvalue"))).unwrap_or_default();
            if name.is_empty() && val.is_empty() {
                None
            } else {
                Some(format!("{} {}", name, val))
            }
        }
        None => None,
    };
    let industry = opt_string(rk.and_then(|v| v.get("industryName")));

    if support.is_none() && resistance.is_none() && rating_text.is_none() {
        return None;
    }
    Some(SupportResistance {
        cycle: cycle.to_string(),
        support_level: support,
        resistance_level: resistance,
        level_desc,
        rating_text,
        rating_level,
        rating_status,
        bullish_events: bullish,
        bearish_events: bearish,
        rank_str,
        industry_name: industry,
    })
}

pub fn parse_fundflow(content: &Value) -> Option<FundFlow> {
    let fs = content.get("fundFlowSpread").and_then(|v| v.get("result"))?;
    let grp = |k: &str| fs.get(k).and_then(|v| v.as_object());
    let num = |o: Option<&serde_json::Map<String, Value>>| o.and_then(|m| opt_f64(m.get("netTurnover")));
    let rate = |o: Option<&serde_json::Map<String, Value>>| o.and_then(|m| opt_string(m.get("turnoverInRate")));

    let super_g = grp("superGrp");
    let large_g = grp("largeGrp");
    let medium_g = grp("mediumGrp");
    let little_g = grp("littleGrp");

    let super_net = num(super_g);
    let large_net = num(large_g);
    let medium_net = num(medium_g);
    let little_net = num(little_g);

    let main_net = match (super_net, large_net) {
        (Some(a), Some(b)) => Some(a + b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };

    Some(FundFlow {
        super_net,
        large_net,
        medium_net,
        little_net,
        super_rate: rate(super_g),
        large_rate: rate(large_g),
        medium_rate: rate(medium_g),
        little_rate: rate(little_g),
        main_net,
    })
}

pub fn parse_vote(root: &Value) -> Option<Vote> {
    let records = root.get("voteRecords")?;
    let periods = records.get("voteRes").and_then(|v| v.as_array());
    let mut week_up = None;
    let mut week_down = None;
    let mut week_rate = None;
    if let Some(arr) = periods {
        for p in arr {
            if p.get("title").and_then(|v| v.as_str()) == Some("本周") {
                week_up = opt_string(p.get("voteUp"));
                week_down = opt_string(p.get("voteDown"));
                week_rate = opt_string(p.get("voteUpRate"));
            }
        }
    }
    Some(Vote {
        vote_up: opt_string(root.get("voteUp")),
        vote_down: opt_string(root.get("voteDown")),
        total_num: opt_string(root.get("totalNum")),
        vote_up_rate: opt_string(root.get("voteUpRate")),
        vote_down_rate: opt_string(root.get("voteDownRate")),
        week_up,
        week_down,
        week_rate,
    })
}

/// 单支股票的四接口抓取 + 解析。任意接口返回 403 会向上抛出（含 "403" 字样）。
pub fn crawl_one(
    client: &Client,
    limiter: &mut RateLimiter,
    stock: &StockRef,
    config: &crate::crawler::CrawlConfig,
    state: &Arc<Mutex<AppState>>,
) -> Result<StockResult, String> {
    let page_referer = build_page_referer(&stock.code);
    let ua = random_ua();
    let headers = build_headers(&page_referer, ua);
    let url = build_analysis_url(&stock.code);
    if let Ok(mut g) = state.lock() {
        g.current_endpoint = "评分".into();
    }
    let data = fetch_json(client, &url, &headers, limiter, config.max_retries, config.timeout)?;
    let result = result_of(&data);
    if result.is_null() {
        return Err("分析接口返回空 Result".into());
    }
    let mut out = parse_analysis(result, &stock.name);

    for cyc in ["long", "short"] {
        if let Ok(mut g) = state.lock() {
            g.current_endpoint = format!("支撑阻力({})", cyc);
        }
        let kurl = build_kline_url(&stock.code, cyc);
        match fetch_json(client, &kurl, &headers, limiter, config.max_retries, config.timeout) {
            Ok(kd) => {
                if let Some(sr) = parse_kline(result_of(&kd), cyc) {
                    out.support.push(sr);
                }
            }
            Err(e) => {
                if let Ok(mut g) = state.lock() {
                    g.push_log(format!("  支撑阻力({}) 失败: {}", cyc, e));
                }
            }
        }
    }

    if let Ok(mut g) = state.lock() {
        g.current_endpoint = "资金流向".into();
    }
    let furl = build_fundflow_url(&stock.code);
    let fheaders = build_headers(&build_tab_referer(&stock.code, "资金"), ua);
    if let Ok(fd) = fetch_json(client, &furl, &fheaders, limiter, config.max_retries, config.timeout) {
        let froot = result_of(&fd);
        let inner = froot.get("Result").unwrap_or(froot);
        let content = inner.get("content").unwrap_or(&Value::Null);
        out.fund_flow = parse_fundflow(content);
    }

    if let Ok(mut g) = state.lock() {
        g.current_endpoint = "投票".into();
    }
    let vurl = build_vote_url(&stock.code);
    let vheaders = build_headers(&build_tab_referer(&stock.code, "股评"), ua);
    if let Ok(vd) = fetch_json(client, &vurl, &vheaders, limiter, config.max_retries, config.timeout) {
        out.vote = parse_vote(result_of(&vd));
    }

    if let Ok(mut g) = state.lock() {
        g.current_endpoint = "完成".into();
    }
    Ok(out)
}
