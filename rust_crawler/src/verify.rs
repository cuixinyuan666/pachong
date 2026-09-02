//! 源站核对链接：未拿到/失败 ≠ 源站确认无数据，给人打开网页对照。

#[derive(Debug, Clone)]
pub struct VerifyLink {
    pub source: String,
    pub kind: String, // page=给人看；request=本次接口
    pub label: String,
    pub url: String,
}

pub fn baidu_ai_page_url(code: &str) -> String {
    format!("https://finance.baidu.com/ai-tech-analysi/stock/ab-{code}")
}

pub fn baidu_stock_page_url(code: &str) -> String {
    format!("https://finance.baidu.com/stock/ab-{code}")
}

pub fn baidu_analysis_api_url(code: &str) -> String {
    format!(
        "https://finance.pae.baidu.com/vapi/v1/analysis?code={code}&market=ab&financeType=stock"
    )
}

pub fn em_stock_page_url(code: &str) -> String {
    format!("https://data.eastmoney.com/stockcomment/stock/{code}.html")
}

pub fn em_list_page_url() -> String {
    "https://data.eastmoney.com/stockcomment/".into()
}

pub fn source_verify_links(code: &str, sources: &[&str]) -> Vec<VerifyLink> {
    let mut out = Vec::new();
    let srcs: Vec<String> = sources.iter().map(|s| s.to_lowercase()).collect();
    if srcs.iter().any(|s| s == "baidu") {
        out.push(VerifyLink {
            source: "百度财经".into(),
            kind: "page".into(),
            label: "AI分析页".into(),
            url: baidu_ai_page_url(code),
        });
        out.push(VerifyLink {
            source: "百度财经".into(),
            kind: "page".into(),
            label: "个股页".into(),
            url: baidu_stock_page_url(code),
        });
        out.push(VerifyLink {
            source: "百度财经".into(),
            kind: "request".into(),
            label: "五维评分接口".into(),
            url: baidu_analysis_api_url(code),
        });
    }
    if srcs.iter().any(|s| s == "em") {
        out.push(VerifyLink {
            source: "东方财富".into(),
            kind: "page".into(),
            label: "千股千评个股页".into(),
            url: em_stock_page_url(code),
        });
        out.push(VerifyLink {
            source: "东方财富".into(),
            kind: "page".into(),
            label: "千股千评列表页".into(),
            url: em_list_page_url(),
        });
    }
    out
}

pub fn classify_kind(detail: &str) -> String {
    let d = detail.to_lowercase();
    if detail.contains("网络错误")
        || d.contains("dns")
        || d.contains("timed out")
        || d.contains("timeout")
        || d.contains("connect")
        || d.contains("connection")
        || d.contains("proxy")
        || d.contains("tls")
        || d.contains("ssl")
        || d.contains("certificate")
        || d.contains("证书")
    {
        return "网络错误".into();
    }
    if detail.contains("403") || d.contains("challenge") || d.contains("forbidden") || detail.contains("验证页") {
        return "反爬拦截".into();
    }
    if detail.contains("未拿到") || detail.contains("空壳") {
        return "未拿到数据".into();
    }
    if detail.contains("完整性") || detail.contains("缺") && detail.contains("项") {
        return "数据不完整".into();
    }
    if detail.contains("JSON") || detail.contains("解析") {
        return "解析失败".into();
    }
    if detail.contains("HTTP") {
        return "HTTP错误".into();
    }
    if detail.contains("保存") || detail.contains("落库") || detail.contains("数据库") {
        return "落库失败".into();
    }
    "其它错误".into()
}

/// 新电脑全是「网络错误」时的对照说明（弹窗里展示）。
pub fn network_hint() -> &'static str {
    "新电脑常见原因（请逐项核对）：\n\
     1) Windows 防火墙第一次拦截了本 exe（设置里放行专用/公用网络）\n\
     2) 本机开了 Clash/系统代理，旧版程序不会自动走代理（本版会读系统代理）\n\
     3) 用浏览器能打开源站、但本程序不行 → 多半是防火墙只拦了本程序\n\
     4) Cookie 缺失通常是 HTTP 403，不是「网络错误」；可把 baidu_cookies.json 放到 exe 旁\n\
     点下方源站链接：若浏览器也打不开，是这台电脑的网络本身到不了数据源。"
}
