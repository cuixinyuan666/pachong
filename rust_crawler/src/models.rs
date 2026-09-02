//! 百度四接口解析结果的数据结构（与 http.rs / db.rs 字段一一对应）。

#[derive(Debug, Clone, Default)]
pub struct StockRef {
    pub code: String,
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct Scores {
    pub synthesis_rating: Option<String>,
    pub technology: Option<f64>,
    pub capital: Option<f64>,
    pub market: Option<f64>,
    pub finance: Option<f64>,
    pub is_new: Option<String>,
    pub update_time: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SupportResistance {
    pub cycle: String,
    pub support_level: Option<String>,
    pub resistance_level: Option<String>,
    pub level_desc: Option<String>,
    pub rating_text: Option<String>,
    pub rating_level: Option<String>,
    pub rating_status: Option<String>,
    pub bullish_events: Option<String>,
    pub bearish_events: Option<String>,
    pub rank_str: Option<String>,
    pub industry_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct FundFlow {
    pub super_net: Option<f64>,
    pub large_net: Option<f64>,
    pub medium_net: Option<f64>,
    pub little_net: Option<f64>,
    pub super_rate: Option<String>,
    pub large_rate: Option<String>,
    pub medium_rate: Option<String>,
    pub little_rate: Option<String>,
    pub main_net: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct Vote {
    pub vote_up: Option<String>,
    pub vote_down: Option<String>,
    pub total_num: Option<String>,
    pub vote_up_rate: Option<String>,
    pub vote_down_rate: Option<String>,
    pub week_up: Option<String>,
    pub week_down: Option<String>,
    pub week_rate: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct StockResult {
    pub name: Option<String>,
    pub scores: Scores,
    pub support: Vec<SupportResistance>,
    pub fund_flow: Option<FundFlow>,
    pub vote: Option<Vote>,
}
