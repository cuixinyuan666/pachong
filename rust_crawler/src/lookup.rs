//! 按股票代码查询 market_data.db 各表最新一行，供界面展示。
//! 用独立只读连接，不抢爬虫那条写连接。

use std::time::Duration;

use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OptionalExtension, Row};

#[derive(Debug, Clone)]
pub struct Field {
    pub label: String,
    pub value: String,
    /// 可全市场排序时填目录 id；日期/长文等不可比的为空。
    pub sort_id: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub struct Section {
    pub title: String,
    /// 长文（诊断文字）整行铺开，不挤进两列。
    pub wide: bool,
    pub rows: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct StockSnapshot {
    pub code: String,
    pub name: String,
    pub found: bool,
    pub hint: String,
    pub sections: Vec<Section>,
}

impl StockSnapshot {
    pub fn as_text(&self) -> String {
        let mut s = format!("{} {}\n{}\n", self.code, self.name, self.hint);
        for sec in &self.sections {
            s.push('\n');
            s.push_str(&sec.title);
            s.push('\n');
            for f in &sec.rows {
                s.push_str(&format!("  {}  {}\n", f.label, f.value));
            }
        }
        s
    }
}

/// 只留 6 位数字代码：000001、1、sz000001、000001.SZ 都能认。
pub fn normalize_code(raw: &str) -> String {
    let t = raw.trim().to_uppercase();
    let t = t
        .trim_start_matches("SZ")
        .trim_start_matches("SH")
        .trim_start_matches("BJ")
        .trim_start_matches('.');
    let digits: String = t.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return String::new();
    }
    if digits.len() >= 6 {
        digits[digits.len() - 6..].to_string()
    } else {
        format!("{digits:0>6}")
    }
}

fn dash(s: &str) -> String {
    let t = s.trim();
    if t.is_empty() {
        "—".into()
    } else {
        t.to_string()
    }
}

fn cell(row: &Row, idx: usize) -> String {
    match row.get_ref(idx) {
        Ok(ValueRef::Null) => "—".into(),
        Ok(ValueRef::Integer(i)) => i.to_string(),
        Ok(ValueRef::Real(n)) => fmt_num(n),
        Ok(ValueRef::Text(t)) => dash(&String::from_utf8_lossy(t)),
        Ok(ValueRef::Blob(_)) => "—".into(),
        Err(_) => "—".into(),
    }
}

fn ctrl_degree(op: &str) -> String {
    if op == "—" {
        return "—".into();
    }
    let n: f64 = match op.parse() {
        Ok(v) => v,
        Err(_) => return "—".into(),
    };
    if n < 0.3 {
        "低度控盘".into()
    } else if n < 0.7 {
        "中度控盘".into()
    } else {
        "高度控盘".into()
    }
}

fn fmt_num(n: f64) -> String {
    if !n.is_finite() {
        return "—".into();
    }
    if n.abs() >= 1_0000.0 && n.abs() < 1.0e12 && (n.fract().abs() > 0.0 || n.abs() >= 1000.0) {
        // 市值等大数：大于等于 1 万用万/亿，便于一眼看
        if n.abs() >= 1.0e8 {
            return format!("{:.2} 亿", n / 1.0e8);
        }
        if n.abs() >= 1.0e4 {
            return format!("{:.2} 万", n / 1.0e4);
        }
    }
    let s = format!("{n:.4}");
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn kv(rows: &mut Vec<Field>, label: &str, value: String) {
    rows.push(Field {
        label: label.into(),
        value,
        sort_id: None,
    });
}

fn kvs(rows: &mut Vec<Field>, label: &str, value: String, sort_id: &'static str) {
    rows.push(Field {
        label: label.into(),
        value,
        sort_id: Some(sort_id),
    });
}

fn latest_row(
    conn: &Connection,
    sql: &str,
    code: &str,
) -> rusqlite::Result<Option<Vec<String>>> {
    conn.query_row(sql, params![code], |row| {
        let n = row.as_ref().column_count();
        let mut v = Vec::with_capacity(n);
        for i in 0..n {
            v.push(cell(row, i));
        }
        Ok(v)
    })
    .optional()
}

fn table_ok(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        params![name],
        |_| Ok(()),
    )
    .is_ok()
}

/// 打开库，取出该代码在各表里「最新」的一行。
pub fn lookup_stock(db_path: &str, raw_code: &str) -> Result<StockSnapshot, String> {
    let code = normalize_code(raw_code);
    if code.is_empty() {
        return Err("请输入股票代码（如 000001）".into());
    }
    if !std::path::Path::new(db_path).exists() {
        return Err(format!("找不到数据库文件：{db_path}"));
    }
    let conn = Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("打开数据库失败: {e}"))?;
    conn.busy_timeout(Duration::from_millis(8000))
        .map_err(|e| format!("设置等待锁超时失败: {e}"))?;
    let conn = &conn;

    let mut name = String::new();
    if table_ok(conn, "stocks") {
        if let Ok(Some(n)) = conn
            .query_row(
                "SELECT name FROM stocks WHERE code=?1",
                params![&code],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()
        {
            if let Some(n) = n {
                name = n;
            }
        }
    }

    let mut sections = Vec::new();
    let mut found = false;

    if table_ok(conn, "scores") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT trade_date, code, name, synthesis, technology, capital, market, finance, \
             is_new, update_time, crawl_date, status \
             FROM scores WHERE code=?1 ORDER BY COALESCE(update_time, trade_date) DESC LIMIT 1",
            &code,
        ) {
            found = true;
            if name.is_empty() {
                name = r.get(2).cloned().unwrap_or_default();
                if name == "—" {
                    name.clear();
                }
            }
            let mut rows = Vec::new();
            kv(&mut rows, "批次日", r[0].clone());
            kv(&mut rows, "分析日", r[9].clone());
            kv(&mut rows, "抓取日", r[10].clone());
            kv(&mut rows, "状态", r[11].clone());
            kv(&mut rows, "综合评级", r[3].clone());
            kvs(&mut rows, "技术", r[4].clone(), "scores.technology");
            kvs(&mut rows, "资金", r[5].clone(), "scores.capital");
            kvs(&mut rows, "市场", r[6].clone(), "scores.market");
            kvs(&mut rows, "财务", r[7].clone(), "scores.finance");
            kv(&mut rows, "是否次新", r[8].clone());
            sections.push(Section {
                title: "百度财经 · 综合评分".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "support_resistance") {
        let mut stmt = conn
            .prepare(
                "SELECT cycle, support_level, resistance_level, level_desc, rating_text, rating_level, \
                 rating_status, bullish_events, bearish_events, rank_str, industry_name, \
                 update_time, trade_date \
                 FROM support_resistance WHERE code=?1 ORDER BY COALESCE(update_time, trade_date) DESC",
            )
            .map_err(|e| e.to_string())?;
        let all: Vec<Vec<String>> = stmt
            .query_map(params![&code], |row| {
                let n = row.as_ref().column_count();
                let mut v = Vec::with_capacity(n);
                for i in 0..n {
                    v.push(cell(row, i));
                }
                Ok(v)
            })
            .map_err(|e| e.to_string())?
            .filter_map(|x| x.ok())
            .collect();
        let mut seen = std::collections::HashSet::new();
        for r in all {
            let cyc = r[0].clone();
            if !seen.insert(cyc.clone()) {
                continue;
            }
            found = true;
            let cyc_cn = match cyc.as_str() {
                "long" => "长期",
                "short" => "短期",
                other => other,
            };
            let mut rows = Vec::new();
            let sid = if cyc == "short" { "short" } else { "long" };
            kvs(
                &mut rows,
                "支撑位",
                r[1].clone(),
                if sid == "short" {
                    "sr.short.support_level"
                } else {
                    "sr.long.support_level"
                },
            );
            kvs(
                &mut rows,
                "阻力位",
                r[2].clone(),
                if sid == "short" {
                    "sr.short.resistance_level"
                } else {
                    "sr.long.resistance_level"
                },
            );
            kv(&mut rows, "智能评级", r[4].clone());
            kv(&mut rows, "评级等级", r[5].clone());
            kv(&mut rows, "评级状态", r[6].clone());
            kv(&mut rows, "行业", r[10].clone());
            kv(&mut rows, "排名", r[9].clone());
            kv(&mut rows, "说明", r[3].clone());
            kv(&mut rows, "看多事件", r[7].clone());
            kv(&mut rows, "看空事件", r[8].clone());
            kv(&mut rows, "分析日", r[11].clone());
            sections.push(Section {
                title: format!("百度财经 · 支撑/阻力（{cyc_cn}）"),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "fund_flow") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT super_net, large_net, medium_net, little_net, main_net, \
             super_rate, large_rate, medium_rate, little_rate, trade_date, update_time \
             FROM fund_flow WHERE code=?1 ORDER BY COALESCE(update_time, trade_date) DESC LIMIT 1",
            &code,
        ) {
            found = true;
            let mut rows = Vec::new();
            kvs(&mut rows, "超大单净额", r[0].clone(), "fund_flow.super_net");
            kvs(&mut rows, "大单净额", r[1].clone(), "fund_flow.large_net");
            kvs(&mut rows, "中单净额", r[2].clone(), "fund_flow.medium_net");
            kvs(&mut rows, "小单净额", r[3].clone(), "fund_flow.little_net");
            kvs(&mut rows, "主力净流入", r[4].clone(), "fund_flow.main_net");
            kvs(&mut rows, "超大占比", r[5].clone(), "fund_flow.super_rate");
            kvs(&mut rows, "大单占比", r[6].clone(), "fund_flow.large_rate");
            kvs(&mut rows, "中单占比", r[7].clone(), "fund_flow.medium_rate");
            kvs(&mut rows, "小单占比", r[8].clone(), "fund_flow.little_rate");
            kv(&mut rows, "批次日", r[9].clone());
            kv(&mut rows, "分析日", r[10].clone());
            sections.push(Section {
                title: "百度财经 · 资金流向（亿）".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "vote") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT vote_up, vote_down, total_num, vote_up_rate, vote_down_rate, \
             week_up, week_down, week_rate, trade_date, update_time \
             FROM vote WHERE code=?1 ORDER BY COALESCE(update_time, trade_date) DESC LIMIT 1",
            &code,
        ) {
            found = true;
            let mut rows = Vec::new();
            kvs(&mut rows, "看涨", r[0].clone(), "vote.vote_up");
            kvs(&mut rows, "看跌", r[1].clone(), "vote.vote_down");
            kvs(&mut rows, "总票数", r[2].clone(), "vote.total_num");
            kvs(&mut rows, "看涨率", r[3].clone(), "vote.vote_up_rate");
            kvs(&mut rows, "看跌率", r[4].clone(), "vote.vote_down_rate");
            kvs(&mut rows, "本周看涨", r[5].clone(), "vote.week_up");
            kvs(&mut rows, "本周看跌", r[6].clone(), "vote.week_down");
            kvs(&mut rows, "本周看涨率", r[7].clone(), "vote.week_rate");
            kv(&mut rows, "批次日", r[8].clone());
            kv(&mut rows, "分析日", r[9].clone());
            sections.push(Section {
                title: "百度财经 · 看涨/看跌投票".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "em_comment") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT name, trade_date, emc_total_score, emc_rank, emc_rank_up, emc_focus, \
             emc_org_participate, emc_ratio, emc_prime_cost, emc_prime_cost_20d, emc_prime_cost_60d, \
             emc_prime_inflow, emc_superdeal_in, emc_superdeal_out, emc_bigdeal_in, emc_bigdeal_out, \
             emc_buy_superdeal_ratio, emc_buy_bigdeal_ratio, crawl_date \
             FROM em_comment WHERE code=?1 ORDER BY trade_date DESC LIMIT 1",
            &code,
        ) {
            found = true;
            if name.is_empty() && r[0] != "—" {
                name = r[0].clone();
            }
            let mut rows = Vec::new();
            kv(&mut rows, "数据日", r[1].clone());
            kv(&mut rows, "抓取日", r[18].clone());
            kvs(&mut rows, "综合得分", r[2].clone(), "em_comment.emc_total_score");
            kvs(&mut rows, "全市场排名", r[3].clone(), "em_comment.emc_rank");
            kvs(&mut rows, "排名变动", r[4].clone(), "em_comment.emc_rank_up");
            kvs(&mut rows, "关注指数", r[5].clone(), "em_comment.emc_focus");
            kvs(&mut rows, "机构参与度", r[6].clone(), "em_comment.emc_org_participate");
            kv(&mut rows, "控盘程度", ctrl_degree(&r[6]));
            kvs(&mut rows, "机构参与比例", r[7].clone(), "em_comment.emc_ratio");
            kvs(&mut rows, "主力成本(实时)", r[8].clone(), "em_comment.emc_prime_cost");
            kvs(&mut rows, "主力成本(20日)", r[9].clone(), "em_comment.emc_prime_cost_20d");
            kvs(&mut rows, "主力成本(60日)", r[10].clone(), "em_comment.emc_prime_cost_60d");
            kvs(&mut rows, "主力净流入", r[11].clone(), "em_comment.emc_prime_inflow");
            kvs(&mut rows, "超大单流入", r[12].clone(), "em_comment.emc_superdeal_in");
            kvs(&mut rows, "超大单流出", r[13].clone(), "em_comment.emc_superdeal_out");
            kvs(&mut rows, "大单流入", r[14].clone(), "em_comment.emc_bigdeal_in");
            kvs(&mut rows, "大单流出", r[15].clone(), "em_comment.emc_bigdeal_out");
            kvs(
                &mut rows,
                "买入超大单占比",
                r[16].clone(),
                "em_comment.emc_buy_superdeal_ratio",
            );
            kvs(
                &mut rows,
                "买入大单占比",
                r[17].clone(),
                "em_comment.emc_buy_bigdeal_ratio",
            );
            sections.push(Section {
                title: "东方财富 · 千股千评".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "em_valuation") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT name, trade_date, emv_pe_ttm, emv_pe_lar, emv_pb_mrq, emv_pcf_ocf_lar, \
             emv_pcf_ocf_ttm, emv_ps_ttm, emv_peg, emv_total_market_cap, emv_float_market_cap, \
             emv_board, crawl_date \
             FROM em_valuation WHERE code=?1 ORDER BY trade_date DESC LIMIT 1",
            &code,
        ) {
            found = true;
            if name.is_empty() && r[0] != "—" {
                name = r[0].clone();
            }
            let mut rows = Vec::new();
            kv(&mut rows, "数据日", r[1].clone());
            kv(&mut rows, "抓取日", r[12].clone());
            kvs(&mut rows, "PE(TTM)", r[2].clone(), "em_valuation.emv_pe_ttm");
            kvs(&mut rows, "PE(LAR)", r[3].clone(), "em_valuation.emv_pe_lar");
            kvs(&mut rows, "PB(MRQ)", r[4].clone(), "em_valuation.emv_pb_mrq");
            kvs(&mut rows, "PCF_OCF(LAR)", r[5].clone(), "em_valuation.emv_pcf_ocf_lar");
            kvs(&mut rows, "PCF_OCF(TTM)", r[6].clone(), "em_valuation.emv_pcf_ocf_ttm");
            kvs(&mut rows, "PS(TTM)", r[7].clone(), "em_valuation.emv_ps_ttm");
            kvs(&mut rows, "PEG", r[8].clone(), "em_valuation.emv_peg");
            kvs(&mut rows, "总市值", r[9].clone(), "em_valuation.emv_total_market_cap");
            kvs(&mut rows, "流通市值", r[10].clone(), "em_valuation.emv_float_market_cap");
            kv(&mut rows, "板块", r[11].clone());
            sections.push(Section {
                title: "东方财富 · 基本面估值".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "em_diag_prob") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT trade_date, emt_rise_1_prob, emt_rise_5_prob, emt_avg_1_inc, emt_avg_5_inc, \
             emt_all_count_1, emt_all_count_5, emt_rank_ratio, crawl_date \
             FROM em_diag_prob WHERE code=?1 ORDER BY trade_date DESC LIMIT 1",
            &code,
        ) {
            found = true;
            let mut rows = Vec::new();
            kv(&mut rows, "数据日", r[0].clone());
            kvs(&mut rows, "次日上涨概率", with_pct(&r[1]), "em_diag_prob.emt_rise_1_prob");
            kvs(&mut rows, "5日上涨概率", with_pct(&r[2]), "em_diag_prob.emt_rise_5_prob");
            kvs(&mut rows, "次日平均涨跌", r[3].clone(), "em_diag_prob.emt_avg_1_inc");
            kvs(&mut rows, "5日平均涨跌", r[4].clone(), "em_diag_prob.emt_avg_5_inc");
            kvs(&mut rows, "打败比例", with_pct(&r[7]), "em_diag_prob.emt_rank_ratio");
            kvs(&mut rows, "样本数(次日)", r[5].clone(), "em_diag_prob.emt_all_count_1");
            kvs(&mut rows, "样本数(5日)", r[6].clone(), "em_diag_prob.emt_all_count_5");
            kv(&mut rows, "抓取日", r[8].clone());
            sections.push(Section {
                title: "东方财富 · 诊断概率".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "em_participation") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT trade_date, emp_wish, emp_wish_5d, emp_wish_change, emp_wish_5d_change, crawl_date \
             FROM em_participation WHERE code=?1 ORDER BY trade_date DESC LIMIT 1",
            &code,
        ) {
            found = true;
            let mut rows = Vec::new();
            kv(&mut rows, "数据日", r[0].clone());
            kvs(&mut rows, "当日参与意愿值", r[1].clone(), "em_participation.emp_wish");
            kvs(&mut rows, "五日平均参与意愿值", r[2].clone(), "em_participation.emp_wish_5d");
            kvs(&mut rows, "当日参与意愿变化%", r[3].clone(), "em_participation.emp_wish_change");
            kvs(&mut rows, "五日参与意愿变化%", r[4].clone(), "em_participation.emp_wish_5d_change");
            kv(&mut rows, "抓取日", r[5].clone());
            sections.push(Section {
                title: "东方财富 · 参与意愿".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "em_popularity") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT trade_date, emp_market_rank, emp_market_num, emp_industry_rank, emp_change_rate, \
             emp_market_stock_num, emp_focus_rank, emp_focus_total, emp_focus_index, crawl_date \
             FROM em_popularity WHERE code=?1 ORDER BY trade_date DESC LIMIT 1",
            &code,
        ) {
            found = true;
            let mut rows = Vec::new();
            kv(&mut rows, "数据日", r[0].clone());
            kvs(
                &mut rows,
                "综合市场排名",
                format!("{} / {}", r[1], r[2]),
                "em_popularity.emp_market_rank",
            );
            kvs(&mut rows, "行业排名", r[3].clone(), "em_popularity.emp_industry_rank");
            kvs(&mut rows, "综合得分变化率%", r[4].clone(), "em_popularity.emp_change_rate");
            kvs(&mut rows, "全市场股票数", r[5].clone(), "em_popularity.emp_market_stock_num");
            kvs(
                &mut rows,
                "关注排名",
                format!("{} / {}", r[6], r[7]),
                "em_popularity.emp_focus_rank",
            );
            kvs(&mut rows, "关注指数", r[8].clone(), "em_popularity.emp_focus_index");
            kv(&mut rows, "抓取日", r[9].clone());
            sections.push(Section {
                title: "东方财富 · 市场排名".into(),
                wide: false,
                rows,
            });
        }
    }

    if table_ok(conn, "em_diag_text") {
        if let Ok(Some(r)) = latest_row(
            conn,
            "SELECT trade_date, emt_comment_txt, emt_words_explain, crawl_date \
             FROM em_diag_text WHERE code=?1 ORDER BY trade_date DESC LIMIT 1",
            &code,
        ) {
            found = true;
            let mut rows = Vec::new();
            kv(&mut rows, "数据日", r[0].clone());
            kv(&mut rows, "趋势量能/支撑压力", r[1].clone());
            kv(&mut rows, "消息面/资金面", r[2].clone());
            kv(&mut rows, "抓取日", r[3].clone());
            sections.push(Section {
                title: "东方财富 · 定性诊断".into(),
                wide: true,
                rows,
            });
        }
    }

    if table_ok(conn, "crawl_stats") {
        let mut stmt = conn
            .prepare(
                "SELECT source, crawl_count, last_success, last_status, last_attempt, updated_at, empty_streak \
                 FROM crawl_stats WHERE code=?1 ORDER BY source",
            )
            .map_err(|e| e.to_string())?;
        let stats: Vec<Vec<String>> = stmt
            .query_map(params![&code], |row| {
                let n = row.as_ref().column_count();
                let mut v = Vec::with_capacity(n);
                for i in 0..n {
                    v.push(cell(row, i));
                }
                Ok(v)
            })
            .map_err(|e| e.to_string())?
            .filter_map(|x| x.ok())
            .collect();
        if !stats.is_empty() {
            found = true;
            let mut rows = Vec::new();
            for r in stats {
                let src = match r[0].as_str() {
                    "baidu" => "百度",
                    "em" => "东财",
                    other => other,
                };
                let ok = if r[2] == "1" { "成功" } else { "未成功" };
                let count_id = match r[0].as_str() {
                    "em" => "crawl_stats.em.crawl_count",
                    _ => "crawl_stats.baidu.crawl_count",
                };
                let streak_id = match r[0].as_str() {
                    "em" => "crawl_stats.em.empty_streak",
                    _ => "crawl_stats.baidu.empty_streak",
                };
                kvs(&mut rows, &format!("{src} 次数"), r[1].clone(), count_id);
                kv(&mut rows, &format!("{src} 最近"), format!("{} / {}", ok, r[3]));
                kv(&mut rows, &format!("{src} 尝试日"), r[4].clone());
                kv(&mut rows, &format!("{src} 更新"), r[5].clone());
                kvs(&mut rows, &format!("{src} 空壳连击"), r[6].clone(), streak_id);
            }
            sections.push(Section {
                title: "抓取统计".into(),
                wide: false,
                rows,
            });
        }
    }

    let hint = if found {
        "以下为库内该股各表最新一行（不是实时行情）".into()
    } else {
        format!("库里没有 {code}。先抓取百度/东财，或确认代码是否为 6 位 A 股。")
    };

    Ok(StockSnapshot {
        code,
        name: if name.is_empty() || name == "—" {
            String::new()
        } else {
            name
        },
        found,
        hint,
        sections,
    })
}

fn with_pct(s: &str) -> String {
    if s == "—" {
        s.into()
    } else {
        format!("{s} %")
    }
}

/// 全市场排名一行。
#[derive(Debug, Clone)]
pub struct RankRow {
    pub rank: usize,
    pub code: String,
    pub name: String,
    #[allow(dead_code)]
    pub value: f64,
    pub value_text: String,
}

/// 某个字段对已抓取全市场的排序结果。
#[derive(Debug, Clone)]
pub struct RankBoard {
    pub spec_id: String,
    pub label: String,
    pub ascending: bool,
    pub rows: Vec<RankRow>,
}

impl RankBoard {
    pub fn origin_rank(&self, code: &str) -> Option<usize> {
        self.rows.iter().find(|r| r.code == code).map(|r| r.rank)
    }
}

#[derive(Clone, Copy)]
enum LatestBy {
    /// 百度表：真实分析日优先。
    Ut,
    /// 东财表：按数据日。
    Td,
    /// crawl_stats：每股一行。
    None,
}

struct RankSpec {
    id: &'static str,
    label: &'static str,
    table: &'static str,
    value_expr: &'static str,
    extra_sql: &'static str,
    latest: LatestBy,
    has_name: bool,
    pct: bool,
}

fn rank_specs() -> &'static [RankSpec] {
    &[
        RankSpec { id: "scores.technology", label: "技术", table: "scores", value_expr: "CAST(s.technology AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: true, pct: false },
        RankSpec { id: "scores.capital", label: "资金", table: "scores", value_expr: "CAST(s.capital AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: true, pct: false },
        RankSpec { id: "scores.market", label: "市场", table: "scores", value_expr: "CAST(s.market AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: true, pct: false },
        RankSpec { id: "scores.finance", label: "财务", table: "scores", value_expr: "CAST(s.finance AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: true, pct: false },
        RankSpec { id: "sr.long.support_level", label: "支撑位（长期）", table: "support_resistance", value_expr: "CAST(s.support_level AS REAL)", extra_sql: "AND s.cycle='long'", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "sr.long.resistance_level", label: "阻力位（长期）", table: "support_resistance", value_expr: "CAST(s.resistance_level AS REAL)", extra_sql: "AND s.cycle='long'", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "sr.short.support_level", label: "支撑位（短期）", table: "support_resistance", value_expr: "CAST(s.support_level AS REAL)", extra_sql: "AND s.cycle='short'", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "sr.short.resistance_level", label: "阻力位（短期）", table: "support_resistance", value_expr: "CAST(s.resistance_level AS REAL)", extra_sql: "AND s.cycle='short'", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "fund_flow.super_net", label: "超大单净额", table: "fund_flow", value_expr: "s.super_net", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "fund_flow.large_net", label: "大单净额", table: "fund_flow", value_expr: "s.large_net", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "fund_flow.medium_net", label: "中单净额", table: "fund_flow", value_expr: "s.medium_net", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "fund_flow.little_net", label: "小单净额", table: "fund_flow", value_expr: "s.little_net", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "fund_flow.main_net", label: "主力净流入", table: "fund_flow", value_expr: "s.main_net", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "fund_flow.super_rate", label: "超大占比", table: "fund_flow", value_expr: "CAST(REPLACE(REPLACE(s.super_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "fund_flow.large_rate", label: "大单占比", table: "fund_flow", value_expr: "CAST(REPLACE(REPLACE(s.large_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "fund_flow.medium_rate", label: "中单占比", table: "fund_flow", value_expr: "CAST(REPLACE(REPLACE(s.medium_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "fund_flow.little_rate", label: "小单占比", table: "fund_flow", value_expr: "CAST(REPLACE(REPLACE(s.little_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "vote.vote_up", label: "看涨", table: "vote", value_expr: "CAST(REPLACE(s.vote_up,',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "vote.vote_down", label: "看跌", table: "vote", value_expr: "CAST(REPLACE(s.vote_down,',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "vote.total_num", label: "总票数", table: "vote", value_expr: "CAST(REPLACE(s.total_num,',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "vote.vote_up_rate", label: "看涨率", table: "vote", value_expr: "CAST(REPLACE(REPLACE(s.vote_up_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "vote.vote_down_rate", label: "看跌率", table: "vote", value_expr: "CAST(REPLACE(REPLACE(s.vote_down_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "vote.week_up", label: "本周看涨", table: "vote", value_expr: "CAST(REPLACE(s.week_up,',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "vote.week_down", label: "本周看跌", table: "vote", value_expr: "CAST(REPLACE(s.week_down,',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: false },
        RankSpec { id: "vote.week_rate", label: "本周看涨率", table: "vote", value_expr: "CAST(REPLACE(REPLACE(s.week_rate,'%',''),',','') AS REAL)", extra_sql: "", latest: LatestBy::Ut, has_name: false, pct: true },
        RankSpec { id: "em_comment.emc_total_score", label: "综合得分", table: "em_comment", value_expr: "s.emc_total_score", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_rank", label: "全市场排名", table: "em_comment", value_expr: "s.emc_rank", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_rank_up", label: "排名变动", table: "em_comment", value_expr: "s.emc_rank_up", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_focus", label: "关注指数", table: "em_comment", value_expr: "s.emc_focus", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_org_participate", label: "机构参与度", table: "em_comment", value_expr: "s.emc_org_participate", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_ratio", label: "机构参与比例", table: "em_comment", value_expr: "s.emc_ratio", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_prime_cost", label: "主力成本(实时)", table: "em_comment", value_expr: "s.emc_prime_cost", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_prime_cost_20d", label: "主力成本(20日)", table: "em_comment", value_expr: "s.emc_prime_cost_20d", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_prime_cost_60d", label: "主力成本(60日)", table: "em_comment", value_expr: "s.emc_prime_cost_60d", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_prime_inflow", label: "主力净流入", table: "em_comment", value_expr: "s.emc_prime_inflow", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_superdeal_in", label: "超大单流入", table: "em_comment", value_expr: "s.emc_superdeal_in", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_superdeal_out", label: "超大单流出", table: "em_comment", value_expr: "s.emc_superdeal_out", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_bigdeal_in", label: "大单流入", table: "em_comment", value_expr: "s.emc_bigdeal_in", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_bigdeal_out", label: "大单流出", table: "em_comment", value_expr: "s.emc_bigdeal_out", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_buy_superdeal_ratio", label: "买入超大单占比", table: "em_comment", value_expr: "s.emc_buy_superdeal_ratio", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_comment.emc_buy_bigdeal_ratio", label: "买入大单占比", table: "em_comment", value_expr: "s.emc_buy_bigdeal_ratio", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_pe_ttm", label: "PE(TTM)", table: "em_valuation", value_expr: "s.emv_pe_ttm", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_pe_lar", label: "PE(LAR)", table: "em_valuation", value_expr: "s.emv_pe_lar", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_pb_mrq", label: "PB(MRQ)", table: "em_valuation", value_expr: "s.emv_pb_mrq", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_pcf_ocf_lar", label: "PCF_OCF(LAR)", table: "em_valuation", value_expr: "s.emv_pcf_ocf_lar", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_pcf_ocf_ttm", label: "PCF_OCF(TTM)", table: "em_valuation", value_expr: "s.emv_pcf_ocf_ttm", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_ps_ttm", label: "PS(TTM)", table: "em_valuation", value_expr: "s.emv_ps_ttm", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_peg", label: "PEG", table: "em_valuation", value_expr: "s.emv_peg", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_total_market_cap", label: "总市值", table: "em_valuation", value_expr: "s.emv_total_market_cap", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_valuation.emv_float_market_cap", label: "流通市值", table: "em_valuation", value_expr: "s.emv_float_market_cap", extra_sql: "", latest: LatestBy::Td, has_name: true, pct: false },
        RankSpec { id: "em_diag_prob.emt_rise_1_prob", label: "次日上涨概率", table: "em_diag_prob", value_expr: "s.emt_rise_1_prob", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: true },
        RankSpec { id: "em_diag_prob.emt_rise_5_prob", label: "5日上涨概率", table: "em_diag_prob", value_expr: "s.emt_rise_5_prob", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: true },
        RankSpec { id: "em_diag_prob.emt_avg_1_inc", label: "次日平均涨跌", table: "em_diag_prob", value_expr: "s.emt_avg_1_inc", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_diag_prob.emt_avg_5_inc", label: "5日平均涨跌", table: "em_diag_prob", value_expr: "s.emt_avg_5_inc", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_diag_prob.emt_rank_ratio", label: "打败比例", table: "em_diag_prob", value_expr: "s.emt_rank_ratio", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: true },
        RankSpec { id: "em_diag_prob.emt_all_count_1", label: "样本数(次日)", table: "em_diag_prob", value_expr: "s.emt_all_count_1", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_diag_prob.emt_all_count_5", label: "样本数(5日)", table: "em_diag_prob", value_expr: "s.emt_all_count_5", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_participation.emp_wish", label: "当日参与意愿值", table: "em_participation", value_expr: "s.emp_wish", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_participation.emp_wish_5d", label: "五日平均参与意愿值", table: "em_participation", value_expr: "s.emp_wish_5d", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_participation.emp_wish_change", label: "当日参与意愿变化%", table: "em_participation", value_expr: "s.emp_wish_change", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_participation.emp_wish_5d_change", label: "五日参与意愿变化%", table: "em_participation", value_expr: "s.emp_wish_5d_change", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_popularity.emp_market_rank", label: "综合市场排名", table: "em_popularity", value_expr: "s.emp_market_rank", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_popularity.emp_industry_rank", label: "行业排名", table: "em_popularity", value_expr: "s.emp_industry_rank", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_popularity.emp_change_rate", label: "综合得分变化率%", table: "em_popularity", value_expr: "s.emp_change_rate", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_popularity.emp_market_stock_num", label: "全市场股票数", table: "em_popularity", value_expr: "s.emp_market_stock_num", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_popularity.emp_focus_rank", label: "关注排名", table: "em_popularity", value_expr: "s.emp_focus_rank", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "em_popularity.emp_focus_index", label: "关注指数", table: "em_popularity", value_expr: "s.emp_focus_index", extra_sql: "", latest: LatestBy::Td, has_name: false, pct: false },
        RankSpec { id: "crawl_stats.baidu.crawl_count", label: "百度 次数", table: "crawl_stats", value_expr: "s.crawl_count", extra_sql: "AND s.source='baidu'", latest: LatestBy::None, has_name: false, pct: false },
        RankSpec { id: "crawl_stats.em.crawl_count", label: "东财 次数", table: "crawl_stats", value_expr: "s.crawl_count", extra_sql: "AND s.source='em'", latest: LatestBy::None, has_name: false, pct: false },
        RankSpec { id: "crawl_stats.baidu.empty_streak", label: "百度 空壳连击", table: "crawl_stats", value_expr: "s.empty_streak", extra_sql: "AND s.source='baidu'", latest: LatestBy::None, has_name: false, pct: false },
        RankSpec { id: "crawl_stats.em.empty_streak", label: "东财 空壳连击", table: "crawl_stats", value_expr: "s.empty_streak", extra_sql: "AND s.source='em'", latest: LatestBy::None, has_name: false, pct: false },
    ]
}

fn spec_by_id(id: &str) -> Option<&'static RankSpec> {
    rank_specs().iter().find(|s| s.id == id)
}

/// 当前详情里第一个能排的字段；优先综合得分 / 技术。
pub fn default_rank_id(snap: &StockSnapshot) -> Option<&'static str> {
    const PREFER: &[&str] = &[
        "em_comment.emc_total_score",
        "scores.technology",
        "em_diag_prob.emt_rise_1_prob",
        "fund_flow.main_net",
        "vote.vote_up_rate",
    ];
    let have: Vec<&str> = snap
        .sections
        .iter()
        .flat_map(|s| s.rows.iter())
        .filter_map(|f| f.sort_id)
        .collect();
    PREFER
        .iter()
        .copied()
        .find(|id| have.contains(id))
        .or_else(|| have.first().copied())
}

fn open_ro(db_path: &str) -> Result<Connection, String> {
    if !std::path::Path::new(db_path).exists() {
        return Err(format!("找不到数据库文件：{db_path}"));
    }
    let conn = Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("打开数据库失败: {e}"))?;
    conn.busy_timeout(Duration::from_millis(8000))
        .map_err(|e| format!("设置等待锁超时失败: {e}"))?;
    Ok(conn)
}

fn rank_sql(spec: &RankSpec, ascending: bool) -> String {
    let dir = if ascending { "ASC" } else { "DESC" };
    let name_fb = if spec.has_name { "s.name" } else { "NULL" };
    let extra = spec.extra_sql;
    let ve = spec.value_expr;
    let table = spec.table;
    match table {
        "scores" | "support_resistance" | "fund_flow" | "vote" | "em_comment"
        | "em_valuation" | "em_diag_prob" | "em_participation" | "em_popularity"
        | "crawl_stats" => {}
        _ => unreachable!("ranking table whitelist"),
    }
    let filter = format!("({ve}) IS NOT NULL AND CAST(({ve}) AS TEXT) GLOB '*[0-9]*'");
    match spec.latest {
        LatestBy::None => format!(
            "SELECT s.code AS code, COALESCE(st.name, {name_fb}, '') AS name, ({ve}) AS v \
             FROM {table} s LEFT JOIN stocks st ON st.code = s.code \
             WHERE {filter} {extra} ORDER BY v {dir}, s.code ASC"
        ),
        LatestBy::Ut => format!(
            "SELECT code, name, v FROM ( \
                SELECT s.code AS code, COALESCE(st.name, {name_fb}, '') AS name, ({ve}) AS v, \
                       ROW_NUMBER() OVER (PARTITION BY s.code ORDER BY COALESCE(s.update_time, s.trade_date) DESC) AS rn \
                FROM {table} s LEFT JOIN stocks st ON st.code = s.code \
                WHERE 1=1 {extra} \
             ) WHERE rn = 1 AND v IS NOT NULL AND CAST(v AS TEXT) GLOB '*[0-9]*' \
             ORDER BY v {dir}, code ASC"
        ),
        LatestBy::Td => format!(
            "SELECT code, name, v FROM ( \
                SELECT s.code AS code, COALESCE(st.name, {name_fb}, '') AS name, ({ve}) AS v, \
                       ROW_NUMBER() OVER (PARTITION BY s.code ORDER BY s.trade_date DESC) AS rn \
                FROM {table} s LEFT JOIN stocks st ON st.code = s.code \
                WHERE 1=1 {extra} \
             ) WHERE rn = 1 AND v IS NOT NULL AND CAST(v AS TEXT) GLOB '*[0-9]*' \
             ORDER BY v {dir}, code ASC"
        ),
    }
}

/// 对已抓取全市场按某一字段正序/倒序排名（各表取该股最新一行）。
pub fn rank_market(db_path: &str, spec_id: &str, ascending: bool) -> Result<RankBoard, String> {
    let spec = spec_by_id(spec_id).ok_or_else(|| format!("该字段不支持排序：{spec_id}"))?;
    let conn = open_ro(db_path)?;
    if !table_ok(&conn, spec.table) {
        return Err(format!("库里还没有「{}」这张表", spec.label));
    }
    let sql = rank_sql(spec, ascending);
    let mut stmt = conn.prepare(&sql).map_err(|e| format!("排名查询失败: {e}"))?;
    let mapped = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1).unwrap_or_default(),
                row.get::<_, f64>(2)?,
            ))
        })
        .map_err(|e| format!("排名读取失败: {e}"))?;
    let mut rows = Vec::new();
    for (i, item) in mapped.enumerate() {
        let (code, name, value) = item.map_err(|e| format!("排名行失败: {e}"))?;
        let mut value_text = fmt_num(value);
        if spec.pct && value_text != "—" {
            value_text = format!("{value_text} %");
        }
        rows.push(RankRow {
            rank: i + 1,
            code,
            name,
            value,
            value_text,
        });
    }
    if rows.is_empty() {
        return Err(format!("「{}」在已抓取数据里没有可比较的数值", spec.label));
    }
    Ok(RankBoard {
        spec_id: spec.id.to_string(),
        label: spec.label.to_string(),
        ascending,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn normalize_codes() {
        assert_eq!(normalize_code("1"), "000001");
        assert_eq!(normalize_code("000001.SZ"), "000001");
        assert_eq!(normalize_code("sz000001"), "000001");
        assert_eq!(normalize_code("SH600000"), "600000");
        assert_eq!(normalize_code("  "), "");
    }

    #[test]
    fn lookup_reads_latest_score_row() {
        let path = std::env::temp_dir().join("mp_lookup_v19.db");
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(crate::db::SCHEMA).unwrap();
            conn.execute_batch(crate::db::EM_SCHEMA).unwrap();
            conn.execute(
                "INSERT INTO stocks(code,name,market) VALUES('000001','平安银行','SZ')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO scores(trade_date,code,name,synthesis,technology,capital,market,finance,\
                 is_new,update_time,crawl_date,status) \
                 VALUES('2026-09-01','000001','平安银行','8.1','7','6','5','4','0',\
                 '2026-09-01','2026-09-01','ok')",
                [],
            )
            .unwrap();
        }
        let snap = lookup_stock(path.to_str().unwrap(), "1").unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(snap.code, "000001");
        assert_eq!(snap.name, "平安银行");
        assert!(snap.found);
        assert!(snap.sections.iter().any(|s| s.title.contains("综合评分")));
        let text = snap.as_text();
        assert!(text.contains("综合评级"));
        assert!(text.contains("8.1"));
        assert!(snap.sections.iter().any(|s| {
            s.rows.iter().any(|f| f.sort_id == Some("scores.technology"))
        }));
    }

    #[test]
    fn rank_market_asc_then_desc() {
        let path = std::env::temp_dir().join("mp_rank_v110.db");
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(crate::db::SCHEMA).unwrap();
            conn.execute_batch(crate::db::EM_SCHEMA).unwrap();
            for (code, name, tech) in [
                ("000001", "平安银行", "3"),
                ("000002", "万科A", "1"),
                ("000003", "PT金田A", "2"),
            ] {
                conn.execute(
                    "INSERT INTO stocks(code,name,market) VALUES(?1,?2,'SZ')",
                    rusqlite::params![code, name],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO scores(trade_date,code,name,synthesis,technology,capital,market,finance,\
                     is_new,update_time,crawl_date,status) \
                     VALUES('2026-09-01',?1,?2,'买',?3,'0','0','0','0','2026-09-01','2026-09-01','ok')",
                    rusqlite::params![code, name, tech],
                )
                .unwrap();
            }
            // 旧日期不应参与排名
            conn.execute(
                "INSERT INTO scores(trade_date,code,name,synthesis,technology,capital,market,finance,\
                 is_new,update_time,crawl_date,status) \
                 VALUES('2026-08-01','000002','万科A','买','99','0','0','0','0','2026-08-01','2026-08-01','ok')",
                [],
            )
            .unwrap();
        }
        let p = path.to_str().unwrap();
        let asc = rank_market(p, "scores.technology", true).unwrap();
        let desc = rank_market(p, "scores.technology", false).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            asc.rows.iter().map(|r| r.code.as_str()).collect::<Vec<_>>(),
            vec!["000002", "000003", "000001"]
        );
        assert_eq!(
            desc.rows.iter().map(|r| r.code.as_str()).collect::<Vec<_>>(),
            vec!["000001", "000003", "000002"]
        );
        assert_eq!(asc.origin_rank("000001"), Some(3));
        assert_eq!(desc.origin_rank("000001"), Some(1));
    }
}

