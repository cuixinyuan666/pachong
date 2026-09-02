#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""东方财富「千股千评」API 探针 —— 确认精确端点与字段名（零依赖）。
用法: python probe_em_stockcomment.py
"""
import json
import urllib.parse
import urllib.request

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REF = "https://data.eastmoney.com/stockcomment/"


def get(base: str, qs: dict):
    url = base + "?" + urllib.parse.urlencode(qs)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REF})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read().decode("utf-8"))


# 候选1：千股千评主列表（全市场个股批量，补 columns）
c1 = ("list:RPT_DMSK_TS_STOCKNEW",
      "https://datacenter-web.eastmoney.com/api/data/v1/get",
      {"reportName": "RPT_DMSK_TS_STOCKNEW", "columns": "ALL", "pageSize": "2", "pageNumber": "1",
       "sortColumns": "SECURITY_CODE", "sortTypes": "1", "client": "PC", "source": "WEB"})
# 候选2：个股估值详情（取 000001 作样例）—— 已确认为估值维度
c2 = ("detail:RPT_VALUEANALYSIS_DET",
      "https://datacenter-web.eastmoney.com/api/data/v1/get",
      {"reportName": "RPT_VALUEANALYSIS_DET", "columns": "ALL",
       "filter": '(SECURITY_CODE="000001")', "client": "PC", "source": "WEB"})
# 候选3：综合诊断（千股千评核心：得分/机构参与度/主力成本/关注指数）—— 探测 reportName
c3 = ("diag:RPT_DMSK_TS_STOCKCOMMENT",
      "https://datacenter-web.eastmoney.com/api/data/v1/get",
      {"reportName": "RPT_DMSK_TS_STOCKCOMMENT", "columns": "ALL", "pageSize": "2", "pageNumber": "1",
       "sortColumns": "SECURITY_CODE", "sortTypes": "1", "client": "PC", "source": "WEB"})

for name, base, qs in (c1, c2, c3):
    try:
        d = get(base, qs)
        result = d.get("result") or {}
        rows = result.get("data") or [] if isinstance(result, dict) else []
        print(f"\n===== {name} | success={d.get('success')} =====")
        if rows:
            print("字段(", len(rows[0]), "个):", list(rows[0].keys()))
            print("样例:", json.dumps(rows[0], ensure_ascii=False)[:900])
        else:
            print("无 data；返回键:", list(d.keys()), "| 内容:", str(d)[:200])
    except Exception as e:
        print(f"\n===== {name} | ERROR: {e} =====")
