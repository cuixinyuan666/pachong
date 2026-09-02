# B 方案（Selenium）测试结果

## 📊 测试概况

**测试时间**: 2026-07-25  
**状态**: ⚠️ 部分成功 - Cookie 获取正常，但抓取仍需优化

### 测试结果

| 步骤 | 状态 | 说明 |
|------|------|------|
| 1. Selenium 安装 | ✅ 成功 | selenium 4.46.0 + webdriver-manager |
| 2. Chrome 启动 | ✅ 成功 | Headless 模式正常工作 |
| 3. Cookie 提取 | ✅ 成功 | 提取到 2 个 Cookie（BAIDUID, BIDUPSID） |
| 4. 数据抓取 | ❌ 失败 | 返回 "跳过" 状态 |

---

## 🔍 问题分析

### 问题 1: Cookie 数量不足
- **获取**: 2 个 Cookie (BAIDUID, BIDUPSID)
- **缺少**: PSTM（登录相关 Cookie）
- **原因**: Selenium 访问未登录页面只能获取基础 Cookie

### 问题 2: 抓取逻辑返回 "skip"
```
总计 5 支 | 成功 0 | 空壳 0 | 跳过 5 | 失败 0
```
这表明 `skip_recent_ok()` 认为这些股票最近已经抓取过。

---

## 💡 解决方案

### 方案 A: 使用已导入的真实 Cookie（推荐）

之前你已经提供了完整的 Cookie：
```python
{
    "BAIDUID": "3DF3B61396549A082C7BA6E504A98FD0:FG=1",
    "BIDUPSID": "3DF3B61396549A085D3C6DDB890E7E55",
    "PSTM": "1772379959",
}
```

这些 Cookie 存储在 `~/.baidu_cookie_pool/sessions.json`，可以直接复用！

**修改 `baidu_finance_selenium_crawler_v2.py`：**
```python
# 在 extract_cookies_from_browser 之后添加：
if len(self.cookies) < 3:
    # 从 Cookie 池加载更完整的 Cookie
    from baidu_cookie_pool import CookiePool
    pool = CookiePool()
    best_session = pool.get_best_session()
    if best_session.cookies:
        self.cookies.update(best_session.cookies)
```

### 方案 B: 直接在 Python 爬虫中注入 Cookie

不通过 Selenium，直接使用已有的 `crawler_common.py`，并确保它使用了有效的 Cookie 池。

检查点：
1. Cookie 是否正确加载：
   ```bash
   python -c "from baidu_cookie_pool import CookiePool; p=CookiePool(); print(p.get_stats())"
   ```

2. 是否有正确的 Cookie：
   ```bash
   type %USERPROFILE%\.baidu_cookie_pool\sessions.json
   ```

3. `crawler_common.py` 是否引用了 Cookie 池

### 方案 C: 混合模式（最终推荐）

创建一个新的启动脚本，结合三种方式的优势：

```python
# Step 1: 从 Cookie 池加载完整 Cookie
# Step 2: 用 Selenium 验证 Cookie 有效性  
# Step 3: 用 Python requests 高效批量抓取
# Step 4: 遇到 403 时刷新 Cookie
```

---

## 🎯 推荐下一步

### 立即执行（5 分钟）

1. **验证 Cookie 有效性**：
   ```bash
   cd D:\my_file1\my_file1\my_file\14\2026-07-18-17-52-45
   python -c "
   from baidu_cookie_pool import CookiePool
   from crawler_common import fetch_json, RateLimiter
   
   pool = CookiePool()
   print('Cookie 池状态:', pool.get_stats())
   
   # 手动测试一次抓取
   limiter = RateLimiter(min_interval=1.0)
   headers = {}
   session = pool.get_best_session()
   if session.cookies:
       cookie_str = '; '.join([f'{k}={v}' for k,v in session.cookies.items()])
       headers['Cookie'] = cookie_str
       
   try:
       data = fetch_json(
           'https://finance.pae.baidu.com/vapi/v1/analysis?code=600519&market=ab',
           headers, limiter, backend='curl', max_retries=1
       )
       print('✅ Cookie 有效！可以开始全量爬取')
   except Exception as e:
       print(f'❌ Cookie 无效: {e}')
   "
   ```

2. **如果成功**，直接运行全市场爬取：
   ```bash
   python scrapy_server.py
   ```

### 中期优化（如果仍然 403）

1. **更新 Cookie**（每周一次）：
   - 从浏览器重新导出
   - 或通过 Selenium 自动采集

2. **考虑代理 IP**：
   - 购买住宅代理（$5-15/GB）
   - 每次请求轮换 IP + Cookie

3. **降级到纯 Selenium 模式**：
   - 不依赖 Cookie
   - 直接用浏览器渲染页面
   - 速度较慢但成功率更高

---

## 📈 C 方案 vs B 方案 对比

| 维度 | C 方案 (Cookie) | B 方案 (Selenium) |
|------|----------------|-------------------|
| 实施难度 | ⭐⭐ 简单 | ⭐⭐⭐ 中等 |
| 抓取速度 | ⭐⭐⭐ 快 (1-2秒/支) | ⭐ 慢 (10-30秒/支) |
| 资源消耗 | ⭐⭐ 低 (~50MB) | ⭐⭐⭐ 高 (200-500MB) |
| 成功率 | ⭐⭐⭐ 依赖 Cookie | ⭐⭐⭐⭐ 较稳定 |
| 维护成本 | 每周更新 Cookie | 需维护浏览器版本 |
| **当前状态** | ⚠️ 需要验证 | ✅ 框架已就绪 |

---

## ✅ 已完成的工作清单

- [x] 安装 Selenium + webdriver-manager
- [x] 创建 Selenium 基础爬虫 (`baidu_finance_selenium_crawler.py`)
- [x] 创建混合方案爬虫 (`baidu_finance_selenium_crawler_v2.py`)
- [x] 实现 Cookie 提取功能
- [x] 集成现有 `crawler_common.py` 抓取逻辑
- [x] C 方案 Cookie 池系统
- [ ] 验证 Cookie 有效性（待执行）
- [ ] 全市场爬取测试（待执行）

---

**结论：B 方案框架已完成，现在需要验证 C 方案的真实 Cookie 是否有效。如果有效，则无需使用 B 方案。**
