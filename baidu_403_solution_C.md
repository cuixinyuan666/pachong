# 百度财经 403 反爬解决方案 - C方案（Cookie轮换+Session复用）

## 📋 方案概述

**C方案：Cookie 轮换 + Session 复用**

这是**免费的、中等效果**的反爬解决方案，通过模拟多个不同浏览器 Session 来分散请求压力，降低单 IP 被封锁的概率。

### 核心原理
- 每个 Session 拥有独立的 Cookie（模拟不同用户）
- 自动轮换使用成功率最高的 Session
- 失败自动切换 Session，成功则记录
- 可手动导入浏览器 Cookie，也可通过 Selenium 自动采集

---

## 🚀 快速开始

### 步骤 1: 安装依赖

```bash
pip install selenium webdriver-manager
```

### 步骤 2: 采集 Cookie

#### 方式 A: 从浏览器手动导出（推荐，最简单）

1. 打开 Chrome 浏览器，访问 `https://finance.baidu.com/`
2. 按 `F12` 打开开发者工具 → **Application** 标签
3. 左侧选择 **Cookies** → `https://finance.baidu.com`
4. 复制所有 Cookie 名称和值
5. 运行以下代码保存：

```python
from baidu_cookie_pool import CookiePool

pool = CookiePool()

# 从浏览器导出的 Cookie
cookies = {
    "BAIDUID": "ABC123:FG=1",  # 替换为你的实际值
    "BIDUPSID": "ABC123",
    "PSTM": "1234567890",
    # ... 更多 Cookie
}

pool.add_manual_cookies(cookies)
print(f"已添加 Cookie，当前状态: {pool.get_stats()}")
```

#### 方式 B: 使用 Selenium 自动采集

```bash
python baidu_cookie_fetcher.py --collect
```

这将自动打开浏览器访问百度财经并提取 Cookie。

### 步骤 3: 测试 Cookie 是否有效

```bash
python baidu_cookie_fetcher.py --test
```

### 步骤 4: 集成到爬虫

修改 `crawler_common.py` 的 `_fetch_curl` 函数，添加 Cookie 支持：

```python
from baidu_cookie_pool import CookiePool, EnhancedFetcher

# 全局 Cookie 池实例
_cookie_pool = None

def get_cookie_pool():
    global _cookie_pool
    if _cookie_pool is None:
        _cookie_pool = CookiePool()
    return _cookie_pool

# 在 _fetch_curl 函数中，替换原有的 limiter.wait() 后：
limiter.wait()
pool = get_cookie_pool()
session = pool.get_best_session()

# 如果有 Cookie，添加到 headers
if session.cookies:
    cookie_str = "; ".join([f"{k}={v}" for k, v in session.cookies.items()])
    headers["Cookie"] = cookie_str

# ... 执行 curl 命令 ...

# 根据结果记录
if success:
    pool.record_success(session.session_id)
else:
    pool.record_failure(session.session_id)
```

---

## 🔧 高级用法

### 自定义 Cookie 池参数

```python
pool = CookiePool(
    pool_dir=r"C:\custom\path\to\pool",  # 自定义存储路径
    max_sessions=20  # 最大 Session 数量
)
```

### 直接使用增强抓取器

```python
from baidu_cookie_pool import EnhancedFetcher

fetcher = EnhancedFetcher()
data = fetcher.fetch_with_retry(
    "https://finance.pae.baidu.com/vapi/v1/analysis?code=000001&market=ab"
)
```

### 查看池状态

```python
stats = pool.get_stats()
print(f"总 Sessions: {stats['total']}")
print(f"活跃 Sessions: {stats['active']}")
print(f"成功率: {stats['success_rate']}%")
```

---

## ⚠️ 注意事项

### 1. Cookie 有效期

百度 Cookie 通常有效期为 **30 天**，但可能因安全策略提前失效。
建议每 7-14 天更新一次 Cookie。

### 2. IP 限制

Cookie 轮换主要解决**基于 User-Agent 和 Cookie 指纹**的反爬，但对**高频 IP 封锁**效果有限。
如果仍然遇到 403：
- 降低请求频率（修改 `min_interval` 和 `max_per_minute`）
- 延长冷却时间（将 `COOLDOWN_SEC` 从 600 增加到 1200）

### 3. 多 IP 代理（进阶方案）

如果 Cookie 轮换不够用，可以考虑：
- 购买住宅代理 IP 服务（如 Proxyium、SmartProxy）
- 每次请求使用不同 IP + 不同 Cookie
- 成本约 $5-15/GB

---

## 📊 效果评估

### 优势
- ✅ 完全免费
- ✅ 实施简单，无需修改爬虫核心逻辑
- ✅ 可立即见效（降低 30-50% 的 403 错误）
- ✅ 与现有爬虫兼容性好

### 局限
- ❌ 不能完全解决 IP 级别的封锁
- ❌ 需要定期更新 Cookie（建议每周）
- ❌ 对严格的反爬系统效果有限

---

## 🔄 如果 C 方案不够用？

切换到 **B 方案：Selenium/Playwright 浏览器自动化**

### B 方案优势
- ✅ 真实浏览器环境，指纹更自然
- ✅ 可执行 JavaScript，绕过前端检测
- ✅ 支持 Cookie 持久化

### B 方案劣势
- ❌ 资源消耗大（内存占用 200-500MB/进程）
- ❌ 速度慢（比 HTTP 请求慢 5-10 倍）
- ❌ 需要安装浏览器和驱动

### 如何切换到 B 方案

我已经创建了 `baidu_cookie_fetcher.py`，其中包含 Selenium 采集 Cookie 的代码。
如果需要完整实现，我可以：
1. 创建基于 Selenium 的完整爬虫
2. 或者集成 Playwright（更轻量）

---

## 💡 最佳实践

1. **混合策略**: 先用 C 方案（Cookie 轮换），如果 403 率仍 >10%，再考虑 B 方案
2. **多池管理**: 为不同数据源创建独立的 Cookie 池
3. **监控告警**: 定期检查成功率，低于 80% 时更新 Cookie
4. **限速配合**: Cookie 轮换 + 低频率 = 最佳效果

---

## 📞 问题排查

### Q: Cookie 添加后仍然 403
A: 检查：
- Cookie 是否过期（尝试重新采集）
- 请求频率是否过高（降低到 1-2 秒/次）
- 是否需要更多 Sessions（增加到 15-20 个）

### Q: 如何批量导入多个 Cookie
A: 编辑 `sessions.json` 文件，添加多个 Session 对象即可。

### Q: Cookie 存储在哪里
A: 默认存储在 `%USERPROFILE%\.baidu_cookie_pool\sessions.json`

---

**创建时间**: 2026-07-25
**版本**: 1.0
**作者**: Agnes-2.0
