# 百度财经 403 反爬 - C 方案快速开始指南

## ✅ 已完成的工作

C 方案（Cookie 轮换 + Session 复用）已经集成到你的爬虫系统中！

### 已创建的文件

| 文件 | 用途 |
|------|------|
| `baidu_cookie_pool.py` | Cookie 池核心模块 |
| `baidu_cookie_fetcher.py` | Selenium Cookie 采集器 |
| `patch_crawler_common.py` | 自动修补脚本 |
| `crawler_common.py.backup` | 原始文件备份 |
| `crawler_common.py` | **已集成 Cookie 支持** |

### Cookie 池状态

当前已有 **4 个 Session**，成功率 **33.33%**（需要导入真实 Cookie）。

---

## 🚀 立即使用（3 步完成）

### 步骤 1: 从浏览器导出 Cookie

1. 打开 Chrome 浏览器
2. 访问：`https://finance.baidu.com/`
3. 按 `F12` 打开开发者工具
4. 点击 **Application** 标签
5. 左侧展开 **Cookies** → 选择 `https://finance.baidu.com`
6. 找到以下 Cookie 并复制它们的 **Name** 和 **Value**：
   - `BAIDUID`
   - `BIDUPSID`
   - `PSTM`
   - （可选更多...）

### 步骤 2: 将 Cookie 添加到池中

在命令行运行：

```bash
python -c "
from baidu_cookie_pool import CookiePool
pool = CookiePool()

# 替换为你从浏览器复制的实际值
cookies = {
    'BAIDUID': '这里粘贴你的BAIDUID值',
    'BIDUPSID': '这里粘贴你的BIDUPSID值',
    'PSTM': '这里粘贴你的PSTM值',
}

pool.add_manual_cookies(cookies)
print('Cookie 添加成功!')
print(f'当前状态: {pool.get_stats()}')
"
```

### 步骤 3: 测试效果

重新运行爬虫，观察 403 错误是否减少：

```bash
python scrapy_server.py
```

或通过 Web 界面触发抓取：
- 访问 http://127.0.0.1:8765
- 点击 "开始全市场抓取"

---

## 📊 预期效果

### 改善前
- 连续 8 支股票遭 403 → 冷却 600 秒
- 最多冷却 12 次后停止抓取

### 改善后（预期）
- Cookie 轮换可分散请求指纹
- 降低 30-50% 的 403 错误率
- 减少冷却次数，提高抓取效率

---

## 🔍 监控和维护

### 查看 Cookie 池状态

```bash
python -c "
from baidu_cookie_pool import CookiePool
pool = CookiePool()
print('Cookie 池统计:')
for k, v in pool.get_stats().items():
    print(f'  {k}: {v}')
"
```

### 更新 Cookie

建议每 **7-14 天** 更新一次 Cookie：
- 重复步骤 1-2
- 旧的 Cookie 会自动被淘汰

### 高级选项

增加最大 Session 数量（默认 10）：

```python
pool = CookiePool(max_sessions=20)
```

自定义存储路径：

```python
pool = CookiePool(pool_dir=r"C:\custom\path")
```

---

## ❓ 如果效果不够好？

如果 403 率仍然 >10%，考虑升级到 **B 方案**（Selenium 浏览器自动化）：

### B 方案优势
- ✅ 真实浏览器环境
- ✅ 更难被检测
- ✅ 可执行 JavaScript

### B 方案劣势
- ❌ 资源消耗大（内存 200-500MB）
- ❌ 速度慢 5-10 倍
- ❌ 维护成本高

切换到 B 方案很简单，我已经准备好了 `baidu_cookie_fetcher.py`，只需扩展即可。

---

## 💡 最佳实践

1. **多 Cookie 轮换**: 保持 10-20 个活跃 Session
2. **降低频率**: min_interval=1.5s, max_per_minute=30
3. **延长冷却**: COOLDOWN_SEC=900 (15分钟)
4. **定期更新**: 每周检查 Cookie 有效性

---

## 📞 常见问题

### Q: Cookie 从哪里获取？
A: 从浏览器导出（见步骤 1），或用 Selenium 自动采集

### Q: Cookie 多久失效？
A: 通常 30 天，但可能被提前吊销

### Q: 需要付费吗？
A: C 方案完全免费！

### Q: 会影响现有功能吗？
A: 不会，已做兼容处理，无 Cookie 时自动回退到原有逻辑

---

**准备好了吗？开始执行 [3 步快速使用](#立即使用3-步完成) 吧！**
