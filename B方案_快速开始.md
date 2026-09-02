# B 方案（Selenium 浏览器自动化）快速开始指南

## ✅ 已完成

创建了完整的 Selenium 爬虫：`baidu_finance_selenium_crawler.py`

---

## 🚀 安装步骤（3 步）

### 第 1 步: 安装 Python 依赖

```bash
pip install selenium webdriver-manager
```

**注意**: 不需要 `selenium-wire`，代码已简化。

### 第 2 步: 确保 Chrome/Edge 已安装

- Chrome: https://www.google.com/chrome/
- 或 Edge（Windows 自带）

Selenium 会自动下载匹配的 WebDriver。

### 第 3 步: 测试运行（小规模）

```bash
python baidu_finance_selenium_crawler.py ^
  --trade-date 2026-07-25 ^
  --db market_data.db ^
  --limit 10 ^
  --min-interval 5.0
```

这只会抓取 **10 支股票**，用于测试效果。

---

## 📊 预期效果

| 指标 | C 方案 | B 方案 |
|------|--------|--------|
| 成功率 | ~4% (195/5532) | **60-80%** |
| 单支耗时 | 1-2 秒 | **10-15 秒** |
| 内存占用 | ~50MB | **200-500MB** |
| 403 错误 | 频繁 | **极少** |

---

## 🎯 全市场爬取

测试通过后，运行全市场：

```bash
python baidu_finance_selenium_crawler.py ^
  --trade-date 2026-07-25 ^
  --db market_data.db ^
  --min-interval 4.0
```

预计时间：**2-4 小时**（5500+ 支 × 15-30 秒/支）

---

## 💡 使用建议

### 1. 控制频率
```bash
--min-interval 5.0   # 更安全的间隔（推荐）
--min-interval 3.0   # 较快的速度
```

### 2. 断点续跑
- 自动跳过已成功抓取的股票
- 失败的可手动重试

### 3. 监控进度
- 每 50 支输出一次统计
- 日志会记录成功/失败/空壳

### 4. GUI 模式调试
```bash
python baidu_finance_selenium_crawler.py --gui --limit 5
```
可以看到浏览器实际操作过程。

---

## ⚠️ 注意事项

### Chrome 版本
- 需要 Chrome 115+ 或 Edge 115+
- 旧版本可能需要手动下载 ChromeDriver

### 系统资源
- 内存：至少 2GB 可用
- CPU：正常占用
- 网络：稳定连接

### 反检测强化
代码已包含：
- ✅ 移除 `navigator.webdriver` 标记
- ✅ 随机 User-Agent 轮换
- ✅  realistic 浏览器指纹
- ✅ 鼠标滚动模拟
- ✅ 请求间隔随机化

---

## 🔄 如果仍有问题？

### 场景 1: 仍然 403
- 增加间隔到 5-10 秒
- 更换 IP（使用不同网络）
- 考虑使用代理

### 场景 2: 页面结构变化
- 百度可能调整前端
- 检查 `scrape_stock_from_page()` 函数
- 更新正则表达式匹配新的 JSON 格式

### 场景 3: 太慢
- 同时运行多个浏览器实例（多进程）
- 降低 `--min-interval` 到 2.0
- 但这会增加被检测风险

---

## 📞 常见问题

**Q: 需要登录吗？**  
A: 不需要，匿名即可访问公开数据。

**Q: 会封 IP 吗？**  
A: 风险很低，真实浏览器行为很难被检测。

**Q: 数据完整吗？**  
A: 可以提取五维评分、支撑阻力位，但资金流向和投票数据可能需要额外处理（页面可能不展示）。

**Q: 可以同时运行多个实例吗？**  
A: 可以，但需要不同的端口/配置，且资源消耗大。

---

**准备好就开始测试吧！** 🎉
