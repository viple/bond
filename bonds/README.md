# 可转债数据下载器 (Convertible Bond Data Downloader)

从通达信 TDX + AkShare 获取 A 股可转债日线数据，支持增量更新、多线程下载、技术指标计算、Web 可视化。

## 项目结构

```
bonds/
├── run.py                    # 统一入口
├── requirements.txt          # Python 依赖
├── .gitignore
├── config.json               # 项目配置文件
├── bond/
│   ├── bond.py               # 原始版本（保留参考）
│   ├── bond_downloader.py    # 增强版下载器（核心）
│   └── config.json           # 下载器配置文件
└── web/
    ├── app.py                # Flask Web 服务
    └── templates/
        └── index.html        # 前端页面
```

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 仅下载数据
python run.py

# 启动 Web 界面
python run.py --web

# 自定义端口
python run.py --web --port 8080
```

## 配置说明

配置文件 `bond/config.json`：

| 配置段 | 说明 |
|--------|------|
| `tdx` | 通达信连接参数（IP，端口，多线程等） |
| `data` | 数据范围（起始/结束日期，存储路径） |
| `download` | 存储方式（CSV / Parquet） |
| `features` | 功能开关（增量更新、多线程、技术指标、备用源） |
| `email_config` | 邮件通知配置 |
| `wecom_config` | 企业微信通知配置 |

`end_date` 支持填写 `"latest"` 自动使用当天日期。

## 功能特性

- **增量更新**：已有数据不会重复下载，仅补充缺失日期
- **多线程下载**：并行下载多只可转债，连接池限流
- **技术指标**：自动计算 MA5/10/20、RSI14、布林带
- **备用数据源**：通达信无数据时自动切换 AkShare
- **Web 界面**：实时进度查看 + 数据预览
- **通知推送**：支持邮件/企业微信通知下载完成
- **多种存储**：CSV / Parquet / SQLite
