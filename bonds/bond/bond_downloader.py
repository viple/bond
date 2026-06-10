import os
import json
import warnings
import threading
from datetime import datetime
import pandas as pd
import akshare as ak
from pytdx.hq import TdxHq_API

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# ----------------------------- 配置加载 -----------------------------
def load_config(config_path="config.json"):
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

CONFIG = load_config()
TDX_CONFIG = CONFIG["tdx"]
DATA_CONFIG = CONFIG["data"]
DOWNLOAD_CONFIG = CONFIG["download"]

# 解析日期
START_DATE = datetime.strptime(DATA_CONFIG["start_date"], "%Y-%m-%d")
END_DATE = datetime.strptime(DATA_CONFIG["end_date"], "%Y-%m-%d")
CATEGORY = DATA_CONFIG["category"]
BASE_PATH = DATA_CONFIG["base_path"]

# ----------------------------- 全局进度变量 -----------------------------
# 用于记录下载状态，供 Web 界面读取
download_progress = {
    "status": "idle",          # idle, running, completed, error
    "total": 0,
    "current": 0,
    "current_code": "",
    "current_name": "",
    "message": "",
    "errors": []
}

progress_lock = threading.Lock()

def update_progress(**kwargs):
    with progress_lock:
        for k, v in kwargs.items():
            if k in download_progress:
                download_progress[k] = v

# ----------------------------- 辅助函数 -----------------------------
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def create_path(code):
    """每只债券单独保存的路径"""
    ensure_dir(BASE_PATH)
    return os.path.join(BASE_PATH, f"{code}.csv")

def get_bond_list():
    """从 AkShare 获取可转债列表"""
    bond_info = ak.bond_zh_cov()
    symbols = bond_info['债券代码'].tolist()
    names = bond_info['债券简称'].tolist()
    # 市场代码：深圳 0（代码12开头），上海 1
    markets = [0 if s[:2] == '12' else 1 for s in symbols]
    return list(zip(markets, symbols, names))

# ----------------------------- 下载核心函数 -----------------------------
def download_bond_data(market, code, name, api, start_date, end_date):
    """
    下载单只债券的日线数据，返回 DataFrame
    """
    start = 1
    count = 800
    data = []
    while True:
        try:
            bars = api.get_security_bars(CATEGORY, market, code, start, count)
            if bars is None or len(bars) == 0:
                break
            data.extend(bars)
            start += 800
            # 如果最新一条数据日期早于 start_date，可以提前终止（可选）
            # if bars and pd.to_datetime(bars[-1]['datetime']) < start_date:
            #     break
        except Exception as e:
            print(f"获取 {code} 数据出错: {e}")
            break

    if not data:
        return None

    df = api.to_df(data)
    df.loc[df['amount'] <= 0.01, ['vol', 'amount']] = 0
    df = df.set_index('datetime')
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df['name'] = name
    df['code'] = code

    # 计算涨幅和振幅
    df['rise'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1)
    df['rise'].iloc[0] = df['close'].iloc[0] / 100 - 1   # 首日基准100元
    df['Amp'] = (df['high'] - df['low']) / df['close'].shift(1)
    df['Amp'].iloc[0] = (df['high'].iloc[0] - df['low'].iloc[0]) / 100

    df = df[['name', 'code', 'open', 'high', 'low', 'close', 'vol', 'amount', 'rise', 'Amp']]
    # 按日期范围筛选
    df = df.loc[start_date:end_date]
    return df

def run_download():
    """主下载任务，会在后台线程中运行"""
    update_progress(status="running", current=0, errors=[])
    # 获取债券列表
    bond_list = get_bond_list()
    total = len(bond_list)
    update_progress(total=total)

    # 创建 API 实例（每个任务一个连接，避免冲突）
    api = TdxHq_API(
        multithread=TDX_CONFIG["multithread"],
        heartbeat=TDX_CONFIG["heartbeat"],
        auto_retry=TDX_CONFIG["auto_retry"]
    )

    success_count = 0
    for idx, (market, code, name) in enumerate(bond_list, 1):
        update_progress(current=idx, current_code=code, current_name=name, message=f"正在下载 {name}({code})")
        try:
            # 连接通达信
            if not api.connect(TDX_CONFIG["ip"], TDX_CONFIG["port"]):
                raise Exception("通达信连接失败")
            df = download_bond_data(market, code, name, api, START_DATE, END_DATE)
            api.disconnect()

            if df is None or df.empty:
                update_progress(errors=download_progress["errors"] + [f"{code} 无数据"])
                continue

            # 保存单独 CSV
            if DOWNLOAD_CONFIG["save_individual_csv"]:
                individual_path = create_path(code)
                df.to_csv(individual_path, mode=DOWNLOAD_CONFIG["individual_csv_mode"], encoding='utf-8')
                update_progress(message=f"已保存 {code} -> {individual_path}")

            # 保存汇总 CSV（追加模式）
            if DOWNLOAD_CONFIG["save_summary_csv"]:
                summary_path = DOWNLOAD_CONFIG["summary_csv_path"]
                ensure_dir(os.path.dirname(summary_path))
                # 如果是第一次写入，写入表头；否则追加不写表头
                header = not os.path.exists(summary_path)
                df.to_csv(summary_path, mode='a', header=header, encoding='utf-8')

            success_count += 1

        except Exception as e:
            update_progress(errors=download_progress["errors"] + [f"{code} 失败: {str(e)}"])
            if api:
                api.disconnect()
            continue

    update_progress(status="completed", message=f"下载完成！成功 {success_count}/{total} 只")