#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
可转债日线数据下载器（增强版）
功能：增量更新、多线程、技术指标、多种存储方式、数据源切换、通知、健壮性增强
支持结束日期自动为当前日期（配置文件填写 "latest" 或命令行 --end latest）
"""

import os
import json
import time
import sqlite3
import smtplib
import argparse
import warnings
import threading
from functools import wraps
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import numpy as np
import akshare as ak
from pytdx.hq import TdxHq_API
import requests


# ========================= TDX IP 连接池 =========================
# 5 个最优通达信 IP（由 from pytdx.py 测速选出）
TDX_IP_POOL = [
    ("60.191.117.167", 7709),   # 0.084s
    ("115.238.56.198", 7709),   # 0.089s
    ("115.238.90.165", 7709),   # 0.089s
    ("180.153.18.170", 7709),   # 0.098s
    ("218.75.126.9", 7709),    # 0.103s
]


class TdxIpPool:
    """TDX IP 连接池，带自动故障转移和健康状态追踪。"""

    def __init__(self, pool=None):
        self._pool = pool or TDX_IP_POOL
        self._dead = set()
        self._lock = threading.Lock()

    def connect(self, api, timeout=3.0):
        """遍历 IP 池尝试连接，返回 (ip,port) 或 None。"""
        with self._lock:
            candidates = [s for s in self._pool if s not in self._dead]
            if not candidates:
                candidates = list(self._pool)
                self._dead.clear()
        for ip, port in candidates:
            try:
                if api.connect(ip, port, time_out=timeout):
                    with self._lock:
                        self._dead.discard((ip, port))
                    return ip, port
            except Exception:
                with self._lock:
                    self._dead.add((ip, port))
        return None

warnings.filterwarnings("ignore")

# ========================= 全局配置 (延迟加载) =========================
_CONFIG = None


def load_config(config_path=None):
    """加载配置文件，如文件不存在则创建默认配置"""
    global _CONFIG
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
    default_config = {
        "tdx": {
            "ip": "115.238.90.165",
            "port": 7709,
            "multithread": True,
            "heartbeat": True,
            "auto_retry": True
        },
        "data": {
            "start_date": "2020-01-01",
            "end_date": "latest",
            "category": 9,
            "base_path": "./all_stock_candle/bond"
        },
        "download": {
            "save_individual_csv": True,
            "individual_csv_mode": "w",
            "save_summary_csv": True,
            "summary_csv_path": "./all_stock_candle/bond/summary.csv"
        },
        "features": {
            "incremental_update": True,
            "multi_thread": True,
            "max_workers": 5,
            "use_parquet": False,
            "use_sqlite": False,
            "sqlite_db_path": "./bonds.db",
            "use_technical_indicators": True,
            "fallback_to_akshare": True,
            "notify_email": False,
            "notify_wecom": False
        },
        "email_config": {
            "smtp_server": "smtp.qq.com",
            "smtp_port": 465,
            "from_addr": "",
            "password": "",
            "to_addr": ""
        },
        "wecom_config": {
            "webhook_key": ""
        }
    }
    config_dir = os.path.dirname(config_path)
    if config_dir and not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
    if not os.path.exists(config_path):
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)
        print(f"已创建默认配置文件: {config_path}，请根据实际情况修改")
    with open(config_path, "r", encoding="utf-8-sig") as f:
        _CONFIG = json.load(f)
    return _CONFIG


def get_config():
    """获取当前配置，尚未加载时自动加载"""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_config()
    return _CONFIG


# ========================= 日期解析 =========================
def parse_date(date_str, default=None):
    """解析日期字符串，支持 'latest'/'now' 表示当天"""
    if date_str is None:
        return default
    if isinstance(date_str, str) and date_str.lower() in ("latest", "now"):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return datetime.strptime(date_str, "%Y-%m-%d")


# ========================= 全局进度变量 =========================
download_progress = {
    "status": "idle",
    "total": 0,
    "current": 0,
    "current_code": "",
    "current_name": "",
    "message": "",
    "errors": []
}
progress_lock = threading.Lock()


def update_progress(**kwargs):
    """线程安全地更新进度字典"""
    with progress_lock:
        for k, v in kwargs.items():
            if k in download_progress:
                download_progress[k] = v


def get_progress_snapshot():
    """返回进度字典的线程安全快照，供 web 层调用"""
    with progress_lock:
        return {
            "status": download_progress["status"],
            "total": download_progress["total"],
            "current": download_progress["current"],
            "current_code": download_progress["current_code"],
            "current_name": download_progress["current_name"],
            "message": download_progress["message"],
            "errors": download_progress["errors"][-20:]
        }


def reset_progress():
    """重置进度变量"""
    with progress_lock:
        download_progress["status"] = "idle"
        download_progress["total"] = 0
        download_progress["current"] = 0
        download_progress["current_code"] = ""
        download_progress["current_name"] = ""
        download_progress["message"] = ""
        download_progress["errors"] = []


# ========================= 辅助函数 =========================
def ensure_dir(path):
    """确保目录存在"""
    Path(path).mkdir(parents=True, exist_ok=True)


def retry(max_retries=3, delay=2, backoff=2):
    """重试装饰器，指数退避"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        break
                    time.sleep(delay * (backoff ** attempt))
            raise last_exception
        return wrapper
    return decorator


# ========================= 通知功能 =========================
def send_email_notification(subject, body):
    """发送邮件通知"""
    cfg = get_config()
    if not cfg["features"].get("notify_email", False):
        return
    try:
        ec = cfg["email_config"]
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = ec["from_addr"]
        msg["To"] = ec["to_addr"]
        server = smtplib.SMTP_SSL(ec["smtp_server"], ec["smtp_port"])
        server.login(ec["from_addr"], ec["password"])
        server.sendmail(ec["from_addr"], [ec["to_addr"]], msg.as_string())
        server.quit()
    except Exception as e:
        print(f"邮件发送失败: {e}")


def send_wecom_notification(message):
    """发送企业微信通知"""
    cfg = get_config()
    if not cfg["features"].get("notify_wecom", False):
        return
    try:
        key = cfg["wecom_config"]["webhook_key"]
        url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"
        data = {"msgtype": "text", "text": {"content": message}}
        requests.post(url, json=data, timeout=5)
    except Exception as e:
        print(f"企业微信通知失败: {e}")


# ========================= 技术指标计算 =========================
def compute_rsi(series, period=14):
    """计算 RSI 指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def add_technical_indicators(df):
    """添加常用技术指标"""
    cfg = get_config()
    if not cfg["features"].get("use_technical_indicators", True):
        return df
    close = df["close"]
    df["MA5"] = close.rolling(window=5).mean().round(2)
    df["MA10"] = close.rolling(window=10).mean().round(2)
    df["MA20"] = close.rolling(window=20).mean().round(2)
    df["RSI14"] = compute_rsi(close, 14).round(2)
    df["BB_middle"] = close.rolling(window=20).mean().round(2)
    bb_std = close.rolling(window=20).std().round(2)
    df["BB_upper"] = (df["BB_middle"] + 2 * bb_std).round(2)
    df["BB_lower"] = (df["BB_middle"] - 2 * bb_std).round(2)
    return df


# ========================= 数据源切换 (AkShare 备用) =========================
AKSHARE_COLUMN_MAP = {
    "日期": "datetime",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "vol",
    "成交额": "amount",
}


def download_from_akshare(code, start_date, end_date):
    """使用 AkShare 作为备用数据源下载"""
    try:
        prefix = "sz" if code.startswith("12") else "sh"
        symbol = f"{prefix}{code}"
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            adjust=""
        )
        if df is None or df.empty:
            return None

        rename_map = {}
        for cn, en in AKSHARE_COLUMN_MAP.items():
            if cn in df.columns:
                rename_map[cn] = en
        df.rename(columns=rename_map, inplace=True)

        if "close" not in df.columns:
            return None

        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        cols = [c for c in ["open", "high", "low", "close", "vol", "amount"] if c in df.columns]
        return df[cols]
    except Exception as e:
        print(f"AkShare 备用源下载失败 {code}: {e}")
        return None


# ========================= 核心下载逻辑 =========================
@retry(max_retries=3, delay=1)
def download_bond_data(market, code, name, api, start_date, end_date):
    """从通达信下载单只可转债数据，支持失败后切换到 AkShare"""
    cfg = get_config()
    category = cfg["data"]["category"]

    start = 1
    count = 800
    data = []

    while True:
        try:
            bars = api.get_security_bars(category, market, code, start, count)
            if bars is None or len(bars) == 0:
                break
            data.extend(bars)
            start += 800
        except Exception as e:
            print(f"通达信获取 {code} 数据出错: {e}")
            break

    if data:
        df = api.to_df(data)
        df.loc[df["amount"] <= 0.01, ["vol", "amount"]] = 0
        df = df.set_index("datetime")
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df["name"] = name
        df["code"] = code
        df["rise"] = (df["close"] - df["close"].shift(1)) / df["close"].shift(1)
        if len(df) > 0:
            df["rise"].iloc[0] = (df["close"].iloc[0] - 100) / 100
        df["Amp"] = (df["high"] - df["low"]) / df["close"].shift(1)
        df["Amp"].iloc[0] = (df["high"].iloc[0] - df["low"].iloc[0]) / 100
        df = df[["name", "code", "open", "high", "low", "close", "vol", "amount", "rise", "Amp"]]
        df = add_technical_indicators(df)
        df = df.loc[start_date:end_date]
        if not df.empty:
            return df

    if cfg["features"].get("fallback_to_akshare", True):
        print(f"通达信无数据，尝试备用源 {code}")
        df = download_from_akshare(code, start_date, end_date)
        if df is not None and not df.empty:
            df["name"] = name
            df["code"] = code
            df["rise"] = (df["close"] - df["close"].shift(1)) / df["close"].shift(1)
            df["rise"].iloc[0] = (df["close"].iloc[0] - 100) / 100
            df["Amp"] = (df["high"] - df["low"]) / df["close"].shift(1)
            df["Amp"].iloc[0] = (df["high"].iloc[0] - df["low"].iloc[0]) / 100
            df = add_technical_indicators(df)
            return df
    return None


def get_incremental_data(market, code, name, api, start_date, end_date, existing_df=None):
    """增量更新：只下载缺失日期区间，然后合并去重"""
    cfg = get_config()
    if cfg["features"].get("incremental_update", True) and existing_df is not None and not existing_df.empty:
        last_date = existing_df.index.max()
        if last_date >= end_date:
            return existing_df
        new_start = last_date + pd.Timedelta(days=1)
        if new_start > end_date:
            return existing_df
        new_df = download_bond_data(market, code, name, api, new_start, end_date)
        if new_df is None or new_df.empty:
            return existing_df
        combined = pd.concat([existing_df, new_df])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = combined.sort_index()
        return combined
    else:
        return download_bond_data(market, code, name, api, start_date, end_date)


# ========================= 数据存储 =========================
def save_individual_data(df, code):
    """保存单只可转债数据到 CSV 或 Parquet"""
    cfg = get_config()
    if not cfg["download"].get("save_individual_csv", True):
        return None
    base_path = cfg["data"]["base_path"]
    ensure_dir(base_path)

    if cfg["features"].get("use_parquet", False):
        file_path = os.path.join(base_path, f"{code}.parquet")
        df.to_parquet(file_path, index=True)
    else:
        file_path = os.path.join(base_path, f"{code}.csv")
        df.to_csv(file_path, encoding="utf-8")
    return file_path


def save_summary_data(df):
    """追加汇总数据，带重复行去除"""
    cfg = get_config()
    if not cfg["download"].get("save_summary_csv", False):
        return
    summary_path = cfg["download"]["summary_csv_path"]
    ensure_dir(os.path.dirname(summary_path))

    if os.path.exists(summary_path):
        try:
            old_df = pd.read_csv(summary_path, encoding="utf-8")
            combined = pd.concat([old_df, df], ignore_index=True)
            combined = combined.drop_duplicates(keep="last").reset_index(drop=True)
        except Exception:
            combined = df
    else:
        combined = df

    combined.to_csv(summary_path, index=False, encoding="utf-8")


def save_to_sqlite(df, code):
    """保存到 SQLite 数据库"""
    cfg = get_config()
    if not cfg["features"].get("use_sqlite", False):
        return
    db_path = cfg["features"]["sqlite_db_path"]
    conn = sqlite3.connect(db_path)
    table_name = f"bond_{code}"
    df.to_sql(table_name, conn, if_exists="replace", index=True)
    conn.close()


# ========================= 获取债券列表 =========================
def get_bond_list():
    """从 akshare 获取当前所有可转债列表"""
    bond_info = ak.bond_zh_cov()
    symbols = bond_info["债券代码"].tolist()
    names = bond_info["债券简称"].tolist()
    markets = [0 if s[:2] == "12" else 1 for s in symbols]
    return list(zip(markets, symbols, names))


# ========================= 信号量控制连接数 =========================
_tdx_semaphore = None


def get_tdx_semaphore():
    """获取 TDX 连接池信号量，避免同时连接过多"""
    global _tdx_semaphore
    if _tdx_semaphore is None:
        cfg = get_config()
        max_conn = cfg["features"].get("max_workers", 5)
        _tdx_semaphore = threading.Semaphore(max(max_conn - 1, 1))
    return _tdx_semaphore


# ========================= 多线程下载任务 =========================
def download_task(market, code, name, start_date, end_date, progress_idx, total):
    """单个线程的下载任务"""
    cfg = get_config()
    api = None
    ip_pool = TdxIpPool()
    sem = get_tdx_semaphore()

    sem.acquire()
    try:
        api = TdxHq_API(
            multithread=True,
            heartbeat=True,
            auto_retry=True
        )
        conn = ip_pool.connect(api)
        if conn is None:
            raise ConnectionError("通达信连接失败，IP 池全部不可达")

        existing_df = None
        if cfg["features"].get("incremental_update", True):
            base_path = cfg["data"]["base_path"]
            if cfg["features"].get("use_parquet", False):
                file_path = os.path.join(base_path, f"{code}.parquet")
            else:
                file_path = os.path.join(base_path, f"{code}.csv")
            if os.path.exists(file_path):
                try:
                    if file_path.endswith(".parquet"):
                        existing_df = pd.read_parquet(file_path)
                    else:
                        existing_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                except Exception as e:
                    print(f"读取已有数据失败 {code}: {e}")

        df = get_incremental_data(market, code, name, api, start_date, end_date, existing_df)

        if df is None or df.empty:
            return (code, False, "无数据")

        save_individual_data(df, code)
        save_summary_data(df)
        save_to_sqlite(df, code)

        update_progress(
            current=progress_idx,
            current_code=code,
            current_name=name,
            message=f"完成 {name}({code})"
        )
        return (code, True, None)
    except Exception as e:
        error_msg = str(e)
        with progress_lock:
            download_progress["errors"].append(f"{code}: {error_msg}")
        return (code, False, error_msg)
    finally:
        if api:
            try:
                api.disconnect()
            except Exception:
                pass
        sem.release()


# ========================= 主下载入口 =========================
def run_download(start_date=None, end_date=None):
    """运行下载流程，支持自定义日期覆盖配置"""
    cfg = get_config()

    if start_date is None:
        start_date = parse_date(cfg["data"].get("start_date"), datetime(2020, 1, 1))
    if end_date is None:
        end_date = parse_date(cfg["data"].get("end_date"), datetime.now())

    update_progress(status="running", current=0, errors=[])
    bond_list = get_bond_list()
    total = len(bond_list)
    update_progress(total=total)

    success_count = 0
    start_time = time.time()

    if cfg["features"].get("multi_thread", True):
        max_workers = cfg["features"].get("max_workers", 5)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for idx, (market, code, name) in enumerate(bond_list, 1):
                future = executor.submit(
                    download_task, market, code, name,
                    start_date, end_date, idx, total
                )
                futures[future] = (code, name)

            for future in as_completed(futures):
                code, name = futures[future]
                try:
                    result_code, success, error = future.result()
                    if success:
                        success_count += 1
                except Exception as e:
                    with progress_lock:
                        download_progress["errors"].append(f"{code}: 线程异常 {e}")
    else:
        ip_pool = TdxIpPool()
        api = TdxHq_API(
            multithread=False,
            heartbeat=True,
            auto_retry=True
        )
        api_connected = False
        for idx, (market, code, name) in enumerate(bond_list, 1):
            update_progress(
                current=idx, current_code=code, current_name=name,
                message=f"正在下载 {name}({code})"
            )
            try:
                if not api_connected:
                    conn = ip_pool.connect(api)
                    if conn is None:
                        raise ConnectionError("通达信连接失败，IP 池全部不可达")
                    api_connected = True

                existing_df = None
                if cfg["features"].get("incremental_update", True):
                    file_path = os.path.join(cfg["data"]["base_path"], f"{code}.csv")
                    if os.path.exists(file_path):
                        try:
                            existing_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                        except Exception:
                            pass

                df = get_incremental_data(market, code, name, api, start_date, end_date, existing_df)
                if df is not None and not df.empty:
                    save_individual_data(df, code)
                    save_summary_data(df)
                    save_to_sqlite(df, code)
                    success_count += 1
                else:
                    with progress_lock:
                        download_progress["errors"].append(f"{code}: 无数据")
            except Exception as e:
                with progress_lock:
                    download_progress["errors"].append(f"{code}: {e}")
        if api_connected:
            api.disconnect()

    elapsed = time.time() - start_time
    msg = f"下载完成！成功 {success_count}/{total} 只，耗时 {elapsed:.2f} 秒"
    update_progress(status="completed", message=msg)

    send_email_notification("可转债数据下载完成", msg)
    send_wecom_notification(msg)
    print(msg)
    return msg


# ========================= 命令行入口 =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="可转债数据下载工具")
    parser.add_argument("--start", type=str, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="结束日期 YYYY-MM-DD 或 'latest'")
    parser.add_argument("--config", type=str, default=None, help="配置文件路径")
    args = parser.parse_args()

    if args.config:
        load_config(args.config)
    else:
        load_config()

    sd = parse_date(args.start) if args.start else None
    ed = parse_date(args.end) if args.end else None
    run_download(start_date=sd, end_date=ed)
