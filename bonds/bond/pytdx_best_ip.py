# coding: utf-8
# see https://github.com/rainx/pytdx/issues/38 IP 优选简易办法
# by yutianst

import datetime
import os
import sys

import pandas as pd
from rich import print

from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API

sys.path.append("..")
import user_config as ucfg

# 去重后的股票行情 IP 列表（按 ip+port 去重，保留首次出现的顺序）
_seen_stock = set()
stock_ip = [
    s for s in [
        {'ip': '114.80.149.19', 'port': 7709},
        {'ip': '114.80.149.22', 'port': 7709},
        {'ip': '114.80.149.84', 'port': 7709},
        {'ip': '114.80.80.222', 'port': 7709},
        {'ip': '115.238.56.198', 'port': 7709},
        {'ip': '115.238.90.165', 'port': 7709},
        {'ip': '117.184.140.156', 'port': 7709},
        {'ip': '119.147.164.60', 'port': 7709},
        {'ip': '123.125.108.23', 'port': 7709},
        {'ip': '123.125.108.24', 'port': 7709},
        {'ip': '124.160.88.183', 'port': 7709},
        {'ip': '180.153.18.17', 'port': 7709},
        {'ip': '180.153.18.170', 'port': 7709},
        {'ip': '180.153.18.171', 'port': 7709},
        {'ip': '180.153.18.172', 'port': 7709},
        {'ip': '180.153.39.51', 'port': 7709},
        {'ip': '202.108.253.131', 'port': 7709},
        {'ip': '218.108.47.69', 'port': 7709},
        {'ip': '218.108.50.178', 'port': 7709},
        {'ip': '218.108.98.244', 'port': 7709},
        {'ip': '218.6.170.55', 'port': 7709},
        {'ip': '218.75.126.9', 'port': 7709},
        {'ip': '218.9.148.108', 'port': 7709},
        {'ip': '221.194.181.176', 'port': 7709},
        {'ip': '58.67.221.146', 'port': 7709},
        {'ip': '59.173.18.69', 'port': 7709},
        {'ip': '60.12.136.250', 'port': 7709},
        {'ip': '60.191.117.167', 'port': 7709},
        {'ip': '61.135.142.88', 'port': 7709},
        {'ip': '61.152.107.168', 'port': 7721},
        {'ip': '61.152.249.56', 'port': 7709},
        {'ip': '61.153.144.179', 'port': 7709},
        {'ip': '61.153.209.138', 'port': 7709},
        {'ip': '61.153.209.139', 'port': 7709},
        {'ip': '103.24.178.242', 'port': 7709},
        {'ip': 'hq.cjis.cn', 'port': 7709},
        {'ip': 'jstdx.gtjas.com', 'port': 7709},
        {'ip': 'shtdx.gtjas.com', 'port': 7709},
    ] if not (s['ip'], s['port']) in _seen_stock and not _seen_stock.add((s['ip'], s['port']))
]

future_ip = [
    {'ip': '106.14.95.149', 'port': 7727, 'name': '扩展市场上海双线'},
    {'ip': '112.74.214.43', 'port': 7727, 'name': '扩展市场深圳双线1'},
    {'ip': '119.147.86.171', 'port': 7727, 'name': '扩展市场深圳主站'},
    {'ip': '119.97.185.5', 'port': 7727, 'name': '扩展市场武汉主站1'},
    {'ip': '120.24.0.77', 'port': 7727, 'name': '扩展市场深圳双线2'},
    {'ip': '124.74.236.94', 'port': 7721},
    {'ip': '202.103.36.71', 'port': 443, 'name': '扩展市场武汉主站2'},
    {'ip': '47.92.127.181', 'port': 7727, 'name': '扩展市场北京主站'},
    {'ip': '59.175.238.38', 'port': 7727, 'name': '扩展市场武汉主站3'},
    {'ip': '61.152.107.141', 'port': 7727, 'name': '扩展市场上海主站1'},
    {'ip': '61.152.107.171', 'port': 7727, 'name': '扩展市场上海主站2'},
    {'ip': '119.147.86.171', 'port': 7721, 'name': '扩展市场深圳主站'},
    {'ip': '47.107.75.159', 'port': 7727, 'name': '扩展市场深圳双线3'},
]


def ping(ip, port=7709, type_='stock'):
    """
    测试单个 pytdx IP 的连通性和响应速度。

    Parameters
    ----------
    ip : str  服务器 IP 或域名。
    port : int  端口号，默认 7709。
    type_ : str  'stock' 测股票行情，'future' 测扩展市场。

    Returns:
        datetime.timedelta or None
        成功返回耗时 timedelta，失败返回 None。
    """
    t0 = datetime.datetime.now()
    if type_ == 'stock':
        api = TdxHq_API()
        try:
            if api.connect(ip, port, time_out=0.7):
                res = api.get_security_list(0, 1)
                api.disconnect()
                if res is not None and len(res) > 800:
                    cost = datetime.datetime.now() - t0
                    print(f'GOOD RESPONSE {ip}  cost={cost.total_seconds():.3f}s')
                    return cost
            print(f'BAD RESPONSE {ip}')
            return None
        except Exception as e:
            _handle_ping_error(e, ip)
            return None
    elif type_ == 'future':
        api = TdxExHq_API()
        try:
            if api.connect(ip, port, time_out=0.7):
                res = api.get_instrument_count()
                api.disconnect()
                if res is not None and res > 20000:
                    cost = datetime.datetime.now() - t0
                    print(f'GOOD FUTURE RESPONSE {ip}  cost={cost.total_seconds():.3f}s  count={res}')
                    return cost
            print(f'BAD FUTURE RESPONSE {ip}')
            return None
        except Exception as e:
            _handle_ping_error(e, ip)
            return None


def _handle_ping_error(e, ip):
    """统一的 ping 异常处理"""
    if isinstance(e, TypeError):
        print(e)
        print('pytdx 版本不兼容，请重新安装: pip uninstall pytdx && pip install pytdx')
    else:
        print(f'BAD RESPONSE {ip}  {type(e).__name__}')


def select_best_ip(_type='stock'):
    """
    测速选出最快的 pytdx 服务器 IP。

    第一阶段对所有 IP 做 ping（connect + 简单查询），
    第二阶段对前 5 个候选做批量证券报价验证（仅当本地缓存路径可用时）。

    Parameters
    ----------
    _type : str  'stock' 测股票，'future' 测扩展市场。

    Returns:
        dict or None
        最优 IP 信息字典 {'ip', 'port'}，全失败返回 None。
    """
    ip_list = stock_ip if _type == 'stock' else future_ip

    # 第一阶段：ping 测速
    timed_out = datetime.timedelta(9, 9, 0)
    data = [(ping(x['ip'], x['port'], _type) or timed_out, x) for x in ip_list]
    results = sorted(
        [(cost, info) for cost, info in data if cost < timed_out],
        key=lambda x: x[0]
    )

    if not results:
        print('没有找到可达的服务器')
        return None

    print(f'\n第一阶段完成：{len(results)}/{len(ip_list)} 个 IP 可达')
    for i, (cost, info) in enumerate(results[:5], 1):
        print(f'  {i}. {info["ip"]}:{info["port"]}  cost={cost.total_seconds():.3f}s')

    # 第二阶段：批量取报价验证（仅在配置完整时执行）
    try:
        csv_dir = ucfg.tdx['csv_lday']
        if os.path.isdir(csv_dir):
            _verify_quotes(results[:5], _type)
    except (AttributeError, KeyError, TypeError):
        print('跳过第二阶段验证（ucfg.tdx.csv_lday 未配置）')

    return results[0][1]


def _verify_quotes(candidates, _type='stock'):
    """
    对候选 IP 做 get_security_quotes 批量验证，取数据最完整的那个。
    不中断第一阶段选出的顺序，只做信息输出。
    """
    if _type != 'stock':
        return
    try:
        csv_dir = ucfg.tdx['csv_lday']
        stock_files = [f[:-4] for f in os.listdir(csv_dir) if f.endswith('.csv')]
    except Exception:
        return

    stock_codes = []
    for code in stock_files:
        if code[:1] == '6':
            stock_codes.append((1, code))
        elif code[:1] in ('0', '3'):
            stock_codes.append((0, code))

    if not stock_codes:
        return

    print(f'\n第二阶段：用 {len(stock_codes)} 只本地股票验证候选 IP...')
    api = TdxHq_API()
    for cost, info in candidates:
        if not api.connect(info['ip'], info['port'], time_out=3.0):
            api.disconnect()
            continue

        dfs = []
        for i in range(0, len(stock_codes), 80):
            batch = stock_codes[i:i + 80]
            try:
                df = api.to_df(api.get_security_quotes(batch))
                dfs.append(df)
            except Exception:
                pass
        api.disconnect()

        if dfs:
            total = len(pd.concat(dfs, ignore_index=True))
            print(f'  {info["ip"]}  获取 {total} 只股票行情')
        else:
            print(f'  {info["ip"]}  获取失败')


if __name__ == '__main__':
    # 仅运行第一阶段测速，快速、无外部依赖
    ip = select_best_ip('stock')
    print(ip)
    # ip = select_best_ip('future')
    # print(ip)
