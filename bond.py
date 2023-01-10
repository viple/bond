import os
from datetime import datetime
import pandas as pd
import akshare as ak
from pytdx.hq import TdxHq_API
import warnings
import csv

#忽略警报
warnings.filterwarnings("ignore")

# 显示设置
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

# 参数 股市最佳ip 期货最佳ip
# stock_ip = [{'ip': '114.80.149.19', 'port': 7709},{'ip': '114.80.149.22', 'port': 7709},{'ip': '114.80.149.84', 'port': 7709},{'ip': '114.80.80.222', 'port': 7709},
# {'ip': '115.238.56.198', 'port': 7709},{'ip': '115.238.90.165', 'port': 7709},{'ip': '117.184.140.156', 'port': 7709},{'ip': '119.147.164.60', 'port': 7709},
# {'ip': '123.125.108.23', 'port': 7709},{'ip': '123.125.108.24', 'port': 7709},{'ip': '124.160.88.183', 'port': 7709},{'ip': '180.153.18.17', 'port': 7709},
# {'ip': '180.153.18.170', 'port': 7709},{'ip': '180.153.18.171', 'port': 7709},{'ip': '180.153.39.51', 'port': 7709},{'ip': '218.108.47.69', 'port': 7709},
# {'ip': '218.108.50.178', 'port': 7709},{'ip': '218.108.98.244', 'port': 7709},{'ip': '218.75.126.9', 'port': 7709},{'ip': '218.9.148.108', 'port': 7709},
# {'ip': '221.194.181.176', 'port': 7709},{'ip': '59.173.18.69', 'port': 7709},{'ip': '60.12.136.250', 'port': 7709},{'ip': '60.191.117.167', 'port': 7709},
# {'ip': '61.135.142.88', 'port': 7709},{'ip': '61.152.107.168', 'port': 7721},{'ip': '61.152.249.56', 'port': 7709},{'ip': '61.153.144.179', 'port': 7709},
# {'ip': '61.153.209.138', 'port': 7709},{'ip': '61.153.209.139', 'port': 7709},{'ip': 'hq.cjis.cn', 'port': 7709},{'ip': 'jstdx.gtjas.com', 'port': 7709},
# {'ip': 'shtdx.gtjas.com', 'port': 7709},{'ip': '180.153.18.170', 'port': 7709},{'ip': '180.153.18.171', 'port': 7709},{'ip': '180.153.18.172', 'port': 7709},
# {'ip': '202.108.253.131', 'port': 7709},{'ip': '60.191.117.167', 'port': 7709},{'ip': '115.238.90.165', 'port': 7709},{'ip': '218.108.98.244', 'port': 7709},
# {'ip': '123.125.108.23', 'port': 7709},{'ip': '123.125.108.24', 'port': 7709},{'ip': '58.67.221.146', 'port': 7709},{'ip': '103.24.178.242', 'port': 7709},
# {'ip': '103.24.178.242', 'port': 7709},{'ip': '218.6.170.55', 'port': 7709}, ]
# future_ip = [{'ip': '106.14.95.149', 'port': 7727, 'name': '扩展市场上海双线'},{'ip': '112.74.214.43', 'port': 7727, 'name': '扩展市场深圳双线1'},
# {'ip': '119.147.86.171', 'port': 7727, 'name': '扩展市场深圳主站'},{'ip': '119.97.185.5', 'port': 7727, 'name': '扩展市场武汉主站1'},
# {'ip': '120.24.0.77', 'port': 7727, 'name': '扩展市场深圳双线2'},{'ip': '124.74.236.94', 'port': 7721},
# {'ip': '202.103.36.71', 'port': 443, 'name': '扩展市场武汉主站2'},{'ip': '47.92.127.181', 'port': 7727, 'name': '扩展市场北京主站'},
# {'ip': '59.175.238.38', 'port': 7727, 'name': '扩展市场武汉主站3'},{'ip': '61.152.107.141', 'port': 7727, 'name': '扩展市场上海主站1'},
# {'ip': '61.152.107.171', 'port': 7727, 'name': '扩展市场上海主站2'},{'ip': '119.147.86.171', 'port': 7721, 'name': '扩展市场深圳主站'},
# {'ip': '47.107.75.159', 'port': 7727, 'name': '扩展市场深圳双线3'}]"""###
ip = "117.184.140.156"
# 设置多线程，心跳包，自动重连
api = TdxHq_API(multithread=True,heartbeat=True,auto_retry=True)
if api.connect('117.184.140.156', 7709):
    api.disconnect()
# 设置日线
category=9
# 设置开始结束时间
start_date = datetime(2020,1,1)
end_date = datetime(2023,1,6)

# 从东方财富获取数据，单次返回当前交易时刻的所有可转债数据
bond_info = ak.bond_zh_cov()
symbols = bond_info['债券代码'].to_list
names = bond_info['债券简称'].to_list
# print(symbols,names)
# pytdx 市场代码 0 深圳 1 上海
markets = [0 if i[:2] == '12' else 1 for i in symbols()]
para_list = list(zip(markets,symbols(),names()))
#print(para_list)

# 创建文件存储路径
def  create_path(ak_code):
    date_str = str(pd.to_datetime(start_date).date())
    #形成日期字符串
    path = os.path.join(".","all_stock_candle",'bond',date_str)
    #保存数据
    if not os.path.exists(path):
        os.makedirs(path)#建立多级文件夹
    file_name = ak_code + ".csv"
    return os.path.join(path,file_name)

def  create_pathy(datey):
    path = os.path.join(".","all_stock_candle",'bond')
    #保存数据
    if not os.path.exists(path):
        os.makedirs(path)#建立多级文件夹
    file_name = datey + ".csv"
    return os.path.join(path,file_name)

#从通达信循环获取K线数据，当数据取完或到指定范围时终止
def get_dars(code,name,market,category,startTime=start_date,endTime=end_date):
    start = 1
    count = 800
    data = []
    
    while True:
        list_data = api.get_security_bars(category,market,code,start,count)
        if(list_data is None) or (len(list_data)==0):
            #连接可能断开重新取一次
            #api.connect(ip,7709)
            list_data = api.get_security_bars(category,market,code,start,count)
            if (list_data is None) or (len(list_data)==0):
                break
        data += list_data
        start += 800
        #if pd.to_datetime(list_data[0]['datetime'])<=startTime:
            #break

    if len(data)>0:
        #转为dataframe保存        
        data = api.to_df(data)
        #无交易时返回0
        data.loc[data['amount']<= 0.01,['vol','amount']]=0
        data = data.set_index('datetime')
        data.index = pd.to_datetime(data.index)
        data = data.sort_index()
        date = data.loc[startTime:endTime]
        #print(data.head())
        data['name'] = name
        #计算涨幅
        data['rise'] = ((data['close']-data['close'].shift(1))/data['close'].shift(1))
        data['rise'][0] = data['close'][0]/100-1
        #计算振幅
        data['Amp'] = (data['high']-data['low'])/data['close'].shift(1)
        data['Amp'][0] = (data['high'][0]-data['low'][0])/100
        data =data[['name','open','high','low','close','vol','amount','rise','Amp']]
        #print(data.index)        
        if len(data)>=1:
            path = create_path(code)
            data.to_csv(path,index=True,mode='w',encoding='gbk')
            list = data.index.date
            for i in list:
                datey = str(i)
                #print(datey)
                pathy = create_path(datey)
                data.to_csv(pathy,index=True,mode='w',encoding='gbk')
# 设置主函数
if __name__ == '__main__':
    for i in range (len(para_list)):
        market = para_list[i][0]
        code = para_list[i][1]
        name = para_list[i][2]
        get_dars(code,name,market,category,startTime=start_date,endTime=end_date)
# 流量统计
# api.get_traffic_stats()
#dateyy = data.groupby('datetime').name.apply(lambda x : x.tolist()).to_frame()['name'].apply(pd.Series).add_prefix('datetime').fillna(' ')
