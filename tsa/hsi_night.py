import futu
from utils import get_ts, fut_series_name, data_root
from datetime import timedelta
import datetime as dt
from WindPy import w
from procs.ft import load_hist_data
from os import listdir, path
import pandas as pd
import arrow as ar


# 夜盘策略1：夜盘期货的高水和低水有相对稳定的极值，适合晚上放限价单
# 夜盘策略2：夜盘在美股开市前后的1分钟，走势往往相反

def get_usopen_time(as_of_date=None):
    # 美国市场开市对应的中国时间，夏令时是晚上9点半，冬令时是晚上10点半
    aod = as_of_date or dt.date.today()
    us_open_time = ar.get(dt.datetime.combine(aod, dt.time(9,30,0)), 'US/Eastern')
    china_time = us_open_time.to('Asia/Shanghai')
    return china_time.strftime('%Y-%m-%d %H:%M:%S')


def get_active_series_range(year, month):
    # 起始时间是上一个系列最后交易日早上9:15
    # 结束时间是本系列最后一个交易日凌晨3:00
    # 最后一个交易日晚上没有夜盘
    if not w.isconnected():
        w.start()
    last_month = dt.date(year, month, 1) - timedelta(days=1)
    this_series = fut_series_name(year, month)  # 上一个系列
    last_series = fut_series_name(last_month.year, last_month.month)  # 当下系列
    series = ['HSIF{0}.HK'.format(a) for a in [last_series, this_series]]
    sed = w.wss(series, 'ltdate_new').Data
    start_time = sed[0][0].date().isoformat() + ' 09:15:00'
    end_time = sed[0][1].date().isoformat() + ' 03:00:00'
    return start_time, end_time


def load_hsi_futs(year, month):
    # 保存给定年月的活跃恒生指数合约1分钟数据，包括夜盘
    start_time, end_time = get_active_series_range(year, month)
    start_date = start_time.split(' ')[0]
    end_date = end_time.split(' ')[0]
    series_name = fut_series_name(year, month)
    code = 'HK.HSI' + series_name
    data = load_hist_data(code, start_date, end_date, ktype=futu.KLType.K_1M)
    data = data[(data.index >= start_time) & (data.index <= end_time)]
    return data


def save_hsi_futs(year, month):
    # 保存活跃合约数据
    data = load_hsi_futs(year, month)
    name = get_active_series(dt.date(year, month, 1))
    data.to_pickle(path.join(data_root, '1min', 'index', name))


def get_active_series(as_of_date=None):
    # 获取指定日期的活跃合约
    aod = as_of_date or dt.date.today()
    series = fut_series_name(aod.year, aod.month)
    return 'HK.HSI' + series


def get_main_series():
    # 把所有活跃合约的数据拼凑在一起，成为主连
    pth = path.join(data_root, '1min', 'index')
    data_files = listdir(pth)
    hsi_files = [f for f in data_files if f.startswith('HK.HSI')]
    hsi_data = [get_ts(f, 'index', '1min') for f in hsi_files]
    return pd.concat(hsi_data, ignore_index=False).sort_index(ascending=True)


def get_night_high_low(trd_date, hsi_fut):
    # 获取指定交易日恒生指数期货夜盘的最高和最低
    start_time = trd_date.isoformat() + ' 17:15:00'
    end_time = (trd_date + timedelta(days=1)).isoformat() + ' 03:00:00'
    night = hsi_fut[(hsi_fut.index >= start_time) & (hsi_fut.index <= end_time)]
    night_high = night['high'].max()
    night_low = night['low'].min()
    return night_high, night_low


def get_usopen_klines(trd_date, hsi_fut):
    us_open = get_usopen_time(trd_date)
    before_open = ar.get(us_open).shift(minutes=-1).strftime('%Y-%m-%d %H:%M:%S')
    return hsi_fut.loc[before_open], hsi_fut.loc[us_open]


if __name__ == '__main__':
    start_date = dt.date(2023, 12, 28)
    end_date = dt.date(2024, 3, 10)
    hsi = get_ts('HSI.HI', 'index', '1d')
    diffs = hsi['close'].diff()
    hsi = hsi[(hsi.index >= start_date) & (hsi.index <= end_date)]
    diffs = diffs[(diffs.index >= start_date) & (diffs.index <= end_date)]
    hsi_fut = get_main_series()
    result = []
    for t, d in hsi.iterrows():
        close = d['close']
        # night_high, night_low = get_night_high_low(t, hsi_fut)
        # result.append((t, close, night_high, night_low, diffs.loc[t]))
        before, after = get_usopen_klines(t, hsi_fut)
        # night_open = hsi_fut.loc[t.isoformat() + ' 17:16:00']
        result.append((t, close, diffs.loc[t], #night_open['open'],
                       before['open'], before['high'], before['low'], before['close'],
                       after['open'], after['high'], after['low'], after['close']))
    hls = pd.DataFrame(result, columns=['date', 'close', 'change', 'bo', 'bh', 'bl', 'bc', 'ao', 'ah', 'al', 'ac'])
    hls.to_excel(path.join(data_root, 'highlows2.xlsx'))


