from pathlib import Path
import pandas as pd
import pytz
from os.path import join, exists
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import arrow as ar


root = Path(__file__).parent.parent.parent
temp_file = str(root.joinpath('temp'))
data_root = str(root.joinpath('data'))
daily_data_cols = ['open', 'high', 'low', 'close', 'volume', 'amt']

timezones = {'US': pytz.timezone('US/Eastern'),
             'CN': pytz.timezone('Asia/Shanghai'),
             'HK': pytz.timezone('Asia/Shanghai'),
             'JP': pytz.timezone('Asia/Tokyo')}


def get_ts(code, inst_type, freq=None, start=None, end=None):
    # 获取time series
    file_path = join(data_root, freq or '1d', inst_type, code)
    if exists(file_path):
        data = pd.read_pickle(file_path)
    else:
        return pd.DataFrame(columns=daily_data_cols)

    if start:
        data = data[data.index >= start]
    if end:
        data = data[data.index <= end]
    return data


def plot_ts(time_series):
    sns.lineplot(data=time_series, x=time_series.index, y=time_series.values)
    plt.show()


def win_ratio(series, start_date, end_date):
    # 核算一个价格时间序列的胜率
    if type(series.index[0]) != str:
        series.index = [i.isoformat() for i in series.index]
    data = series[(series.index >= start_date) & (series.index <= end_date)]
    rtn = data.diff()
    positive_count = rtn[rtn > 0].count()
    win_ratio = positive_count / rtn.count()
    return win_ratio


def fut_series_name(year, month):
    return str(year)[-2:] + f"{month:02}"


def big_vs_small(big_code, small_code, start_date=None, end_date=None):
    ed = end_date or dt.date.today().isoformat()
    sd = start_date or ar.get(ed).shift(years=-1).date().isoformat()
    big = get_ts(big_code, 'index')['close']
    small = get_ts(small_code, 'index')['close']
    big_rtn = big.pct_change()
    small_rtn = small.pct_change()
    rtn_diff = (big_rtn - small_rtn).dropna()
    rtn_diff = rtn_diff[(rtn_diff.index >= sd) & (rtn_diff.index <= ed)]
    return rtn_diff.cumsum()


