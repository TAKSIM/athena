from pathlib import Path
import pandas as pd
import pytz
from os.path import join, exists


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


def fut_series_name(year, month):
    return str(year)[-2:] + f"{month:02}"


