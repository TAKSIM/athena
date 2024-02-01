# 处理通达信客户端输出的数据
import pandas as pd
import numpy as np
from os import listdir, remove
from os.path import join, exists
from utils import temp_file, data_root, daily_data_cols


def parse_daily_data(code='000300.SH', file_path=None):
    # daily data from 国信证券终端（通达信）
    fn = file_path or join(temp_file, code + '.csv')
    data = pd.read_csv(fn, index_col='date', names=['date', 'open', 'high', 'low', 'close', 'volumn', 'amt'],
                       parse_dates=['date'], skipfooter=1, encoding='gb2312', engine='python')
    if data.empty:
        return None
    else:
        data.index = data.index.date
        return data


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


def convert_data(path):
    # 在data root的1d下面，搜索所有csv文件，更新到对应的pickle文件中。对于申万一级行业数据，从万得下载后更新
    files = [f for f in listdir(path) if f.endswith('.csv')]
    i = 0
    total = len(files)
    progress = np.floor(total / 10)
    for f in files:
        code = f[:-4]
        fp = join(path, f)
        if code.split('.')[1] != 'BJ':  # 不要北交所的
            data = parse_daily_data(file_path=fp)
            if data is not None:  # 新股上市的是空文件
                data_path = join(path, code)
                if exists(data_path):
                    old_data = pd.read_pickle(data_path)
                    data = old_data.combine_first(data)
                data.to_pickle(data_path)
        remove(fp)
        i += 1
        if i % progress == 0:
            print('{:.0%}'.format(i/total))


def convert_daily_data():
    convert_data(join(data_root, '1d', 'stock'))
    convert_data(join(data_root, '1d', 'index'))
