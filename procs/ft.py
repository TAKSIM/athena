import futu
import pandas as pd
from os.path import join, exists
from utils import daily_data_cols, data_root
import datetime as dt
# 从富途的API读取数据并规整为统一格式


def load_hist_data(code, start, end, ktype=None):
    result = pd.DataFrame()
    with futu.OpenQuoteContext() as ctx:
        ret, data, page_req_key = ctx.request_history_kline(code, start, end, ktype=ktype or futu.KLType.K_DAY)
        if ret == futu.RET_OK:
            result = pd.concat([result, data], ignore_index=False)
        else:
            raise Exception('获取数据时出错：{0}'.format(data))
        while page_req_key:
            ret, data, page_req_key = ctx.request_history_kline(code, start, end, ktype=futu.KLType.K_1M,
                                                                page_req_key=page_req_key)
            if ret == futu.RET_OK:
                result = pd.concat([result, data], ignore_index=False)
    hists = result[['time_key', 'open', 'high', 'low', 'close', 'volume', 'turnover']].set_index('time_key')
    hists.columns = daily_data_cols
    return hists


def get_futures_info(code):
    with futu.OpenQuoteContext() as ctx:
        ret, info = ctx.get_future_info([code])
        if ret == futu.RET_OK:
            return info
        else:
            # raise Exception('获取期货合约{0}的数据时出错：{1}'.format(code, info))
            print(info)
            return None


def update_data(code, inst_type, freq):
    file = join(data_root, freq, inst_type, code)
    data = None
    if exists(file):
        data = pd.read_pickle(file)
    end_date = dt.date.today().isoformat()
    start_date = data is None and '2023-01-01' or data.index.max().split(' ')[0]
    ktype = freq == '1min' and futu.KLType.K_1M or futu.KLType.K_1M or futu.KLType
    new_data = load_hist_data(code, start_date, end_date, ktype=ktype)
    if data is not None:
        new_data = new_data[~new_data.index.isin(data.index)]
        if new_data.empty:  # 如果没有新的数据，都已经涵盖在了原始数据中国
            return data
        new_data = pd.concat([data, new_data], verify_integrity=True).sort_index(ascending=True)
    new_data.to_pickle(file)
    print('存储新数据：{0}，{1}至{2}'.format(code, start_date, end_date))
    return new_data


if __name__ == '__main__':
    update_data('HK.HSI2403', 'index', '1min')


