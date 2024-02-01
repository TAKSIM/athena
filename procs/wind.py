from WindPy import w
import pandas as pd
import datetime as dt
from procs.tdx import get_ts
import os
from utils import data_root, daily_data_cols, timezones
import pytz


def update_index_data():
    industries = pd.read_pickle(os.path.join(data_root, 'sw2021_industry'))
    sw_codes = industries[industries['tier'] == 1].index.to_list()
    hk_us_indices = ['DJI.GI', 'SPX.GI', 'IXIC.GI', 'HSI.HI']
    codes = sw_codes + hk_us_indices
    update_local_data(codes)


def update_local_data(codes, as_of_date=None):
    # 用万得来更新本地数据，每个代码都是先读取历史数据，然后再用万得取新数据，然后合并存储
    aod = as_of_date or dt.date.today()
    now = dt.datetime.now(tz=pytz.timezone('Asia/Shanghai'))

    if not w.isconnected():
        w.start()
    err, exchs = w.wss(codes, 'exch_eng,country', usedf=True)
    if err == 0:
        exchs = exchs.replace('SYWG', 'SSE')  # 申万指数用上交所的日历
    else:
        raise Exception('获取指数对应的交易所时出现问题')

    for code in codes:
        old_data = get_ts(code, 'index')
        # 计算起始日
        if old_data.empty:
            print(f'本地没有{code}的数据，默认从2021年1月1日起下载')
            start_date = dt.date(2021, 1, 1)
        else:
            start_date = max(old_data.index) + dt.timedelta(days=1)
        # 计算截止日
        exch = exchs.loc[code]['EXCH_ENG']
        tz = timezones[exchs.loc[code]['COUNTRY']]
        last_trd_day = w.tdaysoffset(0, aod.isoformat(), 'TradingCalendar={0}'.format(exch))
        if last_trd_day.ErrorCode != 0:
            raise Exception('获取指数{0}的最后交易日时出错'.format(code))
        end_date = last_trd_day.Data[0][0].date()
        last_trd_time = dt.datetime.combine(end_date, dt.time(17, 0, 0), tz)
        # 不符合逻辑的时候，不更新
        if now <= last_trd_time:
            print('{0}在{1}日的交易还未收盘，更新至前一交易日收盘'.format(code, end_date))
            end_date = w.tdaysoffset(-1, end_date.isoformat(), 'TradingCalendar={0}'.format(exch)).Data[0][0].date()
        if start_date > end_date:
            print('{0}的数据更新起始日期{1}大于程序运行日{2}，不做任何处理'.format(code, start_date, aod))
            continue

        err, data = w.wsd(code, ','.join(daily_data_cols), start_date.isoformat(), end_date.isoformat(), usedf=True)
        if err == 0:
            file_path = os.path.join(data_root, '1d', 'index', code)
            data.columns = daily_data_cols
            if len(data) == 1:
                # 只有一天的数据时，万得返回的DataFrame是code作为index，而不是日期
                old_data.loc[end_date] = data.loc[code]
                old_data.to_pickle(file_path)
            else:
                new_data = pd.concat([old_data, data])
                new_data.to_pickle(file_path)
            print(f'更新{code}的数据从{start_date}到{end_date}')
        else:
            raise Exception(f'万得读取{code}的数据时出错，错误代码{err}')


if __name__ == '__main__':
    update_index_data()
