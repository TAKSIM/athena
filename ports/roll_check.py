import futu
from procs.ft import get_futures_info
import datetime as dt

code_map = {'HK.HSI': 'HSIF', }


def get_position():
    with futu.OpenFutureTradeContext() as ctx:
        ret, position = ctx.position_list_query()
        if ret == futu.RET_OK:
            pos = position[['code', 'qty', 'currency']].set_index(['code'])
            return pos
        else:
            raise Exception("获取持仓数据时出错")


if __name__ == "__main__":
    pos = get_position()
    td = dt.date.today()
    for code, p in pos.iterrows():
        if p['qty'] != 0:
            if code.split('.')[0] != 'US':
                info = get_futures_info(code)
                last_trade_time = info['last_trade_time'][0]
                days_left = (dt.date.fromisoformat(last_trade_time) - td).days
                print('{0}: 最后交易日{1}，还剩{2}天'.format(info['name'][0], last_trade_time, days_left))
