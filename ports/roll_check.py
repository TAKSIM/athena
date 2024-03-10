import futu
from procs.ft import get_futures_info
import pandas as pd
import datetime as dt
from WindPy import w

code_map = {'HK.HSI': 'HSIF', }
month_map = {'01': 'F', '02': 'G', '03': 'H', '04': 'J', '05': 'K', '06': 'M',
             '07': 'N', '08': 'Q', '09': 'U', '10': 'V', '11': 'X', '12': 'Z'}
# COMEX黄金期货的代码从富途转为万得：从类似于US.GC2403转换为GCH24E.CMX
gold_code_converter = lambda futu_code: 'GC' + month_map[futu_code[-2:]] + futu_code[-4:-2] + 'E.CMX'
nq_code_converter = lambda futu_code: 'NQ' + month_map[futu_code[-2:]] + futu_code[-4:-2] + 'E.CME'


def get_position():
    with futu.OpenFutureTradeContext() as ctx:
        ret, position = ctx.position_list_query()
        if ret == futu.RET_OK:
            pos = position[['code', 'qty', 'currency']].set_index(['code'])
            return pos
        else:
            raise Exception("获取持仓数据时出错")


def convert_code(futu_code):
    # 将富途的证券代码转换为万得的证券代码，富途的很多数据要收钱，有些贵，万得里面都有
    region, code = futu_code.split('.')
    series = code[-4:]
    inst = code[:-4]
    if inst == 'GC':
        return gold_code_converter(futu_code)
    elif inst == 'NQ':
        return nq_code_converter(futu_code)
    else:
        raise Exception("未编写转换功能的富途代码")


def list_maturities(futu_code_list):
    pos = get_position()
    td = dt.date.today()
    wind_codes = []
    data = []
    # 先用富途的api获取有权限的资产信息
    for code in futu_code_list:
        info = get_futures_info(code)
        if info is None:
            wc = convert_code(code)
            wind_codes.append(wc)
        else:
            last_trade_time = info['last_trade_time'][0]
            days_left = (dt.date.fromisoformat(last_trade_time) - td).days
            data.append([info['name'][0], last_trade_time, days_left])
    # 有富途api无法获取的资产信息，大概率是没有数据权限，转用万得
    if wind_codes:
        if not w.isconnected():
            w.start()
        sec_info = w.wss(wind_codes, "sec_name,lasttrade_date")
        if sec_info.ErrorCode == 0:
            for i, name in enumerate(sec_info.Data[0]):
                last_trade_time = sec_info.Data[1][i].date()
                days_left = (last_trade_time - td).days
                data.append([name, last_trade_time, days_left])
        else:
            raise Exception('万得无法获得相关资产的信息：{0}'.format(wind_codes))

    return pd.DataFrame(data, columns=['名称', '最后交易日', '剩余天数']).set_index('名称').sort_values(
        by=['剩余天数'], ascending=True)


if __name__ == "__main__":
    pos = get_position()
    code_list = [code for code, p in pos.iterrows() if p['qty'] != 0]
    result = list_maturities(code_list)
    print(result)


