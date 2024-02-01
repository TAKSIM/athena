from pathlib import Path
import pytz


root = Path(__file__).parent.parent.parent
temp_file = str(root.joinpath('temp'))
data_root = str(root.joinpath('data'))
daily_data_cols = ['open', 'high', 'low', 'close', 'volume', 'amt']

timezones = {'US': pytz.timezone('US/Eastern'),
             'CN': pytz.timezone('Asia/Shanghai'),
             'HK': pytz.timezone('Asia/Shanghai'),
             'JP': pytz.timezone('Asia/Tokyo')}
