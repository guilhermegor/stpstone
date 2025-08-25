# pypi.org libs
import os

from keyring import get_password


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.us.exchange.tiingo import TiingoUS
from stpstone.utils.cals.cal_abc import DatesBR


df_ = TiingoUS(
    session=None,
    date_start=DatesBR().sub_working_days(DatesBR().curr_date(), 52),
    date_end=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
    cls_db=None,
    token=get_password('TIINGO', 'API_KEY'),
    list_slugs=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META']
).source('ohlcv_adjusted', bool_debug=False, bool_fetch=True)
print(f'DF TIINGO: \n{df_}')
