# pypi.org libs
import os

from keyring import get_password


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.us.exchange.alphavantage import AlphaVantageUS
from stpstone.utils.cals.cal_abc import DatesBR


df_ = AlphaVantageUS(
    session=None,
    date_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 5),
    cls_db=None,
    token=get_password('ALPHAVANTAGE', 'API_KEY'),
    list_slugs=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
).source('ohlcv_not_adjusted', bool_debug=False, bool_fetch=True)
print(f'DF ALPHAVANTAGE: \n{df_}')
