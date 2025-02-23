# pypi.org libs
import os
from keyring import get_password
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.us.exchange.alphavantage import AlphaVantageUS
from stpstone.utils.cals.handling_dates import DatesBR


df_ = AlphaVantageUS(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5), 
    cls_db=None, 
    token=get_password('ALPHAVANTAGE', 'API_KEY'), 
    list_slugs=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
).source('ohlcv_not_adjusted', bl_debug=False, bl_fetch=True)
print(f'DF ALPHAVANTAGE: \n{df_}')