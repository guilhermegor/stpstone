# pypi.org libs
import os
from keyring import get_password
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.us.exchange.tiingo import TiingoUS
from stpstone.utils.cals.handling_dates import DatesBR


df_ = TiingoUS(
    session=None,
    dt_inf=DatesBR().sub_working_days(DatesBR().curr_date, 52), 
    dt_sup=DatesBR().sub_working_days(DatesBR().curr_date, 1), 
    cls_db=None, 
    token=get_password('TIINGO', 'API_KEY'), 
    list_slugs=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META']
).source('ohlcv_adjusted', bl_debug=False, bl_fetch=True)
print(f'DF TIINGO: \n{df_}')