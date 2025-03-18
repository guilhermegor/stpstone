# pypi.org libs
import os
from keyring import get_password
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.ww.exchange.markets.fmp import FMPWW


df_ = FMPWW(
    session=None,
    cls_db=None,
    token=get_password('FMP', 'API_KEY'),
    list_slugs=['AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','BRK.B','META','JNJ','V','UNH','XOM','JPM','WMT','PG','MA','HD','CVX','ABBV','PEP']
).source('stocks_ohlcv_yesterday', bl_debug=False, bl_fetch=True)
print(f'DF FMP_WW: \n{df_}')
