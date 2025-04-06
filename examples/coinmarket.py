# pypi.org libs
import os
from keyring import get_password
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.ww.exchange.crypto.coinmarket import CoinMarket
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.proxy_scrape import ProxyScrapeAll


session = ProxyScrapeAll(bl_new_proxy=True).session
df_ = CoinMarket(
    session=session,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5),
    cls_db=None,
    token=get_password('COIN_MARKET', 'API_KEY')
).source('ohlcv_latest', bl_debug=False, bl_fetch=True)
print(f'DF MKTDATA COIN MARKET: \n{df_}')
