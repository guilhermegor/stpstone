# pypi.org libs
import os
from keyring import get_password
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.ww.exchange.crypto.coincap import CoinCap
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ProxyScrapeAll, ProxyScrapeCountry
from stpstone.utils.connections.netops.sessions.proxy_nova import ProxyNova


cls_session = ProxyScrapeAll(bl_new_proxy=True)

if cls_session.session.proxies == {}:
    raise Exception("No proxies available")
print(f"Proxies available: {cls_session.session.proxies}")

df_ = CoinCap(
    session=cls_session.session,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5),
    cls_db=None,
    token=get_password('COIN_CAP', 'API_KEY')
).source('ohlcv_latest', bl_fetch=True)
print(f'DF MKTDATA COINCAP: \n{df_}')
