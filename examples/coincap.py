# pypi.org libs
import os

from keyring import get_password


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.ww.exchange.crypto.coincap import CoinCap
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


cls_session = YieldFreeProxy(bool_new_proxy=True)

if cls_session.session.proxies == {}:
    raise Exception("No proxies available")
print(f"Proxies available: {cls_session.session.proxies}")

df_ = CoinCap(
    session=cls_session.session,
    date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 5),
    cls_db=None,
    token=get_password('COIN_CAP', 'API_KEY')
).source('ohlcv_latest', bool_fetch=True)
print(f'DF MKTDATA COINCAP: \n{df_}')
