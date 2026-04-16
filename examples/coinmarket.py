# pypi.org libs
import os

from keyring import get_password


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.ww.exchange.crypto.coinmarket import CoinMarket
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


session = YieldFreeProxy(bool_new_proxy=True).session
df_ = CoinMarket(
	session=session,
	date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 5),
	cls_db=None,
	token=get_password("COIN_MARKET", "API_KEY"),
).source("ohlcv_latest", bool_debug=False, bool_fetch=True)
print(f"DF MKTDATA COIN MARKET: \n{df_}")
