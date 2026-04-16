# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.ww.exchange.crypto.coinpaprika import CoinPaprika
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


session = YieldFreeProxy(bool_new_proxy=True).session
df_ = CoinPaprika(
	session=session,
	date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 5),
	cls_db=None,
).source("ohlcv_latest", bool_debug=True, bool_fetch=True)
print(f"DF MKTDATA COIN PAPRIKA: \n{df_}")
