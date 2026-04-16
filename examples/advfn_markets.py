# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.ww.exchange.markets.advfn import ADVFNWW
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


session = YieldFreeProxy(
	bool_new_proxy=True,
	bool_use_timer=True,
	float_min_ratio_times_alive_dead=0.02,
	float_max_timeout=600,
).session
print(session.proxies)

cls_adfn_ww = ADVFNWW(
	session=None,
	date_start=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 5),
	date_end=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 0),
	str_market="BOV",
	str_ticker="PETR4",
)
df_ = cls_adfn_ww.source("daily_ohlcv", bool_fetch=True)
print(f"DF OHLCV: \n{df_}")
print(df_.info())
