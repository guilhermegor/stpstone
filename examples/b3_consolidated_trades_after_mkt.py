import os


os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.br.exchange.b3_consolidated_trades_after_mkt import (
	ConsolidatedTrdsAfterMktB3,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


session = YieldFreeProxy(
	bool_new_proxy=True,
	bool_use_timer=True,
	float_min_ratio_times_alive_dead=0.02,
	float_max_timeout=600,
).session
print(session.proxies)

cls_ = ConsolidatedTrdsAfterMktB3(
	session=None,
	date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
	cls_db=None,
)

df_ = cls_.source("consolidated_trades_information_after_mkt", bool_fetch=True)
print(f"DF CONSOLIDATED TRADES AFTER MARKET B3: \n{df_}")
df_.info()
