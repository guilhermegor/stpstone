import os


os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.br.exchange.futures_closing_adj import FuturesClosingAdjB3
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


session = YieldFreeProxy(
    bool_new_proxy=True,
    bool_use_timer=True,
    float_min_ratio_times_alive_dead=0.02,
    float_max_timeout=600
).session
print(session.proxies)

cls_ = FuturesClosingAdjB3(
    session=None, date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 3), cls_db=None
)

df_ = cls_.source("futures_closing_adj", bool_fetch=True)
print(f"DF FUTURES CLOSING ADJ B3: \n{df_}")
df_.info()
