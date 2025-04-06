import os
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.br.exchange.futures_closing_adj import FuturesClosingAdjB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.proxy_scrape import ProxyScrapeAll

# session = ProxyScrapeAll(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = FuturesClosingAdjB3(
    session=None, dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 3), cls_db=None
)

df_ = cls_.source("futures_closing_adj", bl_fetch=True, bl_debug=False)
print(f"DF FUTURES CLOSING ADJ B3: \n{df_}")
df_.info()
