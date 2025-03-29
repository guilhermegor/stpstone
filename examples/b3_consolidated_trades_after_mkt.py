# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.consolidated_trades_after_mkt import ConsolidatedTrdsAfterMktB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ProxyScrape


# session = ProxyScrape(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = ConsolidatedTrdsAfterMktB3(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    cls_db=None
)

df_ = cls_.source("consolidated_trades_information_after_mkt", bl_fetch=True, bl_debug=False)
print(f"DF CONSOLIDATED TRADES AFTER MARKET B3: \n{df_}")
df_.info()
