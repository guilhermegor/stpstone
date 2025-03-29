# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.registries.b3_trd_sec import B3TrdSec
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ReqSession


# session = ReqSession(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = B3TrdSec(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    cls_db=None
)

df_ = cls_.source("instruments_list", bl_fetch=True, bl_debug=False)
print(f"DF INSTRUMENTS LIST B3: \n{df_}")
df_.info()
