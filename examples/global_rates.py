import os
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.ww.macroeconomics.global_rates import GlobalRates
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession

# session = ReqSession(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = GlobalRates(
    session=None, dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 3), cls_db=None
)

# df_ = cls_.source("euribor", bl_fetch=True, bl_debug=False)
# print(f"DF EURIBOR: \n{df_}")
# df_.info()

# df_ = cls_.source("libor", bl_fetch=True, bl_debug=False)
# print(f"DF LIBOR: \n{df_}")
# df_.info()

df_ = cls_.source("ester", bl_fetch=True, bl_debug=False)
print(f"DF ESTER: \n{df_}")
df_.info()
