import os
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.ww.macroeconomics.global_rates import GlobalRates
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy

# session = YieldFreeProxy(
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

# df_ = cls_.source("ester", bl_fetch=True, bl_debug=False)
# print(f"DF ESTER: \n{df_}")
# df_.info()

# df_ = cls_.source("sonia", bl_fetch=True, bl_debug=False)
# print(f"DF SONIA: \n{df_}")
# df_.info()

# df_ = cls_.source("sofr", bl_fetch=True, bl_debug=False)
# print(f"DF SOFR: \n{df_}")
# df_.info()

# df_ = cls_.source("central_banks", bl_fetch=True, bl_debug=False)
# print(f"DF CENTRAL BANKS RATES: \n{df_}")
# df_.info()

# df_ = cls_.source("usa_cpi", bl_fetch=True, bl_debug=False)
# print(f"DF AMERICAN CPI RATES: \n{df_}")
# df_.info()

# df_ = cls_.source("british_cpi", bl_fetch=True, bl_debug=False)
# print(f"DF BRITISH CPI RATES: \n{df_}")
# df_.info()

# df_ = cls_.source("canadian_cpi", bl_fetch=True, bl_debug=False)
# print(f"DF CANADIAN CPI RATES: \n{df_}")
# df_.info()

df_ = cls_.source("european_cpi", bl_fetch=True, bl_debug=False)
print(f"DF EUROPEAN CPI RATES: \n{df_}")
df_.info()
