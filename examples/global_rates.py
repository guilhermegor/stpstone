import os


os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.ww.macroeconomics.global_rates import GlobalRates
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


# session = YieldFreeProxy(
#     bool_new_proxy=True,
#     bool_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = GlobalRates(
    session=None, date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 3), cls_db=None
)

# df_ = cls_.source("euribor", bool_fetch=True)
# print(f"DF EURIBOR: \n{df_}")
# df_.info()

# df_ = cls_.source("libor", bool_fetch=True)
# print(f"DF LIBOR: \n{df_}")
# df_.info()

# df_ = cls_.source("ester", bool_fetch=True)
# print(f"DF ESTER: \n{df_}")
# df_.info()

# df_ = cls_.source("sonia", bool_fetch=True)
# print(f"DF SONIA: \n{df_}")
# df_.info()

# df_ = cls_.source("sofr", bool_fetch=True)
# print(f"DF SOFR: \n{df_}")
# df_.info()

# df_ = cls_.source("central_banks", bool_fetch=True)
# print(f"DF CENTRAL BANKS RATES: \n{df_}")
# df_.info()

# df_ = cls_.source("usa_cpi", bool_fetch=True)
# print(f"DF AMERICAN CPI RATES: \n{df_}")
# df_.info()

# df_ = cls_.source("british_cpi", bool_fetch=True)
# print(f"DF BRITISH CPI RATES: \n{df_}")
# df_.info()

# df_ = cls_.source("canadian_cpi", bool_fetch=True)
# print(f"DF CANADIAN CPI RATES: \n{df_}")
# df_.info()

df_ = cls_.source("european_cpi", bool_fetch=True)
print(f"DF EUROPEAN CPI RATES: \n{df_}")
df_.info()
