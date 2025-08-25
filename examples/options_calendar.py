# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.options_calendar import OptionsCalendarB3
from stpstone.utils.cals.cal_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


# session = YieldFreeProxy(
#     bool_new_proxy=True,
#     bool_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = OptionsCalendarB3(
    session=None,
    cls_db=None
)
df_ = cls_.source('settlement_dates', bool_fetch=True)
print(f'DF : \n{df_}')
df_.info()
