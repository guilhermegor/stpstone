# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.options_calendar import OptionsCalendarB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy


# session = YieldFreeProxy(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = OptionsCalendarB3(
    session=None,
    cls_db=None
)
df_ = cls_.source('settlement_dates', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()
