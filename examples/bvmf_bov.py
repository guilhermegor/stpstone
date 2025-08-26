# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.bvmf_bov import BVMFBOV
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


# session = YieldFreeProxy(
#     bool_new_proxy=True,
#     bool_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = BVMFBOV(
    session=None,
    date_ref=DatesBRAnbima().add_months(DatesBRAnbima().curr_date(), -1)
)
df_ = cls_.source('volumes', bool_fetch=True)
print(f'DF VOLUMES: \n{df_}')
print(df_.info())
