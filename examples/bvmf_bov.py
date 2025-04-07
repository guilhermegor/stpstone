# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.bvmf_bov import BVMFBOV
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy


# session = YieldFreeProxy(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = BVMFBOV(
    session=None,
    dt_ref=DatesBR().add_months(DatesBR().curr_date, -1)
)
df_ = cls_.source('volumes', bl_fetch=True, bl_debug=False)
print(f'DF VOLUMES: \n{df_}')
print(df_.info())
