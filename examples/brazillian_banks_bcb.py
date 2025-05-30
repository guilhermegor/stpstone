# pypi.org libs
import os
import sys
# local libs
sys.path.append(os.path.abspath(os.path.join(os.path.realpath(__file__), "..", "..")))
from stpstone.ingestion.countries.br.registries.brazillian_banks import BrazillianBanksBCB
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy


# session = YieldFreeProxy(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = BrazillianBanksBCB(
    session=None
)
df_ = cls_.source("bcb_registry" , bl_fetch=True)
print(f"DF VOLUMES: \n{df_}")
df_.info()
