# pypi.org libs
import os
import sys


# local libs
sys.path.append(os.path.abspath(os.path.join(os.path.realpath(__file__), "..", "..")))
from stpstone.ingestion.countries.br.registries.brazillian_banks import BrazilianBanksBCB
from stpstone.utils.calendars.calendar_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


# session = YieldFreeProxy(
#     bool_new_proxy=True,
#     bool_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = BrazilianBanksBCB(
    session=None
)
df_ = cls_.source("bcb_registry" , bool_fetch=True)
print(f"DF VOLUMES: \n{df_}")
df_.info()
