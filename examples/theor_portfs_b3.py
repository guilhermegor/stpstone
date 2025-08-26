# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.indexes_theor_portf import IndexesTheorPortfB3
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


# session = YieldFreeProxy(bool_new_proxy=True).session

cls_ = IndexesTheorPortfB3(
    session=None,
    date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
    cls_db=None
)

print('*** IBOV THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibov', bool_debug=Falsebool_fetch=True)
print(f'DF: \n{df_}')
df_.info()

print('\n*** IBRX100 THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibrx100', bool_debug=Falsebool_fetch=True)
print(f'DF: \n{df_}')
df_.info()

print('\n*** IBRX50 THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibrx50', bool_debug=Falsebool_fetch=True)
print(f'DF: \n{df_}')
df_.info()

print('\n*** IBRA THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibra', bool_debug=Falsebool_fetch=True)
print(f'DF: \n{df_}')
df_.info()
