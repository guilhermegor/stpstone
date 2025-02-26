# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.br.exchange.indexes_theor_portf import IndexesTheorPortfB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


# session = ReqSession(bl_new_proxy=True).session

cls_ = IndexesTheorPortfB3(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1), 
    cls_db=None
)

print('*** IBOV THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibov', bl_debug=False, bl_fetch=True)
print(f'DF: \n{df_}')
df_.info()

print('\n*** IBRX100 THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibrx100', bl_debug=False, bl_fetch=True)
print(f'DF: \n{df_}')
df_.info()

print('\n*** IBRX50 THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibrx50', bl_debug=False, bl_fetch=True)
print(f'DF: \n{df_}')
df_.info()

print('\n*** IBRA THEORETICAL PORTFOLIO ***')
df_ = cls_.source('ibra', bl_debug=False, bl_fetch=True)
print(f'DF: \n{df_}')
df_.info()