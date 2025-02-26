# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.mtm.br.exchange.historical_sigma import HistoricalSigmaB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


# session = ReqSession(
#     bl_new_proxy=True, 
#     bl_use_timer=True, 
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = HistoricalSigmaB3(
    session=None,
    cls_db=None
)

# print('*** HISTORICAL SIGMA B3 - GROUP 1 ***')
# df_ = cls_.source('group_1', bl_fetch=True, bl_debug=False)
# print(f'DF : \n{df_}')
# df_.info()

# print('*** HISTORICAL SIGMA B3 - GROUP 2 ***')
# df_ = cls_.source('group_2', bl_fetch=True, bl_debug=False)
# print(f'DF : \n{df_}')
# df_.info()

# print('*** HISTORICAL SIGMA B3 - GROUP 3 ***')
# df_ = cls_.source('group_3', bl_fetch=True, bl_debug=False)
# print(f'DF : \n{df_}')
# df_.info()

print('*** HISTORICAL SIGMA ASSETS B3 ***')
df_ = cls_.composition
print(f'DF : \n{df_}')
df_.info()