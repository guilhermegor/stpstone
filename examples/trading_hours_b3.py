# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.br.exchange.trading_hours import TradingHoursB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


# session = ReqSession(
#     bl_new_proxy=True, 
#     bl_use_timer=True, 
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = TradingHoursB3(
    session=None,
    cls_db=None
)
print('*** STOCKS TRADING HOURS ***')
df_ = cls_.source('stocks', bl_fetch=True, bl_debug=False)
print(f'DF : \n{df_}')
df_.info()

print('*** STOCK OPTIONS TRADING HOURS ***')
df_ = cls_.source('stock_options', bl_fetch=True, bl_debug=False)
print(f'DF : \n{df_}')
df_.info()