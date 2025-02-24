# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.br.exchange.search_by_trading_session import SearchByTradingB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


session = ReqSession(
    bl_new_proxy=True, 
    bl_use_timer=True, 
    float_min_ratio_times_alive_dead=0.02,
    float_max_timeout=600
).session
print(session.proxies)

class_sbt_b3 = SearchByTradingB3(
    session=session,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1), 
    cls_db=None
)
df_price_report = class_sbt_b3.source('price_report', bl_fetch=True, bl_debug=False)
print(f'DF PRICE REPORT B3: \n{df_price_report}')
print(df_price_report.info())